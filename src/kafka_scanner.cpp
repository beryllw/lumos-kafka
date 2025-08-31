#include "kafka_scanner.hpp"
#include "kafka_utils.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include <chrono>

namespace duckdb {

// Constant definitions: manage magic numbers for maintainability
constexpr idx_t KAFKA_MAX_THREAD_LIMIT = 16;          // Maximum consumer threads
constexpr int KAFKA_CONSUME_TIMEOUT_MS = 5;           // Message consume timeout (ms)
constexpr int KAFKA_BATCH_TIMEOUT_MS = 10;            // Unbounded stream batch timeout (ms)
constexpr idx_t KAFKA_MAX_MSG_PER_PART_PER_POLL = 10; // Max messages per partition per poll
constexpr int KAFKA_START_PARTITION_RETRY = 3;        // Partition start retry count

// Forward declarations: core data structures
struct KafkaGlobalState;
struct KafkaLocalState;
struct PartitionConsumeState;
static InsertionOrderPreservingMap<string> KafkaScanToString(TableFunctionToStringInput &input);
static void KafkaScanSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
                               const TableFunction &function);
static unique_ptr<FunctionData> KafkaScanDeserialize(Deserializer &deserializer, TableFunction &function);

struct KafkaBindData : public FunctionData {
	// Basic configuration
	string bootstrap_servers; // Kafka address
	string group_id;          // Consumer group ID
	string topic;             // Topic to consume
	string scan_startup_mode; // Startup offset mode (earliest/latest)
	string scan_bounded_mode; // Stream mode (unbounded/latest-offset)
	bool is_bounded;          // Whether it is a bounded stream

	// librdkafka configuration (global reuse)
	unique_ptr<RdKafka::Conf> global_conf;
	unique_ptr<RdKafka::Conf> topic_conf;

	// Return column definitions (fixed: topic/partition/offset/value)
	vector<string> result_column_names = {"topic", "partition", "time", "offset", "value"};
	vector<LogicalType> result_column_types = {
	    LogicalType::VARCHAR, // topic
	    LogicalType::INTEGER, // partition
	    LogicalType::BIGINT,  // time
	    LogicalType::BIGINT,  // offset
	    LogicalType::VARCHAR  // value
	};

	// Constructor: parse parameters and initialize configuration
	KafkaBindData(const unordered_map<string, string> &named_params) {
		// 1. Required parameter validation
		ExtractParam(named_params, "bootstrap_servers", bootstrap_servers);
		ExtractParam(named_params, "group_id", group_id);
		ExtractParam(named_params, "topic", topic);

		// 2. Optional parameter defaults
		scan_startup_mode = ExtractParamOrDefault(named_params, "scan_startup_mode", "earliest");
		scan_bounded_mode = ExtractParamOrDefault(named_params, "scan_bounded_mode", "unbounded");
		is_bounded = (scan_bounded_mode != "unbounded");

		// 3. Initialize librdkafka configuration
		unordered_map<string, string> kafka_conf_kv = {
		    {"bootstrap.servers", bootstrap_servers},
		    {"group.id", group_id},
		    {"auto.offset.reset", scan_startup_mode}, // Startup offset mapping
		    {"enable.auto.commit", "false"},          // Manual offset commit (avoid duplicate consumption)
		};

		// Override user custom config ("kafka." prefix means native Kafka config)
		for (const auto &param_pair : named_params) {
			const string &key = param_pair.first;
			const string &value = param_pair.second;
			if (key.find("kafka.") == 0) {        // Match prefix
				string kafka_key = key.substr(6); // Remove "kafka." prefix
				kafka_conf_kv[kafka_key] = value;
			}
		}

		// Create configuration
		global_conf = KafkaUtils::CreateGlobalConf(kafka_conf_kv);
		topic_conf = KafkaUtils::CreateTopicConf();
	}

	// Copy function (required by FunctionData interface)
	unique_ptr<FunctionData> Copy() const override {
		unordered_map<string, string> named_params = {{"bootstrap_servers", bootstrap_servers},
		                                              {"group_id", group_id},
		                                              {"topic", topic},
		                                              {"scan_startup_mode", scan_startup_mode},
		                                              {"scan_bounded_mode", scan_bounded_mode}};
		return make_uniq<KafkaBindData>(named_params);
	}

	// Equality check (required by FunctionData interface)
	bool Equals(const FunctionData &other_p) const override {
		const auto &other = other_p.Cast<KafkaBindData>();
		return bootstrap_servers == other.bootstrap_servers && group_id == other.group_id && topic == other.topic &&
		       scan_startup_mode == other.scan_startup_mode && scan_bounded_mode == other.scan_bounded_mode;
	}

private:
	// Helper: extract required parameter
	void ExtractParam(const unordered_map<string, string> &params, const string &key, string &out) {
		auto it = params.find(key);
		if (it == params.end()) {
			throw BinderException(StringUtil::Format("Kafka parameter '%s' is missing", key));
		}
		out = it->second;
	}

	// Helper: extract optional parameter (with default value)
	string ExtractParamOrDefault(const unordered_map<string, string> &params, const string &key,
	                             const string &default_val) {
		auto it = params.find(key);
		return (it != params.end()) ? it->second : default_val;
	}
};

struct KafkaGlobalState : public GlobalTableFunctionState {
	const KafkaBindData &bind_data;                       // Bind data (read-only)
	vector<int32_t> all_partitions;                       // All topic partitions
	unordered_map<int32_t, int64_t> part_high_watermarks; // End offset for bounded stream partitions
	mutex partition_assign_mutex;                         // Partition assignment lock (protect static assignment)
	atomic<idx_t> thread_launch_counter {0};              // Thread launch sequence counter
	idx_t total_threads;

	// Constructor: initialize global metadata
	explicit KafkaGlobalState(const KafkaBindData &bind_data_p) : bind_data(bind_data_p) {
		// 1. Create temporary consumer for metadata query (not for actual consumption)
		string err_str;
		unique_ptr<RdKafka::Consumer> meta_consumer(RdKafka::Consumer::create(bind_data.global_conf.get(), err_str));

		if (!meta_consumer) {
			throw InternalException(StringUtil::Format("Create metadata consumer failed: %s", err_str));
		}

		// 2. Get all topic partitions
		all_partitions = KafkaUtils::GetTopicPartitions(meta_consumer.get(), bind_data.topic);

		// 3. Bounded stream: query end offset (high watermark) for all partitions
		if (bind_data.is_bounded) {
			for (int32_t part : all_partitions) {
				int64_t high = KafkaUtils::GetPartitionHighWatermark(meta_consumer.get(), bind_data.topic, part);
				part_high_watermarks[part] = high;
			}
		}
		total_threads = MaxThreads();
	}

	// Static partition assignment: evenly assign partitions by thread launch sequence
	vector<PartitionConsumeState> AssignPartitionsToThread(idx_t thread_launch_seq) {
		lock_guard<mutex> lock(partition_assign_mutex);
		vector<PartitionConsumeState> assigned_parts;
		size_t partition_count = all_partitions.size();

		// Assign by launch sequence (total_threads ≤ partition_count, ensured by MaxThreads)
		size_t base_count = partition_count / total_threads;
		idx_t remainder = partition_count % total_threads;
		size_t start_idx = thread_launch_seq * base_count + MinValue(thread_launch_seq, remainder);
		size_t end_idx = start_idx + base_count + (thread_launch_seq < remainder ? 1 : 0);

		// Assign partitions and initialize consume state
		for (size_t i = start_idx; i < end_idx; ++i) {
			int32_t part = all_partitions[i];
			if (bind_data.is_bounded) {
				assigned_parts.emplace_back(part, part_high_watermarks[part]);
			} else {
				assigned_parts.emplace_back(part);
			}
		}

		return assigned_parts;
	}

	// Max threads: not more than partition count and not more than 16
	idx_t MaxThreads() const override {
		return 1;
	}
};

struct KafkaLocalState : public LocalTableFunctionState {
	const KafkaBindData &bind_data;                    // Bind data (read-only)
	vector<PartitionConsumeState> assigned_partitions; // Partitions assigned to this thread
	idx_t thread_launch_seq;                           // Thread launch sequence

	// Thread-local consumer (KafkaConsumer API supports multi-partition assign)
	unique_ptr<RdKafka::KafkaConsumer> consumer;

	// Batch buffer (designed per DuckDB standard chunk size)
	DataChunk batch_buffer;
	std::chrono::steady_clock::time_point batch_start_time; // Batch start time
	bool is_batch_started;                                  // Whether current batch has started
	bool all_partitions_completed;                          // Whether all partitions are completed for bounded stream

	// Constructor: initialize KafkaConsumer and assign partitions
	explicit KafkaLocalState(const KafkaBindData &bind_data_p, idx_t launch_seq)
	    : bind_data(bind_data_p), thread_launch_seq(launch_seq), is_batch_started(false),
	      all_partitions_completed(false) {
		// 1. Initialize batch buffer
		batch_buffer.Initialize(Allocator::DefaultAllocator(), bind_data.result_column_types);

		// 2. Create KafkaConsumer instance (supports multi-partition assign)
		string err_str;
		consumer =
		    unique_ptr<RdKafka::KafkaConsumer>(RdKafka::KafkaConsumer::create(bind_data.global_conf.get(), err_str));

		if (!consumer) {
			throw InternalException(
			    StringUtil::Format("Thread (seq: %lld) create Kafka consumer failed: %s", thread_launch_seq, err_str));
		}
	}

	// Assign and start partition consumption in batch
	void AssignAndStartPartitions() {
		if (assigned_partitions.empty()) {
			return;
		}

		// Build partition-offset mapping for assign
		vector<RdKafka::TopicPartition *> partitions;
		for (auto &part_state : assigned_partitions) {
			// Determine initial offset
			int64_t start_offset = (part_state.current_offset == -1)
			                           ? (bind_data.scan_startup_mode == "earliest" ? RdKafka::Topic::OFFSET_BEGINNING
			                                                                        : RdKafka::Topic::OFFSET_END)
			                           : part_state.current_offset;

			// Create TopicPartition object
			auto *tp = RdKafka::TopicPartition::create(bind_data.topic, part_state.partition_id, start_offset);
			partitions.push_back(tp);
			part_state.is_started = true;
		}

		// Batch assign partitions (KafkaConsumer API core improvement)
		RdKafka::ErrorCode err = consumer->assign(partitions);
		// Release TopicPartition resources
		for (auto *tp : partitions) {
			delete tp;
		}

		if (err != RdKafka::ERR_NO_ERROR) {
			throw InternalException(StringUtil::Format("Thread (seq: %lld) assign partitions failed: %s",
			                                           thread_launch_seq, RdKafka::err2str(err)));
		}
	}

	// Stop consumption for all partitions
	void StopAllPartitions() {
		if (!assigned_partitions.empty()) {
			consumer->unassign();
			for (auto &part_state : assigned_partitions) {
				part_state.is_started = false;
			}
		}
	}

	// Start a new batch
	void StartNewBatch() {
		batch_buffer.Reset();
		batch_start_time = std::chrono::steady_clock::now();
		is_batch_started = true;
	}

	// Check if batch needs to be flushed
	bool NeedFlushBatch() const {
		if (!is_batch_started)
			return false;

		// Condition 1: reach standard chunk size
		if (batch_buffer.size() >= STANDARD_VECTOR_SIZE) {
			return true;
		}

		// Condition 2: unbounded stream timeout
		if (!bind_data.is_bounded) {
			auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() -
			                                                                      batch_start_time);
			if (duration.count() >= KAFKA_BATCH_TIMEOUT_MS) {
				return true;
			}
		}

		return false;
	}

	// Write message to batch
	void WriteMessageToBatch(const RdKafka::Message &msg) {
		if (!is_batch_started) {
			StartNewBatch();
		}

		// Get column data pointers
		auto topic_vec = FlatVector::GetData<string_t>(batch_buffer.data[0]);
		auto partition_vec = FlatVector::GetData<int32_t>(batch_buffer.data[1]);
		auto time_vec = FlatVector::GetData<int64_t>(batch_buffer.data[2]);
		auto offset_vec = FlatVector::GetData<int64_t>(batch_buffer.data[3]);
		auto &value_validity = FlatVector::Validity(batch_buffer.data[4]);
		auto value_vec = FlatVector::GetData<string_t>(batch_buffer.data[4]);

		// Fill data
		idx_t row_idx = batch_buffer.size();
		topic_vec[row_idx] = StringVector::AddString(batch_buffer.data[0], bind_data.topic);
		partition_vec[row_idx] = msg.partition();
		offset_vec[row_idx] = msg.offset();
		time_vec[row_idx] = msg.timestamp().timestamp;

		if (msg.payload()) {
			value_vec[row_idx] =
			    StringVector::AddString(batch_buffer.data[4], static_cast<const char *>(msg.payload()), msg.len());
			value_validity.SetValid(row_idx);
		} else {
			value_validity.SetInvalid(row_idx);
		}

		batch_buffer.SetCardinality(row_idx + 1);

		// Update current offset for corresponding partition
		for (auto &part_state : assigned_partitions) {
			if (part_state.partition_id == msg.partition()) {
				part_state.current_offset = msg.offset() + 1;
				break;
			}
		}
	}

	// Check if all partitions are completed
	bool CheckAllPartitionsCompleted() {
		if (!bind_data.is_bounded)
			return false;

		all_partitions_completed = true;
		for (auto &part_state : assigned_partitions) {
			part_state.CheckCompleted();
			if (!part_state.is_completed) {
				all_partitions_completed = false;
				break;
			}
		}
		return all_partitions_completed;
	}

	// Destructor: release resources
	~KafkaLocalState() override {
		StopAllPartitions();
	}
};

// Bind function: parse parameters and set return types
static unique_ptr<FunctionData> KafkaScanBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
	// Parse named parameters (convert to lowercase to avoid case sensitivity)
	unordered_map<string, string> named_params;
	for (const auto &param_pair : input.named_parameters) {
		string key = param_pair.first;
		const auto &value = param_pair.second;
		named_params[StringUtil::Lower(key)] = StringValue::Get(value);
	}

	// Create BindData (internal validation for required parameters)
	auto bind_data = make_uniq<KafkaBindData>(named_params);

	// Set return column info (reuse BindData definition to avoid hardcoding)
	names = bind_data->result_column_names;
	return_types = bind_data->result_column_types;

	return std::move(bind_data);
}

// Global state initialization: query topic metadata
static unique_ptr<GlobalTableFunctionState> KafkaInitGlobalState(ClientContext &context,
                                                                 TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<KafkaBindData>();
	return make_uniq<KafkaGlobalState>(bind_data);
}

// Local state initialization: assign partitions and start consumption
static unique_ptr<LocalTableFunctionState> KafkaInitLocalState(ExecutionContext &context, TableFunctionInitInput &input,
                                                               GlobalTableFunctionState *global_state_p) {
	auto &bind_data = input.bind_data->Cast<KafkaBindData>();
	auto &global_state = global_state_p->Cast<KafkaGlobalState>();

	// 1. Get thread launch sequence (atomic increment, ensure uniqueness)
	idx_t thread_launch_seq = global_state.thread_launch_counter.fetch_add(1);

	// 2. Create local state
	auto local_state = make_uniq<KafkaLocalState>(bind_data, thread_launch_seq);

	// 3. Assign partitions (global state ensures thread safety)
	local_state->assigned_partitions = global_state.AssignPartitionsToThread(thread_launch_seq);
	if (local_state->assigned_partitions.empty()) {
		return std::move(local_state);
	}

	// 4. Sort partitions (ensure consistent processing order)
	sort(
	    local_state->assigned_partitions.begin(), local_state->assigned_partitions.end(),
	    [](const PartitionConsumeState &a, const PartitionConsumeState &b) { return a.partition_id < b.partition_id; });

	// 5. Batch assign and start partition consumption (with retry mechanism)
	int retry = KAFKA_START_PARTITION_RETRY;
	while (retry-- > 0) {
		try {
			local_state->AssignAndStartPartitions();
			break; // Batch start successful, exit retry
		} catch (const Exception &e) {
			// Last retry failed, throw exception
			if (retry == 0) {
				throw InternalException(
				    StringUtil::Format("Thread (seq: %lld) assign partitions failed after %d retries: %s",
				                       thread_launch_seq, KAFKA_START_PARTITION_RETRY, e.what()));
			}
			// Wait briefly before retry
			std::this_thread::sleep_for(std::chrono::milliseconds(100 * (KAFKA_START_PARTITION_RETRY - retry)));
		}
	}

	return std::move(local_state);
}

// Core scan function: consume Kafka messages and output chunk (adapted for KafkaConsumer API)
static void KafkaScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &local_state = data.local_state->Cast<KafkaLocalState>();
	auto &bind_data = data.bind_data->Cast<KafkaBindData>();

	// 1. Check if all partitions are completed for bounded stream (return empty chunk if done)
	if (local_state.CheckAllPartitionsCompleted()) {
		output.SetCardinality(0);
		return;
	}

	// 2. Initialize batch (start on first call)
	if (!local_state.is_batch_started) {
		local_state.StartNewBatch();
	}

	// 3. Consume messages until output condition is met
	while (true) {
		// Check if need to end early
		if (local_state.CheckAllPartitionsCompleted()) {
			output.Reference(local_state.batch_buffer);
			output.SetCardinality(local_state.batch_buffer.size());
			local_state.is_batch_started = false;
			return;
		}

		// Consume messages from all assigned partitions (KafkaConsumer handles multi-partition)
		unique_ptr<RdKafka::Message> msg(local_state.consumer->consume(KAFKA_CONSUME_TIMEOUT_MS));

		// Handle message result
		string err_msg;
		if (KafkaUtils::IsMessageError(*msg, err_msg)) {
			if (err_msg == "Partition EOF reached") {
				// Bounded stream: partition reached end offset, continue other partitions
				continue;
			} else if (err_msg == "Consume timed out") {
				// No message timeout: check if batch needs output
				if (local_state.NeedFlushBatch() || local_state.CheckAllPartitionsCompleted()) {
					break;
				}
				continue;
			} else {
				// Other errors: terminate scan and throw exception
				throw InternalException(StringUtil::Format("Thread (seq: %lld) consume message failed: %s",
				                                           local_state.thread_launch_seq, err_msg));
			}
		}

		// Message OK: write to buffer
		local_state.WriteMessageToBatch(*msg);

		// Check if batch needs output
		if (local_state.NeedFlushBatch()) {
			break;
		}
	}

	// Output current batch data
	output.Reference(local_state.batch_buffer);
	output.SetCardinality(local_state.batch_buffer.size());
	local_state.is_batch_started = false;
}

// Kafka scan function constructor (register metadata and callbacks)
KafkaScanFunction::KafkaScanFunction()
    : TableFunction("kafka_scan",         // Function name
                    {},                   // Positional parameters (none, all use named parameters)
                    KafkaScan,            // Core scan function
                    KafkaScanBind,        // Bind function
                    KafkaInitGlobalState, // Global initialization function
                    KafkaInitLocalState   // Local initialization function
      ) {

	global_initialization = TableFunctionInitialization::INITIALIZE_ON_SCHEDULE;

	// Register supported named parameters (user input)
	named_parameters["bootstrap_servers"] = LogicalType::VARCHAR; // Kafka address
	named_parameters["group_id"] = LogicalType::VARCHAR;          // Consumer group ID
	named_parameters["topic"] = LogicalType::VARCHAR;             // Topic to consume
	named_parameters["scan_startup_mode"] = LogicalType::VARCHAR; // Startup offset (earliest/latest)
	named_parameters["scan_bounded_mode"] = LogicalType::VARCHAR; // Stream mode (unbounded/latest-offset)
	// Native Kafka config (prefix kafka.)
	named_parameters["kafka.sasl.username"] = LogicalType::VARCHAR;
	named_parameters["kafka.sasl.password"] = LogicalType::VARCHAR;
	named_parameters["kafka.security.protocol"] = LogicalType::VARCHAR;

	// Function property config (DuckDB extension spec)
	to_string = KafkaScanToString;      // Function stringification (for EXPLAIN)
	serialize = KafkaScanSerialize;     // Serialization (not implemented)
	deserialize = KafkaScanDeserialize; // Deserialization (not implemented)
	projection_pushdown = false;        // Disable projection pushdown (Kafka messages need full consumption)
}

// Function stringification: for EXPLAIN statement display
static InsertionOrderPreservingMap<string> KafkaScanToString(TableFunctionToStringInput &input) {
	InsertionOrderPreservingMap<string> result;
	auto &bind_data = input.bind_data->Cast<KafkaBindData>();
	result["Table"] = bind_data.topic;
	result["Type"] = bind_data.is_bounded ? "Bounded Kafka Scan" : "Unbounded Kafka Scan";
	return result;
}

// Serialization function (not implemented)
static void KafkaScanSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
                               const TableFunction &function) {
	throw NotImplementedException("KafkaScanSerialize is not implemented");
}

// Deserialization function (not implemented)
static unique_ptr<FunctionData> KafkaScanDeserialize(Deserializer &deserializer, TableFunction &function) {
	throw NotImplementedException("KafkaScanDeserialize is not implemented");
}

} // namespace duckdb