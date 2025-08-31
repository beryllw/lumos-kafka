#pragma once

#include <atomic>
#include <vector>
#include <string>
#include <thread>

#include "duckdb/function/table_function.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include <librdkafka/rdkafkacpp.h>
#include "duckdb/common/enums/set_scope.hpp"
#include "kafka_utils.hpp"

namespace duckdb {
struct KafkaBindData;

// Partition consume state (defined only once, remove duplicate definitions)
struct PartitionConsumeState {
	int32_t partition_id;   // Partition ID
	int64_t current_offset; // Next offset to consume (-1 means not started)
	int64_t end_offset;     // End offset for bounded stream (-1 means unbounded)
	bool is_completed;      // Whether bounded stream is completed
	bool is_started;        // Whether partition consumption has started

	// Unbounded stream constructor
	PartitionConsumeState(int32_t part_id)
	    : partition_id(part_id), current_offset(-1), end_offset(-1), is_completed(false), is_started(false) {
	}

	// Bounded stream constructor
	PartitionConsumeState(int32_t part_id, int64_t end_off)
	    : partition_id(part_id), current_offset(-1), end_offset(end_off), is_completed(false), is_started(false) {
	}

	// Check if bounded stream is completed (helper method for unified logic)
	void CheckCompleted() {
		if (end_offset == -1)
			return; // Unbounded stream does not check
		if (end_offset == 0) {
			is_completed = true;
		} else {
			is_completed = (current_offset >= end_offset);
		}
	}
};

// Kafka scan function class (core TableFunction implementation)
class KafkaScanFunction : public TableFunction {
public:
	KafkaScanFunction();
};

// Auxiliary function class (optional: e.g. clear Kafka consumer cache)
class KafkaClearCacheFunction : public TableFunction {
public:
	KafkaClearCacheFunction();
	static void ClearCacheOnSetting(ClientContext &context, SetScope scope, Value &parameter);
};

} // namespace duckdb
