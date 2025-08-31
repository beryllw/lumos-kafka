#include "kafka_utils.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {

unique_ptr<RdKafka::Conf> KafkaUtils::CreateGlobalConf(const unordered_map<std::string, std::string> &config_kv) {
	unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
	if (!conf) {
		throw InternalException("Failed to create Kafka global config");
	}

	for (const auto &config_pair : config_kv) {
		const std::string &key = config_pair.first;
		const std::string &value = config_pair.second;
		std::string err_str;
		if (conf->set(key, value, err_str) != RdKafka::Conf::CONF_OK) {
			throw InvalidConfigurationException(StringUtil::Format("Kafka config '%s' set failed: %s", key, err_str));
		}
	}
	return conf;
}

unique_ptr<RdKafka::Conf> KafkaUtils::CreateTopicConf() {
	unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC));
	if (!conf) {
		throw InternalException("Failed to create Kafka topic config");
	}
	return conf;
}

std::vector<int32_t> KafkaUtils::GetTopicPartitions(RdKafka::Consumer *consumer, const std::string &topic,
                                                    int timeout_ms) {
	std::vector<int32_t> partitions;
	RdKafka::Metadata *metadata = nullptr;
	RdKafka::ErrorCode err = consumer->metadata(false, nullptr, &metadata, timeout_ms);

	if (err != RdKafka::ERR_NO_ERROR) {
		throw InternalException(StringUtil::Format("Query Kafka metadata failed: %s", RdKafka::err2str(err)));
	}

	const RdKafka::Metadata::TopicMetadataVector *topic_metas = metadata->topics();
	bool topic_found = false;

	for (auto it = topic_metas->begin(); it != topic_metas->end(); ++it) {
		const RdKafka::TopicMetadata *topic_meta = *it;
		if (topic_meta->topic() == topic) {
			topic_found = true;
			const RdKafka::TopicMetadata::PartitionMetadataVector *part_metas = topic_meta->partitions();

			for (auto part_it = part_metas->begin(); part_it != part_metas->end(); ++part_it) {
				const RdKafka::PartitionMetadata *part_meta = *part_it;
				partitions.push_back(part_meta->id());
			}
			break;
		}
	}

	delete metadata;

	if (!topic_found) {
		throw InternalException(StringUtil::Format("Topic '%s' not found", topic));
	}
	if (partitions.empty()) {
		throw InternalException(StringUtil::Format("Topic '%s' exists but has no partitions", topic));
	}

	return partitions;
}

int64_t KafkaUtils::GetPartitionHighWatermark(RdKafka::Consumer *consumer, const std::string &topic, int32_t partition,
                                              int timeout_ms) {
	int64_t low = 0, high = 0;
	RdKafka::ErrorCode err = consumer->query_watermark_offsets(topic, partition, &low, &high, timeout_ms);

	if (err != RdKafka::ERR_NO_ERROR) {
		throw InternalException(
		    StringUtil::Format("Query partition %d watermark failed: %s", partition, RdKafka::err2str(err)));
	}
	return high;
}

bool KafkaUtils::IsMessageError(const RdKafka::Message &msg, std::string &err_msg_out) {
	switch (msg.err()) {
	case RdKafka::ERR_NO_ERROR:
		return false;
	case RdKafka::ERR__PARTITION_EOF:
		err_msg_out = "Partition EOF reached";
		return true;
	case RdKafka::ERR__TIMED_OUT:
		err_msg_out = "Consume timed out";
		return true;
	default:
		err_msg_out = StringUtil::Format("Consume error: %s", RdKafka::err2str(msg.err()));
		return true;
	}
}

} // namespace duckdb