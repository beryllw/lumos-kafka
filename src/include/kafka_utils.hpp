#pragma once

#include <librdkafka/rdkafkacpp.h>
#include <string>
#include <vector>
#include <unordered_map>
#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {

class KafkaUtils {
public:
	// 1. 创建全局配置（CONF_GLOBAL）
	static unique_ptr<RdKafka::Conf> CreateGlobalConf(const std::unordered_map<std::string, std::string> &config_kv);

	// 2. 创建 Topic 配置（CONF_TOPIC）
	static unique_ptr<RdKafka::Conf> CreateTopicConf();

	// 3. 查询 Topic 所有分区
	static std::vector<int32_t> GetTopicPartitions(RdKafka::Consumer *consumer, const std::string &topic,
	                                               int timeout_ms = 5000);

	// 4. 查询分区水位（有界流结束偏移）
	static int64_t GetPartitionHighWatermark(RdKafka::Consumer *consumer, const std::string &topic, int32_t partition,
	                                         int timeout_ms = 5000);

	// 5. 解析 librdkafka 消息错误
	static bool IsMessageError(const RdKafka::Message &msg, std::string &err_msg_out);
};

} // namespace duckdb