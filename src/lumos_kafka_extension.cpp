#define DUCKDB_EXTENSION_MAIN

#include "lumos_kafka_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "kafka_scanner.hpp"

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

static void LoadInternal(DatabaseInstance &instance) {

	// Register kafka scan function
	TableFunction kafka_scan_function = KafkaScanFunction();
	ExtensionUtil::RegisterFunction(instance, kafka_scan_function);
}

void LumosKafkaExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string LumosKafkaExtension::Name() {
	return "lumos_kafka";
}

std::string LumosKafkaExtension::Version() const {
#ifdef EXT_VERSION_LUMOS_KAFKA
	return EXT_VERSION_LUMOS_KAFKA;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void lumos_kafka_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::LumosKafkaExtension>();
}

DUCKDB_EXTENSION_API const char *lumos_kafka_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
