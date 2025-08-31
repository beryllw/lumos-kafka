# Lumos-Kafka

This repository is based on https://github.com/duckdb/extension-template, check it out if you want to build and ship your own DuckDB extension.

---

This extension, `lumos_kafka`, allow you to scan Kafka topics in batch or streaming mode.


## Building
### Managing dependencies
DuckDB extensions uses VCPKG for dependency management. Enabling VCPKG is very simple: follow the [installation instructions](https://vcpkg.io/en/getting-started) or just run the following:
```shell
git clone https://github.com/Microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake
```
Note: VCPKG is only required for extensions that want to rely on it for dependency management. If you want to develop an extension without dependencies, or want to do your own dependency management, just skip this step. Note that the example extension uses VCPKG to build with a dependency for instructive purposes, so when skipping this step the build may not work without removing the dependency.

### Build steps
Now to build the extension, run:
```sh
make
```
The main binaries that will be built are:
```sh
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/lumos_kafka/lumos_kafka.duckdb_extension
```
- `duckdb` is the binary for the duckdb shell with the extension code automatically loaded.
- `unittest` is the test runner of duckdb. Again, the extension is already linked into the binary.
- `lumos_kafka.duckdb_extension` is the loadable binary as it would be distributed.

## Running the extension
To run the extension code, simply start the shell with `./build/release/duckdb`.

You can then directly query Kafka data using the `kafka_scan()` table function. It accepts named parameters to configure the Kafka connection and consumption behavior, and returns Kafka message metadata and content as a table.

```
D select * from kafka_scan(bootstrap_servers='localhost:9092',group_id='test',topic='test-topic',scan_bounded_mode='latest');
┌────────────┬───────────┬───────────────┬────────┬─────────┐
│    topic   │ partition │     time      │ offset │  value  │
│   varchar  │   int32   │     int64     │ int64  │ varchar │
├────────────┼───────────┼───────────────┼────────┼─────────┤
│ test-topic │         0 │ 1756608152679 │      0 │ test1   │
│ test-topic │         0 │ 1756608189938 │      1 │ test2   │
│ test-topic │         0 │ 1756611365750 │      2 │ test0   │
│ test-topic │         1 │ 1756611152494 │      0 │ test3   │
│ test-topic │         1 │ 1756611167614 │      1 │ test4   │
│ test-topic │         1 │ 1756611206153 │      2 │ test5   │
│ test-topic │         1 │ 1756611285598 │      3 │ test5   │
└────────────┴───────────┴───────────────┴────────┴─────────┘
```

Key Output Columns:

- `topic`: Kafka topic the message belongs to
- `partition`: Kafka partition ID of the message
- `time`: Message timestamp (milliseconds since epoch, int64)
- `offset`: Message offset within its partition
- `value`: Message content (stored as varchar)

## Running the tests
Different tests can be created for DuckDB extensions. The primary way of testing DuckDB extensions should be the SQL tests in `./test/sql`. These SQL tests can be run using:
```sh
make test
```

### Installing the deployed binaries
To install your extension binaries from S3, you will need to do two things. Firstly, DuckDB should be launched with the
`allow_unsigned_extensions` option set to true. How to set this will depend on the client you're using. Some examples:

CLI:
```shell
duckdb -unsigned
```

Python:
```python
con = duckdb.connect(':memory:', config={'allow_unsigned_extensions' : 'true'})
```

NodeJS:
```js
db = new duckdb.Database(':memory:', {"allow_unsigned_extensions": "true"});
```

Secondly, you will need to set the repository endpoint in DuckDB to the HTTP url of your bucket + version of the extension
you want to install. To do this run the following SQL query in DuckDB:
```sql
SET custom_extension_repository='bucket.s3.eu-west-1.amazonaws.com/<your_extension_name>/latest';
```
Note that the `/latest` path will allow you to install the latest extension version available for your current version of
DuckDB. To specify a specific version, you can pass the version instead.

After running these steps, you can install and load your extension using the regular INSTALL/LOAD commands in DuckDB:
```sql
INSTALL lumos_kafka
LOAD lumos_kafka
```
