# Test lumos-kafka Extension With Docker Kafka

```bash
docker-compose up -d
docker logs kafka

# 进入容器：
docker exec -it kafka bash
# 创建测试主题：
kafka-topics --create --topic test-topic --bootstrap-server localhost:9092 --partitions 2 --replication-factor 1

# 启动生产者：
kafka-console-producer --topic test-topic --bootstrap-server localhost:9092

# 另开终端，进入容器启动消费者：
docker exec -it kafka bash
kafka-console-consumer --topic test-topic --bootstrap-server localhost:9092 --from-beginning
```

```sql
-- ./build/release/duckdb
select * from kafka_scan(scan_bounded_mode='latest-offset',topic='test-topic1', scan_startup_mode='earliest',bootstrap_servers='localhost:9092',group_id='test');
┌─────────────┬───────────┬────────┬─────────┐
│    topic    │ partition │ offset │  value  │
│   varchar   │   int32   │ int64  │ varchar │
├─────────────┼───────────┼────────┼─────────┤
│ test-topic1 │         0 │      0 │ test1   │
│ test-topic1 │         0 │      1 │ test2   │
│ test-topic1 │         0 │      2 │ test3   │
└─────────────┴───────────┴────────┴─────────┘
```

## IT Test

需要启用两个扩展 

```
COPY  (select * from kafka_scan(scan_bounded_mode='latest-offset',topic='test-topic', scan_startup_mode='earliest',bootstrap_servers='localhost:9092',group_id='test')) TO 'test_print' (format 'print_test',header 'true');

COPY  (select * from kafka_scan(scan_bounded_mode='unbounded',topic='test-topic', scan_startup_mode='earliest',bootstrap_servers='localhost:9092',group_id='test')) TO 'test_print' (format 'print_test',header 'true');
```