//package org.example.job;
//
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.sink.SinkFunction;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
//import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
//import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
//import org.apache.flink.connector.jdbc.JdbcSink;
//import com.clickhouse.jdbc.ClickHouseDriver;
//import org.example.dto.KafkaData;
//
//import java.util.Properties;
//import java.sql.PreparedStatement;
//import java.sql.Timestamp;
//import java.time.LocalDateTime;
//
//public class KafkaToClickHouseJob {
//
//
//    public static void main(String[] args) throws Exception {
//        // 设置执行环境
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(5000); // 每5秒一次checkpoint
//
//        // Kafka配置
//        Properties kafkaProps = new Properties();
//        kafkaProps.setProperty("bootstrap.servers", "192.168.1.124:9092");
//        kafkaProps.setProperty("group.id", "flink-clickhouse-consumer");
//
//        // 创建Kafka数据源
//        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
//                "test_ck",
//                new SimpleStringSchema(),
//                kafkaProps
//        );
//        kafkaConsumer.setStartFromLatest(); // 从最新位置开始消费
//
//        // 添加数据源 - 移除了不必要的类型转换
//        DataStream<String> kafkaDataStream = env.addSource(kafkaConsumer);
//
//        // 数据转换：将JSON字符串转换为KafkaData对象
//        DataStream<KafkaData> transformedStream = kafkaDataStream
//                .map(new MapFunction<String, KafkaData>() {
//                    @Override
//                    public KafkaData map(String value) throws Exception {
//                        try {
//                            // 假设JSON格式：{"id":"123","message":"test","value":45.6,"eventTime":"2025-09-26 10:30:00"}
//                            // 这里简化处理，实际应用中可使用Jackson/Gson解析完整JSON
//                            String[] parts = value.replace("{", "").replace("}", "").split(",");
//
//                            String id = extractValue(parts[0]);
//                            String message = extractValue(parts[1]);
//                            String numValue = extractValue(parts[2]);
//                            String eventTime = LocalDateTime.now().toString();
//
//                            if (parts.length > 3) {
//                                try {
//                                    String timeStr = extractValue(parts[3]).replace("\"", "");
//                                    eventTime = timeStr;
//                                } catch (Exception e) {
//                                }
//                            }
//
//                            return new KafkaData(id, message, numValue, eventTime);
//                        } catch (Exception e) {
//                            // 数据格式错误时返回默认值
//                            System.err.println("数据格式错误: " + value + ", 错误: " + e.getMessage());
//                            return new KafkaData("error", value, 0.0, Timestamp.valueOf(LocalDateTime.now()));
//                        }
//                    }
//
//                    private String extractValue(String part) {
//                        return part.split(":")[1].replace("\"", "").trim();
//                    }
//                })
//                .filter(data -> !"error".equals(data.getId())); // 过滤掉错误数据
//
//        // 创建ClickHouse Sink
//        String clickhouseUrl = "jdbc:clickhouse://192.168.1.124:8123/default";
//        String insertSql = "INSERT INTO kafka_data (id, message, value, event_time) VALUES (?, ?, ?, ?)";
//
//        // 配置JDBC连接参数
//        JdbcConnectionOptions jdbcOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
//                .withUrl(clickhouseUrl)
//                .withDriverName(ClickHouseDriver.class.getName())
//                .withUsername("default") // 默认用户名
//                .withPassword("") // 默认空密码
//                .withConnectionCheckTimeoutSeconds(60)
//                .build();
//
//        // 配置执行选项（批量插入优化）
//        JdbcExecutionOptions executionOptions = JdbcExecutionOptions.builder()
//                .withBatchSize(1000) // 批量大小
//                .withBatchIntervalMs(1000) // 批量间隔
//                .withMaxRetries(3) // 重试次数
//                .build();
//
//        // 添加ClickHouse Sink - 移除了不必要的类型转换
//        SinkFunction<KafkaData> sink = JdbcSink.sink(
//                insertSql,
//                (PreparedStatement ps, KafkaData data) -> {
//                    ps.setString(1, data.getId());
//                    ps.setString(2, data.getMessage());
//                    ps.setDouble(3, data.getValue());
//                    ps.setTimestamp(4, data.getEventTime());
//                },
//                executionOptions,
//                jdbcOptions
//        );
//        transformedStream.addSink(sink).name("ClickHouse Sink");
//
//        // 打印数据流用于调试
//        transformedStream.map(data -> "处理数据: " + data.toString()).print();
//
//        // 执行作业
//        env.execute("Kafka to ClickHouse Data Sync");
//    }
//}
