package org.example.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.example.dto.KafkaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Properties;

// org.example.job.JobV2

public class JobV2 {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger log = LoggerFactory.getLogger(JobV2.class);



    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);

        // Kafka消费者配置
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "192.168.250.42:9092");
        kafkaProps.setProperty("group.id", "flink-clickhouse-consumer");
        kafkaProps.setProperty("auto.offset.reset", "earliest");

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "test_ck",
                new SimpleStringSchema(),
                kafkaProps
        );

        DataStream<String> kafkaDataStream = env.addSource(kafkaConsumer)
                .name("Kafka Source");

        // 解析JSON数据
        DataStream<KafkaData> transformedStream = kafkaDataStream
                .map(new KafkaDataMapper())
                .filter(data -> !"error".equals(data.getId()));

        // ClickHouse Sink配置
        String clickhouseUrl = "jdbc:clickhouse://192.168.1.124:8123/default";
        String insertSql = "INSERT INTO testdb.test_events (id, message, value, event_time) VALUES (?, ?, ?, ?)";

        JdbcConnectionOptions jdbcOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(clickhouseUrl)
                .withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
                .withUsername("default")
                .withPassword("65e84be3")
                .build();

        JdbcExecutionOptions executionOptions = JdbcExecutionOptions.builder()
                .withBatchSize(1000)
                .withBatchIntervalMs(1000)
                .withMaxRetries(3) // 添加重试机制
                .build();

        // 使用独立的StatementBuilder类替代匿名内部类
        SinkFunction<KafkaData> sink = JdbcSink.sink(
                insertSql,
                new KafkaDataStatementBuilder(),
                executionOptions,
                jdbcOptions
        );

        transformedStream.addSink(sink).name("ClickHouse Sink");
        transformedStream.map(data -> "处理数据: " + data.toString()).print();

        env.execute("Kafka to ClickHouse Data Sync");
    }

    // 独立的StatementBuilder类，避免匿名内部类带来的序列化问题
    private static class KafkaDataStatementBuilder implements JdbcStatementBuilder<KafkaData> {
        @Override
        public void accept(PreparedStatement ps, KafkaData data) throws SQLException {
            ps.setString(1, data.getId());
            ps.setString(2, data.getMessage());
            ps.setDouble(3, data.getValue());
            ps.setString(4, data.getEventTime());
        }
    }

    // 自定义MapFunction实现
    private static class KafkaDataMapper implements MapFunction<String, KafkaData> {

        @Override
        public KafkaData map(String value) throws Exception {
            try {
                KafkaData kafkaData = objectMapper.readValue(value, KafkaData.class);
                System.out.println("解析到kafka数据={}" + kafkaData);
                return kafkaData;
            } catch (Exception e) {
                log.error("JSON解析失败: " + value + ", 错误: " + e.getMessage(), e);
                //System.out.println("JSON解析失败: " + value + ", 错误: " + e);
                return new KafkaData("error", value, 1.2D, LocalDateTime.now().toString());
            }
        }
    }
}
