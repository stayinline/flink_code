package org.example.job.kafka;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 构建 Kafka Source（Flink 1.14：{@link FlinkKafkaConsumer}）。
 * <p>
 * Flink 1.15+ {@code KafkaSource} 等价写法见类注释与 {@code FlinkKafkaConnectorDemoGuide.md}。
 */
public final class StudyKafkaSourceFactory {

    private StudyKafkaSourceFactory() {
    }

    /**
     * <pre>
     * // Flink 1.15+ KafkaSource 等价：
     * KafkaSource.&lt;String&gt;builder()
     *     .setBootstrapServers(broker)
     *     .setTopics(TOPIC_IN)
     *     .setGroupId(groupId)
     *     .setStartingOffsets(OffsetsInitializer.latest())  // earliest / committedOffsets()
     *     .setValueOnlyDeserializer(new SimpleStringSchema())
     *     .build();
     * env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "KafkaSource");
     * </pre>
     */
    public static FlinkKafkaConsumer<KafkaEnrichedRecord> build(
            String broker,
            String topic,
            String groupId,
            KafkaConnectorConfigurator.KafkaConnectorOptions options) {

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, mapAutoOffsetReset(options.offsetMode));
        // 动态分区发现（扩分区后下次 fetch metadata 可感知新分区）
        props.setProperty(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "30000");
        props.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                "org.apache.kafka.clients.consumer.RangeAssignor");

        FlinkKafkaConsumer<KafkaEnrichedRecord> consumer = new FlinkKafkaConsumer<>(
                Collections.singletonList(topic),
                new StudyKafkaDeserializationSchema(),
                props
        );

        applyOffsetInitializer(consumer, topic, options.offsetMode);
        return consumer;
    }

    private static void applyOffsetInitializer(
            FlinkKafkaConsumer<KafkaEnrichedRecord> consumer,
            String topic,
            String offsetMode) {
        switch (offsetMode) {
            case KafkaConnectorConfigurator.OFFSET_EARLIEST:
                consumer.setStartFromEarliest();
                break;
            case KafkaConnectorConfigurator.OFFSET_COMMITTED:
                consumer.setStartFromGroupOffsets();
                break;
            case KafkaConnectorConfigurator.OFFSET_LATEST:
            default:
                consumer.setStartFromLatest();
                break;
        }
        // 演示用：特定 offset 可从外部传入（Savepoint/测试重置）
        if (Boolean.parseBoolean(System.getProperty("kafka.offset.specific", "false"))) {
            Map<KafkaTopicPartition, Long> specific = new HashMap<>();
            specific.put(new KafkaTopicPartition(topic, 0), 0L);
            consumer.setStartFromSpecificOffsets(specific);
        }
    }

    private static String mapAutoOffsetReset(String offsetMode) {
        if (KafkaConnectorConfigurator.OFFSET_EARLIEST.equals(offsetMode)) {
            return "earliest";
        }
        if (KafkaConnectorConfigurator.OFFSET_COMMITTED.equals(offsetMode)) {
            return "earliest";
        }
        return "latest";
    }
}
