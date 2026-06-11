package org.example.job.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * 构建 Kafka Sink（Flink 1.14：{@link FlinkKafkaProducer}）。
 * <p>
 * Flink 1.15+ {@code KafkaSink} 等价写法见指南。
 */
public final class StudyKafkaSinkFactory {

    private StudyKafkaSinkFactory() {
    }

    /**
     * <pre>
     * // Flink 1.15+ KafkaSink 等价：
     * KafkaSink.&lt;String&gt;builder()
     *     .setBootstrapServers(broker)
     *     .setRecordSerializer(KafkaRecordSerializationSchema.builder()
     *         .setTopic(TOPIC_OUT)
     *         .setValueSerializationSchema(new SimpleStringSchema())
     *         .build())
     *     .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
     *     .setTransactionalIdPrefix("flink-kafka-demo-txn-")
     *     .build();
     * stream.sinkTo(kafkaSink);
     * </pre>
     */
    public static FlinkKafkaProducer<String> build(
            String broker,
            String topic,
            KafkaConnectorConfigurator.KafkaConnectorOptions options) {

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        props.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");

        FlinkKafkaProducer.Semantic semantic = resolveSemantic(options.sinkSemantic);
        if (semantic == FlinkKafkaProducer.Semantic.EXACTLY_ONCE) {
            props.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "900000");
            props.setProperty("transaction.timeout.ms", "900000");
        }

        KeyedSerializationSchema<String> schema = new KeyedSerializationSchema<String>() {
            @Override
            public byte[] serializeKey(String element) {
                // 按 studentId 路由分区（JSON 内字段；生产建议用 Avro + 明确 key）
                String key = extractStudentId(element);
                return key == null ? null : key.getBytes(StandardCharsets.UTF_8);
            }

            @Override
            public byte[] serializeValue(String element) {
                return element.getBytes(StandardCharsets.UTF_8);
            }

            @Override
            public String getTargetTopic(String element) {
                return topic;
            }
        };

        return new FlinkKafkaProducer<>(
                topic,
                schema,
                props,
                semantic
        );
    }

    private static FlinkKafkaProducer.Semantic resolveSemantic(String sinkSemantic) {
        if (KafkaConnectorConfigurator.SINK_EXACTLY_ONCE.equalsIgnoreCase(sinkSemantic)) {
            return FlinkKafkaProducer.Semantic.EXACTLY_ONCE;
        }
        return FlinkKafkaProducer.Semantic.AT_LEAST_ONCE;
    }

    private static String extractStudentId(String json) {
        if (json == null) {
            return null;
        }
        int idx = json.indexOf("\"studentId\"");
        if (idx < 0) {
            return null;
        }
        int colon = json.indexOf(':', idx);
        int q1 = json.indexOf('"', colon + 1);
        int q2 = json.indexOf('"', q1 + 1);
        if (q1 < 0 || q2 < 0) {
            return null;
        }
        return json.substring(q1 + 1, q2);
    }
}
