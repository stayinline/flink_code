package org.example.job.kafka;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;

/**
 * 反序列化时保留 partition/offset 元数据（Flink 1.14 {@link org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer}）。
 * <p>
 * Flink 1.15+ {@code KafkaSource} 等价：{@code KafkaRecordDeserializationSchema} 读取 {@code ConsumerRecord} 元数据。
 */
public class StudyKafkaDeserializationSchema implements KafkaDeserializationSchema<KafkaEnrichedRecord> {

    @Override
    public boolean isEndOfStream(KafkaEnrichedRecord nextElement) {
        return false;
    }

    @Override
    public KafkaEnrichedRecord deserialize(ConsumerRecord<byte[], byte[]> record) {
        String json = record.value() == null ? null
                : new String(record.value(), StandardCharsets.UTF_8);
        return new KafkaEnrichedRecord(
                json,
                record.topic(),
                record.partition(),
                record.offset(),
                record.timestamp());
    }

    @Override
    public TypeInformation<KafkaEnrichedRecord> getProducedType() {
        return TypeInformation.of(KafkaEnrichedRecord.class);
    }
}
