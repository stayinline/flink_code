package org.example.job.kafka;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Kafka 消费元数据 + 原始 JSON，用于观察分区、Offset 与 Subtask 分配。
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class KafkaEnrichedRecord implements Serializable {

    private String json;
    private String topic;
    private int partition;
    private long offset;
    private long kafkaTimestamp;
}
