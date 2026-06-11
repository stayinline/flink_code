package org.example.job.kafka;

import java.util.HashMap;
import java.util.Map;

/**
 * 模拟 Flink 恢复时 Offset 优先级：Checkpoint Source 状态 > Kafka committed offset。
 */
public final class KafkaOffsetRestoreSimulator {

    private KafkaOffsetRestoreSimulator() {
    }

    public static OffsetRestoreDecision decide(
            Map<Integer, Long> checkpointOffsets,
            Map<Integer, Long> kafkaCommittedOffsets,
            Map<Integer, Long> savepointOffsets,
            boolean fromSavepoint) {

        Map<Integer, Long> chosen = new HashMap<>();
        String source;

        if (fromSavepoint && savepointOffsets != null && !savepointOffsets.isEmpty()) {
            chosen.putAll(savepointOffsets);
            source = "Savepoint 中 KafkaSource Operator State";
        } else if (checkpointOffsets != null && !checkpointOffsets.isEmpty()) {
            chosen.putAll(checkpointOffsets);
            source = "最近成功 Checkpoint 中 Source 状态";
        } else if (kafkaCommittedOffsets != null && !kafkaCommittedOffsets.isEmpty()) {
            chosen.putAll(kafkaCommittedOffsets);
            source = "Kafka __consumer_offsets（仅无 CK/SP 的冷启动）";
        } else {
            source = "OffsetsInitializer 策略（earliest/latest）";
        }

        return new OffsetRestoreDecision(chosen, source);
    }

    public static class OffsetRestoreDecision {
        public final Map<Integer, Long> offsets;
        public final String source;

        public OffsetRestoreDecision(Map<Integer, Long> offsets, String source) {
            this.offsets = offsets;
            this.source = source;
        }
    }
}
