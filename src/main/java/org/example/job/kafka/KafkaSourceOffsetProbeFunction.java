package org.example.job.kafka;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.example.dto.StateDemoEvent;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 打印 Kafka partition/offset 与 Flink subtask 对应关系。
 * Offset 真实恢复来源：Checkpoint 中 {@link FlinkKafkaConsumer} 的 Operator State，而非仅 Kafka committed offset。
 */
public class KafkaSourceOffsetProbeFunction extends RichMapFunction<KafkaEnrichedRecord, StateDemoEvent> {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private transient AtomicLong count;
    private transient long lastLogMs;

    @Override
    public void open(Configuration parameters) {
        count = new AtomicLong();
        lastLogMs = System.currentTimeMillis();
    }

    @Override
    public StateDemoEvent map(KafkaEnrichedRecord record) throws Exception {
        long n = count.incrementAndGet();
        long now = System.currentTimeMillis();

        StateDemoEvent event = null;
        if (record.getJson() != null) {
            event = objectMapper.readValue(record.getJson(), StateDemoEvent.class);
        }

        if (n <= 3 || now - lastLogMs >= 4000) {
            System.out.printf(
                    "[KAFKA-SRC] subtask=%d/%d partition=%d offset=%d kafkaTs=%d | "
                            + "CK 恢复时以 Checkpoint Source 状态为准，非 Kafka committed offset%n",
                    getRuntimeContext().getIndexOfThisSubtask(),
                    getRuntimeContext().getNumberOfParallelSubtasks(),
                    record.getPartition(),
                    record.getOffset(),
                    record.getKafkaTimestamp());
            lastLogMs = now;
        }
        return event;
    }
}
