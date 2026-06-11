package org.example.job.quality;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.dto.DirtyDataRecord;
import org.example.dto.StateDemoEvent;

/**
 * Kafka 原始 JSON → 校验 → 主流 / 脏数据侧输出。
 */
public class DataQualityIngressFunction extends ProcessFunction<String, StateDemoEvent> {

    public static final OutputTag<DirtyDataRecord> DIRTY_DATA_TAG =
            new OutputTag<DirtyDataRecord>("dirty-data") {};

    private final boolean dlqEnabled;

    public DataQualityIngressFunction(boolean dlqEnabled) {
        this.dlqEnabled = dlqEnabled;
    }

    private transient ObjectMapper objectMapper;

    @Override
    public void open(Configuration parameters) {
        objectMapper = new ObjectMapper();
    }

    @Override
    public void processElement(String raw, Context ctx, Collector<StateDemoEvent> out) throws Exception {
        StateDemoEvent event;
        try {
            event = objectMapper.readValue(raw, StateDemoEvent.class);
        } catch (Exception e) {
            emitDirty(ctx, DirtyDataRecord.of(
                    raw, DirtyDataRecord.REASON_PARSE_FAIL, e.getMessage(), "parse-error"));
            return;
        }

        var dirty = DataQualityValidator.validate(event, raw);
        if (dirty.isPresent()) {
            emitDirty(ctx, dirty.get());
            return;
        }

        ReconciliationReporter.onAccepted(event);
        System.out.printf("[DQ-OK] eventId=%s student=%s course=%s tag=%s%n",
                event.getEventId(), event.getStudentId(), event.getCourseId(), event.getTag());
        out.collect(event);
    }

    private void emitDirty(Context ctx, DirtyDataRecord dlq) {
        if (dlqEnabled) {
            ctx.output(DIRTY_DATA_TAG, dlq);
            ReconciliationReporter.onDlq(dlq);
            System.out.printf("[DQ-DIRTY] reason=%s eventId=%s | %s%n",
                    dlq.getReason(), dlq.getEventId(), dlq.getDetail());
        } else {
            System.out.printf("[DQ-DROP] 静默丢弃 reason=%s | %s（无 DLQ，不可追溯）%n",
                    dlq.getReason(), dlq.getDetail());
        }
    }
}
