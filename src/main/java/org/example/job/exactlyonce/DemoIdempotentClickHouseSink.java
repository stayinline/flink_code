package org.example.job.exactlyonce;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.example.dto.StudySummaryRecord;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 模拟 ClickHouse 幂等写路线：主键 {@link StudySummaryRecord#getDedupKey()} 覆盖（upsert）。
 * <p>
 * 重复发送同 dedupKey 时保留最新 totalWatchSec，下游最终一致，无需 Kafka 事务。
 */
public class DemoIdempotentClickHouseSink extends RichSinkFunction<StudySummaryRecord> {

    /** 模拟 ClickHouse 表（dedupKey → 最新行） */
    public static final ConcurrentHashMap<String, StudySummaryRecord> IDEMPOTENT_STORE = new ConcurrentHashMap<>();

    @Override
    public void open(Configuration parameters) {
        System.out.println("[CK-UPSERT] 幂等 Sink 就绪：dedupKey = statDate|studentId|courseId");
    }

    @Override
    public void invoke(StudySummaryRecord value, Context context) {
        StudySummaryRecord prev = IDEMPOTENT_STORE.get(value.getDedupKey());
        IDEMPOTENT_STORE.put(value.getDedupKey(), value);
        if (prev != null) {
            System.out.printf("[CK-UPSERT] 覆盖 dedupKey=%s | %ds → %ds tag=%s%n",
                    value.getDedupKey(), prev.getTotalWatchSec(), value.getTotalWatchSec(), value.getTag());
        } else {
            System.out.printf("[CK-UPSERT] 插入 dedupKey=%s | total=%ds tag=%s%n",
                    value.getDedupKey(), value.getTotalWatchSec(), value.getTag());
        }
    }
}
