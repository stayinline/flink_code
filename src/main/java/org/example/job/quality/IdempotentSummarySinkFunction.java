package org.example.job.quality;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.example.dto.StudySummaryRecord;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 幂等结果表：dedupKey + replayVersion 覆盖写（模拟 ClickHouse ReplacingMergeTree / UPSERT）。
 * <p>
 * 补数/回放重复写同一业务主键时，保留更高 replayVersion 或更新 totalWatchSec。
 */
public class IdempotentSummarySinkFunction extends RichSinkFunction<StudySummaryRecord> {

    public static final ConcurrentHashMap<String, VersionedSummary> SUMMARY_STORE = new ConcurrentHashMap<>();

    @Override
    public void open(Configuration parameters) {
        System.out.println("[IDEMPOTENT-SUMMARY] 就绪 | dedupKey=statDate|studentId|courseId | 支持 replayVersion 覆盖");
    }

    @Override
    public void invoke(StudySummaryRecord value, Context context) {
        int version = resolveVersion(value.getTag());
        VersionedSummary incoming = new VersionedSummary(value, version);
        SUMMARY_STORE.merge(value.getDedupKey(), incoming, IdempotentSummarySinkFunction::mergeByVersion);

        VersionedSummary stored = SUMMARY_STORE.get(value.getDedupKey());
        System.out.printf("[IDEMPOTENT-SUMMARY] dedupKey=%s v=%d total=%ds tag=%s%n",
                value.getDedupKey(), stored.version, stored.record.getTotalWatchSec(), value.getTag());
        ReconciliationReporter.onSummaryWritten(value);
    }

    private static VersionedSummary mergeByVersion(VersionedSummary old, VersionedSummary neu) {
        if (neu.version >= old.version) {
            return neu;
        }
        return old;
    }

    static int resolveVersion(String tag) {
        if (tag == null) {
            return 1;
        }
        if (tag.contains("replay-v2")) {
            return 2;
        }
        if (tag.contains("replay-v3") || tag.contains("backfill")) {
            return 3;
        }
        if (tag.contains("replay")) {
            return 2;
        }
        return 1;
    }

    public static void clearStore() {
        SUMMARY_STORE.clear();
    }

    public static class VersionedSummary {
        public final StudySummaryRecord record;
        public final int version;

        public VersionedSummary(StudySummaryRecord record, int version) {
            this.record = record;
            this.version = version;
        }
    }
}
