package org.example.job.quality;

import org.example.dto.DirtyDataRecord;
import org.example.dto.StateDemoEvent;
import org.example.dto.StudySummaryRecord;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 对账计数器：源端接受数、DLQ 数、汇总层 sum（单测与 Job 日志共用）。
 */
public final class ReconciliationReporter {

    private static final AtomicLong ACCEPTED_COUNT = new AtomicLong();
    private static final AtomicLong ACCEPTED_WATCH_SUM = new AtomicLong();
    private static final AtomicLong DLQ_COUNT = new AtomicLong();
    private static final Map<String, AtomicLong> DLQ_BY_REASON = new ConcurrentHashMap<>();

    private ReconciliationReporter() {
    }

    public static void reset() {
        ACCEPTED_COUNT.set(0);
        ACCEPTED_WATCH_SUM.set(0);
        DLQ_COUNT.set(0);
        DLQ_BY_REASON.clear();
    }

    public static void onAccepted(StateDemoEvent event) {
        ACCEPTED_COUNT.incrementAndGet();
        if (StateDemoEvent.TYPE_VIDEO_PROGRESS.equals(event.getEventType())
                && event.getWatchSec() != null) {
            ACCEPTED_WATCH_SUM.addAndGet(event.getWatchSec());
        }
    }

    public static void onDlq(DirtyDataRecord record) {
        DLQ_COUNT.incrementAndGet();
        DLQ_BY_REASON.computeIfAbsent(record.getReason(), k -> new AtomicLong())
                .incrementAndGet();
    }

    public static void onSummaryWritten(StudySummaryRecord record) {
        // 汇总层写入由 Idempotent Sink 记录，对账在 report() 中与源端比对
    }

    public static ReconciliationSnapshot snapshot(long sourceSentCount, long sourceSentWatchSum) {
        return new ReconciliationSnapshot(
                sourceSentCount,
                sourceSentWatchSum,
                ACCEPTED_COUNT.get(),
                ACCEPTED_WATCH_SUM.get(),
                DLQ_COUNT.get(),
                DLQ_BY_REASON
        );
    }

    public static void printReport(long sourceSentCount, long sourceSentWatchSum) {
        ReconciliationSnapshot snap = snapshot(sourceSentCount, sourceSentWatchSum);
        System.out.println("========== [RECON] 对账报告 ==========");
        System.out.printf("  源端发送: count=%d watchSum=%d%n", snap.sourceSentCount, snap.sourceSentWatchSum);
        System.out.printf("  校验通过: count=%d watchSum=%d%n", snap.acceptedCount, snap.acceptedWatchSum);
        System.out.printf("  DLQ 脏数据: count=%d%n", snap.dlqCount);
        snap.dlqByReason.forEach((reason, cnt) ->
                System.out.printf("    - %s: %d%n", reason, cnt.get()));
        long expectedAccepted = snap.sourceSentCount - snap.dlqCount;
        boolean countOk = snap.acceptedCount == expectedAccepted;
        System.out.printf("  count 对账: %s (accepted=%d, sent-dlq=%d)%n",
                countOk ? "✅ PASS" : "❌ FAIL", snap.acceptedCount, expectedAccepted);
        System.out.println("  说明: Exactly-Once ≠ 业务对账；汇总层还需 dedupKey UPSERT + 定期 RECON");
        System.out.println("======================================");
    }

    public static class ReconciliationSnapshot {
        public final long sourceSentCount;
        public final long sourceSentWatchSum;
        public final long acceptedCount;
        public final long acceptedWatchSum;
        public final long dlqCount;
        public final Map<String, AtomicLong> dlqByReason;

        public ReconciliationSnapshot(long sourceSentCount, long sourceSentWatchSum,
                                    long acceptedCount, long acceptedWatchSum,
                                    long dlqCount, Map<String, AtomicLong> dlqByReason) {
            this.sourceSentCount = sourceSentCount;
            this.sourceSentWatchSum = sourceSentWatchSum;
            this.acceptedCount = acceptedCount;
            this.acceptedWatchSum = acceptedWatchSum;
            this.dlqCount = dlqCount;
            this.dlqByReason = dlqByReason;
        }
    }
}
