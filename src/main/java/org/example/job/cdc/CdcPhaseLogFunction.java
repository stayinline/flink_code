package org.example.job.cdc;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.example.dto.CdcChangeEvent;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 区分全量快照（Snapshot）与 binlog 增量阶段，打印可观测日志。
 */
public class CdcPhaseLogFunction extends RichMapFunction<String, CdcChangeEvent> {

    private transient AtomicLong snapshotCount;
    private transient AtomicLong binlogCount;
    private transient boolean seenBinlog;
    private transient long lastLogMs;

    @Override
    public void open(Configuration parameters) {
        snapshotCount = new AtomicLong();
        binlogCount = new AtomicLong();
        seenBinlog = false;
        lastLogMs = System.currentTimeMillis();
    }

    @Override
    public CdcChangeEvent map(String json) {
        CdcChangeEvent event = CdcDebeziumJsonParser.parse(json).orElse(null);
        if (event == null) {
            System.out.println("[CDC-PARSE] 跳过无法解析的事件");
            return null;
        }

        if (event.isSnapshotPhase()) {
            snapshotCount.incrementAndGet();
        } else {
            if (!seenBinlog) {
                seenBinlog = true;
                System.out.println("========== [CDC-PHASE] Snapshot → Binlog 切换 ==========");
                System.out.printf("  快照阶段累计: %d 条 | 开始消费增量 binlog%n", snapshotCount.get());
                System.out.println("======================================================");
            }
            binlogCount.incrementAndGet();
        }

        long now = System.currentTimeMillis();
        long total = snapshotCount.get() + binlogCount.get();
        if (total <= 5 || now - lastLogMs >= 5000) {
            String phaseTag = event.isSnapshotPhase() ? "SNAPSHOT" : "BINLOG";
            System.out.printf(
                    "[CDC-%s] subtask=%d op=%s id=%s student=%s course=%s status=%s | file=%s pos=%s snapshot=%s%n",
                    phaseTag,
                    getRuntimeContext().getIndexOfThisSubtask(),
                    event.getOp(),
                    event.getPrimaryKeyId(),
                    event.getStudentId(),
                    event.getCourseId(),
                    event.getEnrollStatus(),
                    event.getBinlogFile(),
                    event.getBinlogPos(),
                    event.getSnapshotLabel());
            lastLogMs = now;
        }
        return event;
    }
}
