package org.example.job.runtime;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 模拟慢外部 Sink（JDBC / ClickHouse / HTTP）。
 * <p>
 * 下游慢 → OutputGate/Network Buffer 积压 → 上游 InputGate 反压 → busy/backpressured 指标变化。
 * 盲目提高 Source 并行度无法突破此瓶颈。
 */
public class SlowRuntimeSinkFunction extends RichSinkFunction<String> {

    private final long sleepMs;
    private final String slotSharingGroup;

    private transient AtomicLong sinkCount;
    private transient long lastLogMs;

    public SlowRuntimeSinkFunction(long sleepMs, String slotSharingGroup) {
        this.sleepMs = sleepMs;
        this.slotSharingGroup = slotSharingGroup;
    }

    @Override
    public void open(Configuration parameters) {
        sinkCount = new AtomicLong();
        lastLogMs = System.currentTimeMillis();
        System.out.printf(
                "[RT-SINK] subtask=%d/%d sleep=%dms slotGroup=%s | "
                        + "瓶颈在此：加 Source 并行度无效，应优化 Sink 或扩 Sink 并行度%n",
                getRuntimeContext().getIndexOfThisSubtask(),
                getRuntimeContext().getNumberOfParallelSubtasks(),
                sleepMs,
                slotSharingGroup);
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        if (sleepMs > 0) {
            Thread.sleep(sleepMs);
        }
        long count = sinkCount.incrementAndGet();
        long now = System.currentTimeMillis();
        if (count <= 2 || now - lastLogMs >= 5000) {
            System.out.printf("[RT-SINK-OUT] subtask=%d processed=%d | %s%n",
                    getRuntimeContext().getIndexOfThisSubtask(), count,
                    value.length() > 120 ? value.substring(0, 120) + "..." : value);
            lastLogMs = now;
        }
    }
}
