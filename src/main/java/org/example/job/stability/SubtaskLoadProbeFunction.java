package org.example.job.stability;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.example.dto.StateDemoEvent;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 记录各 subtask 处理条数，日志对照 UI {@code numRecordsInPerSecond} / 倾斜排查。
 */
public class SubtaskLoadProbeFunction extends RichMapFunction<StateDemoEvent, StateDemoEvent> {

    private transient AtomicLong localCount;
    private transient long lastLogMs;

    @Override
    public void open(Configuration parameters) {
        localCount = new AtomicLong();
        lastLogMs = System.currentTimeMillis();
    }

    @Override
    public StateDemoEvent map(StateDemoEvent event) throws Exception {
        long count = localCount.incrementAndGet();
        long now = System.currentTimeMillis();
        if (now - lastLogMs >= 3000 || count <= 3) {
            System.out.printf(
                    "[LOAD-PROBE] subtask=%d records=%d course=%s tag=%s | "
                            + "UI: busyTimeMsPerSecond↑ + numRecordsIn 不均 → 倾斜候选%n",
                    getRuntimeContext().getIndexOfThisSubtask(),
                    count,
                    event.getCourseId(),
                    event.getTag());
            lastLogMs = now;
        }
        return event;
    }
}
