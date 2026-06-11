package org.example.job.runtime;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.example.dto.StateDemoEvent;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 打印 Subtask 索引、并行度、TaskName，对照 UI Metrics（busy / backpressured / idle）。
 */
public class RuntimeSubtaskProbeFunction extends RichMapFunction<StateDemoEvent, StateDemoEvent> {

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
        if (count <= 2 || now - lastLogMs >= 4000) {
            System.out.printf(
                    "[RT-PROBE] subtask=%d/%d task=%s records=%d student=%s course=%s tag=%s | "
                            + "JM 调度 Subtask→Slot；反压时 busy↓ backpressured↑%n",
                    getRuntimeContext().getIndexOfThisSubtask(),
                    getRuntimeContext().getNumberOfParallelSubtasks(),
                    getRuntimeContext().getTaskName(),
                    count,
                    event.getStudentId(),
                    event.getCourseId(),
                    event.getTag());
            lastLogMs = now;
        }
        return event;
    }
}
