package org.example.job.checkpoint;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.example.dto.StateDemoEvent;

/**
 * 慢支路模拟：每条记录 sleep，制造反压与 barrier 对齐等待（Aligned CK 场景）。
 */
public class SlowBranchMapFunction extends RichMapFunction<StateDemoEvent, StateDemoEvent> {

    private final long sleepMs;

    public SlowBranchMapFunction(long sleepMs) {
        this.sleepMs = sleepMs;
    }

    @Override
    public StateDemoEvent map(StateDemoEvent event) throws Exception {
        if (sleepMs > 0) {
            Thread.sleep(sleepMs);
        }
        return event;
    }
}
