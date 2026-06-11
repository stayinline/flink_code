package org.example.job.runtime;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.example.dto.StateDemoEvent;
import org.example.job.window.AmountSumAccumulator;
import org.example.job.window.AmountSumResult;

/**
 * 窗口增量聚合有效观看秒数；Managed Memory 在 Window 排序/状态场景下由 Flink 统一预算。
 */
public class StudyWatchWindowAggregator
        implements AggregateFunction<StateDemoEvent, AmountSumAccumulator, AmountSumResult> {

    @Override
    public AmountSumAccumulator createAccumulator() {
        return new AmountSumAccumulator();
    }

    @Override
    public AmountSumAccumulator add(StateDemoEvent event, AmountSumAccumulator acc) {
        if (StateDemoEvent.TYPE_VIDEO_PROGRESS.equals(event.getEventType())) {
            int watch = event.getWatchSec() != null ? event.getWatchSec() : 0;
            acc.add(watch);
        }
        return acc;
    }

    @Override
    public AmountSumResult getResult(AmountSumAccumulator acc) {
        return new AmountSumResult(acc.sum, acc.count);
    }

    @Override
    public AmountSumAccumulator merge(AmountSumAccumulator a, AmountSumAccumulator b) {
        a.merge(b);
        return a;
    }
}
