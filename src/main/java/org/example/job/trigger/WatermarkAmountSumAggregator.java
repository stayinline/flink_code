package org.example.job.trigger;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.example.dto.WatermarkDemoEvent;
import org.example.job.window.AmountSumAccumulator;
import org.example.job.window.AmountSumResult;

/**
 * 对 {@link WatermarkDemoEvent} 做增量 sum/count，演示自定义 Trigger 与 aggregate 可配合使用
 * （无 Evictor 时不会缓存全量元素）。
 */
public class WatermarkAmountSumAggregator
        implements AggregateFunction<WatermarkDemoEvent, AmountSumAccumulator, AmountSumResult> {

    @Override
    public AmountSumAccumulator createAccumulator() {
        return new AmountSumAccumulator();
    }

    @Override
    public AmountSumAccumulator add(WatermarkDemoEvent event, AmountSumAccumulator acc) {
        acc.add(event.getAmount());
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
