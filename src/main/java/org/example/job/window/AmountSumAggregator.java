package org.example.job.window;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.example.dto.UserOrderEvent;

/**
 * 增量聚合 sum(amount)，对应 Step4 陷阱③：
 * 数据量大时用 aggregate/reduce，而非 process 全量迭代 Iterable。
 */
public class AmountSumAggregator implements AggregateFunction<UserOrderEvent, AmountSumAccumulator, AmountSumResult> {

    @Override
    public AmountSumAccumulator createAccumulator() {
        return new AmountSumAccumulator();
    }

    @Override
    public AmountSumAccumulator add(UserOrderEvent event, AmountSumAccumulator acc) {
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
