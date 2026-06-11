package org.example.job.runtime;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.job.window.AmountSumResult;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * 窗口触发日志：标注 Subtask、Shuffle 后 key 分布；keyBy 后数据经 Network Buffer（ResultPartition → InputGate）交换。
 */
public class RuntimeWindowLogFunction
        extends ProcessWindowFunction<AmountSumResult, String, String, TimeWindow> {

    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter
            .ofPattern("HH:mm:ss.SSS")
            .withZone(ZoneId.systemDefault());

    @Override
    public void open(Configuration parameters) {
        System.out.printf("[RT-WINDOW] subtask=%d/%d 就绪 | keyBy 后走 Network Shuffle%n",
                getRuntimeContext().getIndexOfThisSubtask(),
                getRuntimeContext().getNumberOfParallelSubtasks());
    }

    @Override
    public void process(String key,
                        Context context,
                        Iterable<AmountSumResult> elements,
                        Collector<String> out) {
        AmountSumResult result = elements.iterator().next();
        TimeWindow w = context.window();

        out.collect(String.format(
                "[RT-WINDOW-FIRED] subtask=%d key=%s | 窗口=[%s ~ %s) | count=%d sum=%.0fs | "
                        + "热点 key 无法靠 rebalance 消除，只能改 key 或两阶段聚合",
                getRuntimeContext().getIndexOfThisSubtask(),
                key,
                TIME_FMT.format(Instant.ofEpochMilli(w.getStart())),
                TIME_FMT.format(Instant.ofEpochMilli(w.getEnd())),
                result.count,
                result.sum));
    }
}
