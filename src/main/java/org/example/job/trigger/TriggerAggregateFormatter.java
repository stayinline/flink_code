package org.example.job.trigger;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.job.window.AmountSumResult;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * 格式化 aggregate 结果，标注 early-fire / final-fire。
 */
public class TriggerAggregateFormatter
        extends ProcessWindowFunction<AmountSumResult, String, String, TimeWindow> {

    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter
            .ofPattern("HH:mm:ss.SSS")
            .withZone(ZoneId.systemDefault());

    private final String branchLabel;

    public TriggerAggregateFormatter(String branchLabel) {
        this.branchLabel = branchLabel;
    }

    @Override
    public void process(String userId,
                        Context context,
                        Iterable<AmountSumResult> elements,
                        Collector<String> out) {
        AmountSumResult result = elements.iterator().next();
        TimeWindow w = context.window();
        long wm = context.currentWatermark();
        String fireType = wm < w.getEnd() ? "EARLY-FIRE" : "FINAL-FIRE";

        out.collect(String.format(
                "[%s][%s] userId=%s | 窗口=[%s ~ %s) | WM=%s | aggregate count=%d sum=%.1f | 增量聚合无全量缓存",
                branchLabel,
                fireType,
                userId,
                TIME_FMT.format(Instant.ofEpochMilli(w.getStart())),
                TIME_FMT.format(Instant.ofEpochMilli(w.getEnd())),
                wm == Long.MIN_VALUE ? "MIN_VALUE" : TIME_FMT.format(Instant.ofEpochMilli(wm)),
                result.count,
                result.sum
        ));
    }
}
