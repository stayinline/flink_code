package org.example.job.window;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * 仅负责格式化 aggregate 结果，不缓存窗口内全量元素。
 */
public class WindowSumResultFormatter extends ProcessWindowFunction<AmountSumResult, String, String, TimeWindow> {

    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter
            .ofPattern("HH:mm:ss.SSS")
            .withZone(ZoneId.systemDefault());

    private final String windowType;
    private final String note;

    public WindowSumResultFormatter(String windowType, String note) {
        this.windowType = windowType;
        this.note = note;
    }

    @Override
    public void process(String userId,
                        Context context,
                        Iterable<AmountSumResult> elements,
                        Collector<String> out) {
        AmountSumResult result = elements.iterator().next();
        TimeWindow window = context.window();
        String range = String.format("[%s ~ %s)",
                TIME_FMT.format(Instant.ofEpochMilli(window.getStart())),
                TIME_FMT.format(Instant.ofEpochMilli(window.getEnd())));

        out.collect(String.format(
                "[%s] userId=%s | 窗口=%s 左闭右开 | count=%d sum=%.1f | %s",
                windowType, userId, range, result.count, result.sum, note
        ));
    }
}
