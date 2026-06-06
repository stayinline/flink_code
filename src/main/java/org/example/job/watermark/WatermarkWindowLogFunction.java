package org.example.job.watermark;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.dto.WatermarkDemoEvent;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * 窗口触发时打印结果，用于验证 WM 推进后窗口是否正常关闭。
 */
public class WatermarkWindowLogFunction
        extends ProcessWindowFunction<WatermarkDemoEvent, String, String, TimeWindow> {

    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter
            .ofPattern("HH:mm:ss.SSS")
            .withZone(ZoneId.systemDefault());

    @Override
    public void process(String userId,
                        Context context,
                        Iterable<WatermarkDemoEvent> elements,
                        Collector<String> out) {
        double sum = 0;
        long count = 0;
        StringBuilder detail = new StringBuilder();
        for (WatermarkDemoEvent e : elements) {
            sum += e.getAmount();
            count++;
            if (detail.length() > 0) {
                detail.append(", ");
            }
            detail.append(String.format("%s@%s", e.getEventId(),
                    TIME_FMT.format(Instant.ofEpochMilli(e.getTs()))));
        }
        TimeWindow w = context.window();
        out.collect(String.format(
                "[WINDOW-FIRED] subtask=%d userId=%s | 窗口=[%s ~ %s) | count=%d sum=%.1f | 明细=[%s] | triggerWM>=%s",
                getRuntimeContext().getIndexOfThisSubtask(),
                userId,
                TIME_FMT.format(Instant.ofEpochMilli(w.getStart())),
                TIME_FMT.format(Instant.ofEpochMilli(w.getEnd())),
                count,
                sum,
                detail,
                TIME_FMT.format(Instant.ofEpochMilli(w.getEnd()))
        ));
    }
}
