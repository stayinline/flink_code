package org.example.job.late;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.dto.WatermarkDemoEvent;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * 窗口触发日志：区分首次触发（FIRST-FIRE）与 allowedLateness 迟到重算（LATE-UPDATE）。
 * <p>
 * 要点：allowedLateness 触发的是<strong>同一窗口</strong>的结果更新，而非新窗口。
 */
public class LateDataWindowLogFunction
        extends ProcessWindowFunction<WatermarkDemoEvent, String, String, TimeWindow> {

    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter
            .ofPattern("HH:mm:ss.SSS")
            .withZone(ZoneId.systemDefault());

    private transient ValueState<Boolean> alreadyFired;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Boolean> desc = new ValueStateDescriptor<>("alreadyFired", Boolean.class);
        alreadyFired = getRuntimeContext().getState(desc);
    }

    @Override
    public void process(String userId,
                        Context context,
                        Iterable<WatermarkDemoEvent> elements,
                        Collector<String> out) throws Exception {
        double sum = 0;
        long count = 0;
        StringBuilder detail = new StringBuilder();
        for (WatermarkDemoEvent e : elements) {
            sum += e.getAmount();
            count++;
            if (detail.length() > 0) {
                detail.append(", ");
            }
            detail.append(String.format("%s(ts=%s,tag=%s)",
                    e.getEventId(),
                    TIME_FMT.format(Instant.ofEpochMilli(e.getTs())),
                    e.getTag()));
        }

        TimeWindow w = context.window();
        long currentWm = context.currentWatermark();
        boolean isUpdate = Boolean.TRUE.equals(alreadyFired.value());
        alreadyFired.update(true);

        String triggerType = isUpdate ? "LATE-UPDATE" : "FIRST-FIRE";
        String layerHint = isUpdate
                ? "② allowedLateness 内迟到 → 同窗口增量重算（非新窗口）"
                : "WM≥window.end 首次关闭窗口";

        out.collect(String.format(
                "[%s] subtask=%d userId=%s | 窗口=[%s ~ %s) | count=%d sum=%.1f | WM=%s | 明细=[%s] | %s",
                triggerType,
                getRuntimeContext().getIndexOfThisSubtask(),
                userId,
                TIME_FMT.format(Instant.ofEpochMilli(w.getStart())),
                TIME_FMT.format(Instant.ofEpochMilli(w.getEnd())),
                count,
                sum,
                formatWm(currentWm),
                detail,
                layerHint
        ));
    }

    @Override
    public void clear(Context context) throws Exception {
        alreadyFired.clear();
    }

    private static String formatWm(long wm) {
        return wm == Long.MIN_VALUE ? "MIN_VALUE" : TIME_FMT.format(Instant.ofEpochMilli(wm));
    }
}
