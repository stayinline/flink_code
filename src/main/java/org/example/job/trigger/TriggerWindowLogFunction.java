package org.example.job.trigger;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.dto.WatermarkDemoEvent;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * 窗口触发日志：区分 early-fire（WM 未过窗口 end）与 final-fire（WM ≥ window.end）。
 */
public class TriggerWindowLogFunction
        extends ProcessWindowFunction<WatermarkDemoEvent, String, String, TimeWindow> {

    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter
            .ofPattern("HH:mm:ss.SSS")
            .withZone(ZoneId.systemDefault());

    private final String branchLabel;

    public TriggerWindowLogFunction(String branchLabel) {
        this.branchLabel = branchLabel;
    }

    @Override
    public void process(String userId,
                        Context context,
                        Iterable<WatermarkDemoEvent> elements,
                        Collector<String> out) {
        double sum = 0;
        long count = 0;
        long minTs = Long.MAX_VALUE;
        long maxTs = Long.MIN_VALUE;
        String firstId = null;
        String lastId = null;

        for (WatermarkDemoEvent e : elements) {
            sum += e.getAmount();
            count++;
            if (e.getTs() < minTs) {
                minTs = e.getTs();
            }
            if (e.getTs() > maxTs) {
                maxTs = e.getTs();
            }
            if (firstId == null) {
                firstId = e.getEventId();
            }
            lastId = e.getEventId();
        }

        TimeWindow w = context.window();
        long wm = context.currentWatermark();
        String fireType = wm < w.getEnd() ? "EARLY-FIRE" : "FINAL-FIRE";

        out.collect(String.format(
                "[%s][%s] subtask=%d userId=%s | 窗口=[%s ~ %s) | WM=%s | count=%d sum=%.1f | "
                        + "tsRange=[%s ~ %s] | first=%s last=%s",
                branchLabel,
                fireType,
                getRuntimeContext().getIndexOfThisSubtask(),
                userId,
                TIME_FMT.format(Instant.ofEpochMilli(w.getStart())),
                TIME_FMT.format(Instant.ofEpochMilli(w.getEnd())),
                wm == Long.MIN_VALUE ? "MIN_VALUE" : TIME_FMT.format(Instant.ofEpochMilli(wm)),
                count,
                sum,
                count > 0 ? TIME_FMT.format(Instant.ofEpochMilli(minTs)) : "-",
                count > 0 ? TIME_FMT.format(Instant.ofEpochMilli(maxTs)) : "-",
                firstId == null ? "-" : firstId,
                lastId == null ? "-" : lastId
        ));
    }
}
