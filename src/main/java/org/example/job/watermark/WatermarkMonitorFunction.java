package org.example.job.watermark;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.example.dto.WatermarkDemoEvent;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * 打印每条事件到达时的 currentWatermark，用于观察 WM 推进与乱序。
 */
public class WatermarkMonitorFunction extends ProcessFunction<WatermarkDemoEvent, WatermarkDemoEvent> {

    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter
            .ofPattern("HH:mm:ss.SSS")
            .withZone(ZoneId.systemDefault());

    @Override
    public void processElement(WatermarkDemoEvent event,
                               Context ctx,
                               Collector<WatermarkDemoEvent> out) {
        long wm = ctx.timerService().currentWatermark();
        String wmStr = wm == Long.MIN_VALUE ? "MIN_VALUE(未初始化)" : TIME_FMT.format(Instant.ofEpochMilli(wm));
        int subtask = getRuntimeContext().getIndexOfThisSubtask();

        System.out.printf("[WM-MONITOR] subtask=%d | eventId=%s userId=%s source=%s tag=%s | eventTs=%s | currentWM=%s%n",
                subtask,
                event.getEventId(),
                event.getUserId(),
                event.getSource(),
                event.getTag(),
                TIME_FMT.format(Instant.ofEpochMilli(event.getTs())),
                wmStr);
        out.collect(event);
    }
}
