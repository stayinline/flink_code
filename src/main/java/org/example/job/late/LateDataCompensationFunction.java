package org.example.job.late;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.example.dto.WatermarkDemoEvent;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * 侧输出补偿落库模拟：打印「写入补录表」日志，演示端到端不丢数叙事。
 * <p>
 * 生产可替换为 ClickHouse ReplacingMergeTree / JDBC Sink，幂等键 = (userId, windowStart, eventId)。
 */
public class LateDataCompensationFunction extends ProcessFunction<WatermarkDemoEvent, String> {

    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter
            .ofPattern("HH:mm:ss.SSS")
            .withZone(ZoneId.systemDefault());

    @Override
    public void processElement(WatermarkDemoEvent event,
                               Context ctx,
                               Collector<String> out) {
        long wm = ctx.timerService().currentWatermark();
        String record = String.format(
                "[COMPENSATE-DB] ③ sideOutput 严重迟到 → 补录表 | eventId=%s userId=%s ts=%s amount=%.1f tag=%s | currentWM=%s | SQL=INSERT INTO late_compensation(user_id,event_id,ts,amount) VALUES(...)",
                event.getEventId(),
                event.getUserId(),
                TIME_FMT.format(Instant.ofEpochMilli(event.getTs())),
                event.getAmount(),
                event.getTag(),
                wm == Long.MIN_VALUE ? "MIN_VALUE" : TIME_FMT.format(Instant.ofEpochMilli(wm))
        );
        System.out.println(record);
        out.collect(record);
    }
}
