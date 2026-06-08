package org.example.job.timer;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.example.dto.OrderPaymentEvent;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * 打印每条订单事件到达时的 currentWatermark，用于观察事件时间定时器与 WM 推进关系。
 */
public class OrderEventMonitorFunction extends ProcessFunction<OrderPaymentEvent, OrderPaymentEvent> {

    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter
            .ofPattern("HH:mm:ss.SSS")
            .withZone(ZoneId.systemDefault());

    @Override
    public void processElement(OrderPaymentEvent event,
                               Context ctx,
                               Collector<OrderPaymentEvent> out) {
        long wm = ctx.timerService().currentWatermark();
        String wmStr = wm == Long.MIN_VALUE ? "MIN_VALUE(未初始化)" : TIME_FMT.format(Instant.ofEpochMilli(wm));

        System.out.printf(
                "[WM-MONITOR] subtask=%d | orderId=%s eventType=%-13s tag=%-10s | eventTs=%s | currentWM=%s%n",
                getRuntimeContext().getIndexOfThisSubtask(),
                event.getOrderId(),
                event.getEventType(),
                event.getTag(),
                TIME_FMT.format(Instant.ofEpochMilli(event.getTs())),
                wmStr);
        out.collect(event);
    }
}
