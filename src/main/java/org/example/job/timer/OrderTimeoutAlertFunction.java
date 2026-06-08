package org.example.job.timer;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.dto.OrderPaymentEvent;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * 订单超时未支付告警：下单注册事件时间定时器，支付则删定时器，否则 {@code onTimer} 告警。
 * <p>
 * 生产超时 15 分钟；本 Demo 使用 {@link FlinkOrderTimeoutDemoJob#TIMEOUT_MS}（15 秒）便于观察。
 */
public class OrderTimeoutAlertFunction extends KeyedProcessFunction<String, OrderPaymentEvent, String> {

    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter
            .ofPattern("HH:mm:ss.SSS")
            .withZone(ZoneId.systemDefault());

    private final long timeoutMs;

    private transient ValueState<Long> pendingTimerTs;
    private transient ValueState<OrderPaymentEvent> pendingOrder;

    public OrderTimeoutAlertFunction(long timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    @Override
    public void open(Configuration parameters) {
        pendingTimerTs = getRuntimeContext().getState(
                new ValueStateDescriptor<>("pending-timer-ts", Long.class));
        pendingOrder = getRuntimeContext().getState(
                new ValueStateDescriptor<>("pending-order", OrderPaymentEvent.class));
    }

    @Override
    public void processElement(OrderPaymentEvent event,
                               Context ctx,
                               Collector<String> out) throws Exception {
        long wm = ctx.timerService().currentWatermark();
        String wmStr = formatTs(wm, true);

        if (OrderPaymentEvent.TYPE_ORDER_CREATED.equals(event.getEventType())) {
            handleOrderCreated(event, ctx, out, wmStr);
        } else if (OrderPaymentEvent.TYPE_PAYMENT.equals(event.getEventType())) {
            handlePayment(event, ctx, out, wmStr);
        } else {
            out.collect(String.format("[IGNORE] orderId=%s unknown eventType=%s", event.getOrderId(), event.getEventType()));
        }
    }

    private void handleOrderCreated(OrderPaymentEvent event,
                                    Context ctx,
                                    Collector<String> out,
                                    String wmStr) throws Exception {
        // 同一 orderId 重复下单：先删旧定时器，避免定时器泄漏
        Long oldTimer = pendingTimerTs.value();
        if (oldTimer != null) {
            ctx.timerService().deleteEventTimeTimer(oldTimer);
            out.collect(String.format(
                    "[TIMER-DELETE] orderId=%s reason=re-order | oldTimerTs=%s",
                    event.getOrderId(), formatTs(oldTimer, false)));
        }

        long fireTs = event.getTs() + timeoutMs;
        pendingOrder.update(event);
        pendingTimerTs.update(fireTs);
        ctx.timerService().registerEventTimeTimer(fireTs);

        out.collect(String.format(
                "[TIMER-REGISTER] subtask=%d orderId=%s userId=%s | orderTs=%s | fireAt=%s (+%ds) | currentWM=%s | timerType=EventTime",
                getRuntimeContext().getIndexOfThisSubtask(),
                event.getOrderId(),
                event.getUserId(),
                formatTs(event.getTs(), false),
                formatTs(fireTs, false),
                timeoutMs / 1000,
                wmStr));
    }

    private void handlePayment(OrderPaymentEvent event,
                               Context ctx,
                               Collector<String> out,
                               String wmStr) throws Exception {
        Long timerTs = pendingTimerTs.value();
        OrderPaymentEvent order = pendingOrder.value();

        if (timerTs == null) {
            out.collect(String.format(
                    "[PAID-NO-PENDING] orderId=%s | paymentTs=%s | currentWM=%s | 无待支付订单（可能已超时或乱序）",
                    event.getOrderId(),
                    formatTs(event.getTs(), false),
                    wmStr));
            return;
        }

        ctx.timerService().deleteEventTimeTimer(timerTs);
        pendingTimerTs.clear();
        pendingOrder.clear();

        long latencyMs = event.getTs() - (order != null ? order.getTs() : event.getTs());
        out.collect(String.format(
                "[PAID-IN-TIME] subtask=%d orderId=%s userId=%s | orderTs=%s | paymentTs=%s | payLatency=%dms | "
                        + "deletedTimer=%s | currentWM=%s",
                getRuntimeContext().getIndexOfThisSubtask(),
                event.getOrderId(),
                event.getUserId(),
                order != null ? formatTs(order.getTs(), false) : "-",
                formatTs(event.getTs(), false),
                latencyMs,
                formatTs(timerTs, false),
                wmStr));
    }

    @Override
    public void onTimer(long timestamp,
                        OnTimerContext ctx,
                        Collector<String> out) throws Exception {
        Long registered = pendingTimerTs.value();
        if (registered == null || registered != timestamp) {
            return;
        }

        OrderPaymentEvent order = pendingOrder.value();
        long wm = ctx.timerService().currentWatermark();

        out.collect(String.format(
                "[TIMEOUT-ALERT] subtask=%d orderId=%s userId=%s | orderTs=%s | fireAt=%s | amount=%.1f | "
                        + "currentWM=%s | 超时未支付，触发告警",
                getRuntimeContext().getIndexOfThisSubtask(),
                ctx.getCurrentKey(),
                order != null ? order.getUserId() : "-",
                order != null ? formatTs(order.getTs(), false) : "-",
                formatTs(timestamp, false),
                order != null && order.getAmount() != null ? order.getAmount() : 0.0,
                formatTs(wm, true)));

        pendingTimerTs.clear();
        pendingOrder.clear();
    }

    private static String formatTs(long ts, boolean allowMinValue) {
        if (allowMinValue && ts == Long.MIN_VALUE) {
            return "MIN_VALUE";
        }
        return TIME_FMT.format(Instant.ofEpochMilli(ts));
    }

    public long getTimeoutMs() {
        return timeoutMs;
    }
}
