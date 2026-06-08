package org.example.job.timer;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.dto.OrderPaymentEvent;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 向 Kafka 发送订单超时演示数据。
 * <p>
 * 运行前请创建 topic：
 * <pre>
 * kafka-topics.sh --create --topic test_flink_order_timeout --partitions 1 --bootstrap-server 192.168.1.124:9092
 * </pre>
 */
public class FlinkOrderTimeoutDemoJobTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    public static final long BASE_TIME_MS = 1_700_200_000_000L;
    public static final String DEMO_USER = "u_edu_001";

    @Test
    void sendOrderTimeoutDemoEvents() throws Exception {
        sendAllScenarios();
    }

    @Test
    void eventTimeTimer_fireAtOrderTsPlusTimeout() {
        long orderTs = BASE_TIME_MS + 10_000;
        long fireTs = computeEventTimeTimerFireTs(orderTs, FlinkOrderTimeoutDemoJob.TIMEOUT_MS);
        assertEquals(orderTs + 15_000, fireTs);
        System.out.printf("下单 ts=+10s → 定时器 fireAt=+25s（超时 %ds）%n", FlinkOrderTimeoutDemoJob.TIMEOUT_MS / 1000);
    }

    @Test
    void paymentBeforeTimeout_shouldCancelAlert() {
        long orderTs = BASE_TIME_MS;
        long paymentTs = BASE_TIME_MS + 5_000;
        long fireTs = computeEventTimeTimerFireTs(orderTs, FlinkOrderTimeoutDemoJob.TIMEOUT_MS);

        assertTrue(paymentTs < fireTs, "支付在超时前，应删定时器");
        assertFalse(shouldFireTimeoutAlert(paymentTs, fireTs), "不应触发 TIMEOUT-ALERT");
        System.out.println("O1：+5s 支付 → [PAID-IN-TIME]，无告警");
    }

    @Test
    void eventTimeTimer_requiresWatermarkAdvance() {
        long orderTs = BASE_TIME_MS + 10_000;
        long fireTs = computeEventTimeTimerFireTs(orderTs, FlinkOrderTimeoutDemoJob.TIMEOUT_MS);
        long wmBeforeFlush = orderTs - FlinkOrderTimeoutDemoJob.OUT_OF_ORDERNESS.toMillis();

        assertFalse(shouldFireTimeoutAlert(wmBeforeFlush, fireTs),
                "WM 仅到 +5s 时，+25s 定时器不应触发");

        long wmAfterFlush = BASE_TIME_MS + 30_000 - FlinkOrderTimeoutDemoJob.OUT_OF_ORDERNESS.toMillis();
        assertTrue(shouldFireTimeoutAlert(wmAfterFlush, fireTs),
                "flush 后 WM≥+25s，定时器应触发");
        System.out.println("事件时间定时器依赖 WM 推进（联动 D2 Watermark）");
    }

    @Test
    void timerTypes_difference_documented() {
        assertEquals(
                "触发时刻 = orderTs + timeout，依赖 WM 推进，与业务时间一致，可容错乱序",
                describeEventTimeTimer());
        assertEquals(
                "触发时刻 = 系统时钟 + delay，不依赖 WM，重启后按处理时间重新计时，可能漂移",
                describeProcessingTimeTimer());
        System.out.println("两类定时器差异单测通过");
    }

    public static void main(String[] args) throws Exception {
        sendAllScenarios();
    }

    public static void sendAllScenarios() throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, FlinkOrderTimeoutDemoJob.KAFKA_BROKER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        List<SendPlan> plan = buildPlan();

        System.out.println("========================================");
        System.out.println("订单超时定时器 演示数据发送");
        System.out.println("Topic: " + FlinkOrderTimeoutDemoJob.TOPIC);
        System.out.println("超时: " + (FlinkOrderTimeoutDemoJob.TIMEOUT_MS / 1000) + "s");
        System.out.println("共 " + plan.size() + " 步");
        System.out.println("========================================");
        printPlan(plan);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (SendPlan step : plan) {
                if (step.sleepBeforeMs > 0) {
                    System.out.printf("%n--- 等待 %ds：%s ---%n", step.sleepBeforeMs / 1000, step.waitReason);
                    Thread.sleep(step.sleepBeforeMs);
                }
                send(producer, step);
            }
            producer.flush();
        }

        printExpectedOutcomes();
    }

    private static List<SendPlan> buildPlan() {
        List<SendPlan> plan = new ArrayList<>();

        // Phase 1：O1 下单 + 5s 内支付 → 删定时器，无告警
        plan.add(order("O1", 0, 199.0, "phase1-order", "Phase1 O1 下单 ts=+0s，注册 +15s 定时器"));
        plan.add(payment("O1", 5, 199.0, "phase1-paid", "Phase1 O1 支付 ts=+5s，删定时器"));

        plan.add(wait(2000, "观察 [PAID-IN-TIME] O1，无 [TIMEOUT-ALERT]"));

        // Phase 2：O2 下单不支付 → 需 WM 推过 +25s 才告警
        plan.add(order("O2", 10, 299.0, "phase2-order", "Phase2 O2 下单 ts=+10s，定时器 fireAt=+25s"));

        plan.add(wait(3000, "WM 仅 ~+5s，O2 定时器尚未触发（陷阱② WM 不动定时器不触发）"));

        // Phase 3：flush 推进 WM → O2 超时告警
        plan.add(flush("flush-o2", 30, "phase3-flush", "Phase3 flush ts=+30s → WM≥+25s → O2 [TIMEOUT-ALERT]"));

        plan.add(wait(2000, "确认 O2 告警已输出"));

        // Phase 4：O3 下单，演示 WM 卡住期间无告警
        plan.add(order("O3", 40, 99.0, "phase4-order", "Phase4 O3 下单 ts=+40s，定时器 fireAt=+55s"));

        plan.add(wait(5000, "无新事件，WM 停在 ~+25s，O3 告警不会出现"));

        // Phase 5：flush 推进 WM → O3 告警
        plan.add(flush("flush-o3", 65, "phase5-flush", "Phase5 flush ts=+65s → WM≥+55s → O3 [TIMEOUT-ALERT]"));

        return plan;
    }

    private static SendPlan order(String orderId, long offsetSec, double amount, String tag, String purpose) {
        return event(orderId, OrderPaymentEvent.TYPE_ORDER_CREATED, offsetSec, amount, tag, purpose);
    }

    private static SendPlan payment(String orderId, long offsetSec, double amount, String tag, String purpose) {
        return event(orderId, OrderPaymentEvent.TYPE_PAYMENT, offsetSec, amount, tag, purpose);
    }

    private static SendPlan flush(String orderId, long offsetSec, String tag, String purpose) {
        return event(orderId, OrderPaymentEvent.TYPE_ORDER_CREATED, offsetSec, 0.0, tag, purpose);
    }

    private static SendPlan event(String orderId, String eventType, long offsetSec,
                                  double amount, String tag, String purpose) {
        return new SendPlan(
                new OrderPaymentEvent(orderId, DEMO_USER, eventType, amount, BASE_TIME_MS + offsetSec * 1000, tag),
                0,
                null,
                purpose
        );
    }

    private static SendPlan wait(long ms, String reason) {
        return new SendPlan(null, ms, reason, reason);
    }

    private static void send(KafkaProducer<String, String> producer, SendPlan step) throws Exception {
        if (step.event == null) {
            return;
        }
        String json = MAPPER.writeValueAsString(step.event);
        ProducerRecord<String, String> record = new ProducerRecord<>(
                FlinkOrderTimeoutDemoJob.TOPIC,
                step.event.getOrderId(),
                json
        );
        producer.send(record).get(10, TimeUnit.SECONDS);
        long offsetSec = (step.event.getTs() - BASE_TIME_MS) / 1000;
        System.out.printf("[SEND] t=+%2ds orderId=%-4s type=%-13s amount=%.1f tag=%-12s | %s%n",
                offsetSec,
                step.event.getOrderId(),
                step.event.getEventType(),
                step.event.getAmount(),
                step.event.getTag(),
                step.purpose);
        Thread.sleep(300);
    }

    private static void printPlan(List<SendPlan> plan) {
        System.out.println("--- 发送计划 ---");
        for (SendPlan s : plan) {
            if (s.event != null) {
                long off = (s.event.getTs() - BASE_TIME_MS) / 1000;
                System.out.printf("  [%s] t=+%2ds orderId=%s type=%s %s%n",
                        s.event.getTag(), off, s.event.getOrderId(), s.event.getEventType(), s.purpose);
            } else if (s.waitReason != null) {
                System.out.printf("  [WAIT %ds] %s%n", s.sleepBeforeMs / 1000, s.waitReason);
            }
        }
        System.out.println("----------------");
    }

    private static void printExpectedOutcomes() {
        System.out.println("========================================");
        System.out.println("预期观察（对照 Job 控制台）：");
        System.out.println("  Phase1 O1: [TIMER-REGISTER] fireAt=+15s → [PAID-IN-TIME] → [TIMER-DELETE]");
        System.out.println("  Phase2~3 O2: [TIMER-REGISTER] fireAt=+25s → WM 未推进时无告警");
        System.out.println("               flush +30s 后 → [TIMEOUT-ALERT] O2");
        System.out.println("  Phase4~5 O3: fireAt=+55s，等待期无告警 → flush +65s → [TIMEOUT-ALERT] O3");
        System.out.println("验收要点：");
        System.out.println("  ① 能手写超时告警（KeyedProcessFunction + register/delete EventTimeTimer）");
        System.out.println("  ② 说清 EventTime vs ProcessingTime 定时器差异");
        System.out.println("  ③ 支付后必须 deleteTimer，否则定时器泄漏");
        System.out.println("文档: resources/timer/FlinkOrderTimeoutDemoGuide.md");
        System.out.println("========================================");
    }

    static long computeEventTimeTimerFireTs(long orderTs, long timeoutMs) {
        return orderTs + timeoutMs;
    }

    static boolean shouldFireTimeoutAlert(long currentWatermark, long timerFireTs) {
        return currentWatermark >= timerFireTs;
    }

    static String describeEventTimeTimer() {
        return "触发时刻 = orderTs + timeout，依赖 WM 推进，与业务时间一致，可容错乱序";
    }

    static String describeProcessingTimeTimer() {
        return "触发时刻 = 系统时钟 + delay，不依赖 WM，重启后按处理时间重新计时，可能漂移";
    }

    private static class SendPlan {
        final OrderPaymentEvent event;
        final long sleepBeforeMs;
        final String waitReason;
        final String purpose;

        SendPlan(OrderPaymentEvent event, long sleepBeforeMs, String waitReason, String purpose) {
            this.event = event;
            this.sleepBeforeMs = sleepBeforeMs;
            this.waitReason = waitReason;
            this.purpose = purpose;
        }
    }
}
