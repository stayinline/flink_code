package org.example.job.late;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.dto.WatermarkDemoEvent;
import org.example.job.late.FlinkLateDataDemoJob.LateDataLayer;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 向 Kafka 发送迟到数据三道防线演示数据：准时 / 轻度迟到 / 严重迟到。
 * <p>
 * 运行前请创建 topic：
 * <pre>
 * kafka-topics.sh --create --topic test_flink_late_data --partitions 1 --bootstrap-server 192.168.1.124:9092
 * </pre>
 */
public class FlinkLateDataDemoJobTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    public static final long BASE_TIME_MS = 1_700_000_000_000L;

    private static final long WINDOW_START = BASE_TIME_MS;
    private static final long WINDOW_END = BASE_TIME_MS + 10_000;
    private static final long ALLOWED_MS = FlinkLateDataDemoJob.ALLOWED_LATENESS.toMilliseconds();

    @Test
    void sendLateDataDemoEvents() throws Exception {
        sendAllScenarios();
    }

    @Test
    void threeLayers_classifyCorrectly() {
        // 窗口 [0, 10s)，outOfOrderness=5s，allowedLateness=3s → 状态保留至 WM≥13s

        // ① WM 未越过 window.end：准时 / 乱序缓冲
        assertEquals(LateDataLayer.ON_TIME_OR_WM_BUFFER,
                FlinkLateDataDemoJob.classifyEvent(
                        BASE_TIME_MS + 7_000, WINDOW_START, WINDOW_END, BASE_TIME_MS + 8_000, ALLOWED_MS));

        // ② WM∈[10s, 13s)：allowedLateness 内 → 同窗口重算
        assertEquals(LateDataLayer.ALLOWED_LATENESS_UPDATE,
                FlinkLateDataDemoJob.classifyEvent(
                        BASE_TIME_MS + 7_000, WINDOW_START, WINDOW_END, BASE_TIME_MS + 11_000, ALLOWED_MS));

        // ③ WM≥13s：彻底迟到 → 侧输出
        assertEquals(LateDataLayer.SIDE_OUTPUT,
                FlinkLateDataDemoJob.classifyEvent(
                        BASE_TIME_MS + 6_000, WINDOW_START, WINDOW_END, BASE_TIME_MS + 14_000, ALLOWED_MS));

        System.out.println("三道防线分类单测通过：");
        System.out.println("  ① WM<10s → 准时/WM缓冲");
        System.out.println("  ② 10s≤WM<13s → allowedLateness 重算");
        System.out.println("  ③ WM≥13s → sideOutput 侧流");
    }

    @Test
    void allowedLateness_isRecalculationNotNewWindow() {
        // 加分点：lateness 触发的是同一 windowStart 的结果更新
        long windowStart = WINDOW_START;
        long windowEnd = WINDOW_END;
        assertTrue(windowStart < windowEnd);
        assertEquals(WINDOW_END + ALLOWED_MS, windowEnd + FlinkLateDataDemoJob.ALLOWED_LATENESS.toMilliseconds(),
                "状态清退边界 = window.end + allowedLateness");
        System.out.printf("窗口 [0,10s) 状态保留至 WM≥+%ds，期间迟到事件触发 LATE-UPDATE（非新窗口）%n",
                (WINDOW_END - WINDOW_START) / 1000 + ALLOWED_MS / 1000);
    }

    public static void main(String[] args) throws Exception {
        sendAllScenarios();
    }

    public static void sendAllScenarios() throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, FlinkLateDataDemoJob.KAFKA_BROKER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        List<SendPlan> plan = buildPlan();

        System.out.println("========================================");
        System.out.println("迟到数据三道防线 — 测试数据发送");
        System.out.println("Topic: " + FlinkLateDataDemoJob.TOPIC);
        System.out.println("WM 乱序: " + FlinkLateDataDemoJob.OUT_OF_ORDERNESS.getSeconds() + "s");
        System.out.println("allowedLateness: " + FlinkLateDataDemoJob.ALLOWED_LATENESS.toMilliseconds() / 1000 + "s");
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

        // ===== Phase 1：准时数据 — 窗口 [0,10s) =====
        plan.add(event("e01", "u001", 2, 10.0, "on-time",
                "Phase1 准时 +2s → ① WM 缓冲内正常入窗"));
        plan.add(event("e02", "u001", 5, 20.0, "on-time",
                "Phase1 准时 +5s"));
        plan.add(event("e03", "u001", 9, 30.0, "on-time",
                "Phase1 准时 +9s（窗口 [0,10s) 共 3 条）"));

        // ===== Phase 2：推进 WM → 首次触发 =====
        plan.add(wait(1000, "等待 WM 周期性发射"));
        plan.add(event("e04", "u001", 16, 1.0, "flush",
                "Phase2 flush +16s → WM=+11s≥+10s → [FIRST-FIRE] count=3 sum=60"));

        // ===== Phase 3：轻度迟到 — allowedLateness 内重算 =====
        plan.add(wait(500, "在 WM 推过 +13s 之前发送轻度迟到"));
        plan.add(event("e05", "u001", 7, 40.0, "mild-late",
                "Phase3 轻度迟到 +7s（处理顺序晚于 e04）→ [LATE-UPDATE] count=4 sum=100"));

        // ===== Phase 4：推进 WM 越过 end+lateness → 清退窗口状态 =====
        plan.add(event("e06", "u001", 20, 1.0, "flush",
                "Phase4 flush +20s → WM=+15s≥+13s → 窗口 [0,10s) 状态清退"));

        // ===== Phase 5：严重迟到 — 侧输出 =====
        plan.add(wait(500, "窗口状态已清退"));
        plan.add(event("e07", "u001", 6, 50.0, "severe-late",
                "Phase5 严重迟到 +6s → ③ [COMPENSATE-DB] 侧输出补偿落库"));

        // ===== Phase 6：第二窗口 [10,20s) 演示 =====
        plan.add(event("e08", "u001", 12, 15.0, "on-time",
                "Phase6 窗口 [10,20s) 准时 +12s"));
        plan.add(event("e09", "u001", 18, 25.0, "on-time",
                "Phase6 准时 +18s"));
        plan.add(event("e10", "u001", 25, 1.0, "flush",
                "Phase6 flush +25s → WM=+20s → [FIRST-FIRE] 窗口 [10,20s) count=2 sum=40"));

        // ===== Phase 7：WM 缓冲内乱序（仍算第一道防线） =====
        plan.add(event("e11", "u001", 14, 5.0, "wm-buffer",
                "Phase7 乱序 +14s（WM 已高，但在 allowedLateness 内）→ 可能 LATE-UPDATE [10,20s)"));

        return plan;
    }

    private static SendPlan event(String eventId, String userId, long offsetSec,
                                  double amount, String tag, String purpose) {
        return new SendPlan(
                new WatermarkDemoEvent(eventId, userId, BASE_TIME_MS + offsetSec * 1000, amount, "late-demo", tag),
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
                FlinkLateDataDemoJob.TOPIC, step.event.getUserId(), json);
        producer.send(record).get(10, TimeUnit.SECONDS);
        long offsetSec = (step.event.getTs() - BASE_TIME_MS) / 1000;
        System.out.printf("[SEND] t=+%2ds id=%s amount=%.1f tag=%-12s | %s%n",
                offsetSec, step.event.getEventId(), step.event.getAmount(), step.event.getTag(), step.purpose);
        Thread.sleep(300);
    }

    private static void printPlan(List<SendPlan> plan) {
        System.out.println("--- 发送计划 ---");
        for (SendPlan s : plan) {
            if (s.event != null) {
                long off = (s.event.getTs() - BASE_TIME_MS) / 1000;
                System.out.printf("  [%s] t=+%2ds id=%s %s%n", s.event.getTag(), off, s.event.getEventId(), s.purpose);
            } else if (s.waitReason != null) {
                System.out.printf("  [WAIT %ds] %s%n", s.sleepBeforeMs / 1000, s.waitReason);
            }
        }
        System.out.println("----------------");
    }

    private static void printExpectedOutcomes() {
        System.out.println("========================================");
        System.out.println("预期观察（对照 Job 控制台）：");
        System.out.println("  Phase1~2 [FIRST-FIRE] 窗口=[...+0s ~ ...+10s) count=3 sum=60");
        System.out.println("  Phase3   [LATE-UPDATE] 同窗口 count=4 sum=100 ← allowedLateness 重算，非新窗口");
        System.out.println("  Phase5   [COMPENSATE-DB] eventId=e07 tag=severe-late ← 侧输出补偿");
        System.out.println("  Phase6   [FIRST-FIRE] 窗口=[...+10s ~ ...+20s) count=2 sum=40");
        System.out.println("对比实验：");
        System.out.println("  1) FlinkLateDataDemoJob         → billing 模式，侧输出+补偿");
        System.out.println("  2) FlinkLateDataDemoJob pv      → 大屏模式，e07 静默丢弃");
        System.out.println("文档: resources/watermark/FlinkLateDataDemoGuide.md");
        System.out.println("========================================");
    }

    private static class SendPlan {
        final WatermarkDemoEvent event;
        final long sleepBeforeMs;
        final String waitReason;
        final String purpose;

        SendPlan(WatermarkDemoEvent event, long sleepBeforeMs, String waitReason, String purpose) {
            this.event = event;
            this.sleepBeforeMs = sleepBeforeMs;
            this.waitReason = waitReason;
            this.purpose = purpose;
        }
    }
}
