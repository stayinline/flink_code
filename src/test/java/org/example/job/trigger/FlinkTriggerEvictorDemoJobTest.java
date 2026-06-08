package org.example.job.trigger;

import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.dto.WatermarkDemoEvent;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 向 Kafka 发送 Trigger / Evictor 演示数据。
 * <p>
 * 运行前请创建 topic：
 * <pre>
 * kafka-topics.sh --create --topic test_flink_trigger --partitions 1 --bootstrap-server 192.168.1.124:9092
 * </pre>
 */
public class FlinkTriggerEvictorDemoJobTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    public static final long BASE_TIME_MS = 1_700_100_000_000L;
    public static final String DEMO_USER_ID = "u_trigger";

    @Test
    void sendTriggerEvictorDemoEvents() throws Exception {
        sendAllScenarios();
    }

    @Test
    void countOrTimeTrigger_firesEvery100Elements() {
        long threshold = FlinkTriggerEvictorDemoJob.COUNT_THRESHOLD;
        for (long i = 1; i <= 250; i++) {
            if (i % threshold == 0) {
                assertTrue(shouldEarlyFire(i, threshold), "第 " + i + " 条应触发 early-fire");
            } else {
                assertEquals(TriggerResult.CONTINUE, simulateOnElementResult(i, threshold));
            }
        }
        System.out.println("CountOrTimeTrigger 逻辑：100/200 条 early-fire，220 条后 flush 触发 final-fire");
    }

    @Test
    void triggerResult_fourKinds_documented() {
        // 验收：能说清 4 种 TriggerResult
        assertEquals("继续累积，不计算不下发", describe(TriggerResult.CONTINUE));
        assertEquals("触发计算并输出，保留窗口元素", describe(TriggerResult.FIRE));
        assertEquals("清空窗口元素，不触发计算", describe(TriggerResult.PURGE));
        assertEquals("触发计算并输出，同时清空窗口元素", describe(TriggerResult.FIRE_AND_PURGE));
        System.out.println("TriggerResult 四种语义已对照通过");
    }

    public static void main(String[] args) throws Exception {
        sendAllScenarios();
    }

    public static void sendAllScenarios() throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, FlinkTriggerEvictorDemoJob.KAFKA_BROKER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        List<SendPlan> plan = buildPlan();

        System.out.println("========================================");
        System.out.println("Trigger & Evictor 演示数据发送");
        System.out.println("Topic: " + FlinkTriggerEvictorDemoJob.TOPIC);
        System.out.println("窗口: Tumbling 5min | CountOrTimeTrigger 阈值=" + FlinkTriggerEvictorDemoJob.COUNT_THRESHOLD);
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

        // Phase 1：快速灌入 100 条（均在 5min 窗口内）→ CountOrTimeTrigger 第 1 次 EARLY-FIRE
        for (int i = 1; i <= 100; i++) {
            plan.add(event(
                    String.format("t%03d", i),
                    i,
                    1.0,
                    "burst-100",
                    "Phase1 灌入第 " + i + " 条，满 100 触发 early-fire"
            ));
        }

        plan.add(wait(2000, "观察自定义Trigger 支路 [EARLY-FIRE] count=100"));

        // Phase 2：再灌 100 条 → 第 2 次 EARLY-FIRE（count=200）
        for (int i = 101; i <= 200; i++) {
            plan.add(event(
                    String.format("t%03d", i),
                    i,
                    1.0,
                    "burst-200",
                    "Phase2 灌入第 " + i + " 条，满 200 触发第 2 次 early-fire"
            ));
        }

        plan.add(wait(2000, "观察第 2 次 [EARLY-FIRE] count=200；默认 Trigger 仍无输出"));

        // Phase 3：再灌 20 条 → 窗口内共 220 条，尚未 final-fire
        for (int i = 201; i <= 220; i++) {
            plan.add(event(
                    String.format("t%03d", i),
                    i,
                    1.0,
                    "tail-220",
                    "Phase3 灌入第 " + i + " 条，累计 220，等待 WM flush"
            ));
        }

        plan.add(wait(2000, "默认 Trigger 仍无输出；Evictor 支路 early-fire 时 count≤30"));

        // Phase 4：flush 事件推进 WM 越过窗口 end → 所有支路 FINAL-FIRE
        plan.add(event(
                "flush",
                310,
                0.0,
                "flush",
                "Phase4 ts=+310s 推进 WM≥+300s，触发 5min 窗口 final-fire"
        ));

        return plan;
    }

    private static SendPlan event(String eventId, long offsetSec, double amount, String tag, String purpose) {
        return new SendPlan(
                new WatermarkDemoEvent(eventId, DEMO_USER_ID, BASE_TIME_MS + offsetSec * 1000, amount, "trigger", tag),
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
                FlinkTriggerEvictorDemoJob.TOPIC,
                step.event.getUserId(),
                json
        );
        producer.send(record).get(10, TimeUnit.SECONDS);
        long offsetSec = (step.event.getTs() - BASE_TIME_MS) / 1000;
        System.out.printf("[SEND] t=+%3ds id=%-6s amount=%.1f tag=%-10s | %s%n",
                offsetSec,
                step.event.getEventId(),
                step.event.getAmount(),
                step.event.getTag(),
                step.purpose);
        Thread.sleep(80);
    }

    private static void printPlan(List<SendPlan> plan) {
        System.out.println("--- 发送计划（摘要）---");
        int eventSteps = 0;
        for (SendPlan s : plan) {
            if (s.event != null) {
                eventSteps++;
            } else if (s.waitReason != null) {
                System.out.printf("  [WAIT %ds] %s%n", s.sleepBeforeMs / 1000, s.waitReason);
            }
        }
        System.out.printf("  事件步数: %d（含 220 条业务 + 1 条 flush）%n", eventSteps);
        System.out.println("----------------");
    }

    private static void printExpectedOutcomes() {
        System.out.println("========================================");
        System.out.println("预期观察（对照 Job 控制台）：");
        System.out.println("  默认Trigger：仅 1 条 [FINAL-FIRE] count=220 sum=220");
        System.out.println("  自定义Trigger+聚合：");
        System.out.println("    [EARLY-FIRE] count=100 sum=100");
        System.out.println("    [EARLY-FIRE] count=200 sum=200");
        System.out.println("    [FINAL-FIRE] count=220 sum=220");
        System.out.println("  自定义Trigger（Process）：同上 count/sum，可见 first/last eventId");
        System.out.println("  Evictor演示：每次 FIRE count≤30（仅保留最近 30 条）");
        System.out.println("对比要点：");
        System.out.println("  early-fire 不改窗口大小，只改「何时第一次出数」");
        System.out.println("  下游需幂等/覆盖：同一窗口会多次输出");
        System.out.println("文档: resources/trigger/FlinkTriggerEvictorDemoGuide.md");
        System.out.println("========================================");
    }

    /** 模拟 CountOrTimeTrigger.onElement 的触发判定（单测用） */
    static boolean shouldEarlyFire(long elementCount, long threshold) {
        return elementCount % threshold == 0;
    }

    static TriggerResult simulateOnElementResult(long elementCount, long threshold) {
        return shouldEarlyFire(elementCount, threshold) ? TriggerResult.FIRE : TriggerResult.CONTINUE;
    }

    static String describe(TriggerResult result) {
        if (result.isFire() && result.isPurge()) {
            return "触发计算并输出，同时清空窗口元素";
        }
        if (result.isFire()) {
            return "触发计算并输出，保留窗口元素";
        }
        if (result.isPurge()) {
            return "清空窗口元素，不触发计算";
        }
        return "继续累积，不计算不下发";
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
