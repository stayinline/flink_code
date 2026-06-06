package org.example.job.window;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.dto.UserOrderEvent;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 分场景向 Kafka 发送 (userId, ts, amount) 测试数据。
 * <p>
 * 场景 A — Tumbling 5s：u001 在 [0,5) 3 条、[5,10) 2 条 → 共 2 条窗口输出
 * 场景 B — Sliding 双窗口：u002 仅在 ts=+7s 有 1 条 → 应输出 2 条（验收②）
 * 场景 C — Session merge：u003 先闭合两段会话，再发迟到 ts=+6s → merge 为 1 条（验收③）
 * 场景 D — watermark 推进：u004 ts=+30s 触发全部窗口关闭
 */
public class FlinkWindowDemoJobTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    /** Flink EventTime 窗口默认按 epoch 对齐；BASE 取 5s 整倍数便于阅读 */
    private static final long EPOCH_ALIGN_MS = 0L;

    public static final long BASE_TIME_MS = 1_700_000_000_000L;

    private static final long SLIDING_SIZE_MS = 10_000;
    private static final long SLIDE_MS = 5_000;

    @Test
    void sendWindowTestEvents() throws Exception {
        sendAllTestEvents();
    }

    /** 验收②：size=10/slide=5 时 ts=+7s 属于 2 个窗口 */
    @Test
    void slidingWindowMembership_at7s_belongsToTwoWindows() {
        long eventTs = BASE_TIME_MS + 7_000;
        List<long[]> windows = WindowTimeAxisHelper.slidingWindowsContaining(
                eventTs, SLIDING_SIZE_MS, SLIDE_MS, EPOCH_ALIGN_MS);
        assertEquals(2, windows.size(), "ts=+7s 应落入 [0,10) 与 [5,15) 两个滑动窗口");
        printSlidingMembership(eventTs, windows);
    }

    /** Step4①：slide 越小，同等 horizon 内窗口副本越多 */
    @Test
    void slidingWindowCount_inflatesWhenSlideShrinks() {
        long horizon = 60_000;
        int countSlide5 = WindowTimeAxisHelper.slidingWindowCount(horizon, SLIDING_SIZE_MS, 5_000);
        int countSlide1 = WindowTimeAxisHelper.slidingWindowCount(horizon, SLIDING_SIZE_MS, 1_000);
        System.out.printf("60s 内 size=10s: slide=5s → %d 个窗口副本; slide=1s → %d 个窗口副本%n",
                countSlide5, countSlide1);
        assert countSlide1 > countSlide5 : "slide 越小窗口数量应膨胀";
    }

    public static void main(String[] args) throws Exception {
        sendAllTestEvents();
    }

    public static void sendAllTestEvents() throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, FlinkWindowDemoJob.KAFKA_BROKER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        List<ScenarioEvent> plan = buildScenarioPlan();

        printHeader(plan);
        printSlidingMembership(BASE_TIME_MS + 7_000,
                WindowTimeAxisHelper.slidingWindowsContaining(
                        BASE_TIME_MS + 7_000, SLIDING_SIZE_MS, SLIDE_MS, EPOCH_ALIGN_MS));

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (ScenarioEvent item : plan) {
                UserOrderEvent event = item.event;
                String json = OBJECT_MAPPER.writeValueAsString(event);
                producer.send(new ProducerRecord<>(FlinkWindowDemoJob.TOPIC_NAME, event.getUserId(), json))
                        .get(10, TimeUnit.SECONDS);
                long offsetSec = (event.getTs() - BASE_TIME_MS) / 1000;
                System.out.printf("[SEND][%s] t=+%2ds userId=%s amount=%.1f | %s%n",
                        item.scenario, offsetSec, event.getUserId(), event.getAmount(), item.purpose);
                Thread.sleep(item.delayMs);
            }
            producer.flush();
        }

        printExpectedOutcomes();
    }

    private static List<ScenarioEvent> buildScenarioPlan() {
        List<ScenarioEvent> events = new ArrayList<>();

        // --- 场景 A：Tumbling 5s（u001）---
        events.add(sc("A-Tumbling", "u001 [0,5) 第1条", u("u001", 1, 10)));
        events.add(sc("A-Tumbling", "u001 [0,5) 第2条", u("u001", 3, 20)));
        events.add(sc("A-Tumbling", "u001 [0,5) 第3条 → 窗口1 sum=60", u("u001", 4, 30)));
        events.add(sc("A-Tumbling", "u001 [5,10) 第1条", u("u001", 6, 40)));
        events.add(sc("A-Tumbling", "u001 [5,10) 第2条 → 窗口2 sum=90", u("u001", 8, 50)));

        // --- 场景 B：Sliding 双窗口（u002 仅 ts=+7s 一条）---
        events.add(sc("B-Sliding", "u002 单条 ts=+7s → 应进 2 个滑动窗口", u("u002", 7, 100)));

        // --- 场景 C：Session + merge（u003）---
        events.add(sc("C-Session", "u003 会话段1", u("u003", 1, 10)));
        events.add(sc("C-Session", "u003 会话段1", u("u003", 2, 20)));
        events.add(sc("C-Session", "u003 会话段2（与段1间隔8s>gap）", u("u003", 10, 30)));
        events.add(sc("C-Session", "u003 会话段2", u("u003", 11, 40)));
        // 故意乱序：迟到 ts=+6s，桥接段1(ts=2)与段2(ts=10)，gap(10-6)=4<5 → merge
        events.add(sc("C-Session", "u003 迟到 ts=+6s → 触发 Session merge", u("u003", 6, 5), 500));

        // --- 场景 D：推进 watermark ---
        events.add(sc("D-Flush", "推进 watermark，关闭所有未触发窗口", u("u004", 30, 1)));

        return events;
    }

    private static UserOrderEvent u(String userId, long offsetSec, double amount) {
        return new UserOrderEvent(userId, BASE_TIME_MS + offsetSec * 1000, amount);
    }

    private static ScenarioEvent sc(String scenario, String purpose, UserOrderEvent event) {
        return new ScenarioEvent(scenario, purpose, event, 300);
    }

    private static ScenarioEvent sc(String scenario, String purpose, UserOrderEvent event, long delayMs) {
        return new ScenarioEvent(scenario, purpose, event, delayMs);
    }

    private static void printHeader(List<ScenarioEvent> plan) {
        System.out.println("========================================");
        System.out.println("Flink 窗口分场景测试数据发送");
        System.out.println("Broker: " + FlinkWindowDemoJob.KAFKA_BROKER);
        System.out.println("Topic:  " + FlinkWindowDemoJob.TOPIC_NAME);
        System.out.println("BASE_TIME: " + BASE_TIME_MS + " (窗口对齐基准)");
        System.out.println("共 " + plan.size() + " 条");
        System.out.println("========================================");
    }

    private static void printSlidingMembership(long eventTs, List<long[]> windows) {
        long offsetSec = (eventTs - BASE_TIME_MS) / 1000;
        System.out.println("--- 验收② Sliding 窗口归属（ts=+" + offsetSec + "s）---");
        for (int i = 0; i < windows.size(); i++) {
            long[] w = windows.get(i);
            System.out.printf("  窗口%d: [+%ds ~ +%ds)%n",
                    i + 1, (w[0] - BASE_TIME_MS) / 1000, (w[1] - BASE_TIME_MS) / 1000);
        }
        System.out.println("  → 共 " + windows.size() + " 个窗口（期望 2）");
        System.out.println("----------------------------------------");
    }

    private static void printExpectedOutcomes() {
        System.out.println("========================================");
        System.out.println("预期输出条数差异（同一 Kafka 数据源，按 userId 分组）：");
        System.out.println("  u001 TUMBLING  → 2 条（[0,5) sum=60, [5,10) sum=90）");
        System.out.println("  u002 SLIDING   → 2 条（[0,10) sum=100, [5,15) sum=100）← 验收②");
        System.out.println("  u003 SESSION   → merge 后 1 条（sum=105, count=5）← 验收③");
        System.out.println("  u001 SLIDING   → 多于 TUMBLING（重叠窗口导致条数膨胀）");
        System.out.println("详细原理见 resources/window/FlinkWindowDemoGuide.md");
        System.out.println("========================================");
    }

    private static class ScenarioEvent {
        final String scenario;
        final String purpose;
        final UserOrderEvent event;
        final long delayMs;

        ScenarioEvent(String scenario, String purpose, UserOrderEvent event, long delayMs) {
            this.scenario = scenario;
            this.purpose = purpose;
            this.event = event;
            this.delayMs = delayMs;
        }
    }
}
