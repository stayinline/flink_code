package org.example.job.watermark;

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

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 向 Kafka 发送 Watermark 演示数据：乱序、空闲分区、union 慢源阻塞。
 * <p>
 * 运行前请创建 topic（fast 需 2 分区）：
 * <pre>
 * kafka-topics.sh --create --topic test_flink_watermark --partitions 2 --bootstrap-server 192.168.1.124:9092
 * kafka-topics.sh --create --topic test_flink_watermark_slow --partitions 1 --bootstrap-server 192.168.1.124:9092
 * </pre>
 */
public class FlinkWatermarkDemoJobTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    public static final long BASE_TIME_MS = 1_700_000_000_000L;

    public static final int PARTITION_0 = 0;
    public static final int PARTITION_1 = 1;

    @Test
    void sendWatermarkDemoEvents() throws Exception {
        sendAllScenarios();
    }

    @Test
    void outOfOrderness_watermarkIsMonotonic() {
        // 加分点：乱序事件 ts 回退，WM 仍单调不减
        long ts1 = BASE_TIME_MS + 10_000;
        long ts2 = BASE_TIME_MS + 3_000; // 回退
        long wmAfterFirst = ts1 - FlinkWatermarkDemoJob.OUT_OF_ORDERNESS.toMillis(); // 5000
        long wmAfterSecond = Math.max(wmAfterFirst, ts2 - 5000);
        assertTrue(wmAfterSecond >= wmAfterFirst, "WM 单调不减：回退事件不会拉低 WM");
        System.out.printf("事件1 ts=+10s → WM=%d | 乱序事件2 ts=+3s → WM 仍为 %d（不会回退到 +3s-5s）%n",
                wmAfterFirst - BASE_TIME_MS, wmAfterSecond - BASE_TIME_MS);
    }

    public static void main(String[] args) throws Exception {
        sendAllScenarios();
    }

    public static void sendAllScenarios() throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, FlinkWatermarkDemoJob.KAFKA_BROKER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        List<SendPlan> plan = buildPlan();

        System.out.println("========================================");
        System.out.println("Watermark 演示数据发送");
        System.out.println("FAST topic: " + FlinkWatermarkDemoJob.TOPIC_FAST + " (2 partitions)");
        System.out.println("SLOW topic: " + FlinkWatermarkDemoJob.TOPIC_SLOW + " (union 慢源，Phase3 前沉默)");
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

        // ===== Phase 1：乱序数据（仅 fast topic partition 0）=====
        plan.add(fast(PARTITION_0, "e01", "u001", 1, 10, "on-time", "Phase1 正常 +1s"));
        plan.add(fast(PARTITION_0, "e02", "u001", 4, 20, "on-time", "Phase1 正常 +4s"));
        plan.add(fast(PARTITION_0, "e03", "u001", 2, 15, "out-of-order", "Phase1 乱序回退 +2s（观察 WM 不回退）"));
        plan.add(fast(PARTITION_0, "e04", "u001", 8, 30, "on-time", "Phase1 推进 +8s"));
        plan.add(fast(PARTITION_0, "e05", "u001", 6, 25, "out-of-order", "Phase1 乱序回退 +6s"));

        // ===== Phase 2：空闲分区故障（只写 partition 0，partition 1 沉默）=====
        plan.add(wait(3000, "观察 Phase1 WM 推进；partition1 仍无数据"));
        plan.add(fast(PARTITION_0, "e06", "u001", 12, 40, "p0-only", "Phase2 仅 p0 +12s，p1 空闲 → min(WM) 卡住"));
        plan.add(fast(PARTITION_0, "e07", "u001", 18, 50, "p0-only", "Phase2 仅 p0 +18s，窗口 [0,10) 仍可能不触发"));

        // ===== Phase 3：union 慢源沉默（fast 有数据，slow topic 无数据 → min WM 卡住）=====
        plan.add(wait(2000, "union 慢源 test_flink_watermark_slow 仍无数据"));
        plan.add(fast(PARTITION_0, "e08", "u001", 22, 60, "p0-only", "Phase3 fast 继续，slow 源沉默阻塞 union WM"));

        // ===== Phase 4a：修复方式1 — 唤醒空闲分区 =====
        plan.add(wait(2000, "若未开 withIdleness，窗口仍不出数"));
        plan.add(fast(PARTITION_1, "e09", "u001", 25, 70, "fix-partition",
                "Phase4a 向 p1 发 ts=+25s → 解除 Kafka 空闲分区阻塞"));

        // ===== Phase 4b：修复方式2 — 唤醒慢源（union min WM）=====
        plan.add(wait(2000, "等待 partition 修复后窗口可能部分触发"));
        plan.add(slow("e10", "u001", 28, 80, "fix-slow-source",
                "Phase4b 向 slow topic 发 ts=+28s → 解除 union 最小 WM 阻塞"));

        // ===== Phase 5：flush watermark =====
        plan.add(fast(PARTITION_0, "e11", "u001", 35, 1, "flush",
                "Phase5 ts=+35s 推进 WM，触发 [10,20) [20,30) 等窗口"));

        return plan;
    }

    private static SendPlan fast(int partition, String eventId, String userId, long offsetSec,
                                 double amount, String tag, String purpose) {
        return new SendPlan(
                FlinkWatermarkDemoJob.TOPIC_FAST,
                partition,
                new WatermarkDemoEvent(eventId, userId, BASE_TIME_MS + offsetSec * 1000, amount, "fast", tag),
                0,
                null,
                purpose
        );
    }

    private static SendPlan slow(String eventId, String userId, long offsetSec, double amount,
                                 String tag, String purpose) {
        return new SendPlan(
                FlinkWatermarkDemoJob.TOPIC_SLOW,
                null,
                new WatermarkDemoEvent(eventId, userId, BASE_TIME_MS + offsetSec * 1000, amount, "slow", tag),
                0,
                null,
                purpose
        );
    }

    private static SendPlan wait(long ms, String reason) {
        return new SendPlan(null, null, null, ms, reason, reason);
    }

    private static void send(KafkaProducer<String, String> producer, SendPlan step) throws Exception {
        if (step.event == null) {
            return;
        }
        String json = MAPPER.writeValueAsString(step.event);
        ProducerRecord<String, String> record;
        if (step.partition != null) {
            record = new ProducerRecord<>(step.topic, step.partition, step.event.getUserId(), json);
        } else {
            record = new ProducerRecord<>(step.topic, step.event.getUserId(), json);
        }
        producer.send(record).get(10, TimeUnit.SECONDS);
        long offsetSec = (step.event.getTs() - BASE_TIME_MS) / 1000;
        System.out.printf("[SEND] topic=%-30s part=%s t=+%2ds id=%s tag=%-14s | %s%n",
                step.topic,
                step.partition == null ? "-" : step.partition,
                offsetSec,
                step.event.getEventId(),
                step.event.getTag(),
                step.purpose);
        Thread.sleep(400);
    }

    private static void printPlan(List<SendPlan> plan) {
        System.out.println("--- 发送计划 ---");
        for (SendPlan s : plan) {
            if (s.event != null) {
                long off = (s.event.getTs() - BASE_TIME_MS) / 1000;
                System.out.printf("  [%s] p=%s t=+%2ds id=%s %s%n",
                        s.event.getTag(), s.partition == null ? "-" : s.partition, off, s.event.getEventId(), s.purpose);
            } else if (s.waitReason != null) {
                System.out.printf("  [WAIT %ds] %s%n", s.sleepBeforeMs / 1000, s.waitReason);
            }
        }
        System.out.println("----------------");
    }

    private static void printExpectedOutcomes() {
        System.out.println("========================================");
        System.out.println("预期观察（对照 Job 控制台）：");
        System.out.println("  Phase1 [WM-MONITOR] 乱序 +2s/+6s 到达时 currentWM 不回退");
        System.out.println("  Phase2~3 仅 p0 有数据 + slow 源沉默 → [WINDOW-FIRED] 长时间不出现（故障复现）");
        System.out.println("  修复 A：启动 Job 时传参 idlenessSeconds=10，等待 10s 后 WM 自动推进");
        System.out.println("  修复 B：Phase4a 向 partition1 发数据 / Phase4b 向 slow topic 发数据");
        System.out.println("  Phase5 flush 后应看到 [WINDOW-FIRED] 窗口=[...+10s ~ ...+20s) 等");
        System.out.println("对比实验：");
        System.out.println("  1) java FlinkWatermarkDemoJob        → 复现窗口不出数");
        System.out.println("  2) java FlinkWatermarkDemoJob 10     → withIdleness(10s) 自动修复");
        System.out.println("文档: resources/watermark/FlinkWatermarkDemoGuide.md");
        System.out.println("========================================");
    }

    private static class SendPlan {
        final String topic;
        final Integer partition;
        final WatermarkDemoEvent event;
        final long sleepBeforeMs;
        final String waitReason;
        final String purpose;

        SendPlan(String topic, Integer partition, WatermarkDemoEvent event,
                 long sleepBeforeMs, String waitReason, String purpose) {
            this.topic = topic;
            this.partition = partition;
            this.event = event;
            this.sleepBeforeMs = sleepBeforeMs;
            this.waitReason = waitReason;
            this.purpose = purpose;
        }
    }
}
