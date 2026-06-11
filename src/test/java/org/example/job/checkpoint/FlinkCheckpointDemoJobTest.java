package org.example.job.checkpoint;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.dto.StateDemoEvent;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Checkpoint 演示数据发送 + barrier 对齐逻辑单测。
 * <p>
 * 运行前请创建 topic：
 * <pre>
 * kafka-topics.sh --create --topic test_flink_checkpoint --partitions 2 --bootstrap-server 192.168.1.124:9092
 * kafka-topics.sh --create --topic test_flink_checkpoint_slow --partitions 1 --bootstrap-server 192.168.1.124:9092
 * </pre>
 */
public class FlinkCheckpointDemoJobTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    public static final long BASE_TIME_MS = 1_700_400_000_000L;
    public static final String DEMO_STUDENT = "S20001";

    @Test
    void sendCheckpointDemoEvents() throws Exception {
        sendAllScenarios();
    }

    @Test
    void alignedBarrier_waitsUntilAllChannelsReady() {
        BarrierAlignmentSimulator sim = new BarrierAlignmentSimulator();
        sim.onBarrier("fast-channel");
        assertFalse(sim.isAlignmentComplete(), "仅 fast 收到 barrier，Aligned 未完成对齐");

        sim.onBarrier("slow-channel");
        assertTrue(sim.isAlignmentComplete(), "fast+slow 齐 → 可开始快照");
        System.out.println("Aligned：union 算子需等齐所有上游 channel 的 barrier");
    }

    @Test
    void unalignedBarrier_doesNotBlockOnSlowChannel() {
        // Unaligned：barrier 越过 in-flight 数据，不阻塞等慢 channel 排空
        boolean unalignedMode = true;
        boolean slowChannelStillProcessing = true;
        boolean checkpointCanProceed = unalignedMode || !slowChannelStillProcessing;
        assertTrue(checkpointCanProceed, "Unaligned 下慢 channel 仍在处理也可继续 CK");
        System.out.println("Unaligned：in-flight 数据写入快照，alignment time 趋近 0，状态体积增大");
    }

    @Test
    void checkpointTimeout_sixRootCausesChecklist() {
        List<String> causes = List.of(
                "反压（对齐慢）→ 看 backpressure / alignment time",
                "状态过大 → 看 state size / sync duration",
                "磁盘慢 → 看 async duration / 存储 IO",
                "barrier 对齐时间长 → union 慢输入 / aligned 模式",
                "GC → TaskManager GC 日志 / heap",
                "外部 Sink 慢 → 2PC pre-commit / sink sync 阶段"
        );
        assertEquals(6, causes.size());
        causes.forEach(c -> System.out.println("  □ " + c));
    }

    @Test
    void tuningOptions_coverAlignedVsUnalignedTradeoff() {
        String aligned = "低状态开销、恢复快；反压下 alignment 易超时";
        String unaligned = "反压下 CK 稳；快照含 in-flight，状态更大、恢复更复杂";
        assertTrue(aligned.contains("alignment"));
        assertTrue(unaligned.contains("in-flight"));
        System.out.println("Aligned 取舍: " + aligned);
        System.out.println("Unaligned 取舍: " + unaligned);
    }

    public static void main(String[] args) throws Exception {
        sendAllScenarios();
    }

    public static void sendAllScenarios() throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, FlinkCheckpointDemoJob.KAFKA_BROKER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        List<SendPlan> plan = buildPlan();

        System.out.println("========================================");
        System.out.println("Checkpoint 演示数据发送");
        System.out.println("FAST: " + FlinkCheckpointDemoJob.TOPIC_FAST);
        System.out.println("SLOW: " + FlinkCheckpointDemoJob.TOPIC_SLOW);
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

        // Phase 1：快支路正常心跳，积累 MapState
        plan.add(fast("e01", DEMO_STUDENT, "C_JAVA", 60, 1, "fast-normal",
                "Phase1 快支路 +60s，积累状态供 CK 快照"));
        plan.add(fast("e02", DEMO_STUDENT, "C_PYTHON", 45, 2, "fast-normal",
                "Phase1 多课程 MapState"));
        plan.add(fast("e03", "S20002", "C_MATH", 90, 3, "fast-normal",
                "Phase1 第二学员，增大 state size"));

        // Phase 2：慢支路 burst（Job 若 slowBranchMs=300，alignment time 显著升高）
        plan.add(wait(2000, "等待 Phase1 处理，观察第一次 CK"));
        plan.add(slow("e04", DEMO_STUDENT, "C_JAVA", 30, 4, "slow-lms",
                "Phase2 慢支路 LMS 回流 +30s（每条 sleep 300ms 时拖对齐）"));
        plan.add(slow("e05", DEMO_STUDENT, "C_ENG", 40, 5, "slow-lms",
                "Phase2 慢支路连续 2 条"));
        plan.add(fast("e06", DEMO_STUDENT, "C_JAVA", 20, 6, "fast-during-slow",
                "Phase2 快支路并行到达 → union 对齐等慢支路"));

        // Phase 3：状态膨胀（多学员多课程）
        plan.add(wait(3000, "观察 UI alignment time / sync duration"));
        for (int i = 0; i < 5; i++) {
            plan.add(fast("e1" + i, "S200" + (10 + i), "C_COURSE_" + i, 120, 10 + i, "state-growth",
                    "Phase3 扩状态 → state size 上升"));
        }

        // Phase 4：慢支路沉默 + 快支路继续（类似 WM min，这里是 barrier 对齐等慢输入）
        plan.add(wait(2000, "慢支路停发，快支路继续"));
        plan.add(fast("e20", DEMO_STUDENT, "C_JAVA", 15, 20, "fast-only",
                "Phase4 仅快支路，观察 aligned 下慢支路空闲 channel 行为"));

        // Phase 5：恢复慢支路 flush
        plan.add(slow("e21", DEMO_STUDENT, "C_JAVA", 10, 21, "slow-flush",
                "Phase5 慢支路恢复，解除对齐等待"));

        return plan;
    }

    private static SendPlan fast(String eventId, String studentId, String courseId,
                                 int watchSec, long offsetSec, String tag, String purpose) {
        return new SendPlan(
                FlinkCheckpointDemoJob.TOPIC_FAST,
                null,
                progress(eventId, studentId, courseId, watchSec, offsetSec, tag),
                0,
                null,
                purpose
        );
    }

    private static SendPlan slow(String eventId, String studentId, String courseId,
                                 int watchSec, long offsetSec, String tag, String purpose) {
        return new SendPlan(
                FlinkCheckpointDemoJob.TOPIC_SLOW,
                null,
                progress(eventId, studentId, courseId, watchSec, offsetSec, tag),
                0,
                null,
                purpose
        );
    }

    private static SendPlan wait(long ms, String reason) {
        return new SendPlan(null, null, null, ms, reason, reason);
    }

    private static StateDemoEvent progress(String eventId, String studentId, String courseId,
                                           int watchSec, long offsetSec, String tag) {
        return new StateDemoEvent(
                eventId, studentId, courseId,
                StateDemoEvent.TYPE_VIDEO_PROGRESS,
                watchSec, null, null,
                BASE_TIME_MS + offsetSec * 1000,
                tag
        );
    }

    private static void send(KafkaProducer<String, String> producer, SendPlan step) throws Exception {
        if (step.event == null) {
            return;
        }
        String json = MAPPER.writeValueAsString(step.event);
        ProducerRecord<String, String> record = new ProducerRecord<>(
                step.topic,
                step.event.getStudentId(),
                json
        );
        producer.send(record).get(10, TimeUnit.SECONDS);
        System.out.printf("[SEND] topic=%-32s id=%s student=%s course=%s tag=%-14s | %s%n",
                step.topic, step.event.getEventId(), step.event.getStudentId(),
                step.event.getCourseId(), step.event.getTag(), step.purpose);
        Thread.sleep(400);
    }

    private static void printPlan(List<SendPlan> plan) {
        System.out.println("--- 发送计划 ---");
        for (SendPlan s : plan) {
            if (s.event != null) {
                System.out.printf("  [%s] id=%s %s%n", s.event.getTag(), s.event.getEventId(), s.purpose);
            } else if (s.waitReason != null) {
                System.out.printf("  [WAIT %ds] %s%n", s.sleepBeforeMs / 1000, s.waitReason);
            }
        }
        System.out.println("----------------");
    }

    private static void printExpectedOutcomes() {
        System.out.println("========================================");
        System.out.println("预期观察（对照 Job 控制台 + Flink UI）：");
        System.out.println("  [CK-STATE] mapEntries 随 Phase1~3 增长");
        System.out.println("  [CK-COMPLETE] checkpointId 递增；若 [CK-ABORTED] 查 UI 失败原因");
        System.out.println("  UI → Checkpoints：alignment time（aligned+slowBranchMs=300 时升高）");
        System.out.println("  UI → sync / async duration、state size");
        System.out.println("对比实验：");
        System.out.println("  1) FlinkCheckpointDemoJob aligned 300 0     → 易见 alignment 慢");
        System.out.println("  2) FlinkCheckpointDemoJob unaligned 300 0   → alignment≈0，CK 更易成功");
        System.out.println("  3) FlinkCheckpointDemoJob aligned 0 200 rocksdb → 慢 Sink 拉长 sync");
        System.out.println("文档: resources/checkpoint/FlinkCheckpointDemoGuide.md");
        System.out.println("========================================");
    }

    /** 模拟 Aligned barrier 多 channel 对齐 */
    static class BarrierAlignmentSimulator {
        private final Map<String, Boolean> channelBarrier = new HashMap<>();

        void onBarrier(String channel) {
            channelBarrier.put(channel, true);
        }

        boolean isAlignmentComplete() {
            return channelBarrier.size() >= 2
                    && channelBarrier.values().stream().allMatch(Boolean::booleanValue);
        }
    }

    private static class SendPlan {
        final String topic;
        final Integer partition;
        final StateDemoEvent event;
        final long sleepBeforeMs;
        final String waitReason;
        final String purpose;

        SendPlan(String topic, Integer partition, StateDemoEvent event,
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
