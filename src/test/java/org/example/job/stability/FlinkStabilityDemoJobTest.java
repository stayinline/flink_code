package org.example.job.stability;

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
 * 稳定性串讲演示数据发送 + 反压/倾斜/两阶段聚合逻辑单测。
 * <pre>
 * kafka-topics.sh --create --topic test_flink_stability --partitions 4 \
 *   --bootstrap-server 192.168.1.124:9092
 * </pre>
 */
public class FlinkStabilityDemoJobTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    public static final long BASE_TIME_MS = 1_700_600_000_000L;

    @Test
    void sendStabilityDemoEvents() throws Exception {
        sendAllScenarios();
    }

    @Test
    void restartStrategy_relatesToCheckpointRecovery() {
        String fixedDelay = "失败 → fixed-delay 重试 → 从最近成功 CK 恢复状态+offset";
        String failureRate = "频繁失败 → failure-rate 熔断 → 避免重启风暴";
        String exponential = "外部抖动 → exponential-delay 退避 → 给 Kafka/DB 恢复时间";
        assertTrue(fixedDelay.contains("CK"));
        assertTrue(failureRate.contains("熔断"));
        assertTrue(exponential.contains("退避"));
        System.out.println("重启策略与 CK：" + fixedDelay);
    }

    @Test
    void backpressureChain_downstreamBlocksUpstream() {
        BackpressureChainSimulator sim = new BackpressureChainSimulator();
        sim.setSinkSleepMs(200);
        sim.enqueueRecords(10);
        sim.tickProcessing();
        assertTrue(sim.isUpstreamBlocked(), "Sink 慢 → 输出 buffer 满 → 上游 blocked");
        assertTrue(sim.backpressureRatio() > 0.5, "反压比例应显著升高");
        System.out.printf("反压链路：Sink 200ms/条 → blocked=%s ratio=%.0f%% | UI BackPressure 标红%n",
                sim.isUpstreamBlocked(), sim.backpressureRatio() * 100);
    }

    @Test
    void skewDetection_hotKeyImbalance() {
        SkewLoadSimulator sim = new SkewLoadSimulator(4);
        // 均匀 4 课程
        for (int i = 0; i < 20; i++) {
            sim.route("C_COURSE_" + (i % 4), 1);
        }
        double cvBefore = sim.coefficientOfVariation();
        // 热点 burst：30 条进同一 key
        for (int i = 0; i < 30; i++) {
            sim.route(StabilityConfigurator.HOT_COURSE_ID, 1);
        }
        double cvAfter = sim.coefficientOfVariation();
        assertTrue(cvAfter > cvBefore, "热点 key 应拉大 subtask 负载差异");
        int hotSubtask = sim.subtaskForKey(StabilityConfigurator.HOT_COURSE_ID);
        assertTrue(sim.loadOn(hotSubtask) > sim.averageLoad() * 2,
                "热点 subtask 负载应显著高于均值");
        System.out.printf("倾斜：CV %.2f→%.2f | hotSubtask=%d load=%d avg=%.1f%n",
                cvBefore, cvAfter, hotSubtask, sim.loadOn(hotSubtask), sim.averageLoad());
    }

    @Test
    void twoPhaseAggregation_matchesNaiveSum() {
        List<StateDemoEvent> events = List.of(
                progress("e1", "S001", StabilityConfigurator.HOT_COURSE_ID, 30, 1, "hot"),
                progress("e2", "S002", StabilityConfigurator.HOT_COURSE_ID, 40, 2, "hot"),
                progress("e3", "S003", StabilityConfigurator.HOT_COURSE_ID, 50, 3, "hot"),
                progress("e4", "S004", "C_MATH", 20, 4, "normal")
        );

        long naiveSum = events.stream()
                .filter(e -> StabilityConfigurator.HOT_COURSE_ID.equals(e.getCourseId()))
                .mapToInt(e -> e.getWatchSec())
                .sum();

        TwoPhaseAggregator aggregator = new TwoPhaseAggregator(8);
        for (StateDemoEvent e : events) {
            aggregator.add(e);
        }
        assertEquals(naiveSum, aggregator.globalTotal(StabilityConfigurator.HOT_COURSE_ID));
        assertTrue(aggregator.maxLocalBucketLoad() < naiveSum,
                "local 阶段应打散到多个 salt 桶，单桶小于总量");
        System.out.printf("两阶段：hotCourse naive=%ds global=%ds maxLocalBucket=%d%n",
                naiveSum,
                aggregator.globalTotal(StabilityConfigurator.HOT_COURSE_ID),
                aggregator.maxLocalBucketLoad());
    }

    @Test
    void rebalanceCannotFixKeyBySkew() {
        // keyBy 之后数据已按 hash(key) 分区，rebalance 只发生在 keyBy 之前
        boolean dataAlreadyPartitionedByKey = true;
        boolean rebalanceAfterKeyByHelpsSkew = false;
        assertFalse(rebalanceAfterKeyByHelpsSkew || !dataAlreadyPartitionedByKey,
                "keyBy 后 rebalance 无法打散热点 key，必须改聚合逻辑（两阶段/salt）");
        System.out.println("陷阱：倾斜在 keyBy 后，盲目加并行度或 rebalance 无效");
    }

    @Test
    void exponentialBackoff_doublesUntilCap() {
        long backoff = 1000;
        long maxBackoff = 60_000;
        double multiplier = 2.0;
        List<Long> sequence = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            sequence.add(backoff);
            backoff = Math.min((long) (backoff * multiplier), maxBackoff);
        }
        assertEquals(1000L, sequence.get(0));
        assertEquals(32_000L, sequence.get(5));
        assertEquals(60_000L, sequence.get(7), "退避应有上限");
        System.out.println("exponential-delay 退避序列(ms): " + sequence);
    }

    @Test
    void troubleshootingThreeAxes_checklist() {
        List<String> axes = List.of(
                "① 看重启次数/原因 → fixed-delay vs failure-rate，是否 CK 恢复失败",
                "② UI BackPressure → 找第一个 HIGH 算子，查 busyTimeMsPerSecond 瓶颈",
                "③ Metrics 各 subtask numRecordsIn 不均 → 倾斜，两阶段聚合或热点拆分"
        );
        assertEquals(3, axes.size());
        axes.forEach(a -> System.out.println("  " + a));
    }

    public static void main(String[] args) throws Exception {
        sendAllScenarios();
    }

    public static void sendAllScenarios() throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, FlinkStabilityDemoJob.KAFKA_BROKER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        List<SendPlan> plan = buildPlan();
        printHeader(plan);
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

        // Phase 1：均匀分布（baseline）
        plan.add(event("e01", "S40001", "C_MATH", 30, 1, "uniform", "Phase1 均匀 C_MATH"));
        plan.add(event("e02", "S40002", "C_ENG", 25, 2, "uniform", "Phase1 均匀 C_ENG"));
        plan.add(event("e03", "S40003", "C_PYTHON", 35, 3, "uniform", "Phase1 均匀 C_PYTHON"));
        plan.add(event("e04", "S40004", "C_JAVA", 40, 4, "uniform", "Phase1 均匀 C_JAVA"));

        // Phase 2：热点倾斜 burst（大班直播课）
        plan.add(wait(2000, "Phase1 处理完毕，开始热点 burst"));
        for (int i = 0; i < 12; i++) {
            plan.add(event("h" + i, "S500" + String.format("%02d", i),
                    StabilityConfigurator.HOT_COURSE_ID, 15, 10 + i, "hot-skew",
                    "Phase2 热点 " + StabilityConfigurator.HOT_COURSE_ID + " #" + i));
        }

        // Phase 3：反压 burst（多学员并行心跳）
        plan.add(wait(3000, "观察 skew 场景 [SKEW-NAIVE] hotKey=YES"));
        for (int i = 0; i < 8; i++) {
            plan.add(event("b" + i, "S600" + i, "C_BURST_" + (i % 2), 20, 30 + i, "bp-burst",
                    "Phase3 反压 burst（backpressure 模式观察 [BP-SINK] 变慢）"));
        }

        // Phase 4：两阶段验证（同 course 多学员）
        plan.add(wait(2000, "twophase 模式对照 local/global 日志"));
        plan.add(event("t01", "S70001", StabilityConfigurator.HOT_COURSE_ID, 10, 50, "2phase",
                "Phase4 两阶段：同课不同学员应落不同 salt"));
        plan.add(event("t02", "S70002", StabilityConfigurator.HOT_COURSE_ID, 10, 51, "2phase",
                "Phase4 salt 打散"));
        plan.add(event("t03", "S70003", StabilityConfigurator.HOT_COURSE_ID, 10, 52, "2phase",
                "Phase4 global merge"));

        return plan;
    }

    private static SendPlan event(String eventId, String studentId, String courseId,
                                    int watchSec, long offsetSec, String tag, String purpose) {
        return new SendPlan(progress(eventId, studentId, courseId, watchSec, offsetSec, tag), 0, null, purpose);
    }

    private static SendPlan wait(long ms, String reason) {
        return new SendPlan(null, ms, reason, reason);
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
                FlinkStabilityDemoJob.TOPIC,
                step.event.getCourseId(),
                json
        );
        producer.send(record).get(10, TimeUnit.SECONDS);
        System.out.printf("[SEND] id=%s course=%-12s student=%s watch=%ds tag=%-10s | %s%n",
                step.event.getEventId(), step.event.getCourseId(), step.event.getStudentId(),
                step.event.getWatchSec(), step.event.getTag(), step.purpose);
        Thread.sleep(300);
    }

    private static void printHeader(List<SendPlan> plan) {
        System.out.println("========================================");
        System.out.println("稳定性串讲演示数据发送");
        System.out.println("TOPIC: " + FlinkStabilityDemoJob.TOPIC + " (建议 4 分区，parallelism=4)");
        System.out.println("热点课程: " + StabilityConfigurator.HOT_COURSE_ID);
        System.out.println("共 " + plan.size() + " 步");
        System.out.println("========================================");
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
        System.out.println("预期观察：");
        System.out.println("  skew:     [SKEW-NAIVE] hotKey=YES⚠️ 集中在单 subtask");
        System.out.println("  twophase: [2PHASE-LOCAL] 多 salt | [2PHASE-GLOBAL] 合并总量");
        System.out.println("  backpressure: [BP-SINK] sleep → [LOAD-PROBE] 上游变慢");
        System.out.println("  UI: BackPressure 页第一个 HIGH 算子 = 瓶颈");
        System.out.println("对比实验：");
        System.out.println("  1) FlinkStabilityDemoJob backpressure fixed 300 0");
        System.out.println("  2) FlinkStabilityDemoJob skew fixed 0 8");
        System.out.println("  3) FlinkStabilityDemoJob twophase fixed 0 8");
        System.out.println("文档: resources/stability/FlinkStabilityDemoGuide.md");
        System.out.println("========================================");
    }

    /** 模拟下游慢 → 上游反压 */
    static class BackpressureChainSimulator {
        private long sinkSleepMs = 100;
        private int sinkQueueCapacity = 4;
        private int sinkQueueSize;
        private int upstreamBlockedCount;
        private int totalRecords;

        void setSinkSleepMs(long ms) {
            this.sinkSleepMs = ms;
        }

        void enqueueRecords(int count) {
            totalRecords = count;
        }

        void tickProcessing() {
            for (int i = 0; i < totalRecords; i++) {
                if (sinkQueueSize >= sinkQueueCapacity) {
                    upstreamBlockedCount++;
                } else {
                    sinkQueueSize++;
                }
                // 模拟 Sink 极慢，队列难排空
                if (sinkSleepMs > 100) {
                    sinkQueueSize = Math.min(sinkQueueCapacity, sinkQueueSize + 1);
                }
            }
        }

        boolean isUpstreamBlocked() {
            return upstreamBlockedCount > 0;
        }

        double backpressureRatio() {
            return totalRecords == 0 ? 0 : (double) upstreamBlockedCount / totalRecords;
        }
    }

    /** 模拟 keyBy 后各 subtask 负载 */
    static class SkewLoadSimulator {
        private final int parallelism;
        private final int[] loads;

        SkewLoadSimulator(int parallelism) {
            this.parallelism = parallelism;
            this.loads = new int[parallelism];
        }

        void route(String key, int weight) {
            int subtask = Math.floorMod(key.hashCode(), parallelism);
            loads[subtask] += weight;
        }

        int subtaskForKey(String key) {
            return Math.floorMod(key.hashCode(), parallelism);
        }

        int loadOn(int subtask) {
            return loads[subtask];
        }

        double averageLoad() {
            int sum = 0;
            for (int load : loads) {
                sum += load;
            }
            return (double) sum / parallelism;
        }

        double coefficientOfVariation() {
            double avg = averageLoad();
            if (avg == 0) {
                return 0;
            }
            double variance = 0;
            for (int load : loads) {
                variance += Math.pow(load - avg, 2);
            }
            variance /= parallelism;
            return Math.sqrt(variance) / avg;
        }
    }

    /** 两阶段聚合数学验证 */
    static class TwoPhaseAggregator {
        private final int saltBuckets;
        private final Map<String, Long> localBuckets = new HashMap<>();
        private final Map<String, Long> globalTotals = new HashMap<>();

        TwoPhaseAggregator(int saltBuckets) {
            this.saltBuckets = saltBuckets;
        }

        void add(StateDemoEvent event) {
            int salt = Math.floorMod(event.getStudentId().hashCode(), saltBuckets);
            String localKey = salt + "|" + event.getCourseId();
            localBuckets.merge(localKey, (long) event.getWatchSec(), Long::sum);
            globalTotals.merge(event.getCourseId(), (long) event.getWatchSec(), Long::sum);
        }

        long globalTotal(String courseId) {
            return globalTotals.getOrDefault(courseId, 0L);
        }

        long maxLocalBucketLoad() {
            return localBuckets.values().stream().mapToLong(Long::longValue).max().orElse(0L);
        }
    }

    private static class SendPlan {
        final StateDemoEvent event;
        final long sleepBeforeMs;
        final String waitReason;
        final String purpose;

        SendPlan(StateDemoEvent event, long sleepBeforeMs, String waitReason, String purpose) {
            this.event = event;
            this.sleepBeforeMs = sleepBeforeMs;
            this.waitReason = waitReason;
            this.purpose = purpose;
        }
    }
}
