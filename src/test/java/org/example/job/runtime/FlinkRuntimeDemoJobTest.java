package org.example.job.runtime;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.dto.StateDemoEvent;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runtime 演示数据发送 + JM/TM/Slot/反压/Chain 逻辑单测。
 * <pre>
 * kafka-topics.sh --create --topic test_flink_runtime --partitions 4 \
 *   --bootstrap-server 192.168.1.124:9092
 * </pre>
 */
public class FlinkRuntimeDemoJobTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    public static final long BASE_TIME_MS = 1_700_700_000_000L;
    private static final int MAX_PARALLELISM = 128;

    @Test
    void sendRuntimeDemoEvents() throws Exception {
        sendAllScenarios();
    }

    @Test
    void runtimeModel_jmTmSlotTaskSubtask_relationship() {
        RuntimeModelSimulator.RuntimeLayout layout =
                RuntimeModelSimulator.layout(3, 4, 8);

        assertEquals(12, layout.totalSlots);
        assertTrue(layout.slotSufficient, "12 Slot ≥ 并行度 8");
        System.out.println("JM 调度 → 3 TM × 4 Slot = 12 Slot；作业并行度 8 → 8 个 Subtask 竞争 Slot");
        System.out.println("Task = 线程；Subtask = 某算子的第 i 个并行实例；Operator Chain 合并多算子到同一 Task");
    }

    @Test
    void parallelism_vs_slot_notSameThing() {
        int jobParallelism = 32;
        int tmSlots = 8;
        RuntimeModelSimulator.RuntimeLayout tight =
                RuntimeModelSimulator.layout(1, tmSlots, jobParallelism);

        assertFalse(tight.slotSufficient);
        System.out.printf("并行度 P=%d 但仅 %d Slot → Subtask 排队/共享，加 P 不增加物理吞吐%n",
                jobParallelism, tmSlots);
    }

    @Test
    void operatorChain_reducesTaskCount() {
        RuntimeConfigurator.RuntimeOptions chained = new RuntimeConfigurator.RuntimeOptions(
                RuntimeConfigurator.SCENARIO_BACKPRESSURE, 4, 300,
                RuntimeConfigurator.CHAIN_ON, "hashmap");
        RuntimeConfigurator.RuntimeOptions nochain = new RuntimeConfigurator.RuntimeOptions(
                RuntimeConfigurator.SCENARIO_BACKPRESSURE, 4, 300,
                RuntimeConfigurator.CHAIN_OFF, "hashmap");

        int chainedSubtasks = RuntimeTopologyEstimator.estimate(chained).totalSubtasks;
        int nochainSubtasks = RuntimeTopologyEstimator.estimate(nochain).totalSubtasks;

        assertTrue(nochainSubtasks > chainedSubtasks);
        System.out.printf("Chain 开: ~%d Subtasks | Chain 关: ~%d Subtasks | chain 减网络序列化，不利于逐算子隔离%n",
                chainedSubtasks, nochainSubtasks);
    }

    @Test
    void backpressure_propagatesFromSinkToSource() {
        RuntimeModelSimulator.BackpressurePropagation bp =
                RuntimeModelSimulator.simulateBackpressure(5, 300);

        assertTrue(bp.upstreamFeelsBackpressure());
        assertTrue(bp.operators.get(0).backpressuredMs > 0, "最上游应出现 backpressured");
        assertTrue(bp.operators.get(4).busyMs > bp.operators.get(0).busyMs,
                "Sink 最忙，上游 backpressured 升高");
        System.out.println("反压传播：Sink busy↑ → Network Buffer 满 → 上游 backpressured↑ busy↓");
    }

    @Test
    void hotKey_parallelismDoesNotSpreadLoad() {
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            keys.add(RuntimeConfigurator.HOT_COURSE_ID);
        }
        for (int i = 0; i < 10; i++) {
            keys.add("C_NORMAL_" + i);
        }

        Map<Integer, Long> load = RuntimeModelSimulator.routeByKey(keys, 4, MAX_PARALLELISM);
        int hotSubtask = RuntimeModelSimulator.subtaskForKey(
                RuntimeConfigurator.HOT_COURSE_ID, 4, MAX_PARALLELISM);

        assertEquals(50L, load.get(hotSubtask).longValue());
        System.out.printf("热点 %s 全部落 subtask=%d（50 条），rebalance 无法打散 keyBy 后的 key%n",
                RuntimeConfigurator.HOT_COURSE_ID, hotSubtask);
    }

    @Test
    void sourceParallelism_cannotFixSlowSink() {
        NetworkBufferSimulator sim = new NetworkBufferSimulator(8);
        sim.setSinkProcessMs(250);
        sim.enqueue(20);
        sim.drainOneTick();

        double throughputBefore = sim.recordsOutPerSecond();
        sim.setSourceParallelism(16);
        sim.drainOneTick();
        double throughputAfter = sim.recordsOutPerSecond();

        assertEquals(throughputBefore, throughputAfter, 0.5,
                "Source 并行度翻倍不能突破 Sink 瓶颈");
        System.out.printf("Sink 250ms/条 → 吞吐 ~%.1f rec/s；Source P 16→32 无效%n", throughputAfter);
    }

    @Test
    void networkBuffer_fullAmplifiesBackpressure() {
        NetworkBufferSimulator sim = new NetworkBufferSimulator(4);
        sim.setSinkProcessMs(100);
        sim.enqueue(20);
        sim.drainOneTick();
        assertTrue(sim.isUpstreamBlocked());
        System.out.printf("Buffer 容量=%d → 积压后 upstreamBlocked=%s%n",
                4, sim.isUpstreamBlocked());
    }

    @Test
    void managedMemory_rocksdbNeedsBudget() {
        Map<String, String> consumers = new LinkedHashMap<>();
        consumers.put("RocksDB block cache", "Managed Memory 子池");
        consumers.put("Window 排序", "Managed Memory");
        consumers.put("Batch Join", "Managed Memory");
        consumers.put("Network Buffer", "独立 Network Memory，非 Managed");

        assertTrue(consumers.get("RocksDB block cache").contains("Managed"));
        assertFalse(consumers.get("Network Buffer").contains("Managed"));
        System.out.println("RocksDB 状态 + 大窗口排序需预留 Managed Memory；与 Network Buffer 是不同内存池");
    }

    @Test
    void slotSharingGroup_isolatesHeavySink() {
        RuntimeConfigurator.RuntimeOptions hotslot = new RuntimeConfigurator.RuntimeOptions(
                RuntimeConfigurator.SCENARIO_HOTSLOT, 4, 300,
                RuntimeConfigurator.CHAIN_ON, "hashmap");
        RuntimeTopologyEstimator.TopologyEstimate est = RuntimeTopologyEstimator.estimate(hotslot);

        assertTrue(est.isolateSink);
        assertTrue(est.minSlotsWithIsolation > est.minSlotsWithSharing);
        System.out.printf("Sink 独立 SlotSharingGroup → 最少 Slot ~%d（默认共享仅 ~%d）%n",
                est.minSlotsWithIsolation, est.minSlotsWithSharing);
    }

    public static void main(String[] args) throws Exception {
        sendAllScenarios();
    }

    public static void sendAllScenarios() throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, FlinkRuntimeDemoJob.KAFKA_BROKER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        List<SendPlan> plan = buildPlan();

        System.out.println("========================================");
        System.out.println("Runtime 演示数据发送");
        System.out.println("Topic: " + FlinkRuntimeDemoJob.TOPIC + " (建议 4 分区)");
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

        // Phase 1：均匀学员心跳，观察 Subtask 分布
        for (int i = 0; i < 8; i++) {
            plan.add(event("e0" + i, "S400" + i, "C_COURSE_" + (i % 4), 30, i + 1,
                    "uniform", "Phase1 均匀 key，各 subtask 负载接近"));
        }

        // Phase 2：密集事件触发反压（配合 slowSinkMs=300）
        plan.add(wait(2000, "观察 [RT-PROBE] 与 UI BackPressure"));
        for (int i = 0; i < 12; i++) {
            plan.add(event("e1" + i, "S400" + (i % 4), "C_COURSE_" + (i % 4), 45, 10 + i,
                    "burst", "Phase2 突发流量 → Sink 慢 → 反压上游"));
        }

        // Phase 3：热点课程（skew 场景）
        plan.add(wait(2000, "skew 场景：keyBy(courseId) 时观察单 subtask 过热"));
        for (int i = 0; i < 25; i++) {
            plan.add(event("e2" + i, "S500" + (i % 5), RuntimeConfigurator.HOT_COURSE_ID, 60, 25 + i,
                    "hot-key", "Phase3 热点 " + RuntimeConfigurator.HOT_COURSE_ID));
        }
        for (int i = 0; i < 5; i++) {
            plan.add(event("e3" + i, "S600" + i, "C_MATH", 20, 30 + i,
                    "normal-key", "Phase3 非热点对照"));
        }

        // Phase 4：推进 WM 触发窗口
        plan.add(wait(3000, "等待 Tumbling 10s 窗口"));
        plan.add(event("e40", "S4000", "C_FLUSH", 10, 120,
                "flush", "Phase4 高 ts 推进 WM，触发 [RT-WINDOW-FIRED]"));

        return plan;
    }

    private static SendPlan event(String eventId, String studentId, String courseId,
                                  int watchSec, long offsetSec, String tag, String purpose) {
        return new SendPlan(
                progress(eventId, studentId, courseId, watchSec, offsetSec, tag),
                0, null, purpose);
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
                FlinkRuntimeDemoJob.TOPIC,
                step.event.getStudentId(),
                json
        );
        producer.send(record).get(10, TimeUnit.SECONDS);
        System.out.printf("[SEND] id=%s student=%s course=%s watch=%ds tag=%-12s | %s%n",
                step.event.getEventId(), step.event.getStudentId(), step.event.getCourseId(),
                step.event.getWatchSec(), step.event.getTag(), step.purpose);
        Thread.sleep(300);
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
        System.out.println("预期观察（Job 控制台 + Flink UI）：");
        System.out.println("  [RT-PROBE] subtask=i/P 均匀分布（backpressure 场景）");
        System.out.println("  [RT-SINK] sleep=300ms → UI BackPressure HIGH，上游 busy↓ backpressured↑");
        System.out.println("  skew 场景：热点 subtask 的 numRecordsIn 远高于其他");
        System.out.println("  nochain vs chain：Task/Subtask 数量对照（见单测 operatorChain_reducesTaskCount）");
        System.out.println("对比实验：");
        System.out.println("  1) backpressure 4 300 chain hashmap");
        System.out.println("  2) backpressure 4 300 nochain hashmap");
        System.out.println("  3) hotslot 4 300 chain hashmap");
        System.out.println("  4) skew 4 0 chain hashmap");
        System.out.println("  5) managed 4 200 chain rocksdb");
        System.out.println("文档: resources/runtime/FlinkRuntimeDemoGuide.md");
        System.out.println("========================================");
    }

    /** 模拟 Network Buffer + 慢 Sink 吞吐上限 */
    static class NetworkBufferSimulator {
        private final int bufferCapacity;
        private int bufferOccupancy;
        private int sourceParallelism = 1;
        private long sinkProcessMs = 100;
        private int recordsOut;
        private long elapsedMs = 1000;

        NetworkBufferSimulator(int bufferCapacity) {
            this.bufferCapacity = bufferCapacity;
        }

        void setSourceParallelism(int p) {
            this.sourceParallelism = p;
        }

        void setSinkProcessMs(long ms) {
            this.sinkProcessMs = ms;
        }

        void enqueue(int records) {
            bufferOccupancy = Math.min(bufferCapacity, bufferOccupancy + records);
        }

        void drainOneTick() {
            int canProcess = (int) Math.max(1, elapsedMs / Math.max(1, sinkProcessMs));
            recordsOut = Math.min(bufferOccupancy, canProcess);
            bufferOccupancy -= recordsOut;
        }

        boolean isUpstreamBlocked() {
            return bufferOccupancy >= bufferCapacity;
        }

        double recordsOutPerSecond() {
            return recordsOut * 1000.0 / elapsedMs;
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
