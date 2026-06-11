package org.example.job.exactlyonce;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.dto.StateDemoEvent;
import org.example.dto.StudySummaryRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 端到端 Exactly-Once 演示数据发送 + 2PC / 幂等逻辑单测。
 * <p>
 * 运行前请创建 topic：
 * <pre>
 * kafka-topics.sh --create --topic test_flink_exactlyonce --partitions 2 --bootstrap-server 192.168.1.124:9092
 * </pre>
 */
public class FlinkExactlyOnceDemoJobTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    public static final long BASE_TIME_MS = 1_700_500_000_000L;
    public static final String DEMO_STUDENT = "S30001";
    public static final String DEMO_COURSE = "C_FLINK_EO";

    @BeforeEach
    void clearStores() {
        DemoTwoPhaseCommitSink.COMMITTED_STORE.clear();
        DemoIdempotentClickHouseSink.IDEMPOTENT_STORE.clear();
    }

    @Test
    void sendExactlyOnceDemoEvents() throws Exception {
        sendAllScenarios();
    }

    @Test
    void twoPhaseCommit_normalLifecycle() {
        TwoPhaseCommitLifecycleSimulator sim = new TwoPhaseCommitLifecycleSimulator();

        sim.beginTransaction("txn-1");
        sim.invoke("txn-1", record("e01", 60));
        sim.invoke("txn-1", record("e02", 105));
        sim.onCheckpointSnapshot(); // preCommit
        assertFalse(sim.isCommitted("txn-1"), "preCommit 后数据仍不可对外见");

        sim.onCheckpointComplete(1); // commit
        assertTrue(sim.isCommitted("txn-1"), "notifyCheckpointComplete 后 commit");
        assertEquals(105, sim.committedTotal(DEMO_STUDENT, DEMO_COURSE));
        System.out.println("2PC 正常流：begin → invoke → preCommit(CK) → commit(notifyCheckpointComplete)");
    }

    @Test
    void twoPhaseCommit_abortOnCheckpointFailure() {
        TwoPhaseCommitLifecycleSimulator sim = new TwoPhaseCommitLifecycleSimulator();

        sim.beginTransaction("txn-2");
        sim.invoke("txn-2", record("e03", 30));
        sim.onCheckpointSnapshot();
        sim.onCheckpointAborted();
        assertTrue(sim.wasAborted("txn-2"), "CK 失败应 abort 当前 pending 事务");
        assertFalse(sim.isCommitted("txn-2"), "abort 后数据不可对外可见");

        sim.beginTransaction("txn-3");
        sim.invoke("txn-3", record("e04", 90));
        sim.onCheckpointSnapshot();
        sim.onCheckpointComplete(2);
        assertEquals(90, sim.committedTotal(DEMO_STUDENT, DEMO_COURSE));
        System.out.println("CK 失败后新事务重放：旧 txn abort，新 txn 正常 commit");
    }

    @Test
    void idempotentSink_dedupKeyOverwritesDuplicate() {
        String dedupKey = "2024-01-05|" + DEMO_STUDENT + "|" + DEMO_COURSE;
        StudySummaryRecord r1 = StudySummaryRecord.of("e10", DEMO_STUDENT, DEMO_COURSE, 60, "2024-01-05", "normal");
        StudySummaryRecord r2 = StudySummaryRecord.of("e10-retry", DEMO_STUDENT, DEMO_COURSE, 60, "2024-01-05", "duplicate-retry");

        DemoIdempotentClickHouseSink.IDEMPOTENT_STORE.put(dedupKey, r1);
        DemoIdempotentClickHouseSink.IDEMPOTENT_STORE.put(dedupKey, r2);

        assertEquals(1, DemoIdempotentClickHouseSink.IDEMPOTENT_STORE.size());
        assertEquals("duplicate-retry", DemoIdempotentClickHouseSink.IDEMPOTENT_STORE.get(dedupKey).getTag());
        System.out.println("幂等路线：同 dedupKey 重复写 → 主键覆盖，最终 1 行");
    }

    @Test
    void exactlyOnceSemantics_threeSegmentAnswer() {
        List<String> segments = List.of(
                "① Source 可重放：Kafka offset 写入 Checkpoint，故障恢复从上次 offset 重读",
                "② Flink 内部 EO：barrier 对齐 + 算子状态快照，失败回滚到最近成功 CK",
                "③ Sink 收口：2PC 事务 commit 在 notifyCheckpointComplete；或幂等 dedupKey 覆盖重复"
        );
        assertEquals(3, segments.size());
        segments.forEach(s -> System.out.println("  " + s));
        System.out.println("加分点：Exactly-Once 是状态语义，不等于不重复发送，靠下游事务/幂等收口");
    }

    @Test
    void kafkaTransactionTimeout_vs_checkpointInterval_trap() {
        long ckIntervalMs = ExactlyOnceConfigurator.DEFAULT_CHECKPOINT_INTERVAL_MS;
        long riskyTxnTimeoutMs = 15_000;
        long safeTxnTimeoutMs = 120_000;
        assertTrue(riskyTxnTimeoutMs < ckIntervalMs * 2,
                "15s < 2×10s 属于危险配置，进行中事务可能被 broker 中止");
        assertFalse(safeTxnTimeoutMs < ckIntervalMs * 2,
                "生产建议 transaction.timeout.ms ≥ 2×checkpoint 间隔");
        System.out.printf("陷阱：Kafka transaction.timeout.ms 建议 ≥ %dms（当前 CK 间隔 %dms）%n",
                ckIntervalMs * 2, ckIntervalMs);
    }

    public static void main(String[] args) throws Exception {
        sendAllScenarios();
    }

    public static void sendAllScenarios() throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, FlinkExactlyOnceDemoJob.KAFKA_BROKER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        List<SendPlan> plan = buildPlan();

        System.out.println("========================================");
        System.out.println("Exactly-Once 演示数据发送");
        System.out.println("TOPIC: " + FlinkExactlyOnceDemoJob.TOPIC);
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

        // Phase 1：正常学习心跳，积累状态供 CK + 2PC preCommit
        plan.add(event("e01", DEMO_STUDENT, DEMO_COURSE, 60, 1, "normal",
                "Phase1 +60s，等待第一次 CK → [2PC-PRE-COMMIT/COMMIT]"));
        plan.add(event("e02", DEMO_STUDENT, DEMO_COURSE, 45, 2, "normal",
                "Phase1 累计 105s"));
        plan.add(event("e03", "S30002", "C_MATH", 90, 3, "normal",
                "Phase1 第二学员，扩大状态"));

        // Phase 2：模拟 at-least-once 重投（同 eventId 语义重复，测幂等路线）
        plan.add(wait(12_000, "等待 CK#1 完成，观察 [2PC-COMMIT]"));
        plan.add(event("e02", DEMO_STUDENT, DEMO_COURSE, 45, 4, "duplicate-retry",
                "Phase2 重复 eventId=e02（Source 重放场景，2PC 不应双写）"));

        // Phase 3：多课程并行，观察 subtask 级事务
        plan.add(event("e04", DEMO_STUDENT, "C_PYTHON", 30, 5, "normal",
                "Phase3 同学员新课程"));
        plan.add(event("e05", DEMO_STUDENT, DEMO_COURSE, 20, 6, "normal",
                "Phase3 C_FLINK_EO 累计 +20s"));

        // Phase 4：burst 触发连续 CK，观察 abort / 新 txn
        plan.add(wait(12_000, "等待 CK#2，对照 pending commit 队列"));
        for (int i = 0; i < 4; i++) {
            plan.add(event("e1" + i, "S300" + (10 + i), "C_BURST", 15, 10 + i, "burst",
                    "Phase4 burst 扩事务缓冲"));
        }

        // Phase 5：flush
        plan.add(event("e20", DEMO_STUDENT, DEMO_COURSE, 10, 20, "flush",
                "Phase5 最终 +10s，对照 COMMITTED_STORE / CK-UPSERT"));

        return plan;
    }

    private static SendPlan event(String eventId, String studentId, String courseId,
                                  int watchSec, long offsetSec, String tag, String purpose) {
        return new SendPlan(
                progress(eventId, studentId, courseId, watchSec, offsetSec, tag),
                0,
                null,
                purpose
        );
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

    private static StudySummaryRecord record(String eventId, long totalSec) {
        return StudySummaryRecord.of(eventId, DEMO_STUDENT, DEMO_COURSE, totalSec, "2024-01-05", "test");
    }

    private static void send(KafkaProducer<String, String> producer, SendPlan step) throws Exception {
        if (step.event == null) {
            return;
        }
        String json = MAPPER.writeValueAsString(step.event);
        ProducerRecord<String, String> record = new ProducerRecord<>(
                FlinkExactlyOnceDemoJob.TOPIC,
                step.event.getStudentId(),
                json
        );
        producer.send(record).get(10, TimeUnit.SECONDS);
        System.out.printf("[SEND] id=%s student=%s course=%s watch=%ds tag=%-16s | %s%n",
                step.event.getEventId(), step.event.getStudentId(),
                step.event.getCourseId(), step.event.getWatchSec(),
                step.event.getTag(), step.purpose);
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
        System.out.println("预期观察（对照 Job 控制台）：");
        System.out.println("  [EO-AGG] 累计学习秒数");
        System.out.println("  2PC 模式：[2PC-BEGIN] → [2PC-INVOKE] → [2PC-PRE-COMMIT] → [2PC-COMMIT]");
        System.out.println("  幂等模式：[CK-UPSERT] duplicate-retry 覆盖同 dedupKey");
        System.out.println("  CK 失败时：[2PC-ABORT] 未 commit 事务丢弃");
        System.out.println("对比实验：");
        System.out.println("  1) FlinkExactlyOnceDemoJob 2pc 0 hashmap");
        System.out.println("  2) FlinkExactlyOnceDemoJob idempotent 0");
        System.out.println("  3) FlinkExactlyOnceDemoJob 2pc 200 hashmap  → commit 慢");
        System.out.println("文档: resources/exactlyonce/FlinkExactlyOnceDemoGuide.md");
        System.out.println("========================================");
    }

    /** 模拟 TwoPhaseCommitSinkFunction 与 Checkpoint 协调时序 */
    static class TwoPhaseCommitLifecycleSimulator {
        private final java.util.Map<String, java.util.List<StudySummaryRecord>> txnBuffer = new java.util.HashMap<>();
        private final java.util.Map<String, StudySummaryRecord> committed = new java.util.HashMap<>();
        private final java.util.Set<String> committedTxns = new java.util.HashSet<>();
        private final java.util.Set<String> abortedTxns = new java.util.HashSet<>();
        private String pendingCommitTxn;

        void beginTransaction(String txnId) {
            txnBuffer.put(txnId, new ArrayList<>());
        }

        void invoke(String txnId, StudySummaryRecord record) {
            txnBuffer.computeIfAbsent(txnId, k -> new ArrayList<>()).add(record);
        }

        void onCheckpointSnapshot() {
            pendingCommitTxn = txnBuffer.keySet().stream().reduce((a, b) -> b).orElse(null);
        }

        void onCheckpointComplete(long checkpointId) {
            if (pendingCommitTxn != null) {
                for (StudySummaryRecord r : txnBuffer.getOrDefault(pendingCommitTxn, List.of())) {
                    committed.put(r.getDedupKey(), r);
                }
                committedTxns.add(pendingCommitTxn);
                txnBuffer.remove(pendingCommitTxn);
                pendingCommitTxn = null;
            }
        }

        void onCheckpointAborted() {
            if (pendingCommitTxn != null) {
                abortedTxns.add(pendingCommitTxn);
                txnBuffer.remove(pendingCommitTxn);
                pendingCommitTxn = null;
            }
        }

        boolean isCommitted(String txnId) {
            return committedTxns.contains(txnId);
        }

        boolean wasAborted(String txnId) {
            return abortedTxns.contains(txnId);
        }

        long committedTotal(String studentId, String courseId) {
            String key = "2024-01-05|" + studentId + "|" + courseId;
            StudySummaryRecord r = committed.get(key);
            return r != null ? r.getTotalWatchSec() : 0;
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
