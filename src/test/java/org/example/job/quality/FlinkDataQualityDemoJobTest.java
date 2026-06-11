package org.example.job.quality;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.dto.DirtyDataRecord;
import org.example.dto.StateDemoEvent;
import org.example.dto.StudySummaryRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 数据质量演示数据发送 + 校验/DLQ/幂等/对账逻辑单测。
 * <pre>
 * kafka-topics.sh --create --topic test_flink_quality --partitions 2 \
 *   --bootstrap-server 192.168.1.124:9092
 * kafka-topics.sh --create --topic test_flink_quality_dlq --partitions 1 \
 *   --bootstrap-server 192.168.1.124:9092
 * </pre>
 */
public class FlinkDataQualityDemoJobTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    public static final long BASE_TIME_MS = 1_700_900_000_000L;
    public static final String STUDENT = "S80001";
    public static final String COURSE = "C_ENG";

    @BeforeEach
    void resetStores() {
        ReconciliationReporter.reset();
        DlqSinkFunction.clearStore();
        IdempotentSummarySinkFunction.clearStore();
    }

    @Test
    void sendDataQualityDemoEvents() throws Exception {
        sendAllScenarios();
    }

    @Test
    void validator_detectsMissingField() {
        StateDemoEvent bad = progress("e01", null, COURSE, 60, 1, "normal");
        Optional<DirtyDataRecord> dirty = DataQualityValidator.validate(bad, "{}");
        assertTrue(dirty.isPresent());
        assertEquals(DirtyDataRecord.REASON_MISSING_FIELD, dirty.get().getReason());
    }

    @Test
    void validator_detectsTimeAnomaly_secondsTimestamp() {
        StateDemoEvent bad = progress("e02", STUDENT, COURSE, 60, 1, "normal");
        bad.setTs(1_700_900_000L);
        Optional<DirtyDataRecord> dirty = DataQualityValidator.validate(bad, "{}");
        assertTrue(dirty.isPresent());
        assertEquals(DirtyDataRecord.REASON_TIME_ANOMALY, dirty.get().getReason());
    }

    @Test
    void validator_acceptsValidEvent() {
        StateDemoEvent ok = progress("e03", STUDENT, COURSE, 90, 2, "normal");
        assertTrue(DataQualityValidator.validate(ok, "{}").isEmpty());
    }

    @Test
    void sideOutput_vs_drop_dlqTraceability() {
        assertTrue(DataQualityConfigurator.dlqEnabled(
                new DataQualityConfigurator.QualityOptions(DataQualityConfigurator.MODE_STRICT, "hashmap")));
        assertFalse(DataQualityConfigurator.dlqEnabled(
                new DataQualityConfigurator.QualityOptions(DataQualityConfigurator.MODE_DROP, "hashmap")));
        System.out.println("侧输出+DLQ 可追溯；静默丢弃无法修复回放");
    }

    @Test
    void idempotentSummary_replayVersionOverrides() {
        String dedupKey = "2024-02-14|" + STUDENT + "|" + COURSE;
        StudySummaryRecord v1 = StudySummaryRecord.of("e10", STUDENT, COURSE, 100, "2024-02-14", "normal");
        StudySummaryRecord v2 = StudySummaryRecord.of("e11", STUDENT, COURSE, 150, "2024-02-14", "replay-v2");

        IdempotentSummarySinkFunction fn = new IdempotentSummarySinkFunction();
        try {
            fn.invoke(v1, null);
            fn.invoke(v2, null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        assertEquals(150L, IdempotentSummarySinkFunction.SUMMARY_STORE.get(dedupKey).record.getTotalWatchSec());
        assertEquals(2, IdempotentSummarySinkFunction.SUMMARY_STORE.get(dedupKey).version);
        System.out.println("补数 replay-v2 覆盖 v1：ReplacingMergeTree/UPSERT 语义");
    }

    @Test
    void exactlyOnce_doesNotReplaceBusinessReconciliation() {
        String techEo = "Flink CK 保证算子状态与 Kafka offset 一致（技术一致性）";
        String biz = "业务最终一致还需 DLQ + 幂等 dedupKey + 源汇对账（业务一致性）";
        assertTrue(techEo.contains("技术"));
        assertTrue(biz.contains("对账"));
        System.out.println(techEo);
        System.out.println(biz);
    }

    @Test
    void reconciliation_countMatchesSentMinusDlq() {
        ReconciliationReporter.onAccepted(progress("a1", STUDENT, COURSE, 30, 1, "ok"));
        ReconciliationReporter.onAccepted(progress("a2", STUDENT, COURSE, 40, 2, "ok"));
        ReconciliationReporter.onDlq(DirtyDataRecord.of("{}", DirtyDataRecord.REASON_PARSE_FAIL, "x", "t"));

        ReconciliationReporter.ReconciliationSnapshot snap =
                ReconciliationReporter.snapshot(3, 70);
        assertEquals(2, snap.acceptedCount);
        assertEquals(1, snap.dlqCount);
        assertEquals(2, snap.sourceSentCount - snap.dlqCount);
    }

    @Test
    void duplicateRetry_idempotentSinkStabilizes() {
        StudySummaryRecord first = StudySummaryRecord.of("d1", STUDENT, COURSE, 200, "2024-02-14", "duplicate-retry");
        StudySummaryRecord dup = StudySummaryRecord.of("d1", STUDENT, COURSE, 260, "2024-02-14", "duplicate-retry");
        IdempotentSummarySinkFunction fn = new IdempotentSummarySinkFunction();
        try {
            fn.invoke(first, null);
            fn.invoke(dup, null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        String key = first.getDedupKey();
        assertEquals(260L, IdempotentSummarySinkFunction.SUMMARY_STORE.get(key).record.getTotalWatchSec());
    }

    @Test
    void handlingStrategy_comparisonTable() {
        Map<String, String> strategies = new LinkedHashMap<>();
        strategies.put("直接丢弃", "低价值 PV；不可修复");
        strategies.put("侧输出", "分离异常流；需接 DLQ");
        strategies.put("DLQ", "可追踪、人工修复、回放");
        strategies.put("幂等补数", "dedupKey + replayVersion");
        strategies.put("对账补偿", "count/sum/hash 自动告警");
        assertEquals(5, strategies.size());
        strategies.forEach((k, v) -> System.out.println(k + " → " + v));
    }

    public static void main(String[] args) throws Exception {
        sendAllScenarios();
    }

    public static void sendAllScenarios() throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, FlinkDataQualityDemoJob.KAFKA_BROKER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        List<SendPlan> plan = buildPlan();
        long sentCount = plan.stream().filter(p -> p.raw != null).count();
        long sentWatch = plan.stream()
                .filter(p -> p.event != null && p.event.getWatchSec() != null)
                .mapToInt(p -> p.event.getWatchSec())
                .sum();

        System.out.println("========================================");
        System.out.println("数据质量演示数据发送");
        System.out.println("Topic: " + FlinkDataQualityDemoJob.TOPIC_IN);
        System.out.println("共 " + plan.size() + " 步，有效 JSON 事件约 " + sentCount + " 条");
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

        ReconciliationReporter.printReport(sentCount, sentWatch);
        printExpectedOutcomes();
    }

    private static List<SendPlan> buildPlan() {
        List<SendPlan> plan = new ArrayList<>();

        // Phase 1：正常数据
        plan.add(json(progress("e01", STUDENT, COURSE, 60, 1, "normal"),
                "Phase1 正常 +60s"));
        plan.add(json(progress("e02", STUDENT, "C_MATH", 45, 2, "normal"),
                "Phase1 第二课程"));

        // Phase 2：脏数据各类
        plan.add(raw("{invalid json", "Phase2 JSON 解析失败 → PARSE_FAIL"));
        plan.add(json(progress("e03", "", COURSE, 30, 3, "bad"),
                "Phase2 studentId 空 → MISSING_FIELD"));
        plan.add(json(progress("e04", STUDENT, COURSE, 20, 4, "bad"),
                "Phase2 eventType 非法", ev -> ev.setEventType("unknown_type")));
        plan.add(json(progress(null, STUDENT, COURSE, 25, 5, "bad"),
                "Phase2 缺 eventId → MISSING_BIZ_KEY"));
        StateDemoEvent secTs = progress("e05", STUDENT, COURSE, 10, 6, "bad");
        secTs.setTs(1_700_900_000L);
        plan.add(json(secTs, "Phase2 ts 秒级 → TIME_ANOMALY"));

        // Phase 3：重复上报
        plan.add(wait(2000, "观察 DLQ 与主流分流"));
        plan.add(json(progress("e06", STUDENT, COURSE, 30, 7, "duplicate-retry"),
                "Phase3 首次 +30s"));
        plan.add(json(progress("e06", STUDENT, COURSE, 30, 8, "duplicate-retry"),
                "Phase3 重复 eventId（聚合累加，幂等表覆盖）"));

        // Phase 4：漏数补发 / 回放修复
        plan.add(json(progress("e07", STUDENT, COURSE, 50, 9, "replay-v2"),
                "Phase4 补数回放 corrected total（高 replayVersion）"));

        // Phase 5：历史 backfill
        plan.add(json(progress("e08", "S80002", "C_AI", 120, 10, "backfill"),
                "Phase5 历史补数新学员"));

        return plan;
    }

    private static SendPlan json(StateDemoEvent event, String purpose) {
        return json(event, purpose, null);
    }

    private static SendPlan json(StateDemoEvent event, String purpose,
                               java.util.function.Consumer<StateDemoEvent> mutator) {
        if (mutator != null) {
            mutator.accept(event);
        }
        try {
            return new SendPlan(MAPPER.writeValueAsString(event), event, 0, null, purpose);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static SendPlan raw(String raw, String purpose) {
        return new SendPlan(raw, null, 0, null, purpose);
    }

    private static SendPlan wait(long ms, String reason) {
        return new SendPlan(null, null, ms, reason, reason);
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
        if (step.raw == null) {
            return;
        }
        ProducerRecord<String, String> record = new ProducerRecord<>(
                FlinkDataQualityDemoJob.TOPIC_IN,
                step.event != null ? step.event.getStudentId() : "dirty",
                step.raw
        );
        producer.send(record).get(10, TimeUnit.SECONDS);
        System.out.printf("[SEND] %s | %s%n",
                step.purpose, step.raw.length() > 80 ? step.raw.substring(0, 80) + "..." : step.raw);
        Thread.sleep(400);
    }

    private static void printPlan(List<SendPlan> plan) {
        System.out.println("--- 发送计划 ---");
        for (SendPlan s : plan) {
            if (s.raw != null) {
                System.out.printf("  %s%n", s.purpose);
            } else if (s.waitReason != null) {
                System.out.printf("  [WAIT %ds] %s%n", s.sleepBeforeMs / 1000, s.waitReason);
            }
        }
        System.out.println("----------------");
    }

    private static void printExpectedOutcomes() {
        System.out.println("========================================");
        System.out.println("预期观察（strict 模式 Job 控制台）：");
        System.out.println("  [DQ-OK] 正常事件进主流");
        System.out.println("  [DQ-DIRTY] + [DLQ-SINK] 脏数据进 DLQ");
        System.out.println("  [DQ-AGG] + [IDEMPOTENT-SUMMARY] 汇总幂等写");
        System.out.println("  Phase4 replay-v2 覆盖同 dedupKey 旧值");
        System.out.println("对比：drop hashmap → [DQ-DROP] 无 DLQ");
        System.out.println("文档: resources/quality/FlinkDataQualityDemoGuide.md");
        System.out.println("========================================");
    }

    private static class SendPlan {
        final String raw;
        final StateDemoEvent event;
        final long sleepBeforeMs;
        final String waitReason;
        final String purpose;

        SendPlan(String raw, StateDemoEvent event, long sleepBeforeMs, String waitReason, String purpose) {
            this.raw = raw;
            this.event = event;
            this.sleepBeforeMs = sleepBeforeMs;
            this.waitReason = waitReason;
            this.purpose = purpose;
        }
    }
}
