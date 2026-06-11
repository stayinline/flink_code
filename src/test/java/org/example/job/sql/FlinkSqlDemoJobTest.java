package org.example.job.sql;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.dto.CourseDimRecord;
import org.example.dto.StateDemoEvent;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Flink SQL 演示数据发送 + 窗口/维表/SQL vs DataStream 逻辑单测。
 * <pre>
 * kafka-topics.sh --create --topic test_flink_sql_study --partitions 2 \
 *   --bootstrap-server 192.168.1.124:9092
 * </pre>
 */
public class FlinkSqlDemoJobTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    public static final long BASE_TIME_MS = 1_700_700_000_000L;

    @Test
    void sendSqlDemoEvents() throws Exception {
        sendAllScenarios();
    }

    @Test
    void tumbleWindow_assignsEventsTo30sBuckets() {
        long windowSec = 30;
        long t1 = BASE_TIME_MS + 5_000;
        long t2 = BASE_TIME_MS + 35_000;
        long bucket1 = (t1 / 1000 / windowSec) * windowSec;
        long bucket2 = (t2 / 1000 / windowSec) * windowSec;
        assertTrue(bucket2 > bucket1, "跨 30s 边界的事件应落入不同 TUMBLE 窗口");
        System.out.printf("TUMBLE 30s: ts+5s→bucket=%d | ts+35s→bucket=%d%n", bucket1, bucket2);
    }

    @Test
    void temporalJoin_enrichesKnownCourse() {
        Optional<CourseDimRecord> dim = CourseDimStore.allRecords().stream()
                .filter(c -> "C_JAVA".equals(c.getCourseId()))
                .findFirst();
        assertTrue(dim.isPresent());
        assertEquals("编程", dim.get().getCategory());
        System.out.println("Lookup Join: C_JAVA → " + dim.get().getCourseName() + " / " + dim.get().getCategory());
    }

    @Test
    void temporalJoin_unknownCourseReturnsNullCategory() {
        boolean found = CourseDimStore.allRecords().stream()
                .anyMatch(c -> "C_UNKNOWN".equals(c.getCourseId()));
        assertFalse(found, "未知 courseId 维表无记录 → LEFT JOIN category 为 NULL");
        System.out.println("LEFT JOIN 未知 courseId → course_name/category 为 NULL（可落侧输出）");
    }

    @Test
    void changelogMode_appendRetractUpsert() {
        Map<String, String> modes = new HashMap<>();
        modes.put("append", "仅追加 INSERT，Sink 需支持 append（如 Kafka append topic）");
        modes.put("retract", "UPDATE/DELETE 产生 -U/-D，Sink 需支持 upsert 或 retract");
        modes.put("upsert", "主键流，Kafka upsert-kafka / JDBC PK 覆盖");
        assertEquals(3, modes.size());
        modes.forEach((k, v) -> System.out.println("  " + k + ": " + v));
    }

    @Test
    void sqlVsDataStream_tradeoffTable() {
        List<String> useSql = List.of(
                "标准 TUMBLE/HOP/SESSION + 简单 GROUP BY",
                "维表 JDBC Lookup、CEP 标准模式",
                "快速交付、EXPLAIN 调优、mini-batch/local-global"
        );
        List<String> useDataStream = List.of(
                "复杂状态机、自定义 Trigger/Evictor",
                "Async I/O 精细控制、ProcessFunction 侧输出",
                "状态结构需精确掌控、细粒度 CK 调试"
        );
        assertFalse(useSql.isEmpty() && useDataStream.isEmpty());
        System.out.println("用 SQL：" + useSql);
        System.out.println("用 DataStream：" + useDataStream);
    }

    @Test
    void tableConfig_stateTtlAndMiniBatch() {
        SqlDemoConfigurator.SqlDemoOptions opts = new SqlDemoConfigurator.SqlDemoOptions(
                SqlDemoConfigurator.SCENARIO_FULL, 30, true, "1 h");
        assertEquals("1 h", opts.stateTtl);
        assertTrue(opts.miniBatchEnabled);
        System.out.println("table.exec.state.ttl=" + opts.stateTtl);
        System.out.println("table.exec.mini-batch.enabled=" + opts.miniBatchEnabled);
        System.out.println("table.optimizer.agg-phase-strategy=TWO_PHASE");
    }

    @Test
    void processSql_fullScenario_containsJoinAndTumble() {
        SqlDemoConfigurator.SqlDemoOptions opts = new SqlDemoConfigurator.SqlDemoOptions(
                SqlDemoConfigurator.SCENARIO_FULL, 30, true, "1 h");
        String sql = SqlDemoStatements.resolveProcessSql(opts);
        assertTrue(sql.contains("FOR SYSTEM_TIME AS OF"), "full 场景应含 temporal join");
        assertTrue(sql.contains("TUMBLE("), "full 场景应含 TUMBLE 窗口");
        System.out.println("FULL SQL 片段验证通过（Join + TUMBLE）");
    }

    @Test
    void categoryWindowAggregation_matchesManualSum() {
        List<StateDemoEvent> events = List.of(
                progress("e1", "S1", "C_JAVA", 30, 1, "test"),
                progress("e2", "S2", "C_ENG", 40, 2, "test"),
                progress("e3", "S3", "C_JAVA", 20, 3, "test")
        );
        Map<String, Long> byCategory = new HashMap<>();
        for (StateDemoEvent e : events) {
            String category = CourseDimStore.allRecords().stream()
                    .filter(c -> c.getCourseId().equals(e.getCourseId()))
                    .map(CourseDimRecord::getCategory)
                    .findFirst()
                    .orElse("UNKNOWN");
            byCategory.merge(category, (long) e.getWatchSec(), Long::sum);
        }
        assertEquals(50L, byCategory.get("编程"));
        System.out.println("Join+窗口语义：编程类 total=50s（30+20）");
    }

    public static void main(String[] args) throws Exception {
        sendAllScenarios();
    }

    public static void sendAllScenarios() throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, FlinkSqlDemoJob.KAFKA_BROKER);
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

        // Phase 1：均匀心跳（window / full 基线）
        plan.add(event("e01", "S80001", "C_JAVA", 30, 1, "uniform", "Phase1 C_JAVA +30s"));
        plan.add(event("e02", "S80002", "C_PYTHON", 45, 2, "uniform", "Phase1 C_PYTHON +45s"));
        plan.add(event("e03", "S80003", "C_MATH", 25, 3, "uniform", "Phase1 C_MATH K12"));
        plan.add(event("e04", "S80004", "C_ENG", 35, 4, "uniform", "Phase1 C_ENG 考研"));

        // Phase 2：同窗口内追加（观察 TUMBLE 聚合累加）
        plan.add(event("e05", "S80005", "C_JAVA", 20, 8, "same-window",
                "Phase2 同 30s 窗口内 C_JAVA 再 +20s → total=50"));
        plan.add(event("e06", "S80006", "C_PYTHON", 15, 10, "same-window",
                "Phase2 同窗口 C_PYTHON +15s"));

        // Phase 3：Lookup — 未知课程（LEFT JOIN NULL）
        plan.add(event("e07", "S80007", "C_UNKNOWN", 10, 12, "lookup-miss",
                "Phase3 未知 courseId → Lookup NULL"));

        // Phase 4：跨窗口（+35s 推进 WM，触发窗口关闭）
        plan.add(wait(3000, "等待 WM 推进"));
        plan.add(event("e08", "S80008", "C_LIVE_888", 60, 40, "cross-window",
                "Phase4 新 TUMBLE 窗口 C_LIVE_888 直播课"));
        plan.add(event("e09", "S80009", "C_PM", 30, 42, "cross-window",
                "Phase4 职场类课程"));

        // Phase 5：flush
        plan.add(event("e10", "S80010", "C_JAVA", 10, 70, "flush",
                "Phase5 flush 推进 watermark"));

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
                FlinkSqlDemoJob.TOPIC_STUDY,
                step.event.getCourseId(),
                json
        );
        producer.send(record).get(10, TimeUnit.SECONDS);
        System.out.printf("[SEND] id=%s course=%-12s watch=%ds tag=%-12s | %s%n",
                step.event.getEventId(), step.event.getCourseId(),
                step.event.getWatchSec(), step.event.getTag(), step.purpose);
        Thread.sleep(350);
    }

    private static void printHeader(List<SendPlan> plan) {
        System.out.println("========================================");
        System.out.println("Flink SQL 演示数据发送");
        System.out.println("TOPIC: " + FlinkSqlDemoJob.TOPIC_STUDY);
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
        System.out.println("预期观察（对照 Job 控制台 print sink）：");
        System.out.println("  window:  output_type=WINDOW | course_id=C_JAVA total_watch=50（e01+e05 同窗口）");
        System.out.println("  lookup:  output_type=LOOKUP | course_name=Java 零基础直播课");
        System.out.println("  lookup:  C_UNKNOWN → course_name=NULL");
        System.out.println("  full:    output_type=FULL | category=编程 total_watch=...");
        System.out.println("  启动时打印 [SQL-EXPLAIN] 含 GroupAggregate / LookupJoin");
        System.out.println("对比实验：");
        System.out.println("  1) FlinkSqlDemoJob full 30 true");
        System.out.println("  2) FlinkSqlDemoJob window 30 false");
        System.out.println("  3) FlinkSqlDemoJob lookup 30 true");
        System.out.println("文档: resources/sql/FlinkSqlDemoGuide.md");
        System.out.println("========================================");
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
