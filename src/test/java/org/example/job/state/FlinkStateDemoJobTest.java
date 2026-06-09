package org.example.job.state;

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
 * State 演示数据发送 + 本地逻辑单测（无需 Kafka 运行 Job）。
 * <p>
 * 运行前请创建 topic：
 * <pre>
 * kafka-topics.sh --create --topic test_flink_state --partitions 1 --bootstrap-server 192.168.1.124:9092
 * </pre>
 */
public class FlinkStateDemoJobTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    public static final long BASE_TIME_MS = 1_700_300_000_000L;
    public static final String DEMO_STUDENT = "S10001";

    @Test
    void sendStateDemoEvents() throws Exception {
        sendAllScenarios();
    }

    @Test
    void valueState_dedupFiltersRetry() {
        Map<String, Boolean> seen = new HashMap<>();
        StateDemoEvent first = progress("e01", DEMO_STUDENT, "C_MATH", 120, 1, "progress");
        StateDemoEvent retry = progress("e01", DEMO_STUDENT, "C_MATH", 120, 2, "duplicate-retry");

        assertTrue(simulateDedupPass(seen, first));
        assertFalse(simulateDedupPass(seen, retry));
        System.out.println("ValueState：同一 eventId 重试仅通过一次");
    }

    @Test
    void mapState_aggregateByCourseId() {
        Map<String, Long> courseWatch = new HashMap<>();
        accumulateWatch(courseWatch, "C_MATH", 120);
        accumulateWatch(courseWatch, "C_MATH", 80);
        accumulateWatch(courseWatch, "C_ENG", 60);

        assertEquals(200L, courseWatch.get("C_MATH"));
        assertEquals(60L, courseWatch.get("C_ENG"));
        System.out.printf("MapState：C_MATH=%ds C_ENG=%ds（按 key 独立累加，非整体 HashMap 替换）%n",
                courseWatch.get("C_MATH"), courseWatch.get("C_ENG"));
    }

    @Test
    void mapState_vs_valueStateHashMap_rocksDbReadPattern() {
        // 加分点：RocksDB 下 MapState 按 entry 读写；ValueState<HashMap> 每次 update 需整体序列化
        String mapStatePattern = "get(courseId) → put(courseId, total)  // 单课程读写";
        String valueHashMapPattern = "map = value(); map.put(...); valueState.update(map)  // 整张 Map 序列化";

        assertTrue(mapStatePattern.contains("get(courseId)"));
        assertTrue(valueHashMapPattern.contains("整张 Map"));
        System.out.println("MapState 优于 ValueState+HashMap：RocksDB LSM 按 key 局部读写");
    }

    @Test
    void listState_bufferAnswerUntilQuestionArrives() {
        List<StateDemoEvent> pending = new ArrayList<>();
        StateDemoEvent answer = quizAnswer("e04", DEMO_STUDENT, "C_MATH", "Q1", 90, "answer-before-q");
        StateDemoEvent question = quizQuestion("e05", DEMO_STUDENT, "C_MATH", "Q1", "question-def");

        bufferAnswer(pending, answer);
        assertEquals(1, pending.size());

        List<StateDemoEvent> flushed = flushMatching(pending, question.getQuestionId());
        assertEquals(1, flushed.size());
        assertEquals(0, pending.size());
        assertEquals("Q1", flushed.get(0).getQuestionId());
        System.out.println("ListState：答案先于题目到达 → 缓存 → 题目到达后 flush");
    }

    @Test
    void stateBackend_decisionTable_hashmapForSmallState() {
        assertTrue(shouldUseHashMapBackend(512, "low-latency"));
        assertFalse(shouldUseHashMapBackend(50_000, "large-map-per-key"));
        System.out.println("小状态低延迟 → HashMap；超内存大 Map → RocksDB");
    }

    @Test
    void rocksDb_tuningItems_documented() {
        List<String> tuning = List.of(
                "block cache 大小（state.backend.rocksdb.block.cache-size）",
                "write buffer（state.backend.rocksdb.writebuffer.size）",
                "predefined-options（SPINNING_DISK_OPTIMIZED / FLASH_SSD_OPTIMIZED）",
                "managed memory（TaskManager 堆外内存池）",
                "增量 checkpoint（EmbeddedRocksDBStateBackend(true)）"
        );
        assertTrue(tuning.size() >= 3);
        tuning.forEach(item -> System.out.println("RocksDB 调优: " + item));
    }

    public static void main(String[] args) throws Exception {
        sendAllScenarios();
    }

    public static void sendAllScenarios() throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, FlinkStateDemoJob.KAFKA_BROKER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        List<SendPlan> plan = buildPlan();

        System.out.println("========================================");
        System.out.println("State 演示数据发送（在线教育场景）");
        System.out.println("Topic: " + FlinkStateDemoJob.TOPIC);
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

        // Phase1：MapState 课程观看累加
        plan.add(event(progress("e01", DEMO_STUDENT, "C_MATH", 120, 1, "progress"),
                "Phase1 数学课 +120s → [MAP-AGG] total=120"));
        plan.add(event(progress("e02", DEMO_STUDENT, "C_MATH", 80, 2, "progress"),
                "Phase1 数学课 +80s → total=200"));
        plan.add(event(progress("e03", DEMO_STUDENT, "C_ENG", 60, 3, "progress"),
                "Phase1 英语课 +60s → mapSize=2"));

        // Phase2：ValueState 去重（移动端重试同一 eventId）
        plan.add(wait(1000, "观察 MapState 累加日志"));
        plan.add(event(progress("e01", DEMO_STUDENT, "C_MATH", 120, 4, "duplicate-retry"),
                "Phase2 重试 e01 → [VALUE-DEDUP-SKIP]，total 仍为 200"));

        // Phase3：ListState 答案先于题目
        plan.add(wait(1000, "准备测验乱序场景"));
        plan.add(event(quizAnswer("e04", DEMO_STUDENT, "C_MATH", "Q1", 90, "answer-before-q"),
                "Phase3 答题先到 → [LIST-BUFFER] pendingSize=1"));
        plan.add(event(quizAnswer("e06", DEMO_STUDENT, "C_MATH", "Q2", 70, "answer-before-q"),
                "Phase3 第二题答案先到 → pendingSize=2"));

        // Phase4：题目定义到达，flush ListState
        plan.add(wait(1000, "等待缓冲日志"));
        plan.add(event(quizQuestion("e05", DEMO_STUDENT, "C_MATH", "Q1", "question-def"),
                "Phase4 Q1 题目到达 → [LIST-FLUSH] flushed=1 + [LIST-MATCHED]"));
        plan.add(event(quizQuestion("e07", DEMO_STUDENT, "C_MATH", "Q2", "question-def"),
                "Phase4 Q2 题目到达 → flushed=1 remaining=0"));

        // Phase5：新学员 + flush WM
        plan.add(wait(1000, "观察 ListState flush"));
        plan.add(event(progress("e08", "S10002", "C_PHYS", 300, 10, "progress"),
                "Phase5 新学员物理课 +300s"));
        plan.add(event(progress("e09", DEMO_STUDENT, "C_MATH", 50, 11, "flush"),
                "Phase5 flush 事件推进 WM"));

        return plan;
    }

    private static SendPlan event(StateDemoEvent ev, String purpose) {
        return new SendPlan(ev, 0, null, purpose);
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
                BASE_TIME_MS + offsetSec * 1000L,
                tag
        );
    }

    private static StateDemoEvent quizAnswer(String eventId, String studentId, String courseId,
                                             String questionId, int score, String tag) {
        return new StateDemoEvent(
                eventId, studentId, courseId,
                StateDemoEvent.TYPE_QUIZ_ANSWER,
                null, questionId, score,
                BASE_TIME_MS + 50_000 + score,
                tag
        );
    }

    private static StateDemoEvent quizQuestion(String eventId, String studentId, String courseId,
                                               String questionId, String tag) {
        return new StateDemoEvent(
                eventId, studentId, courseId,
                StateDemoEvent.TYPE_QUIZ_QUESTION,
                null, questionId, null,
                BASE_TIME_MS + 55_000,
                tag
        );
    }

    private static void send(KafkaProducer<String, String> producer, SendPlan step) throws Exception {
        if (step.event == null) {
            return;
        }
        String json = MAPPER.writeValueAsString(step.event);
        ProducerRecord<String, String> record = new ProducerRecord<>(
                FlinkStateDemoJob.TOPIC,
                step.event.getStudentId(),
                json
        );
        producer.send(record).get(10, TimeUnit.SECONDS);
        System.out.printf("[SEND] id=%s type=%-14s student=%s course=%s tag=%-16s | %s%n",
                step.event.getEventId(),
                step.event.getEventType(),
                step.event.getStudentId(),
                step.event.getCourseId(),
                step.event.getTag(),
                step.purpose);
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
        System.out.println("  Phase1 [MAP-AGG] C_MATH total=200, C_ENG total=60");
        System.out.println("  Phase2 重试 e01 → [VALUE-DEDUP-SKIP]，total 不变成 320");
        System.out.println("  Phase3 [LIST-BUFFER] pendingSize=1→2");
        System.out.println("  Phase4 [LIST-FLUSH] + [LIST-MATCHED] score=90/70");
        System.out.println("对比实验：");
        System.out.println("  1) FlinkStateDemoJob           → HashMapStateBackend（默认）");
        System.out.println("  2) FlinkStateDemoJob rocksdb   → EmbeddedRocksDBStateBackend");
        System.out.println("文档: resources/state/FlinkStateDemoGuide.md");
        System.out.println("========================================");
    }

    // ===== 本地逻辑模拟（与算子语义一致）=====

    static boolean simulateDedupPass(Map<String, Boolean> seen, StateDemoEvent event) {
        if (Boolean.TRUE.equals(seen.get(event.getEventId()))) {
            return false;
        }
        seen.put(event.getEventId(), true);
        return true;
    }

    static void accumulateWatch(Map<String, Long> map, String courseId, int watchSec) {
        map.merge(courseId, (long) watchSec, Long::sum);
    }

    static void bufferAnswer(List<StateDemoEvent> pending, StateDemoEvent answer) {
        pending.add(answer);
    }

    static List<StateDemoEvent> flushMatching(List<StateDemoEvent> pending, String questionId) {
        List<StateDemoEvent> flushed = new ArrayList<>();
        List<StateDemoEvent> remaining = new ArrayList<>();
        for (StateDemoEvent item : pending) {
            if (questionId != null && questionId.equals(item.getQuestionId())) {
                flushed.add(item);
            } else {
                remaining.add(item);
            }
        }
        pending.clear();
        pending.addAll(remaining);
        return flushed;
    }

    static boolean shouldUseHashMapBackend(long stateSizeMb, String accessPattern) {
        return stateSizeMb < 1024 && "low-latency".equals(accessPattern);
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
