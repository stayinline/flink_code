package org.example.job.savepoint;

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
 * Savepoint 演示：Kafka 数据发送 + 恢复逻辑单测（UID / 状态兼容 / 并行度 / allowNonRestoredState）。
 * <p>
 * 运行前请创建 topic：
 * <pre>
 * kafka-topics.sh --create --topic test_flink_savepoint --partitions 4 --bootstrap-server 192.168.1.124:9092
 * </pre>
 */
public class FlinkSavepointDemoJobTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    public static final long BASE_TIME_MS = 1_700_500_000_000L;
    public static final String STUDENT_A = "S30001";
    public static final String STUDENT_B = "S30002";

    @Test
    void sendSavepointDemoEvents() throws Exception {
        sendAllScenarios();
    }

    @Test
    void checkpoint_vs_savepoint_lifecycleDiff() {
        Map<String, String> checkpoint = new LinkedHashMap<>();
        checkpoint.put("触发", "自动（间隔 / 对齐 barrier）");
        checkpoint.put("目的", "故障恢复");
        checkpoint.put("生命周期", "Flink 管理，默认保留最近 N 个");
        checkpoint.put("发布", "不推荐直接用于版本发布");

        Map<String, String> savepoint = new LinkedHashMap<>();
        savepoint.put("触发", "手动（stop --savepointPath / savepoint 命令）");
        savepoint.put("目的", "发布 / 迁移 / 回滚");
        savepoint.put("生命周期", "可长期保留在 HDFS/S3");
        savepoint.put("发布", "生产灰度升级标准路径");

        assertEquals("自动（间隔 / 对齐 barrier）", checkpoint.get("触发"));
        assertEquals("手动（stop --savepointPath / savepoint 命令）", savepoint.get("触发"));
        System.out.println("Checkpoint 适合宕机恢复；Savepoint 适合发布回滚 — 勿混用场景");
    }

    @Test
    void operatorUid_mustMatchForRestore() {
        SavepointRestoreValidator.SavepointMetadata sp = baselineSavepointMetadata();
        // 新拓扑把累计算子 UID 改错（非追加），模拟 broken-uid 发布
        SavepointRestoreValidator.JobTopology broken = new SavepointRestoreValidator.JobTopology(2, 128)
                .addOperator(SavepointOperatorUids.KAFKA_SOURCE)
                .addOperator(SavepointOperatorUids.COURSE_CREDIT_ACCUMULATOR_BROKEN)
                .addState(SavepointOperatorUids.COURSE_CREDIT_ACCUMULATOR_BROKEN,
                        CourseCreditAccumulateFunction.STATE_DESCRIPTOR_NAME,
                        "String", "Long");

        SavepointRestoreValidator.RestoreResult result =
                SavepointRestoreValidator.validate(sp, broken, false);

        assertFalse(result.success);
        assertTrue(result.errors.stream().anyMatch(e -> e.contains(SavepointOperatorUids.COURSE_CREDIT_ACCUMULATOR)));
        System.out.println("UID 变更 → 恢复失败: " + result.errors.get(0));
    }

    @Test
    void compatibleUpgrade_v1ToV2_sameUidAndDescriptor() {
        SavepointRestoreValidator.SavepointMetadata sp = baselineSavepointMetadata();
        SavepointRestoreValidator.JobTopology v2 = baselineTopology();

        SavepointRestoreValidator.RestoreResult result =
                SavepointRestoreValidator.validate(sp, v2, false);

        assertTrue(result.success, "V1→V2 同 UID + 同 MapState 描述符应可恢复");
        System.out.println("兼容升级 V1→V2：状态可挂载，V2 仅新增 promotion 业务逻辑");
    }

    @Test
    void stateTypeChange_breaksRestore() {
        SavepointRestoreValidator.SavepointMetadata sp = baselineSavepointMetadata();
        SavepointRestoreValidator.JobTopology breaking = new SavepointRestoreValidator.JobTopology(2, 128)
                .addOperator(SavepointOperatorUids.KAFKA_SOURCE)
                .addOperator(SavepointOperatorUids.COURSE_CREDIT_ACCUMULATOR)
                .addState(SavepointOperatorUids.COURSE_CREDIT_ACCUMULATOR,
                        CourseCreditAccumulateFunction.STATE_DESCRIPTOR_NAME,
                        "String", "CreditRecord"); // Long → POJO 不兼容

        SavepointRestoreValidator.RestoreResult result =
                SavepointRestoreValidator.validate(sp, breaking, false);

        assertFalse(result.success);
        assertTrue(result.errors.stream().anyMatch(e -> e.contains("状态不兼容")));
        System.out.println("状态值类型变更: " + result.errors.get(0));
    }

    @Test
    void parallelismRescale_redistributesKeyGroups() {
        int maxParallelism = 128;
        int oldP = 2;
        int newP = 4;

        int subtaskOld = SavepointRestoreValidator.resolveSubtask(STUDENT_A, oldP, maxParallelism);
        int subtaskNew = SavepointRestoreValidator.resolveSubtask(STUDENT_A, newP, maxParallelism);

        assertTrue(subtaskOld >= 0 && subtaskOld < oldP);
        assertTrue(subtaskNew >= 0 && subtaskNew < newP);
        System.out.printf("学员 %s：并行度 %d→%d，subtask %d→%d（keyGroup 重分配，状态总量不变）%n",
                STUDENT_A, oldP, newP, subtaskOld, subtaskNew);
    }

    @Test
    void allowNonRestoredState_whenStatefulOperatorRemoved() {
        SavepointRestoreValidator.SavepointMetadata sp = baselineSavepointMetadata();
        // 新拓扑删除了累计算子（仅保留 Source）
        SavepointRestoreValidator.JobTopology trimmed = new SavepointRestoreValidator.JobTopology(2, 128)
                .addOperator(SavepointOperatorUids.KAFKA_SOURCE);

        SavepointRestoreValidator.RestoreResult strict =
                SavepointRestoreValidator.validate(sp, trimmed, false);
        SavepointRestoreValidator.RestoreResult relaxed =
                SavepointRestoreValidator.validate(sp, trimmed, true);

        assertFalse(strict.success);
        assertTrue(relaxed.success);
        assertFalse(relaxed.warnings.isEmpty());
        System.out.println("删除有状态算子：strict 失败，-n allowNonRestored 可启动但丢失该算子状态");
    }

    @Test
    void statelessVsStateful_changeRisk() {
        String statelessRisk = "改 filter/map 逻辑：无状态，Savepoint 中无对应状态，发布风险低";
        String statefulRisk = "改 keyBy/process 或 UID/描述符：直接导致恢复失败或状态丢失";
        assertTrue(statelessRisk.contains("无状态"));
        assertTrue(statefulRisk.contains("恢复失败"));
        System.out.println("无状态算子变更: " + statelessRisk);
        System.out.println("有状态算子变更: " + statefulRisk);
    }

    @Test
    void v2PromotionBonus_appliesOnlyInV2() {
        CourseCreditAccumulateFunction v1 = new CourseCreditAccumulateFunction(
                CourseCreditAccumulateFunction.JobVersion.V1);
        CourseCreditAccumulateFunction v2 = new CourseCreditAccumulateFunction(
                CourseCreditAccumulateFunction.JobVersion.V2);

        int raw = 100;
        long v1Credit = invokeApply(v1, raw, "promotion-summer");
        long v2Credit = invokeApply(v2, raw, "promotion-summer");

        assertEquals(100L, v1Credit);
        assertEquals(110L, v2Credit);
        System.out.println("V2 promotion：100s → credited 110s（+10%），状态 schema 仍为 Long");
    }

    private static long invokeApply(CourseCreditAccumulateFunction fn, int watchSec, String tag) {
        try {
            var method = CourseCreditAccumulateFunction.class.getDeclaredMethod(
                    "applyVersionLogic", int.class, String.class);
            method.setAccessible(true);
            return (long) method.invoke(fn, watchSec, tag);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static SavepointRestoreValidator.SavepointMetadata baselineSavepointMetadata() {
        return new SavepointRestoreValidator.SavepointMetadata(2)
                .addOperator(SavepointOperatorUids.KAFKA_SOURCE)
                .addOperator(SavepointOperatorUids.COURSE_CREDIT_ACCUMULATOR)
                .addState(SavepointOperatorUids.COURSE_CREDIT_ACCUMULATOR,
                        CourseCreditAccumulateFunction.STATE_DESCRIPTOR_NAME,
                        "String", "Long");
    }

    private static SavepointRestoreValidator.JobTopology baselineTopology() {
        return new SavepointRestoreValidator.JobTopology(2, 128)
                .addOperator(SavepointOperatorUids.KAFKA_SOURCE)
                .addOperator(SavepointOperatorUids.COURSE_CREDIT_ACCUMULATOR)
                .addState(SavepointOperatorUids.COURSE_CREDIT_ACCUMULATOR,
                        CourseCreditAccumulateFunction.STATE_DESCRIPTOR_NAME,
                        "String", "Long");
    }

    public static void main(String[] args) throws Exception {
        sendAllScenarios();
    }

    public static void sendAllScenarios() throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, FlinkSavepointDemoJob.KAFKA_BROKER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        List<SendPlan> plan = buildPlan();

        System.out.println("========================================");
        System.out.println("Savepoint 演示数据发送");
        System.out.println("Topic: " + FlinkSavepointDemoJob.TOPIC + " (建议 4 分区，便于并行度 2→4 观察)");
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

        // Phase 1：V1 基线 — 多学员多课程，积累 MapState（Savepoint 前）
        plan.add(event("e01", STUDENT_A, "C_JAVA", 120, 1, "baseline",
                "Phase1 学员A Java +120s"));
        plan.add(event("e02", STUDENT_A, "C_PYTHON", 90, 2, "baseline",
                "Phase1 学员A Python +90s"));
        plan.add(event("e03", STUDENT_B, "C_MATH", 60, 3, "baseline",
                "Phase1 学员B 数学 +60s"));

        // Phase 2：继续积累，等待手动 Savepoint
        plan.add(wait(3000, "观察 [SP-STATE] mapEntries 增长；此时可 flink stop --savepointPath"));
        plan.add(event("e04", STUDENT_A, "C_JAVA", 30, 4, "pre-savepoint",
                "Phase2 Savepoint 前再 +30s Java"));
        plan.add(event("e05", STUDENT_B, "C_ENG", 45, 5, "pre-savepoint",
                "Phase2 学员B 英语 +45s"));

        // Phase 3：模拟从 Savepoint 恢复后继续消费（需先完成 stop + v2 启动）
        plan.add(wait(5000, "若已 V2 恢复，观察 version=V2 日志前缀"));
        plan.add(event("e06", STUDENT_A, "C_JAVA", 50, 6, "post-restore",
                "Phase3 恢复后 +50s，累计应延续 Savepoint 中状态"));
        plan.add(event("e07", STUDENT_A, "C_JAVA", 100, 7, "promotion-summer",
                "Phase3 V2 促销课 +100s（应 credited=110s）"));
        plan.add(event("e08", "S30003", "C_AI", 200, 8, "new-student",
                "Phase3 新学员，验证扩并行度后新 key 路由"));

        // Phase 4：批量扩状态（观察 Savepoint 体积 / 恢复耗时）
        plan.add(wait(2000, "观察并行度 4 时不同 subtask 分布"));
        for (int i = 0; i < 4; i++) {
            plan.add(event("e1" + i, "S300" + (10 + i), "C_BATCH_" + i, 80, 10 + i, "state-growth",
                    "Phase4 扩状态 → Savepoint 体积上升"));
        }

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

    private static void send(KafkaProducer<String, String> producer, SendPlan step) throws Exception {
        if (step.event == null) {
            return;
        }
        String json = MAPPER.writeValueAsString(step.event);
        ProducerRecord<String, String> record = new ProducerRecord<>(
                FlinkSavepointDemoJob.TOPIC,
                step.event.getStudentId(),
                json
        );
        producer.send(record).get(10, TimeUnit.SECONDS);
        System.out.printf("[SEND] id=%s student=%s course=%s watch=%ds tag=%-16s | %s%n",
                step.event.getEventId(), step.event.getStudentId(), step.event.getCourseId(),
                step.event.getWatchSec(), step.event.getTag(), step.purpose);
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
        System.out.println("预期观察：");
        System.out.println("  [SP-STATE] mapEntries 随 Phase1~2 增长；last total 在 Savepoint 后可延续");
        System.out.println("  Phase3 promotion 事件：V2 下 credited > raw（+10%）");
        System.out.println("  并行度 2→4：同一学员可能落到不同 subtask，但 total 语义不变");
        System.out.println("发布 playbook：");
        System.out.println("  " + SavepointCliCommands.stopWithSavepoint("<JOB_ID>", null));
        System.out.println("  " + SavepointCliCommands.runFromSavepoint(
                "target/flink_code-1.0-SNAPSHOT.jar",
                "file:///tmp/flink-savepoint-demo/savepoints/savepoint-xxx",
                false));
        System.out.println("文档: resources/savepoint/FlinkSavepointDemoGuide.md");
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
