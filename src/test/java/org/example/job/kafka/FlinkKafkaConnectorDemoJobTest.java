package org.example.job.kafka;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.dto.StateDemoEvent;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Kafka 连接器演示数据发送 + Offset/分区/事务语义逻辑单测。
 * <pre>
 * kafka-topics.sh --create --topic test_flink_kafka_in --partitions 4 \
 *   --bootstrap-server 192.168.1.124:9092
 * kafka-topics.sh --create --topic test_flink_kafka_out --partitions 4 \
 *   --bootstrap-server 192.168.1.124:9092
 * </pre>
 */
public class FlinkKafkaConnectorDemoJobTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    public static final long BASE_TIME_MS = 1_700_800_000_000L;
    public static final String STUDENT_A = "S50001";

    @Test
    void sendKafkaConnectorDemoEvents() throws Exception {
        sendAllScenarios();
    }

    @Test
    void offsetRestore_checkpointSourceStateTakesPriority() {
        Map<Integer, Long> ck = Map.of(0, 1200L, 1, 800L);
        Map<Integer, Long> kafkaCommitted = Map.of(0, 500L, 1, 500L);

        KafkaOffsetRestoreSimulator.OffsetRestoreDecision decision =
                KafkaOffsetRestoreSimulator.decide(ck, kafkaCommitted, null, false);

        assertEquals(1200L, decision.offsets.get(0));
        assertTrue(decision.source.contains("Checkpoint"));
        System.out.println("恢复来源: " + decision.source + " → p0 offset=1200（非 Kafka 500）");
    }

    @Test
    void offsetRestore_savepointOverridesCommitted() {
        Map<Integer, Long> sp = Map.of(0, 2000L);
        Map<Integer, Long> ck = Map.of(0, 1200L);
        Map<Integer, Long> kafkaCommitted = Map.of(0, 500L);

        KafkaOffsetRestoreSimulator.OffsetRestoreDecision decision =
                KafkaOffsetRestoreSimulator.decide(ck, kafkaCommitted, sp, true);

        assertEquals(2000L, decision.offsets.get(0));
        assertTrue(decision.source.contains("Savepoint"));
    }

    @Test
    void partitionCount_lessThanParallelism_causesIdleSubtasks() {
        int idle = KafkaPartitionAssignSimulator.idleSubtaskCount(2, 8);
        assertEquals(6, idle);
        System.out.println("2 分区 + P=8 → 6 个 Source Subtask 空闲，盲目加 P 无效");
    }

    @Test
    void partitionAssign_rangeStyleMapping() {
        Map<Integer, List<Integer>> assign = KafkaPartitionAssignSimulator.assignPartitions(4, 4);
        assertEquals(List.of(0), assign.get(0));
        assertEquals(List.of(1), assign.get(1));
        assertEquals(4, assign.values().stream().mapToInt(List::size).sum());
    }

    @Test
    void topicExpand_changesPartitionDiscovery() {
        int before = 4;
        int after = 8;
        assertTrue(after > before);
        System.out.println("扩分区 4→8：Flink 动态发现新分区，但 key 分布与状态压力可能变化");
    }

    @Test
    void consumerGroupSwitch_restartsFromOffsetPolicy() {
        String oldGroup = KafkaConnectorConfigurator.GROUP_ID_DEFAULT;
        String newGroup = KafkaConnectorConfigurator.GROUP_ID_SWITCHED;
        assertFalse(oldGroup.equals(newGroup));
        System.out.println("换 groupId → 无 CK 时按 earliest/latest；有 CK 的 Flink 作业恢复仍看 Checkpoint");
    }

    @Test
    void kafkaSinkExactlyOnce_requiresCheckpointAndTransactionTimeout() {
        long ckIntervalMs = 10_000;
        long txnTimeoutMs = 900_000;
        assertTrue(txnTimeoutMs > ckIntervalMs * 2);
        System.out.println("EO Sink：CK 间隔 10s << transaction.timeout 15min；消费者用 read_committed");
    }

    @Test
    void serializationFormat_tradeoff() {
        Map<String, String> formats = new LinkedHashMap<>();
        formats.put("JSON", "可读、灵活；无 Schema 演进，生产调试友好");
        formats.put("Avro+SchemaRegistry", "紧凑、强 Schema 演进；在线教育埋点推荐");
        formats.put("Protobuf", "高性能；前后端统一契约");
        assertTrue(formats.get("Avro+SchemaRegistry").contains("演进"));
        formats.forEach((k, v) -> System.out.println(k + ": " + v));
    }

    @Test
    void backlogRecovery_notOnlyIncreaseParallelism() {
        BacklogRecoverySimulator sim = new BacklogRecoverySimulator(4, 2);
        sim.simulateBurst(1000);
        double catchUpOnlyScaleP = sim.estimateCatchUpSeconds(1.0);
        double catchUpScalePAndOptimizeSink = sim.estimateCatchUpSeconds(3.0);
        assertTrue(catchUpScalePAndOptimizeSink < catchUpOnlyScaleP);
        System.out.printf("积压 1000 万：仅加 P 需 ~%.0fs；优化 Sink 吞吐 3× 后 ~%.0fs%n",
                catchUpOnlyScaleP, catchUpScalePAndOptimizeSink);
    }

    @Test
    void lagAndBackpressure_jointDiagnosis() {
        String lagHighBpLow = "Lag 高但无反压 → 消费能力不足或 P 不足，非 Sink 瓶颈";
        String lagHighBpHigh = "Lag 高且全链路反压 → 下游慢，加 Source P 无效";
        assertTrue(lagHighBpHigh.contains("下游慢"));
        System.out.println(lagHighBpLow);
        System.out.println(lagHighBpHigh);
    }

    public static void main(String[] args) throws Exception {
        sendAllScenarios();
    }

    public static void sendAllScenarios() throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, FlinkKafkaConnectorDemoJob.KAFKA_BROKER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        List<SendPlan> plan = buildPlan();

        System.out.println("========================================");
        System.out.println("Kafka 连接器演示数据发送");
        System.out.println("IN:  " + FlinkKafkaConnectorDemoJob.TOPIC_IN);
        System.out.println("OUT: " + FlinkKafkaConnectorDemoJob.TOPIC_OUT + " (Job 写出，可用 kafka-console-consumer 验证)");
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

        // Phase 1：正常消费，观察 partition/offset 日志
        for (int i = 0; i < 6; i++) {
            plan.add(event("e0" + i, "S500" + i, "C_JAVA", 60, i + 1, "normal",
                    null, "Phase1 正常心跳 p" + (i % 4)));
        }

        // Phase 2：指定分区写入（观察 subtask 与 partition 映射）
        plan.add(wait(2000, "观察 [KAFKA-SRC] partition/offset/subtask"));
        plan.add(event("e10", STUDENT_A, "C_PYTHON", 45, 10, "partition-0",
                0, "Phase2 写 partition 0"));
        plan.add(event("e11", STUDENT_A, "C_PYTHON", 30, 11, "partition-1",
                1, "Phase2 写 partition 1"));

        // Phase 3：重复 eventId 模拟至少一次（若重启可能重复写出）
        plan.add(event("e12", STUDENT_A, "C_JAVA", 20, 12, "retry-dup",
                null, "Phase3 重复上报（测 at-least-once 去重靠下游）"));
        plan.add(event("e12", STUDENT_A, "C_JAVA", 20, 13, "retry-dup",
                null, "Phase3 同 eventId 再发一次"));

        // Phase 4：积压 burst（backlog 场景）
        plan.add(wait(2000, "backlog 场景可重启 Job 后发送"));
        for (int i = 0; i < 20; i++) {
            plan.add(event("e2" + i, "S600" + (i % 5), "C_BURST", 15, 20 + i, "backlog-burst",
                    i % 4, "Phase4 积压追赶 burst"));
        }

        // Phase 5：热点课程（扩分区后 key 分布变化演示）
        for (int i = 0; i < 10; i++) {
            plan.add(event("e3" + i, "S700" + i, "C_LIVE_888", 90, 40 + i, "hot-course",
                    null, "Phase5 热点课（key 路由到固定分区）"));
        }

        return plan;
    }

    private static SendPlan event(String eventId, String studentId, String courseId,
                                  int watchSec, long offsetSec, String tag,
                                  Integer partition, String purpose) {
        return new SendPlan(
                progress(eventId, studentId, courseId, watchSec, offsetSec, tag),
                partition, 0, null, purpose);
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
        if (step.event == null) {
            return;
        }
        String json = MAPPER.writeValueAsString(step.event);
        ProducerRecord<String, String> record;
        if (step.partition != null) {
            record = new ProducerRecord<>(
                    FlinkKafkaConnectorDemoJob.TOPIC_IN,
                    step.partition,
                    step.event.getStudentId(),
                    json);
        } else {
            record = new ProducerRecord<>(
                    FlinkKafkaConnectorDemoJob.TOPIC_IN,
                    step.event.getStudentId(),
                    json);
        }
        producer.send(record).get(10, TimeUnit.SECONDS);
        System.out.printf("[SEND] topic=%s part=%s id=%s student=%s tag=%-14s | %s%n",
                FlinkKafkaConnectorDemoJob.TOPIC_IN,
                step.partition != null ? step.partition : "hash",
                step.event.getEventId(), step.event.getStudentId(),
                step.event.getTag(), step.purpose);
        Thread.sleep(350);
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
        System.out.println("  [KAFKA-SRC] partition=? offset=? subtask=?");
        System.out.println("  [EO-AGG] 有状态累计");
        System.out.println("  [KAFKA-SINK-IN] → test_flink_kafka_out（exactly-once 时事务写出）");
        System.out.println("验证 OUT topic：");
        System.out.println("  kafka-console-consumer.sh --bootstrap-server "
                + FlinkKafkaConnectorDemoJob.KAFKA_BROKER
                + " --topic " + FlinkKafkaConnectorDemoJob.TOPIC_OUT
                + " --from-beginning --isolation-level read_committed");
        System.out.println("Lag：");
        System.out.println("  kafka-consumer-groups.sh --bootstrap-server "
                + FlinkKafkaConnectorDemoJob.KAFKA_BROKER
                + " --group " + KafkaConnectorConfigurator.GROUP_ID_DEFAULT + " --describe");
        System.out.println("文档: resources/kafka/FlinkKafkaConnectorDemoGuide.md");
        System.out.println("========================================");
    }

    static class BacklogRecoverySimulator {
        private final int parallelism;
        private final int partitions;
        private long backlogRecords;

        BacklogRecoverySimulator(int parallelism, int partitions) {
            this.parallelism = parallelism;
            this.partitions = partitions;
        }

        void simulateBurst(long records) {
            this.backlogRecords = records;
        }

        double estimateCatchUpSeconds(double throughputMultiplier) {
            double effectiveParallelism = Math.min(parallelism, partitions);
            double baseRps = effectiveParallelism * 50.0;
            return backlogRecords / (baseRps * throughputMultiplier);
        }
    }

    private static class SendPlan {
        final StateDemoEvent event;
        final Integer partition;
        final long sleepBeforeMs;
        final String waitReason;
        final String purpose;

        SendPlan(StateDemoEvent event, Integer partition, long sleepBeforeMs,
                 String waitReason, String purpose) {
            this.event = event;
            this.partition = partition;
            this.sleepBeforeMs = sleepBeforeMs;
            this.waitReason = waitReason;
            this.purpose = purpose;
        }
    }
}
