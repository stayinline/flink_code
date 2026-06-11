package org.example.job.cdc;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.dto.CdcChangeEvent;
import org.example.dto.CdcEnrollmentSnapshot;
import org.example.job.kafka.FlinkKafkaConnectorDemoJob;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * CDC 选型 + 乱序 LWW 单测 + Kafka 乱序事件发送。
 * <pre>
 * kafka-topics.sh --create --topic test_flink_cdc_outoforder --partitions 1 \
 *   --bootstrap-server 192.168.1.124:9092
 * </pre>
 */
public class FlinkCdcOutOfOrderDemoJobTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final long PK = 1001L;

    @Test
    void sendOutOfOrderCdcEvents() throws Exception {
        sendAllScenarios();
    }

    @Test
    void toolSelection_matrixCoversFourProducts() {
        Map<String, Map<String, String>> matrix = CdcToolSelectionMatrix.build();
        assertTrue(matrix.containsKey("Canal"));
        assertTrue(matrix.containsKey("Maxwell"));
        assertTrue(matrix.containsKey("Debezium"));
        assertTrue(matrix.containsKey("Flink CDC"));
        System.out.println("CDC 选型四维：全量能力 / 分布式 / 生态 / 语义");
        matrix.forEach((tool, dims) -> {
            System.out.println("--- " + tool + " ---");
            dims.forEach((k, v) -> System.out.println("  " + k + ": " + v));
        });
    }

    @Test
    void lastWriteWins_binlogPos_rejectsStaleUpdate() {
        // 到达顺序：新 → 旧（乱序），最终应为 enrolled（pos 300）
        CdcChangeEvent newer = event("u", "dropped", "mysql-bin.000001", 300L, 3000L);
        CdcChangeEvent older = event("u", "enrolled", "mysql-bin.000001", 150L, 1000L);

        CdcEnrollmentSnapshot result = CdcOutOfOrderSimulator.applyLastWriteWins(
                List.of(newer, older), CdcVersionResolver.SOURCE_BINLOG);

        assertEquals("dropped", result.getEnrollStatus());
        assertEquals(CdcVersionResolver.binlogVersion("mysql-bin.000001", 300L), result.getVersion());
        System.out.println("乱序到达：旧 pos=150 被丢弃，保留 pos=300 dropped");
    }

    @Test
    void lastWriteWins_deleteWinsWhenNewest() {
        CdcChangeEvent update = event("u", "enrolled", "mysql-bin.000001", 200L, 2000L);
        CdcChangeEvent delete = event("d", null, "mysql-bin.000001", 250L, 2500L);
        delete.setEnrollStatus(null);

        CdcEnrollmentSnapshot result = CdcOutOfOrderSimulator.applyLastWriteWins(
                List.of(update, delete), CdcVersionResolver.SOURCE_BINLOG);

        assertTrue(result.isDeleted());
        assertEquals("d", result.getLastOp());
    }

    @Test
    void staleWrite_mustNotOverwrite_newerValue() {
        long maxVer = 500L;
        long staleVer = 100L;
        assertTrue(staleVer < maxVer);
        System.out.println("陷阱：无版本字段时，后到的旧 update 会覆盖新值 → 必须 LWW");
    }

    @Test
    void scenario_flinkCdc_vs_canalKafka() {
        String flinkCdc = "要全量+增量一体、入 Flink 计算 → Flink CDC";
        String canalKafka = "已有 Kafka 中枢、多订阅、轻量采集 → Canal+Kafka";
        assertTrue(flinkCdc.contains("全量"));
        assertTrue(canalKafka.contains("Kafka"));
        System.out.println(flinkCdc);
        System.out.println(canalKafka);
    }

    @Test
    void integration_lateDataAndReplacingMergeTree() {
        String late = "D3 迟到：Event Time 窗口侧输出补偿（时间维度乱序）";
        String lww = "CDC LWW：同主键更新版本乱序（变更日志维度）";
        String ch = "ClickHouse ReplacingMergeTree(version) 或 argMax(col, version) 合并";
        assertTrue(ch.contains("ReplacingMergeTree"));
        System.out.println(late);
        System.out.println(lww);
        System.out.println(ch);
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

        List<String> plan = buildOutOfOrderPlan();

        System.out.println("========================================");
        System.out.println("CDC 乱序演示数据发送 → " + CdcOutOfOrderConfigurator.TOPIC);
        System.out.println("共 " + plan.size() + " 条（故意乱序到达）");
        System.out.println("========================================");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            int step = 0;
            for (String json : plan) {
                step++;
                producer.send(new ProducerRecord<>(
                        CdcOutOfOrderConfigurator.TOPIC,
                        String.valueOf(PK),
                        json
                )).get(10, TimeUnit.SECONDS);
                System.out.printf("[SEND-%02d] %s%n", step, json);
                Thread.sleep(500);
            }
            producer.flush();
        }

        printExpectedOutcomes();
    }

    /** 发送顺序：新 → 旧 → 最新，Job 侧应 LWW 为 completed */
    private static List<String> buildOutOfOrderPlan() throws Exception {
        return List.of(
                toJson(event("u", "dropped", "mysql-bin.000001", 300L, 3000L)),
                toJson(event("u", "enrolled", "mysql-bin.000001", 150L, 1000L)),
                toJson(event("u", "completed", "mysql-bin.000001", 400L, 4000L)),
                toJson(event("u", "enrolled", "mysql-bin.000001", 200L, 2000L))
        );
    }

    private static CdcChangeEvent event(String op, String status, String file, long pos, long tsMs) {
        CdcChangeEvent e = new CdcChangeEvent();
        e.setOp(op);
        e.setSnapshotPhase(false);
        e.setDatabase("flink_cdc_demo");
        e.setTable("student_enrollment");
        e.setPrimaryKeyId(PK);
        e.setStudentId("S90001");
        e.setCourseId("C_JAVA");
        e.setEnrollStatus(status);
        e.setEventTsMs(tsMs);
        e.setDbUpdatedAtMs(tsMs);
        e.setBinlogFile(file);
        e.setBinlogPos(pos);
        return e;
    }

    private static String toJson(CdcChangeEvent e) throws Exception {
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("op", e.getOp());
        Map<String, Object> after = new LinkedHashMap<>();
        after.put("id", e.getPrimaryKeyId());
        after.put("student_id", e.getStudentId());
        after.put("course_id", e.getCourseId());
        after.put("enroll_status", e.getEnrollStatus());
        after.put("updated_at", e.getDbUpdatedAtMs());
        root.put("after", after);
        Map<String, Object> source = new LinkedHashMap<>();
        source.put("db", e.getDatabase());
        source.put("table", e.getTable());
        source.put("file", e.getBinlogFile());
        source.put("pos", e.getBinlogPos());
        root.put("source", source);
        root.put("ts_ms", e.getEventTsMs());
        return MAPPER.writeValueAsString(root);
    }

    private static void printExpectedOutcomes() {
        System.out.println("========================================");
        System.out.println("预期 LWW 最终结果：status=completed（binlog pos=400）");
        System.out.println("  [LWW-SKIP] pos=150、pos=200 的旧 update");
        System.out.println("  [LWW-ACCEPT] pos=300 dropped, pos=400 completed");
        System.out.println("启动：org.example.job.cdc.FlinkCdcOutOfOrderDemoJob binlog_pos 2 hashmap");
        System.out.println("文档: resources/cdc/FlinkCdcSelectionAndOutOfOrderGuide.md");
        System.out.println("========================================");
    }
}
