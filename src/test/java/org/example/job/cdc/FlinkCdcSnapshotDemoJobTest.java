package org.example.job.cdc;

import org.example.dto.CdcChangeEvent;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * CDC 增量快照：水位合并单测 + MySQL 灌数（需可连 {@link CdcDemoConfigurator#jdbcUrl()}）。
 */
public class FlinkCdcSnapshotDemoJobTest {

    @Test
    void incrementalSnapshot_chunkWatermarkMerge_consistent() {
        Map<Long, IncrementalSnapshotSimulator.Row> table = new HashMap<>();
        table.put(1L, new IncrementalSnapshotSimulator.Row(1, "S1", "enrolled"));
        table.put(2L, new IncrementalSnapshotSimulator.Row(2, "S2", "enrolled"));
        table.put(3L, new IncrementalSnapshotSimulator.Row(3, "S3", "enrolled"));

        List<IncrementalSnapshotSimulator.BinlogEvent> binlog = List.of(
                // chunk [1,2] 快照期间 id=2 被更新
                new IncrementalSnapshotSimulator.BinlogEvent(50, "u",
                        new IncrementalSnapshotSimulator.Row(2, "S2", "dropped")),
                new IncrementalSnapshotSimulator.BinlogEvent(80, "c",
                        new IncrementalSnapshotSimulator.Row(4, "S4", "enrolled"))
        );

        List<long[]> chunks = List.of(new long[]{1, 2}, new long[]{3, 4});
        List<IncrementalSnapshotSimulator.Row> result =
                IncrementalSnapshotSimulator.runIncrementalSnapshot(table, binlog, chunks);

        Optional<IncrementalSnapshotSimulator.Row> id2 = result.stream()
                .filter(r -> r.getId() == 2).findFirst();
        assertTrue(id2.isPresent());
        assertEquals("dropped", id2.get().getStatus(), "水位窗口内 binlog 应覆盖 chunk 快照");

        System.out.println("无锁增量快照：chunk 快照 + (lowWM, highWM] binlog 合并 → 不丢不重");
        result.forEach(r -> System.out.printf("  id=%d student=%s status=%s%n",
                r.getId(), r.getStudentId(), r.getStatus()));
    }

    @Test
    void parser_detectsSnapshotPhase() {
        String snapshotJson = "{"
                + "\"op\":\"r\","
                + "\"after\":{\"id\":1,\"student_id\":\"S90001\",\"course_id\":\"C_JAVA\",\"enroll_status\":\"enrolled\"},"
                + "\"source\":{\"db\":\"flink_cdc_demo\",\"table\":\"student_enrollment\",\"snapshot\":\"true\"},"
                + "\"ts_ms\":1700000001000"
                + "}";
        CdcChangeEvent event = CdcDebeziumJsonParser.parse(snapshotJson).orElseThrow();
        assertTrue(event.isSnapshotPhase());
        assertEquals("r", event.getOp());

        String binlogJson = "{"
                + "\"op\":\"u\","
                + "\"after\":{\"id\":1,\"student_id\":\"S90001\",\"course_id\":\"C_JAVA\",\"enroll_status\":\"dropped\"},"
                + "\"source\":{\"db\":\"flink_cdc_demo\",\"table\":\"student_enrollment\",\"file\":\"mysql-bin.000001\",\"pos\":1234},"
                + "\"ts_ms\":1700000002000"
                + "}";
        CdcChangeEvent upd = CdcDebeziumJsonParser.parse(binlogJson).orElseThrow();
        assertFalse(upd.isSnapshotPhase());
        assertEquals("u", upd.getOp());
        System.out.println("Snapshot: " + event);
        System.out.println("Binlog:   " + upd);
    }

    @Test
    void comparison_traditionalDumpVsIncrementalSnapshot() {
        Map<String, String> compare = new LinkedHashMap<>();
        compare.put("mysqldump+锁表", "长锁表、不可并行、切换增量易丢");
        compare.put("Canal 仅增量", "无全量一致性锚点，冷启动麻烦");
        compare.put("Flink CDC 增量快照", "chunk 并行 + 无锁 + 水位合并 + CK 断点");
        assertEquals(3, compare.size());
        compare.forEach((k, v) -> System.out.println(k + " → " + v));
    }

    @Test
    void exactlyOnce_cdcOffsetInCheckpoint() {
        String eo = "CDC Source 将 binlog 位点写入 Flink Checkpoint；failover 从 CK 恢复，不依赖外部 offset 表 alone";
        assertTrue(eo.contains("Checkpoint"));
        System.out.println(eo);
    }

    @Test
    void pitfalls_primaryKeyChunkAndDdl() {
        List<String> pitfalls = List.of(
                "无主键表：无法 chunk 切分，需全表扫或指定 chunk key",
                "全量背压：chunk 过大/Sink 慢会拖长 snapshot 阶段",
                "DDL 变更：需 Schema 演进策略或重启对齐",
                "transaction.timeout：大表 initial 快照耗时长"
        );
        assertEquals(4, pitfalls.size());
        pitfalls.forEach(p -> System.out.println("  □ " + p));
    }

    @Test
    void sendCdcDemoDataToMysql() throws Exception {
        mutateMysqlDemoData();
    }

    public static void main(String[] args) throws Exception {
        mutateMysqlDemoData();
    }

    /**
     * 在 Job 运行期间调用：INSERT / UPDATE / DELETE，观察 [CDC-BINLOG] 日志。
     */
    public static void mutateMysqlDemoData() throws Exception {
        System.out.println("========================================");
        System.out.println("CDC Demo MySQL 数据变更");
        System.out.println("JDBC: " + CdcDemoConfigurator.jdbcUrl());
        System.out.println("========================================");

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            System.out.println("未找到 MySQL Driver，请确认 flink-connector-mysql-cdc 依赖");
            return;
        }

        try (Connection conn = DriverManager.getConnection(
                CdcDemoConfigurator.jdbcUrl(),
                CdcDemoConfigurator.mysqlUser(),
                CdcDemoConfigurator.mysqlPassword())) {

            runPhase1Insert(conn);
            Thread.sleep(2000);
            runPhase2Update(conn);
            Thread.sleep(2000);
            runPhase3Delete(conn);

            printCurrentTable(conn);
            printExpectedOutcomes();
        } catch (Exception e) {
            System.out.println("MySQL 连接失败（请执行 init_mysql.sql 并配置 -Dcdc.mysql.*）：" + e.getMessage());
            throw e;
        }
    }

    private static void runPhase1Insert(Connection conn) throws Exception {
        System.out.println("--- Phase1 INSERT（latest 模式：Job 先启后写可捕获）---");
        String sql = "INSERT INTO student_enrollment (student_id, course_id, enroll_status) VALUES (?,?,?)";
        List<Object[]> rows = List.of(
                new Object[]{"S90101", "C_FLINK", "enrolled"},
                new Object[]{"S90102", "C_KAFKA", "enrolled"}
        );
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (Object[] row : rows) {
                ps.setString(1, (String) row[0]);
                ps.setString(2, (String) row[1]);
                ps.setString(3, (String) row[2]);
                ps.executeUpdate();
                System.out.printf("[MYSQL] INSERT student=%s course=%s%n", row[0], row[1]);
            }
        }
    }

    private static void runPhase2Update(Connection conn) throws Exception {
        System.out.println("--- Phase2 UPDATE（应出现 [CDC-BINLOG] op=u）---");
        String sql = "UPDATE student_enrollment SET enroll_status='dropped' WHERE student_id='S90001'";
        try (Statement st = conn.createStatement()) {
            int n = st.executeUpdate(sql);
            System.out.println("[MYSQL] UPDATE S90001 → dropped, rows=" + n);
        }
    }

    private static void runPhase3Delete(Connection conn) throws Exception {
        System.out.println("--- Phase3 DELETE（应出现 op=d）---");
        String sql = "DELETE FROM student_enrollment WHERE student_id='S90003'";
        try (Statement st = conn.createStatement()) {
            int n = st.executeUpdate(sql);
            System.out.println("[MYSQL] DELETE S90003, rows=" + n);
        }
    }

    private static void printCurrentTable(Connection conn) throws Exception {
        System.out.println("--- 当前表数据 ---");
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT id, student_id, course_id, enroll_status FROM student_enrollment ORDER BY id")) {
            while (rs.next()) {
                System.out.printf("  id=%d student=%s course=%s status=%s%n",
                        rs.getLong("id"), rs.getString("student_id"),
                        rs.getString("course_id"), rs.getString("enroll_status"));
            }
        }
    }

    private static void printExpectedOutcomes() {
        System.out.println("========================================");
        System.out.println("预期 Job 日志：");
        System.out.println("  initial 模式首次启动：[CDC-SNAPSHOT] op=r 读取存量");
        System.out.println("  快照结束：========== [CDC-PHASE] Snapshot → Binlog 切换 ==========");
        System.out.println("  Phase2/3 变更：[CDC-BINLOG] op=u / op=d");
        System.out.println("启动：org.example.job.cdc.FlinkCdcSnapshotDemoJob initial 2 1024 hashmap");
        System.out.println("文档: resources/cdc/FlinkCdcSnapshotDemoGuide.md");
        System.out.println("========================================");
    }
}
