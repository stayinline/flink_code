package org.example.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 解析后的 Debezium / Flink CDC 变更事件（在线教育教务表示例）。
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class CdcChangeEvent implements Serializable {

    /** r=快照读 c=插入 u=更新 d=删除 */
    private String op;
    /** true=全量快照阶段 false=纯 binlog 增量 */
    private boolean snapshotPhase;
    private String snapshotLabel;
    private String database;
    private String table;
    private Long primaryKeyId;
    private String studentId;
    private String courseId;
    private String enrollStatus;
    private long eventTsMs;
    /** 业务表 updated_at（毫秒），可选版本字段 */
    private Long dbUpdatedAtMs;
    private String binlogFile;
    private Long binlogPos;
    private String rawJson;

    public boolean isDelete() {
        return "d".equalsIgnoreCase(op);
    }

    public boolean isSnapshotRead() {
        return "r".equalsIgnoreCase(op) || snapshotPhase;
    }

    @Override
    public String toString() {
        return String.format(
                "CdcEvent{phase=%s op=%s id=%s student=%s course=%s status=%s ts=%d}",
                snapshotPhase ? "SNAPSHOT" : "BINLOG",
                op, primaryKeyId, studentId, courseId, enrollStatus, eventTsMs);
    }
}
