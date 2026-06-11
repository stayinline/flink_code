package org.example.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * DLQ 脏数据记录（可追踪、可修复、可回放）。
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class DirtyDataRecord implements Serializable {

    public static final String REASON_PARSE_FAIL = "PARSE_FAIL";
    public static final String REASON_MISSING_FIELD = "MISSING_FIELD";
    public static final String REASON_INVALID_ENUM = "INVALID_ENUM";
    public static final String REASON_TIME_ANOMALY = "TIME_ANOMALY";
    public static final String REASON_MISSING_BIZ_KEY = "MISSING_BIZ_KEY";

    /** DLQ 行主键：优先 eventId，否则 hash(rawPayload) */
    private String dlqId;
    private String rawPayload;
    private String reason;
    private String detail;
    /** 处理时间（毫秒） */
    private long detectedAtMs;
    private String eventId;
    private String studentId;
    /** 来源标签：normal / replay / backfill 等 */
    private String sourceTag;
    /** 补数版本号，回放修复时递增 */
    private int replayVersion;
    /** 是否已修复待回放 */
    private boolean replayable;

    public static DirtyDataRecord of(String raw, String reason, String detail, String sourceTag) {
        DirtyDataRecord r = new DirtyDataRecord();
        r.rawPayload = raw;
        r.reason = reason;
        r.detail = detail;
        r.detectedAtMs = System.currentTimeMillis();
        r.sourceTag = sourceTag != null ? sourceTag : "unknown";
        r.replayVersion = 0;
        r.replayable = true;
        r.dlqId = "dlq-" + Math.abs(String.valueOf(raw).hashCode());
        return r;
    }
}
