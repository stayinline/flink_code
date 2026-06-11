package org.example.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 学习时长汇总写出记录（Exactly-Once Demo Sink 输入）。
 * <p>
 * {@link #dedupKey} 用于 ClickHouse 幂等路线：主键覆盖 / ReplacingMergeTree。
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class StudySummaryRecord implements Serializable {

    private String eventId;
    private String studentId;
    private String courseId;
    /** 累计有效学习秒数 */
    private long totalWatchSec;
    /** 业务日期 yyyy-MM-dd，幂等键组成部分 */
    private String statDate;
    /** 来源事件 tag：normal / duplicate-retry / ck-fail-recovery 等 */
    private String tag;
    /** 幂等去重键：statDate + studentId + courseId（同窗口重算覆盖） */
    private String dedupKey;

    public static StudySummaryRecord of(String eventId, String studentId, String courseId,
                                        long totalWatchSec, String statDate, String tag) {
        String dedupKey = statDate + "|" + studentId + "|" + courseId;
        return new StudySummaryRecord(eventId, studentId, courseId, totalWatchSec, statDate, tag, dedupKey);
    }

    @Override
    public String toString() {
        return String.format(
                "StudySummary{student=%s course=%s total=%ds date=%s dedupKey=%s tag=%s}",
                studentId, courseId, totalWatchSec, statDate, dedupKey, tag);
    }
}
