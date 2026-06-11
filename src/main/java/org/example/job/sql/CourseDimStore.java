package org.example.job.sql;

import org.example.dto.CourseDimRecord;

import java.util.Arrays;
import java.util.List;

/**
 * 在线教育课程维表（模拟 MySQL / Redis 维表，供 SQL FOR SYSTEM_TIME AS OF Join）。
 */
public final class CourseDimStore {

    private CourseDimStore() {
    }

    public static List<CourseDimRecord> allRecords() {
        return Arrays.asList(
                new CourseDimRecord("C_JAVA", "Java 零基础直播课", "编程"),
                new CourseDimRecord("C_PYTHON", "Python 数据分析", "编程"),
                new CourseDimRecord("C_MATH", "小学奥数思维课", "K12"),
                new CourseDimRecord("C_ENG", "考研英语冲刺班", "考研"),
                new CourseDimRecord("C_LIVE_888", "名师大班直播课", "直播"),
                new CourseDimRecord("C_PM", "产品经理实战营", "职场")
        );
    }
}
