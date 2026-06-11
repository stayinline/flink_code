package org.example.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 两阶段聚合局部结果（local-global 第一阶段输出）。
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class CourseWatchPartial implements Serializable {

    private String courseId;
    private int salt;
    private long partialWatchSec;
    private long partialCount;
    private String tag;

    public static CourseWatchPartial of(String courseId, int salt, long watchSec, String tag) {
        return new CourseWatchPartial(courseId, salt, watchSec, 1L, tag);
    }

    @Override
    public String toString() {
        return String.format(
                "CourseWatchPartial{course=%s salt=%d partialSec=%d count=%d tag=%s}",
                courseId, salt, partialWatchSec, partialCount, tag);
    }
}
