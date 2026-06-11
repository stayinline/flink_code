package org.example.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 课程维表记录（Flink SQL Lookup Join 演示）。
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class CourseDimRecord implements Serializable {

    private String courseId;
    private String courseName;
    private String category;

    @Override
    public String toString() {
        return String.format("CourseDim{courseId='%s', courseName='%s', category='%s'}",
                courseId, courseName, category);
    }
}
