package org.example.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class EducationExposureEvent implements Serializable {

    private String exposureId;
    private String requestId;
    private String studentId;
    private String courseId;
    private Long ts;
    private String scene;
    private String tag;

    @Override
    public String toString() {
        return String.format(
                "EducationExposureEvent{exposureId='%s', requestId='%s', studentId='%s', "
                        + "courseId='%s', ts=%d, scene='%s', tag='%s'}",
                exposureId, requestId, studentId, courseId, ts, scene, tag);
    }
}
