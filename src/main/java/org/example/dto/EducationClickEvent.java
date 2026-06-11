package org.example.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class EducationClickEvent implements Serializable {

    private String clickId;
    private String requestId;
    private String studentId;
    private String courseId;
    private Long ts;
    private String action;
    private String tag;

    @Override
    public String toString() {
        return String.format(
                "EducationClickEvent{clickId='%s', requestId='%s', studentId='%s', "
                        + "courseId='%s', ts=%d, action='%s', tag='%s'}",
                clickId, requestId, studentId, courseId, ts, action, tag);
    }
}
