package org.example.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class JoinResult implements Serializable {

    private String joinType;
    private String requestId;
    private String studentId;
    private String courseId;
    private String exposureId;
    private String clickId;
    private Long exposureTs;
    private Long clickTs;
    private Long delayMs;
    private String note;

    public static JoinResult matched(
            String joinType, EducationExposureEvent exposure, EducationClickEvent click, String note) {
        Long delay = null;
        if (exposure != null && click != null && exposure.getTs() != null && click.getTs() != null) {
            delay = click.getTs() - exposure.getTs();
        }
        return new JoinResult(
                joinType,
                exposure == null ? click.getRequestId() : exposure.getRequestId(),
                exposure == null ? click.getStudentId() : exposure.getStudentId(),
                exposure == null ? click.getCourseId() : exposure.getCourseId(),
                exposure == null ? null : exposure.getExposureId(),
                click == null ? null : click.getClickId(),
                exposure == null ? null : exposure.getTs(),
                click == null ? null : click.getTs(),
                delay,
                note);
    }

    public static JoinResult leftNull(
            String joinType, EducationExposureEvent exposure, String note) {
        return new JoinResult(
                joinType,
                exposure.getRequestId(),
                exposure.getStudentId(),
                exposure.getCourseId(),
                exposure.getExposureId(),
                null,
                exposure.getTs(),
                null,
                null,
                note);
    }

    @Override
    public String toString() {
        return String.format(
                "JoinResult{type='%s', requestId='%s', studentId='%s', courseId='%s', "
                        + "exposureId='%s', clickId='%s', exposureTs=%s, clickTs=%s, "
                        + "delayMs=%s, note='%s'}",
                joinType,
                requestId,
                studentId,
                courseId,
                exposureId,
                clickId,
                exposureTs,
                clickTs,
                delayMs,
                note);
    }
}
