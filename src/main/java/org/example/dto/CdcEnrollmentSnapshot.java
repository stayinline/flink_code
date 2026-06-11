package org.example.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 乱序治理后的选课状态快照（last-write-wins 输出）。
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class CdcEnrollmentSnapshot implements Serializable {

    private Long primaryKeyId;
    private String studentId;
    private String courseId;
    private String enrollStatus;
    private boolean deleted;
    private long version;
    private String versionSource;
    private long eventTsMs;
    private String lastOp;

    @Override
    public String toString() {
        return String.format(
                "EnrollmentSnapshot{id=%s student=%s course=%s status=%s deleted=%s version=%d src=%s op=%s}",
                primaryKeyId, studentId, courseId, enrollStatus, deleted, version, versionSource, lastOp);
    }
}
