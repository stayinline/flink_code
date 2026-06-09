package org.example.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * State 类型与后端演示事件（在线教育场景）。
 * <p>
 * JSON 示例：
 * <pre>
 * {"eventId":"e01","studentId":"S10001","courseId":"C_MATH","eventType":"video_progress",
 *  "watchSec":120,"questionId":null,"score":null,"ts":1700000001000,"tag":"progress"}
 * </pre>
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class StateDemoEvent implements Serializable {

    public static final String TYPE_VIDEO_PROGRESS = "video_progress";
    public static final String TYPE_QUIZ_ANSWER = "quiz_answer";
    public static final String TYPE_QUIZ_QUESTION = "quiz_question";

    private String eventId;
    private String studentId;
    private String courseId;
    /** video_progress | quiz_answer | quiz_question */
    private String eventType;
    /** 本次上报有效观看秒数（video_progress） */
    private Integer watchSec;
    /** 测验题目 ID（quiz_answer / quiz_question） */
    private String questionId;
    /** 得分（quiz_answer） */
    private Integer score;
    /** 事件时间（毫秒） */
    private Long ts;
    /** 场景标签：progress / duplicate-retry / answer-before-q / question-def 等 */
    private String tag;

    @Override
    public String toString() {
        return String.format(
                "StateDemoEvent{eventId='%s', studentId='%s', courseId='%s', eventType='%s', "
                        + "watchSec=%s, questionId='%s', score=%s, ts=%d, tag='%s'}",
                eventId, studentId, courseId, eventType, watchSec, questionId, score, ts, tag);
    }
}
