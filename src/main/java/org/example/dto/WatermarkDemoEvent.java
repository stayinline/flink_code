package org.example.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Watermark 演示事件。
 * JSON 示例：{"eventId":"e001","userId":"u001","ts":1700000001000,"amount":10.0,"source":"fast","tag":"on-time"}
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class WatermarkDemoEvent implements Serializable {

    private String eventId;
    private String userId;
    /** 事件时间（毫秒） */
    private Long ts;
    private Double amount;
    /** 数据来源标识：fast / slow */
    private String source;
    /** 场景标签：on-time / late / flush 等 */
    private String tag;

    @Override
    public String toString() {
        return String.format("WatermarkDemoEvent{eventId='%s', userId='%s', ts=%d, amount=%s, source='%s', tag='%s'}",
                eventId, userId, ts, amount, source, tag);
    }
}
