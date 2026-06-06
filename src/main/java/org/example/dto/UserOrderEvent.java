package org.example.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 窗口演示标准数据源：(userId, ts, amount)
 * JSON 示例：{"userId":"u001","ts":1700000001000,"amount":10.0}
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserOrderEvent implements Serializable {

    private String userId;
    /** 事件时间戳（毫秒） */
    private Long ts;
    private Double amount;

    @Override
    public String toString() {
        return String.format("UserOrderEvent{userId='%s', ts=%d, amount=%s}", userId, ts, amount);
    }
}
