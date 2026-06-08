package org.example.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 订单 / 支付事件，用于 KeyedProcessFunction + 事件时间定时器演示。
 * <p>
 * JSON 示例：
 * <pre>
 * {"orderId":"O001","userId":"u001","eventType":"ORDER_CREATED","amount":99.0,"ts":1700000001000,"tag":"on-time"}
 * {"orderId":"O001","userId":"u001","eventType":"PAYMENT","amount":99.0,"ts":1700000005000,"tag":"paid"}
 * </pre>
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderPaymentEvent implements Serializable {

    public static final String TYPE_ORDER_CREATED = "ORDER_CREATED";
    public static final String TYPE_PAYMENT = "PAYMENT";

    private String orderId;
    private String userId;
    /** ORDER_CREATED | PAYMENT */
    private String eventType;
    private Double amount;
    /** 事件时间（毫秒） */
    private Long ts;
    /** 场景标签：on-time / late / flush 等 */
    private String tag;

    @Override
    public String toString() {
        return String.format(
                "OrderPaymentEvent{orderId='%s', userId='%s', eventType='%s', amount=%s, ts=%d, tag='%s'}",
                orderId, userId, eventType, amount, ts, tag);
    }
}
