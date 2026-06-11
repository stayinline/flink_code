package org.example.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 订单流事件（待关联商品维表）。
 * JSON 示例：{"orderId":"O001","productId":"P100","quantity":1,"amount":99.0,"ts":1700000001000,"tag":"cache-miss"}
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderLookupEvent implements Serializable {

    private String orderId;
    private String productId;
    private Integer quantity;
    private Double amount;
    /** 事件时间（毫秒） */
    private Long ts;
    /** 场景标签：cache-miss / cache-hit / dim-updated / unknown-product 等 */
    private String tag;

    @Override
    public String toString() {
        return String.format("OrderLookupEvent{orderId='%s', productId='%s', quantity=%d, amount=%s, ts=%d, tag='%s'}",
                orderId, productId, quantity, amount, ts, tag);
    }
}
