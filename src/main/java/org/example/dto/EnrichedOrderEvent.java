package org.example.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 订单关联商品维表后的宽表事件。
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class EnrichedOrderEvent implements Serializable {

    private String orderId;
    private String productId;
    private String productName;
    private String category;
    private Double unitPrice;
    private Integer quantity;
    private Double amount;
    private Long ts;
    /** 查找来源：CACHE / DB / MISS */
    private String lookupSource;
    /** 本次 lookup 耗时（毫秒），缓存命中通常接近 0 */
    private Long lookupLatencyMs;
    private String tag;

    @Override
    public String toString() {
        return String.format(
                "EnrichedOrder{orderId='%s', productId='%s', name='%s', category='%s', "
                        + "unitPrice=%s, qty=%d, amount=%s, source=%s, latencyMs=%d, tag='%s'}",
                orderId, productId, productName, category, unitPrice, quantity, amount,
                lookupSource, lookupLatencyMs, tag);
    }
}
