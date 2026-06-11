package org.example.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 商品维表记录（模拟 MySQL/Redis 中的商品主数据）。
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ProductDim implements Serializable {

    private String productId;
    private String productName;
    private String category;
    private Double unitPrice;
    /** 维表记录最后更新时间（毫秒） */
    private Long updateTime;

    @Override
    public String toString() {
        return String.format("ProductDim{productId='%s', productName='%s', category='%s', unitPrice=%s, updateTime=%d}",
                productId, productName, category, unitPrice, updateTime);
    }
}
