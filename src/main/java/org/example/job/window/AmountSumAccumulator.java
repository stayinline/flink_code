package org.example.job.window;

import lombok.Data;

import java.io.Serializable;

/**
 * aggregate 增量累加器，避免 ProcessWindowFunction 全量缓存元素
 */
@Data
public class AmountSumAccumulator implements Serializable {

    public double sum;
    public long count;

    public void add(double amount) {
        sum += amount;
        count++;
    }

    public void merge(AmountSumAccumulator other) {
        sum += other.sum;
        count += other.count;
    }
}
