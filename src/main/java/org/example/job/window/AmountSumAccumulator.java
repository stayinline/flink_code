package org.example.job.window;

import java.io.Serializable;

/** aggregate 增量累加器，避免 ProcessWindowFunction 全量缓存元素 */
public class AmountSumAccumulator implements Serializable {

    double sum;
    long count;

    void add(double amount) {
        sum += amount;
        count++;
    }

    void merge(AmountSumAccumulator other) {
        sum += other.sum;
        count += other.count;
    }
}
