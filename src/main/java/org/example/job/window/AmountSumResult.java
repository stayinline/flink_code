package org.example.job.window;

import java.io.Serializable;

public class AmountSumResult implements Serializable {

    public final double sum;
    public final long count;

    public AmountSumResult(double sum, long count) {
        this.sum = sum;
        this.count = count;
    }
}
