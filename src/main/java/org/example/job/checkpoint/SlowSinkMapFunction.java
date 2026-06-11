package org.example.job.checkpoint;

import org.apache.flink.api.common.functions.RichMapFunction;

/**
 * 模拟外部 Sink 慢（如 JDBC 2PC pre-commit 耗时），拉长 checkpoint 的 sync 阶段。
 */
public class SlowSinkMapFunction extends RichMapFunction<String, String> {

    private final long sleepMs;

    public SlowSinkMapFunction(long sleepMs) {
        this.sleepMs = sleepMs;
    }

    @Override
    public String map(String value) throws Exception {
        if (sleepMs > 0) {
            Thread.sleep(sleepMs);
        }
        return value;
    }
}
