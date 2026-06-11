package org.example.job.stability;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * 模拟慢外部 Sink（JDBC / ClickHouse / HTTP），制造反压链路。
 * <p>
 * 下游慢 → TaskManager 输出 buffer 满 → 上游算子 blocked → UI BackPressure HIGH。
 */
public class SlowBackpressureSinkFunction extends RichSinkFunction<String> {

    private final long sleepMs;

    public SlowBackpressureSinkFunction(long sleepMs) {
        this.sleepMs = sleepMs;
    }

    @Override
    public void open(Configuration parameters) {
        System.out.printf("[BP-SINK] 慢 Sink 就绪 sleep=%dms | 瓶颈在此，盲目加 Source 并行度无效%n", sleepMs);
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        if (sleepMs > 0) {
            Thread.sleep(sleepMs);
        }
        System.out.println("[BP-SINK-OUT] " + value);
    }
}
