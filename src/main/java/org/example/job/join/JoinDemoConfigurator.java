package org.example.job.join;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 双流 Join 演示：Regular / Interval / Window / StateProbe 模式配置。
 */
public final class JoinDemoConfigurator {

    public static final String MODE_INTERVAL = "interval";
    public static final String MODE_REGULAR = "regular";
    public static final String MODE_WINDOW = "window";
    public static final String MODE_PROBE = "probe";

    /** 点击在曝光后 10 分钟内有效：click.ts ∈ [exposure.ts, exposure.ts + 10min] */
    public static final long DEFAULT_INTERVAL_LOWER_MS = 0L;
    public static final long DEFAULT_INTERVAL_UPPER_MS = 10 * 60 * 1000L;

    public static final int DEFAULT_WINDOW_SEC = 30;
    public static final long DEFAULT_STATE_TTL_HOURS = 0L;

    private JoinDemoConfigurator() {
    }

    public static JoinDemoOptions resolveOptions(String[] args) {
        String mode = resolveStringArg(args, 0, "join.mode", MODE_INTERVAL);
        int intervalUpperMin = (int) resolveLongArg(args, 1, "join.interval.upper.min", 10);
        int windowSec = (int) resolveLongArg(args, 2, "join.window.sec", DEFAULT_WINDOW_SEC);
        long stateTtlHours = resolveLongArg(args, 3, "join.state.ttl.hours", DEFAULT_STATE_TTL_HOURS);
        return new JoinDemoOptions(mode, intervalUpperMin, windowSec, stateTtlHours);
    }

    public static void configureEnvironment(StreamExecutionEnvironment env) {
        env.enableCheckpointing(10_000);
    }

    public static Time intervalLowerBound() {
        return Time.milliseconds(DEFAULT_INTERVAL_LOWER_MS);
    }

    public static Time intervalUpperBound(int upperMin) {
        return Time.milliseconds(upperMin * 60_000L);
    }

    public static Time windowSize(int windowSec) {
        return Time.seconds(windowSec);
    }

    public static String describeMode(String mode) {
        switch (mode) {
            case MODE_REGULAR:
                return "Regular Join（双流状态无限保留，必须配 State TTL 防膨胀）";
            case MODE_WINDOW:
                return "Window Join（同一 EventTime 窗口内 Join）";
            case MODE_PROBE:
                return "Interval 状态探针（观察 buffer 随 WM 清理，upper=+" + DEFAULT_INTERVAL_UPPER_MS / 60000 + "min）";
            case MODE_INTERVAL:
            default:
                return "Interval Join（click.ts ∈ [exposure.ts, exposure.ts + N min]，状态按时间自动清理）";
        }
    }

    private static String resolveStringArg(String[] args, int index, String sysProp, String defaultValue) {
        if (args != null && args.length > index && args[index] != null && !args[index].isBlank()) {
            return args[index].trim().toLowerCase();
        }
        return System.getProperty(sysProp, defaultValue).trim().toLowerCase();
    }

    private static long resolveLongArg(String[] args, int index, String sysProp, long defaultValue) {
        if (args != null && args.length > index && args[index] != null && !args[index].isBlank()) {
            return Long.parseLong(args[index]);
        }
        return Long.parseLong(System.getProperty(sysProp, String.valueOf(defaultValue)));
    }

    public static class JoinDemoOptions {
        public final String mode;
        public final int intervalUpperMin;
        public final int windowSec;
        public final long stateTtlHours;

        public JoinDemoOptions(String mode, int intervalUpperMin, int windowSec, long stateTtlHours) {
            this.mode = mode;
            this.intervalUpperMin = intervalUpperMin;
            this.windowSec = windowSec;
            this.stateTtlHours = stateTtlHours;
        }
    }
}
