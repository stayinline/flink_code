package org.example.job.stability;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.example.job.checkpoint.CheckpointConfigurator;
import org.example.job.state.StateBackendConfigurator;

/**
 * 重启策略 + 场景模式（反压 / 倾斜 / 两阶段聚合）配置。
 */
public final class StabilityConfigurator {

    public static final String SCENARIO_BACKPRESSURE = "backpressure";
    public static final String SCENARIO_SKEW = "skew";
    public static final String SCENARIO_TWOPHASE = "twophase";

    public static final String RESTART_FIXED = "fixed";
    public static final String RESTART_FAILURE_RATE = "failure-rate";
    public static final String RESTART_EXPONENTIAL = "exponential";

    /** 测试数据中的热点课程（大班直播课） */
    public static final String HOT_COURSE_ID = "C_LIVE_888";
    /** 两阶段聚合 salt 桶数 */
    public static final int DEFAULT_SALT_BUCKETS = 8;

    private StabilityConfigurator() {
    }

    public static StabilityOptions resolveOptions(String[] args) {
        String scenario = resolveStringArg(args, 0, "stability.scenario", SCENARIO_BACKPRESSURE);
        String restart = resolveStringArg(args, 1, "stability.restart", RESTART_FIXED);
        long slowSinkMs = resolveLongArg(args, 2, "stability.slow.sink.ms", 200);
        int saltBuckets = (int) resolveLongArg(args, 3, "stability.salt.buckets", DEFAULT_SALT_BUCKETS);
        String backend = StateBackendConfigurator.resolveBackend(shiftBackendArg(args));
        return new StabilityOptions(scenario, restart, slowSinkMs, saltBuckets, backend);
    }

    private static String[] shiftBackendArg(String[] args) {
        if (args != null && args.length > 4 && args[4] != null && !args[4].isBlank()) {
            return new String[]{args[4]};
        }
        return new String[0];
    }

    public static void configureEnvironment(StreamExecutionEnvironment env, StabilityOptions options) {
        StateBackendConfigurator.configure(env, options.backend);
        CheckpointConfigurator.configure(env,
                new CheckpointConfigurator.CheckpointOptions(
                        CheckpointConfigurator.MODE_ALIGNED, 0, 0, options.backend));

        configureRestartStrategy(env, options.restart);
    }

    static void configureRestartStrategy(StreamExecutionEnvironment env, String restart) {
        switch (restart) {
            case RESTART_FAILURE_RATE:
                // 5 分钟内最多 3 次失败，间隔 15s 再重启
                env.setRestartStrategy(RestartStrategies.failureRateRestart(
                        3,
                        Time.minutes(5),
                        Time.seconds(15)));
                break;
            case RESTART_EXPONENTIAL:
                // 本地 Demo 用较短 fixed-delay 模拟；集群 flink-conf 配 exponential-delay 见文档
                env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, Time.seconds(2)));
                break;
            case RESTART_FIXED:
            default:
                // 最多 5 次，间隔 10s；恢复时从最近成功 CK 加载状态
                env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.seconds(10)));
                break;
        }
    }

    public static String describeScenario(String scenario) {
        switch (scenario) {
            case SCENARIO_SKEW:
                return "数据倾斜（keyBy(courseId) + 热点 " + HOT_COURSE_ID + " → 单 subtask 过载）";
            case SCENARIO_TWOPHASE:
                return "两阶段聚合（salt 前缀打散 local → global merge，缓解热点 key）";
            case SCENARIO_BACKPRESSURE:
            default:
                return "反压链路（慢 Sink sleep → buffer 满 → 上游阻塞，UI BackPressure 标红）";
        }
    }

    public static String describeRestart(String restart) {
        switch (restart) {
            case RESTART_FAILURE_RATE:
                return "failure-rate（单位时间内失败次数上限，适合偶发故障）";
            case RESTART_EXPONENTIAL:
                return "exponential-delay（退避递增 1s→2s→4s…，适合外部依赖抖动）";
            case RESTART_FIXED:
            default:
                return "fixed-delay（固定间隔重试，配合 CK 恢复到最近快照）";
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

    public static class StabilityOptions {
        public final String scenario;
        public final String restart;
        public final long slowSinkMs;
        public final int saltBuckets;
        public final String backend;

        public StabilityOptions(String scenario, String restart, long slowSinkMs, int saltBuckets, String backend) {
            this.scenario = scenario;
            this.restart = restart;
            this.slowSinkMs = slowSinkMs;
            this.saltBuckets = saltBuckets;
            this.backend = backend;
        }
    }
}
