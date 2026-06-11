package org.example.job.quality;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.job.checkpoint.CheckpointConfigurator;
import org.example.job.state.StateBackendConfigurator;

/**
 * 数据质量演示配置。
 * <p>
 * 启动参数：{@code mode backend}
 * <pre>
 *   strict hashmap     — 全量校验 + DLQ + 幂等汇总（默认）
 *   drop hashmap       — 脏数据静默丢弃（对照组，无 DLQ）
 *   replay hashmap     — 允许回放 tag，独立 consumer group 见文档
 * </pre>
 */
public final class DataQualityConfigurator {

    public static final String MODE_STRICT = "strict";
    public static final String MODE_DROP = "drop";
    public static final String MODE_REPLAY = "replay";

    public static final String GROUP_ID_DEFAULT = "flink-quality-demo-consumer";
    public static final String GROUP_ID_REPLAY = "flink-quality-replay-consumer";

    private DataQualityConfigurator() {
    }

    public static QualityOptions resolveOptions(String[] args) {
        String mode = resolveStringArg(args, 0, "quality.mode", MODE_STRICT);
        String backend = StateBackendConfigurator.resolveBackend(shiftBackendArg(args));
        return new QualityOptions(mode, backend);
    }

    private static String[] shiftBackendArg(String[] args) {
        if (args != null && args.length > 1 && args[1] != null && !args[1].isBlank()) {
            return new String[]{args[1]};
        }
        return new String[0];
    }

    public static void configureEnvironment(StreamExecutionEnvironment env, QualityOptions options) {
        StateBackendConfigurator.configure(env, options.backend);
        CheckpointConfigurator.configure(env,
                new CheckpointConfigurator.CheckpointOptions(
                        CheckpointConfigurator.MODE_ALIGNED, 0, 0, options.backend));
    }

    public static boolean dlqEnabled(QualityOptions options) {
        return !MODE_DROP.equals(options.mode);
    }

    public static String resolveGroupId(QualityOptions options) {
        if (MODE_REPLAY.equals(options.mode)) {
            return GROUP_ID_REPLAY;
        }
        return GROUP_ID_DEFAULT;
    }

    public static String describeMode(QualityOptions options) {
        switch (options.mode) {
            case MODE_DROP:
                return "静默丢弃（无侧输出/DLQ，仅适合低价值 PV 类指标）";
            case MODE_REPLAY:
                return "回放模式（consumerGroup=" + GROUP_ID_REPLAY + "，修复数据 tag=replay-vN）";
            case MODE_STRICT:
            default:
                return "严格校验 + Side Output + DLQ + 幂等汇总表";
        }
    }

    private static String resolveStringArg(String[] args, int index, String sysProp, String defaultValue) {
        if (args != null && args.length > index && args[index] != null && !args[index].isBlank()) {
            return args[index].trim().toLowerCase();
        }
        return System.getProperty(sysProp, defaultValue).trim().toLowerCase();
    }

    public static class QualityOptions {
        public final String mode;
        public final String backend;

        public QualityOptions(String mode, String backend) {
            this.mode = mode;
            this.backend = backend;
        }
    }
}
