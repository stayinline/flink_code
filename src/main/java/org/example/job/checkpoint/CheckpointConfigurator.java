package org.example.job.checkpoint;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.job.state.StateBackendConfigurator;

import java.time.Duration;

/**
 * Checkpoint 对齐 / 非对齐、超时、存储等配置入口。
 */
public final class CheckpointConfigurator {

    public static final String MODE_ALIGNED = "aligned";
    public static final String MODE_UNALIGNED = "unaligned";

    public static final long DEFAULT_CHECKPOINT_INTERVAL_MS = 10_000;
    public static final long DEFAULT_CHECKPOINT_TIMEOUT_MS = 60_000;
    public static final long DEFAULT_ALIGNMENT_TIMEOUT_MS = 30_000;
    public static final long DEFAULT_MIN_PAUSE_BETWEEN_MS = 5_000;

    private CheckpointConfigurator() {
    }

    public static CheckpointOptions resolveOptions(String[] args) {
        String mode = resolveStringArg(args, 0, "checkpoint.mode", MODE_ALIGNED);
        long slowBranchMs = resolveLongArg(args, 1, "checkpoint.slow.branch.ms", 0);
        long sinkSlowMs = resolveLongArg(args, 2, "checkpoint.sink.slow.ms", 0);
        String backend = StateBackendConfigurator.resolveBackend(shiftBackendArg(args));
        return new CheckpointOptions(mode, slowBranchMs, sinkSlowMs, backend);
    }

    /** args[3] 可选为 state backend（hashmap / rocksdb） */
    private static String[] shiftBackendArg(String[] args) {
        if (args != null && args.length > 3 && args[3] != null && !args[3].isBlank()) {
            return new String[]{args[3]};
        }
        return new String[0];
    }

    public static void configure(StreamExecutionEnvironment env, CheckpointOptions options) {
        StateBackendConfigurator.configure(env, options.backend);

        env.enableCheckpointing(DEFAULT_CHECKPOINT_INTERVAL_MS);
        CheckpointConfig ck = env.getCheckpointConfig();
        ck.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        ck.setCheckpointTimeout(DEFAULT_CHECKPOINT_TIMEOUT_MS);
        ck.setMinPauseBetweenCheckpoints(DEFAULT_MIN_PAUSE_BETWEEN_MS);
        ck.setMaxConcurrentCheckpoints(1);
        ck.setTolerableCheckpointFailureNumber(3);
        ck.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // Aligned：多输入 union 需等齐 barrier；反压时 alignment time 升高
        ck.setAlignmentTimeout(Duration.ofMillis(DEFAULT_ALIGNMENT_TIMEOUT_MS));

        if (MODE_UNALIGNED.equalsIgnoreCase(options.mode)) {
            ck.enableUnalignedCheckpoints();
        }
    }

    public static String describeMode(String mode) {
        if (MODE_UNALIGNED.equalsIgnoreCase(mode)) {
            return "Unaligned（barrier 与 in-flight 数据一并快照，反压下 CK 不易超时，状态略大）";
        }
        return "Aligned（barrier 对齐后快照，多输入 union 需等齐，反压时 alignment time 升高）";
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

    public static class CheckpointOptions {
        public final String mode;
        public final long slowBranchMs;
        public final long sinkSlowMs;
        public final String backend;

        public CheckpointOptions(String mode, long slowBranchMs, long sinkSlowMs, String backend) {
            this.mode = mode;
            this.slowBranchMs = slowBranchMs;
            this.sinkSlowMs = sinkSlowMs;
            this.backend = backend;
        }
    }
}
