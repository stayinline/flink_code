package org.example.job.exactlyonce;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.job.state.StateBackendConfigurator;

/**
 * 端到端 Exactly-Once 演示：Checkpoint EXACTLY_ONCE + Sink 模式切换。
 */
public final class ExactlyOnceConfigurator {

    public static final String SINK_2PC = "2pc";
    public static final String SINK_IDEMPOTENT = "idempotent";

    public static final long DEFAULT_CHECKPOINT_INTERVAL_MS = 10_000;
    public static final long DEFAULT_CHECKPOINT_TIMEOUT_MS = 60_000;
    public static final long DEFAULT_MIN_PAUSE_BETWEEN_MS = 5_000;

    private ExactlyOnceConfigurator() {
    }

    public static ExactlyOnceOptions resolveOptions(String[] args) {
        String sinkMode = resolveStringArg(args, 0, "exactlyonce.sink", SINK_2PC);
        long commitSlowMs = resolveLongArg(args, 1, "exactlyonce.commit.slow.ms", 0);
        String backend = StateBackendConfigurator.resolveBackend(shiftBackendArg(args));
        return new ExactlyOnceOptions(sinkMode, commitSlowMs, backend);
    }

    private static String[] shiftBackendArg(String[] args) {
        if (args != null && args.length > 2 && args[2] != null && !args[2].isBlank()) {
            return new String[]{args[2]};
        }
        return new String[0];
    }

    public static void configure(StreamExecutionEnvironment env, ExactlyOnceOptions options) {
        StateBackendConfigurator.configure(env, options.backend);

        env.enableCheckpointing(DEFAULT_CHECKPOINT_INTERVAL_MS);
        CheckpointConfig ck = env.getCheckpointConfig();
        ck.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        ck.setCheckpointTimeout(DEFAULT_CHECKPOINT_TIMEOUT_MS);
        ck.setMinPauseBetweenCheckpoints(DEFAULT_MIN_PAUSE_BETWEEN_MS);
        ck.setMaxConcurrentCheckpoints(1);
        ck.setTolerableCheckpointFailureNumber(3);
        ck.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    }

    public static String describeSinkMode(String mode) {
        if (SINK_IDEMPOTENT.equalsIgnoreCase(mode)) {
            return "幂等写（模拟 ClickHouse 主键覆盖 / upsert，靠 dedupKey 收口重复）";
        }
        return "2PC（TwoPhaseCommitSinkFunction：begin→preCommit(CK)→commit(notifyCheckpointComplete)→abort）";
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

    public static class ExactlyOnceOptions {
        public final String sinkMode;
        public final long commitSlowMs;
        public final String backend;

        public ExactlyOnceOptions(String sinkMode, long commitSlowMs, String backend) {
            this.sinkMode = sinkMode;
            this.commitSlowMs = commitSlowMs;
            this.backend = backend;
        }
    }
}
