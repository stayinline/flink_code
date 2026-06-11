package org.example.job.savepoint;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.job.state.StateBackendConfigurator;

import java.nio.file.Paths;

/**
 * Savepoint 演示 Job 启动参数：版本、并行度、后端、Savepoint 恢复路径、allowNonRestoredState。
 * <p>
 * 启动示例：
 * <pre>
 *   v1 2 hashmap
 *   v2 4 hashmap file:///tmp/flink-savepoint-demo/savepoints/savepoint-xxx true
 *   v2 2 hashmap broken-uid
 * </pre>
 */
public final class SavepointConfigurator {

    public static final String VERSION_V1 = "v1";
    public static final String VERSION_V2 = "v2";

    public static final String SAVEPOINT_BASE_DIR = "file:///tmp/flink-savepoint-demo/savepoints";
    public static final String CHECKPOINT_BASE_DIR = "file:///tmp/flink-savepoint-demo/checkpoints";

    public static final long CHECKPOINT_INTERVAL_MS = 10_000;
    public static final long CHECKPOINT_TIMEOUT_MS = 60_000;

    private SavepointConfigurator() {
    }

    public static SavepointOptions resolveOptions(String[] args) {
        String version = resolveStringArg(args, 0, "savepoint.job.version", VERSION_V1);
        int parallelism = (int) resolveLongArg(args, 1, "savepoint.parallelism", 2);
        String backend = resolveStringArg(args, 2, "state.backend", StateBackendConfigurator.BACKEND_HASHMAP);
        String savepointPath = resolveOptionalArg(args, 3, "savepoint.restore.path");
        boolean allowNonRestored = resolveBooleanArg(args, 4, "savepoint.allow.non.restored", false);
        boolean brokenUid = "broken-uid".equalsIgnoreCase(version)
                || "true".equalsIgnoreCase(System.getProperty("savepoint.broken.uid", "false"));
        if ("broken-uid".equalsIgnoreCase(version)) {
            version = VERSION_V2;
        }
        return new SavepointOptions(version, parallelism, backend, savepointPath, allowNonRestored, brokenUid);
    }

    public static void configureEnvironment(StreamExecutionEnvironment env, SavepointOptions options) {
        StateBackendConfigurator.configure(env, options.backend);

        env.enableCheckpointing(CHECKPOINT_INTERVAL_MS);
        CheckpointConfig ck = env.getCheckpointConfig();
        ck.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        ck.setCheckpointTimeout(CHECKPOINT_TIMEOUT_MS);
        ck.setMinPauseBetweenCheckpoints(5_000);
        ck.setMaxConcurrentCheckpoints(1);
        ck.setTolerableCheckpointFailureNumber(3);
        // 保留外部化 Checkpoint，便于与 Savepoint 对比（CK 也可在 cancel 后恢复，但生命周期由 Flink 管理）
        ck.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        ck.setCheckpointStorage(CHECKPOINT_BASE_DIR + "/" + options.backend);
    }

    public static String describeVersion(String version) {
        if (VERSION_V2.equalsIgnoreCase(version)) {
            return "V2（兼容升级：同 UID + 同 MapState 描述符，新增 promotion 学分加成逻辑）";
        }
        return "V1（基线：按 courseId 累计有效观看秒数）";
    }

    public static String resolveAccumulatorUid(SavepointOptions options) {
        if (options.brokenUid) {
            return SavepointOperatorUids.COURSE_CREDIT_ACCUMULATOR_BROKEN;
        }
        return SavepointOperatorUids.COURSE_CREDIT_ACCUMULATOR;
    }

    public static CourseCreditAccumulateFunction.JobVersion toJobVersion(SavepointOptions options) {
        return VERSION_V2.equalsIgnoreCase(options.version)
                ? CourseCreditAccumulateFunction.JobVersion.V2
                : CourseCreditAccumulateFunction.JobVersion.V1;
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

    private static String resolveOptionalArg(String[] args, int index, String sysProp) {
        if (args != null && args.length > index && args[index] != null && !args[index].isBlank()) {
            String value = args[index].trim();
            if ("none".equalsIgnoreCase(value) || "-".equals(value)) {
                return null;
            }
            return value;
        }
        String fromProp = System.getProperty(sysProp, "").trim();
        return fromProp.isEmpty() ? null : fromProp;
    }

    private static boolean resolveBooleanArg(String[] args, int index, String sysProp, boolean defaultValue) {
        if (args != null && args.length > index && args[index] != null && !args[index].isBlank()) {
            return Boolean.parseBoolean(args[index].trim());
        }
        return Boolean.parseBoolean(System.getProperty(sysProp, String.valueOf(defaultValue)));
    }

    public static String defaultLocalSavepointDir() {
        return Paths.get(System.getProperty("java.io.tmpdir"), "flink-savepoint-demo", "savepoints")
                .toAbsolutePath()
                .toString()
                .replace('\\', '/');
    }

    public static class SavepointOptions {
        public final String version;
        public final int parallelism;
        public final String backend;
        public final String savepointPath;
        public final boolean allowNonRestoredState;
        public final boolean brokenUid;

        public SavepointOptions(String version, int parallelism, String backend,
                                String savepointPath, boolean allowNonRestoredState, boolean brokenUid) {
            this.version = version;
            this.parallelism = parallelism;
            this.backend = backend;
            this.savepointPath = savepointPath;
            this.allowNonRestoredState = allowNonRestoredState;
            this.brokenUid = brokenUid;
        }
    }
}
