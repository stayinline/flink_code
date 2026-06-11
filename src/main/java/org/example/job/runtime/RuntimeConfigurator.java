package org.example.job.runtime;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.job.checkpoint.CheckpointConfigurator;
import org.example.job.state.StateBackendConfigurator;

/**
 * Flink Runtime 演示：场景、并行度、Operator Chain、慢 Sink、Network Buffer、Managed Memory。
 * <p>
 * 启动参数：{@code scenario parallelism slowSinkMs chainMode backend}
 * <pre>
 *   backpressure 4 300 chain hashmap
 *   backpressure 4 300 nochain hashmap
 *   hotslot 4 300 chain hashmap
 *   skew 4 0 chain hashmap
 *   managed 4 200 chain rocksdb
 * </pre>
 */
public final class RuntimeConfigurator {

    public static final String SCENARIO_BACKPRESSURE = "backpressure";
    public static final String SCENARIO_HOTSLOT = "hotslot";
    public static final String SCENARIO_SKEW = "skew";
    public static final String SCENARIO_MANAGED = "managed";

    public static final String CHAIN_ON = "chain";
    public static final String CHAIN_OFF = "nochain";

    public static final String SLOT_GROUP_DEFAULT = "default";
    public static final String SLOT_GROUP_HEAVY_SINK = "heavy-sink";

    /** 测试数据热点课程（大班直播） */
    public static final String HOT_COURSE_ID = "C_LIVE_888";

    public static final int DEFAULT_PARALLELISM = 4;
    public static final long DEFAULT_SLOW_SINK_MS = 200;

    private RuntimeConfigurator() {
    }

    public static RuntimeOptions resolveOptions(String[] args) {
        String scenario = resolveStringArg(args, 0, "runtime.scenario", SCENARIO_BACKPRESSURE);
        int parallelism = (int) resolveLongArg(args, 1, "runtime.parallelism", DEFAULT_PARALLELISM);
        long slowSinkMs = resolveLongArg(args, 2, "runtime.slow.sink.ms", DEFAULT_SLOW_SINK_MS);
        String chainMode = resolveStringArg(args, 3, "runtime.chain", CHAIN_ON);
        String backend = StateBackendConfigurator.resolveBackend(shiftBackendArg(args));
        return new RuntimeOptions(scenario, parallelism, slowSinkMs, chainMode, backend);
    }

    private static String[] shiftBackendArg(String[] args) {
        if (args != null && args.length > 4 && args[4] != null && !args[4].isBlank()) {
            return new String[]{args[4]};
        }
        return new String[0];
    }

    public static Configuration createFlinkConfiguration(RuntimeOptions options) {
        Configuration configuration = StateBackendConfigurator.createFlinkConfiguration(options.backend);
        if (SCENARIO_MANAGED.equals(options.scenario) || StateBackendConfigurator.BACKEND_ROCKSDB.equals(options.backend)) {
            configuration.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("256m"));
        }
        // 刻意收紧 Network Memory，放大反压可见性（仅本地 Demo）
        if (SCENARIO_BACKPRESSURE.equals(options.scenario) && options.slowSinkMs >= 200) {
            configuration.set(TaskManagerOptions.NETWORK_MEMORY_MIN, MemorySize.parse("32mb"));
            configuration.set(TaskManagerOptions.NETWORK_MEMORY_MAX, MemorySize.parse("64mb"));
        }
        return configuration;
    }

    public static void configureEnvironment(StreamExecutionEnvironment env, RuntimeOptions options) {
        StateBackendConfigurator.configure(env, options.backend);
        CheckpointConfigurator.configure(env,
                new CheckpointConfigurator.CheckpointOptions(
                        CheckpointConfigurator.MODE_ALIGNED, 0, 0, options.backend));

        if (isChainDisabled(options)) {
            env.disableOperatorChaining();
        }
    }

    public static boolean isChainDisabled(RuntimeOptions options) {
        return CHAIN_OFF.equalsIgnoreCase(options.chainMode);
    }

    public static boolean isolateSinkSlot(RuntimeOptions options) {
        return SCENARIO_HOTSLOT.equals(options.scenario);
    }

    public static boolean keyByCourseForSkew(RuntimeOptions options) {
        return SCENARIO_SKEW.equals(options.scenario);
    }

    public static String describeScenario(RuntimeOptions options) {
        switch (options.scenario) {
            case SCENARIO_HOTSLOT:
                return "Slot 隔离（Sink 独立 slotSharingGroup=heavy-sink，与默认组争用 Slot）";
            case SCENARIO_SKEW:
                return "热点 key（keyBy(courseId) + " + HOT_COURSE_ID + "，加并行度无法打散）";
            case SCENARIO_MANAGED:
                return "Managed Memory（RocksDB 后端 + 256m managed，状态/排序吃托管内存）";
            case SCENARIO_BACKPRESSURE:
            default:
                if (isChainDisabled(options)) {
                    return "反压 + 禁用 Operator Chain（Task 数↑，便于 UI 对照 Subtask 分布）";
                }
                return "反压链路（慢 Sink → Network Buffer 满 → 上游 busy/backpressured）";
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

    public static class RuntimeOptions {
        public final String scenario;
        public final int parallelism;
        public final long slowSinkMs;
        public final String chainMode;
        public final String backend;

        public RuntimeOptions(String scenario, int parallelism, long slowSinkMs, String chainMode, String backend) {
            this.scenario = scenario;
            this.parallelism = parallelism;
            this.slowSinkMs = slowSinkMs;
            this.chainMode = chainMode;
            this.backend = backend;
        }
    }
}
