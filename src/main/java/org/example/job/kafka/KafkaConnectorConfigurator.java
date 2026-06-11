package org.example.job.kafka;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.job.checkpoint.CheckpointConfigurator;
import org.example.job.state.StateBackendConfigurator;

/**
 * Kafka 连接器工程化演示配置。
 * <p>
 * 启动参数：{@code scenario offsetMode sinkSemantic parallelism backend}
 * <pre>
 *   normal latest at-least-once 4 hashmap
 *   recovery committed exactly-once 2 hashmap
 *   partition-mismatch latest at-least-once 8 hashmap
 *   group-switch earliest at-least-once 4 hashmap
 *   backlog latest exactly-once 4 rocksdb
 * </pre>
 */
public final class KafkaConnectorConfigurator {

    public static final String SCENARIO_NORMAL = "normal";
    public static final String SCENARIO_RECOVERY = "recovery";
    public static final String SCENARIO_PARTITION_MISMATCH = "partition-mismatch";
    public static final String SCENARIO_GROUP_SWITCH = "group-switch";
    public static final String SCENARIO_BACKLOG = "backlog";

    public static final String OFFSET_EARLIEST = "earliest";
    public static final String OFFSET_LATEST = "latest";
    public static final String OFFSET_COMMITTED = "committed";

    public static final String SINK_AT_LEAST_ONCE = "at-least-once";
    public static final String SINK_EXACTLY_ONCE = "exactly-once";

    public static final String GROUP_ID_DEFAULT = "flink-kafka-demo-consumer";
    public static final String GROUP_ID_SWITCHED = "flink-kafka-demo-consumer-v2";

    public static final String TRANSACTIONAL_ID_PREFIX = "flink-kafka-demo-txn-";

    private KafkaConnectorConfigurator() {
    }

    public static KafkaConnectorOptions resolveOptions(String[] args) {
        String scenario = resolveStringArg(args, 0, "kafka.scenario", SCENARIO_NORMAL);
        String offsetMode = resolveStringArg(args, 1, "kafka.offset.mode", OFFSET_LATEST);
        String sinkSemantic = resolveStringArg(args, 2, "kafka.sink.semantic", SINK_AT_LEAST_ONCE);
        int parallelism = (int) resolveLongArg(args, 3, "kafka.parallelism", 4);
        String backend = StateBackendConfigurator.resolveBackend(shiftBackendArg(args));

        if (SCENARIO_RECOVERY.equals(scenario)) {
            offsetMode = OFFSET_COMMITTED;
            sinkSemantic = SINK_EXACTLY_ONCE;
        }
        if (SCENARIO_PARTITION_MISMATCH.equals(scenario)) {
            parallelism = 8;
        }
        if (SCENARIO_BACKLOG.equals(scenario)) {
            sinkSemantic = SINK_EXACTLY_ONCE;
        }

        return new KafkaConnectorOptions(scenario, offsetMode, sinkSemantic, parallelism, backend);
    }

    private static String[] shiftBackendArg(String[] args) {
        if (args != null && args.length > 4 && args[4] != null && !args[4].isBlank()) {
            return new String[]{args[4]};
        }
        return new String[0];
    }

    public static void configureEnvironment(StreamExecutionEnvironment env, KafkaConnectorOptions options) {
        StateBackendConfigurator.configure(env, options.backend);
        CheckpointConfigurator.configure(env,
                new CheckpointConfigurator.CheckpointOptions(
                        CheckpointConfigurator.MODE_ALIGNED, 0, 0, options.backend));

        // Exactly-Once Sink 依赖 Checkpoint；间隔需小于 Kafka transaction.timeout.ms
        env.enableCheckpointing(10_000);
        CheckpointConfig ck = env.getCheckpointConfig();
        ck.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        ck.setCheckpointTimeout(60_000);
        ck.setMinPauseBetweenCheckpoints(5_000);
    }

    public static String resolveGroupId(KafkaConnectorOptions options) {
        if (SCENARIO_GROUP_SWITCH.equals(options.scenario)) {
            return GROUP_ID_SWITCHED;
        }
        return GROUP_ID_DEFAULT;
    }

    public static String describeScenario(KafkaConnectorOptions options) {
        switch (options.scenario) {
            case SCENARIO_RECOVERY:
                return "故障恢复（committed offset + Checkpoint 内 Source 状态优先）";
            case SCENARIO_PARTITION_MISMATCH:
                return "分区<并行度（P=8, topic 建议 2~4 分区 → 部分 Subtask 空闲）";
            case SCENARIO_GROUP_SWITCH:
                return "消费者组切换（groupId=" + GROUP_ID_SWITCHED + " → 按 offset 策略重新消费）";
            case SCENARIO_BACKLOG:
                return "积压追赶（burst 数据 + EO Sink，Lag↓ 不能只靠盲目加 P）";
            case SCENARIO_NORMAL:
            default:
                return "正常消费 → 有状态聚合 → KafkaSink（" + options.sinkSemantic + "）";
        }
    }

    public static String describeOffsetMode(String mode) {
        switch (mode) {
            case OFFSET_EARLIEST:
                return "earliest（无 CK 时从分区开头；有 CK 时以 CK 中 Source 状态为准）";
            case OFFSET_COMMITTED:
                return "committed（Kafka __consumer_offsets；Flink 作业仍以 Checkpoint Source 状态优先恢复）";
            case OFFSET_LATEST:
            default:
                return "latest（新 group 从末尾；恢复作业忽略，用 Checkpoint offset）";
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

    public static class KafkaConnectorOptions {
        public final String scenario;
        public final String offsetMode;
        public final String sinkSemantic;
        public final int parallelism;
        public final String backend;

        public KafkaConnectorOptions(String scenario, String offsetMode, String sinkSemantic,
                                     int parallelism, String backend) {
            this.scenario = scenario;
            this.offsetMode = offsetMode;
            this.sinkSemantic = sinkSemantic;
            this.parallelism = parallelism;
            this.backend = backend;
        }
    }
}
