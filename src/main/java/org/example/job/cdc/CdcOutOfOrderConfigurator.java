package org.example.job.cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.job.checkpoint.CheckpointConfigurator;
import org.example.job.state.StateBackendConfigurator;

/**
 * CDC 乱序治理 Demo 配置。
 * <p>
 * 启动参数：{@code versionStrategy parallelism backend}
 * <pre>
 *   binlog_pos 2 hashmap
 *   debezium_ts_ms 2 hashmap
 *   db_updated_at 2 hashmap
 * </pre>
 */
public final class CdcOutOfOrderConfigurator {

    public static final String TOPIC = "test_flink_cdc_outoforder";
    public static final String GROUP_ID = "flink-cdc-oos-demo";

    private CdcOutOfOrderConfigurator() {
    }

    public static OutOfOrderOptions resolveOptions(String[] args) {
        String versionStrategy = resolveStringArg(args, 0, "cdc.lww.strategy", CdcVersionResolver.SOURCE_BINLOG);
        int parallelism = (int) resolveLongArg(args, 1, "cdc.oos.parallelism", 2);
        String backend = StateBackendConfigurator.resolveBackend(shiftBackendArg(args));
        return new OutOfOrderOptions(versionStrategy, parallelism, backend);
    }

    private static String[] shiftBackendArg(String[] args) {
        if (args != null && args.length > 2 && args[2] != null && !args[2].isBlank()) {
            return new String[]{args[2]};
        }
        return new String[0];
    }

    public static void configureEnvironment(StreamExecutionEnvironment env, OutOfOrderOptions options) {
        StateBackendConfigurator.configure(env, options.backend);
        CheckpointConfigurator.configure(env,
                new CheckpointConfigurator.CheckpointOptions(
                        CheckpointConfigurator.MODE_ALIGNED, 0, 0, options.backend));
    }

    public static String describeStrategy(String strategy) {
        switch (strategy) {
            case CdcVersionResolver.SOURCE_DEBEZIUM_TS:
                return "Debezium ts_ms（简单但多源不一定单调）";
            case CdcVersionResolver.SOURCE_DB_UPDATED_AT:
                return "业务 updated_at（须 DB 每次更新递增）";
            case CdcVersionResolver.SOURCE_BINLOG:
            default:
                return "binlog file+pos（推荐，与 MySQL 写入顺序一致）";
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

    public static class OutOfOrderOptions {
        public final String versionStrategy;
        public final int parallelism;
        public final String backend;

        public OutOfOrderOptions(String versionStrategy, int parallelism, String backend) {
            this.versionStrategy = versionStrategy;
            this.parallelism = parallelism;
            this.backend = backend;
        }
    }
}
