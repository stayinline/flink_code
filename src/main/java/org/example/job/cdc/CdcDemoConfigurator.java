package org.example.job.cdc;

import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.job.checkpoint.CheckpointConfigurator;
import org.example.job.state.StateBackendConfigurator;

import java.util.Properties;

/**
 * Flink CDC 增量快照演示配置。
 * <p>
 * 启动参数：{@code startupMode parallelism chunkSize backend}
 * <pre>
 *   initial 2 1024 hashmap    — 增量快照 + binlog（默认）
 *   latest 2 1024 hashmap     — 仅增量（需先启动 Job 再写数据）
 *   earliest 2 1024 hashmap   — 跳过快照，从 binlog 头消费
 * </pre>
 * VM：{@code -Dcdc.mysql.host=192.168.1.124 -Dcdc.mysql.password=xxx}
 */
public final class CdcDemoConfigurator {

    public static final String STARTUP_INITIAL = "initial";
    public static final String STARTUP_LATEST = "latest";
    public static final String STARTUP_EARLIEST = "earliest";

    public static final String DEFAULT_HOST = "192.168.1.124";
    public static final int DEFAULT_PORT = 3306;
    public static final String DEFAULT_DATABASE = "flink_cdc_demo";
    public static final String DEFAULT_TABLE = "student_enrollment";
    public static final String DEFAULT_USER = "root";
    public static final String DEFAULT_PASSWORD = "root";

    public static final int DEFAULT_CHUNK_SIZE = 1024;

    private CdcDemoConfigurator() {
    }

    public static CdcDemoOptions resolveOptions(String[] args) {
        String startup = resolveStringArg(args, 0, "cdc.startup", STARTUP_INITIAL);
        int parallelism = (int) resolveLongArg(args, 1, "cdc.parallelism", 2);
        int chunkSize = (int) resolveLongArg(args, 2, "cdc.chunk.size", DEFAULT_CHUNK_SIZE);
        String backend = StateBackendConfigurator.resolveBackend(shiftBackendArg(args));
        return new CdcDemoOptions(startup, parallelism, chunkSize, backend);
    }

    private static String[] shiftBackendArg(String[] args) {
        if (args != null && args.length > 3 && args[3] != null && !args[3].isBlank()) {
            return new String[]{args[3]};
        }
        return new String[0];
    }

    public static void configureEnvironment(StreamExecutionEnvironment env, CdcDemoOptions options) {
        StateBackendConfigurator.configure(env, options.backend);
        CheckpointConfigurator.configure(env,
                new CheckpointConfigurator.CheckpointOptions(
                        CheckpointConfigurator.MODE_ALIGNED, 0, 0, options.backend));
    }

    public static StartupOptions resolveStartupOptions(String startup) {
        switch (startup) {
            case STARTUP_LATEST:
                return StartupOptions.latest();
            case STARTUP_EARLIEST:
                return StartupOptions.earliest();
            case STARTUP_INITIAL:
            default:
                return StartupOptions.initial();
        }
    }

    public static Properties buildDebeziumProperties(CdcDemoOptions options) {
        Properties props = new Properties();
        // 无锁快照（增量快照核心）
        props.setProperty("debezium.snapshot.locking.mode", "none");
        props.setProperty("scan.incremental.snapshot.enabled", "true");
        props.setProperty("scan.incremental.snapshot.chunk.size", String.valueOf(options.chunkSize));
        props.setProperty("debezium.snapshot.mode", "initial");
        props.setProperty("debezium.include.schema.changes", "false");
        return props;
    }

    public static String mysqlHost() {
        return System.getProperty("cdc.mysql.host", DEFAULT_HOST);
    }

    public static int mysqlPort() {
        return Integer.parseInt(System.getProperty("cdc.mysql.port", String.valueOf(DEFAULT_PORT)));
    }

    public static String mysqlUser() {
        return System.getProperty("cdc.mysql.user", DEFAULT_USER);
    }

    public static String mysqlPassword() {
        return System.getProperty("cdc.mysql.password", DEFAULT_PASSWORD);
    }

    public static String jdbcUrl() {
        return String.format(
                "jdbc:mysql://%s:%d/%s?useSSL=false&characterEncoding=utf8&serverTimezone=Asia/Shanghai",
                mysqlHost(), mysqlPort(), DEFAULT_DATABASE);
    }

    public static String describeStartup(String startup) {
        switch (startup) {
            case STARTUP_LATEST:
                return "latest（仅 binlog 增量，适合 Job 先启动后灌数）";
            case STARTUP_EARLIEST:
                return "earliest（跳过快照，从 binlog 最早位点）";
            case STARTUP_INITIAL:
            default:
                return "initial（FLIP-27 增量快照 chunk + 水位合并 + binlog）";
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

    public static class CdcDemoOptions {
        public final String startup;
        public final int parallelism;
        public final int chunkSize;
        public final String backend;

        public CdcDemoOptions(String startup, int parallelism, int chunkSize, String backend) {
            this.startup = startup;
            this.parallelism = parallelism;
            this.chunkSize = chunkSize;
            this.backend = backend;
        }
    }
}
