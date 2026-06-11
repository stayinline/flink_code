package org.example.job.sql;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Flink SQL Table 配置：state TTL、mini-batch、场景参数。
 */
public final class SqlDemoConfigurator {

    public static final String SCENARIO_WINDOW = "window";
    public static final String SCENARIO_LOOKUP = "lookup";
    public static final String SCENARIO_FULL = "full";

    public static final int DEFAULT_WINDOW_SEC = 30;
    public static final String DEFAULT_STATE_TTL = "1 h";
    public static final String DEFAULT_MINI_BATCH_LATENCY = "5 s";

    private SqlDemoConfigurator() {
    }

    public static SqlDemoOptions resolveOptions(String[] args) {
        String scenario = resolveStringArg(args, 0, "sql.scenario", SCENARIO_FULL);
        int windowSec = (int) resolveLongArg(args, 1, "sql.window.sec", DEFAULT_WINDOW_SEC);
        boolean miniBatch = resolveBooleanArg(args, 2, "sql.mini.batch", true);
        String stateTtl = resolveStringArg(args, 3, "sql.state.ttl", DEFAULT_STATE_TTL);
        return new SqlDemoOptions(scenario, windowSec, miniBatch, stateTtl);
    }

    public static void applyTableConfig(StreamTableEnvironment tEnv, SqlDemoOptions options) {
        Configuration conf = tEnv.getConfig().getConfiguration();
        conf.setString("table.exec.state.ttl", options.stateTtl);
        if (options.miniBatchEnabled) {
            conf.setString("table.exec.mini-batch.enabled", "true");
            conf.setString("table.exec.mini-batch.allow-latency", DEFAULT_MINI_BATCH_LATENCY);
            conf.setString("table.exec.mini-batch.size", "5000");
        } else {
            conf.setString("table.exec.mini-batch.enabled", "false");
        }
        // local-global 两阶段聚合（与 DataStream 倾斜治理同源优化）
        conf.setString("table.optimizer.agg-phase-strategy", "TWO_PHASE");
    }

    public static String describeScenario(String scenario) {
        switch (scenario) {
            case SCENARIO_WINDOW:
                return "TUMBLE 滚动窗口聚合（对应 D1 Window Demo）";
            case SCENARIO_LOOKUP:
                return "FOR SYSTEM_TIME AS OF 维表 Lookup Join（对应 D8 DimJoin Demo）";
            case SCENARIO_FULL:
            default:
                return "维表 Join + TUMBLE 窗口按 category 汇总（SQL 一体化）";
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

    private static boolean resolveBooleanArg(String[] args, int index, String sysProp, boolean defaultValue) {
        if (args != null && args.length > index && args[index] != null && !args[index].isBlank()) {
            return Boolean.parseBoolean(args[index]);
        }
        return Boolean.parseBoolean(System.getProperty(sysProp, String.valueOf(defaultValue)));
    }

    public static class SqlDemoOptions {
        public final String scenario;
        public final int windowSec;
        public final boolean miniBatchEnabled;
        public final String stateTtl;

        public SqlDemoOptions(String scenario, int windowSec, boolean miniBatchEnabled, String stateTtl) {
            this.scenario = scenario;
            this.windowSec = windowSec;
            this.miniBatchEnabled = miniBatchEnabled;
            this.stateTtl = stateTtl;
        }
    }
}
