package org.example.job.sql;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.example.job.state.StateBackendConfigurator;

/**
 * Flink SQL 深化演示 Job — 滚动窗口聚合 + 维表 Lookup Join。
 * <p>
 * 对应 D1（TUMBLE 窗口）+ D8（FOR SYSTEM_TIME AS OF 维表 Join），展示 SQL 与 DataStream 取舍。
 * <p>
 * 启动参数：{@code scenario windowSec miniBatch stateTtl}
 * 例：{@code full 30 true} 或 {@code window 30 false} 或 {@code lookup 30 true}
 * VM：{@code -Dsql.scenario=full -Dsql.window.sec=30 -Dsql.mini.batch=true}
 * <p>
 * 文档：{@code src/main/resources/sql/FlinkSqlDemoGuide.md}
 */
public class FlinkSqlDemoJob {

    public static final String KAFKA_BROKER = "192.168.1.124:9092";
    public static final String TOPIC_STUDY = "test_flink_sql_study";
    public static final String GROUP_ID = "flink-sql-demo-consumer";

    public static void main(String[] args) throws Exception {
        SqlDemoConfigurator.SqlDemoOptions options = SqlDemoConfigurator.resolveOptions(args);

        Configuration flinkConfig = StateBackendConfigurator.createFlinkConfiguration(
                StateBackendConfigurator.BACKEND_HASHMAP);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2, flinkConfig);
        env.setParallelism(2);
        env.enableCheckpointing(5000);

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        SqlDemoConfigurator.applyTableConfig(tEnv, options);

        tEnv.executeSql(SqlDemoStatements.createStudySourceDdl(KAFKA_BROKER, TOPIC_STUDY, GROUP_ID));
        CourseDimRegistrar.registerCourseDim(tEnv, env);
        tEnv.executeSql(SqlDemoStatements.createPrintSinkDdl());

        String processSql = SqlDemoStatements.resolveProcessSql(options);
        String explain = tEnv.explainSql("INSERT INTO sql_print_sink " + processSql);
        System.out.println("========================================");
        System.out.println("[SQL-EXPLAIN] 执行计划（查 Aggregate / Join / Lookup）");
        System.out.println(explain);
        System.out.println("========================================");

        tEnv.executeSql("INSERT INTO sql_print_sink " + processSql);

        printStartupBanner(options);
        env.execute("Flink SQL Demo - Window + Lookup Join");
    }

    private static void printStartupBanner(SqlDemoConfigurator.SqlDemoOptions options) {
        System.out.println("========================================");
        System.out.println("Flink SQL 深化 Demo 已启动");
        System.out.println("Kafka: " + KAFKA_BROKER + " | topic: " + TOPIC_STUDY);
        System.out.println("场景: " + SqlDemoConfigurator.describeScenario(options.scenario));
        System.out.println("TUMBLE 窗口: " + options.windowSec + "s | state.ttl: " + options.stateTtl);
        System.out.println("mini-batch: " + options.miniBatchEnabled + " | agg-phase: TWO_PHASE");
        System.out.println("动态表: study_source (Kafka changelog) + course_dim (Lookup 维表)");
        System.out.println("文档: resources/sql/FlinkSqlDemoGuide.md");
        System.out.println("请运行 FlinkSqlDemoJobTest 发送测试数据");
        System.out.println("对比实验：");
        System.out.println("  1) full 30 true     → Join + TUMBLE 一体化");
        System.out.println("  2) window 30 false  → 纯窗口，关 mini-batch");
        System.out.println("  3) lookup 30 true   → 纯维表 Join");
        System.out.println("========================================");
    }
}
