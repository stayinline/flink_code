package org.example.job.cdc;

import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.dto.CdcChangeEvent;
import org.example.job.state.StateBackendConfigurator;

/**
 * Flink CDC 增量快照（FLIP-27）演示 Job。
 * <p>
 * 观察全量 chunk 快照 → binlog 增量切换；配合 {@link FlinkCdcSnapshotDemoJobTest} 写 MySQL 验证增量捕获。
 * <p>
 * 启动参数：{@code startupMode parallelism chunkSize backend}
 * 例：{@code initial 2 1024 hashmap} 或 {@code latest 2 1024 hashmap}
 * <p>
 * 文档：{@code src/main/resources/cdc/FlinkCdcSnapshotDemoGuide.md}
 */
public class FlinkCdcSnapshotDemoJob {

    public static void main(String[] args) throws Exception {
        CdcDemoConfigurator.CdcDemoOptions options = CdcDemoConfigurator.resolveOptions(args);

        Configuration flinkConfig = StateBackendConfigurator.createFlinkConfiguration(options.backend);
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(options.parallelism, flinkConfig);
        env.setParallelism(options.parallelism);

        CdcDemoConfigurator.configureEnvironment(env, options);

        String tableList = CdcDemoConfigurator.DEFAULT_DATABASE + "." + CdcDemoConfigurator.DEFAULT_TABLE;

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(CdcDemoConfigurator.mysqlHost())
                .port(CdcDemoConfigurator.mysqlPort())
                .databaseList(CdcDemoConfigurator.DEFAULT_DATABASE)
                .tableList(tableList)
                .username(CdcDemoConfigurator.mysqlUser())
                .password(CdcDemoConfigurator.mysqlPassword())
                .startupOptions(CdcDemoConfigurator.resolveStartupOptions(options.startup))
                .deserializer(new JsonDebeziumDeserializationSchema())
                .debeziumProperties(CdcDemoConfigurator.buildDebeziumProperties(options))
                .serverTimeZone("Asia/Shanghai")
                .build();

        DataStream<CdcChangeEvent> changeStream = env
                .fromSource(mySqlSource, org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(),
                        "MySql-Cdc-IncrementalSnapshot")
                .name("MySql-CDC-Source")
                .map(new CdcPhaseLogFunction())
                .name("CdcPhaseMonitor")
                .filter(new ValidEventFilter())
                .name("ValidCdcEvent");

        changeStream.print("CDC变更");

        printStartupBanner(options, tableList);
        env.execute("Flink CDC Incremental Snapshot Demo - [" + options.startup + "]");
    }

    private static void printStartupBanner(CdcDemoConfigurator.CdcDemoOptions options, String tableList) {
        System.out.println("========================================");
        System.out.println("Flink CDC 增量快照 Demo 已启动");
        System.out.println("MySQL: " + CdcDemoConfigurator.mysqlHost() + ":" + CdcDemoConfigurator.mysqlPort());
        System.out.println("表: " + tableList);
        System.out.println("启动模式: " + CdcDemoConfigurator.describeStartup(options.startup));
        System.out.println("chunk.size: " + options.chunkSize + " | 无锁: snapshot.locking.mode=none");
        System.out.println("并行度: " + options.parallelism);
        System.out.println("State Backend: " + StateBackendConfigurator.describeBackend(options.backend));
        System.out.println("Checkpoint: 位点存 Source State → Exactly-Once 恢复");
        System.out.println("文档: resources/cdc/FlinkCdcSnapshotDemoGuide.md");
        System.out.println("请先执行 resources/cdc/init_mysql.sql 建库建表");
        System.out.println("再运行 FlinkCdcSnapshotDemoJobTest 写入/更新测试数据");
        System.out.println("对比实验：");
        System.out.println("  1) initial 2 1024 hashmap  — 增量快照 + binlog");
        System.out.println("  2) latest 2 1024 hashmap    — 仅增量（Job 先启后写）");
        System.out.println("  3) earliest 2 1024 hashmap  — 跳过快照");
        System.out.println("========================================");
    }

    private static class ValidEventFilter implements FilterFunction<CdcChangeEvent> {
        @Override
        public boolean filter(CdcChangeEvent event) {
            return event != null && event.getPrimaryKeyId() != null;
        }
    }
}
