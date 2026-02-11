package org.example.job;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.example.config.TableSyncConfig;
import org.example.function.FullSyncSourceFunction;
import org.example.function.IncrementalSyncSourceFunction;
import org.example.function.DebeziumToMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

public class TableSyncJob {
    private static final Logger LOG = LoggerFactory.getLogger(TableSyncJob.class);

    public static void run(TableSyncConfig config, String sourceDbUrl, String sourceDbUsername, String sourceDbPassword, 
                          String targetDbUrl, String targetDbUsername, String targetDbPassword) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.setParallelism(config.getParallelism());

        // Create sink for both full and incremental sync
        JdbcConnectionOptions jdbcOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(targetDbUrl)
                .withDriverName("com.mysql.cj.jdbc.Driver")
                .withUsername(targetDbUsername)
                .withPassword(targetDbPassword)
                .build();

        JdbcExecutionOptions executionOptions = JdbcExecutionOptions.builder()
                .withBatchSize(config.getBatchSize())
                .withBatchIntervalMs(1000)
                .withMaxRetries(3)
                .build();

        String insertSql = buildInsertSql(config);
        String upsertSql = buildUpsertSql(config);

        // Full sync stream
        if (config.isEnableFullSync()) {
            DataStream<Map<String, Object>> fullSyncStream = env.addSource(
                    new FullSyncSourceFunction(config, sourceDbUrl, sourceDbUsername, sourceDbPassword))
                    .name("Full Sync Source");

            fullSyncStream
                    .filter(new FilterFunction<Map<String, Object>>() {
                        @Override
                        public boolean filter(Map<String, Object> value) throws Exception {
                            return value != null && !value.isEmpty();
                        }
                    })
                    .addSink(JdbcSink.sink(
                            insertSql,
                            new DbStatementBuilder(config),
                            executionOptions,
                            jdbcOptions
                    )).name("Full Sync Sink");

            LOG.info("Enabled full sync for table {}.{}", config.getSourceDatabase(), config.getSourceTableName());
        }

        // Incremental sync stream
        if (config.isEnableIncrementalSync()) {
            DataStream<String> cdcStream = env.addSource(
                    IncrementalSyncSourceFunction.createSource(config, sourceDbUrl, sourceDbUsername, sourceDbPassword))
                    .name("CDC Source");

            DataStream<Map<String, Object>> incrementalStream = cdcStream
                    .map(new DebeziumToMapFunction())
                    .filter(new FilterFunction<Map<String, Object>>() {
                        @Override
                        public boolean filter(Map<String, Object> value) throws Exception {
                            return value != null && !value.isEmpty();
                        }
                    });

            incrementalStream
                    .addSink(JdbcSink.sink(
                            upsertSql,
                            new DbStatementBuilder(config),
                            executionOptions,
                            jdbcOptions
                    )).name("Incremental Sync Sink");

            LOG.info("Enabled incremental sync for table {}.{}", config.getSourceDatabase(), config.getSourceTableName());
        }

        env.execute("Table Sync Job: " + config.getSourceDatabase() + "." + config.getSourceTableName());
    }

    private static String buildInsertSql(TableSyncConfig config) {
        String columnsStr = String.join(", ", config.getColumns());
        String placeholders = String.join(", ", java.util.Collections.nCopies(config.getColumns().length, "?"));
        return String.format("INSERT INTO %s.%s (%s) VALUES (%s)", 
                config.getTargetDatabase(), config.getTargetTableName(), columnsStr, placeholders);
    }

    private static String buildUpsertSql(TableSyncConfig config) {
        String columnsStr = String.join(", ", config.getColumns());
        String placeholders = String.join(", ", java.util.Collections.nCopies(config.getColumns().length, "?"));
        
        StringBuilder updateClause = new StringBuilder();
        for (int i = 0; i < config.getColumns().length; i++) {
            if (!config.getColumns()[i].equals(config.getPrimaryKey())) {
                updateClause.append(config.getColumns()[i]).append(" = VALUES(").append(config.getColumns()[i]).append(")");
                if (i < config.getColumns().length - 1) {
                    updateClause.append(", ");
                }
            }
        }
        
        return String.format("INSERT INTO %s.%s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s", 
                config.getTargetDatabase(), config.getTargetTableName(), columnsStr, placeholders, updateClause.toString());
    }

    private static class DbStatementBuilder implements org.apache.flink.connector.jdbc.JdbcStatementBuilder<Map<String, Object>> {
        private final TableSyncConfig config;

        public DbStatementBuilder(TableSyncConfig config) {
            this.config = config;
        }

        @Override
        public void accept(PreparedStatement ps, Map<String, Object> data) throws SQLException {
            for (int i = 0; i < config.getColumns().length; i++) {
                String column = config.getColumns()[i];
                Object value = data.get(column);
                ps.setObject(i + 1, value);
            }
        }
    }
}