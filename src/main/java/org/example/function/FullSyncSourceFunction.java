package org.example.function;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.example.config.TableSyncConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

public class FullSyncSourceFunction extends RichSourceFunction<Map<String, Object>> {
    private static final Logger LOG = LoggerFactory.getLogger(FullSyncSourceFunction.class);
    private final TableSyncConfig config;
    private final String dbUrl;
    private final String dbUsername;
    private final String dbPassword;
    private Connection connection;
    private PreparedStatement statement;
    private boolean running = true;

    public FullSyncSourceFunction(TableSyncConfig config, String dbUrl, String dbUsername, String dbPassword) {
        this.config = config;
        this.dbUrl = dbUrl;
        this.dbUsername = dbUsername;
        this.dbPassword = dbPassword;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = DriverManager.getConnection(dbUrl, dbUsername, dbPassword);
        String columnsStr = String.join(", ", config.getColumns());
        String query = String.format("SELECT %s FROM %s.%s", columnsStr, config.getSourceDatabase(), config.getSourceTableName());
        statement = connection.prepareStatement(query);
        LOG.info("Opened connection for full sync of table {}.{}", config.getSourceDatabase(), config.getSourceTableName());
    }

    @Override
    public void run(SourceContext<Map<String, Object>> ctx) throws Exception {
        int retryCount = 0;
        final int maxRetries = 3;
        
        while (retryCount < maxRetries && running) {
            try {
                ResultSet resultSet = statement.executeQuery();
                int count = 0;
                
                while (resultSet.next() && running) {
                    Map<String, Object> row = new HashMap<>();
                    for (String column : config.getColumns()) {
                        try {
                            row.put(column, resultSet.getObject(column));
                        } catch (Exception e) {
                            LOG.warn("Error reading column {}: {}", column, e.getMessage());
                            row.put(column, null);
                        }
                    }
                    synchronized (ctx.getCheckpointLock()) {
                        ctx.collect(row);
                        count++;
                    }
                }
                
                LOG.info("Full sync completed for table {}.{}, processed {} rows", 
                         config.getSourceDatabase(), config.getSourceTableName(), count);
                break;
            } catch (Exception e) {
                retryCount++;
                if (retryCount < maxRetries) {
                    LOG.warn("Full sync failed, retrying {}/{}", retryCount, maxRetries, e);
                    Thread.sleep(1000 * retryCount);
                } else {
                    LOG.error("Full sync failed after {} retries", maxRetries, e);
                    throw e;
                }
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
        LOG.info("Full sync cancelled for table {}.{}", config.getSourceDatabase(), config.getSourceTableName());
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (statement != null) {
            statement.close();
        }
        if (connection != null) {
            connection.close();
        }
        LOG.info("Closed connection for full sync of table {}.{}", config.getSourceDatabase(), config.getSourceTableName());
    }
}