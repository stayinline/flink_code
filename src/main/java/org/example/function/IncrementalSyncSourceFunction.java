package org.example.function;

import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.example.config.TableSyncConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class IncrementalSyncSourceFunction {
    private static final Logger LOG = LoggerFactory.getLogger(IncrementalSyncSourceFunction.class);

    public static SourceFunction<String> createSource(TableSyncConfig config, String dbUrl, String dbUsername, String dbPassword) {
        // Extract MySQL host and port from URL
        String host = extractHostFromUrl(dbUrl);
        int port = extractPortFromUrl(dbUrl);
        
        Properties debeziumProperties = new Properties();
        debeziumProperties.setProperty("debezium.snapshot.locking.mode", "none");
        debeziumProperties.setProperty("debezium.binlog.error.handling.mode", "skip");
        
        // Create the source with explicit type parameters
        SourceFunction<String> source = (SourceFunction<String>) MySqlSource.builder()
                .hostname(host)
                .port(port)
                .databaseList(config.getSourceDatabase())
                .tableList(config.getSourceDatabase() + "." + config.getSourceTableName())
                .username(dbUsername)
                .password(dbPassword)
                .startupOptions(StartupOptions.latest())
                .deserializer((DebeziumDeserializationSchema) new JsonDebeziumDeserializationSchema())
                .debeziumProperties(debeziumProperties)
                .build();
        
        LOG.info("Created MySQL CDC source for incremental sync of table {}.{}", 
                 config.getSourceDatabase(), config.getSourceTableName());
        
        return source;
    }

    private static String extractHostFromUrl(String url) {
        int start = url.indexOf("//") + 2;
        int end = url.indexOf(":", start);
        if (end == -1) {
            end = url.indexOf("?", start);
            if (end == -1) {
                end = url.length();
            }
        }
        return url.substring(start, end);
    }

    private static int extractPortFromUrl(String url) {
        int start = url.indexOf(":", url.indexOf("//") + 2);
        if (start == -1) {
            return 3306; // Default MySQL port
        }
        start += 1;
        int end = url.indexOf("?", start);
        if (end == -1) {
            end = url.indexOf("/", start);
            if (end == -1) {
                end = url.length();
            }
        }
        try {
            return Integer.parseInt(url.substring(start, end));
        } catch (NumberFormatException e) {
            return 3306;
        }
    }
}