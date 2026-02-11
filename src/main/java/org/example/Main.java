package org.example;

import org.example.config.TableSyncConfig;
import org.example.manager.TableSyncManager;

public class Main {
    public static void main(String[] args) {
        // Database connection parameters
        String sourceDbUrl = "jdbc:mysql://localhost:3306/source_db";
        String sourceDbUsername = "root";
        String sourceDbPassword = "password";
        
        String targetDbUrl = "jdbc:mysql://localhost:3306/target_db";
        String targetDbUsername = "root";
        String targetDbPassword = "password";
        
        // Create table sync manager with max 2 parallel jobs
        TableSyncManager manager = new TableSyncManager(
                2,
                sourceDbUrl,
                sourceDbUsername,
                sourceDbPassword,
                targetDbUrl,
                targetDbUsername,
                targetDbPassword
        );
        
        // Configure sample table sync
        TableSyncConfig userTableConfig = new TableSyncConfig();
        userTableConfig.setSourceTableName("users");
        userTableConfig.setSourceDatabase("source_db");
        userTableConfig.setTargetTableName("users");
        userTableConfig.setTargetDatabase("target_db");
        userTableConfig.setPrimaryKey("id");
        userTableConfig.setColumns(new String[]{"id", "name", "email", "created_at", "updated_at"});
        userTableConfig.setEnableFullSync(true);
        userTableConfig.setEnableIncrementalSync(true);
        userTableConfig.setBatchSize(1000);
        userTableConfig.setParallelism(1);
        
        // Add table sync job
        manager.addTableSync(userTableConfig);
        
        // Add another sample table sync
        TableSyncConfig productTableConfig = new TableSyncConfig();
        productTableConfig.setSourceTableName("products");
        productTableConfig.setSourceDatabase("source_db");
        productTableConfig.setTargetTableName("products");
        productTableConfig.setTargetDatabase("target_db");
        productTableConfig.setPrimaryKey("id");
        productTableConfig.setColumns(new String[]{"id", "name", "price", "stock", "created_at", "updated_at"});
        productTableConfig.setEnableFullSync(true);
        productTableConfig.setEnableIncrementalSync(true);
        productTableConfig.setBatchSize(1000);
        productTableConfig.setParallelism(1);
        
        manager.addTableSync(productTableConfig);
        
        System.out.println("Started table synchronization jobs");
        System.out.println("Active jobs: " + manager.getActiveJobsCount());
        
        // Wait for all jobs to complete (optional)
        // manager.waitForAllJobs();
        
        // Shutdown manager when done (optional)
        // manager.shutdown();
    }
}