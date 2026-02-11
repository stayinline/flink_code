package org.example.manager;

import org.example.config.TableSyncConfig;
import org.example.job.TableSyncJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class TableSyncManager {
    private static final Logger LOG = LoggerFactory.getLogger(TableSyncManager.class);
    private final ExecutorService executorService;
    private final List<Future<?>> jobFutures;
    private final String sourceDbUrl;
    private final String sourceDbUsername;
    private final String sourceDbPassword;
    private final String targetDbUrl;
    private final String targetDbUsername;
    private final String targetDbPassword;

    public TableSyncManager(int maxParallelJobs, String sourceDbUrl, String sourceDbUsername, String sourceDbPassword, 
                          String targetDbUrl, String targetDbUsername, String targetDbPassword) {
        this.executorService = Executors.newFixedThreadPool(maxParallelJobs);
        this.jobFutures = new ArrayList<>();
        this.sourceDbUrl = sourceDbUrl;
        this.sourceDbUsername = sourceDbUsername;
        this.sourceDbPassword = sourceDbPassword;
        this.targetDbUrl = targetDbUrl;
        this.targetDbUsername = targetDbUsername;
        this.targetDbPassword = targetDbPassword;
    }

    public void addTableSync(TableSyncConfig config) {
        Future<?> future = executorService.submit(() -> {
            try {
                LOG.info("Starting sync job for table {}.{}", config.getSourceDatabase(), config.getSourceTableName());
                TableSyncJob.run(config, sourceDbUrl, sourceDbUsername, sourceDbPassword, 
                                targetDbUrl, targetDbUsername, targetDbPassword);
                LOG.info("Completed sync job for table {}.{}", config.getSourceDatabase(), config.getSourceTableName());
            } catch (Exception e) {
                LOG.error("Failed to run sync job for table {}.{}", config.getSourceDatabase(), config.getSourceTableName(), e);
            }
        });
        jobFutures.add(future);
        LOG.info("Added sync job for table {}.{} to queue", config.getSourceDatabase(), config.getSourceTableName());
    }

    public void addMultipleTableSyncs(List<TableSyncConfig> configs) {
        for (TableSyncConfig config : configs) {
            addTableSync(config);
        }
        LOG.info("Added {} sync jobs to queue", configs.size());
    }

    public void shutdown() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
        LOG.info("TableSyncManager shutdown complete");
    }

    public int getActiveJobsCount() {
        int active = 0;
        for (Future<?> future : jobFutures) {
            if (!future.isDone()) {
                active++;
            }
        }
        return active;
    }

    public int getCompletedJobsCount() {
        int completed = 0;
        for (Future<?> future : jobFutures) {
            if (future.isDone()) {
                completed++;
            }
        }
        return completed;
    }

    public void waitForAllJobs() {
        for (Future<?> future : jobFutures) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                LOG.error("Error waiting for job completion", e);
            }
        }
        LOG.info("All sync jobs completed");
    }
}