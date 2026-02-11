package org.example.config;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableSyncConfig implements Serializable {
    private String sourceTableName;
    private String sourceDatabase;
    private String targetTableName;
    private String targetDatabase;
    private String primaryKey;
    private String[] columns;
    private boolean enableFullSync;
    private boolean enableIncrementalSync;
    private int batchSize = 1000;
    private int parallelism = 1;
}