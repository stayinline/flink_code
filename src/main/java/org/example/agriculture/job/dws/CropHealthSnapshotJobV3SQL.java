package org.example.agriculture.job.dws;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class CropHealthSnapshotJobV3SQL {

    public static void main(String[] args) throws Exception {
        // -------------------------------
        // 1. Flink 执行环境
        // -------------------------------
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000); // 每5秒一次 checkpoint

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        // -------------------------------
        // 2. 注册 Kafka 数据源表
        // -------------------------------
        tEnv.executeSql(
                "CREATE TABLE plant_health_source (" +
                        "greenhouse_id STRING," +
                        "ts TIMESTAMP(3)," +
                        "crop_type STRING," +
                        "growth_stage STRING," +
                        "plant_height_cm DOUBLE," +
                        "leaf_count INT," +
                        "canopy_coverage DOUBLE," +
                        "chlorophyll_index DOUBLE," +
                        "wilting_score DOUBLE," +
                        "fruit_count INT," +
                        "disease_risk STRING," +
                        "pest_risk STRING," +
                        "soil_moisture DOUBLE," +
                        "soil_ec DOUBLE," +
                        "soil_ph DOUBLE," +
                        "temperature DOUBLE," +
                        "humidity DOUBLE," +
                        "WATERMARK FOR ts AS ts - INTERVAL '15' SECOND" +
                        ") WITH (" +
                        "'connector' = 'kafka'," +
                        "'topic' = 'dwd_plant_health_detail'," +
                        "'properties.bootstrap.servers' = '192.168.1.124:9092'," +
                        "'properties.group.id' = 'crop-health-snapshot-sql-consumer'," +
                        "'format' = 'json'," +
                        "'scan.startup.mode' = 'latest-offset'" +
                        ")"
        );

        tEnv.executeSql(
                "CREATE TABLE soil_source (" +
                        "greenhouse_id STRING," +
                        "sensor_id STRING," +
                        "ts TIMESTAMP(3)," +
                        "soil_temperature DOUBLE," +
                        "soil_moisture DOUBLE," +
                        "soil_ec DOUBLE," +
                        "soil_ph DOUBLE," +
                        "soil_n DOUBLE," +
                        "soil_p DOUBLE," +
                        "soil_k DOUBLE," +
                        "WATERMARK FOR ts AS ts - INTERVAL '15' SECOND" +
                        ") WITH (" +
                        "'connector' = 'kafka'," +
                        "'topic' = 'dwd_soil_detail'," +
                        "'properties.bootstrap.servers' = '192.168.1.124:9092'," +
                        "'properties.group.id' = 'crop-health-snapshot-sql-consumer'," +
                        "'format' = 'json'," +
                        "'scan.startup.mode' = 'latest-offset'" +
                        ")"
        );

        tEnv.executeSql(
                "CREATE TABLE farm_operation_source (" +
                        "greenhouse_id STRING," +
                        "`timestamp` BIGINT," +
                        "operation_type STRING" +
                        ") WITH (" +
                        "'connector' = 'kafka'," +
                        "'topic' = 'dwd_farm_operation'," +
                        "'properties.bootstrap.servers' = '192.168.1.124:9092'," +
                        "'properties.group.id' = 'crop-health-snapshot-sql-consumer'," +
                        "'format' = 'json'," +
                        "'scan.startup.mode' = 'latest-offset'" +
                        ")"
        );

        // -------------------------------
        // 3. 注册 Upsert Kafka sink
        // -------------------------------
        tEnv.executeSql(
                "CREATE TABLE kafka_sink (" +
                        "greenhouse_id STRING," +
                        "stat_time TIMESTAMP(3)," +
                        "crop_type STRING," +
                        "growth_stage STRING," +
                        "avg_plant_height DOUBLE," +
                        "avg_leaf_count DOUBLE," +
                        "avg_chlorophyll DOUBLE," +
                        "avg_soil_moisture DOUBLE," +
                        "avg_soil_ec DOUBLE," +
                        "disease_risk_rate DOUBLE," +
                        "pest_risk_rate DOUBLE," +
                        "last_operation_type STRING," +
                        "last_operation_time TIMESTAMP(3)," +
                        "PRIMARY KEY (greenhouse_id) NOT ENFORCED" +
                        ") WITH (" +
                        "'connector' = 'upsert-kafka'," +
                        "'topic' = 'dws_crop_health_snapshot'," +
                        "'properties.bootstrap.servers' = '192.168.1.124:9092'," +
                        "'key.format' = 'json'," +
                        "'value.format' = 'json'" +
                        ")"
        );

        // -------------------------------
        // 4. 注册 ClickHouse sink
        // -------------------------------
        tEnv.executeSql(
                "CREATE TABLE clickhouse_sink (" +
                        "greenhouse_id STRING," +
                        "stat_time TIMESTAMP(3)," +
                        "crop_type STRING," +
                        "growth_stage STRING," +
                        "avg_plant_height DOUBLE," +
                        "avg_leaf_count DOUBLE," +
                        "avg_chlorophyll DOUBLE," +
                        "avg_soil_moisture DOUBLE," +
                        "avg_soil_ec DOUBLE," +
                        "disease_risk_rate DOUBLE," +
                        "pest_risk_rate DOUBLE," +
                        "last_operation_type STRING," +
                        "last_operation_time TIMESTAMP(3)" +
                        ") WITH (" +
                        "'connector' = 'jdbc'," +
                        "'url' = 'jdbc:clickhouse://192.168.1.124:8123'," +
                        "'table-name' = 'default.dws_crop_health_snapshot'," +
                        "'username' = 'default'," +
                        "'password' = '65e84be3'" +
                        ")"
        );

        // -------------------------------
        // 5. 数据处理 SQL
        // -------------------------------
        String processSql =
                "WITH plant_health_with_soil AS (" +
                        "    SELECT ph.greenhouse_id," +
                        "           ph.ts," +
                        "           ph.crop_type," +
                        "           ph.growth_stage," +
                        "           ph.plant_height_cm," +
                        "           ph.leaf_count," +
                        "           ph.chlorophyll_index," +
                        "           s.soil_moisture," +
                        "           s.soil_ec," +
                        "           CASE UPPER(ph.disease_risk) " +
                        "                WHEN 'LOW' THEN 0.1 " +
                        "                WHEN 'MEDIUM' THEN 0.5 " +
                        "                WHEN 'HIGH' THEN 0.9 " +
                        "                ELSE 0.0 END AS disease_risk_rate," +
                        "           CASE UPPER(ph.pest_risk) " +
                        "                WHEN 'LOW' THEN 0.1 " +
                        "                WHEN 'MEDIUM' THEN 0.5 " +
                        "                WHEN 'HIGH' THEN 0.9 " +
                        "                ELSE 0.0 END AS pest_risk_rate " +
                        "    FROM plant_health_source ph " +
                        "    JOIN soil_source s ON ph.greenhouse_id = s.greenhouse_id" +
                        "), " +
                        "latest_farm_operation AS (" +
                        "    SELECT greenhouse_id," +
                        "           operation_type," +
                        "           TO_TIMESTAMP_LTZ(`timestamp`, 3) AS operation_time," +
                        "           ROW_NUMBER() OVER (PARTITION BY greenhouse_id ORDER BY `timestamp` DESC) AS rn " +
                        "    FROM farm_operation_source" +
                        ") " +
                        "SELECT phs.greenhouse_id," +
                        "       phs.ts AS stat_time," +
                        "       phs.crop_type," +
                        "       phs.growth_stage," +
                        "       phs.plant_height_cm AS avg_plant_height," +
                        "       phs.leaf_count AS avg_leaf_count," +
                        "       phs.chlorophyll_index AS avg_chlorophyll," +
                        "       phs.soil_moisture AS avg_soil_moisture," +
                        "       phs.soil_ec AS avg_soil_ec," +
                        "       phs.disease_risk_rate," +
                        "       phs.pest_risk_rate," +
                        "       lfo.operation_type AS last_operation_type," +
                        "       lfo.operation_time AS last_operation_time " +
                        "FROM plant_health_with_soil phs " +
                        "LEFT JOIN latest_farm_operation lfo " +
                        "ON phs.greenhouse_id = lfo.greenhouse_id AND lfo.rn = 1 " +
                        "WHERE lfo.greenhouse_id IS NOT NULL";

        // -------------------------------
        // 6. 双写
        // -------------------------------
        // 写 Kafka
        tEnv.executeSql("INSERT INTO kafka_sink " + processSql);

        // 写 ClickHouse
//        tEnv.executeSql("INSERT INTO clickhouse_sink " + processSql);

        // -------------------------------
        // 7. 执行 Flink 作业
        // -------------------------------
        env.execute("DWS Greenhouse Crop Health Snapshot Processing (Kafka + ClickHouse)");
    }
}