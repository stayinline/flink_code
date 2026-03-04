package org.example.agriculture.job.dwd;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class PlantHealthDetailJobV2SQL {

    public static void main(String[] args) throws Exception {
        // 设置执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000); // 每5秒一次checkpoint

        // 创建表环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        // 注册Kafka数据源表 - 植物视觉数据
        tEnv.executeSql("CREATE TABLE plant_vision_source (" +
                "greenhouse_id STRING," +
                "`timestamp` BIGINT," +
                "plant_basic STRUCT<" +
                "crop_type STRING," +
                "growth_stage STRING," +
                "plant_height_cm DOUBLE," +
                "leaf_count INT," +
                "canopy_coverage DOUBLE" +
                ")," +
                "plant_health STRUCT<" +
                "chlorophyll_index DOUBLE," +
                "wilting_score DOUBLE," +
                "disease_risk STRING," +
                "pest_risk STRING" +
                ")," +
                "fruit_info STRUCT<" +
                "fruit_count INT" +
                ")," +
                "WATERMARK FOR TO_TIMESTAMP_LTZ(`timestamp`, 3) AS TO_TIMESTAMP_LTZ(`timestamp`, 3) - INTERVAL '15' SECOND" +
                ") WITH (" +
                "'connector' = 'kafka'," +
                "'topic' = 'smart_agriculture_plant_vision'," +
                "'properties.bootstrap.servers' = '192.168.1.124:9092'," +
                "'properties.group.id' = 'plant-vision-sql-consumer'," +
                "'format' = 'json'," +
                "'scan.startup.mode' = 'latest-offset'" +
                ")");

        // 注册Kafka数据源表 - 土壤传感器数据
        tEnv.executeSql("CREATE TABLE soil_sensor_source (" +
                "greenhouse_id STRING," +
                "sensor_id STRING," +
                "`timestamp` BIGINT," +
                "metrics STRUCT<" +
                "soil_temperature DOUBLE," +
                "soil_moisture DOUBLE," +
                "soil_ec DOUBLE," +
                "soil_ph DOUBLE," +
                "soil_n DOUBLE," +
                "soil_p DOUBLE," +
                "soil_k DOUBLE" +
                ")," +
                "WATERMARK FOR TO_TIMESTAMP_LTZ(`timestamp`, 3) AS TO_TIMESTAMP_LTZ(`timestamp`, 3) - INTERVAL '15' SECOND" +
                ") WITH (" +
                "'connector' = 'kafka'," +
                "'topic' = 'smart_agriculture_sensor_dirt'," +
                "'properties.bootstrap.servers' = '192.168.1.124:9092'," +
                "'properties.group.id' = 'soil-sensor-sql-consumer'," +
                "'format' = 'json'," +
                "'scan.startup.mode' = 'latest-offset'" +
                ")");

        // 注册Kafka数据源表 - 环境传感器数据
        tEnv.executeSql("CREATE TABLE env_sensor_source (" +
                "greenhouse_id STRING," +
                "sensor_id STRING," +
                "`timestamp` BIGINT," +
                "metrics STRUCT<" +
                "temperature DOUBLE," +
                "humidity DOUBLE," +
                "co2 INT," +
                "light INT," +
                "soil_temperature DOUBLE," +
                "soil_moisture DOUBLE" +
                ")," +
                "device STRUCT<" +
                "status STRING" +
                ")," +
                "WATERMARK FOR TO_TIMESTAMP_LTZ(`timestamp`, 3) AS TO_TIMESTAMP_LTZ(`timestamp`, 3) - INTERVAL '15' SECOND" +
                ") WITH (" +
                "'connector' = 'kafka'," +
                "'topic' = 'smart_agriculture_sensor'," +
                "'properties.bootstrap.servers' = '192.168.1.124:9092'," +
                "'properties.group.id' = 'env-sensor-sql-consumer'," +
                "'format' = 'json'," +
                "'scan.startup.mode' = 'latest-offset'" +
                ")");

        // 注册ClickHouse结果表
        tEnv.executeSql("CREATE TABLE clickhouse_sink (" +
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
                "PRIMARY KEY (greenhouse_id, ts) NOT ENFORCED" +
                ") WITH (" +
                "'connector' = 'jdbc'," +
                "'url' = 'jdbc:clickhouse://192.168.1.124:8123/default'," +
                "'table-name' = 'dwd_greenhouse_plant_health_detail'," +
                "'username' = 'default'," +
                "'password' = '65e84be3'," +
                "'driver' = 'com.clickhouse.jdbc.ClickHouseDriver'," +
                "'sink.buffer-flush.max-rows' = '1000'," +
                "'sink.buffer-flush.interval' = '1000'," +
                "'sink.max-retries' = '3'" +
                ")");

        // 注册Kafka结果表（旁路输出）
        tEnv.executeSql("CREATE TABLE kafka_sink (" +
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
                "humidity DOUBLE" +
                ") WITH (" +
                "'connector' = 'kafka'," +
                "'topic' = 'dwd_plant_health_detail'," +
                "'properties.bootstrap.servers' = '192.168.1.124:9092'," +
                "'format' = 'json'," +
                "'sink.partitioner' = 'round-robin'" +
                ")");

        // 执行SQL查询，进行数据处理和JOIN
        String sql = "WITH plant_vision AS (" +
                "SELECT " +
                "greenhouse_id, " +
                "TO_TIMESTAMP_LTZ(`timestamp`, 3) AS ts, " +
                "plant_basic.crop_type, " +
                "plant_basic.growth_stage, " +
                "plant_basic.plant_height_cm, " +
                "plant_basic.leaf_count, " +
                "plant_basic.canopy_coverage, " +
                "plant_health.chlorophyll_index, " +
                "plant_health.wilting_score, " +
                "fruit_info.fruit_count, " +
                "plant_health.disease_risk, " +
                "plant_health.pest_risk " +
                "FROM plant_vision_source " +
                "WHERE plant_basic IS NOT NULL AND plant_health IS NOT NULL AND fruit_info IS NOT NULL " +
                "), " +
                "soil_data AS (" +
                "SELECT " +
                "greenhouse_id, " +
                "TO_TIMESTAMP_LTZ(`timestamp`, 3) AS ts, " +
                "metrics.soil_temperature, " +
                "metrics.soil_moisture, " +
                "metrics.soil_ec, " +
                "metrics.soil_ph, " +
                "metrics.soil_n, " +
                "metrics.soil_p, " +
                "metrics.soil_k, " +
                "ROW_NUMBER() OVER (PARTITION BY greenhouse_id ORDER BY TO_TIMESTAMP_LTZ(`timestamp`, 3) DESC) AS rn " +
                "FROM soil_sensor_source " +
                "WHERE metrics IS NOT NULL " +
                "), " +
                "latest_soil AS (" +
                "SELECT * FROM soil_data WHERE rn = 1 " +
                "), " +
                "env_data AS (" +
                "SELECT " +
                "greenhouse_id, " +
                "TO_TIMESTAMP_LTZ(`timestamp`, 3) AS ts, " +
                "metrics.temperature, " +
                "metrics.humidity, " +
                "metrics.co2, " +
                "metrics.light, " +
                "metrics.soil_temperature, " +
                "metrics.soil_moisture, " +
                "device.status, " +
                "ROW_NUMBER() OVER (PARTITION BY greenhouse_id ORDER BY TO_TIMESTAMP_LTZ(`timestamp`, 3) DESC) AS rn " +
                "FROM env_sensor_source " +
                "WHERE metrics IS NOT NULL AND device IS NOT NULL " +
                "), " +
                "latest_env AS (" +
                "SELECT * FROM env_data WHERE rn = 1 " +
                ") " +
                "INSERT INTO clickhouse_sink " +
                "SELECT " +
                "pv.greenhouse_id, " +
                "pv.ts, " +
                "pv.crop_type, " +
                "pv.growth_stage, " +
                "pv.plant_height_cm, " +
                "pv.leaf_count, " +
                "pv.canopy_coverage, " +
                "pv.chlorophyll_index, " +
                "pv.wilting_score, " +
                "pv.fruit_count, " +
                "pv.disease_risk, " +
                "pv.pest_risk, " +
                "ls.soil_moisture, " +
                "ls.soil_ec, " +
                "ls.soil_ph, " +
                "le.temperature, " +
                "le.humidity " +
                "FROM plant_vision pv " +
                "LEFT JOIN latest_soil ls ON pv.greenhouse_id = ls.greenhouse_id " +
                "LEFT JOIN latest_env le ON pv.greenhouse_id = le.greenhouse_id " +
                "WHERE ls.greenhouse_id IS NOT NULL AND le.greenhouse_id IS NOT NULL;";

        tEnv.executeSql(sql);

        // 同时写入Kafka旁路输出
        String kafkaSql = "WITH plant_vision AS (" +
                "SELECT " +
                "greenhouse_id, " +
                "TO_TIMESTAMP_LTZ(`timestamp`, 3) AS ts, " +
                "plant_basic.crop_type, " +
                "plant_basic.growth_stage, " +
                "plant_basic.plant_height_cm, " +
                "plant_basic.leaf_count, " +
                "plant_basic.canopy_coverage, " +
                "plant_health.chlorophyll_index, " +
                "plant_health.wilting_score, " +
                "fruit_info.fruit_count, " +
                "plant_health.disease_risk, " +
                "plant_health.pest_risk " +
                "FROM plant_vision_source " +
                "WHERE plant_basic IS NOT NULL AND plant_health IS NOT NULL AND fruit_info IS NOT NULL " +
                "), " +
                "soil_data AS (" +
                "SELECT " +
                "greenhouse_id, " +
                "TO_TIMESTAMP_LTZ(`timestamp`, 3) AS ts, " +
                "metrics.soil_temperature, " +
                "metrics.soil_moisture, " +
                "metrics.soil_ec, " +
                "metrics.soil_ph, " +
                "metrics.soil_n, " +
                "metrics.soil_p, " +
                "metrics.soil_k, " +
                "ROW_NUMBER() OVER (PARTITION BY greenhouse_id ORDER BY TO_TIMESTAMP_LTZ(`timestamp`, 3) DESC) AS rn " +
                "FROM soil_sensor_source " +
                "WHERE metrics IS NOT NULL " +
                "), " +
                "latest_soil AS (" +
                "SELECT * FROM soil_data WHERE rn = 1 " +
                "), " +
                "env_data AS (" +
                "SELECT " +
                "greenhouse_id, " +
                "TO_TIMESTAMP_LTZ(`timestamp`, 3) AS ts, " +
                "metrics.temperature, " +
                "metrics.humidity, " +
                "metrics.co2, " +
                "metrics.light, " +
                "metrics.soil_temperature, " +
                "metrics.soil_moisture, " +
                "device.status, " +
                "ROW_NUMBER() OVER (PARTITION BY greenhouse_id ORDER BY TO_TIMESTAMP_LTZ(`timestamp`, 3) DESC) AS rn " +
                "FROM env_sensor_source " +
                "WHERE metrics IS NOT NULL AND device IS NOT NULL " +
                "), " +
                "latest_env AS (" +
                "SELECT * FROM env_data WHERE rn = 1 " +
                ") " +
                "INSERT INTO kafka_sink " +
                "SELECT " +
                "pv.greenhouse_id, " +
                "pv.ts, " +
                "pv.crop_type, " +
                "pv.growth_stage, " +
                "pv.plant_height_cm, " +
                "pv.leaf_count, " +
                "pv.canopy_coverage, " +
                "pv.chlorophyll_index, " +
                "pv.wilting_score, " +
                "pv.fruit_count, " +
                "pv.disease_risk, " +
                "pv.pest_risk, " +
                "ls.soil_moisture, " +
                "ls.soil_ec, " +
                "ls.soil_ph, " +
                "le.temperature, " +
                "le.humidity " +
                "FROM plant_vision pv " +
                "LEFT JOIN latest_soil ls ON pv.greenhouse_id = ls.greenhouse_id " +
                "LEFT JOIN latest_env le ON pv.greenhouse_id = le.greenhouse_id " +
                "WHERE ls.greenhouse_id IS NOT NULL AND le.greenhouse_id IS NOT NULL;";

        tEnv.executeSql(kafkaSql);

        // 执行作业
        env.execute("DWD Greenhouse Plant Health Detail Processing (SQL)");
    }
}
