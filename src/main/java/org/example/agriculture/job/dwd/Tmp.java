package org.example.agriculture.job.dwd;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import com.clickhouse.jdbc.ClickHouseDriver;

public class Tmp {

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
        // 2. Kafka 源表（拆开 struct 字段）
        // -------------------------------
        tEnv.executeSql("CREATE TABLE plant_vision_source (" +
                "greenhouse_id STRING," +
                "`timestamp` BIGINT," +
                "plant_basic ROW<" +
                "  crop_type STRING," +
                "  growth_stage STRING," +
                "  plant_height_cm DOUBLE," +
                "  leaf_count INT," +
                "  canopy_coverage DOUBLE" +
                ">," +
                "plant_health ROW<" +
                "  chlorophyll_index DOUBLE," +
                "  wilting_score DOUBLE," +
                "  disease_risk STRING," +
                "  pest_risk STRING" +
                ">," +
                "fruit_info ROW<" +
                "  fruit_count INT" +
                ">," +
                "ts AS TO_TIMESTAMP_LTZ(`timestamp`, 3)," +
                "WATERMARK FOR ts AS ts - INTERVAL '15' SECOND" +
                ") WITH (" +
                "'connector' = 'kafka'," +
                "'topic' = 'smart_agriculture_plant_vision'," +
                "'properties.bootstrap.servers' = '192.168.1.124:9092'," +
                "'properties.group.id' = 'plant-vision-sql-consumer'," +
                "'format' = 'json'," +
                "'scan.startup.mode' = 'latest-offset'" +
                ")");

        tEnv.executeSql("CREATE TABLE soil_sensor_source (" +
                "greenhouse_id STRING," +
                "`timestamp` BIGINT," +
                "metrics ROW<" +
                "  soil_temperature DOUBLE," +
                "  soil_moisture DOUBLE," +
                "  soil_ec DOUBLE," +
                "  soil_ph DOUBLE," +
                "  soil_n DOUBLE," +
                "  soil_p DOUBLE," +
                "  soil_k DOUBLE" +
                ">," +
                "ts AS TO_TIMESTAMP_LTZ(`timestamp`, 3)," +
                "WATERMARK FOR ts AS ts - INTERVAL '15' SECOND" +
                ") WITH (" +
                "'connector' = 'kafka'," +
                "'topic' = 'smart_agriculture_sensor_dirt'," +
                "'properties.bootstrap.servers' = '192.168.1.124:9092'," +
                "'properties.group.id' = 'soil-sensor-sql-consumer'," +
                "'format' = 'json'," +
                "'scan.startup.mode' = 'latest-offset'" +
                ")");

        tEnv.executeSql("CREATE TABLE env_sensor_source (" +
                "greenhouse_id STRING," +
                "`timestamp` BIGINT," +
                "metrics ROW<" +
                "  temperature DOUBLE," +
                "  humidity DOUBLE," +
                "  co2 INT," +
                "  light INT," +
                "  soil_temperature DOUBLE," +
                "  soil_moisture DOUBLE" +
                ">," +
                "device ROW<" +
                "  device_status STRING" +
                ">," +
                "ts AS TO_TIMESTAMP_LTZ(`timestamp`, 3)," +
                "WATERMARK FOR ts AS ts - INTERVAL '15' SECOND" +
                ") WITH (" +
                "'connector' = 'kafka'," +
                "'topic' = 'smart_agriculture_sensor'," +
                "'properties.bootstrap.servers' = '192.168.1.124:9092'," +
                "'properties.group.id' = 'env-sensor-sql-consumer'," +
                "'format' = 'json'," +
                "'scan.startup.mode' = 'latest-offset'" +
                ")");

        // ClickHouse配置
        String clickhouseUrl = "jdbc:clickhouse://192.168.1.124:8123/default";
        String username = "default";
        String password = "65e84be3";

        // -------------------------------
        // 4. Kafka sink
        // -------------------------------
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
                "humidity DOUBLE," +
                "PRIMARY KEY (greenhouse_id) NOT ENFORCED" +
                ") WITH (" +
                "'connector' = 'upsert-kafka'," +
                "'topic' = 'dwd_plant_health_detail'," +
                "'properties.bootstrap.servers' = '192.168.1.124:9092'," +
                "'key.format' = 'json'," +
                "'value.format' = 'json'" +
                ")");

// -------------------------------
// 4. ClickHouse sink
// -------------------------------
        tEnv.executeSql(
                "CREATE TABLE clickhouse_sink (" +
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
                        "'connector' = 'jdbc'," +
                        "'url' = 'jdbc:clickhouse://192.168.1.124:8123/default'," +
                        "'table-name' = 'dwd_greenhouse_plant_health_detail'," +
                        "'driver' = 'com.clickhouse.jdbc.ClickHouseDriver'," +
                        "'username' = 'default'," +
                        "'password' = '65e84be3'" +
                        ")"
        );

// -------------------------------
// 5. Interval Join SQL
// -------------------------------
        String sqlIntervalJoin =
                "SELECT pv.greenhouse_id," +
                        "       pv.ts," +
                        "       pv.plant_basic.crop_type AS crop_type," +
                        "       pv.plant_basic.growth_stage AS growth_stage," +
                        "       pv.plant_basic.plant_height_cm AS plant_height_cm," +
                        "       pv.plant_basic.leaf_count AS leaf_count," +
                        "       pv.plant_basic.canopy_coverage AS canopy_coverage," +
                        "       pv.plant_health.chlorophyll_index AS chlorophyll_index," +
                        "       pv.plant_health.wilting_score AS wilting_score," +
                        "       pv.fruit_info.fruit_count AS fruit_count," +
                        "       pv.plant_health.disease_risk AS disease_risk," +
                        "       pv.plant_health.pest_risk AS pest_risk," +
                        "       ss.metrics.soil_moisture AS soil_moisture," +
                        "       ss.metrics.soil_ec AS soil_ec," +
                        "       ss.metrics.soil_ph AS soil_ph," +
                        "       es.metrics.temperature AS temperature," +
                        "       es.metrics.humidity AS humidity " +
                        "FROM plant_vision_source AS pv " +
                        "LEFT JOIN soil_sensor_source AS ss " +
                        "  ON pv.greenhouse_id = ss.greenhouse_id " +
                        " AND ss.ts BETWEEN pv.ts - INTERVAL '15' SECOND AND pv.ts + INTERVAL '5' SECOND " +
                        "LEFT JOIN env_sensor_source AS es " +
                        "  ON pv.greenhouse_id = es.greenhouse_id " +
                        " AND es.ts BETWEEN pv.ts - INTERVAL '15' SECOND AND pv.ts + INTERVAL '5' SECOND";

// -------------------------------
// 6. 写入 Kafka & ClickHouse 双 Sink
// -------------------------------
// 写 Kafka
//        tEnv.executeSql("INSERT INTO kafka_sink " + sqlIntervalJoin);

// 写 ClickHouse
        tEnv.executeSql("INSERT INTO clickhouse_sink " + sqlIntervalJoin);
    }
}