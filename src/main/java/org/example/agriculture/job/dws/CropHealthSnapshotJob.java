package org.example.agriculture.job.dws;

import com.clickhouse.jdbc.ClickHouseDriver;
import lombok.Data;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Properties;

public class CropHealthSnapshotJob {

    public static void main(String[] args) throws Exception {
        // 设置执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000); // 每5秒一次checkpoint

        // ClickHouse配置
        String clickhouseUrl = "jdbc:clickhouse://192.168.1.124:8123/default";
        String username = "default";
        String password = "65e84be3";

        // 从DWD表读取植物健康详情数据（10分钟时间窗口）
        int timeWindowMinutes = 10;
        DataStream<DwdPlantHealthDetail> dwdPlantHealthStream = readDwdPlantHealthData(env, clickhouseUrl, username, password, timeWindowMinutes);

        // 从DWD表读取土壤详情数据（10分钟时间窗口）
        DataStream<DwdSoilDetail> dwdSoilStream = readDwdSoilData(env, clickhouseUrl, username, password, timeWindowMinutes);

        // 从ODS表读取农事操作数据（因为DWD层可能没有农事操作表）
        DataStream<FarmOperationRecord> farmOperationStream = readFarmOperationData(env, clickhouseUrl, username, password);

        // 定义Watermark策略
        WatermarkStrategy<DwdPlantHealthDetail> plantHealthWm = WatermarkStrategy
                .<DwdPlantHealthDetail>forBoundedOutOfOrderness(Duration.ofSeconds(15))
                .withTimestampAssigner(new SerializableTimestampAssigner<DwdPlantHealthDetail>() {
                    @Override
                    public long extractTimestamp(DwdPlantHealthDetail element, long recordTimestamp) {
                        return element.getTs().atZone(java.time.ZoneId.systemDefault()).toEpochSecond() * 1000;
                    }
                })
                .withIdleness(Duration.ofMinutes(1));

        WatermarkStrategy<DwdSoilDetail> soilWm = WatermarkStrategy
                .<DwdSoilDetail>forBoundedOutOfOrderness(Duration.ofSeconds(15))
                .withTimestampAssigner(new SerializableTimestampAssigner<DwdSoilDetail>() {
                    @Override
                    public long extractTimestamp(DwdSoilDetail element, long recordTimestamp) {
                        return element.getTs().atZone(java.time.ZoneId.systemDefault()).toEpochSecond() * 1000;
                    }
                })
                .withIdleness(Duration.ofMinutes(1));

        WatermarkStrategy<FarmOperationRecord> farmOperationWm = WatermarkStrategy
                .<FarmOperationRecord>forBoundedOutOfOrderness(Duration.ofSeconds(15))
                .withTimestampAssigner(new SerializableTimestampAssigner<FarmOperationRecord>() {
                    @Override
                    public long extractTimestamp(FarmOperationRecord element, long recordTimestamp) {
                        return element.getTimestamp() * 1000;
                    }
                })
                .withIdleness(Duration.ofMinutes(1));

        // 应用Watermark策略
        DataStream<DwdPlantHealthDetail> plantHealthWithWm = dwdPlantHealthStream.assignTimestampsAndWatermarks(plantHealthWm);
        DataStream<DwdSoilDetail> soilWithWm = dwdSoilStream.assignTimestampsAndWatermarks(soilWm);
        DataStream<FarmOperationRecord> farmOperationWithWm = farmOperationStream.assignTimestampsAndWatermarks(farmOperationWm);

        // 进行数据聚合和加工
        DataStream<DwsCropHealthSnapshot> dwsStream = plantHealthWithWm
                .keyBy(DwdPlantHealthDetail::getGreenhouseId)
                .process(new ProcessFunction<DwdPlantHealthDetail, DwsCropHealthSnapshot>() {
                    @Override
                    public void processElement(DwdPlantHealthDetail plantHealth, Context ctx, Collector<DwsCropHealthSnapshot> out) throws Exception {
                        // 计算风险比例
                        double diseaseRiskRate = calculateRiskRate(plantHealth.getDiseaseRisk());
                        double pestRiskRate = calculateRiskRate(plantHealth.getPestRisk());

                        // 构建DWS数据（这里简化处理，实际应该根据时间窗口进行聚合）
                        DwsCropHealthSnapshot dwsData = new DwsCropHealthSnapshot(
                                plantHealth.getGreenhouseId(),
                                plantHealth.getTs(),
                                plantHealth.getCropType(),
                                plantHealth.getGrowthStage(),
                                plantHealth.getPlantHeightCm(),
                                plantHealth.getLeafCount(),
                                plantHealth.getChlorophyllIndex(),
                                plantHealth.getSoilMoisture(),
                                plantHealth.getSoilEc(),
                                diseaseRiskRate,
                                pestRiskRate,
                                null, // 后续需要与农事操作数据JOIN
                                null
                        );

                        out.collect(dwsData);
                    }
                });

        // 创建ClickHouse Sink
        String insertSql = "INSERT INTO dws_greenhouse_crop_health_snapshot " +
                "(greenhouse_id, stat_time, crop_type, growth_stage, avg_plant_height, avg_leaf_count, avg_chlorophyll, " +
                "avg_soil_moisture, avg_soil_ec, disease_risk_rate, pest_risk_rate, " +
                "last_operation_type, last_operation_time) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        // 配置JDBC连接参数
        JdbcConnectionOptions jdbcOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(clickhouseUrl)
                .withDriverName(ClickHouseDriver.class.getName())
                .withUsername(username)
                .withPassword(password)
                .withConnectionCheckTimeoutSeconds(60)
                .build();

        // 配置执行选项（批量插入优化）
        JdbcExecutionOptions executionOptions = JdbcExecutionOptions.builder()
                .withBatchSize(1000) // 批量大小
                .withBatchIntervalMs(1000) // 批量间隔
                .withMaxRetries(3) // 重试次数
                .build();

        // 添加ClickHouse Sink
        SinkFunction<DwsCropHealthSnapshot> sink = JdbcSink.sink(
                insertSql,
                (PreparedStatement ps, DwsCropHealthSnapshot data) -> {
                    ps.setString(1, data.getGreenhouseId());
                    ps.setTimestamp(2, Timestamp.valueOf(data.getStatTime()));
                    ps.setString(3, data.getCropType());
                    ps.setString(4, data.getGrowthStage());
                    ps.setDouble(5, data.getAvgPlantHeight());
                    ps.setDouble(6, data.getAvgLeafCount());
                    ps.setDouble(7, data.getAvgChlorophyll());
                    ps.setDouble(8, data.getAvgSoilMoisture());
                    ps.setDouble(9, data.getAvgSoilEc());
                    ps.setDouble(10, data.getDiseaseRiskRate());
                    ps.setDouble(11, data.getPestRiskRate());
                    ps.setString(12, data.getLastOperationType());
                    ps.setTimestamp(13, data.getLastOperationTime() != null ? Timestamp.valueOf(data.getLastOperationTime()) : null);
                },
                executionOptions,
                jdbcOptions
        );
        dwsStream
                .rebalance()
                .addSink(sink).name("ClickHouse DWS Crop Health Snapshot Sink");

        // 打印数据流用于调试
        dwsStream.map(data -> "处理作物健康快照数据: " + data.toString()).print();

        // 执行作业
        env.execute("DWS Greenhouse Crop Health Snapshot Processing");
    }

    /**
     * 从DWD表读取植物健康详情数据
     */
    private static DataStream<DwdPlantHealthDetail> readDwdPlantHealthData(StreamExecutionEnvironment env, String clickhouseUrl, String username, String password, int timeWindowMinutes) {
        String query = "SELECT greenhouse_id, ts, crop_type, growth_stage, plant_height_cm, leaf_count, canopy_coverage, " +
                "chlorophyll_index, wilting_score, fruit_count, disease_risk, pest_risk, " +
                "soil_moisture, soil_ec, soil_ph, temperature, humidity " +
                "FROM dwd_greenhouse_plant_health_detail " +
                "WHERE ts >= now() - INTERVAL '" + timeWindowMinutes + " minute'";

        TypeInformation<?>[] plantHealthTypes = new TypeInformation[]{
                TypeInformation.of(String.class),
                TypeInformation.of(Timestamp.class),
                TypeInformation.of(String.class),
                TypeInformation.of(String.class),
                TypeInformation.of(Double.class),
                TypeInformation.of(Integer.class),
                TypeInformation.of(Double.class),
                TypeInformation.of(Double.class),
                TypeInformation.of(Double.class),
                TypeInformation.of(Integer.class),
                TypeInformation.of(String.class),
                TypeInformation.of(String.class),
                TypeInformation.of(Double.class),
                TypeInformation.of(Double.class),
                TypeInformation.of(Double.class),
                TypeInformation.of(Double.class),
                TypeInformation.of(Double.class)
        };
        RowTypeInfo plantHealthRowType = new RowTypeInfo(plantHealthTypes);

        JdbcInputFormat jdbcInputFormat = JdbcInputFormat.buildJdbcInputFormat()
                .setDrivername(ClickHouseDriver.class.getName())
                .setDBUrl(clickhouseUrl)
                .setUsername(username)
                .setPassword(password)
                .setQuery(query)
                .setRowTypeInfo(plantHealthRowType)
                .finish();

        return env.createInput(jdbcInputFormat)
                .map(new MapFunction<org.apache.flink.types.Row, DwdPlantHealthDetail>() {
                    @Override
                    public DwdPlantHealthDetail map(org.apache.flink.types.Row row) throws Exception {
                        return new DwdPlantHealthDetail(
                                row.getField(0).toString(),
                                ((Timestamp) row.getField(1)).toLocalDateTime(),
                                row.getField(2).toString(),
                                row.getField(3).toString(),
                                (Double) row.getField(4),
                                (Integer) row.getField(5),
                                (Double) row.getField(6),
                                (Double) row.getField(7),
                                (Double) row.getField(8),
                                (Integer) row.getField(9),
                                row.getField(10).toString(),
                                row.getField(11).toString(),
                                (Double) row.getField(12),
                                (Double) row.getField(13),
                                (Double) row.getField(14),
                                (Double) row.getField(15),
                                (Double) row.getField(16)
                        );
                    }
                })
                .name("DWD Plant Health Detail Source");
    }

    /**
     * 从DWD表读取土壤详情数据
     */
    private static DataStream<DwdSoilDetail> readDwdSoilData(StreamExecutionEnvironment env, String clickhouseUrl, String username, String password, int timeWindowMinutes) {
        String query = "SELECT greenhouse_id, sensor_id, ts, soil_temperature, soil_moisture, soil_ec, soil_ph, " +
                "soil_n, soil_p, soil_k " +
                "FROM dwd_greenhouse_soil_detail " +
                "WHERE ts >= now() - INTERVAL '" + timeWindowMinutes + " minute'";

        TypeInformation<?>[] soilTypes = new TypeInformation[]{
                TypeInformation.of(String.class),
                TypeInformation.of(String.class),
                TypeInformation.of(Timestamp.class),
                TypeInformation.of(Double.class),
                TypeInformation.of(Double.class),
                TypeInformation.of(Double.class),
                TypeInformation.of(Double.class),
                TypeInformation.of(Double.class),
                TypeInformation.of(Double.class),
                TypeInformation.of(Double.class)
        };
        RowTypeInfo soilRowType = new RowTypeInfo(soilTypes);

        JdbcInputFormat jdbcInputFormat = JdbcInputFormat.buildJdbcInputFormat()
                .setDrivername(ClickHouseDriver.class.getName())
                .setDBUrl(clickhouseUrl)
                .setUsername(username)
                .setPassword(password)
                .setQuery(query)
                .setRowTypeInfo(soilRowType)
                .finish();

        return env.createInput(jdbcInputFormat)
                .map(new MapFunction<org.apache.flink.types.Row, DwdSoilDetail>() {
                    @Override
                    public DwdSoilDetail map(org.apache.flink.types.Row row) throws Exception {
                        return new DwdSoilDetail(
                                row.getField(0).toString(),
                                row.getField(1).toString(),
                                ((Timestamp) row.getField(2)).toLocalDateTime(),
                                (Double) row.getField(3),
                                (Double) row.getField(4),
                                (Double) row.getField(5),
                                (Double) row.getField(6),
                                (Double) row.getField(7),
                                (Double) row.getField(8),
                                (Double) row.getField(9)
                        );
                    }
                })
                .name("DWD Soil Detail Source");
    }

    /**
     * 从ODS表读取农事操作数据
     */
    private static DataStream<FarmOperationRecord> readFarmOperationData(StreamExecutionEnvironment env, String clickhouseUrl, String username, String password) {
        String query = "SELECT greenhouse_id, timestamp, operation_type " +
                "FROM ods_greenhouse_farm_operation " +
                "WHERE timestamp >= toUnixTimestamp(now() - INTERVAL '24 hour') * 1000";

        TypeInformation<?>[] farmOperationTypes = new TypeInformation[]{
                TypeInformation.of(String.class),
                TypeInformation.of(Long.class),
                TypeInformation.of(String.class)
        };
        RowTypeInfo farmOperationRowType = new RowTypeInfo(farmOperationTypes);

        JdbcInputFormat jdbcInputFormat = JdbcInputFormat.buildJdbcInputFormat()
                .setDrivername(ClickHouseDriver.class.getName())
                .setDBUrl(clickhouseUrl)
                .setUsername(username)
                .setPassword(password)
                .setQuery(query)
                .setRowTypeInfo(farmOperationRowType)
                .finish();

        return env.createInput(jdbcInputFormat)
                .map(new MapFunction<org.apache.flink.types.Row, FarmOperationRecord>() {
                    @Override
                    public FarmOperationRecord map(org.apache.flink.types.Row row) throws Exception {
                        return new FarmOperationRecord(
                                row.getField(0).toString(),
                                (Long) row.getField(1),
                                row.getField(2).toString()
                        );
                    }
                })
                .name("ODS Farm Operation Source");
    }

    /**
     * 计算风险比例
     */
    private static double calculateRiskRate(String riskLevel) {
        switch (riskLevel != null ? riskLevel.toUpperCase() : "") {
            case "LOW":
                return 0.1;
            case "MEDIUM":
                return 0.5;
            case "HIGH":
                return 0.9;
            default:
                return 0.0;
        }
    }

    /**
     * DWD植物健康详情数据结构
     */
    @Data
    private static class DwdPlantHealthDetail {
        private String greenhouseId;
        private LocalDateTime ts;
        private String cropType;
        private String growthStage;
        private double plantHeightCm;
        private int leafCount;
        private double canopyCoverage;
        private double chlorophyllIndex;
        private double wiltingScore;
        private int fruitCount;
        private String diseaseRisk;
        private String pestRisk;
        private double soilMoisture;
        private double soilEc;
        private double soilPh;
        private double temperature;
        private double humidity;

        public DwdPlantHealthDetail(String greenhouseId, LocalDateTime ts, String cropType, String growthStage, double plantHeightCm, int leafCount, double canopyCoverage, double chlorophyllIndex, double wiltingScore, int fruitCount, String diseaseRisk, String pestRisk, double soilMoisture, double soilEc, double soilPh, double temperature, double humidity) {
            this.greenhouseId = greenhouseId;
            this.ts = ts;
            this.cropType = cropType;
            this.growthStage = growthStage;
            this.plantHeightCm = plantHeightCm;
            this.leafCount = leafCount;
            this.canopyCoverage = canopyCoverage;
            this.chlorophyllIndex = chlorophyllIndex;
            this.wiltingScore = wiltingScore;
            this.fruitCount = fruitCount;
            this.diseaseRisk = diseaseRisk;
            this.pestRisk = pestRisk;
            this.soilMoisture = soilMoisture;
            this.soilEc = soilEc;
            this.soilPh = soilPh;
            this.temperature = temperature;
            this.humidity = humidity;
        }
    }

    /**
     * DWD土壤详情数据结构
     */
    @Data
    private static class DwdSoilDetail {
        private String greenhouseId;
        private String sensorId;
        private LocalDateTime ts;
        private double soilTemperature;
        private double soilMoisture;
        private double soilEc;
        private double soilPh;
        private double soilN;
        private double soilP;
        private double soilK;

        public DwdSoilDetail(String greenhouseId, String sensorId, LocalDateTime ts, double soilTemperature, double soilMoisture, double soilEc, double soilPh, double soilN, double soilP, double soilK) {
            this.greenhouseId = greenhouseId;
            this.sensorId = sensorId;
            this.ts = ts;
            this.soilTemperature = soilTemperature;
            this.soilMoisture = soilMoisture;
            this.soilEc = soilEc;
            this.soilPh = soilPh;
            this.soilN = soilN;
            this.soilP = soilP;
            this.soilK = soilK;
        }
    }

    /**
     * 农事操作记录数据结构
     */
    @Data
    private static class FarmOperationRecord {
        private String greenhouseId;
        private long timestamp;
        private String operationType;

        public FarmOperationRecord(String greenhouseId, long timestamp, String operationType) {
            this.greenhouseId = greenhouseId;
            this.timestamp = timestamp;
            this.operationType = operationType;
        }
    }

    /**
     * DWS作物健康快照数据结构
     */
    @Data
    public static class DwsCropHealthSnapshot {
        private String greenhouseId;
        private LocalDateTime statTime;
        private String cropType;
        private String growthStage;
        private double avgPlantHeight;
        private double avgLeafCount;
        private double avgChlorophyll;
        private double avgSoilMoisture;
        private double avgSoilEc;
        private double diseaseRiskRate;
        private double pestRiskRate;
        private String lastOperationType;
        private LocalDateTime lastOperationTime;

        public DwsCropHealthSnapshot(String greenhouseId, LocalDateTime statTime, String cropType, String growthStage, double avgPlantHeight, double avgLeafCount, double avgChlorophyll, double avgSoilMoisture, double avgSoilEc, double diseaseRiskRate, double pestRiskRate, String lastOperationType, LocalDateTime lastOperationTime) {
            this.greenhouseId = greenhouseId;
            this.statTime = statTime;
            this.cropType = cropType;
            this.growthStage = growthStage;
            this.avgPlantHeight = avgPlantHeight;
            this.avgLeafCount = avgLeafCount;
            this.avgChlorophyll = avgChlorophyll;
            this.avgSoilMoisture = avgSoilMoisture;
            this.avgSoilEc = avgSoilEc;
            this.diseaseRiskRate = diseaseRiskRate;
            this.pestRiskRate = pestRiskRate;
            this.lastOperationType = lastOperationType;
            this.lastOperationTime = lastOperationTime;
        }

    }
}
