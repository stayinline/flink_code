package org.example.agriculture.job.dws;

import com.clickhouse.jdbc.ClickHouseDriver;
import lombok.Data;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Properties;

public class CropHealthSnapshotJobV2kafka {

    public static void main(String[] args) throws Exception {
        // 设置执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000); // 每5秒一次checkpoint

        // ClickHouse配置
        String clickhouseUrl = "jdbc:clickhouse://192.168.1.124:8123/default";
        String username = "default";
        String password = "65e84be3";

        // Kafka配置
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "192.168.1.124:9092");
        kafkaProps.setProperty("group.id", "crop-health-snapshot-consumer");

        // 从Kafka读取DWD植物健康详情数据
        DataStream<DwdPlantHealthDetail> dwdPlantHealthStream = readPlantHealthFromKafka(env, kafkaProps);

        // 从Kafka读取DWD土壤详情数据
        DataStream<DwdSoilDetail> dwdSoilStream = readSoilFromKafka(env, kafkaProps);

        // 从Kafka读取农事操作数据
        DataStream<FarmOperationRecord> farmOperationStream = readFarmOperationFromKafka(env, kafkaProps);

        // 定义Watermark策略
        WatermarkStrategy<DwdPlantHealthDetail> plantHealthWm = WatermarkStrategy
                .<DwdPlantHealthDetail>forBoundedOutOfOrderness(Duration.ofSeconds(15))
                .withTimestampAssigner((SerializableTimestampAssigner<DwdPlantHealthDetail>) (element, recordTimestamp) ->
                        element.getTs().atZone(ZoneId.systemDefault()).toEpochSecond() * 1000)
                .withIdleness(Duration.ofMinutes(1));

        WatermarkStrategy<DwdSoilDetail> soilWm = WatermarkStrategy
                .<DwdSoilDetail>forBoundedOutOfOrderness(Duration.ofSeconds(15))
                .withTimestampAssigner((SerializableTimestampAssigner<DwdSoilDetail>) (element, recordTimestamp) ->
                        element.getTs().atZone(ZoneId.systemDefault()).toEpochSecond() * 1000)
                .withIdleness(Duration.ofMinutes(1));

        WatermarkStrategy<FarmOperationRecord> farmOperationWm = WatermarkStrategy
                .<FarmOperationRecord>forBoundedOutOfOrderness(Duration.ofSeconds(15))
                .withTimestampAssigner((SerializableTimestampAssigner<FarmOperationRecord>) (element, recordTimestamp) ->
                        element.getTimestamp() * 1000)
                .withIdleness(Duration.ofMinutes(1));

        // 应用Watermark策略
        DataStream<DwdPlantHealthDetail> plantHealthWithWm = dwdPlantHealthStream.assignTimestampsAndWatermarks(plantHealthWm);
        DataStream<DwdSoilDetail> soilWithWm = dwdSoilStream.assignTimestampsAndWatermarks(soilWm);
        DataStream<FarmOperationRecord> farmOperationWithWm = farmOperationStream.assignTimestampsAndWatermarks(farmOperationWm);

        // 第一步：植物健康数据与土壤数据最近值JOIN
        DataStream<PlantHealthWithSoil> plantHealthWithSoil = plantHealthWithWm
                .keyBy(DwdPlantHealthDetail::getGreenhouseId)
                .connect(soilWithWm.keyBy(DwdSoilDetail::getGreenhouseId))
                .process(new org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction<String, DwdPlantHealthDetail, DwdSoilDetail, PlantHealthWithSoil>() {

                    private org.apache.flink.api.common.state.ValueState<DwdSoilDetail> soilState;

                    @Override
                    public void open(org.apache.flink.configuration.Configuration parameters) {
                        soilState = getRuntimeContext().getState(
                                new org.apache.flink.api.common.state.ValueStateDescriptor<>("soilState", DwdSoilDetail.class)
                        );
                    }

                    @Override
                    public void processElement1(DwdPlantHealthDetail plantHealth, Context ctx, Collector<PlantHealthWithSoil> out) throws Exception {
                        DwdSoilDetail soil = soilState.value();
                        if (soil != null) {
                            out.collect(new PlantHealthWithSoil(plantHealth, soil));
                        }
                    }

                    @Override
                    public void processElement2(DwdSoilDetail soil, Context ctx, Collector<PlantHealthWithSoil> out) throws Exception {
                        soilState.update(soil);
                    }
                });

        // 第二步：JOIN结果与农事操作数据最近值JOIN
        DataStream<DwsCropHealthSnapshot> dwsStream = plantHealthWithSoil
                .keyBy(PlantHealthWithSoil::getGreenhouseId)
                .connect(farmOperationWithWm.keyBy(FarmOperationRecord::getGreenhouseId))
                .process(new org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction<String, PlantHealthWithSoil, FarmOperationRecord, DwsCropHealthSnapshot>() {

                    private org.apache.flink.api.common.state.ValueState<FarmOperationRecord> farmOperationState;

                    @Override
                    public void open(org.apache.flink.configuration.Configuration parameters) {
                        farmOperationState = getRuntimeContext().getState(
                                new org.apache.flink.api.common.state.ValueStateDescriptor<>("farmOperationState", FarmOperationRecord.class)
                        );
                    }

                    @Override
                    public void processElement1(PlantHealthWithSoil phws, Context ctx, Collector<DwsCropHealthSnapshot> out) throws Exception {
                        FarmOperationRecord farmOperation = farmOperationState.value();

                        DwdPlantHealthDetail plantHealth = phws.getPlantHealth();
                        DwdSoilDetail soil = phws.getSoil();

                        // 计算风险比例
                        double diseaseRiskRate = calculateRiskRate(plantHealth.getDiseaseRisk());
                        double pestRiskRate = calculateRiskRate(plantHealth.getPestRisk());

                        // 构建DWS数据
                        DwsCropHealthSnapshot dwsData = new DwsCropHealthSnapshot(
                                plantHealth.getGreenhouseId(),
                                plantHealth.getTs(),
                                plantHealth.getCropType(),
                                plantHealth.getGrowthStage(),
                                plantHealth.getPlantHeightCm(),
                                plantHealth.getLeafCount(),
                                plantHealth.getChlorophyllIndex(),
                                soil.getSoilMoisture(),
                                soil.getSoilEc(),
                                diseaseRiskRate,
                                pestRiskRate,
                                farmOperation != null ? farmOperation.getOperationType() : null,
                                farmOperation != null ? LocalDateTime.ofInstant(java.time.Instant.ofEpochMilli(farmOperation.getTimestamp()), ZoneId.systemDefault()) : null
                        );

                        out.collect(dwsData);
                    }

                    @Override
                    public void processElement2(FarmOperationRecord farmOperation, Context ctx, Collector<DwsCropHealthSnapshot> out) throws Exception {
                        farmOperationState.update(farmOperation);
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

        // 添加Kafka旁路Sink
        DataStream<String> kafkaOutputStream = dwsStream
                .map(new MapFunction<DwsCropHealthSnapshot, String>() {
                    private final ObjectMapper objectMapper = new ObjectMapper();

                    @Override
                    public String map(DwsCropHealthSnapshot data) throws Exception {
                        try {
                            // 将DwsCropHealthSnapshot对象转换为JSON字符串
                            return objectMapper.writeValueAsString(data);
                        } catch (Exception e) {
                            System.err.println("JSON序列化错误: " + e.getMessage());
                            return null;
                        }
                    }
                })
                .filter((FilterFunction<String>) value -> {
                    // 过滤掉null值
                    return value != null;
                });

        // 创建Kafka Producer
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<String>(
                "dws_crop_health_snapshot",
                new SimpleStringSchema(),
                kafkaProps
        );

        // 添加Kafka Sink
        kafkaOutputStream.addSink(kafkaProducer).name("Kafka DWS Crop Health Snapshot Sink");

        // 打印数据流用于调试
        dwsStream.map(data -> "处理作物健康快照数据: " + data.toString()).print();

        // 执行作业
        env.execute("DWS Greenhouse Crop Health Snapshot Processing (Kafka Source)");
    }

    /**
     * 从Kafka读取植物健康详情数据
     */
    private static DataStream<DwdPlantHealthDetail> readPlantHealthFromKafka(StreamExecutionEnvironment env, Properties kafkaProps) {
        // 创建Kafka数据源
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<String>(
                "dwd_plant_health_detail",
                new SimpleStringSchema(),
                kafkaProps
        );
        kafkaConsumer.setStartFromLatest();

        // 添加数据源并转换
        return env.addSource(kafkaConsumer)
                .map(new MapFunction<String, DwdPlantHealthDetail>() {
                    private final ObjectMapper objectMapper = new ObjectMapper();

                    @Override
                    public DwdPlantHealthDetail map(String value) throws Exception {
                        try {
                            // 使用Jackson解析JSON字符串
                            return objectMapper.readValue(value, DwdPlantHealthDetail.class);
                        } catch (Exception e) {
                            // 数据格式错误时返回null
                            System.err.println("植物健康数据格式错误: " + value + ", 错误: " + e.getMessage());
                            return null;
                        }
                    }
                })
                .filter(new FilterFunction<DwdPlantHealthDetail>() {
                    @Override
                    public boolean filter(DwdPlantHealthDetail data) throws Exception {
                        // 过滤掉null数据
                        return data != null;
                    }
                })
                .name("Kafka DWD Plant Health Detail Source");
    }

    /**
     * 从Kafka读取土壤详情数据
     */
    private static DataStream<DwdSoilDetail> readSoilFromKafka(StreamExecutionEnvironment env, Properties kafkaProps) {
        // 创建Kafka数据源
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<String>(
                "dwd_soil_detail",
                new SimpleStringSchema(),
                kafkaProps
        );
        kafkaConsumer.setStartFromLatest();

        // 添加数据源并转换
        return env.addSource(kafkaConsumer)
                .map(new MapFunction<String, DwdSoilDetail>() {
                    private final ObjectMapper objectMapper = new ObjectMapper();

                    @Override
                    public DwdSoilDetail map(String value) throws Exception {
                        try {
                            // 使用Jackson解析JSON字符串
                            return objectMapper.readValue(value, DwdSoilDetail.class);
                        } catch (Exception e) {
                            // 数据格式错误时返回null
                            System.err.println("土壤数据格式错误: " + value + ", 错误: " + e.getMessage());
                            return null;
                        }
                    }
                })
                .filter(new FilterFunction<DwdSoilDetail>() {
                    @Override
                    public boolean filter(DwdSoilDetail data) throws Exception {
                        // 过滤掉null数据
                        return data != null;
                    }
                })
                .name("Kafka DWD Soil Detail Source");
    }

    /**
     * 从Kafka读取农事操作数据
     */
    private static DataStream<FarmOperationRecord> readFarmOperationFromKafka(StreamExecutionEnvironment env, Properties kafkaProps) {
        // 创建Kafka数据源
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<String>(
                "dwd_farm_operation",
                new SimpleStringSchema(),
                kafkaProps
        );
        kafkaConsumer.setStartFromLatest();

        // 添加数据源并转换
        return env.addSource(kafkaConsumer)
                .map(new MapFunction<String, FarmOperationRecord>() {
                    private final ObjectMapper objectMapper = new ObjectMapper();

                    @Override
                    public FarmOperationRecord map(String value) throws Exception {
                        try {
                            // 使用Jackson解析JSON字符串
                            return objectMapper.readValue(value, FarmOperationRecord.class);
                        } catch (Exception e) {
                            // 数据格式错误时返回null
                            System.err.println("农事操作数据格式错误: " + value + ", 错误: " + e.getMessage());
                            return null;
                        }
                    }
                })
                .filter(new FilterFunction<FarmOperationRecord>() {
                    @Override
                    public boolean filter(FarmOperationRecord data) throws Exception {
                        // 过滤掉null数据
                        return data != null;
                    }
                })
                .name("Kafka DWD Farm Operation Source");
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

        public DwdPlantHealthDetail() {}

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

        public DwdSoilDetail() {}

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

        public FarmOperationRecord() {}

        public FarmOperationRecord(String greenhouseId, long timestamp, String operationType) {
            this.greenhouseId = greenhouseId;
            this.timestamp = timestamp;
            this.operationType = operationType;
        }
    }

    /**
     * 植物健康数据与土壤数据的JOIN结果
     */
    private static class PlantHealthWithSoil {
        private DwdPlantHealthDetail plantHealth;
        private DwdSoilDetail soil;

        public PlantHealthWithSoil(DwdPlantHealthDetail plantHealth, DwdSoilDetail soil) {
            this.plantHealth = plantHealth;
            this.soil = soil;
        }

        public DwdPlantHealthDetail getPlantHealth() {
            return plantHealth;
        }

        public DwdSoilDetail getSoil() {
            return soil;
        }

        public String getGreenhouseId() {
            return plantHealth.getGreenhouseId();
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

        public DwsCropHealthSnapshot() {}

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
