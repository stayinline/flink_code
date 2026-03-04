package org.example.agriculture.job.dwd;

import com.clickhouse.jdbc.ClickHouseDriver;
import lombok.Data;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.example.agriculture.dto.ods.GreenhouseSensorData;
import org.example.agriculture.dto.ods.PlantVisionData;
import org.example.agriculture.dto.ods.SoilSensorData;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

public class PlantHealthDetailJobV2 {

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

        // 从Kafka读取植物视觉数据
        DataStream<PlantVisionData> plantVisionStream = readPlantVisionData(env, kafkaProps);

        // 从Kafka读取土壤传感器数据
        DataStream<SoilSensorData> soilStream = readSoilSensorData(env, kafkaProps);

        // 从Kafka读取环境传感器数据
        DataStream<GreenhouseSensorData> envStream = readEnvSensorData(env, kafkaProps);

        // 定义Watermark策略
        WatermarkStrategy<PlantVisionData> visionWm =
                WatermarkStrategy
                        .<PlantVisionData>forBoundedOutOfOrderness(Duration.ofSeconds(15))
                        .withTimestampAssigner((e, ts) -> e.getTimestamp())
                        .withIdleness(Duration.ofMinutes(1));

        WatermarkStrategy<SoilSensorData> soilWm =
                WatermarkStrategy
                        .<SoilSensorData>forBoundedOutOfOrderness(Duration.ofSeconds(15))
                        .withTimestampAssigner((e, ts) -> e.getTimestamp())
                        .withIdleness(Duration.ofMinutes(1));

        WatermarkStrategy<GreenhouseSensorData> envWm =
                WatermarkStrategy
                        .<GreenhouseSensorData>forBoundedOutOfOrderness(Duration.ofSeconds(15))
                        .withTimestampAssigner((e, ts) -> e.getTimestamp())
                        .withIdleness(Duration.ofMinutes(1));
        // 应用Watermark策略
        DataStream<PlantVisionData> visionWithWm = plantVisionStream.assignTimestampsAndWatermarks(visionWm);
        DataStream<SoilSensorData> soilWithWm = soilStream.assignTimestampsAndWatermarks(soilWm);
        DataStream<GreenhouseSensorData> envWithWm = envStream.assignTimestampsAndWatermarks(envWm);

        // 第一步：植物视觉数据与土壤数据最近值JOIN
        DataStream<VisionWithSoil> visionWithSoil = visionWithWm
                .keyBy(PlantVisionData::getGreenhouseId)
                .connect(soilWithWm.keyBy(SoilSensorData::getGreenhouseId))
                .process(new KeyedCoProcessFunction<String, PlantVisionData, SoilSensorData, VisionWithSoil>() {

                    private org.apache.flink.api.common.state.ValueState<SoilSensorData> soilState;

                    @Override
                    public void open(Configuration parameters) {
                        soilState = getRuntimeContext().getState(
                                new org.apache.flink.api.common.state.ValueStateDescriptor<>("soilState", SoilSensorData.class)
                        );
                    }

                    @Override
                    public void processElement1(PlantVisionData vision, Context ctx, Collector<VisionWithSoil> out) throws Exception {
                        SoilSensorData soil = soilState.value();
                        if (soil != null) {
                            out.collect(new VisionWithSoil(vision, soil));
                        }
                    }

                    @Override
                    public void processElement2(SoilSensorData soil, Context ctx, Collector<VisionWithSoil> out) throws Exception {
                        soilState.update(soil);
                    }
                });

        // 第二步：JOIN结果与环境数据最近值JOIN
        DataStream<DwdPlantHealthDetail> dwdStream = visionWithSoil
                .keyBy(VisionWithSoil::getGreenhouseId)
                .connect(envWithWm.keyBy(GreenhouseSensorData::getGreenhouseId))
                .process(new KeyedCoProcessFunction<String, VisionWithSoil, GreenhouseSensorData, DwdPlantHealthDetail>() {

                    private org.apache.flink.api.common.state.ValueState<GreenhouseSensorData> envState;

                    @Override
                    public void open(Configuration parameters) {
                        envState = getRuntimeContext().getState(
                                new org.apache.flink.api.common.state.ValueStateDescriptor<>("envState", GreenhouseSensorData.class)
                        );
                    }

                    @Override
                    public void processElement1(VisionWithSoil vs, Context ctx, Collector<DwdPlantHealthDetail> out) throws Exception {
                        GreenhouseSensorData env = envState.value();
                        if (env == null) return;

                        PlantVisionData vision = vs.getVision();
                        SoilSensorData soil = vs.getSoil();

                        DwdPlantHealthDetail dwdData = new DwdPlantHealthDetail(
                                vision.getGreenhouseId(),
                                Instant.ofEpochMilli(vision.getTimestamp()).atZone(java.time.ZoneId.systemDefault()).toLocalDateTime(),
                                vision.getPlantBasic().getCropType(),
                                vision.getPlantBasic().getGrowthStage(),
                                vision.getPlantBasic().getPlantHeightCm(),
                                vision.getPlantBasic().getLeafCount(),
                                vision.getPlantBasic().getCanopyCoverage(),
                                vision.getPlantHealth().getChlorophyllIndex(),
                                vision.getPlantHealth().getWiltingScore(),
                                vision.getFruitInfo().getFruitCount(),
                                vision.getPlantHealth().getDiseaseRisk(),
                                vision.getPlantHealth().getPestRisk(),
                                soil.getMetrics().getSoilMoisture(),
                                soil.getMetrics().getSoilEc(),
                                soil.getMetrics().getSoilPh(),
                                env.getMetrics().getTemperature(),
                                env.getMetrics().getHumidity()
                        );

                        out.collect(dwdData);
                    }

                    @Override
                    public void processElement2(GreenhouseSensorData env, Context ctx, Collector<DwdPlantHealthDetail> out) throws Exception {
                        envState.update(env);
                    }
                });

        // 创建ClickHouse Sink
        String insertSql = "INSERT INTO dwd_greenhouse_plant_health_detail " +
                "(greenhouse_id, ts, crop_type, growth_stage, plant_height_cm, leaf_count, canopy_coverage, " +
                "chlorophyll_index, wilting_score, fruit_count, disease_risk, pest_risk, " +
                "soil_moisture, soil_ec, soil_ph, temperature, humidity) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

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
        SinkFunction<DwdPlantHealthDetail> sink = JdbcSink.sink(
                insertSql,
                (PreparedStatement ps, DwdPlantHealthDetail data) -> {
                    ps.setString(1, data.getGreenhouseId());
                    ps.setTimestamp(2, Timestamp.valueOf(data.getTs()));
                    ps.setString(3, data.getCropType());
                    ps.setString(4, data.getGrowthStage());
                    ps.setDouble(5, data.getPlantHeightCm());
                    ps.setInt(6, data.getLeafCount());
                    ps.setDouble(7, data.getCanopyCoverage());
                    ps.setDouble(8, data.getChlorophyllIndex());
                    ps.setDouble(9, data.getWiltingScore());
                    ps.setInt(10, data.getFruitCount());
                    ps.setString(11, data.getDiseaseRisk());
                    ps.setString(12, data.getPestRisk());
                    ps.setDouble(13, data.getSoilMoisture());
                    ps.setDouble(14, data.getSoilEc());
                    ps.setDouble(15, data.getSoilPh());
                    ps.setDouble(16, data.getTemperature());
                    ps.setDouble(17, data.getHumidity());
                },
                executionOptions,
                jdbcOptions
        );
        dwdStream
                .rebalance()
                .addSink(sink).name("ClickHouse DWD Plant Health Detail Sink");

        // 添加Kafka旁路Sink，topic为dwd_前缀
        DataStream<String> kafkaOutputStream = dwdStream
                .map(new MapFunction<DwdPlantHealthDetail, String>() {
                    private final ObjectMapper objectMapper = new ObjectMapper();

                    @Override
                    public String map(DwdPlantHealthDetail data) throws Exception {
                        try {
                            // 将DwdPlantHealthDetail对象转换为JSON字符串
                            return objectMapper.writeValueAsString(data);
                        } catch (Exception e) {
                            System.err.println("JSON序列化错误: " + e.getMessage());
                            return null;
                        }
                    }
                })
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        // 过滤掉null值
                        return value != null;
                    }
                });

        // 创建Kafka Producer
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<String>(
                "dwd_plant_health_detail",
                new SimpleStringSchema(),
                kafkaProps
        );

        // 添加Kafka Sink
        kafkaOutputStream.addSink(kafkaProducer).name("Kafka DWD Plant Health Detail Sink");

        // 打印数据流用于调试
        dwdStream.map(data -> "处理植物健康详情数据: " + data.toString()).print();

        // 执行作业
        env.execute("DWD Greenhouse Plant Health Detail Processing");
    }

    /**
     * 读取植物视觉数据
     */
    private static DataStream<PlantVisionData> readPlantVisionData(StreamExecutionEnvironment env, Properties baseProps) {
        KafkaSource<String> kafkaSource = KafkaSource.<String>
                builder()
                .setBootstrapServers(baseProps.getProperty("bootstrap.servers"))
                .setTopics("smart_agriculture_plant_vision")
                .setGroupId("plant-vision-consumer")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        return env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Plant Vision Kafka Source")
                .map(new MapFunction<String, PlantVisionData>() {
                    private final ObjectMapper objectMapper = new ObjectMapper();

                    @Override
                    public PlantVisionData map(String value) throws Exception {
                        try {
                            return objectMapper.readValue(value, PlantVisionData.class);
                        } catch (Exception e) {
                            System.err.println("植物视觉数据格式错误: " + value + ", 错误: " + e.getMessage());
                            return null;
                        }
                    }
                })
                .filter(new FilterFunction<PlantVisionData>() {
                    @Override
                    public boolean filter(PlantVisionData data) throws Exception {
                        return data != null &&
                                data.getGreenhouseId() != null &&
                                !data.getGreenhouseId().isEmpty() &&
                                data.getPlantBasic() != null &&
                                data.getPlantHealth() != null &&
                                data.getFruitInfo() != null;
                    }
                });
    }

    /**
     * 读取土壤传感器数据
     */
    private static DataStream<SoilSensorData> readSoilSensorData(StreamExecutionEnvironment env, Properties baseProps) {
        KafkaSource<String> kafkaSource = KafkaSource.<String>
                builder()
                .setBootstrapServers(baseProps.getProperty("bootstrap.servers"))
                .setTopics("smart_agriculture_sensor_dirt")
                .setGroupId("soil-sensor-consumer")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        return env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Soil Sensor Kafka Source")
                .map(new MapFunction<String, SoilSensorData>() {
                    private final ObjectMapper objectMapper = new ObjectMapper();

                    @Override
                    public SoilSensorData map(String value) throws Exception {
                        try {
                            return objectMapper.readValue(value, SoilSensorData.class);
                        } catch (Exception e) {
                            System.err.println("土壤传感器数据格式错误: " + value + ", 错误: " + e.getMessage());
                            return null;
                        }
                    }
                })
                .filter(new FilterFunction<SoilSensorData>() {
                    @Override
                    public boolean filter(SoilSensorData data) throws Exception {
                        return data != null &&
                                data.getGreenhouseId() != null && !data.getGreenhouseId().isEmpty() &&
                                data.getMetrics() != null;
                    }
                });
    }

    /**
     * 读取环境传感器数据
     */
    private static DataStream<GreenhouseSensorData> readEnvSensorData(StreamExecutionEnvironment env, Properties baseProps) {
        KafkaSource<String> kafkaSource = KafkaSource.<String>
                builder()
                .setBootstrapServers(baseProps.getProperty("bootstrap.servers"))
                .setTopics("smart_agriculture_sensor")
                .setGroupId("env-sensor-consumer")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        return env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Env Sensor Kafka Source")
                .map(new MapFunction<String, GreenhouseSensorData>() {
                    private final ObjectMapper objectMapper = new ObjectMapper();

                    @Override
                    public GreenhouseSensorData map(String value) throws Exception {
                        try {
                            return objectMapper.readValue(value, GreenhouseSensorData.class);
                        } catch (Exception e) {
                            System.err.println("环境传感器数据格式错误: " + value + ", 错误: " + e.getMessage());
                            return null;
                        }
                    }
                })
                .filter(new FilterFunction<GreenhouseSensorData>() {
                    @Override
                    public boolean filter(GreenhouseSensorData data) throws Exception {
                        return data != null &&
                                data.getGreenhouseId() != null && !data.getGreenhouseId().isEmpty() &&
                                data.getMetrics() != null;
                    }
                });
    }

    /**
     * 植物视觉数据与土壤数据的JOIN结果
     */
    private static class VisionWithSoil {
        private PlantVisionData vision;
        private SoilSensorData soil;

        public VisionWithSoil(PlantVisionData vision, SoilSensorData soil) {
            this.vision = vision;
            this.soil = soil;
        }

        public PlantVisionData getVision() {
            return vision;
        }

        public SoilSensorData getSoil() {
            return soil;
        }

        public String getGreenhouseId() {
            return vision.getGreenhouseId();
        }
    }

    /**
     * 植物视觉数据与土壤数据的JOIN结果（旧版，保留为兼容）
     */
    private static class VisionSoilJoin {
        private PlantVisionData vision;
        private SoilSensorData soil;

        public VisionSoilJoin(PlantVisionData vision, SoilSensorData soil) {
            this.vision = vision;
            this.soil = soil;
        }

        public PlantVisionData getVision() {
            return vision;
        }

        public SoilSensorData getSoil() {
            return soil;
        }

        public String getGreenhouseId() {
            return vision.getGreenhouseId();
        }
    }

    /**
     * DWD植物健康详情数据结构
     */
    @Data
    public static class DwdPlantHealthDetail {
        private String greenhouseId;
        private java.time.LocalDateTime ts;
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

        public DwdPlantHealthDetail(String greenhouseId, java.time.LocalDateTime ts, String cropType, String growthStage, double plantHeightCm, int leafCount, double canopyCoverage, double chlorophyllIndex, double wiltingScore, int fruitCount, String diseaseRisk, String pestRisk, double soilMoisture, double soilEc, double soilPh, double temperature, double humidity) {
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
}