package org.example.agriculture.job.ods;

import com.clickhouse.jdbc.ClickHouseDriver;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.example.agriculture.dto.ods.PlantVisionData;

import java.sql.PreparedStatement;
import java.util.Properties;

public class PlantVisionDataJob {

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
        kafkaProps.setProperty("group.id", "plant-vision-consumer");

        // 创建Kafka数据源
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "smart_agriculture_plant_vision",
                new SimpleStringSchema(),
                kafkaProps
        );
        kafkaConsumer.setStartFromLatest(); // 从最新位置开始消费

        // 添加数据源
        DataStream<String> kafkaDataStream = env.addSource(kafkaConsumer);

        // 数据转换：将JSON字符串转换为PlantVisionData对象
        DataStream<PlantVisionData> transformedStream = kafkaDataStream
                .map(new MapFunction<String, PlantVisionData>() {
                    private final ObjectMapper objectMapper = new ObjectMapper();

                    @Override
                    public PlantVisionData map(String value) throws Exception {
                        try {
                            // 使用Jackson解析JSON字符串
                            return objectMapper.readValue(value, PlantVisionData.class);
                        } catch (Exception e) {
                            // 数据格式错误时返回null
                            System.err.println("数据格式错误: " + value + ", 错误: " + e.getMessage());
                            return null;
                        }
                    }
                })
                .filter(new FilterFunction<PlantVisionData>() {
                    @Override
                    public boolean filter(PlantVisionData data) throws Exception {
                        // 过滤掉null数据
                        return data != null;
                    }
                });

        // 数据清洗和过滤
        DataStream<PlantVisionData> cleanedStream = transformedStream
                .filter(new FilterFunction<PlantVisionData>() {
                    @Override
                    public boolean filter(PlantVisionData data) throws Exception {
                        // 过滤掉无效数据
                        return data.getPlantDetectId() != null && !data.getPlantDetectId().isEmpty() &&
                               data.getGreenhouseId() != null && !data.getGreenhouseId().isEmpty() &&
                               data.getCameraId() != null && !data.getCameraId().isEmpty() &&
                               data.getPlantBasic() != null &&
                               data.getPlantHealth() != null &&
                               data.getFruitInfo() != null &&
                               data.getStressAnalysis() != null &&
                               data.getModelVersion() != null && !data.getModelVersion().isEmpty();
                    }
                });

        // 数据合规校验
        DataStream<PlantVisionData> validatedStream = cleanedStream
                .map(new MapFunction<PlantVisionData, PlantVisionData>() {
                    @Override
                    public PlantVisionData map(PlantVisionData data) throws Exception {
                        // 合规校验
                        validateData(data);
                        return data;
                    }
                });

        // 创建ClickHouse Sink
        String insertSql = "INSERT INTO ods_greenhouse_plant_vision " +
                "(plant_detect_id, greenhouse_id, camera_id, timestamp, " +
                "crop_type, growth_stage, plant_height_cm, leaf_count, canopy_coverage, " +
                "leaf_color_index, chlorophyll_index, wilting_score, disease_risk, pest_risk, " +
                "fruit_count, avg_fruit_diameter_mm, fruit_color_stage, " +
                "water_stress, nutrient_stress, light_stress, " +
                "confidence, model_version) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

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
        SinkFunction<PlantVisionData> sink = JdbcSink.sink(
                insertSql,
                (PreparedStatement ps, PlantVisionData data) -> {
                    ps.setString(1, data.getPlantDetectId());
                    ps.setString(2, data.getGreenhouseId());
                    ps.setString(3, data.getCameraId());
                    ps.setLong(4, data.getTimestamp());
                    ps.setString(5, data.getPlantBasic().getCropType());
                    ps.setString(6, data.getPlantBasic().getGrowthStage());
                    ps.setDouble(7, data.getPlantBasic().getPlantHeightCm());
                    ps.setInt(8, data.getPlantBasic().getLeafCount());
                    ps.setDouble(9, data.getPlantBasic().getCanopyCoverage());
                    ps.setDouble(10, data.getPlantHealth().getLeafColorIndex());
                    ps.setDouble(11, data.getPlantHealth().getChlorophyllIndex());
                    ps.setDouble(12, data.getPlantHealth().getWiltingScore());
                    ps.setString(13, data.getPlantHealth().getDiseaseRisk());
                    ps.setString(14, data.getPlantHealth().getPestRisk());
                    ps.setInt(15, data.getFruitInfo().getFruitCount());
                    ps.setDouble(16, data.getFruitInfo().getAvgFruitDiameterMm());
                    ps.setString(17, data.getFruitInfo().getFruitColorStage());
                    ps.setString(18, data.getStressAnalysis().getWaterStress());
                    ps.setString(19, data.getStressAnalysis().getNutrientStress());
                    ps.setString(20, data.getStressAnalysis().getLightStress());
                    ps.setDouble(21, data.getConfidence());
                    ps.setString(22, data.getModelVersion());
                },
                executionOptions,
                jdbcOptions
        );
        validatedStream.addSink(sink).name("ClickHouse Plant Vision Sink");

        // 打印数据流用于调试
        validatedStream.map(data -> "处理植物视觉数据: " + data.toString()).print();

        // 执行作业
        env.execute("Plant Vision Data Processing");
    }

    /**
     * 数据合规校验
     */
    private static void validateData(PlantVisionData data) {
        // 生长阶段校验
        if (!isValidGrowthStage(data.getPlantBasic().getGrowthStage())) {
            System.out.println("[警告] 生长阶段异常: " + data.getPlantBasic().getGrowthStage());
        }

        // 风险等级校验
        if (!isValidRiskLevel(data.getPlantHealth().getDiseaseRisk())) {
            System.out.println("[警告] 病害风险等级异常: " + data.getPlantHealth().getDiseaseRisk());
        }

        if (!isValidRiskLevel(data.getPlantHealth().getPestRisk())) {
            System.out.println("[警告] 虫害风险等级异常: " + data.getPlantHealth().getPestRisk());
        }

        // 胁迫状态校验
        if (!isValidStressState(data.getStressAnalysis().getWaterStress())) {
            System.out.println("[警告] 水分胁迫状态异常: " + data.getStressAnalysis().getWaterStress());
        }

        if (!isValidStressState(data.getStressAnalysis().getNutrientStress())) {
            System.out.println("[警告] 养分胁迫状态异常: " + data.getStressAnalysis().getNutrientStress());
        }

        if (!isValidStressState(data.getStressAnalysis().getLightStress())) {
            System.out.println("[警告] 光照胁迫状态异常: " + data.getStressAnalysis().getLightStress());
        }

        // 置信度校验
        if (data.getConfidence() < 0 || data.getConfidence() > 1) {
            System.out.println("[警告] 置信度异常: " + data.getConfidence());
        }

        // 冠层覆盖率校验
        if (data.getPlantBasic().getCanopyCoverage() < 0 || data.getPlantBasic().getCanopyCoverage() > 1) {
            System.out.println("[警告] 冠层覆盖率异常: " + data.getPlantBasic().getCanopyCoverage());
        }

        // 萎蔫评分校验
        if (data.getPlantHealth().getWiltingScore() < 0 || data.getPlantHealth().getWiltingScore() > 1) {
            System.out.println("[警告] 萎蔫评分异常: " + data.getPlantHealth().getWiltingScore());
        }
    }

    /**
     * 验证生长阶段是否有效
     */
    private static boolean isValidGrowthStage(String growthStage) {
        String[] validStages = {"SEEDLING", "FLOWERING", "FRUITING"};
        for (String stage : validStages) {
            if (stage.equals(growthStage)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 验证风险等级是否有效
     */
    private static boolean isValidRiskLevel(String riskLevel) {
        String[] validLevels = {"LOW", "MEDIUM", "HIGH", "NONE"};
        for (String level : validLevels) {
            if (level.equals(riskLevel)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 验证胁迫状态是否有效
     */
    private static boolean isValidStressState(String stressState) {
        String[] validStates = {"NORMAL", "LOW", "MEDIUM", "HIGH", "NONE"};
        for (String state : validStates) {
            if (state.equals(stressState)) {
                return true;
            }
        }
        return false;
    }
}