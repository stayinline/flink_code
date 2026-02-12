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
import org.example.agriculture.dto.ods.FarmOperationData;

import java.sql.PreparedStatement;
import java.util.Properties;

public class PersionDataJob {

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
        kafkaProps.setProperty("group.id", "farm-operation-consumer");

        // 创建Kafka数据源
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "smart_agriculture_sensor_persion",
                new SimpleStringSchema(),
                kafkaProps
        );
        kafkaConsumer.setStartFromLatest(); // 从最新位置开始消费

        // 添加数据源
        DataStream<String> kafkaDataStream = env.addSource(kafkaConsumer);

        // 数据转换：将JSON字符串转换为FarmOperationData对象
        DataStream<FarmOperationData> transformedStream = kafkaDataStream
                .map(new MapFunction<String, FarmOperationData>() {
                    private final ObjectMapper objectMapper = new ObjectMapper();

                    @Override
                    public FarmOperationData map(String value) throws Exception {
                        try {
                            // 使用Jackson解析JSON字符串
                            return objectMapper.readValue(value, FarmOperationData.class);
                        } catch (Exception e) {
                            // 数据格式错误时返回null
                            System.err.println("数据格式错误: " + value + ", 错误: " + e.getMessage());
                            return null;
                        }
                    }
                })
                .filter(new FilterFunction<FarmOperationData>() {
                    @Override
                    public boolean filter(FarmOperationData data) throws Exception {
                        // 过滤掉null数据
                        return data != null;
                    }
                });

        // 数据清洗和过滤
        DataStream<FarmOperationData> cleanedStream = transformedStream
                .filter(new FilterFunction<FarmOperationData>() {
                    @Override
                    public boolean filter(FarmOperationData data) throws Exception {
                        // 过滤掉无效数据
                        return data.getOperationId() != null && !data.getOperationId().isEmpty() &&
                               data.getDataType() != null && !data.getDataType().isEmpty() &&
                               data.getGreenhouseId() != null && !data.getGreenhouseId().isEmpty() &&
                               data.getOperator() != null &&
                               data.getOperation() != null &&
                               data.getControlParams() != null &&
                               data.getDecision() != null &&
                               data.getResult() != null;
                    }
                });

        // 数据合规校验
        DataStream<FarmOperationData> validatedStream = cleanedStream
                .map(new MapFunction<FarmOperationData, FarmOperationData>() {
                    @Override
                    public FarmOperationData map(FarmOperationData data) throws Exception {
                        // 合规校验
                        validateData(data);
                        return data;
                    }
                });

        // 创建ClickHouse Sink
        String insertSql = "INSERT INTO ods_greenhouse_farm_operation " +
                "(operation_id, data_type, greenhouse_id, timestamp, operator_id, operator_name, operator_role, " +
                "operation_type, target_zone, duration_sec, irrigation_mode, water_volume_l, fertilizer_type, fertilizer_amount_kg, " +
                "trigger_source, decision_reason, expected_effect, snapshot_soil_moisture, snapshot_soil_ec, snapshot_soil_temperature, " +
                "execution_status, audit_required, remark) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

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
        SinkFunction<FarmOperationData> sink = JdbcSink.sink(
                insertSql,
                (PreparedStatement ps, FarmOperationData data) -> {
                    ps.setString(1, data.getOperationId());
                    ps.setString(2, data.getDataType());
                    ps.setString(3, data.getGreenhouseId());
                    ps.setLong(4, data.getTimestamp());
                    ps.setString(5, data.getOperator().getId());
                    ps.setString(6, data.getOperator().getName());
                    ps.setString(7, data.getOperator().getRole());
                    ps.setString(8, data.getOperation().getOperationType());
                    ps.setString(9, data.getOperation().getTargetZone());
                    ps.setInt(10, data.getOperation().getDurationSec());
                    ps.setString(11, data.getControlParams().getIrrigationMode());
                    ps.setObject(12, data.getControlParams().getWaterVolumeL());
                    ps.setString(13, data.getControlParams().getFertilizerType());
                    ps.setObject(14, data.getControlParams().getFertilizerAmountKg());
                    ps.setString(15, data.getDecision().getTriggerSource());
                    ps.setString(16, data.getDecision().getReason());
                    ps.setString(17, data.getDecision().getExpectedEffect());
                    ps.setObject(18, data.getDecision().getRelatedSensorSnapshot().getSoilMoisture());
                    ps.setObject(19, data.getDecision().getRelatedSensorSnapshot().getSoilEc());
                    ps.setObject(20, data.getDecision().getRelatedSensorSnapshot().getSoilTemperature());
                    ps.setString(21, data.getResult().getExecutionStatus());
                    ps.setInt(22, data.getResult().isAuditRequired() ? 1 : 0);
                    ps.setString(23, data.getResult().getRemark());
                },
                executionOptions,
                jdbcOptions
        );
        validatedStream.addSink(sink).name("ClickHouse Farm Operation Sink");

        // 打印数据流用于调试
        validatedStream.map(data -> "处理农事操作数据: " + data.toString()).print();

        // 执行作业
        env.execute("Farm Operation Data Processing");
    }

    /**
     * 数据合规校验
     */
    private static void validateData(FarmOperationData data) {
        // 操作类型校验
        if (!isValidOperationType(data.getOperation().getOperationType())) {
            System.out.println("[警告] 操作类型异常: " + data.getOperation().getOperationType());
        }

        // 执行状态校验
        if (!isValidExecutionStatus(data.getResult().getExecutionStatus())) {
            System.out.println("[警告] 执行状态异常: " + data.getResult().getExecutionStatus());
        }

        // 触发来源校验
        if (!isValidTriggerSource(data.getDecision().getTriggerSource())) {
            System.out.println("[警告] 触发来源异常: " + data.getDecision().getTriggerSource());
        }

        // 持续时间校验
        if (data.getOperation().getDurationSec() < 0) {
            System.out.println("[警告] 持续时间异常: " + data.getOperation().getDurationSec());
        }
    }

    /**
     * 验证操作类型是否有效
     */
    private static boolean isValidOperationType(String operationType) {
        String[] validTypes = {"VENTILATION", "IRRIGATION", "FERTILIZATION", "INSPECTION"};
        for (String type : validTypes) {
            if (type.equals(operationType)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 验证执行状态是否有效
     */
    private static boolean isValidExecutionStatus(String executionStatus) {
        String[] validStatuses = {"SUCCESS", "FAIL"};
        for (String status : validStatuses) {
            if (status.equals(executionStatus)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 验证触发来源是否有效
     */
    private static boolean isValidTriggerSource(String triggerSource) {
        String[] validSources = {"MANUAL", "AI"};
        for (String source : validSources) {
            if (source.equals(triggerSource)) {
                return true;
            }
        }
        return false;
    }
}