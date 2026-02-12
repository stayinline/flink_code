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
import org.example.agriculture.dto.ods.SoilSensorData;

import java.sql.PreparedStatement;
import java.util.Properties;

public class DirtDataJob {

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
        kafkaProps.setProperty("group.id", "soil-sensor-consumer");

        // 创建Kafka数据源
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "smart_agriculture_sensor_dirt",
                new SimpleStringSchema(),
                kafkaProps
        );
        kafkaConsumer.setStartFromLatest(); // 从最新位置开始消费

        // 添加数据源
        DataStream<String> kafkaDataStream = env.addSource(kafkaConsumer);

        // 数据转换：将JSON字符串转换为SoilSensorData对象
        DataStream<SoilSensorData> transformedStream = kafkaDataStream
                .map(new MapFunction<String, SoilSensorData>() {
                    private final ObjectMapper objectMapper = new ObjectMapper();

                    @Override
                    public SoilSensorData map(String value) throws Exception {
                        try {
                            // 使用Jackson解析JSON字符串
                            return objectMapper.readValue(value, SoilSensorData.class);
                        } catch (Exception e) {
                            // 数据格式错误时返回null
                            System.err.println("数据格式错误: " + value + ", 错误: " + e.getMessage());
                            return null;
                        }
                    }
                })
                .filter(new FilterFunction<SoilSensorData>() {
                    @Override
                    public boolean filter(SoilSensorData data) throws Exception {
                        // 过滤掉null数据
                        return data != null;
                    }
                });

        // 数据清洗和过滤
        DataStream<SoilSensorData> cleanedStream = transformedStream
                .filter(new FilterFunction<SoilSensorData>() {
                    @Override
                    public boolean filter(SoilSensorData data) throws Exception {
                        // 过滤掉无效数据
                        return data.getSensorId() != null && !data.getSensorId().isEmpty() &&
                               data.getSensorType() != null && !data.getSensorType().isEmpty() &&
                               data.getGreenhouseId() != null && !data.getGreenhouseId().isEmpty() &&
                               data.getLocation() != null &&
                               data.getMetrics() != null &&
                               data.getDevice() != null;
                    }
                });

        // 数据合规校验
        DataStream<SoilSensorData> validatedStream = cleanedStream
                .map(new MapFunction<SoilSensorData, SoilSensorData>() {
                    @Override
                    public SoilSensorData map(SoilSensorData data) throws Exception {
                        // 合规校验
                        validateData(data);
                        return data;
                    }
                });

        // 创建ClickHouse Sink
        String insertSql = "INSERT INTO ods_greenhouse_soil_sensor_data " +
                "(sensor_id, sensor_type, greenhouse_id, timestamp, location_x, location_y, location_z, " +
                "soil_temperature, soil_moisture, soil_ec, soil_ph, soil_n, soil_p, soil_k, " +
                "device_battery, device_status, device_firmware) " +
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
        SinkFunction<SoilSensorData> sink = JdbcSink.sink(
                insertSql,
                (PreparedStatement ps, SoilSensorData data) -> {
                    ps.setString(1, data.getSensorId());
                    ps.setString(2, data.getSensorType());
                    ps.setString(3, data.getGreenhouseId());
                    ps.setLong(4, data.getTimestamp());
                    ps.setDouble(5, data.getLocation().getX());
                    ps.setDouble(6, data.getLocation().getY());
                    ps.setDouble(7, data.getLocation().getZ());
                    ps.setDouble(8, data.getMetrics().getSoilTemperature());
                    ps.setDouble(9, data.getMetrics().getSoilMoisture());
                    ps.setDouble(10, data.getMetrics().getSoilEc());
                    ps.setDouble(11, data.getMetrics().getSoilPh());
                    ps.setDouble(12, data.getMetrics().getSoilN());
                    ps.setDouble(13, data.getMetrics().getSoilP());
                    ps.setDouble(14, data.getMetrics().getSoilK());
                    ps.setInt(15, data.getDevice().getBattery());
                    ps.setString(16, data.getDevice().getStatus());
                    ps.setString(17, data.getDevice().getFirmware());
                },
                executionOptions,
                jdbcOptions
        );
        validatedStream.addSink(sink).name("ClickHouse Soil Sink");

        // 打印数据流用于调试
        validatedStream.map(data -> "处理土壤数据: " + data.toString()).print();

        // 执行作业
        env.execute("Soil Sensor Data Processing");
    }

    /**
     * 数据合规校验
     */
    private static void validateData(SoilSensorData data) {
        // 土壤温度校验
        if (data.getMetrics().getSoilTemperature() < 0 || data.getMetrics().getSoilTemperature() > 50) {
            System.out.println("[警告] 土壤温度异常: " + data.getMetrics().getSoilTemperature() + "°C");
        }

        // 土壤湿度校验
        if (data.getMetrics().getSoilMoisture() < 0 || data.getMetrics().getSoilMoisture() > 100) {
            System.out.println("[警告] 土壤湿度异常: " + data.getMetrics().getSoilMoisture() + "%");
        }

        // 土壤pH校验
        if (data.getMetrics().getSoilPh() < 3 || data.getMetrics().getSoilPh() > 10) {
            System.out.println("[警告] 土壤pH异常: " + data.getMetrics().getSoilPh());
        }

        // 电池电量校验
        if (data.getDevice().getBattery() < 20) {
            System.out.println("[警告] 电池电量低: " + data.getDevice().getBattery() + "%");
        }

        // 设备状态校验
        if (!"ONLINE".equals(data.getDevice().getStatus())) {
            System.out.println("[警告] 设备状态异常: " + data.getDevice().getStatus());
        }
    }
}