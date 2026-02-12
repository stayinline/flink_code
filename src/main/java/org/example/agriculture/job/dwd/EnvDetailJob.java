package org.example.agriculture.job.dwd;

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
import org.example.agriculture.dto.dwd.GreenhouseSensorData;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Properties;

public class EnvDetailJob {

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
        kafkaProps.setProperty("group.id", "env-detail-consumer");

        // 创建Kafka数据源
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "smart_agriculture_sensor",
                new SimpleStringSchema(),
                kafkaProps
        );
        kafkaConsumer.setStartFromEarliest();

        // 添加数据源
        DataStream<String> kafkaDataStream = env.addSource(kafkaConsumer);

        // 数据转换：将JSON字符串转换为GreenhouseSensorData对象
        DataStream<GreenhouseSensorData> transformedStream = kafkaDataStream
                .map(new MapFunction<String, GreenhouseSensorData>() {
                    private final ObjectMapper objectMapper = new ObjectMapper();

                    @Override
                    public GreenhouseSensorData map(String value) throws Exception {
                        try {
                            // 使用Jackson解析JSON字符串
                            return objectMapper.readValue(value, GreenhouseSensorData.class);
                        } catch (Exception e) {
                            // 数据格式错误时返回null
                            System.err.println("数据格式错误: " + value + ", 错误: " + e.getMessage());
                            return null;
                        }
                    }
                })
                .filter(new FilterFunction<GreenhouseSensorData>() {
                    @Override
                    public boolean filter(GreenhouseSensorData data) throws Exception {
                        // 过滤掉null数据
                        return data != null;
                    }
                });

        // 数据清洗和过滤
        DataStream<GreenhouseSensorData> cleanedStream = transformedStream
                .filter(new FilterFunction<GreenhouseSensorData>() {
                    @Override
                    public boolean filter(GreenhouseSensorData data) throws Exception {
                        // 过滤掉无效数据
                        return data.getSensorId() != null && !data.getSensorId().isEmpty() &&
                               data.getGreenhouseId() != null && !data.getGreenhouseId().isEmpty() &&
                               data.getMetrics() != null &&
                               data.getDevice() != null;
                    }
                });

        // 数据转换为DWD层格式
        DataStream<DwdEnvDetail> dwdStream = cleanedStream
                .map(new MapFunction<GreenhouseSensorData, DwdEnvDetail>() {
                    @Override
                    public DwdEnvDetail map(GreenhouseSensorData data) throws Exception {
                        // 转换为DWD层格式
                        return new DwdEnvDetail(
                                data.getGreenhouseId(),
                                data.getSensorId(),
                                Instant.ofEpochMilli(data.getTimestamp()).atZone(java.time.ZoneId.systemDefault()).toLocalDateTime(),
                                data.getMetrics().getTemperature(),
                                data.getMetrics().getHumidity(),
                                data.getMetrics().getCo2(),
                                data.getMetrics().getLight(),
                                data.getMetrics().getSoilTemperature(),
                                data.getMetrics().getSoilMoisture(),
                                data.getDevice().getStatus()
                        );
                    }
                });

        // 创建ClickHouse Sink
        String insertSql = "INSERT INTO dwd_greenhouse_env_detail " +
                "(greenhouse_id, sensor_id, ts, temperature, humidity, co2, light, soil_temperature, soil_moisture, device_status) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

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
        SinkFunction<DwdEnvDetail> sink = JdbcSink.sink(
                insertSql,
                (PreparedStatement ps, DwdEnvDetail data) -> {
                    ps.setString(1, data.getGreenhouseId());
                    ps.setString(2, data.getSensorId());
                    ps.setTimestamp(3, Timestamp.valueOf(data.getTs()));
                    ps.setDouble(4, data.getTemperature());
                    ps.setDouble(5, data.getHumidity());
                    ps.setInt(6, data.getCo2());
                    ps.setInt(7, data.getLight());
                    ps.setDouble(8, data.getSoilTemperature());
                    ps.setDouble(9, data.getSoilMoisture());
                    ps.setString(10, data.getDeviceStatus());
                },
                executionOptions,
                jdbcOptions
        );
        dwdStream.addSink(sink).name("ClickHouse DWD Env Detail Sink");

        // 打印数据流用于调试
        dwdStream.map(data -> "处理环境详情数据: " + data.toString()).print();

        // 执行作业
        env.execute("DWD Greenhouse Environment Detail Processing");
    }

    // DWD环境详情数据结构
    public static class DwdEnvDetail {
        private String greenhouseId;
        private String sensorId;
        private java.time.LocalDateTime ts;
        private double temperature;
        private double humidity;
        private int co2;
        private int light;
        private double soilTemperature;
        private double soilMoisture;
        private String deviceStatus;

        public DwdEnvDetail(String greenhouseId, String sensorId, java.time.LocalDateTime ts, double temperature, double humidity, int co2, int light, double soilTemperature, double soilMoisture, String deviceStatus) {
            this.greenhouseId = greenhouseId;
            this.sensorId = sensorId;
            this.ts = ts;
            this.temperature = temperature;
            this.humidity = humidity;
            this.co2 = co2;
            this.light = light;
            this.soilTemperature = soilTemperature;
            this.soilMoisture = soilMoisture;
            this.deviceStatus = deviceStatus;
        }

        public String getGreenhouseId() {
            return greenhouseId;
        }

        public String getSensorId() {
            return sensorId;
        }

        public java.time.LocalDateTime getTs() {
            return ts;
        }

        public double getTemperature() {
            return temperature;
        }

        public double getHumidity() {
            return humidity;
        }

        public int getCo2() {
            return co2;
        }

        public int getLight() {
            return light;
        }

        public double getSoilTemperature() {
            return soilTemperature;
        }

        public double getSoilMoisture() {
            return soilMoisture;
        }

        public String getDeviceStatus() {
            return deviceStatus;
        }

        @Override
        public String toString() {
            return "DwdEnvDetail{" +
                    "greenhouseId='" + greenhouseId + '\'' +
                    ", sensorId='" + sensorId + '\'' +
                    ", ts=" + ts +
                    ", temperature=" + temperature +
                    ", humidity=" + humidity +
                    ", co2=" + co2 +
                    ", light=" + light +
                    ", soilTemperature=" + soilTemperature +
                    ", soilMoisture=" + soilMoisture +
                    ", deviceStatus='" + deviceStatus + '\'' +
                    '}';
        }
    }
}