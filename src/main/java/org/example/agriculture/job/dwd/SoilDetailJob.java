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
import org.example.agriculture.dto.dwd.SoilSensorData;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Properties;

public class SoilDetailJob {

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
        kafkaProps.setProperty("group.id", "soil-detail-consumer");

        // 创建Kafka数据源
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "smart_agriculture_sensor_dirt",
                new SimpleStringSchema(),
                kafkaProps
        );
        kafkaConsumer.setStartFromEarliest();

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
                               data.getGreenhouseId() != null && !data.getGreenhouseId().isEmpty() &&
                               data.getMetrics() != null;
                    }
                });

        // 数据转换为DWD层格式
        DataStream<DwdSoilDetail> dwdStream = cleanedStream
                .map(new MapFunction<SoilSensorData, DwdSoilDetail>() {
                    @Override
                    public DwdSoilDetail map(SoilSensorData data) throws Exception {
                        // 转换为DWD层格式
                        return new DwdSoilDetail(
                                data.getGreenhouseId(),
                                data.getSensorId(),
                                Instant.ofEpochMilli(data.getTimestamp()).atZone(java.time.ZoneId.systemDefault()).toLocalDateTime(),
                                data.getMetrics().getSoilTemperature(),
                                data.getMetrics().getSoilMoisture(),
                                data.getMetrics().getSoilEc(),
                                data.getMetrics().getSoilPh(),
                                data.getMetrics().getSoilN(),
                                data.getMetrics().getSoilP(),
                                data.getMetrics().getSoilK()
                        );
                    }
                });

        // 创建ClickHouse Sink
        String insertSql = "INSERT INTO dwd_greenhouse_soil_detail " +
                "(greenhouse_id, sensor_id, ts, soil_temperature, soil_moisture, soil_ec, soil_ph, soil_n, soil_p, soil_k) " +
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
        SinkFunction<DwdSoilDetail> sink = JdbcSink.sink(
                insertSql,
                (PreparedStatement ps, DwdSoilDetail data) -> {
                    ps.setString(1, data.getGreenhouseId());
                    ps.setString(2, data.getSensorId());
                    ps.setTimestamp(3, Timestamp.valueOf(data.getTs()));
                    ps.setDouble(4, data.getSoilTemperature());
                    ps.setDouble(5, data.getSoilMoisture());
                    ps.setDouble(6, data.getSoilEc());
                    ps.setDouble(7, data.getSoilPh());
                    ps.setDouble(8, data.getSoilN());
                    ps.setDouble(9, data.getSoilP());
                    ps.setDouble(10, data.getSoilK());
                },
                executionOptions,
                jdbcOptions
        );
        dwdStream.addSink(sink).name("ClickHouse DWD Soil Detail Sink");

        // 打印数据流用于调试
        dwdStream.map(data -> "处理土壤详情数据: " + data.toString()).print();

        // 执行作业
        env.execute("DWD Greenhouse Soil Detail Processing");
    }

    // DWD土壤详情数据结构
    public static class DwdSoilDetail {
        private String greenhouseId;
        private String sensorId;
        private java.time.LocalDateTime ts;
        private double soilTemperature;
        private double soilMoisture;
        private double soilEc;
        private double soilPh;
        private double soilN;
        private double soilP;
        private double soilK;

        public DwdSoilDetail(String greenhouseId, String sensorId, java.time.LocalDateTime ts, double soilTemperature, double soilMoisture, double soilEc, double soilPh, double soilN, double soilP, double soilK) {
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

        public String getGreenhouseId() {
            return greenhouseId;
        }

        public String getSensorId() {
            return sensorId;
        }

        public java.time.LocalDateTime getTs() {
            return ts;
        }

        public double getSoilTemperature() {
            return soilTemperature;
        }

        public double getSoilMoisture() {
            return soilMoisture;
        }

        public double getSoilEc() {
            return soilEc;
        }

        public double getSoilPh() {
            return soilPh;
        }

        public double getSoilN() {
            return soilN;
        }

        public double getSoilP() {
            return soilP;
        }

        public double getSoilK() {
            return soilK;
        }

        @Override
        public String toString() {
            return "DwdSoilDetail{" +
                    "greenhouseId='" + greenhouseId + '\'' +
                    ", sensorId='" + sensorId + '\'' +
                    ", ts=" + ts +
                    ", soilTemperature=" + soilTemperature +
                    ", soilMoisture=" + soilMoisture +
                    ", soilEc=" + soilEc +
                    ", soilPh=" + soilPh +
                    ", soilN=" + soilN +
                    ", soilP=" + soilP +
                    ", soilK=" + soilK +
                    '}';
        }
    }
}