package org.example.job;

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
import org.example.dto.GreenhouseSensorData;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class GreenhouseSensorJob {

    // 告警配置管理类 - 静态单例模式
    private static class AlertConfigManager {
        private static AlertConfigManager instance;
        private final String clickhouseUrl;
        private final String username;
        private final String password;
        private final Map<String, AlertRule> alertRules = new ConcurrentHashMap<>();
        private final ScheduledExecutorService executorService;

        private AlertConfigManager(String clickhouseUrl, String username, String password) {
            this.clickhouseUrl = clickhouseUrl;
            this.username = username;
            this.password = password;
            this.executorService = Executors.newSingleThreadScheduledExecutor();
            // 初始化加载配置
            loadAlertConfigs();
            // 每5分钟更新一次配置
            this.executorService.scheduleAtFixedRate(this::loadAlertConfigs, 5, 5, TimeUnit.MINUTES);
        }

        public static synchronized AlertConfigManager getInstance(String clickhouseUrl, String username, String password) {
            if (instance == null) {
                instance = new AlertConfigManager(clickhouseUrl, username, password);
            }
            return instance;
        }

        public static synchronized AlertConfigManager getInstance() {
            if (instance == null) {
                throw new IllegalStateException("AlertConfigManager not initialized");
            }
            return instance;
        }

        public void loadAlertConfigs() {
            try (Connection conn = DriverManager.getConnection(clickhouseUrl, username, password);
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT rule_name, metric_name, min_value, max_value, alert_message, enabled FROM alert_config WHERE enabled = 1")) {

                Map<String, AlertRule> newRules = new ConcurrentHashMap<>();
                while (rs.next()) {
                    String ruleName = rs.getString("rule_name");
                    String metricName = rs.getString("metric_name");
                    double minValue = rs.getDouble("min_value");
                    double maxValue = rs.getDouble("max_value");
                    String alertMessage = rs.getString("alert_message");
                    boolean enabled = rs.getBoolean("enabled");

                    if (enabled) {
                        newRules.put(metricName, new AlertRule(ruleName, metricName, minValue, maxValue, alertMessage));
                    }
                }

                // 更新规则
                alertRules.clear();
                alertRules.putAll(newRules);
                System.out.println("告警配置更新完成，当前规则数量: " + alertRules.size());

            } catch (Exception e) {
                System.err.println("加载告警配置失败: " + e.getMessage());
            }
        }

        public Map<String, AlertRule> getAlertRules() {
            return alertRules;
        }

        public void shutdown() {
            executorService.shutdown();
        }
    }

    // 告警规则类
    private static class AlertRule {
        private final String ruleName;
        private final String metricName;
        private final double minValue;
        private final double maxValue;
        private final String alertMessage;

        public AlertRule(String ruleName, String metricName, double minValue, double maxValue, String alertMessage) {
            this.ruleName = ruleName;
            this.metricName = metricName;
            this.minValue = minValue;
            this.maxValue = maxValue;
            this.alertMessage = alertMessage;
        }

        public String getRuleName() {
            return ruleName;
        }

        public String getMetricName() {
            return metricName;
        }

        public double getMinValue() {
            return minValue;
        }

        public double getMaxValue() {
            return maxValue;
        }

        public String getAlertMessage() {
            return alertMessage;
        }

        public boolean isViolated(double value) {
            return value < minValue || value > maxValue;
        }
    }

    public static void main(String[] args) throws Exception {
        // 设置执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000); // 每5秒一次checkpoint

        // ClickHouse配置
        String clickhouseUrl = "jdbc:clickhouse://192.168.1.124:8123/default";
        String username = "default";
        String password = "65e84be3";

        // 初始化告警配置管理器（静态单例）
        AlertConfigManager.getInstance(clickhouseUrl, username, password);

        // Kafka配置
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "192.168.1.124:9092");
        kafkaProps.setProperty("group.id", "greenhouse-sensor-consumer");

        // 创建Kafka数据源
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "smart_agriculture_sensor",
                new SimpleStringSchema(),
                kafkaProps
        );
        kafkaConsumer.setStartFromLatest(); // 从最新位置开始消费

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
                               data.getLocation() != null &&
                               data.getMetrics() != null &&
                               data.getDevice() != null;
                    }
                });

        // 数据合规校验和异常告警
        DataStream<GreenhouseSensorData> validatedStream = cleanedStream
                .map(new MapFunction<GreenhouseSensorData, GreenhouseSensorData>() {
                    @Override
                    public GreenhouseSensorData map(GreenhouseSensorData data) throws Exception {
                        // 合规校验和异常告警
                        validateAndAlert(data);
                        return data;
                    }
                });

        // 创建ClickHouse Sink
        String insertSql = "INSERT INTO greenhouse_sensor_data " +
                "(sensor_id, greenhouse_id, timestamp, location_x, location_y, location_z, " +
                "temperature, humidity, co2, light, soil_temperature, soil_moisture, " +
                "device_battery, device_status, device_firmware) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        // 配置JDBC连接参数
        JdbcConnectionOptions jdbcOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(clickhouseUrl)
                .withDriverName(ClickHouseDriver.class.getName())
                .withUsername("default") // 默认用户名
                .withPassword("65e84be3") // 默认空密码
                .withConnectionCheckTimeoutSeconds(60)
                .build();

        // 配置执行选项（批量插入优化）
        JdbcExecutionOptions executionOptions = JdbcExecutionOptions.builder()
                .withBatchSize(1000) // 批量大小
                .withBatchIntervalMs(1000) // 批量间隔
                .withMaxRetries(3) // 重试次数
                .build();

        // 添加ClickHouse Sink
        SinkFunction<GreenhouseSensorData> sink = JdbcSink.sink(
                insertSql,
                (PreparedStatement ps, GreenhouseSensorData data) -> {
                    ps.setString(1, data.getSensorId());
                    ps.setString(2, data.getGreenhouseId());
                    ps.setLong(3, data.getTimestamp());
                    ps.setDouble(4, data.getLocation().getX());
                    ps.setDouble(5, data.getLocation().getY());
                    ps.setDouble(6, data.getLocation().getZ());
                    ps.setDouble(7, data.getMetrics().getTemperature());
                    ps.setDouble(8, data.getMetrics().getHumidity());
                    ps.setInt(9, data.getMetrics().getCo2());
                    ps.setInt(10, data.getMetrics().getLight());
                    ps.setDouble(11, data.getMetrics().getSoilTemperature());
                    ps.setDouble(12, data.getMetrics().getSoilMoisture());
                    ps.setInt(13, data.getDevice().getBattery());
                    ps.setString(14, data.getDevice().getStatus());
                    ps.setString(15, data.getDevice().getFirmware());
                },
                executionOptions,
                jdbcOptions
        );
        validatedStream.addSink(sink).name("ClickHouse Sink");

        // 打印数据流用于调试
        validatedStream.map(data -> "处理数据: " + data.toString()).print();

        // 执行作业
        env.execute("Greenhouse Sensor Data Processing");
    }

    /**
     * 数据合规校验和异常告警
     */
    private static void validateAndAlert(GreenhouseSensorData data) {
        Map<String, AlertRule> alertRules = AlertConfigManager.getInstance().getAlertRules();

        // 温度异常检测
        AlertRule tempRule = alertRules.get("temperature");
        if (tempRule != null && tempRule.isViolated(data.getMetrics().getTemperature())) {
            sendAlert(tempRule.getRuleName(), "大棚 " + data.getGreenhouseId() + " 传感器 " + data.getSensorId() + " 温度值为 " + data.getMetrics().getTemperature() + "°C，" + tempRule.getAlertMessage());
        }

        // 湿度异常检测
        AlertRule humidityRule = alertRules.get("humidity");
        if (humidityRule != null && humidityRule.isViolated(data.getMetrics().getHumidity())) {
            sendAlert(humidityRule.getRuleName(), "大棚 " + data.getGreenhouseId() + " 传感器 " + data.getSensorId() + " 湿度值为 " + data.getMetrics().getHumidity() + "%，" + humidityRule.getAlertMessage());
        }

        // CO2异常检测
        AlertRule co2Rule = alertRules.get("co2");
        if (co2Rule != null && co2Rule.isViolated(data.getMetrics().getCo2())) {
            sendAlert(co2Rule.getRuleName(), "大棚 " + data.getGreenhouseId() + " 传感器 " + data.getSensorId() + " CO2值为 " + data.getMetrics().getCo2() + "ppm，" + co2Rule.getAlertMessage());
        }

        // 土壤湿度异常检测
        AlertRule soilMoistureRule = alertRules.get("soil_moisture");
        if (soilMoistureRule != null && soilMoistureRule.isViolated(data.getMetrics().getSoilMoisture())) {
            sendAlert(soilMoistureRule.getRuleName(), "大棚 " + data.getGreenhouseId() + " 传感器 " + data.getSensorId() + " 土壤湿度值为 " + data.getMetrics().getSoilMoisture() + "%，" + soilMoistureRule.getAlertMessage());
        }

        // 电池电量低告警
        AlertRule batteryRule = alertRules.get("battery");
        if (batteryRule != null && data.getDevice().getBattery() < batteryRule.getMinValue()) {
            sendAlert(batteryRule.getRuleName(), "大棚 " + data.getGreenhouseId() + " 传感器 " + data.getSensorId() + " 电池电量为 " + data.getDevice().getBattery() + "%，" + batteryRule.getAlertMessage());
        }

        // 设备离线告警
        AlertRule statusRule = alertRules.get("status");
        if (statusRule != null && !"ONLINE".equals(data.getDevice().getStatus())) {
            sendAlert(statusRule.getRuleName(), "大棚 " + data.getGreenhouseId() + " 传感器 " + data.getSensorId() + " 状态为 " + data.getDevice().getStatus() + "，" + statusRule.getAlertMessage());
        }
    }

    /**
     * 发送钉钉告警消息
     */
    private static void sendAlert(String title, String message) {
        // 这里实现钉钉消息推送逻辑
        // 由于没有实际的钉钉机器人webhook，这里只是打印告警信息
        System.out.println("[告警] " + title + ": " + message);
        // 实际实现时，需要调用钉钉机器人API发送消息
        // 例如：
        // String webhook = "https://oapi.dingtalk.com/robot/send?access_token=your_token";
        // String json = "{\"msgtype\":\"text\",\"text\":{\"content\":\"[告警] \" + title + \"\\n\" + message + \"\"}}";
        // 然后使用HTTP客户端发送POST请求
    }
}