//package org.example.job;
//
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.connector.jdbc.JdbcInputFormat;
//import org.apache.flink.api.java.typeutils.RowTypeInfo;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.sink.SinkFunction;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
//import org.apache.flink.types.Row;
//import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
//import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
//import org.apache.flink.connector.jdbc.JdbcSink;
//import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
//import org.example.dto.KafkaData;
//import org.example.dto.ValidationRule;
//import org.example.dto.ValidationResult;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.sql.PreparedStatement;
//import java.sql.SQLException;
//import java.time.LocalDateTime;
//import java.util.*;
//import java.util.regex.Pattern;
//
//import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
//
//public class ValidJob {
//    private static final Logger log = LoggerFactory.getLogger(ValidJob.class);
//    private static final ObjectMapper objectMapper = new ObjectMapper();
//
//    public static void main(String[] args) throws Exception {
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(5000);
//
//        // 配置Kafka消费者
//        Properties kafkaProps = new Properties();
//        kafkaProps.setProperty("bootstrap.servers", "192.168.250.42:9092");
//        kafkaProps.setProperty("group.id", "validation-consumer-group");
//        kafkaProps.setProperty("auto.offset.reset", "earliest");
//
//        // 创建Kafka数据源
//        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
//                "test_ck",
//                new org.apache.flink.api.common.serialization.SimpleStringSchema(),
//                kafkaProps
//        );
//
//        DataStream<String> kafkaDataStream = env.addSource(kafkaConsumer)
//                .name("Kafka Source");
//
//        // 解析JSON数据
//        DataStream<KafkaData> transformedStream = kafkaDataStream
//                .map(new KafkaDataMapper())
//                // 校验规则在这里应用，需要时可以直接删除这一行
//                .map(new ValidationMapFunction())
//                .filter(data -> !"error".equals(data.getId()));
//
//        // 可以添加其他处理逻辑...
//        transformedStream.map(data -> "处理数据: " + data.toString()).print();
//
//        env.execute("Data Validation with MapFunction Job");
//    }
//
//    // 校验逻辑的MapFunction实现
//    private static class ValidationMapFunction implements MapFunction<KafkaData, KafkaData> {
//        private static final Logger log = LoggerFactory.getLogger(ValidationMapFunction.class);
//        // 存储规则的映射：sourceType -> fieldName -> List<ValidationRule>
//        private Map<Integer, Map<String, List<ValidationRule>>> rulesMap;
//        // 告警Sink
//        private SinkFunction<ValidationResult> alertSink;
//
//        @Override
//        public void open(Configuration parameters) throws Exception {
//            // 初始化规则映射
//            rulesMap = new HashMap<>();
//            // 加载校验规则
//            loadValidationRules();
//            // 初始化告警Sink
//            initAlertSink();
//        }
//
//        @Override
//        public KafkaData map(KafkaData data) throws Exception {
//            // 对数据进行校验
//            List<ValidationResult> validationResults = validateData(data);
//
//            // 如果有校验失败的结果，发送告警
//            if (!validationResults.isEmpty()) {
//                for (ValidationResult result : validationResults) {
//                    alertSink.invoke(result, null);
//                    log.warn("数据校验失败 - ID: {}, 字段: {}, 错误: {}",
//                            data.getId(), result.getFieldName(), result.getErrorMessage());
//                }
//            } else {
//                log.info("数据校验通过 - ID: {}", data.getId());
//            }
//
//            // 返回原始数据，不改变数据流
//            return data;
//        }
//
//        // 加载校验规则
//        private void loadValidationRules() {
//            try {
//                String validationRulesSql = "SELECT id, rule_name, source_type, field_name, " +
//                        "validation_type, validation_param, error_message, alert_level, enabled " +
//                        "FROM validation_rules WHERE enabled = true";
//
//                // 定义返回的字段类型
//                TypeInformation[] fieldTypes = new TypeInformation[]{
//                        TypeInformation.of(Long.class),        // id
//                        TypeInformation.of(String.class),      // rule_name
//                        TypeInformation.of(Integer.class),     // source_type
//                        TypeInformation.of(String.class),      // field_name
//                        TypeInformation.of(String.class),      // validation_type
//                        TypeInformation.of(String.class),      // validation_param
//                        TypeInformation.of(String.class),      // error_message
//                        TypeInformation.of(String.class),      // alert_level
//                        TypeInformation.of(Boolean.class)      // enabled
//                };
//
//                RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes);
//
//                // 使用JdbcInputFormat读取规则
//                JdbcInputFormat jdbcInputFormat = JdbcInputFormat.buildJdbcInputFormat()
//                        .setDrivername("com.mysql.cj.jdbc.Driver")
//                        .setDBUrl("jdbc:mysql://localhost:3306/validation_db")
//                        .setUsername("root")
//                        .setPassword("password")
//                        .setQuery(validationRulesSql)
//                        .setRowTypeInfo(rowTypeInfo)
//                        .finish();
//
//                // 执行查询并处理结果
//                List<ValidationRule> rules = new ArrayList<>();
//                jdbcInputFormat.open(null);
//                Row row;
//                while ((row = jdbcInputFormat.nextRecord()) != null) {
//                    ValidationRule rule = new ValidationRule();
//                    rule.setId((Long) row.getField(0));
//                    rule.setRuleName((String) row.getField(1));
//                    rule.setSourceType((Integer) row.getField(2));
//                    rule.setFieldName((String) row.getField(3));
//                    rule.setValidationType((String) row.getField(4));
//                    rule.setValidationParam((String) row.getField(5));
//                    rule.setErrorMessage((String) row.getField(6));
//                    rule.setAlertLevel((String) row.getField(7));
//                    rule.setEnabled((Boolean) row.getField(8));
//                    rules.add(rule);
//                }
//                jdbcInputFormat.close();
//
//                // 构建规则映射
//                for (ValidationRule rule : rules) {
//                    Integer sourceType = rule.getSourceType();
//                    String fieldName = rule.getFieldName();
//
//                    // 按sourceType分组
//                    Map<String, List<ValidationRule>> fieldRules = rulesMap.computeIfAbsent(
//                            sourceType, k -> new HashMap<>());
//
//                    // 按fieldName分组
//                    List<ValidationRule> fieldRuleList = fieldRules.computeIfAbsent(
//                            fieldName, k -> new ArrayList<>());
//
//                    fieldRuleList.add(rule);
//                }
//
//                log.info("成功加载 {} 条校验规则", rules.size());
//            } catch (Exception e) {
//                log.error("加载校验规则失败", e);
//                throw new RuntimeException("加载校验规则失败", e);
//            }
//        }
//
//        // 初始化告警Sink
//        private void initAlertSink() {
//            String alertTableSql = "INSERT INTO validation_alerts " +
//                    "(alert_time, field_name, error_message, alert_level, data_id, raw_data) " +
//                    "VALUES (?, ?, ?, ?, ?, ?)";
//
//            JdbcConnectionOptions jdbcOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
//                    .withUrl("jdbc:mysql://localhost:3306/validation_db")
//                    .withDriverName("com.mysql.cj.jdbc.Driver")
//                    .withUsername("root")
//                    .withPassword("password")
//                    .build();
//
//            alertSink = JdbcSink.sink(
//                    alertTableSql,
//                    new JdbcStatementBuilder<ValidationResult>() {
//                        @Override
//                        public void accept(PreparedStatement ps, ValidationResult result) throws SQLException {
//                            ps.setString(1, LocalDateTime.now().toString());
//                            ps.setString(2, result.getFieldName());
//                            ps.setString(3, result.getErrorMessage());
//                            ps.setString(4, result.getAlertLevel());
//                            ps.setString(5, result.getDataId());
//                            ps.setString(6, result.getRawData());
//                        }
//                    },
//                    JdbcExecutionOptions.builder()
//                            .withBatchSize(100)
//                            .withMaxRetries(3)
//                            .build(),
//                    jdbcOptions
//            );
//        }
//
//        // 校验数据
//        private List<ValidationResult> validateData(KafkaData data) {
//            List<ValidationResult> results = new ArrayList<>();
//
//            try {
//                // 获取source_type，默认为0
//                String dataId = data.getId() != null ? data.getId() : "unknown_id";
//                String rawData = objectMapper.writeValueAsString(data);
//
//                // 收集适用于当前source_type的所有规则
//                List<ValidationRule> applicableRules = new ArrayList<>();
//
//                // 添加适用于所有source_type的规则（source_type为null）
//                if (rulesMap.containsKey(null)) {
//                    rulesMap.get(null).values().forEach(applicableRules::addAll);
//                }
//
//
//                // 对每个字段应用校验规则
//                for (ValidationRule rule : applicableRules) {
//                    ValidationResult result = validateField(data, rule, dataId, rawData);
//                    if (!result.isValid()) {
//                        results.add(result);
//                    }
//                }
//            } catch (Exception e) {
//                log.error("数据校验过程发生错误", e);
//            }
//
//            return results;
//        }
//
//        // 字段校验逻辑
//        private ValidationResult validateField(KafkaData data, ValidationRule rule,
//                                               String dataId, String rawData) {
//
//            ValidationResult result = new ValidationResult();
//            result.setFieldName(rule.getFieldName());
//            result.setRuleId(rule.getId());
//            result.setErrorMessage(rule.getErrorMessage());
//            result.setAlertLevel(rule.getAlertLevel());
//            result.setDataId(dataId);
//            result.setRawData(rawData);
//            result.setValid(true);
//
//            String fieldName = rule.getFieldName();
//            String validationType = rule.getValidationType();
//
//            try {
//                // 获取字段值
//                Object fieldValue = getFieldValue(data, fieldName);
//
//                // 检查字段是否存在
//                if (fieldValue == null) {
//                    // 如果规则要求字段必须存在（如非空校验），则标记为无效
//                    if ("NOT_NULL".equals(validationType)) {
//                        result.setValid(false);
//                    }
//                    return result;
//                }
//
//                // 根据校验类型执行不同的校验逻辑
//                switch (validationType) {
//                    case "NOT_NULL":
//                        if (fieldValue == null ||
//                                (fieldValue instanceof String && ((String) fieldValue).trim().isEmpty())) {
//                            result.setValid(false);
//                        }
//                        break;
//
//                    case "TYPE_CHECK":
//                        String type = rule.getValidationParam();
//                        if ("TIMESTAMP_MILLIS".equals(type)) {
//                            // 校验是否为毫秒级时间戳（13位数字）
//                            if (!(fieldValue instanceof Long) ||
//                                    String.valueOf(fieldValue).length() != 13) {
//                                result.setValid(false);
//                            }
//                        } else if ("INTEGER".equals(type) && !(fieldValue instanceof Integer)) {
//                            result.setValid(false);
//                        } else if ("DOUBLE".equals(type) && !(fieldValue instanceof Double)) {
//                            result.setValid(false);
//                        } else if ("STRING".equals(type) && !(fieldValue instanceof String)) {
//                            result.setValid(false);
//                        }
//                        break;
//
//                    case "VALUE_EQUALS":
//                        String expectedValue = rule.getValidationParam();
//                        if (!expectedValue.equals(fieldValue.toString())) {
//                            result.setValid(false);
//                        }
//                        break;
//
//                    case "VALUE_IN":
//                        String[] allowedValues = rule.getValidationParam().split(",");
//                        boolean found = false;
//                        String actualValue = fieldValue.toString();
//                        for (String allowed : allowedValues) {
//                            if (allowed.equals(actualValue)) {
//                                found = true;
//                                break;
//                            }
//                        }
//                        if (!found) {
//                            result.setValid(false);
//                        }
//                        break;
//
//                    case "REGEX_MATCH":
//                        String regex = rule.getValidationParam();
//                        if (!(fieldValue instanceof String) ||
//                                !Pattern.matches(regex, (String) fieldValue)) {
//                            result.setValid(false);
//                        }
//                        break;
//
//                    case "MIN_VALUE":
//                        if (fieldValue instanceof Number) {
//                            double min = Double.parseDouble(rule.getValidationParam());
//                            if (((Number) fieldValue).doubleValue() < min) {
//                                result.setValid(false);
//                            }
//                        } else {
//                            result.setValid(false);
//                        }
//                        break;
//
//                    case "MAX_VALUE":
//                        if (fieldValue instanceof Number) {
//                            double max = Double.parseDouble(rule.getValidationParam());
//                            if (((Number) fieldValue).doubleValue() > max) {
//                                result.setValid(false);
//                            }
//                        } else {
//                            result.setValid(false);
//                        }
//                        break;
//
//                    default:
//                        log.warn("未知的校验类型: {}", validationType);
//                }
//            } catch (Exception e) {
//                log.error("校验字段 {} 时发生错误: {}", fieldName, e.getMessage());
//                result.setValid(false);
//                result.setErrorMessage("校验过程发生错误: " + e.getMessage());
//            }
//
//            return result;
//        }
//
//        // 获取KafkaData中的字段值
//        private Object getFieldValue(KafkaData data, String fieldName) {
//            switch (fieldName) {
//                case "id":
//                    return data.getId();
//                case "message":
//                    return data.getMessage();
//                case "value":
//                    return data.getValue();
//                case "event_time":
//                    return data.getEventTime();
//                // 添加其他字段的映射
//                default:
//                    log.warn("未知的字段名: {}", fieldName);
//                    return null;
//            }
//        }
//    }
//
//    // KafkaData映射器
//    private static class KafkaDataMapper implements MapFunction<String, KafkaData> {
//        @Override
//        public KafkaData map(String value) throws Exception {
//            try {
//                KafkaData kafkaData = objectMapper.readValue(value, KafkaData.class);
//                log.info("解析到kafka数据={}", kafkaData);
//                return kafkaData;
//            } catch (Exception e) {
//                log.error("JSON解析失败: " + value + ", 错误: " + e.getMessage(), e);
//                return new KafkaData("error", value, 1.2D, LocalDateTime.now().toString());
//            }
//        }
//    }
//}
