//package org.example.job;
//
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.api.common.functions.FilterFunction;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.api.java.typeutils.RowTypeInfo;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.connector.jdbc.*;
//import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
//import org.apache.flink.streaming.api.datastream.BroadcastStream;
//import org.apache.flink.streaming.api.datastream.ConnectedStreams;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
//import org.apache.flink.streaming.api.functions.sink.SinkFunction;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
//import org.apache.flink.types.Row;
//import org.apache.flink.util.Collector;
//import org.example.dto.KafkaData;
//import org.example.dto.ValidationRule;
//import org.example.dto.ValidationResult;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.apache.flink.connector.jdbc.JdbcInputFormat;
//import java.sql.PreparedStatement;
//import java.sql.SQLException;
//import java.time.LocalDateTime;
//import java.util.*;
//import java.util.regex.Pattern;
//
//import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
//import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
//
//public class ValidationJob {
//    private static final Logger log = LoggerFactory.getLogger(ValidationJob.class);
//    private static final ObjectMapper objectMapper = new ObjectMapper();
//
//    // 定义规则广播的描述符
//    public static final String RULES_STATE_DESCRIPTOR = "validationRules";
//
//    public static void main(String[] args) throws Exception {
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(5000);
//
//        // 1. 从数据库加载校验规则（使用JdbcInputFormat替代JdbcSource）
//        DataStream<ValidationRule> rulesStream = loadValidationRules(env);
//
//        // 2. 广播规则流
//        DataStream<ValidationRule> broadcastRules = rulesStream.broadcast();
//
//        // 3. 配置Kafka消费者
//        Properties kafkaProps = new Properties();
//        kafkaProps.setProperty("bootstrap.servers", "192.168.250.42:9092");
//        kafkaProps.setProperty("group.id", "validation-consumer-group");
//        kafkaProps.setProperty("auto.offset.reset", "earliest");
//
//        // 4. 创建Kafka数据源
//        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
//                "test_ck",
//                new org.apache.flink.api.common.serialization.SimpleStringSchema(),
//                kafkaProps
//        );
//
//        DataStream<String> kafkaDataStream = env.addSource(kafkaConsumer)
//                .name("Kafka Source");
//
//        // 5. 解析JSON数据
//        DataStream<JsonNode> jsonStream = kafkaDataStream
//                .map(new JsonParser())
//                .filter(Objects::nonNull);
//
//        // 6. 将业务数据流与广播的规则流连接
//        ConnectedStreams<JsonNode, ValidationRule> connectedStream =
//                jsonStream.connect(broadcastRules);
//
//        // 7. 应用校验规则
//        DataStream<Tuple2<JsonNode, List<ValidationResult>>> validatedStream = connectedStream
//                .process(new ValidationProcessFunction())
//                .name("Data Validation Process");
//
//        // 8. 分离校验通过和失败的数据
//        DataStream<JsonNode> validDataStream = validatedStream
//                .filter(new FilterFunction<Tuple2<JsonNode, List<ValidationResult>>>() {
//                    @Override
//                    public boolean filter(Tuple2<JsonNode, List<ValidationResult>> value) throws Exception {
//                        return value.f1.isEmpty();
//                    }
//                })
//                .map(new MapFunction<Tuple2<JsonNode, List<ValidationResult>>, JsonNode>() {
//                    @Override
//                    public JsonNode map(Tuple2<JsonNode, List<ValidationResult>> value) throws Exception {
//                        return value.f0;
//                    }
//                })
//                .name("Valid Data");
//
//        // 9. 处理校验失败的数据并告警
//        DataStream<List<ValidationResult>> invalidDataStream = validatedStream
//                .filter(new FilterFunction<Tuple2<JsonNode, List<ValidationResult>>>() {
//                    @Override
//                    public boolean filter(Tuple2<JsonNode, List<ValidationResult>> value) throws Exception {
//                        return !value.f1.isEmpty();
//                    }
//                })
//                .map(new MapFunction<Tuple2<JsonNode, List<ValidationResult>>, List<ValidationResult>>() {
//                    @Override
//                    public List<ValidationResult> map(Tuple2<JsonNode, List<ValidationResult>> value) throws Exception {
//                        return value.f1;
//                    }
//                })
//                .name("Invalid Data");
//
//        // 10. 存储校验失败的告警信息
//        invalidDataStream.addSink(createAlertSink())
//                .name("Alert Sink");
//
//        // 11. 处理并存储校验通过的数据（原有逻辑）
//        DataStream<KafkaData> transformedStream = validDataStream
//                .map(new KafkaDataMapper())
//                .filter(data -> !"error".equals(data.getId()));
//
//        // 12. ClickHouse Sink配置（原有逻辑）
//        String clickhouseUrl = "jdbc:clickhouse://192.168.1.124:8123/default";
//        String insertSql = "INSERT INTO testdb.test_events (id, message, value, event_time) VALUES (?, ?, ?, ?)";
//
//        JdbcConnectionOptions jdbcOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
//                .withUrl(clickhouseUrl)
//                .withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
//                .withUsername("default")
//                .withPassword("65e84be3")
//                .build();
//
//        JdbcExecutionOptions executionOptions = JdbcExecutionOptions.builder()
//                .withBatchSize(1000)
//                .withBatchIntervalMs(1000)
//                .withMaxRetries(3)
//                .build();
//
//        SinkFunction<KafkaData> sink = JdbcSink.sink(
//                insertSql,
//                new KafkaDataStatementBuilder(),
//                executionOptions,
//                jdbcOptions
//        );
//
//        transformedStream.addSink(sink).name("ClickHouse Sink");
//        transformedStream.map(data -> "处理数据: " + data.toString()).print();
//
//        env.execute("Data Validation and Processing Job");
//    }
//
//    // 从数据库加载校验规则 - 使用JdbcInputFormat适配Flink 1.14.6
//    private static DataStream<ValidationRule> loadValidationRules(StreamExecutionEnvironment env) {
//        String validationRulesSql = "SELECT id, rule_name, source_type, field_name, " +
//                "validation_type, validation_param, error_message, alert_level, enabled " +
//                "FROM validation_rules WHERE enabled = true";
//
//        // 定义返回的字段类型
//        TypeInformation[] fieldTypes = new TypeInformation[]{
//                TypeInformation.of(Long.class),        // id
//                TypeInformation.of(String.class),      // rule_name
//                TypeInformation.of(Integer.class),     // source_type
//                TypeInformation.of(String.class),      // field_name
//                TypeInformation.of(String.class),      // validation_type
//                TypeInformation.of(String.class),      // validation_param
//                TypeInformation.of(String.class),      // error_message
//                TypeInformation.of(String.class),      // alert_level
//                TypeInformation.of(Boolean.class)      // enabled
//        };
//
//        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes);
//
//        // 使用JdbcInputFormat替代JdbcSource
//        JdbcInputFormat jdbcInputFormat = JdbcInputFormat.buildJdbcInputFormat()
//                .setDrivername("com.mysql.cj.jdbc.Driver")
//                .setDBUrl("jdbc:mysql://localhost:3306/validation_db")
//                .setUsername("root")
//                .setPassword("password")
//                .setQuery(validationRulesSql)
//                .setRowTypeInfo(rowTypeInfo)
//                .finish();
//
//        // 创建输入流并转换为ValidationRule对象
//        return env.createInput(jdbcInputFormat)
//                .map(new MapFunction<Row, ValidationRule>() {
//                    @Override
//                    public ValidationRule map(Row row) throws Exception {
//                        ValidationRule rule = new ValidationRule();
//                        rule.setId((Long) row.getField(0));
//                        rule.setRuleName((String) row.getField(1));
//                        rule.setSourceType((Integer) row.getField(2));
//                        rule.setFieldName((String) row.getField(3));
//                        rule.setValidationType((String) row.getField(4));
//                        rule.setValidationParam((String) row.getField(5));
//                        rule.setErrorMessage((String) row.getField(6));
//                        rule.setAlertLevel((String) row.getField(7));
//                        rule.setEnabled((Boolean) row.getField(8));
//                        return rule;
//                    }
//                })
//                .name("Validation Rules Source");
//    }
//
//    // 创建告警信息存储Sink
//    private static SinkFunction<List<ValidationResult>> createAlertSink() {
//        String alertTableSql = "INSERT INTO validation_alerts " +
//                "(alert_time, field_name, error_message, alert_level, data_id, raw_data) " +
//                "VALUES (?, ?, ?, ?, ?, ?)";
//
//        JdbcConnectionOptions jdbcOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
//                .withUrl("jdbc:mysql://localhost:3306/validation_db")
//                .withDriverName("com.mysql.cj.jdbc.Driver")
//                .withUsername("root")
//                .withPassword("password")
//                .build();
//
//        return JdbcSink.sink(
//                alertTableSql,
//                new JdbcStatementBuilder<List<ValidationResult>>() {
//                    @Override
//                    public void accept(PreparedStatement ps, List<ValidationResult> results) throws SQLException {
//                        // 为简化，这里只处理第一个结果，实际应用中可能需要循环处理所有结果
//                        if (!results.isEmpty()) {
//                            ValidationResult result = results.get(0);
//                            ps.setString(1, LocalDateTime.now().toString());
//                            ps.setString(2, result.getFieldName());
//                            ps.setString(3, result.getErrorMessage());
//                            ps.setString(4, result.getAlertLevel());
//                            ps.setString(5, result.getDataId());
//                            ps.setString(6, result.getRawData());
//                        }
//                    }
//                },
//                JdbcExecutionOptions.builder()
//                        .withBatchSize(100)
//                        .withMaxRetries(3)
//                        .build(),
//                jdbcOptions
//        );
//    }
//
//    // JSON解析器
//    private static class JsonParser implements MapFunction<String, JsonNode> {
//        @Override
//        public JsonNode map(String value) throws Exception {
//            try {
//                return objectMapper.readTree(value);
//            } catch (Exception e) {
//                log.error("JSON解析失败: " + value + ", 错误: " + e.getMessage(), e);
//                return null;
//            }
//        }
//    }
//
//    // 数据校验处理函数
//    private static class ValidationProcessFunction extends BroadcastProcessFunction<JsonNode, ValidationRule,
//            Tuple2<JsonNode, List<ValidationResult>>> {
//
//        // 存储规则的映射：sourceType -> fieldName -> List<ValidationRule>
//        private final Map<Integer, Map<String, List<ValidationRule>>> rulesMap = new HashMap<>();
//
//        @Override
//        public void processElement(JsonNode value, ReadOnlyContext ctx,
//                                   Collector<Tuple2<JsonNode, List<ValidationResult>>> out) throws Exception {
//
//            List<ValidationResult> validationResults = new ArrayList<>();
//            String dataId = value.has("id") ? value.get("id").asText() : "unknown_id";
//            String rawData = value.toString();
//
//            // 获取source_type，默认为0
//            int sourceType = value.has("source_type") ? value.get("source_type").asInt() : 0;
//
//            // 收集适用于当前source_type的所有规则
//            List<ValidationRule> applicableRules = new ArrayList<>();
//
//            // 添加适用于所有source_type的规则（source_type为null）
//            if (rulesMap.containsKey(null)) {
//                rulesMap.get(null).values().forEach(applicableRules::addAll);
//            }
//
//            // 添加适用于当前source_type的规则
//            if (rulesMap.containsKey(sourceType)) {
//                rulesMap.get(sourceType).values().forEach(applicableRules::addAll);
//            }
//
//            // 应用所有适用的规则
//            for (ValidationRule rule : applicableRules) {
//                ValidationResult result = validateField(value, rule, dataId, rawData);
//                if (!result.isValid()) {
//                    validationResults.add(result);
//                }
//            }
//
//            out.collect(new Tuple2<>(value, validationResults));
//        }
//
//        @Override
//        public void processBroadcastElement(ValidationRule rule, Context ctx,
//                                            Collector<Tuple2<JsonNode, List<ValidationResult>>> out) throws Exception {
//
//            // 更新规则映射
//            Integer sourceType = rule.getSourceType();
//            String fieldName = rule.getFieldName();
//
//            // 按sourceType分组
//            Map<String, List<ValidationRule>> fieldRules = rulesMap.computeIfAbsent(
//                    sourceType, k -> new HashMap<>());
//
//            // 按fieldName分组
//            List<ValidationRule> rules = fieldRules.computeIfAbsent(
//                    fieldName, k -> new ArrayList<>());
//
//            // 更新规则列表
//            rules.removeIf(r -> r.getId().equals(rule.getId()));
//            if (rule.isEnabled()) {
//                rules.add(rule);
//            }
//        }
//
//        // 字段校验逻辑
//        private ValidationResult validateField(JsonNode data, ValidationRule rule,
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
//                // 检查字段是否存在
//                if (!data.has(fieldName)) {
//                    // 如果规则要求字段必须存在（如非空校验），则标记为无效
//                    if ("NOT_NULL".equals(validationType)) {
//                        result.setValid(false);
//                    }
//                    return result;
//                }
//
//                JsonNode fieldValue = data.get(fieldName);
//
//                // 根据校验类型执行不同的校验逻辑
//                switch (validationType) {
//                    case "NOT_NULL":
//                        if (fieldValue.isNull() || (fieldValue.isTextual() && fieldValue.asText().trim().isEmpty())) {
//                            result.setValid(false);
//                        }
//                        break;
//
//                    case "TYPE_CHECK":
//                        String type = rule.getValidationParam();
//                        if ("TIMESTAMP_MILLIS".equals(type)) {
//                            // 校验是否为毫秒级时间戳（13位数字）
//                            if (!fieldValue.isNumber() ||
//                                    String.valueOf(fieldValue.asLong()).length() != 13) {
//                                result.setValid(false);
//                            }
//                        } else if ("INTEGER".equals(type) && !fieldValue.isInt()) {
//                            result.setValid(false);
//                        } else if ("DOUBLE".equals(type) && !fieldValue.isNumber()) {
//                            result.setValid(false);
//                        } else if ("STRING".equals(type) && !fieldValue.isTextual()) {
//                            result.setValid(false);
//                        }
//                        break;
//
//                    case "VALUE_EQUALS":
//                        String expectedValue = rule.getValidationParam();
//                        if (!expectedValue.equals(fieldValue.asText())) {
//                            result.setValid(false);
//                        }
//                        break;
//
//                    case "VALUE_IN":
//                        String[] allowedValues = rule.getValidationParam().split(",");
//                        boolean found = false;
//                        String actualValue = fieldValue.asText();
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
//                        if (!Pattern.matches(regex, fieldValue.asText())) {
//                            result.setValid(false);
//                        }
//                        break;
//
//                    case "MIN_VALUE":
//                        double min = Double.parseDouble(rule.getValidationParam());
//                        if (fieldValue.asDouble() < min) {
//                            result.setValid(false);
//                        }
//                        break;
//
//                    case "MAX_VALUE":
//                        double max = Double.parseDouble(rule.getValidationParam());
//                        if (fieldValue.asDouble() > max) {
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
//    }
//
//    // KafkaData映射器
//    private static class KafkaDataMapper implements MapFunction<JsonNode, KafkaData> {
//        @Override
//        public KafkaData map(JsonNode value) throws Exception {
//            try {
//                KafkaData kafkaData = new KafkaData();
//                kafkaData.setId(value.get("id").asText());
//                kafkaData.setMessage(value.get("message").asText());
//                kafkaData.setValue(value.get("value").asDouble());
//                kafkaData.setEventTime(value.get("event_time").asText());
//                log.info("解析到kafka数据={}", kafkaData);
//                return kafkaData;
//            } catch (Exception e) {
//                log.error("解析Kafka数据失败: " + value + ", 错误: " + e.getMessage(), e);
//                return new KafkaData("error", value.toString(), 1.2D, LocalDateTime.now().toString());
//            }
//        }
//    }
//
//    // KafkaData StatementBuilder
//    private static class KafkaDataStatementBuilder implements JdbcStatementBuilder<KafkaData> {
//        @Override
//        public void accept(PreparedStatement ps, KafkaData data) throws SQLException {
//            ps.setString(1, data.getId());
//            ps.setString(2, data.getMessage());
//            ps.setDouble(3, data.getValue());
//            ps.setString(4, data.getEventTime());
//        }
//    }
//}
