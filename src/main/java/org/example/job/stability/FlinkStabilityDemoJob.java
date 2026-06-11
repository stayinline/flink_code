package org.example.job.stability;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.example.dto.CourseWatchPartial;
import org.example.dto.StateDemoEvent;
import org.example.job.state.StateBackendConfigurator;

import java.time.Duration;
import java.util.Properties;

/**
 * 重启策略、反压、数据倾斜串讲演示 Job（在线教育学习心跳 → 课程维度聚合）。
 * <p>
 * 场景（启动参数 {@code scenario restart slowSinkMs saltBuckets backend}）：
 * <ul>
 *   <li>{@code backpressure} — 慢 Sink 制造反压，UI BackPressure 定位瓶颈</li>
 *   <li>{@code skew} — 热点课程 {@link StabilityConfigurator#HOT_COURSE_ID} 倾斜</li>
 *   <li>{@code twophase} — salt 前缀两阶段聚合打散热点</li>
 * </ul>
 * <p>
 * 例：{@code skew fixed 0 8 hashmap} 或 {@code backpressure fixed 300 0 hashmap}
 * <p>
 * 文档：{@code src/main/resources/stability/FlinkStabilityDemoGuide.md}
 */
public class FlinkStabilityDemoJob {

    public static final String KAFKA_BROKER = "192.168.1.124:9092";
    public static final String TOPIC = "test_flink_stability";
    public static final String GROUP_ID = "flink-stability-demo-consumer";

    public static final Duration OUT_OF_ORDERNESS = Duration.ofSeconds(5);

    public static void main(String[] args) throws Exception {
        StabilityConfigurator.StabilityOptions options = StabilityConfigurator.resolveOptions(args);

        Configuration flinkConfig = StateBackendConfigurator.createFlinkConfiguration(options.backend);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(4, flinkConfig);
        env.setParallelism(4);
        env.getConfig().setAutoWatermarkInterval(1000);

        StabilityConfigurator.configureEnvironment(env, options);

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKER);
        kafkaProps.setProperty("group.id", GROUP_ID);

        DataStream<StateDemoEvent> sourceStream = buildKafkaStream(env, kafkaProps);

        buildPipeline(sourceStream, options);

        printStartupBanner(options);
        env.execute("Flink Stability Demo - Restart / Backpressure / Skew");
    }

    static void buildPipeline(DataStream<StateDemoEvent> sourceStream,
                              StabilityConfigurator.StabilityOptions options) {
        DataStream<StateDemoEvent> probed = sourceStream
                .map(new SubtaskLoadProbeFunction())
                .name("LoadProbe");

        switch (options.scenario) {
            case StabilityConfigurator.SCENARIO_SKEW:
                buildSkewPipeline(probed);
                break;
            case StabilityConfigurator.SCENARIO_TWOPHASE:
                buildTwoPhasePipeline(probed, options.saltBuckets);
                break;
            case StabilityConfigurator.SCENARIO_BACKPRESSURE:
            default:
                buildBackpressurePipeline(probed, options.slowSinkMs);
                break;
        }
    }

    private static void buildBackpressurePipeline(DataStream<StateDemoEvent> stream, long slowSinkMs) {
        stream
                .keyBy(StateDemoEvent::getStudentId)
                .process(new NaiveCourseAggregateFunction())
                .name("Aggregate-ByCourse")
                .addSink(new SlowBackpressureSinkFunction(slowSinkMs))
                .name("SlowSink-" + slowSinkMs + "ms");
    }

    private static void buildSkewPipeline(DataStream<StateDemoEvent> stream) {
        stream
                .keyBy(StateDemoEvent::getCourseId)
                .process(new NaiveCourseAggregateFunction())
                .name("Skew-Naive-Aggregate")
                .print("倾斜");
    }

    private static void buildTwoPhasePipeline(DataStream<StateDemoEvent> stream, int saltBuckets) {
        stream
                .keyBy(e -> LocalSaltedAggregateFunction.saltedKey(
                        e.getCourseId(), e.getStudentId(), saltBuckets))
                .process(new LocalSaltedAggregateFunction(saltBuckets))
                .name("2Phase-Local")
                .keyBy(CourseWatchPartial::getCourseId)
                .process(new GlobalCourseMergeFunction())
                .name("2Phase-Global")
                .print("两阶段");
    }

    private static DataStream<StateDemoEvent> buildKafkaStream(
            StreamExecutionEnvironment env,
            Properties kafkaProps) {

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                TOPIC,
                new SimpleStringSchema(),
                kafkaProps
        );
        consumer.setStartFromLatest();

        return env
                .addSource(consumer)
                .name("Kafka-StudyHeartbeat")
                .map(new JsonToEventMapper())
                .filter(new ValidEventFilter())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<StateDemoEvent>forBoundedOutOfOrderness(OUT_OF_ORDERNESS)
                                .withTimestampAssigner((event, ts) -> event.getTs())
                )
                .name("Timestamps/WM");
    }

    private static void printStartupBanner(StabilityConfigurator.StabilityOptions options) {
        System.out.println("========================================");
        System.out.println("Flink 稳定性串讲 Demo 已启动");
        System.out.println("Kafka: " + KAFKA_BROKER + " | topic: " + TOPIC);
        System.out.println("并行度: 4（倾斜场景请对照各 subtask [LOAD-PROBE] / [SKEW-NAIVE]）");
        System.out.println("场景: " + StabilityConfigurator.describeScenario(options.scenario));
        System.out.println("重启策略: " + StabilityConfigurator.describeRestart(options.restart));
        System.out.println("慢 Sink: " + options.slowSinkMs + "ms | Salt 桶: " + options.saltBuckets);
        System.out.println("State Backend: " + StateBackendConfigurator.describeBackend(options.backend));
        System.out.println("UI: http://localhost:8081 → BackPressure / Metrics(busyTimeMsPerSecond)");
        System.out.println("文档: resources/stability/FlinkStabilityDemoGuide.md");
        System.out.println("请运行 FlinkStabilityDemoJobTest 发送测试数据");
        System.out.println("对比实验：");
        System.out.println("  1) backpressure fixed 300 0   → 反压链路 + 慢 Sink");
        System.out.println("  2) skew fixed 0 8            → 热点 " + StabilityConfigurator.HOT_COURSE_ID);
        System.out.println("  3) twophase fixed 0 8        → 两阶段聚合打散");
        System.out.println("========================================");
    }

    private static class JsonToEventMapper implements MapFunction<String, StateDemoEvent> {
        private final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public StateDemoEvent map(String value) {
            try {
                return objectMapper.readValue(value, StateDemoEvent.class);
            } catch (Exception e) {
                System.err.println("JSON 解析失败: " + value + ", 错误: " + e.getMessage());
                return null;
            }
        }
    }

    private static class ValidEventFilter implements FilterFunction<StateDemoEvent> {
        @Override
        public boolean filter(StateDemoEvent event) {
            return event != null
                    && event.getStudentId() != null
                    && !event.getStudentId().isEmpty()
                    && event.getCourseId() != null
                    && event.getTs() != null
                    && event.getTs() > 0
                    && event.getEventType() != null;
        }
    }
}
