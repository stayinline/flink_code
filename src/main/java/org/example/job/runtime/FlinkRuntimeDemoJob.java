package org.example.job.runtime;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.example.dto.StateDemoEvent;
import org.example.job.state.StateBackendConfigurator;

import java.time.Duration;
import java.util.Properties;
import java.util.function.Function;

/**
 * Flink 资源模型与运行时架构演示：JM/TM、Slot、Subtask、Operator Chain、反压传播。
 * <p>
 * 拓扑：Source → Map/Probe → keyBy → Tumbling Window/Aggregate → 慢 Sink
 * <p>
 * 启动参数：{@code scenario parallelism slowSinkMs chainMode backend}
 * <pre>
 *   backpressure 4 300 chain hashmap
 *   backpressure 4 300 nochain hashmap
 *   hotslot 4 300 chain hashmap
 *   skew 4 0 chain hashmap
 *   managed 4 200 chain rocksdb
 * </pre>
 * 文档：{@code src/main/resources/runtime/FlinkRuntimeDemoGuide.md}
 */
public class FlinkRuntimeDemoJob {

    public static final String KAFKA_BROKER = "192.168.1.124:9092";
    public static final String TOPIC = "test_flink_runtime";
    public static final String GROUP_ID = "flink-runtime-demo-consumer";

    public static final Duration OUT_OF_ORDERNESS = Duration.ofSeconds(5);
    public static final Time WINDOW_SIZE = Time.seconds(10);

    public static void main(String[] args) throws Exception {
        RuntimeConfigurator.RuntimeOptions options = RuntimeConfigurator.resolveOptions(args);

        Configuration flinkConfig = RuntimeConfigurator.createFlinkConfiguration(options);
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(options.parallelism, flinkConfig);
        env.setParallelism(options.parallelism);
        env.getConfig().setAutoWatermarkInterval(1000);

        RuntimeConfigurator.configureEnvironment(env, options);

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKER);
        kafkaProps.setProperty("group.id", GROUP_ID);

        DataStream<StateDemoEvent> sourceStream = buildKafkaStream(env, kafkaProps, options);
        buildPipeline(sourceStream, options);

        printStartupBanner(options);
        env.execute("Flink Runtime Demo - JM/TM/Slot/Backpressure [" + options.scenario + "]");
    }

    static void buildPipeline(DataStream<StateDemoEvent> sourceStream,
                              RuntimeConfigurator.RuntimeOptions options) {
        Function<StateDemoEvent, String> keySelector = RuntimeConfigurator.keyByCourseForSkew(options)
                ? StateDemoEvent::getCourseId
                : StateDemoEvent::getStudentId;

        String keyLabel = RuntimeConfigurator.keyByCourseForSkew(options) ? "courseId" : "studentId";

        SingleOutputStreamOperator<String> windowedStream = sourceStream
                .keyBy(keySelector)
                .window(TumblingEventTimeWindows.of(WINDOW_SIZE))
                .aggregate(new StudyWatchWindowAggregator(), new RuntimeWindowLogFunction())
                .name("Tumbling10s-Aggregate-" + keyLabel);

        if (RuntimeConfigurator.isolateSinkSlot(options)) {
            windowedStream
                    .addSink(new SlowRuntimeSinkFunction(
                            options.slowSinkMs, RuntimeConfigurator.SLOT_GROUP_HEAVY_SINK))
                    .name("SlowSink-Isolated")
                    .setParallelism(options.parallelism)
                    .slotSharingGroup(RuntimeConfigurator.SLOT_GROUP_HEAVY_SINK);
        } else {
            windowedStream
                    .addSink(new SlowRuntimeSinkFunction(
                            options.slowSinkMs, RuntimeConfigurator.SLOT_GROUP_DEFAULT))
                    .name("SlowSink-" + options.slowSinkMs + "ms");
        }
    }

    private static DataStream<StateDemoEvent> buildKafkaStream(
            StreamExecutionEnvironment env,
            Properties kafkaProps,
            RuntimeConfigurator.RuntimeOptions options) {

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                TOPIC,
                new SimpleStringSchema(),
                kafkaProps
        );
        consumer.setStartFromLatest();

        DataStream<StateDemoEvent> stream = env
                .addSource(consumer)
                .name("Kafka-StudyHeartbeat")
                .map(new JsonToEventMapper())
                .name("JsonParse")
                .filter(new ValidEventFilter())
                .name("ValidFilter")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<StateDemoEvent>forBoundedOutOfOrderness(OUT_OF_ORDERNESS)
                                .withTimestampAssigner((event, ts) -> event.getTs())
                )
                .name("Timestamps/WM")
                .map(new RuntimeSubtaskProbeFunction())
                .name("SubtaskProbe");

        return stream;
    }

    private static void printStartupBanner(RuntimeConfigurator.RuntimeOptions options) {
        RuntimeTopologyEstimator.TopologyEstimate estimate = RuntimeTopologyEstimator.estimate(options);

        System.out.println("========================================");
        System.out.println("Flink Runtime 资源模型演示 Job 已启动");
        System.out.println("Kafka: " + KAFKA_BROKER + " | topic: " + TOPIC);
        System.out.println("场景: " + RuntimeConfigurator.describeScenario(options));
        System.out.println("并行度: " + options.parallelism
                + " | Operator Chain: " + options.chainMode
                + " | 慢 Sink: " + options.slowSinkMs + "ms");
        System.out.println("State Backend: " + StateBackendConfigurator.describeBackend(options.backend));
        if (RuntimeConfigurator.SCENARIO_MANAGED.equals(options.scenario)) {
            System.out.println("Managed Memory: 256m（RocksDB block cache / 排序 / Join 等共享预算）");
        }
        if (options.slowSinkMs >= 200 && RuntimeConfigurator.SCENARIO_BACKPRESSURE.equals(options.scenario)) {
            System.out.println("Network Memory: 32~64mb（收紧以放大反压可见性，生产勿照搬）");
        }
        RuntimeTopologyEstimator.printEstimate(estimate);
        System.out.println("UI: http://localhost:8081");
        System.out.println("  → Job → Task Managers → Slots / Subtasks");
        System.out.println("  → BackPressure（OK / LOW / HIGH）");
        System.out.println("  → Metrics: busyTimeMsPerSecond, backPressuredTimeMsPerSecond, idleTimeMsPerSecond");
        System.out.println("文档: resources/runtime/FlinkRuntimeDemoGuide.md");
        System.out.println("请运行 FlinkRuntimeDemoJobTest 发送测试数据");
        System.out.println("对比实验：");
        System.out.println("  1) backpressure 4 300 chain hashmap     → 反压 + chain");
        System.out.println("  2) backpressure 4 300 nochain hashmap   → 更多 Task，便于对照");
        System.out.println("  3) hotslot 4 300 chain hashmap          → Sink 独立 SlotSharingGroup");
        System.out.println("  4) skew 4 0 chain hashmap               → 热点 key，并行度无效");
        System.out.println("  5) managed 4 200 chain rocksdb          → Managed Memory + RocksDB");
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
