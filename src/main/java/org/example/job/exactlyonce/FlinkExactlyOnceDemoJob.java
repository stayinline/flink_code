package org.example.job.exactlyonce;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.example.dto.StateDemoEvent;
import org.example.dto.StudySummaryRecord;
import org.example.job.state.StateBackendConfigurator;

import java.time.Duration;
import java.util.Properties;

/**
 * 端到端 Exactly-Once 与 2PC Sink 演示 Job（在线教育学习时长 → 汇总写出）。
 * <p>
 * 三段保证：
 * <ol>
 *   <li>Source 可重放：Kafka offset 存入 Checkpoint</li>
 *   <li>Flink 内部 Exactly-Once：barrier + 算子状态快照</li>
 *   <li>Sink 2PC：{@link DemoTwoPhaseCommitSink} 或幂等路线 {@link DemoIdempotentClickHouseSink}</li>
 * </ol>
 * <p>
 * 启动参数：{@code sinkMode commitSlowMs backend}
 * 例：{@code 2pc 0 hashmap} 或 {@code idempotent 0 rocksdb}
 * VM：{@code -Dexactlyonce.sink=2pc -Dexactlyonce.commit.slow.ms=200}
 * <p>
 * 文档：{@code src/main/resources/exactlyonce/FlinkExactlyOnceDemoGuide.md}
 */
public class FlinkExactlyOnceDemoJob {

    public static final String KAFKA_BROKER = "192.168.1.124:9092";
    public static final String TOPIC = "test_flink_exactlyonce";
    public static final String GROUP_ID = "flink-exactlyonce-demo-consumer";

    public static final Duration OUT_OF_ORDERNESS = Duration.ofSeconds(5);

    public static void main(String[] args) throws Exception {
        ExactlyOnceConfigurator.ExactlyOnceOptions options = ExactlyOnceConfigurator.resolveOptions(args);

        Configuration flinkConfig = StateBackendConfigurator.createFlinkConfiguration(options.backend);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2, flinkConfig);
        env.setParallelism(2);
        env.getConfig().setAutoWatermarkInterval(1000);

        ExactlyOnceConfigurator.configure(env, options);

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKER);
        kafkaProps.setProperty("group.id", GROUP_ID);

        DataStream<StateDemoEvent> sourceStream = buildKafkaStream(env, kafkaProps);

        DataStream<StudySummaryRecord> summaryStream = sourceStream
                .keyBy(StateDemoEvent::getStudentId)
                .process(new StudyProgressAggregateFunction())
                .name("StudyProgress-Aggregate");

        attachSink(summaryStream, options);

        printStartupBanner(options);
        env.execute("Flink Exactly-Once Demo - E2E 2PC Sink");
    }

    private static void attachSink(DataStream<StudySummaryRecord> stream,
                                   ExactlyOnceConfigurator.ExactlyOnceOptions options) {
        if (ExactlyOnceConfigurator.SINK_IDEMPOTENT.equalsIgnoreCase(options.sinkMode)) {
            stream
                    .addSink(new DemoIdempotentClickHouseSink())
                    .name("Idempotent-ClickHouse-Sink");
        } else {
            stream
                    .addSink(new DemoTwoPhaseCommitSink(options.commitSlowMs))
                    .name("TwoPhaseCommit-Sink");
        }
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
                .name("Kafka-StudyProgress")
                .map(new JsonToEventMapper())
                .filter(new ValidEventFilter())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<StateDemoEvent>forBoundedOutOfOrderness(OUT_OF_ORDERNESS)
                                .withTimestampAssigner((event, ts) -> event.getTs())
                )
                .name("Timestamps/WM");
    }

    private static void printStartupBanner(ExactlyOnceConfigurator.ExactlyOnceOptions options) {
        System.out.println("========================================");
        System.out.println("Flink 端到端 Exactly-Once 演示 Job 已启动");
        System.out.println("Kafka: " + KAFKA_BROKER + " | topic: " + TOPIC);
        System.out.println("Checkpoint: EXACTLY_ONCE 间隔 "
                + ExactlyOnceConfigurator.DEFAULT_CHECKPOINT_INTERVAL_MS + "ms");
        System.out.println("State Backend: " + StateBackendConfigurator.describeBackend(options.backend));
        System.out.println("Sink 模式: " + ExactlyOnceConfigurator.describeSinkMode(options.sinkMode));
        System.out.println("2PC commit 慢模拟: " + options.commitSlowMs + "ms");
        System.out.println("三段保证：① Kafka offset@CK ② barrier+状态快照 ③ Sink 2PC/幂等");
        System.out.println("文档: resources/exactlyonce/FlinkExactlyOnceDemoGuide.md");
        System.out.println("请运行 FlinkExactlyOnceDemoJobTest 发送测试数据");
        System.out.println("对比实验：");
        System.out.println("  1) 2pc 0 hashmap      → 观察 [2PC-BEGIN/PRE-COMMIT/COMMIT/ABORT]");
        System.out.println("  2) idempotent 0       → 观察 [CK-UPSERT] 幂等覆盖");
        System.out.println("  3) 2pc 200 hashmap    → commit 慢，易接近 CK 超时");
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
