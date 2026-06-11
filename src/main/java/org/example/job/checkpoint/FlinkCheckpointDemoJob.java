package org.example.job.checkpoint;

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
import org.example.job.state.StateBackendConfigurator;

import java.time.Duration;
import java.util.Properties;

/**
 * Checkpoint 机制与超时排查演示 Job（在线教育学习时长聚合）。
 * <p>
 * 功能：
 * <ul>
 *   <li>双 Kafka 源 union → 演示 Aligned barrier 多上游对齐</li>
 *   <li>慢支路 {@link SlowBranchMapFunction} 模拟反压 → alignment time 升高</li>
 *   <li>{@link StudyDurationCheckpointFunction} MapState + CheckpointListener 日志</li>
 *   <li>可选 Unaligned Checkpoint（启动参数 {@code unaligned}）</li>
 *   <li>可选慢 Sink 模拟 2PC pre-commit 慢</li>
 * </ul>
 * <p>
 * 启动参数：{@code mode slowBranchMs sinkSlowMs backend}
 * 例：{@code aligned 300 0 hashmap} 或 {@code unaligned 300 100 rocksdb}
 * VM：{@code -Dcheckpoint.mode=unaligned -Dcheckpoint.slow.branch.ms=300}
 * <p>
 * 文档：{@code src/main/resources/checkpoint/FlinkCheckpointDemoGuide.md}
 */
public class FlinkCheckpointDemoJob {

    public static final String KAFKA_BROKER = "192.168.1.124:9092";
    /** 快支路：App 学习心跳 */
    public static final String TOPIC_FAST = "test_flink_checkpoint";
    /** 慢支路：教务 LMS 回流（模拟处理慢 / 反压） */
    public static final String TOPIC_SLOW = "test_flink_checkpoint_slow";
    public static final String GROUP_ID = "flink-checkpoint-demo-consumer";

    public static final Duration OUT_OF_ORDERNESS = Duration.ofSeconds(5);

    public static void main(String[] args) throws Exception {
        CheckpointConfigurator.CheckpointOptions options = CheckpointConfigurator.resolveOptions(args);

        Configuration flinkConfig = StateBackendConfigurator.createFlinkConfiguration(options.backend);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2, flinkConfig);
        env.setParallelism(2);
        env.getConfig().setAutoWatermarkInterval(1000);

        CheckpointConfigurator.configure(env, options);

        Properties kafkaPropsFast = new Properties();
        kafkaPropsFast.setProperty("bootstrap.servers", KAFKA_BROKER);
        kafkaPropsFast.setProperty("group.id", GROUP_ID + "-fast");

        Properties kafkaPropsSlow = new Properties();
        kafkaPropsSlow.setProperty("bootstrap.servers", KAFKA_BROKER);
        kafkaPropsSlow.setProperty("group.id", GROUP_ID + "-slow");

        DataStream<StateDemoEvent> fastStream = buildKafkaStream(env, kafkaPropsFast, TOPIC_FAST, "fast");
        DataStream<StateDemoEvent> slowStream = buildKafkaStream(env, kafkaPropsSlow, TOPIC_SLOW, "slow")
                .map(new SlowBranchMapFunction(options.slowBranchMs))
                .name("SlowBranch-" + options.slowBranchMs + "ms");

        // union：Aligned CK 时 barrier 需等 fast + slow 两路齐 → 慢支路拖长 alignment time
        DataStream<StateDemoEvent> mergedStream = fastStream.union(slowStream);

        mergedStream
                .keyBy(StateDemoEvent::getStudentId)
                .process(new StudyDurationCheckpointFunction())
                .name("MapState-StudyDuration")
                .map(new SlowSinkMapFunction(options.sinkSlowMs))
                .name("SlowSink-" + options.sinkSlowMs + "ms")
                .print("CK演示");

        printStartupBanner(options);
        env.execute("Flink Checkpoint Demo - Barrier Alignment & Timeout");
    }

    private static DataStream<StateDemoEvent> buildKafkaStream(
            StreamExecutionEnvironment env,
            Properties kafkaProps,
            String topic,
            String sourceLabel) {

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                topic,
                new SimpleStringSchema(),
                kafkaProps
        );
        consumer.setStartFromLatest();

        return env
                .addSource(consumer)
                .name("Kafka-" + sourceLabel)
                .map(new JsonToEventMapper(sourceLabel))
                .filter(new ValidEventFilter())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<StateDemoEvent>forBoundedOutOfOrderness(OUT_OF_ORDERNESS)
                                .withTimestampAssigner((event, ts) -> event.getTs())
                )
                .name("Timestamps/WM-" + sourceLabel);
    }

    private static void printStartupBanner(CheckpointConfigurator.CheckpointOptions options) {
        System.out.println("========================================");
        System.out.println("Flink Checkpoint 演示 Job 已启动");
        System.out.println("Kafka: " + KAFKA_BROKER);
        System.out.println("Topics: " + TOPIC_FAST + " + " + TOPIC_SLOW + " (union → barrier 对齐)");
        System.out.println("Checkpoint 模式: " + CheckpointConfigurator.describeMode(options.mode));
        System.out.println("间隔/超时/对齐超时: "
                + CheckpointConfigurator.DEFAULT_CHECKPOINT_INTERVAL_MS + "ms / "
                + CheckpointConfigurator.DEFAULT_CHECKPOINT_TIMEOUT_MS + "ms / "
                + CheckpointConfigurator.DEFAULT_ALIGNMENT_TIMEOUT_MS + "ms");
        System.out.println("State Backend: " + StateBackendConfigurator.describeBackend(options.backend));
        System.out.println("慢支路 sleep: " + options.slowBranchMs + "ms | 慢 Sink sleep: " + options.sinkSlowMs + "ms");
        System.out.println("UI: http://localhost:8081 → Job → Checkpoints（看 alignment / sync / async / state size）");
        System.out.println("文档: resources/checkpoint/FlinkCheckpointDemoGuide.md");
        System.out.println("请运行 FlinkCheckpointDemoJobTest 发送测试数据");
        System.out.println("对比实验：");
        System.out.println("  1) aligned 300 0     → 慢支路拖长 alignment time，易接近超时");
        System.out.println("  2) unaligned 300 0   → barrier 不等齐，CK 更易成功");
        System.out.println("========================================");
    }

    private static class JsonToEventMapper implements MapFunction<String, StateDemoEvent> {
        private final ObjectMapper objectMapper = new ObjectMapper();
        private final String defaultSource;

        JsonToEventMapper(String defaultSource) {
            this.defaultSource = defaultSource;
        }

        @Override
        public StateDemoEvent map(String value) {
            try {
                StateDemoEvent event = objectMapper.readValue(value, StateDemoEvent.class);
                if (event.getTag() == null || event.getTag().isEmpty()) {
                    event.setTag(defaultSource);
                }
                return event;
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
                    && event.getTs() != null
                    && event.getTs() > 0
                    && event.getEventType() != null;
        }
    }
}
