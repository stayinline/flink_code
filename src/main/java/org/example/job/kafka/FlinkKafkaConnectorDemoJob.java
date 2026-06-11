package org.example.job.kafka;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.dto.StateDemoEvent;
import org.example.dto.StudySummaryRecord;
import org.example.job.exactlyonce.StudyProgressAggregateFunction;
import org.example.job.state.StateBackendConfigurator;

import java.time.Duration;

/**
 * Kafka 连接器工程化演示：Source → 有状态处理 → KafkaSink。
 * <p>
 * 覆盖：分区规划、消费者组、Offset 初始化/Checkpoint、事务写、并行度与分区、积压追赶。
 * <p>
 * Flink 1.14 实现：{@link org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer} /
 * {@link org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer}；
 * 指南含 Flink 1.15+ {@code KafkaSource}/{@code KafkaSink} 等价写法。
 * <p>
 * 启动参数：{@code scenario offsetMode sinkSemantic parallelism backend}
 * <pre>
 *   normal latest at-least-once 4 hashmap
 *   recovery committed exactly-once 2 hashmap
 *   partition-mismatch latest at-least-once 8 hashmap
 *   group-switch earliest at-least-once 4 hashmap
 *   backlog latest exactly-once 4 rocksdb
 * </pre>
 * 文档：{@code src/main/resources/kafka/FlinkKafkaConnectorDemoGuide.md}
 */
public class FlinkKafkaConnectorDemoJob {

    public static final String KAFKA_BROKER = "192.168.1.124:9092";
    public static final String TOPIC_IN = "test_flink_kafka_in";
    public static final String TOPIC_OUT = "test_flink_kafka_out";

    public static final Duration OUT_OF_ORDERNESS = Duration.ofSeconds(5);

    public static void main(String[] args) throws Exception {
        KafkaConnectorConfigurator.KafkaConnectorOptions options =
                KafkaConnectorConfigurator.resolveOptions(args);

        Configuration flinkConfig = StateBackendConfigurator.createFlinkConfiguration(options.backend);
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(options.parallelism, flinkConfig);
        env.setParallelism(options.parallelism);
        env.getConfig().setAutoWatermarkInterval(1000);

        KafkaConnectorConfigurator.configureEnvironment(env, options);

        String groupId = KafkaConnectorConfigurator.resolveGroupId(options);

        DataStream<StateDemoEvent> eventStream = env
                .addSource(StudyKafkaSourceFactory.build(KAFKA_BROKER, TOPIC_IN, groupId, options))
                .name("KafkaSource-StudyHeartbeat")
                .map(new KafkaSourceOffsetProbeFunction())
                .name("OffsetProbe")
                .filter(new ValidEventFilter())
                .name("ValidFilter")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<StateDemoEvent>forBoundedOutOfOrderness(OUT_OF_ORDERNESS)
                                .withTimestampAssigner((event, ts) -> event.getTs())
                )
                .name("Timestamps/WM");

        DataStream<StudySummaryRecord> summaryStream = eventStream
                .keyBy(StateDemoEvent::getStudentId)
                .process(new StudyProgressAggregateFunction())
                .name("StudyProgress-Aggregate");

        DataStream<String> jsonStream = summaryStream
                .map(new SummaryToJsonMapper())
                .name("SummaryToJson")
                .disableChaining();

        jsonStream
                .addSink(StudyKafkaSinkFactory.build(KAFKA_BROKER, TOPIC_OUT, options))
                .name("KafkaSink-" + options.sinkSemantic);

        jsonStream.print("Kafka写出预览");

        printStartupBanner(options, groupId);
        env.execute("Flink Kafka Connector Demo - [" + options.scenario + "]");
    }

    private static void printStartupBanner(
            KafkaConnectorConfigurator.KafkaConnectorOptions options,
            String groupId) {
        int idle = KafkaPartitionAssignSimulator.idleSubtaskCount(4, options.parallelism);

        System.out.println("========================================");
        System.out.println("Flink Kafka 连接器工程化 Demo 已启动");
        System.out.println("Broker: " + KAFKA_BROKER);
        System.out.println("IN:  " + TOPIC_IN + " (建议 4 分区) | OUT: " + TOPIC_OUT);
        System.out.println("消费者组: " + groupId);
        System.out.println("场景: " + KafkaConnectorConfigurator.describeScenario(options));
        System.out.println("Offset 初始化: " + KafkaConnectorConfigurator.describeOffsetMode(options.offsetMode));
        System.out.println("Sink 语义: " + options.sinkSemantic
                + (KafkaConnectorConfigurator.SINK_EXACTLY_ONCE.equals(options.sinkSemantic)
                ? " | transactional.id 前缀由 Flink 管理，CK 间隔 < transaction.timeout.ms" : ""));
        System.out.println("并行度: " + options.parallelism
                + " | 若 topic 4 分区，估算空闲 Source Subtask: " + idle);
        System.out.println("State Backend: " + StateBackendConfigurator.describeBackend(options.backend));
        System.out.println("恢复优先级: Checkpoint/Savepoint Source 状态 > Kafka committed offset");
        System.out.println("Lag 排查: kafka-consumer-groups.sh --describe + Flink UI BackPressure");
        System.out.println("文档: resources/kafka/FlinkKafkaConnectorDemoGuide.md");
        System.out.println("请运行 FlinkKafkaConnectorDemoJobTest 发送测试数据");
        System.out.println("对比实验：");
        System.out.println("  1) normal latest at-least-once 4 hashmap");
        System.out.println("  2) recovery committed exactly-once 2 hashmap");
        System.out.println("  3) partition-mismatch latest at-least-once 8 hashmap");
        System.out.println("  4) group-switch earliest at-least-once 4 hashmap");
        System.out.println("  5) backlog latest exactly-once 4 rocksdb");
        System.out.println("========================================");
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
