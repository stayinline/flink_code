package org.example.job.quality;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.example.dto.DirtyDataRecord;
import org.example.dto.StateDemoEvent;
import org.example.dto.StudySummaryRecord;
import org.example.job.state.StateBackendConfigurator;

import java.time.Duration;
import java.util.Properties;

/**
 * 数据质量与补偿重跑演示：校验 → 主流计算 / 脏数据侧输出 → DLQ → 幂等汇总表。
 * <p>
 * 启动参数：{@code mode backend} 例：{@code strict hashmap} / {@code drop hashmap} / {@code replay hashmap}
 * <p>
 * 文档：{@code src/main/resources/quality/FlinkDataQualityDemoGuide.md}
 */
public class FlinkDataQualityDemoJob {

    public static final String KAFKA_BROKER = "192.168.1.124:9092";
    public static final String TOPIC_IN = "test_flink_quality";
    /** 可选：生产 DLQ 写入独立 topic */
    public static final String TOPIC_DLQ = "test_flink_quality_dlq";

    public static final Duration OUT_OF_ORDERNESS = Duration.ofSeconds(5);

    public static void main(String[] args) throws Exception {
        DataQualityConfigurator.QualityOptions options = DataQualityConfigurator.resolveOptions(args);

        Configuration flinkConfig = StateBackendConfigurator.createFlinkConfiguration(options.backend);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2, flinkConfig);
        env.setParallelism(2);
        env.getConfig().setAutoWatermarkInterval(1000);

        DataQualityConfigurator.configureEnvironment(env, options);

        String groupId = DataQualityConfigurator.resolveGroupId(options);

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKER);
        kafkaProps.setProperty("group.id", groupId);

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                TOPIC_IN,
                new SimpleStringSchema(),
                kafkaProps
        );
        consumer.setStartFromLatest();

        SingleOutputStreamOperator<StateDemoEvent> validatedStream = env
                .addSource(consumer)
                .name("Kafka-QualityIn")
                .process(new DataQualityIngressFunction(DataQualityConfigurator.dlqEnabled(options)))
                .name("DataQuality-Ingress");

        if (DataQualityConfigurator.dlqEnabled(options)) {
            validatedStream.getSideOutput(DataQualityIngressFunction.DIRTY_DATA_TAG)
                    .addSink(new DlqSinkFunction())
                    .name("DLQ-Sink");
        }

        DataStream<StudySummaryRecord> summaryStream = validatedStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<StateDemoEvent>forBoundedOutOfOrderness(OUT_OF_ORDERNESS)
                                .withTimestampAssigner((event, ts) -> event.getTs())
                )
                .name("Timestamps/WM")
                .keyBy(StateDemoEvent::getStudentId)
                .process(new QualityAwareAggregateFunction())
                .name("Quality-Aggregate");

        summaryStream
                .addSink(new IdempotentSummarySinkFunction())
                .name("Idempotent-Summary-Sink");

        summaryStream.print("汇总预览");

        printStartupBanner(options, groupId);
        env.execute("Flink Data Quality Demo - [" + options.mode + "]");
    }

    private static void printStartupBanner(DataQualityConfigurator.QualityOptions options, String groupId) {
        System.out.println("========================================");
        System.out.println("Flink 数据质量与补偿重跑 Demo 已启动");
        System.out.println("Kafka: " + KAFKA_BROKER + " | IN: " + TOPIC_IN + " | DLQ topic(可选): " + TOPIC_DLQ);
        System.out.println("消费者组: " + groupId);
        System.out.println("模式: " + DataQualityConfigurator.describeMode(options));
        System.out.println("链路: Source → 校验 → [主流聚合→幂等表] + [侧输出→DLQ]");
        System.out.println("State Backend: " + StateBackendConfigurator.describeBackend(options.backend));
        System.out.println("Exactly-Once（CK）保证状态一致；业务最终一致靠 DLQ + 幂等写 + 对账");
        System.out.println("文档: resources/quality/FlinkDataQualityDemoGuide.md");
        System.out.println("请运行 FlinkDataQualityDemoJobTest 发送测试数据");
        System.out.println("对比实验：");
        System.out.println("  1) strict hashmap  — 脏数据进 DLQ，主流幂等写");
        System.out.println("  2) drop hashmap    — 脏数据丢弃（对照）");
        System.out.println("  3) replay hashmap  — 修复数据 replay-v2 回放");
        System.out.println("========================================");
    }
}
