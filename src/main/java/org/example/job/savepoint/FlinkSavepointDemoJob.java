package org.example.job.savepoint;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.example.dto.StateDemoEvent;
import org.example.job.state.StateBackendConfigurator;

import java.time.Duration;
import java.util.Properties;

/**
 * Savepoint vs Checkpoint、版本升级、回滚与状态兼容演示 Job（在线教育学分累计）。
 * <p>
 * 功能：
 * <ul>
 *   <li>显式算子 UID — 发布/恢复必备</li>
 *   <li>{@link CourseCreditAccumulateFunction} MapState 累计课程观看时长</li>
 *   <li>V1 / V2 兼容升级（同 UID + 同状态描述符，V2 增加 promotion 加成逻辑）</li>
 *   <li>可选 {@code broken-uid} 复现 UID 变更导致恢复失败</li>
 *   <li>并行度可调，观察 Keyed State 重分配</li>
 * </ul>
 * <p>
 * 启动参数：{@code version parallelism backend [savepointPath] [allowNonRestored]}
 * <pre>
 *   v1 2 hashmap
 *   v2 4 hashmap file:///tmp/flink-savepoint-demo/savepoints/savepoint-xxx false
 *   broken-uid 2 hashmap
 * </pre>
 * 集群恢复：{@code flink run -s &lt;savepoint&gt; job.jar v2 4 hashmap}
 * <p>
 * 文档：{@code src/main/resources/savepoint/FlinkSavepointDemoGuide.md}
 */
public class FlinkSavepointDemoJob {

    public static final String KAFKA_BROKER = "192.168.1.124:9092";
    public static final String TOPIC = "test_flink_savepoint";
    public static final String GROUP_ID = "flink-savepoint-demo-consumer";

    public static final Duration OUT_OF_ORDERNESS = Duration.ofSeconds(5);

    public static void main(String[] args) throws Exception {
        SavepointConfigurator.SavepointOptions options = SavepointConfigurator.resolveOptions(args);

        Configuration flinkConfig = StateBackendConfigurator.createFlinkConfiguration(options.backend);
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(options.parallelism, flinkConfig);
        env.setParallelism(options.parallelism);
        env.getConfig().setAutoWatermarkInterval(1000);

        SavepointConfigurator.configureEnvironment(env, options);

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKER);
        kafkaProps.setProperty("group.id", GROUP_ID);

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                TOPIC,
                new SimpleStringSchema(),
                kafkaProps
        );
        consumer.setStartFromLatest();

        DataStream<StateDemoEvent> eventStream = env
                .addSource(consumer)
                .uid(SavepointOperatorUids.KAFKA_SOURCE)
                .name("Kafka-SavepointDemo")
                .map(new JsonToEventMapper())
                .uid(SavepointOperatorUids.PARSE_FILTER)
                .name("Parse/Filter")
                .filter(new ValidEventFilter())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<StateDemoEvent>forBoundedOutOfOrderness(OUT_OF_ORDERNESS)
                                .withTimestampAssigner((event, ts) -> event.getTs())
                )
                .name("Timestamps/WM");

        String accumulatorUid = SavepointConfigurator.resolveAccumulatorUid(options);
        CourseCreditAccumulateFunction.JobVersion jobVersion = SavepointConfigurator.toJobVersion(options);

        SingleOutputStreamOperator<String> creditStream = eventStream
                .keyBy(StateDemoEvent::getStudentId)
                .process(new CourseCreditAccumulateFunction(jobVersion))
                .uid(accumulatorUid)
                .name("MapState-CourseCredit-" + options.version.toUpperCase());

        creditStream.print("学分累计");

        printStartupBanner(options, accumulatorUid);
        if (options.savepointPath != null) {
            printSavepointRestoreHint(options);
        } else {
            SavepointCliCommands.printReleasePlaybook("<JOB_ID>");
        }

        env.execute("Flink Savepoint Demo - Release & State Compatibility [" + options.version + "]");
    }

    private static void printSavepointRestoreHint(SavepointConfigurator.SavepointOptions options) {
        System.out.println("--- Savepoint 恢复参数（集群请用 flink run -s）---");
        System.out.println("Savepoint 路径: " + options.savepointPath);
        System.out.println("allowNonRestoredState: " + options.allowNonRestoredState);
        System.out.println("推荐命令: " + SavepointCliCommands.runFromSavepoint(
                "target/flink_code-1.0-SNAPSHOT.jar",
                options.savepointPath,
                options.allowNonRestoredState));
        System.out.println("本地 createLocalEnvironment 不自动挂载 Savepoint；请在 Flink 集群执行上述命令");
        System.out.println("-----------------------------------------------");
    }

    private static void printStartupBanner(SavepointConfigurator.SavepointOptions options, String accumulatorUid) {
        System.out.println("========================================");
        System.out.println("Flink Savepoint / 发布恢复 演示 Job 已启动");
        System.out.println("Kafka: " + KAFKA_BROKER + " / topic: " + TOPIC);
        System.out.println("Job 版本: " + SavepointConfigurator.describeVersion(options.version));
        System.out.println("并行度: " + options.parallelism
                + (options.brokenUid ? " | ⚠️ broken-uid 模式（累计算子 UID 与线上一致版本不同）" : ""));
        System.out.println("累计算子 UID: " + accumulatorUid);
        System.out.println("State Backend: " + StateBackendConfigurator.describeBackend(options.backend));
        System.out.println("Checkpoint: 每 " + SavepointConfigurator.CHECKPOINT_INTERVAL_MS
                + "ms 自动触发（故障恢复）；发布请用 Savepoint");
        System.out.println("Savepoint 目录: " + SavepointConfigurator.SAVEPOINT_BASE_DIR);
        System.out.println("文档: resources/savepoint/FlinkSavepointDemoGuide.md");
        System.out.println("请运行 FlinkSavepointDemoJobTest 发送测试数据");
        System.out.println("对比实验：");
        System.out.println("  1) v1 2 hashmap          → 基线积累状态");
        System.out.println("  2) stop --savepointPath  → 手动 Savepoint 后停作业");
        System.out.println("  3) v2 4 hashmap -s SP    → 兼容升级 + 扩并行度");
        System.out.println("  4) broken-uid 2 hashmap  → 复现 UID 变更恢复失败");
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
                    && event.getEventId() != null
                    && !event.getEventId().isEmpty()
                    && event.getStudentId() != null
                    && !event.getStudentId().isEmpty()
                    && event.getEventType() != null
                    && event.getTs() != null
                    && event.getTs() > 0;
        }
    }
}
