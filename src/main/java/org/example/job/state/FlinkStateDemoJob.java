package org.example.job.state;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.example.dto.StateDemoEvent;

import java.time.Duration;
import java.util.Properties;

/**
 * State 类型与 State Backend 演示 Job（在线教育场景）。
 * <p>
 * 三条算子链：
 * <ol>
 *   <li>{@link DeduplicateFunction} — ValueState 去重标记（移动端重试）</li>
 *   <li>{@link CourseProgressMapFunction} — MapState 按 courseId 累计观看时长</li>
 *   <li>{@link PendingQuizBufferFunction} — ListState 缓存「答案先于题目」的待处理事件</li>
 * </ol>
 * <p>
 * Operator State：{@link FlinkKafkaConsumer} 内置 Kafka offset 状态（本 Job 未手写，由 connector 管理）。
 * <p>
 * 启动参数：{@code hashmap}（默认）或 {@code rocksdb}
 * 或 VM 参数：{@code -Dstate.backend=rocksdb}
 * <p>
 * 文档：{@code src/main/resources/state/FlinkStateDemoGuide.md}
 */
public class FlinkStateDemoJob {

    public static final String KAFKA_BROKER = "192.168.1.124:9092";
    public static final String TOPIC = "test_flink_state";
    public static final String GROUP_ID = "flink-state-demo-consumer";

    public static final Duration OUT_OF_ORDERNESS = Duration.ofSeconds(5);

    public static void main(String[] args) throws Exception {
        String backend = StateBackendConfigurator.resolveBackend(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);
        env.getConfig().setAutoWatermarkInterval(200);

        StateBackendConfigurator.configure(env, backend);

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKER);
        kafkaProps.setProperty("group.id", GROUP_ID);

        // Operator State 示例：FlinkKafkaConsumer 在 Checkpoint 时持久化各分区 offset
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                TOPIC,
                new SimpleStringSchema(),
                kafkaProps
        );
        consumer.setStartFromLatest();

        DataStream<StateDemoEvent> eventStream = env
                .addSource(consumer)
                .name("Kafka-StateDemo")
                .map(new JsonToEventMapper())
                .filter(new ValidEventFilter())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<StateDemoEvent>forBoundedOutOfOrderness(OUT_OF_ORDERNESS)
                                .withTimestampAssigner((event, ts) -> event.getTs())
                )
                .name("Timestamps/WM");

        // Step2① ValueState：keyBy(eventId) 去重
        SingleOutputStreamOperator<StateDemoEvent> dedupedStream = eventStream
                .keyBy(StateDemoEvent::getEventId)
                .process(new DeduplicateFunction())
                .name("ValueState-Dedup");

        // Step2② MapState：keyBy(studentId) 按 courseId 聚合观看时长
        SingleOutputStreamOperator<String> mapAggStream = dedupedStream
                .keyBy(StateDemoEvent::getStudentId)
                .process(new CourseProgressMapFunction())
                .name("MapState-CourseProgress");

        // Step2③ ListState：同 studentId 缓存乱序答题
        dedupedStream
                .keyBy(StateDemoEvent::getStudentId)
                .process(new PendingQuizBufferFunction())
                .name("ListState-PendingQuiz")
                .print("测验缓冲");

        mapAggStream.print("课程进度");

        printStartupBanner(backend);
        env.execute("Flink State Demo - ValueState/MapState/ListState & Backend");
    }

    private static void printStartupBanner(String backend) {
        System.out.println("========================================");
        System.out.println("Flink State 类型与后端 演示 Job 已启动");
        System.out.println("Kafka: " + KAFKA_BROKER + " / topic: " + TOPIC);
        System.out.println("State Backend: " + StateBackendConfigurator.describeBackend(backend));
        System.out.println("算子链: ValueState(去重) → MapState(课程聚合) + ListState(答题缓冲)");
        System.out.println("Operator State: Kafka offset 由 FlinkKafkaConsumer 自动管理");
        System.out.println("切换后端: FlinkStateDemoJob rocksdb  或  -Dstate.backend=rocksdb");
        System.out.println("文档: resources/state/FlinkStateDemoGuide.md");
        System.out.println("请运行 FlinkStateDemoJobTest 发送测试数据");
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
                    && !event.getEventType().isEmpty()
                    && event.getTs() != null
                    && event.getTs() > 0;
        }
    }
}
