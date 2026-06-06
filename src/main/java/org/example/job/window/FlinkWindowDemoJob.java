package org.example.job.window;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.example.dto.UserOrderEvent;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

/**
 * Flink 三种窗口对比 Job —— 对齐 Step2 手写要求。
 * <p>
 * 数据源：(userId, ts, amount)，同一份数据分别做 sum(amount)：
 * <ul>
 *   <li>Tumbling 5s</li>
 *   <li>Sliding size=10s / slide=5s（观察一条数据进入几个窗口）</li>
 *   <li>Session gap=5s（含迟到数据 merge）</li>
 * </ul>
 * 聚合方式：{@link AmountSumAggregator} 增量 aggregate + {@link WindowSumResultFormatter} 仅格式化。
 * <p>
 * 原理 / 场景映射 / 陷阱 / 面试话术见 {@code src/main/resources/window/FlinkWindowDemoGuide.md}
 * <p>
 * 运行：先启动本 Job，再运行 {@code FlinkWindowDemoJobTest}。
 */
public class FlinkWindowDemoJob {

    public static final String KAFKA_BROKER = "192.168.1.124:9092";
    public static final String TOPIC_NAME = "test_flink_window";
    public static final String GROUP_ID = "flink-window-demo-consumer";

    /** Step2：Tumbling 5s */
    public static final Time TUMBLING_SIZE = Time.seconds(5);
    /** Step2：Sliding size=10s / slide=5s */
    public static final Time SLIDING_SIZE = Time.seconds(10);
    public static final Time SLIDE_INTERVAL = Time.seconds(5);
    /** Step2：Session gap=5s */
    public static final Time SESSION_GAP = Time.seconds(5);
    /** 允许 5s 乱序，以便演示 Session merge 的迟到数据 */
    public static final Duration MAX_OUT_OF_ORDERNESS = Duration.ofSeconds(5);

    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter
            .ofPattern("HH:mm:ss.SSS")
            .withZone(ZoneId.systemDefault());

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKER);
        kafkaProps.setProperty("group.id", GROUP_ID);

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                TOPIC_NAME,
                new SimpleStringSchema(),
                kafkaProps
        );
        kafkaConsumer.setStartFromLatest();

        DataStream<UserOrderEvent> eventStream = env
                .addSource(kafkaConsumer)
                .map(new JsonToEventMapper())
                .filter(new ValidEventFilter())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<UserOrderEvent>forBoundedOutOfOrderness(MAX_OUT_OF_ORDERNESS)
                                .withTimestampAssigner((event, ts) -> event.getTs())
                );

        eventStream
                .map(e -> String.format("[RAW] userId=%s ts=%s amount=%.1f",
                        e.getUserId(),
                        TIME_FMT.format(Instant.ofEpochMilli(e.getTs())),
                        e.getAmount()))
                .print("原始事件");

        KeyedStream<UserOrderEvent, String> keyedStream = eventStream.keyBy(UserOrderEvent::getUserId);

        // Step2 + Step4③：aggregate 增量 sum，ProcessWindowFunction 只负责输出
        keyedStream
                .window(TumblingEventTimeWindows.of(TUMBLING_SIZE))
                .aggregate(new AmountSumAggregator(), new WindowSumResultFormatter(
                        "TUMBLING",
                        "5s 滚动 | 窗口左闭右开 [start,end) | 每条数据仅归属 1 个窗口"))
                .print("Tumbling-5s");

        keyedStream
                .window(SlidingEventTimeWindows.of(SLIDING_SIZE, SLIDE_INTERVAL))
                .aggregate(new AmountSumAggregator(), new WindowSumResultFormatter(
                        "SLIDING",
                        "size=10s slide=5s | ts=+7s 的单条数据应输出 2 条（[0,10) 与 [5,15)）"))
                .print("Sliding-10s-5s");

        keyedStream
                .window(EventTimeSessionWindows.withGap(SESSION_GAP))
                .aggregate(new AmountSumAggregator(), new WindowSumResultFormatter(
                        "SESSION",
                        "gap=5s | u003 迟到 ts=+6s 应 merge 两段会话为 1 条"))
                .print("Session-gap-5s");

        printStartupBanner();

        env.execute("Flink Window Demo - Tumbling5s / Sliding10s-5s / Session5s");
    }

    private static void printStartupBanner() {
        System.out.println("========================================");
        System.out.println("Flink 窗口对比 Job 已启动");
        System.out.println("Kafka: " + KAFKA_BROKER + " / topic: " + TOPIC_NAME);
        System.out.println("窗口参数: Tumbling=5s | Sliding=10s/5s | Session gap=5s");
        System.out.println("Watermark: forBoundedOutOfOrderness(5s)");
        System.out.println("详细原理与验收对照见 resources/window/FlinkWindowDemoGuide.md");
        System.out.println("请运行 FlinkWindowDemoJobTest 发送分场景测试数据");
        System.out.println("========================================");
    }

    private static class JsonToEventMapper implements MapFunction<String, UserOrderEvent> {
        private final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public UserOrderEvent map(String value) {
            try {
                return objectMapper.readValue(value, UserOrderEvent.class);
            } catch (Exception e) {
                System.err.println("JSON 解析失败: " + value + ", 错误: " + e.getMessage());
                return null;
            }
        }
    }

    private static class ValidEventFilter implements FilterFunction<UserOrderEvent> {
        @Override
        public boolean filter(UserOrderEvent event) {
            return event != null
                    && event.getUserId() != null
                    && !event.getUserId().isEmpty()
                    && event.getAmount() != null
                    && event.getTs() != null
                    && event.getTs() > 0;
        }
    }
}
