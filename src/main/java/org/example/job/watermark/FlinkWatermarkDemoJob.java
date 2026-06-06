package org.example.job.watermark;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.example.dto.WatermarkDemoEvent;

import java.time.Duration;
import java.util.Properties;

/**
 * Watermark 传播与乱序演示 Job。
 * <p>
 * 功能：
 * <ul>
 *   <li>双 Kafka 源 union，演示下游取最小 WM</li>
 *   <li>forBoundedOutOfOrderness(5s) + 可选 withIdleness 修复空闲分区/空闲源</li>
 *   <li>打印每条事件的 currentWatermark</li>
 *   <li>Tumbling 10s 窗口，观察 WM 推进与触发边界</li>
 * </ul>
 * <p>
 * 启动参数：{@code idlenessSeconds}（默认 0 = 不复现修复，10 = 启用 withIdleness）
 * 或 VM 参数：{@code -Dwatermark.idleness.sec=10}
 * <p>
 * 文档：{@code src/main/resources/watermark/FlinkWatermarkDemoGuide.md}
 */
public class FlinkWatermarkDemoJob {

    public static final String KAFKA_BROKER = "192.168.1.124:9092";
    /** 主 topic，建议 2 分区，用于空闲分区故障复现 */
    public static final String TOPIC_FAST = "test_flink_watermark";
    /** 慢速/沉默源 topic，用于 union 取最小 WM 故障复现 */
    public static final String TOPIC_SLOW = "test_flink_watermark_slow";
    public static final String GROUP_ID = "flink-watermark-demo-consumer";

    public static final Duration OUT_OF_ORDERNESS = Duration.ofSeconds(5);
    public static final Duration DEFAULT_IDLENESS = Duration.ofSeconds(10);
    public static final Time WINDOW_SIZE = Time.seconds(10);

    public static void main(String[] args) throws Exception {
        long idlenessSec = resolveIdlenessSeconds(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(5000);
        // 周期性发射 WM（基于当前 maxEventTime - outOfOrderness），与标点 WM 区别见文档 Step4
        env.getConfig().setAutoWatermarkInterval(1000);

        Properties kafkaPropsFast = new Properties();
        kafkaPropsFast.setProperty("bootstrap.servers", KAFKA_BROKER);
        kafkaPropsFast.setProperty("group.id", GROUP_ID + "-fast");

        Properties kafkaPropsSlow = new Properties();
        kafkaPropsSlow.setProperty("bootstrap.servers", KAFKA_BROKER);
        kafkaPropsSlow.setProperty("group.id", GROUP_ID + "-slow");

        DataStream<WatermarkDemoEvent> fastStream = buildKafkaStream(env, kafkaPropsFast, TOPIC_FAST, "fast", idlenessSec);
        DataStream<WatermarkDemoEvent> slowStream = buildKafkaStream(env, kafkaPropsSlow, TOPIC_SLOW, "slow", idlenessSec);

        // Step1②：多输入 union 后，下游 WM = min(WM_fast, WM_slow)
        DataStream<WatermarkDemoEvent> mergedStream = fastStream.union(slowStream);

        DataStream<WatermarkDemoEvent> monitoredStream = mergedStream
                .process(new WatermarkMonitorFunction())
                .name("WatermarkMonitor");

        monitoredStream
                .keyBy(WatermarkDemoEvent::getUserId)
                .window(TumblingEventTimeWindows.of(WINDOW_SIZE))
                .process(new WatermarkWindowLogFunction())
                .name("Tumbling10s")
                .print("窗口触发");

        printStartupBanner(idlenessSec);

        env.execute("Flink Watermark Demo - Out-of-Order & Idle Source");
    }

    private static DataStream<WatermarkDemoEvent> buildKafkaStream(
            StreamExecutionEnvironment env,
            Properties kafkaProps,
            String topic,
            String sourceLabel,
            long idlenessSec) {

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                topic,
                new SimpleStringSchema(),
                kafkaProps
        );
        consumer.setStartFromLatest();

        WatermarkStrategy<WatermarkDemoEvent> wmStrategy = buildWatermarkStrategy(idlenessSec);

        return env
                .addSource(consumer)
                .name("Kafka-" + sourceLabel)
                .map(new JsonToEventMapper(sourceLabel))
                .filter(new ValidEventFilter())
                .assignTimestampsAndWatermarks(wmStrategy)
                .name("Timestamps/WM-" + sourceLabel);
    }

    static WatermarkStrategy<WatermarkDemoEvent> buildWatermarkStrategy(long idlenessSec) {
        WatermarkStrategy<WatermarkDemoEvent> strategy = WatermarkStrategy
                .<WatermarkDemoEvent>forBoundedOutOfOrderness(OUT_OF_ORDERNESS)
                .withTimestampAssigner((event, ts) -> event.getTs());

        if (idlenessSec > 0) {
            strategy = strategy.withIdleness(Duration.ofSeconds(idlenessSec));
        }
        return strategy;
    }

    static long resolveIdlenessSeconds(String[] args) {
        if (args != null && args.length > 0 && !args[0].isBlank()) {
            return Long.parseLong(args[0]);
        }
        return Long.parseLong(System.getProperty("watermark.idleness.sec", "0"));
    }

    private static void printStartupBanner(long idlenessSec) {
        System.out.println("========================================");
        System.out.println("Flink Watermark 演示 Job 已启动");
        System.out.println("Kafka: " + KAFKA_BROKER);
        System.out.println("Topics: " + TOPIC_FAST + " (建议2分区) + " + TOPIC_SLOW + " (union)");
        System.out.println("乱序容忍: forBoundedOutOfOrderness(5s)");
        System.out.println("withIdleness: " + (idlenessSec > 0 ? idlenessSec + "s ✅ 已启用" : "未启用 ⚠️ 空闲分区/源会卡住 WM"));
        System.out.println("窗口: Tumbling 10s | 并行度: 2");
        System.out.println("文档: resources/watermark/FlinkWatermarkDemoGuide.md");
        System.out.println("请运行 FlinkWatermarkDemoJobTest 发送测试数据");
        System.out.println("========================================");
    }

    private static class JsonToEventMapper implements MapFunction<String, WatermarkDemoEvent> {
        private final ObjectMapper objectMapper = new ObjectMapper();
        private final String defaultSource;

        JsonToEventMapper(String defaultSource) {
            this.defaultSource = defaultSource;
        }

        @Override
        public WatermarkDemoEvent map(String value) {
            try {
                WatermarkDemoEvent event = objectMapper.readValue(value, WatermarkDemoEvent.class);
                if (event.getSource() == null || event.getSource().isEmpty()) {
                    event.setSource(defaultSource);
                }
                return event;
            } catch (Exception e) {
                System.err.println("JSON 解析失败: " + value + ", 错误: " + e.getMessage());
                return null;
            }
        }
    }

    private static class ValidEventFilter implements FilterFunction<WatermarkDemoEvent> {
        @Override
        public boolean filter(WatermarkDemoEvent event) {
            return event != null
                    && event.getUserId() != null
                    && !event.getUserId().isEmpty()
                    && event.getTs() != null
                    && event.getTs() > 0
                    && event.getAmount() != null;
        }
    }
}
