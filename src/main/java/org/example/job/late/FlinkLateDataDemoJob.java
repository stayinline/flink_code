package org.example.job.late;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.OutputTag;
import org.example.dto.WatermarkDemoEvent;

import java.time.Duration;
import java.util.Properties;

/**
 * 迟到数据三道防线演示 Job。
 * <p>
 * 三层防线：
 * <ol>
 *   <li>{@code forBoundedOutOfOrderness(5s)} — WM 乱序缓冲，轻度乱序仍进窗口</li>
 *   <li>{@code allowedLateness(3s)} — 窗口首次触发后保留状态，迟到数据触发同窗口增量重算</li>
 *   <li>{@code sideOutputLateData(tag)} — 彻底迟到进侧流，补偿落库不丢数</li>
 * </ol>
 * <p>
 * 启动参数：
 * <ul>
 *   <li>无参 / {@code billing} — 计费模式：开侧输出 + 补偿落库（默认）</li>
 *   <li>{@code pv} — 大屏 PV 模式：不开侧输出，严重迟到静默丢弃</li>
 * </ul>
 * <p>
 * 文档：{@code src/main/resources/watermark/FlinkLateDataDemoGuide.md}
 */
public class FlinkLateDataDemoJob {

    public static final String KAFKA_BROKER = "192.168.1.124:9092";
    public static final String TOPIC = "test_flink_late_data";
    public static final String GROUP_ID = "flink-late-data-demo-consumer";

    /** 第一道防线：WM 乱序容忍 */
    public static final Duration OUT_OF_ORDERNESS = Duration.ofSeconds(5);
    /** 第二道防线：窗口触发后再保留状态时长 */
    public static final Duration ALLOWED_LATENESS = Duration.ofSeconds(3);
    public static final Time WINDOW_SIZE = Time.seconds(10);

    /** 第三道防线：彻底迟到的侧输出标签 */
    public static final OutputTag<WatermarkDemoEvent> LATE_DATA_TAG =
            new OutputTag<WatermarkDemoEvent>("late-data") {};

    public static final String MODE_BILLING = "billing";
    public static final String MODE_PV = "pv";

    public static void main(String[] args) throws Exception {
        String mode = resolveMode(args);
        boolean sideOutputEnabled = MODE_BILLING.equals(mode);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);
        env.getConfig().setAutoWatermarkInterval(200);

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKER);
        kafkaProps.setProperty("group.id", GROUP_ID);

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                TOPIC,
                new SimpleStringSchema(),
                kafkaProps
        );
        consumer.setStartFromLatest();

        WatermarkStrategy<WatermarkDemoEvent> wmStrategy = WatermarkStrategy
                .<WatermarkDemoEvent>forBoundedOutOfOrderness(OUT_OF_ORDERNESS)
                .withTimestampAssigner((event, ts) -> event.getTs());

        DataStream<WatermarkDemoEvent> eventStream = env
                .addSource(consumer)
                .name("Kafka-LateData")
                .map(new JsonToEventMapper())
                .filter(new ValidEventFilter())
                .assignTimestampsAndWatermarks(wmStrategy)
                .name("Timestamps/WM");

        SingleOutputStreamOperator<String> windowResults = buildWindowStream(eventStream, sideOutputEnabled);

        windowResults.print("主输出");

        if (sideOutputEnabled) {
            windowResults.getSideOutput(LATE_DATA_TAG)
                    .process(new LateDataCompensationFunction())
                    .name("LateCompensation")
                    .print("侧输出补偿");
        }

        printStartupBanner(mode, sideOutputEnabled);

        env.execute("Flink Late Data Demo - Three Lines of Defense");
    }

    static SingleOutputStreamOperator<String> buildWindowStream(
            DataStream<WatermarkDemoEvent> eventStream,
            boolean sideOutputEnabled) {
        if (sideOutputEnabled) {
            return eventStream
                    .keyBy(WatermarkDemoEvent::getUserId)
                    .window(TumblingEventTimeWindows.of(WINDOW_SIZE))
                    .allowedLateness(ALLOWED_LATENESS)
                    .sideOutputLateData(LATE_DATA_TAG)
                    .process(new LateDataWindowLogFunction())
                    .name("Tumbling10s-LateDefense");
        }
        return eventStream
                .keyBy(WatermarkDemoEvent::getUserId)
                .window(TumblingEventTimeWindows.of(WINDOW_SIZE))
                .allowedLateness(ALLOWED_LATENESS)
                .process(new LateDataWindowLogFunction())
                .name("Tumbling10s-LateDefense");
    }

    static String resolveMode(String[] args) {
        if (args != null && args.length > 0 && !args[0].isBlank()) {
            String mode = args[0].trim().toLowerCase();
            if (MODE_PV.equals(mode) || MODE_BILLING.equals(mode)) {
                return mode;
            }
            throw new IllegalArgumentException("未知模式: " + args[0] + "，请使用 billing 或 pv");
        }
        return MODE_BILLING;
    }

    /** 判断事件属于哪道防线（供单测与文档对照） */
    static LateDataLayer classifyEvent(long eventTs, long windowStart, long windowEnd,
                                       long watermark, long allowedLatenessMs) {
        if (eventTs >= windowStart && eventTs < windowEnd) {
            if (watermark < windowEnd) {
                return LateDataLayer.ON_TIME_OR_WM_BUFFER;
            }
            if (watermark < windowEnd + allowedLatenessMs) {
                return LateDataLayer.ALLOWED_LATENESS_UPDATE;
            }
            return LateDataLayer.SIDE_OUTPUT;
        }
        return LateDataLayer.OUT_OF_WINDOW;
    }

    enum LateDataLayer {
        /** 准时或 WM 乱序缓冲内 */
        ON_TIME_OR_WM_BUFFER,
        /** allowedLateness 内 → 同窗口重算 */
        ALLOWED_LATENESS_UPDATE,
        /** 超过 allowedLateness → 侧输出 */
        SIDE_OUTPUT,
        /** 不属于该窗口 */
        OUT_OF_WINDOW
    }

    private static void printStartupBanner(String mode, boolean sideOutputEnabled) {
        System.out.println("========================================");
        System.out.println("Flink 迟到数据三道防线 Demo 已启动");
        System.out.println("Kafka: " + KAFKA_BROKER + " | Topic: " + TOPIC);
        System.out.println("模式: " + mode + (MODE_BILLING.equals(mode) ? "（计费，侧输出+补偿）" : "（大屏PV，严重迟到丢弃）"));
        System.out.println("① WM 乱序容忍: forBoundedOutOfOrderness(" + OUT_OF_ORDERNESS.getSeconds() + "s)");
        System.out.println("② 窗口 lateness: allowedLateness(" + ALLOWED_LATENESS.getSeconds() + "s)");
        System.out.println("③ 侧输出: " + (sideOutputEnabled ? "sideOutputLateData ✅" : "未启用 ⚠️"));
        System.out.println("窗口: Tumbling " + WINDOW_SIZE.toMilliseconds() / 1000 + "s | 并行度: 1");
        System.out.println("文档: resources/watermark/FlinkLateDataDemoGuide.md");
        System.out.println("请运行 FlinkLateDataDemoJobTest 发送测试数据");
        System.out.println("========================================");
    }

    private static class JsonToEventMapper implements MapFunction<String, WatermarkDemoEvent> {
        private final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public WatermarkDemoEvent map(String value) {
            try {
                return objectMapper.readValue(value, WatermarkDemoEvent.class);
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
