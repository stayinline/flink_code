package org.example.job.join;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.example.dto.EducationClickEvent;
import org.example.dto.EducationExposureEvent;

import java.time.Duration;
import java.util.Properties;

/**
 * 双流 Join 演示 Job：Regular vs Interval vs Window（在线教育曝光流 + 点击流）。
 * <p>
 * 场景：
 * <ul>
 *   <li>{@code interval} — 点击在曝光后 10 分钟内有效（Interval Join，状态自动清理）</li>
 *   <li>{@code regular} — Regular Inner Join，状态无限增长（可选 State TTL）</li>
 *   <li>{@code window} — Tumbling EventTime Window Join</li>
 *   <li>{@code probe} — Interval 状态探针，观察 buffer 清理</li>
 * </ul>
 * <p>
 * 启动参数：{@code mode intervalUpperMin windowSec stateTtlHours}
 * 例：{@code interval 10 30 0} 或 {@code regular 10 30 24} 或 {@code probe 10 30 0}
 * <p>
 * 文档：{@code src/main/resources/join/FlinkJoinDemoGuide.md}
 */
public class FlinkJoinDemoJob {

    public static final String KAFKA_BROKER = "192.168.1.124:9092";
    public static final String TOPIC_EXPOSURE = "test_flink_join_exposure";
    public static final String TOPIC_CLICK = "test_flink_join_click";
    public static final String GROUP_ID_EXPOSURE = "flink-join-demo-exposure";
    public static final String GROUP_ID_CLICK = "flink-join-demo-click";

    public static final Duration OUT_OF_ORDERNESS = Duration.ofSeconds(5);

    public static void main(String[] args) throws Exception {
        JoinDemoConfigurator.JoinDemoOptions options = JoinDemoConfigurator.resolveOptions(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        JoinDemoConfigurator.configureEnvironment(env);

        Properties exposureProps = kafkaProps(GROUP_ID_EXPOSURE);
        Properties clickProps = kafkaProps(GROUP_ID_CLICK);

        DataStream<EducationExposureEvent> exposureStream = buildExposureStream(env, exposureProps);
        DataStream<EducationClickEvent> clickStream = buildClickStream(env, clickProps);

        buildJoinPipeline(exposureStream, clickStream, options);

        printStartupBanner(options);
        env.execute("Flink Join Demo - Regular / Interval / Window");
    }

    static void buildJoinPipeline(
            DataStream<EducationExposureEvent> exposureStream,
            DataStream<EducationClickEvent> clickStream,
            JoinDemoConfigurator.JoinDemoOptions options) {

        switch (options.mode) {
            case JoinDemoConfigurator.MODE_REGULAR:
                buildRegularJoin(exposureStream, clickStream, options);
                break;
            case JoinDemoConfigurator.MODE_WINDOW:
                buildWindowJoin(exposureStream, clickStream, options);
                break;
            case JoinDemoConfigurator.MODE_PROBE:
                buildStateProbe(exposureStream, clickStream, options);
                break;
            case JoinDemoConfigurator.MODE_INTERVAL:
            default:
                buildIntervalJoin(exposureStream, clickStream, options);
                break;
        }
    }

    private static void buildIntervalJoin(
            DataStream<EducationExposureEvent> exposureStream,
            DataStream<EducationClickEvent> clickStream,
            JoinDemoConfigurator.JoinDemoOptions options) {

        KeyedStream<EducationExposureEvent, String> keyedExposure =
                exposureStream.keyBy(EducationExposureEvent::getRequestId);
        KeyedStream<EducationClickEvent, String> keyedClick =
                clickStream.keyBy(EducationClickEvent::getRequestId);

        keyedExposure
                .intervalJoin(keyedClick)
                .between(
                        JoinDemoConfigurator.intervalLowerBound(),
                        JoinDemoConfigurator.intervalUpperBound(options.intervalUpperMin))
                .process(new EducationIntervalJoinFunction())
                .name("IntervalJoin-0to" + options.intervalUpperMin + "min")
                .map(new JoinResultFormatter())
                .print("IntervalJoin");
    }

    private static void buildRegularJoin(
            DataStream<EducationExposureEvent> exposureStream,
            DataStream<EducationClickEvent> clickStream,
            JoinDemoConfigurator.JoinDemoOptions options) {

        exposureStream
                .keyBy(EducationExposureEvent::getRequestId)
                .connect(clickStream.keyBy(EducationClickEvent::getRequestId))
                .process(new EducationRegularJoinFunction(options.stateTtlHours))
                .name("RegularJoin-ttl" + options.stateTtlHours + "h")
                .map(new JoinResultFormatter())
                .print("RegularJoin");
    }

    private static void buildWindowJoin(
            DataStream<EducationExposureEvent> exposureStream,
            DataStream<EducationClickEvent> clickStream,
            JoinDemoConfigurator.JoinDemoOptions options) {

        exposureStream
                .join(clickStream)
                .where(EducationExposureEvent::getRequestId)
                .equalTo(EducationClickEvent::getRequestId)
                .window(TumblingEventTimeWindows.of(JoinDemoConfigurator.windowSize(options.windowSec)))
                .apply(new EducationWindowJoinFunction())
                .map(new JoinResultFormatter())
                .print("WindowJoin");
    }

    private static void buildStateProbe(
            DataStream<EducationExposureEvent> exposureStream,
            DataStream<EducationClickEvent> clickStream,
            JoinDemoConfigurator.JoinDemoOptions options) {

        long upperMs = options.intervalUpperMin * 60_000L;
        exposureStream
                .keyBy(EducationExposureEvent::getRequestId)
                .connect(clickStream.keyBy(EducationClickEvent::getRequestId))
                .process(new IntervalJoinStateProbeFunction(0L, upperMs))
                .name("IntervalStateProbe")
                .print("StateProbe");
    }

    private static DataStream<EducationExposureEvent> buildExposureStream(
            StreamExecutionEnvironment env, Properties kafkaProps) {

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                TOPIC_EXPOSURE, new SimpleStringSchema(), kafkaProps);
        consumer.setStartFromLatest();

        return env
                .addSource(consumer)
                .name("Kafka-Exposure")
                .map(new JsonToExposureMapper())
                .filter(new ValidExposureFilter())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<EducationExposureEvent>forBoundedOutOfOrderness(OUT_OF_ORDERNESS)
                                .withTimestampAssigner((event, ts) -> event.getTs()))
                .name("WM-Exposure");
    }

    private static DataStream<EducationClickEvent> buildClickStream(
            StreamExecutionEnvironment env, Properties kafkaProps) {

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                TOPIC_CLICK, new SimpleStringSchema(), kafkaProps);
        consumer.setStartFromLatest();

        return env
                .addSource(consumer)
                .name("Kafka-Click")
                .map(new JsonToClickMapper())
                .filter(new ValidClickFilter())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<EducationClickEvent>forBoundedOutOfOrderness(OUT_OF_ORDERNESS)
                                .withTimestampAssigner((event, ts) -> event.getTs()))
                .name("WM-Click");
    }

    private static Properties kafkaProps(String groupId) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", KAFKA_BROKER);
        props.setProperty("group.id", groupId);
        return props;
    }

    private static void printStartupBanner(JoinDemoConfigurator.JoinDemoOptions options) {
        System.out.println("========================================");
        System.out.println("Flink 双流 Join 演示 Job 已启动");
        System.out.println("Kafka: " + KAFKA_BROKER);
        System.out.println("Exposure: " + TOPIC_EXPOSURE + " | Click: " + TOPIC_CLICK);
        System.out.println("模式: " + JoinDemoConfigurator.describeMode(options.mode));
        System.out.println("Interval 上界: +" + options.intervalUpperMin + "min | Window: "
                + options.windowSec + "s | Regular TTL: " + options.stateTtlHours + "h");
        System.out.println("文档: resources/join/FlinkJoinDemoGuide.md");
        System.out.println("请运行 FlinkJoinDemoJobTest 发送测试数据");
        System.out.println("对比实验：");
        System.out.println("  1) interval 10 30 0   → 10min 内点击匹配，状态自动清理");
        System.out.println("  2) regular 10 30 0    → 状态无限增长 ⚠️");
        System.out.println("  3) window 10 30 0     → 同 30s 窗口");
        System.out.println("  4) probe 10 30 0      → [INTERVAL_STATE_PROBE] cleanup");
        System.out.println("========================================");
    }

    private static class JsonToExposureMapper implements MapFunction<String, EducationExposureEvent> {
        private final ObjectMapper mapper = new ObjectMapper();

        @Override
        public EducationExposureEvent map(String value) {
            try {
                return mapper.readValue(value, EducationExposureEvent.class);
            } catch (Exception e) {
                System.err.println("Exposure JSON 解析失败: " + value);
                return null;
            }
        }
    }

    private static class JsonToClickMapper implements MapFunction<String, EducationClickEvent> {
        private final ObjectMapper mapper = new ObjectMapper();

        @Override
        public EducationClickEvent map(String value) {
            try {
                return mapper.readValue(value, EducationClickEvent.class);
            } catch (Exception e) {
                System.err.println("Click JSON 解析失败: " + value);
                return null;
            }
        }
    }

    private static class ValidExposureFilter implements FilterFunction<EducationExposureEvent> {
        @Override
        public boolean filter(EducationExposureEvent e) {
            return e != null && e.getRequestId() != null && e.getTs() != null && e.getTs() > 0;
        }
    }

    private static class ValidClickFilter implements FilterFunction<EducationClickEvent> {
        @Override
        public boolean filter(EducationClickEvent e) {
            return e != null && e.getRequestId() != null && e.getTs() != null && e.getTs() > 0;
        }
    }
}
