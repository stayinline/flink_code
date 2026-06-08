package org.example.job.trigger;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.example.dto.WatermarkDemoEvent;

import java.time.Duration;
import java.util.Properties;

/**
 * Trigger 与 Evictor 演示 Job。
 * <p>
 * 三条对比支路（同一份 Kafka 数据）：
 * <ul>
 *   <li>默认 {@code EventTimeTrigger} — 仅窗口结束时输出一次</li>
 *   <li>{@link CountOrTimeTrigger} — 每 100 条或窗口结束提前/最终 FIRE（不 PURGE）</li>
 *   <li>{@link CountOrTimeTrigger} + {@link CountEvictor} — 演示 Evictor 剔除元素、破坏增量语义</li>
 * </ul>
 * <p>
 * 文档：{@code src/main/resources/trigger/FlinkTriggerEvictorDemoGuide.md}
 */
public class FlinkTriggerEvictorDemoJob {

    public static final String KAFKA_BROKER = "192.168.1.124:9092";
    public static final String TOPIC = "test_flink_trigger";
    public static final String GROUP_ID = "flink-trigger-evictor-demo-consumer";

    public static final Duration OUT_OF_ORDERNESS = Duration.ofSeconds(5);
    /** 大窗口：便于在 WM 推进到 end 之前观察 early-fire */
    public static final Time WINDOW_SIZE = Time.minutes(5);
    /** CountOrTimeTrigger 阈值：每攒满 100 条提前 FIRE */
    public static final long COUNT_THRESHOLD = 100;
    /** Evictor 保留最近 N 条（剔除更早元素） */
    public static final int EVICTOR_KEEP_COUNT = 30;

    public static void main(String[] args) throws Exception {
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

        DataStream<WatermarkDemoEvent> eventStream = env
                .addSource(consumer)
                .name("Kafka-TriggerDemo")
                .map(new JsonToEventMapper())
                .filter(new ValidEventFilter())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WatermarkDemoEvent>forBoundedOutOfOrderness(OUT_OF_ORDERNESS)
                                .withTimestampAssigner((event, ts) -> event.getTs())
                )
                .name("Timestamps/WM");

        KeyedStream<WatermarkDemoEvent, String> keyedStream = eventStream.keyBy(WatermarkDemoEvent::getUserId);

        // 支路 1：默认 EventTimeTrigger（仅窗口结束 FIRE 一次）
        keyedStream
                .window(TumblingEventTimeWindows.of(WINDOW_SIZE))
                .process(new TriggerWindowLogFunction("DEFAULT-EventTimeTrigger"))
                .name("DefaultEventTimeTrigger")
                .print("默认Trigger");

        // 支路 2：CountOrTimeTrigger + aggregate（early-fire 可配合增量聚合）
        keyedStream
                .window(TumblingEventTimeWindows.of(WINDOW_SIZE))
                .trigger(CountOrTimeTrigger.of(COUNT_THRESHOLD))
                .aggregate(
                        new WatermarkAmountSumAggregator(),
                        new TriggerAggregateFormatter("COUNT-OR-TIME+AGG")
                )
                .name("CountOrTimeAggregate")
                .print("自定义Trigger+聚合");

        // 支路 3：CountOrTimeTrigger + ProcessWindowFunction（全量可见，观察 early-fire 多次输出）
        keyedStream
                .window(TumblingEventTimeWindows.of(WINDOW_SIZE))
                .trigger(CountOrTimeTrigger.of(COUNT_THRESHOLD))
                .process(new TriggerWindowLogFunction("COUNT-OR-TIME"))
                .name("CountOrTimeProcess")
                .print("自定义Trigger");

        // 支路 4：CountOrTimeTrigger + CountEvictor（剔除旧元素，count 永远 ≤ keepCount）
        keyedStream
                .window(TumblingEventTimeWindows.of(WINDOW_SIZE))
                .trigger(CountOrTimeTrigger.of(COUNT_THRESHOLD))
                .evictor(CountEvictor.of(EVICTOR_KEEP_COUNT))
                .process(new TriggerWindowLogFunction("COUNT-OR-TIME+Evictor"))
                .name("CountOrTimeEvictor")
                .print("Evictor演示");

        printStartupBanner();
        env.execute("Flink Trigger & Evictor Demo - CountOrTimeTrigger");
    }

    private static void printStartupBanner() {
        System.out.println("========================================");
        System.out.println("Flink Trigger & Evictor 演示 Job 已启动");
        System.out.println("Kafka: " + KAFKA_BROKER + " / topic: " + TOPIC);
        System.out.println("窗口: Tumbling " + WINDOW_SIZE + " | CountOrTimeTrigger 阈值=" + COUNT_THRESHOLD);
        System.out.println("Evictor: CountEvictor.keep=" + EVICTOR_KEEP_COUNT);
        System.out.println("乱序容忍: forBoundedOutOfOrderness(5s)");
        System.out.println("文档: resources/trigger/FlinkTriggerEvictorDemoGuide.md");
        System.out.println("请运行 FlinkTriggerEvictorDemoJobTest 发送测试数据");
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
