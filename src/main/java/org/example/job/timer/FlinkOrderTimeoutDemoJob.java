package org.example.job.timer;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.example.dto.OrderPaymentEvent;

import java.time.Duration;
import java.util.Properties;

/**
 * KeyedProcessFunction + 事件时间定时器演示 Job —— 订单超时未支付告警。
 * <p>
 * 流程：
 * <ol>
 *   <li>{@code ORDER_CREATED} → {@code registerEventTimeTimer(orderTs + timeout)}</li>
 *   <li>{@code PAYMENT} → {@code deleteEventTimeTimer}，输出 {@code [PAID-IN-TIME]}</li>
 *   <li>超时无支付 → {@code onTimer} 输出 {@code [TIMEOUT-ALERT]}</li>
 * </ol>
 * <p>
 * 生产超时 15 分钟；演示超时 {@value #TIMEOUT_MS}ms（15 秒）。
 * 文档：{@code src/main/resources/timer/FlinkOrderTimeoutDemoGuide.md}
 */
public class FlinkOrderTimeoutDemoJob {

    public static final String KAFKA_BROKER = "192.168.1.124:9092";
    public static final String TOPIC = "test_flink_order_timeout";
    public static final String GROUP_ID = "flink-order-timeout-demo-consumer";

    public static final Duration OUT_OF_ORDERNESS = Duration.ofSeconds(5);
    /** 演示用 15s；生产替换为 {@code Duration.ofMinutes(15).toMillis()} */
    public static final long TIMEOUT_MS = 15_000L;
    public static final long PRODUCTION_TIMEOUT_MS = Duration.ofMinutes(15).toMillis();

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

        DataStream<OrderPaymentEvent> eventStream = env
                .addSource(consumer)
                .name("Kafka-OrderTimeout")
                .map(new JsonToEventMapper())
                .filter(new ValidEventFilter())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<OrderPaymentEvent>forBoundedOutOfOrderness(OUT_OF_ORDERNESS)
                                .withTimestampAssigner((event, ts) -> event.getTs())
                )
                .name("Timestamps/WM");

        DataStream<OrderPaymentEvent> monitoredStream = eventStream
                .process(new OrderEventMonitorFunction())
                .name("WatermarkMonitor");

        monitoredStream
                .keyBy(OrderPaymentEvent::getOrderId)
                .process(new OrderTimeoutAlertFunction(TIMEOUT_MS))
                .name("OrderTimeoutAlert")
                .print("订单超时");

        printStartupBanner();
        env.execute("Flink Order Timeout Demo - KeyedProcessFunction + EventTimeTimer");
    }

    private static void printStartupBanner() {
        System.out.println("========================================");
        System.out.println("Flink ProcessFunction + 定时器 演示 Job 已启动");
        System.out.println("Kafka: " + KAFKA_BROKER + " / topic: " + TOPIC);
        System.out.println("超时: " + (TIMEOUT_MS / 1000) + "s（演示）| 生产建议: 15min");
        System.out.println("乱序容忍: forBoundedOutOfOrderness(5s)");
        System.out.println("keyBy: orderId | 定时器与 key 绑定，存状态后端");
        System.out.println("文档: resources/timer/FlinkOrderTimeoutDemoGuide.md");
        System.out.println("请运行 FlinkOrderTimeoutDemoJobTest 发送测试数据");
        System.out.println("========================================");
    }

    private static class JsonToEventMapper implements MapFunction<String, OrderPaymentEvent> {
        private final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public OrderPaymentEvent map(String value) {
            try {
                return objectMapper.readValue(value, OrderPaymentEvent.class);
            } catch (Exception e) {
                System.err.println("JSON 解析失败: " + value + ", 错误: " + e.getMessage());
                return null;
            }
        }
    }

    private static class ValidEventFilter implements FilterFunction<OrderPaymentEvent> {
        @Override
        public boolean filter(OrderPaymentEvent event) {
            return event != null
                    && event.getOrderId() != null
                    && !event.getOrderId().isEmpty()
                    && event.getUserId() != null
                    && !event.getUserId().isEmpty()
                    && event.getEventType() != null
                    && !event.getEventType().isEmpty()
                    && event.getTs() != null
                    && event.getTs() > 0;
        }
    }
}
