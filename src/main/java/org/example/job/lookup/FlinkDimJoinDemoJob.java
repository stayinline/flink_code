package org.example.job.lookup;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.example.dto.EnrichedOrderEvent;
import org.example.dto.OrderLookupEvent;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * 维表 Join 演示 Job — Async I/O + Guava 本地缓存。
 * <p>
 * 功能：
 * <ul>
 *   <li>订单流 Kafka 源 → Async I/O 关联商品维表（模拟 MySQL/Redis）</li>
 *   <li>Guava Cache TTL 缓存，减少外部库 QPS</li>
 *   <li>{@code unorderedWait} 允许乱序完成，提升吞吐</li>
 *   <li>打印 lookupSource（CACHE / DB / MISS）与耗时</li>
 * </ul>
 * <p>
 * 启动参数：{@code cacheTtlSec}（默认 60）、{@code asyncCapacity}（默认 100）
 * 或 VM 参数：{@code -Ddimjoin.cache.ttl.sec=60}、{@code -Ddimjoin.async.capacity=100}
 * <p>
 * 文档：{@code src/main/resources/lookup/FlinkDimJoinDemoGuide.md}
 */
public class FlinkDimJoinDemoJob {

  public static final String KAFKA_BROKER = "192.168.1.124:9092";
  public static final String TOPIC_ORDER = "test_flink_dim_join";
  public static final String GROUP_ID = "flink-dim-join-demo-consumer";

  /** Async I/O 并发请求上限（capacity） */
  public static final int DEFAULT_ASYNC_CAPACITY = 100;
  /** 单次 lookup 超时（毫秒） */
  public static final long ASYNC_TIMEOUT_MS = 5_000;
  /** Guava Cache TTL（秒） */
  public static final long DEFAULT_CACHE_TTL_SEC = 60;
  public static final long CACHE_MAX_SIZE = 10_000;
  /** 模拟外部库查询延迟（毫秒） */
  public static final long SIMULATED_DB_LATENCY_MS = 80;

  public static void main(String[] args) throws Exception {
    long cacheTtlSec = resolveLongArg(args, 0, "dimjoin.cache.ttl.sec", DEFAULT_CACHE_TTL_SEC);
    int asyncCapacity = (int) resolveLongArg(args, 1, "dimjoin.async.capacity", DEFAULT_ASYNC_CAPACITY);

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);
    env.enableCheckpointing(5000);

    Properties kafkaProps = new Properties();
    kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKER);
    kafkaProps.setProperty("group.id", GROUP_ID);

    FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
        TOPIC_ORDER,
        new SimpleStringSchema(),
        kafkaProps
    );
    consumer.setStartFromLatest();

    DataStream<OrderLookupEvent> orderStream = env
        .addSource(consumer)
        .name("Kafka-Order")
        .map(new JsonToOrderMapper())
        .filter(new ValidOrderFilter())
        .name("ParseOrder");

    DataStream<EnrichedOrderEvent> enrichedStream = AsyncDataStream.unorderedWait(
        orderStream,
        new ProductAsyncLookupFunction(cacheTtlSec, CACHE_MAX_SIZE, SIMULATED_DB_LATENCY_MS),
        ASYNC_TIMEOUT_MS,
        TimeUnit.MILLISECONDS,
        asyncCapacity
    ).name("AsyncDimLookup");

    enrichedStream
        .map(new EnrichedLogMapper())
        .name("LogEnriched")
        .print("维表关联");

    printStartupBanner(cacheTtlSec, asyncCapacity);
    env.execute("Flink Dim Join Demo - Async I/O + Guava Cache");
  }

  static long resolveLongArg(String[] args, int index, String sysProp, long defaultValue) {
    if (args != null && args.length > index && args[index] != null && !args[index].isBlank()) {
      return Long.parseLong(args[index]);
    }
    return Long.parseLong(System.getProperty(sysProp, String.valueOf(defaultValue)));
  }

  private static void printStartupBanner(long cacheTtlSec, int asyncCapacity) {
    System.out.println("========================================");
    System.out.println("Flink 维表 Join 演示 Job 已启动");
    System.out.println("Kafka: " + KAFKA_BROKER);
    System.out.println("Topic: " + TOPIC_ORDER);
    System.out.println("Async I/O: unorderedWait | capacity=" + asyncCapacity
        + " | timeout=" + ASYNC_TIMEOUT_MS + "ms");
    System.out.println("Guava Cache: TTL=" + cacheTtlSec + "s | maxSize=" + CACHE_MAX_SIZE);
    System.out.println("模拟 DB 延迟: " + SIMULATED_DB_LATENCY_MS + "ms");
    System.out.println("文档: resources/lookup/FlinkDimJoinDemoGuide.md");
    System.out.println("请运行 FlinkDimJoinDemoJobTest 发送测试数据");
    System.out.println("========================================");
  }

  private static class JsonToOrderMapper implements MapFunction<String, OrderLookupEvent> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public OrderLookupEvent map(String value) {
      try {
        return objectMapper.readValue(value, OrderLookupEvent.class);
      } catch (Exception e) {
        System.err.println("JSON 解析失败: " + value + ", 错误: " + e.getMessage());
        return null;
      }
    }
  }

  private static class ValidOrderFilter implements FilterFunction<OrderLookupEvent> {
    @Override
    public boolean filter(OrderLookupEvent event) {
      return event != null
          && event.getOrderId() != null
          && event.getProductId() != null
          && event.getTs() != null
          && event.getTs() > 0;
    }
  }

  private static class EnrichedLogMapper implements MapFunction<EnrichedOrderEvent, String> {
    @Override
    public String map(EnrichedOrderEvent e) {
      return String.format(
          "[DIM-JOIN] orderId=%s productId=%s name=%s category=%s "
              + "source=%s latencyMs=%d tag=%s",
          e.getOrderId(),
          e.getProductId(),
          e.getProductName() == null ? "N/A" : e.getProductName(),
          e.getCategory() == null ? "N/A" : e.getCategory(),
          e.getLookupSource(),
          e.getLookupLatencyMs(),
          e.getTag()
      );
    }
  }
}
