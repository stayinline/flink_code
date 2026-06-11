package org.example.job.lookup;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.example.dto.OrderLookupEvent;
import org.example.dto.ProductDim;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 维表 Join 演示数据发送 + 缓存/Async 逻辑单测。
 * <p>
 * 运行前请创建 topic：
 * <pre>
 * kafka-topics.sh --create --topic test_flink_dim_join --partitions 2 \
 *   --bootstrap-server 192.168.1.124:9092
 * </pre>
 */
public class FlinkDimJoinDemoJobTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  public static final long BASE_TIME_MS = 1_700_000_000_000L;

  @Test
  void sendDimJoinDemoEvents() throws Exception {
    sendAllScenarios();
  }

  @Test
  void guavaCache_hitAfterFirstMiss() throws Exception {
    ProductAsyncLookupFunction lookup = new ProductAsyncLookupFunction(60, 1000, 0);
    lookup.open(new Configuration());

    OrderLookupEvent order = new OrderLookupEvent("O-UT-01", "P100", 1, 199.0, BASE_TIME_MS, "unit-test");
    EnrichedCollector first = invokeAndCollect(lookup, order);
    EnrichedCollector second = invokeAndCollect(lookup, order);

    assertEquals("DB", first.result.getLookupSource(), "首次应查库");
    assertEquals("CACHE", second.result.getLookupSource(), "二次应命中缓存");
    assertEquals("Java 零基础直播课", second.result.getProductName());
    lookup.close();
  }

  @Test
  void guavaCache_ttlExpiresThenRefetchDb() throws Exception {
    Cache<String, ProductDim> shortTtlCache = CacheBuilder.newBuilder()
        .expireAfterWrite(1, TimeUnit.SECONDS)
        .build();
    shortTtlCache.put("P100", new ProductDim("P100", "旧名称", "编程", 99.0, BASE_TIME_MS));

    shortTtlCache.getIfPresent("P100");
    Thread.sleep(1100);
    ProductDim afterExpire = shortTtlCache.getIfPresent("P100");

    assertTrue(afterExpire == null, "TTL 过期后缓存应失效，需重新查库");
    System.out.println("Guava TTL 验证：1s 过期后 getIfPresent 返回 null → 下次 lookup 走 DB");
  }

  @Test
  void dimStore_unknownProductReturnsMiss() throws Exception {
    ProductDimStore store = new ProductDimStore(0);
    assertTrue(store.getSync("P999").isEmpty(), "未知 SKU 应返回 empty");
  }

  @Test
  void asyncCapacity_shouldNotExceedConfigured() {
    int capacity = FlinkDimJoinDemoJob.DEFAULT_ASYNC_CAPACITY;
    assertTrue(capacity > 0 && capacity <= 1000,
        "capacity 应在合理范围，过大占内存、过小限吞吐");
    System.out.printf("Async capacity=%d, timeout=%dms — 调优见文档 Step4%n",
        capacity, FlinkDimJoinDemoJob.ASYNC_TIMEOUT_MS);
  }

  public static void main(String[] args) throws Exception {
    sendAllScenarios();
  }

  public static void sendAllScenarios() throws Exception {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, FlinkDimJoinDemoJob.KAFKA_BROKER);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.ACKS_CONFIG, "all");

    List<SendPlan> plan = buildPlan();

    System.out.println("========================================");
    System.out.println("维表 Join 演示数据发送");
    System.out.println("Topic: " + FlinkDimJoinDemoJob.TOPIC_ORDER);
    System.out.println("共 " + plan.size() + " 步");
    System.out.println("========================================");
    printPlan(plan);

    try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
      for (SendPlan step : plan) {
        if (step.sleepBeforeMs > 0) {
          System.out.printf("%n--- 等待 %ds：%s ---%n", step.sleepBeforeMs / 1000, step.waitReason);
          Thread.sleep(step.sleepBeforeMs);
        }
        send(producer, step);
      }
      producer.flush();
    }

    printExpectedOutcomes();
  }

  private static List<SendPlan> buildPlan() {
    List<SendPlan> plan = new ArrayList<>();

    // Phase 1：缓存未命中 → 首次查库（DB）
    plan.add(order("O01", "P100", 1, 199.0, 1, "cache-miss",
        "Phase1 首次关联 P100 → lookupSource=DB"));
    plan.add(order("O02", "P101", 1, 299.0, 2, "cache-miss",
        "Phase1 首次关联 P101 → lookupSource=DB"));

    // Phase 2：相同 productId 再次下单 → 缓存命中（CACHE）
    plan.add(wait(500, "等待 Job 处理 Phase1"));
    plan.add(order("O03", "P100", 2, 398.0, 3, "cache-hit",
        "Phase2 重复 P100 → lookupSource=CACHE，latencyMs 应明显低于 DB"));
    plan.add(order("O04", "P101", 1, 299.0, 4, "cache-hit",
        "Phase2 重复 P101 → lookupSource=CACHE"));

    // Phase 3：多品类订单（在线教育购课场景）
    plan.add(order("O05", "P102", 1, 599.0, 5, "edu-exam",
        "Phase3 考研英语冲刺班"));
    plan.add(order("O06", "P103", 3, 297.0, 6, "edu-k12",
        "Phase3 K12 奥数 3 科包"));
    plan.add(order("O07", "P104", 1, 399.0, 7, "edu-career",
        "Phase3 产品经理实战营"));

    // Phase 4：未知商品 → MISS
    plan.add(order("O08", "P999", 1, 1.0, 8, "unknown-product",
        "Phase4 维表无此 SKU → lookupSource=MISS"));

    // Phase 5：高并发 burst（观察 unorderedWait 乱序完成）
    plan.add(wait(1000, "观察 Phase4 输出"));
    for (int i = 0; i < 5; i++) {
      plan.add(order("O1" + i, "P100", 1, 199.0, 10 + i, "burst",
          "Phase5 突发 5 单同 SKU → 应大量 CACHE 命中"));
    }

    return plan;
  }

  private static SendPlan order(String orderId, String productId, int qty, double amount,
                                long offsetSec, String tag, String purpose) {
    return new SendPlan(
        new OrderLookupEvent(orderId, productId, qty, amount, BASE_TIME_MS + offsetSec * 1000, tag),
        0,
        null,
        purpose
    );
  }

  private static SendPlan wait(long ms, String reason) {
    return new SendPlan(null, ms, reason, reason);
  }

  private static void send(KafkaProducer<String, String> producer, SendPlan step) throws Exception {
    if (step.event == null) {
      return;
    }
    String json = MAPPER.writeValueAsString(step.event);
    ProducerRecord<String, String> record = new ProducerRecord<>(
        FlinkDimJoinDemoJob.TOPIC_ORDER,
        step.event.getProductId(),
        json
    );
    producer.send(record).get(10, TimeUnit.SECONDS);
    System.out.printf("[SEND] id=%s productId=%s tag=%-16s | %s%n",
        step.event.getOrderId(), step.event.getProductId(), step.event.getTag(), step.purpose);
    Thread.sleep(300);
  }

  private static void printPlan(List<SendPlan> plan) {
    System.out.println("--- 发送计划 ---");
    for (SendPlan s : plan) {
      if (s.event != null) {
        System.out.printf("  [%s] id=%s productId=%s %s%n",
            s.event.getTag(), s.event.getOrderId(), s.event.getProductId(), s.purpose);
      } else if (s.waitReason != null) {
        System.out.printf("  [WAIT %ds] %s%n", s.sleepBeforeMs / 1000, s.waitReason);
      }
    }
    System.out.println("----------------");
  }

  private static void printExpectedOutcomes() {
    System.out.println("========================================");
    System.out.println("预期观察（对照 Job 控制台）：");
    System.out.println("  Phase1 O01/O02 → source=DB, latencyMs≈80（模拟 MySQL 延迟）");
    System.out.println("  Phase2 O03/O04 → source=CACHE, latencyMs≈0");
    System.out.println("  Phase3 新课程 SKU 首次仍为 DB，之后 CACHE");
    System.out.println("  Phase4 O08 P999 → source=MISS, name=N/A");
    System.out.println("  Phase5 burst → 同 SKU 大量 CACHE 命中");
    System.out.println("对比实验：");
    System.out.println("  1) cacheTtlSec=60（默认）— 维表日内更新可接受短暂不一致");
    System.out.println("  2) cacheTtlSec=5  — 更实时但 DB QPS 升高");
    System.out.println("文档: resources/lookup/FlinkDimJoinDemoGuide.md");
    System.out.println("========================================");
  }

  private static EnrichedCollector invokeAndCollect(ProductAsyncLookupFunction lookup,
                                                    OrderLookupEvent order) throws Exception {
    EnrichedCollector collector = new EnrichedCollector();
    lookup.asyncInvoke(order, collector);
    collector.await(3, TimeUnit.SECONDS);
    assertNotNull(collector.result, "asyncInvoke 应产出 EnrichedOrderEvent");
    return collector;
  }

  private static class EnrichedCollector implements ResultFuture<org.example.dto.EnrichedOrderEvent> {
    org.example.dto.EnrichedOrderEvent result;
    private final java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(1);

    @Override
    public void complete(java.util.Collection<org.example.dto.EnrichedOrderEvent> result) {
      if (!result.isEmpty()) {
        this.result = result.iterator().next();
      }
      latch.countDown();
    }

    @Override
    public void completeExceptionally(Throwable error) {
      latch.countDown();
    }

    void await(long timeout, TimeUnit unit) throws InterruptedException {
      assertTrue(latch.await(timeout, unit), "asyncInvoke 应在超时内完成");
    }
  }

  private static class SendPlan {
    final OrderLookupEvent event;
    final long sleepBeforeMs;
    final String waitReason;
    final String purpose;

    SendPlan(OrderLookupEvent event, long sleepBeforeMs, String waitReason, String purpose) {
      this.event = event;
      this.sleepBeforeMs = sleepBeforeMs;
      this.waitReason = waitReason;
      this.purpose = purpose;
    }
  }
}
