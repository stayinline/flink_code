package org.example.job.lookup;

import org.example.dto.ProductDim;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/**
 * 模拟外部维表存储（MySQL / Redis）。
 * <p>
 * Demo 使用进程内 {@link ConcurrentHashMap} + 可配置查询延迟；
 * 生产环境替换为 JDBC / Redis 客户端即可。
 */
public class ProductDimStore {

  public static final String LOOKUP_SOURCE_DB = "DB";
  public static final String LOOKUP_SOURCE_MISS = "MISS";

  private final Map<String, ProductDim> store = new ConcurrentHashMap<>();
  private final long queryLatencyMs;

  public ProductDimStore(long queryLatencyMs) {
    this.queryLatencyMs = queryLatencyMs;
    seedDefaultCatalog();
  }

  /** 预置在线教育场景商品维表（课程 SKU） */
  private void seedDefaultCatalog() {
    long now = System.currentTimeMillis();
    put(new ProductDim("P100", "Java 零基础直播课", "编程", 199.0, now));
    put(new ProductDim("P101", "Python 数据分析", "编程", 299.0, now));
    put(new ProductDim("P102", "考研英语冲刺班", "考研", 599.0, now));
    put(new ProductDim("P103", "小学奥数思维课", "K12", 99.0, now));
    put(new ProductDim("P104", "产品经理实战营", "职场", 399.0, now));
  }

  public void put(ProductDim dim) {
    store.put(dim.getProductId(), dim);
  }

  public Optional<ProductDim> getSync(String productId) {
    return Optional.ofNullable(store.get(productId));
  }

  public Map<String, ProductDim> snapshot() {
    return Collections.unmodifiableMap(store);
  }

  /**
   * 异步查询维表，模拟网络 I/O 延迟。
   */
  public CompletableFuture<Optional<ProductDim>> getAsync(String productId, Executor executor) {
    return CompletableFuture.supplyAsync(() -> {
      sleepQuietly(queryLatencyMs);
      return Optional.ofNullable(store.get(productId));
    }, executor);
  }

  private static void sleepQuietly(long ms) {
    if (ms <= 0) {
      return;
    }
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
