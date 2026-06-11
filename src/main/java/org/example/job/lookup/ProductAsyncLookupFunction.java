package org.example.job.lookup;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.example.dto.EnrichedOrderEvent;
import org.example.dto.OrderLookupEvent;
import org.example.dto.ProductDim;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Async I/O 维表关联：Guava 本地缓存 + 异步查外部库（Demo 为 {@link ProductDimStore}）。
 * <p>
 * 配合 {@code DataStream.unorderedWait(...)} 使用，允许乱序完成以提升吞吐。
 */
public class ProductAsyncLookupFunction extends RichAsyncFunction<OrderLookupEvent, EnrichedOrderEvent> {

  public static final String LOOKUP_SOURCE_CACHE = "CACHE";

  private final long cacheTtlSec;
  private final long cacheMaxSize;
  private final long queryLatencyMs;

  private transient Cache<String, ProductDim> dimCache;
  private transient ProductDimStore dimStore;
  private transient ExecutorService executor;

  public ProductAsyncLookupFunction(long cacheTtlSec, long cacheMaxSize, long queryLatencyMs) {
    this.cacheTtlSec = cacheTtlSec;
    this.cacheMaxSize = cacheMaxSize;
    this.queryLatencyMs = queryLatencyMs;
  }

  @Override
  public void open(Configuration parameters) {
    dimCache = CacheBuilder.newBuilder()
        .maximumSize(cacheMaxSize)
        .expireAfterWrite(cacheTtlSec, TimeUnit.SECONDS)
        .recordStats()
        .build();
    dimStore = new ProductDimStore(queryLatencyMs);
    executor = Executors.newFixedThreadPool(20);
  }

  @Override
  public void asyncInvoke(OrderLookupEvent order, ResultFuture<EnrichedOrderEvent> resultFuture) {
    long startNs = System.nanoTime();
    String productId = order.getProductId();

    ProductDim cached = dimCache.getIfPresent(productId);
    if (cached != null) {
      complete(order, cached, LOOKUP_SOURCE_CACHE, elapsedMs(startNs), resultFuture);
      return;
    }

    dimStore.getAsync(productId, executor)
        .whenComplete((optDim, error) -> {
          if (error != null) {
            resultFuture.completeExceptionally(error);
            return;
          }
          if (optDim.isPresent()) {
            ProductDim dim = optDim.get();
            dimCache.put(productId, dim);
            complete(order, dim, ProductDimStore.LOOKUP_SOURCE_DB, elapsedMs(startNs), resultFuture);
          } else {
            completeMiss(order, elapsedMs(startNs), resultFuture);
          }
        });
  }

  @Override
  public void timeout(OrderLookupEvent input, ResultFuture<EnrichedOrderEvent> resultFuture) {
    completeMiss(input, -1L, resultFuture);
  }

  @Override
  public void close() {
    if (executor != null) {
      executor.shutdownNow();
    }
  }

  /** 供单测验证缓存统计 */
  CacheStatsSnapshot cacheStats() {
    if (dimCache == null) {
      return new CacheStatsSnapshot(0, 0, 0);
    }
    com.google.common.cache.CacheStats stats = dimCache.stats();
    return new CacheStatsSnapshot(stats.hitCount(), stats.missCount(), dimCache.size());
  }

  private void complete(OrderLookupEvent order, ProductDim dim, String source,
                        long latencyMs, ResultFuture<EnrichedOrderEvent> resultFuture) {
    EnrichedOrderEvent enriched = new EnrichedOrderEvent(
        order.getOrderId(),
        order.getProductId(),
        dim.getProductName(),
        dim.getCategory(),
        dim.getUnitPrice(),
        order.getQuantity(),
        order.getAmount(),
        order.getTs(),
        source,
        latencyMs,
        order.getTag()
    );
    resultFuture.complete(Collections.singleton(enriched));
  }

  private void completeMiss(OrderLookupEvent order, long latencyMs,
                            ResultFuture<EnrichedOrderEvent> resultFuture) {
    EnrichedOrderEvent enriched = new EnrichedOrderEvent(
        order.getOrderId(),
        order.getProductId(),
        null,
        null,
        null,
        order.getQuantity(),
        order.getAmount(),
        order.getTs(),
        ProductDimStore.LOOKUP_SOURCE_MISS,
        latencyMs,
        order.getTag()
    );
    resultFuture.complete(Collections.singleton(enriched));
  }

  private static long elapsedMs(long startNs) {
    return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);
  }

  /** 单测用：同步查缓存（不经过 asyncInvoke） */
  Optional<ProductDim> getCached(String productId) {
    return Optional.ofNullable(dimCache == null ? null : dimCache.getIfPresent(productId));
  }

  public static class CacheStatsSnapshot {
    public final long hitCount;
    public final long missCount;
    public final long size;

    public CacheStatsSnapshot(long hitCount, long missCount, long size) {
      this.hitCount = hitCount;
      this.missCount = missCount;
      this.size = size;
    }
  }
}
