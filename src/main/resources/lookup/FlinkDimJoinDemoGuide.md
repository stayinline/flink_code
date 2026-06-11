# Flink 维表 Join 四种方式 — 学习指南

> 配套代码：`FlinkDimJoinDemoJob` + `FlinkDimJoinDemoJobTest`  
> 数据源：订单流 `(orderId, productId, quantity, amount, ts, tag)` → Async I/O 关联商品维表  
> 核心实现：`ProductAsyncLookupFunction`（Guava Cache + 模拟 MySQL/Redis）

---

## 读前扫盲：维表 Join 解决的是「事实流缺维度属性」

订单流只有 `productId`，报表需要 `productName`、`category`、`unitPrice`。维表（Dimension Table）就是这些**相对静态、按主键查询**的参考数据。

流处理里关联维表，本质是：**每条事实到达时，按 key 去查一份外部或内存中的维度数据**。难点在于：

| 难点 | 说明 |
|------|------|
| 维表可能很大 | 不能简单 `map` 全量加载到每个 subtask |
| 维表会更新 | 缓存 TTL 与一致性权衡 |
| 查库是 I/O 阻塞 | 同步 `map` 会把吞吐压到数据库 QPS 上限 |
| 多种业务约束 | 小表静态 / 大表实时 / 中等表频繁更新 — 方案不同 |

本 Demo **手写 Async I/O + Guava 本地缓存**，用 `lookupSource=CACHE|DB|MISS` 和 `latencyMs` 直观观察缓存命中与查库延迟。

---

## Step 1 原理：四种维表 Join 方案

### ① 预加载全量到内存（open() 加载）

```
Job 启动 → RichFunction.open() → 从 MySQL/文件读全量维表 → ConcurrentHashMap

订单流 ──map/flatMap──→ hashMap.get(productId) ──→ 宽表
```

| 适用 | 维表 **小**（通常 < 几百万行）、**静态或极少更新** |
|------|--------------------------------------------------|
| 优点 | 实现简单、延迟最低、无外部依赖 |
| 缺点 | 维表变大 OOM；更新需重启或自研热加载 |

```java
// 示意：open() 预加载
@Override
public void open(Configuration parameters) {
    dimMap = jdbcTemplate.query("SELECT * FROM product_dim", ...);
}
```

### ② Async I/O 异步查外部库（本 Demo 核心）

```
订单流 ──AsyncDataStream.unorderedWait──→ RichAsyncFunction
                                              │
                    ┌─────────────────────────┼─────────────────────────┐
                    ▼                         ▼                         ▼
              Guava Cache 命中            异步查 MySQL/Redis          超时 → MISS
              (latency≈0)               (latency=几十~几百ms)
```

| 适用 | 维表 **大**、不能全量进内存；需 **近实时** 关联 |
|------|-----------------------------------------------|
| 优点 | 不阻塞主线程；配合缓存降低 DB QPS；`unorderedWait` 提升吞吐 |
| 缺点 | 实现复杂；capacity/超时需调优；缓存带来短暂不一致 |

**本 Demo 实现**：`ProductAsyncLookupFunction` + `AsyncDataStream.unorderedWait`

### ③ Lookup Join（Flink SQL `FOR SYSTEM_TIME AS OF`）

```sql
SELECT o.*, p.product_name, p.category
FROM order_stream AS o
JOIN product_dim FOR SYSTEM_TIME AS OF o.proc_time AS p
  ON o.product_id = p.product_id;
```

| 适用 | 已用 Flink SQL / Table API；维表注册为 **JDBC / HBase / Redis** 等 Lookup 连接器 |
|------|-------------------------------------------------------------------------------------|
| 优点 | 声明式、内置 **cache**（`lookup.cache.max-rows` / `lookup.cache.ttl`） |
| 缺点 | 灵活性低于手写 Async；cache 策略需理解 |

**加分点 — Lookup Join cache 失效与已关联数据**：

- Cache 按 **key + TTL** 失效，不是按维表 binlog 实时推送。
- 维表某 SKU 价格从 199 改为 299：**已输出到下游的宽表不会回溯修改**；cache 过期后**新到达的订单**才拿到新价。
- 若业务要求「改价后立即生效」，需：**缩短 TTL**、**主动 cache 失效**（改维表时发广播流清 cache）、或 **Temporal Table Join**（版本化维表 + proc_time）。

### ④ Broadcast State（维表作广播流）

```
维表 CDC/Kafka ──broadcast──→ BroadcastStream
                                    │
订单流 ──connect──→ BroadcastProcessFunction ──→ 宽表
                    （本地 MapState 存维表快照）
```

| 适用 | 维表 **中等规模**、需 **流式更新**（如商品上下架、课程状态变更） |
|------|----------------------------------------------------------------|
| 优点 | 维表变更可实时反映到关联逻辑；无需每条查库 |
| 缺点 | 广播状态占内存；维表过大 **OOM**；需处理乱序更新 |

---

## Step 2 手写代码对照

| 要求 | 实现 |
|------|------|
| Async I/O 维表关联 | `AsyncDataStream.unorderedWait` + `ProductAsyncLookupFunction` |
| Guava 本地缓存 | `CacheBuilder.expireAfterWrite(cacheTtlSec)` |
| 模拟 MySQL/Redis | `ProductDimStore.getAsync()` + 可配置延迟 |
| 观察 CACHE / DB / MISS | `EnrichedOrderEvent.lookupSource` + `latencyMs` |
| unorderedWait | 允许请求乱序完成，提高吞吐 |

### 关键代码

```java
// FlinkDimJoinDemoJob.java
DataStream<EnrichedOrderEvent> enriched = AsyncDataStream.unorderedWait(
    orderStream,
    new ProductAsyncLookupFunction(cacheTtlSec, CACHE_MAX_SIZE, SIMULATED_DB_LATENCY_MS),
    ASYNC_TIMEOUT_MS,
    TimeUnit.MILLISECONDS,
    asyncCapacity   // capacity：最大并发未完成请求数
).name("AsyncDimLookup");
```

```java
// ProductAsyncLookupFunction.java — 缓存命中 vs 异步查库
ProductDim cached = dimCache.getIfPresent(productId);
if (cached != null) {
    complete(order, cached, "CACHE", ...);
    return;
}
dimStore.getAsync(productId, executor).whenComplete((optDim, error) -> {
    if (optDim.isPresent()) {
        dimCache.put(productId, optDim.get());
        complete(order, optDim.get(), "DB", ...);
    } else {
        completeMiss(order, ...);  // MISS
    }
});
```

### 预置维表（在线教育 SKU）

| productId | productName | category | unitPrice |
|-----------|-------------|----------|-----------|
| P100 | Java 零基础直播课 | 编程 | 199 |
| P101 | Python 数据分析 | 编程 | 299 |
| P102 | 考研英语冲刺班 | 考研 | 599 |
| P103 | 小学奥数思维课 | K12 | 99 |
| P104 | 产品经理实战营 | 职场 | 399 |

---

## Step 3 四种方案对比表（四维打分 1~5，5=最优）

| 方案 | 维表大小 | 更新频率 | 一致性 | 实现复杂度 | 典型场景 |
|------|:--------:|:--------:|:------:|:----------:|----------|
| ① 预加载内存 | 5（仅小表） | 1（差） | 3 | 5（最简单） | 省市区、科目字典 |
| ② **Async I/O + Cache** | 4 | 3 | 3 | 3 | **大商品库、订单关联** ← 本 Demo |
| ③ Lookup Join (SQL) | 4 | 3 | 3 | 4 | SQL 作业、快速上线 |
| ④ Broadcast State | 3 | 5 | 4 | 2 | 课程上下架、价格广播 |

### 简历项目标注（示例话术）

> **我们在线教育订单实时宽表项目用的是 Async I/O + Guava 本地缓存。**  
> 商品 SKU 约 **50 万+**，每天凌晨批量改价/上下架，无法全量 broadcast；订单峰值 **3k QPS**，同步 JDBC 会把 MySQL 打满。  
> Async `unorderedWait` capacity=200、cache TTL=5min，DB QPS 从 3k 降到约 **100**；改价后最多 5 分钟全链路生效，业务接受。  
> 科目字典等小表用 **open() 预加载**，不走 Async。

---

## Step 4 调优与陷阱

### Async I/O

| 参数 | 含义 | 调优建议 |
|------|------|----------|
| **capacity** | 单 subtask 最大并发未完成异步请求 | 过小 → 背压、吞吐低；过大 → 内存与 DB 连接数暴涨。从 `100~200` 起，结合 DB 连接池上限 |
| **timeout** | 单次 lookup 超时 | 略大于 DB P99（如 5s）；超时走 `timeout()` → 本 Demo 标 MISS |
| **ordered vs unorderedWait** | 是否保持输入顺序 | 报表宽表通常 **unorderedWait**；严格顺序用 `orderedWait` |

### Guava Cache TTL

| 权衡 | 说明 |
|------|------|
| TTL 长 | DB 压力小；维表更新反映慢（**最终一致**） |
| TTL 短 | 更实时；DB QPS 升、延迟波动大 |
| 维表日更 | TTL 可设 **几小时**；小时级改价可 **5~15min** |

**陷阱**：以为「改了 MySQL 下游立刻变」— 只有 cache 失效后**新订单**才用新维表；历史已写入 ClickHouse 的宽表不会自动更新。

### Broadcast State OOM

- 维表行数 × 单行大小 × 并行度副本 → 估算堆内存。
- 超过 **几百 MB ~ 几 GB** 应改 Async 或 Lookup Join + 短 TTL。
- 更新过于频繁时 broadcast 状态 merge 成本也高。

### 其他陷阱

1. **同步 map 里查 JDBC** — 一条阻塞一条，吞吐 = DB QPS 上限。
2. **cache 无上限** — 用 `maximumSize` 防 OOM。
3. **Async 线程池过大** — 打爆 DB 连接池；与 `capacity`、连接池 `maxActive` 对齐。

---

## Step 5 面试话术

**问：维表很大且每天更新，实时关联怎么做？**

> 维表 50 万 SKU、日更，我不会 open() 全量加载也不会 broadcast 全表。  
> 订单流用 **Async I/O** 按 `productId` 查 MySQL/Redis，**unorderedWait** 提高吞吐；  
> 算子内 **Guava Cache**，TTL 设 5~15 分钟，把 DB QPS 控在连接池可承受范围。  
> 日更批次在凌晨，TTL 内偶发旧价可接受；大促临时改价会 **缩短 TTL** 或发 **广播流清 cache**。  
> 小字典表（科目、年级）仍 **open() 预加载**。  
> 若团队以 SQL 为主，等价方案是 **Lookup Join** + `lookup.cache.ttl`，原理相同。

**追问：Lookup Join cache 失效后，已关联的数据会变吗？**

> 不会回溯。Cache 只影响**之后**进入算子的订单；已写入 Kafka/OLAP 的宽表是当时的快照。要「改价影响已下单未支付」需业务层重新计价或版本号字段，不是 Flink cache 能单独解决的。

---

## 运行与验收

### 创建 Topic

```bash
kafka-topics.sh --create --topic test_flink_dim_join --partitions 2 \
  --bootstrap-server 192.168.1.124:9092
```

### 启动 Job

```bash
# 默认 cacheTtlSec=60, asyncCapacity=100
org.example.job.lookup.FlinkDimJoinDemoJob

# 调参：更短 TTL、更大 capacity
org.example.job.lookup.FlinkDimJoinDemoJob 30 200
```

### 发送测试数据

```bash
mvn test -Dtest=FlinkDimJoinDemoJobTest#sendDimJoinDemoEvents
# 或仅跑单测（无需 Kafka Job）
mvn test -Dtest=FlinkDimJoinDemoJobTest#guavaCache_hitAfterFirstMiss
```

### 预期日志

```
维表关联> [DIM-JOIN] orderId=O01 productId=P100 name=Java 零基础直播课 source=DB latencyMs=80 tag=cache-miss
维表关联> [DIM-JOIN] orderId=O03 productId=P100 name=Java 零基础直播课 source=CACHE latencyMs=0 tag=cache-hit
维表关联> [DIM-JOIN] orderId=O08 productId=P999 name=N/A source=MISS latencyMs=... tag=unknown-product
```

### 验收清单

| # | 验收项 | 验证方式 |
|---|--------|----------|
| ① | 手写 Async I/O 维表关联 | `ProductAsyncLookupFunction` + `unorderedWait` |
| ② | 能讲清四种方案取舍 | Step1 + Step3 对比表 |
| ③ | CACHE / DB / MISS 可观测 | Job 日志 `lookupSource` |
| ④ | cache TTL 权衡 | 单测 `guavaCache_ttlExpiresThenRefetchDb` + Step4 |

---

## Step 7 在线教育典型业务案例（维表 Join 三角）

> 以下三个场景是在线教育平台里 **维表 Join 最高发** 的业务，分别对应四种方案中的典型选型：  
> **小字典预加载** / **大 SKU Async+Cache** / **课程状态 Broadcast**。  
> 与《FlinkWatermarkDemoGuide》Step 7 互补：那边讲 WM 推进，这边讲 **事实流如何补全维度**。

---

### 案例一：科目/年级字典 — open() 预加载（方案①）

#### 业务背景

学习行为流只有 `subjectCode=MA01`，报表需要「数学」「小学三年级」等可读名称。科目字典 **约 200 行**，一年改几次，全集群一致。

#### 数据模型

```json
{"studentId":"S10001","subjectCode":"MA01","studySec":120,"ts":1717654321000}
```

#### 实现要点

```java
@Override
public void open(Configuration parameters) {
    subjectDict = loadFromMysql("SELECT code, name, grade FROM subject_dict");
}
// map 中 O(1) 查找，无需 Async
```

#### 三要素

| 维度 | 分析 |
|------|------|
| 维表大小 | 极小，每 subtask 几 KB |
| 更新频率 | 学年切换才改，可重启 Job 或日切 reload |
| 一致性 | 强一致可接受短暂重启 |

#### 踩坑

- 不要把 **50 万 SKU 课程表** 也塞进 open() — 和字典混淆是常见架构错误。

---

### 案例二：订单/购课流关联课程 SKU — Async I/O + Cache（方案②，本 Demo）

#### 业务背景

「实时成交额大屏」「渠道 ROI」需要订单流关联 **课程名称、品类、标价**。SKU 表 **50 万+**，每天凌晨 ERP 同步改价、上下架；订单峰值 **2k~5k/s**。

#### 数据模型

```json
{"orderId":"ORD001","productId":"P100","quantity":1,"amount":199.0,"ts":1717654321000}
```

#### 架构

```
Kafka(order) → Flink Async I/O + Guava(5min TTL) → MySQL/Redis(product_dim)
              → Kafka(enriched_order) → ClickHouse 大屏
```

#### 为什么用 Async 而非 Broadcast

| 维度 | 分析 |
|------|------|
| 维表大小 | 50 万行全 broadcast → 每 subtask 数百 MB，**OOM 风险** |
| 更新频率 | 日更批次，5min TTL 最终一致可接受 |
| 延迟 | 大屏 10~30s 延迟可接受；CACHE 命中 latency < 1ms |

**与 Demo 映射**：`P100`~`P104` = 课程 SKU；Phase1 `DB`、Phase2 `CACHE`、Phase4 `P999` = `MISS`。

#### 运维与告警

| 监控项 | 阈值建议 | 说明 |
|--------|----------|------|
| Async in-flight 数 | 持续顶满 capacity | 背压或 DB 慢 |
| DB lookup P99 | > 200ms | 索引/连接池 |
| CACHE 命中率 | < 80% | TTL 过短或 SKU 过于分散 |
| MISS 比例 | > 0.1% | 脏数据或维表未同步 |

#### 踩坑与心得

1. **大促改价**：临时 TTL 1min + 运维脚本 `invalidateAll` 或发控制流清 cache。
2. **已支付订单宽表不会随改价回溯** — 产品要区分「标价」与「实付 amount」。
3. **心得**：教育电商 SKU 是 **Async+Cache 的标准卷**：面试直接讲 QPS、TTL、连接池三角。

---

### 案例三：课程上下架状态 — Broadcast State（方案④）

#### 业务背景

「仅统计**已上架**课程的试听转化」：试听行为流需关联课程 `status=ON_SHELF/OFF_SHELF`。状态变更来自 **运营后台 Kafka**，一天数百次，维表 **约 5 万活跃课程**（可广播）。

#### 数据模型

```json
// 事实流
{"studentId":"S10001","courseId":"C200","eventType":"trial_start","ts":1717654321000}

// 维表广播流
{"courseId":"C200","status":"OFF_SHELF","updateTime":1717654300000}
```

#### 架构

```
Kafka(course_dim_changelog) ──broadcast──┐
                                          ├── BroadcastProcessFunction → 过滤 OFF_SHELF
Kafka(trial_event) ───────────────────────┘
```

#### 为什么用 Broadcast 而非 Async

| 维度 | 分析 |
|------|------|
| 更新频率 | 运营随时下架，需 **分钟级** 生效，纯 TTL cache 不够 |
| 维表大小 | 5 万行可广播；全量 50 万需 **只广播活跃课** 或改 Async |
| 逻辑 | 不仅是 enrich，还要 **按 status 过滤** |

#### OOM 防护

- 只广播 `status IN (ON_SHELF, OFF_SHELF)` 活跃 ID，历史课走冷存储。
- `MapStateDescriptor` 加 TTL 清理长期下架课。

#### 踩坑

- 维表更新乱序：以 `updateTime` 最大为准 merge。
- 与案例二混用：SKU 价格用 Async，**状态机**用 Broadcast — 同一 Job 可组合多方案。

---

### 三案例对照总表

| 案例 | 方案 | 维表规模 | 更新 | 与 Demo 对应 |
|------|------|----------|------|--------------|
| 科目字典 | ① 预加载 | ~200 行 | 年更 | 文档 Step1① |
| 购课订单关联 SKU | ② **Async+Cache** | 50 万+ | 日更 | **本 Demo 代码** |
| 课程上下架过滤 | ④ Broadcast | ~5 万活跃 | 实时 | 文档 Step1④ |

### 与 SQL Lookup Join 的对应

| DataStream 本 Demo | Table API 等价 |
|--------------------|----------------|
| `ProductAsyncLookupFunction` | `CREATE TABLE product_dim (...) WITH ('connector'='jdbc', 'lookup.cache.ttl'='5min')` |
| Guava `expireAfterWrite` | `lookup.cache.ttl` / `lookup.cache.max-rows` |
| `lookupSource=MISS` | LEFT JOIN 无匹配行 |

---

## 与 Watermark / 窗口 Demo 的关系

| 其他 Demo | 维表 Join Demo |
|-----------|----------------|
| 关心事件时间推进 | 关心事实流补全维度属性 |
| `WatermarkDemoEvent` | `OrderLookupEvent` → `EnrichedOrderEvent` |
| 窗口触发依赖 WM | Join 可在 window 前或后，本 Demo 在 **窗口前 enrich** |

典型链路：`Kafka 订单 → Async 维表 Join → keyBy → Tumbling 窗口聚合 GMV` — 先补全 `category` 再按品类 rollup。
