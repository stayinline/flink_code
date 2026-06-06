# Flink 窗口深度演示 — 学习指南

> 配套代码：`FlinkWindowDemoJob` + `FlinkWindowDemoJobTest`  
> 数据源：`(userId, ts, amount)`，Event Time + `WatermarkStrategy.forBoundedOutOfOrderness(5s)`  
> 本文 **Step 6** 基于一次真实运行日志编写（BASE=`1700000000000` → 本地时区显示 `06:13:20.000`）

---

## Step 1 原理：三种窗口时间轴（左闭右开 `[start, end)`）

> **对齐规则**：Flink EventTime 窗口默认按 **Unix epoch** 对齐，不是按测试 BASE 对齐。  
> 本 Demo 的 `BASE_TIME_MS = 1700000000000` 恰好落在 5s 整倍数上，因此窗口边界显示为 `06:13:20 / 06:13:25 / 06:13:30 …`。

### 1. Tumbling 滚动窗口（size = 5s）

```
绝对时间轴（本次运行）:
  06:13:20      06:13:25      06:13:30      06:13:35
  |---- W0 ----|---- W1 ----|---- W2 ----|---- W3 ----|
  [20 ~ 25)     [25 ~ 30)     [30 ~ 35)     [35 ~ 40)

相对 BASE 偏移:  +0~+5s        +5~+10s       +10~+15s

u001 事件落点:
  +1s● +3s● +4s●  → W0，sum=60
              +6s● +8s● → W1，sum=90

归属规则: ts ∈ [start, end)；窗口互不重叠；每条数据只属于 1 个 Tumbling 窗口
触发边界: Watermark ≥ window.end 时触发（本 Demo 允许 5s 乱序）
```

### 2. Sliding 滑动窗口（size = 10s, slide = 5s）

```
绝对时间轴（本次运行）:
  06:13:15      06:13:20      06:13:25      06:13:30      06:13:35
  |------ SW0 --------|
       |------ SW1 --------|
            |------ SW2 --------|
                 |------ SW3 --------|

SW0 [15~25)  SW1 [20~30)  SW2 [25~35)  SW3 [30~40)

u002 仅 1 条 ts=+7s（06:13:27）:
  ● 落入 SW1 [20~30)  ✓
  ● 落入 SW2 [25~35)  ✓
  → SLIDING 输出 2 条（TUMBLING 同 ts 仅 1 条）← 验收②

公式: start = k × slide，满足 start ≤ ts < start + size 的所有窗口
本例 size/slide=2 → 单条数据最多进入 2 个窗口
```

### 3. Session 会话窗口（gap = 5s）

```
u003 事件（按 event time 排序）:
  +1s(10) +2s(20)  ...间隔 8s...  +10s(30) +11s(40)  迟到 +6s(5)

初判（无迟到）:
  会话A: +1,+2        → 结束于 max(ts)+gap = +7s
  会话B: +10,+11      → [10, 16)

迟到 +6s 到达后（6 与 +2 差 4s < gap，6 与 +10 差 4s < gap）:
  会话A + 桥接 +6 + 会话B  → merge 为 [+1, +16) 即 [06:13:21 ~ 06:13:36)
  最终: count=5, sum=105

触发边界: gap 内无新事件且 Watermark 越过会话上界；迟到数据可触发 MergingWindowSet
```

---

## Step 2 手写代码对照

| 要求 | 实现位置 |
|------|----------|
| 数据源 `(userId, ts, amount)` | `UserOrderEvent` |
| Tumbling 5s `sum(amount)` | `TumblingEventTimeWindows.of(5s)` |
| Sliding size=10s / slide=5s | `SlidingEventTimeWindows.of(10s, 5s)` |
| Session gap=5s | `EventTimeSessionWindows.withGap(5s)` |
| Watermark 乱序 | `forBoundedOutOfOrderness(5s)` |
| 增量聚合（陷阱③） | `AmountSumAggregator` + `WindowSumResultFormatter` |

---

## Step 3 场景 → 窗口映射表

| 业务场景 | 推荐窗口 | 选型依据（延迟 / 完整性 / 成本） |
|----------|----------|----------------------------------|
| **每分钟 PV 统计** | Tumbling 1min | 延迟容忍 1min；需完整不重叠分钟桶；成本最低（每事件 1 个窗口） |
| **近 5 分钟滑动热度榜** | Sliding size=5min slide=1min | 需任意时刻「过去 5min」完整视图；允许 1min 更新延迟；成本较高 |
| **用户一次访问会话时长** | Session gap=30min | 会话边界由行为决定；需容忍乱序；状态随活跃 session 数增长 |
| **按自然天对账** | Tumbling 1day + **时区 offset** | 必须对齐业务时区日界；完整性要求 100%；Tumbling 即可 |

### 自然天对账代码示例（Asia/Shanghai UTC+8）

```java
TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8));
// Table API: TUMBLE(ts, INTERVAL '1' DAY, INTERVAL '8' HOUR)
```

> **三要素选型口诀**：固定周期报表 → Tumbling；任意时刻回看 N 分钟 → Sliding；行为驱动分段 → Session。

---

## Step 4 陷阱 / 调优

### ① Sliding 窗口数量膨胀

- 单条数据最多进入 `size/slide = 2` 个窗口（本 Demo 已验证 u002）
- 同一 userId 在 10s 时间跨度内，Sliding 输出 **多于** Tumbling（u001：Tumbling 2 条 vs Sliding 3 条）
- **slide 越小**，horizon 内窗口副本越多 → `FlinkWindowDemoJobTest.slidingWindowCount_inflatesWhenSlideShrinks()`

### ② Session 窗口 merge 机制

- 迟到 `+6s` 把 `[+1,+2]` 与 `[+10,+11]` 两段桥接 → 最终 **1 条** `[06:13:21 ~ 06:13:36) sum=105`
- 对比 Tumbling：同一迟到 `+6s` 只进 `[25~30) sum=5`，**不会**与前后桶合并 → 体现 Session vs Tumbling 本质差异
- 若 DEBUG 开启，可见 `MergingWindowSet - Merging [...] into [...]`

### ③ 大窗口用 aggregate 而非 process 全量缓存

```java
// ✅ 本 Demo 用法：状态仅存 sum/count
.window(...).aggregate(new AmountSumAggregator(), new WindowSumResultFormatter(...))

// ❌ 避免：process 内 Iterable 全量迭代，内存 O(窗口内元素数)
```

### ④ 输出顺序是交错的（读日志时注意）

三种窗口是 **三条独立分支**，各自 watermark 推进节奏不同，因此日志中 `Tumbling` / `Sliding` / `Session` **交替出现** 是正常现象，不要按 print 顺序推断触发先后。

### ⑤ Sliding 可能先出「部分结果」

u001 在 `+4s` 之后 watermark 推进，Sliding `[15~25)` 先输出 `count=3 sum=60`；待 `+6s、+8s` 到达后，`[20~30)` 才输出完整 `count=5 sum=150`。  
说明：**同一窗口在 watermark 推进后输出快照**；若后续仍有同窗口的迟到数据（在 allowedLateness 内），还可能再触发——本 Demo 未开 allowedLateness，故不会出现二次更新。

---

## Step 5 面试话术（约 150 字）

> 选窗口看三个维度：**延迟容忍度、数据完整性、计算成本**。固定周期指标（如每分钟 PV）用 **Tumbling**，边界清晰、状态最少；需要「任意时刻过去 N 分钟」完整视图（如实时热度榜）用 **Sliding**，但一条数据会进入多个重叠窗口，**slide 越小状态副本越多、资源消耗越大**；用户行为分段（如访问会话）用 **Session**，窗口长度动态、需理解 **merge** 与迟到数据。生产里高频 Sliding 常改为 **Tumbling + 多粒度预聚合**（如 1min 桶再 sum）降本。触发边界：**Watermark ≥ 窗口 end**（Event Time 下）。

---

## Step 6 实际运行日志解读（实测）

### 6.1 输入事件一览

| userId | 相对偏移 | 绝对 ts | amount | 场景 |
|--------|----------|---------|--------|------|
| u001 | +1s | 06:13:21 | 10 | A-Tumbling |
| u001 | +3s | 06:13:23 | 20 | A-Tumbling |
| u001 | +4s | 06:13:24 | 30 | A-Tumbling |
| u001 | +6s | 06:13:26 | 40 | A-Tumbling |
| u001 | +8s | 06:13:28 | 50 | A-Tumbling |
| u002 | +7s | 06:13:27 | 100 | B-Sliding（单条） |
| u003 | +1s | 06:13:21 | 10 | C-Session |
| u003 | +2s | 06:13:22 | 20 | C-Session |
| u003 | +10s | 06:13:30 | 30 | C-Session 段2 |
| u003 | +11s | 06:13:31 | 40 | C-Session 段2 |
| u003 | +6s | 06:13:26 | 5 | C-Session **迟到** |
| u004 | +30s | 06:13:50 | 1 | D-Flush watermark |

### 6.2 Tumbling 5s — 实测输出

| userId | 窗口 `[start ~ end)` | count | sum | 解读 |
|--------|----------------------|-------|-----|------|
| u001 | 06:13:20 ~ 06:13:25 | 3 | 60 | +1,+3,+4 → 10+20+30 |
| u001 | 06:13:25 ~ 06:13:30 | 2 | 90 | +6,+8 → 40+50 |
| u002 | 06:13:25 ~ 06:13:30 | 1 | 100 | +7s 仅落 1 个 5s 桶 |
| u003 | 06:13:20 ~ 06:13:25 | 2 | 30 | +1,+2 |
| u003 | 06:13:25 ~ 06:13:30 | 1 | 5 | 迟到 +6 **单独成桶** |
| u003 | 06:13:30 ~ 06:13:35 | 2 | 70 | +10,+11 |

**结论**：u001 共 **2 条** Tumbling 输出；迟到数据在 Tumbling 下 **不会 merge**。

### 6.3 Sliding 10s/5s — 实测输出（重点 u002 验收②）

| userId | 窗口 `[start ~ end)` | count | sum | 解读 |
|--------|----------------------|-------|-----|------|
| u002 | 06:13:20 ~ 06:13:30 | 1 | 100 | SW1 |
| u002 | 06:13:25 ~ 06:13:35 | 1 | 100 | SW2 |
| u001 | 06:13:15 ~ 06:13:25 | 3 | 60 | 先到 3 条时的部分窗口 |
| u001 | 06:13:20 ~ 06:13:30 | 5 | 150 | 5 条全到后的完整窗口 |
| u001 | 06:13:25 ~ 06:13:35 | 2 | 90 | +6,+8 所在后半段 |

**结论**：u002 仅 1 条输入 → **2 条** Sliding 输出，证明 `size10/slide5` 下单事件双归属。  
u001 共 **3 条** Sliding 输出 > Tumbling **2 条** → 窗口重叠导致条数膨胀。

### 6.4 Session gap=5s — 实测输出（重点 u003 merge）

| userId | 窗口 `[start ~ end)` | count | sum | 解读 |
|--------|----------------------|-------|-----|------|
| u002 | 06:13:27 ~ 06:13:32 | 1 | 100 | 单事件会话，上界=27+5 |
| u001 | 06:13:21 ~ 06:13:33 | 5 | 150 | 5 条连续 gap 均 <5s，合成 1 个会话 |
| u003 | 06:13:21 ~ 06:13:36 | 5 | 105 | **merge 后** 1+2+6+10+11 全部合并 |

**结论**：u003 最终 **1 条** Session `sum=105`（10+20+30+40+5），验收③通过。  
对比 Tumbling 下 u003 被切成 **3 条**，直观展示 Session 与 Tumbling 语义差异。

### 6.5 三种窗口输出条数对比（本次运行）

| userId | 输入条数 | Tumbling 输出 | Sliding 输出 | Session 输出 |
|--------|----------|---------------|--------------|--------------|
| u001 | 5 | **2** | **3** | **1** |
| u002 | 1 | **1** | **2** ← 验收② | **1** |
| u003 | 5 | **3** | **4** | **1** ← merge 验收③ |

### 6.6 关键日志原文对照

**验收② — u002 双窗口（Sliding）**

```
Sliding-10s-5s> [SLIDING] userId=u002 | 窗口=[06:13:20.000 ~ 06:13:30.000) ... count=1 sum=100.0
Sliding-10s-5s> [SLIDING] userId=u002 | 窗口=[06:13:25.000 ~ 06:13:35.000) ... count=1 sum=100.0
```

**验收③ — u003 Session merge**

```
Session-gap-5s> [SESSION] userId=u003 | 窗口=[06:13:21.000 ~ 06:13:36.000) ... count=5 sum=105.0
```

**对比 — u003 迟到 +6s 在 Tumbling 中单独成桶**

```
Tumbling-5s> [TUMBLING] userId=u003 | 窗口=[06:13:25.000 ~ 06:13:30.000) ... count=1 sum=5.0
```

**陷阱⑤ — u001 Sliding 先出部分结果**

```
Sliding-10s-5s> [SLIDING] userId=u001 | 窗口=[06:13:15.000 ~ 06:13:25.000) ... count=3 sum=60.0
...（+6s、+8s 到达后）
Sliding-10s-5s> [SLIDING] userId=u001 | 窗口=[06:13:20.000 ~ 06:13:30.000) ... count=5 sum=150.0
```

### 6.7 日志中的 WARN（可忽略）

```
MetricGroup - The operator name Window(...AmountSumAggregator, WindowSumResultFormatter) exceeded the 80 characters length limit
```

算子链名称过长被截断，**不影响计算结果**。若需消除，可在窗口算子后加 `.name("SlidingSum")` 缩短名称。

---

## 加分点：为什么生产常用 Tumbling + 预聚合替代 Sliding？

| Sliding 痛点（实测 u001） | Tumbling + 预聚合方案 |
|---------------------------|----------------------|
| 5 条输入产生 3 条重叠 Sliding 结果 | 每事件只写 1 个 5s/1min Tumbling 桶 |
| 状态量 ∝ 时间跨度/slide | 查询层 sum 最近 N 个桶 ≈ 近 N 分钟热度 |
| checkpoint 大 | 状态可控，易扩缩容 |

本质：**用查询侧合并换写入侧去重**，以可接受分钟级延迟换数倍状态成本下降。

---

## 验收清单（对照实测日志）

| # | 验收项 | 实测结果 |
|---|--------|----------|
| ① | 三种窗口代码 + 解释输出条数差异 | u001：Tumbling 2 条 / Sliding 3 条 / Session 1 条 ✅ |
| ② | +7s 单条在 size10/slide5 属 2 窗口 | u002 Sliding 2 条，sum 均为 100 ✅ |
| ③ | 讲清 Session merge | u003 最终 1 条 `[21~36) sum=105`；Tumbling 同数据 3 条不 merge ✅ |

---

## 运行步骤

```bash
# 1. 先启动 Job（消费 latest offset）
org.example.job.window.FlinkWindowDemoJob

# 2. 再发送分场景测试数据
mvn test -Dtest=FlinkWindowDemoJobTest#sendWindowTestEvents

# 3. 本地纯计算验证（无需 Kafka）
mvn test -Dtest=FlinkWindowDemoJobTest#slidingWindowMembership_at7s_belongsToTwoWindows
```

### 读日志建议顺序

1. 先看 `原始事件>` 确认输入  
2. 按 **userId 分组** 对照 §6.2 / §6.3 / §6.4 表格  
3. 重点看 **u002**（Sliding 双窗口）和 **u003**（Session merge vs Tumbling 不 merge）  
4. 忽略 MetricGroup / WebMonitor 等 WARN
