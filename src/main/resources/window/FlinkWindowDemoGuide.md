# Flink 窗口深度演示 — 学习指南

> 配套代码：`FlinkWindowDemoJob` + `FlinkWindowDemoJobTest`  
> 数据源：`(userId, ts, amount)`，Event Time + `WatermarkStrategy.forBoundedOutOfOrderness(5s)`  
> 本文 **Step 6** 基于一次真实运行日志编写（BASE=`1700000000000` → 本地时区显示 `06:13:20.000`）

---

## 读前扫盲：先把“窗口”想成一把按时间切数据的尺子

流式数据不是一次性放在表里等你 `GROUP BY`，而是一条条持续到达。窗口的作用，是把“无限流”临时切成一个个“有限小段”，让 Flink 可以在某个时间范围内做聚合，例如“每 5 秒成交额”“最近 10 秒热度”“一次学习会话时长”。

本篇只围绕三个基础问题展开：

| 先理解的问题 | 简单解释 | 本文对应位置 |
|-------------|----------|--------------|
| 按什么时间切？ | 本 Demo 使用 **Event Time**，也就是事件自己携带的 `ts` 字段，不是机器收到消息的时间 | `assignTimestampsAndWatermarks(...)` |
| 切成什么形状？ | Tumbling 不重叠，Sliding 会重叠，Session 由用户行为间隔动态决定边界 | Step 1 |
| 什么时候出结果？ | Event Time 窗口通常等 **Watermark 推过窗口结束时间** 后触发 | Step 1 / Step 6 |

三个窗口可以先这样建立直觉：

| 窗口类型 | 入门理解 | 适合场景 | 最容易踩的坑 |
|---------|----------|----------|--------------|
| Tumbling 滚动窗口 | 时间被切成一段段互不重叠的小格子 | 每分钟 PV、每日账单、固定周期报表 | 边界按 epoch 对齐，不一定按你测试数据的 BASE 对齐 |
| Sliding 滑动窗口 | 一个事件可能同时落入多个重叠窗口 | “最近 N 分钟”趋势、热度榜 | slide 越小，窗口副本越多，状态和输出都会膨胀 |
| Session 会话窗口 | 只要相邻事件间隔不超过 gap，就合并成同一段会话 | 用户访问会话、学习会话、断点续学 | 迟到数据可能把两段会话重新 merge |

读本文时建议先盯住一条测试数据：`u002` 在 `ts=+7s` 只有一条事件。它在 Tumbling 中只属于一个窗口，但在 `size=10s / slide=5s` 的 Sliding 中会属于两个窗口。理解这件事后，后面的“窗口数量膨胀”“为什么生产常用预聚合替代 Sliding”会更容易看懂。

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
窗口的边界不是预先固定的，而是随着数据的到来“动态生长”的，迟到数据甚至可以把两个已经“关窗”的会话重新合并成一个。
```

---

## Step 2 手写代码对照


| 要求                          | 实现位置                                               |
| --------------------------- | -------------------------------------------------- |
| 数据源 `(userId, ts, amount)`  | `UserOrderEvent`                                   |
| Tumbling 5s `sum(amount)`   | `TumblingEventTimeWindows.of(5s)`                  |
| Sliding size=10s / slide=5s | `SlidingEventTimeWindows.of(10s, 5s)`              |
| Session gap=5s              | `EventTimeSessionWindows.withGap(5s)`              |
| Watermark 乱序                | `forBoundedOutOfOrderness(5s)`                     |
| 增量聚合（陷阱③）                   | `AmountSumAggregator` + `WindowSumResultFormatter` |


---

## Step 3 场景 → 窗口映射表


| 业务场景            | 推荐窗口                          | 选型依据（延迟 / 完整性 / 成本）                  |
| --------------- | ----------------------------- | ------------------------------------ |
| **每分钟 PV 统计**   | Tumbling 1min                 | 延迟容忍 1min；需完整不重叠分钟桶；成本最低（每事件 1 个窗口）  |
| **近 5 分钟滑动热度榜** | Sliding size=5min slide=1min  | 需任意时刻「过去 5min」完整视图；允许 1min 更新延迟；成本较高 |
| **用户一次访问会话时长**  | Session gap=30min             | 会话边界由行为决定；需容忍乱序；状态随活跃 session 数增长    |
| **按自然天对账**      | Tumbling 1day + **时区 offset** | 必须对齐业务时区日界；完整性要求 100%；Tumbling 即可    |


> 在线教育行业三个最典型落地案例（含生产运维心得）见 **Step 7**。

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


| userId | 相对偏移 | 绝对 ts    | amount | 场景                |
| ------ | ---- | -------- | ------ | ----------------- |
| u001   | +1s  | 06:13:21 | 10     | A-Tumbling        |
| u001   | +3s  | 06:13:23 | 20     | A-Tumbling        |
| u001   | +4s  | 06:13:24 | 30     | A-Tumbling        |
| u001   | +6s  | 06:13:26 | 40     | A-Tumbling        |
| u001   | +8s  | 06:13:28 | 50     | A-Tumbling        |
| u002   | +7s  | 06:13:27 | 100    | B-Sliding（单条）     |
| u003   | +1s  | 06:13:21 | 10     | C-Session         |
| u003   | +2s  | 06:13:22 | 20     | C-Session         |
| u003   | +10s | 06:13:30 | 30     | C-Session 段2      |
| u003   | +11s | 06:13:31 | 40     | C-Session 段2      |
| u003   | +6s  | 06:13:26 | 5      | C-Session **迟到**  |
| u004   | +30s | 06:13:50 | 1      | D-Flush watermark |


### 6.2 Tumbling 5s — 实测输出


| userId | 窗口 `[start ~ end)`  | count | sum | 解读                  |
| ------ | ------------------- | ----- | --- | ------------------- |
| u001   | 06:13:20 ~ 06:13:25 | 3     | 60  | +1,+3,+4 → 10+20+30 |
| u001   | 06:13:25 ~ 06:13:30 | 2     | 90  | +6,+8 → 40+50       |
| u002   | 06:13:25 ~ 06:13:30 | 1     | 100 | +7s 仅落 1 个 5s 桶     |
| u003   | 06:13:20 ~ 06:13:25 | 2     | 30  | +1,+2               |
| u003   | 06:13:25 ~ 06:13:30 | 1     | 5   | 迟到 +6 **单独成桶**      |
| u003   | 06:13:30 ~ 06:13:35 | 2     | 70  | +10,+11             |


**结论**：u001 共 **2 条** Tumbling 输出；迟到数据在 Tumbling 下 **不会 merge**。

### 6.3 Sliding 10s/5s — 实测输出（重点 u002 验收②）


| userId | 窗口 `[start ~ end)`  | count | sum | 解读           |
| ------ | ------------------- | ----- | --- | ------------ |
| u002   | 06:13:20 ~ 06:13:30 | 1     | 100 | SW1          |
| u002   | 06:13:25 ~ 06:13:35 | 1     | 100 | SW2          |
| u001   | 06:13:15 ~ 06:13:25 | 3     | 60  | 先到 3 条时的部分窗口 |
| u001   | 06:13:20 ~ 06:13:30 | 5     | 150 | 5 条全到后的完整窗口  |
| u001   | 06:13:25 ~ 06:13:35 | 2     | 90  | +6,+8 所在后半段  |


**结论**：u002 仅 1 条输入 → **2 条** Sliding 输出，证明 `size10/slide5` 下单事件双归属。  
u001 共 **3 条** Sliding 输出 > Tumbling **2 条** → 窗口重叠导致条数膨胀。

### 6.4 Session gap=5s — 实测输出（重点 u003 merge）


| userId | 窗口 `[start ~ end)`  | count | sum | 解读                           |
| ------ | ------------------- | ----- | --- | ---------------------------- |
| u002   | 06:13:27 ~ 06:13:32 | 1     | 100 | 单事件会话，上界=27+5                |
| u001   | 06:13:21 ~ 06:13:33 | 5     | 150 | 5 条连续 gap 均 <5s，合成 1 个会话     |
| u003   | 06:13:21 ~ 06:13:36 | 5     | 105 | **merge 后** 1+2+6+10+11 全部合并 |


**结论**：u003 最终 **1 条** Session `sum=105`（10+20+30+40+5），验收③通过。  
对比 Tumbling 下 u003 被切成 **3 条**，直观展示 Session 与 Tumbling 语义差异。

### 6.5 三种窗口输出条数对比（本次运行）


| userId | 输入条数 | Tumbling 输出 | Sliding 输出  | Session 输出        |
| ------ | ---- | ----------- | ----------- | ----------------- |
| u001   | 5    | **2**       | **3**       | **1**             |
| u002   | 1    | **1**       | **2** ← 验收② | **1**             |
| u003   | 5    | **3**       | **4**       | **1** ← merge 验收③ |


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

## Step 7 在线教育典型业务案例（三种窗口各一）

> 以下三个场景是在线教育平台里**出现频率最高、最有代表性**的窗口应用，分别对应 Tumbling / Sliding / Session。  
> 选型逻辑统一用 **延迟容忍度 / 数据完整性 / 计算成本** 三要素衡量。

---

### 案例一：Tumbling — 学员「每日有效学习时长」日报 & 家校对账

#### 业务背景

K12 / 职业培训平台需按**自然天**统计每位学员的有效学习时长（观看录播、完成练习、直播出勤等），生成：

- 学员端「今日已学 45 分钟」
- 家长端日报推送
- 财务/运营按**自然日**与第三方渠道（学校、代理商）对账结算

#### 数据模型

```json
{"studentId":"S10001","courseId":"C200","eventType":"video_heartbeat",
 "durationSec":30,"ts":1717654321000}
```

- `keyBy(studentId)` 或 `keyBy(studentId, courseId)`
- 聚合：`sum(durationSec)` 或 `sum(validSeconds)`（需先过滤挂机、倍速作弊）

#### 窗口配置

```java
// 北京时间 00:00 切日，与财务对账口径一致
WatermarkStrategy.<StudyEvent>forBoundedOutOfOrderness(Duration.ofMinutes(2))
    .withTimestampAssigner((e, ts) -> e.getTs());

stream.keyBy(StudyEvent::getStudentId)
    .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8))) // UTC+8
    .aggregate(new StudyDurationAggregator());
```

#### 为什么选 Tumbling（三要素）


| 维度        | 分析                                                          |
| --------- | ----------------------------------------------------------- |
| **延迟容忍度** | 日报 T+0 或 T+1 即可，可接受 **日级延迟**（通常等 watermark 越过当日 24:00 后再出数） |
| **数据完整性** | 必须 **按自然天完整切桶、互不重叠**；不能「近 24h 滑动加总」——否则同一学员一天会被算进两个自然日      |
| **计算成本**  | 每心跳事件只写 **1 个日桶**，状态量 = 活跃学员数 × 1，最低                        |


**不选 Sliding 的原因**：Sliding 24h/1h 会导致同一学习行为同时落入多个重叠窗口，与「按天对账」语义冲突。  
**不选 Session 的原因**：Session 边界由行为间隔决定，无法对齐「北京时间 0 点」这一财务口径。

#### 生产架构要点

```
Kafka(学习行为) → Flink Tumbling 1day → ClickHouse/Doris 日表
                                      → 定时任务 00:15 推送给家长（等 watermark 稳定）
```

- 心跳事件量大（每 30s 一条），**必须用 aggregate** 累加秒数，禁止 process 缓存全天心跳
- 日切边界 Job 重启时依赖 **Checkpoint + 状态恢复**，不可只用 `setStartFromLatest()`
- 与 Demo 类比：类似 u001 的 Tumbling，但窗口从 5s 放大到 1day，且需 **时区 offset**

#### 运维与告警


| 监控项                        | 阈值建议         | 说明                 |
| -------------------------- | ------------ | ------------------ |
| 当前 watermark 滞后 wall-clock | > 10min 告警   | 上游 Kafka 延迟或消费积压   |
| 日切后 1h 内完成率                | < 99% 学员出数告警 | 可能有大量迟到数据          |
| 单 key 状态大小                 | 异常膨胀         | 检查是否误用 process 存明细 |
| Checkpoint 时长              | > 3min 告警    | 日窗口状态 + 大并行度时常见    |


#### 踩坑与心得

1. **时区是第一大坑**：epoch 对齐默认 UTC，`offset` 必须和业务、财务、BI 口径一致；大促前后要专门回归「23:59 学习算哪天」。
2. **迟到数据策略**：日报场景可设 `allowedLateness = 2~4h`，迟到心跳修正当日时长；超过则进 **侧输出流** 写补录表，人工/离线修正。
3. **心跳 vs 有效时长**：Tumbling 只负责「加总」，有效判定（倍速>2x 不计、后台播放不计）应在前置 Filter 完成，否则窗口层无法补救。
4. **日切尖峰**：00:00~00:05 大量窗口同时触发，Sink 易被打满；生产常用 **批量 JDBC + 异步 Sink** 或先写 Kafka 再 OLAP 消费。
5. **心得**：教育行业「按天」诉求极多（学习报告、打卡、续费提醒），**Tumbling + 时区** 是标配；先把口径写进 PRD，再写代码。

---

### 案例二：Sliding — 直播课「近 5 分钟在线人数 & 互动热度」实时大屏

#### 业务背景

双师直播 / 大班课中，主讲和班主任需要实时看到：

- **当前时刻往前推 5 分钟**的在线人数曲线（不是「本分钟」的瞬时值）
- 近 5 分钟弹幕数、举手数、答题参与率排名（互动热度榜）

大屏要求：**任意时刻**问「过去 5 分钟有多少人」，都要能答，且每 **30s~1min** 刷新一次。

#### 数据模型

```json
{"liveRoomId":"L888","studentId":"S10001","eventType":"enter|heartbeat|leave",
 "ts":1717654321000}
```

- `keyBy(liveRoomId)` 统计房间级在线；`keyBy(liveRoomId, studentId)` 去重后 count 近似 UV
- 聚合：`countDistinct(studentId)` 或 HyperLogLog 近似去重

#### 窗口配置

```java
// 近 5 分钟在线：size=5min, slide=30s → 每 30s 刷新一次「过去 5min」视图
stream.keyBy(LiveEvent::getLiveRoomId)
    .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.seconds(30)))
    .aggregate(new OnlineCountAggregator());
```

#### 为什么选 Sliding（三要素）


| 维度        | 分析                                                                   |
| --------- | -------------------------------------------------------------------- |
| **延迟容忍度** | 大屏 **30s~1min 刷新**可接受，不需要秒级                                          |
| **数据完整性** | 必须任意时刻都有「完整 5 分钟窗口」视图；Tumbling 1min 只能看「上一完整分钟」，无法表达「14:03 时过去 5 分钟」 |
| **计算成本**  | **偏高**——size/slide = 10，每条心跳最多进 10 个窗口；晚高峰 10 万人在线时状态压力显著            |


**不选 Tumbling 的原因**：1min Tumbling 只能得到「14:00~14:01 进了多少人」，无法滚动回答「此刻往前 5 分钟」。  
**不选 Session 的原因**：在线人数不是「一次会话」语义，而是固定回看长度。

#### 生产架构要点

```
Kafka(进出房/心跳) → Flink Sliding 5min/30s → Redis(大屏轮询) / WebSocket 推送
                                              ↘ 超阈值 → 钉钉「人数异常下跌」
```

- 与 Demo 类比：同 u002，`size=10/slide=5` 时 1 条进 2 窗；此处 `size=5min/slide=30s` 时 1 条最多进 **10 窗**
- **生产常见降本**：Flink 只做 **Tumbling 30s 预聚合**（每 30s 一条在线快照），大屏查询时 **Redis/ClickHouse sum 最近 10 个桶** ≈ 近 5 分钟——即文档「加分点」方案

#### 运维与告警


| 监控项                | 阈值建议        | 说明                                      |
| ------------------ | ----------- | --------------------------------------- |
| 窗口状态总量             | 较基线 +50% 告警 | slide 调小或直播场次增多时膨胀                      |
| Checkpoint 大小 / 时长 | 持续上升        | Sliding 是状态膨胀重灾区                        |
| 单直播间 QPS           | 超设计容量       | 热门直播间 key 热点，考虑 keyBy 后 rebalance 或本地聚合 |
| Watermark 滞后       | > 1min      | 大屏数据「假死」                                |


#### 踩坑与心得

1. **Sliding 资源陷阱**：晚 8 点开课，10 万心跳/秒 × 10 窗口副本 = 百万级状态更新/秒，**必做容量评估**；能不用 Sliding 就不用。
2. **去重语义**：「在线人数」要在窗口内对 studentId 去重；用 `AggregateFunction` 维护 HyperLogLog 或 RoaringBitmap，不要 `List` 存全量 id。
3. **leave 事件迟到**：学生切后台 leave 事件可能延迟 30s，在线人数会 **虚高**；可结合 heartbeat 超时（45s 无心跳视为离开）在窗口外做状态清理。
4. **大屏刷新 vs slide**：slide 不必小于刷新间隔；slide=30s、前端 60s 轮询足够，再小只会烧资源。
5. **心得**：Sliding 适合「演示效果好、量不大」的直播监控；**量一大就改 Tumbling 预聚合 + 查询层滑动**，这是教育直播团队的常见演进路径。

---

### 案例三：Session — 单次「学习会话」时长 & 断点续学归因

#### 业务背景

自适应学习 / AI 课需要知道学员**一次连续学习**持续了多久、做了几道题、在哪个知识点 dropout：

- 产品：「您本次学习 23 分钟，完成 2 个章节」
- 算法：会话时长 < 3min 标记为「浅尝辄止」，触发挽留 Push
- 运营：分析「打开 App → 离开」的完整路径，优化课程内容长度

「一次学习」的定义：**相邻行为间隔 < 30min** 算同一会话（中间喝水、查字典不算新会话）；超过 30min 视为新会话。

#### 数据模型

```json
{"studentId":"S10001","eventType":"page_view|answer|video_play",
 "knowledgePointId":"KP99","ts":1717654321000}
```

- `keyBy(studentId)` 或 `keyBy(studentId, deviceId)`
- 聚合：`count` 事件数、`sum(studySec)`、`max(knowledgePointId)` 等会话摘要

#### 窗口配置

```java
// gap=30min：教育场景常见「一次学习会话」阈值
stream.keyBy(StudyEvent::getStudentId)
    .window(EventTimeSessionWindows.withGap(Time.minutes(30)))
    .aggregate(new SessionSummaryAggregator(), new SessionSummaryFormatter());
```

#### 为什么选 Session（三要素）


| 维度        | 分析                                                              |
| --------- | --------------------------------------------------------------- |
| **延迟容忍度** | 会话结束后再出数即可（gap 30min 无新事件 + watermark 推进），**分钟~小时级**可接受         |
| **数据完整性** | 必须按**用户行为节奏**切分，不能用固定 30min Tumbling（14:00 和 14:29 的学习可能被硬切成两段） |
| **计算成本**  | **中等偏高**——活跃 session 数 × merge 开销；长会话 + 乱序时 merge 频繁            |


**不选 Tumbling 的原因**：固定 30min 桶无法反映「用户 14:05 开始、14:50 结束」这一完整 45min 会话。  
**不选 Sliding 的原因**：会话长度不固定，Sliding 无法表达「从打开到离开」的语义。

#### 生产架构要点

```
Kafka(学习行为) → Flink Session gap=30min → Kafka(session_summary) → 推荐/Push/BI
                                         ↘ 侧输出：merge 次数过多 → 数据质量监控
```

- 与 Demo 类比：同 u003，gap 从 5s 放大到 30min；迟到事件仍可能 **merge** 两个本已闭合的 session
- 会话结果写 **Upsert 表**（sessionId, start, end, duration, kpCount），供下游 AI 打标

#### 运维与告警


| 监控项               | 阈值建议      | 说明                   |
| ----------------- | --------- | -------------------- |
| Session merge 频率  | 较基线 +100% | 乱序恶化或 gap 过小         |
| 单用户 session 时长    | > 4h 告警   | 挂机刷时长、gap 过大未切分      |
| 状态条目数（活跃 session） | 持续增长不下降   | 检查 watermark 是否正常推进  |
| 迟到数据丢弃量           | > 0.1%    | Side Output 应接住可补救数据 |


#### 踩坑与心得

1. **gap 怎么定**：太短（5min）→ 上厕所被切成两段，时长碎片化；太长（2h）→ 挂机算学习。教育产品通常 **20~30min**，需 A/B 与产品一起定。
2. **merge 是双刃剑**：Demo 中 u003 迟到 +6s merge 两段；生产里若 gap=30min 仍 merge，说明 **乱序严重或 gap 偏小**，会导致会话时长 **被拉长**、Push 时机推迟。要监控 `MergingWindowSet` 日志频率。
3. **Session 不能简单用 Tumbling 替代**：「每日 30min 学习提醒」是 Tumbling 日桶；「本次学了多久」是 Session，**两个指标并存、不可混用**。
4. **sessionId 生成**：窗口触发后下游需要稳定 sessionId，常用 `hash(studentId + windowStart)`，merge 后 start 会变，**必须在 merge 完成后**再发下游或做 Upsert。
5. **allowedLateness**：建议 1~2h，迟到心跳可修正会话结束时间和时长；与 Tumbling 日报共用同一 Kafka 源时，**Session 分支的 watermark 策略要单独评估**。
6. **心得**：Session 最贴「学习行为心理学」，但运维复杂度最高；上线前用 **回放一周生产日志** 估算 merge 率、P99 会话时长，再定 gap。

---

### 三案例对照总表


| 案例          | 窗口                    | 典型指标             | 延迟       | 状态成本  | 最大运维风险                 |
| ----------- | --------------------- | ---------------- | -------- | ----- | ---------------------- |
| 每日学习时长 / 对账 | **Tumbling 1day+时区**  | sum(有效秒数)        | 日级       | 低     | 时区与日切口径不一致             |
| 直播近 5min 在线 | **Sliding 5min/30s**  | countDistinct(人) | 30s~1min | **高** | 晚高峰状态膨胀、Checkpoint 慢   |
| 单次学习会话      | **Session gap=30min** | 会话时长、KP 数        | gap 后触发  | 中~高   | merge 异常、sessionId 不一致 |


### 与本地 Demo 的映射


| Demo                       | 教育案例                                      |
| -------------------------- | ----------------------------------------- |
| u001 Tumbling 5s，2 个桶      | → 放大为 **Tumbling 1day**，学员每日时长 1 条        |
| u002 Sliding 1 条 → 2 窗     | → 放大为 **Sliding 5min**，每条心跳进多窗            |
| u003 Session merge sum=105 | → 放大为 **Session 30min gap**，迟到心跳 merge 会话 |


---

## 加分点：为什么生产常用 Tumbling + 预聚合替代 Sliding？


| Sliding 痛点（实测 u001）      | Tumbling + 预聚合方案             |
| ------------------------ | ---------------------------- |
| 5 条输入产生 3 条重叠 Sliding 结果 | 每事件只写 1 个 5s/1min Tumbling 桶 |
| 状态量 ∝ 时间跨度/slide         | 查询层 sum 最近 N 个桶 ≈ 近 N 分钟热度   |
| checkpoint 大             | 状态可控，易扩缩容                    |


本质：**用查询侧合并换写入侧去重**，以可接受分钟级延迟换数倍状态成本下降。

---

## 验收清单（对照实测日志）


| #   | 验收项                          | 实测结果                                                    |
| --- | ---------------------------- | ------------------------------------------------------- |
| ①   | 三种窗口代码 + 解释输出条数差异            | u001：Tumbling 2 条 / Sliding 3 条 / Session 1 条 ✅         |
| ②   | +7s 单条在 size10/slide5 属 2 窗口 | u002 Sliding 2 条，sum 均为 100 ✅                           |
| ③   | 讲清 Session merge             | u003 最终 1 条 `[21~36) sum=105`；Tumbling 同数据 3 条不 merge ✅ |


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
