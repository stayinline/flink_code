# Flink Trigger 与 Evictor — 学习指南

> 配套代码：`FlinkTriggerEvictorDemoJob` + `FlinkTriggerEvictorDemoJobTest`  
> 数据源：复用 `WatermarkDemoEvent` `(eventId, userId, ts, amount, source, tag)`  
> 窗口：**Tumbling 5min** + 自定义 `CountOrTimeTrigger(100)`

---

## Step 1 原理：Trigger 决定「何时 FIRE」，Evictor 决定「算哪些元素」

### ① 四种 TriggerResult

| 返回值 | 含义 | 窗口元素 | 是否触发 Process/Aggregate |
|--------|------|----------|---------------------------|
| **CONTINUE** | 继续攒数据 | 保留 | 否 |
| **FIRE** | 触发一次计算 | **保留** | 是 |
| **PURGE** | 清空窗口状态 | 清空 | 否 |
| **FIRE_AND_PURGE** | 触发并清空 | 清空 | 是（最后一次） |

```
onElement / onEventTime / onProcessingTime
        │
        ▼
  ┌─────────────┐
  │ TriggerResult│
  └─────────────┘
        │
   CONTINUE ──→ 什么都不做，继续等
   FIRE      ──→ 调用窗口函数输出一次，元素仍在窗口里（可再次 FIRE）
   PURGE     ──→ 丢弃元素，不输出
   FIRE_AND_PURGE ──→ 输出一次并清空（Session 合并等场景常见）
```

**记忆口诀**：FIRE = 「算一次」；PURGE = 「扔数据」；带 PURGE 的通常意味着窗口生命周期结束或重置。

### ② 默认 EventTimeTrigger 行为

Flink `TumblingEventTimeWindows` 默认挂载 `EventTimeTrigger`：

```java
// EventTimeTrigger 核心逻辑（简化）
onElement(...) {
    ctx.registerEventTimeTimer(window.maxTimestamp());  // 注册窗口结束时刻定时器
    return CONTINUE;  // 来一条不 FIRE，只注册定时器
}
onEventTime(time, ...) {
    if (time == window.maxTimestamp()) {
        return FIRE;  // WM 到达窗口结束 → 仅 FIRE 一次
    }
    return CONTINUE;
}
```

```
事件陆续到达窗口 [0, 5min)
  e1, e2, ... e220  → 全部 CONTINUE（不出数）

WM 推进到 window.maxTimestamp (= 5min-1ms)
  → onEventTime 返回 FIRE
  → ProcessWindowFunction / Aggregate 执行 1 次
```

**结论**：默认 Trigger 下，**窗口结束前不会有任何输出**——这就是大屏/看板「分钟窗口但要秒级刷新」不够用的根因。

### ③ Evictor 在 FIRE 之前裁剪元素

```
窗口内元素: [e1, e2, e3, ... e100]
                    │
            Trigger 返回 FIRE
                    │
            Evictor.evictBefore(...)   ← 在计算前执行
                    │
            剩余元素进入 WindowFunction
```

| Evictor 类型 | 行为 |
|--------------|------|
| `CountEvictor(n)` | 只保留最近 n 条，剔除更早的 |
| `TimeEvictor(duration, ...)` | 按时间戳剔除超出保留时长的元素 |

**关键约束**：一旦使用 Evictor，Flink 必须在内存中**缓存窗口全量元素**才能剔除——**与 Aggregate/Reduce 增量语义冲突**（见 Step 4）。

---

## Step 2 手写代码对照

| 要求 | 实现 |
|------|------|
| 每 100 条 early-fire | `CountOrTimeTrigger.of(100)` |
| 窗口结束 final-fire | `onEventTime(window.maxTimestamp()) → FIRE` |
| 不 PURGE | early-fire 返回 `FIRE` 而非 `FIRE_AND_PURGE` |
| 对比默认 Trigger | 支路 1 不指定 trigger（默认 EventTimeTrigger） |
| Evictor 演示 | 支路 4 `.evictor(CountEvictor.of(30))` |
| 增量聚合可配合 Trigger | 支路 2 `aggregate(WatermarkAmountSumAggregator, ...)` |

### 关键代码

```java
// 自定义 Trigger：满 100 条或窗口结束都 FIRE，不 PURGE
keyedStream
    .window(TumblingEventTimeWindows.of(Time.minutes(5)))
    .trigger(CountOrTimeTrigger.of(100))
    .process(new TriggerWindowLogFunction("COUNT-OR-TIME"));

// 默认 Trigger（对比组）
keyedStream
    .window(TumblingEventTimeWindows.of(Time.minutes(5)))
    .process(new TriggerWindowLogFunction("DEFAULT-EventTimeTrigger"));

// Evictor：每次 FIRE 前只保留最近 30 条
keyedStream
    .window(TumblingEventTimeWindows.of(Time.minutes(5)))
    .trigger(CountOrTimeTrigger.of(100))
    .evictor(CountEvictor.of(30))
    .process(new TriggerWindowLogFunction("COUNT-OR-TIME+Evictor"));
```

### CountOrTimeTrigger 核心逻辑

```java
onElement(...) {
    count.add(1);
    ctx.registerEventTimeTimer(window.maxTimestamp());
    if (count.get() % 100 == 0) {
        return TriggerResult.FIRE;   // early-fire，元素不删
    }
    return TriggerResult.CONTINUE;
}
onEventTime(time, ...) {
    if (time == window.maxTimestamp()) {
        return TriggerResult.FIRE;   // 窗口结束 final-fire
    }
    return TriggerResult.CONTINUE;
}
```

---

## Step 3 场景：什么时候需要 early-fire / Evictor？

### ① early-fire 典型场景

| 场景 | 窗口 | 痛点 | Trigger 策略 |
|------|------|------|--------------|
| **实时大屏预览** | Tumbling 5min | 5min 才出数太慢 | 每 30s 或每 500 条 FIRE |
| **大窗口报表中间态** | Tumbling 1h | 运营要「进行中」累计 | CountTrigger / 自定义 CountOrTime |
| **风控阈值告警** | Tumbling 10min | 不能等 10min 才报警 | 每 N 条或 ProcessingTime 周期 FIRE |

**本 Demo 映射**：5min 窗口 + 灌入 220 条 → 在 WM 到 +5min 之前，于 100、200 条各 early-fire 一次。

### ② Evictor 典型场景（少用）

| 场景 | 用法 | 注意 |
|------|------|------|
| 只要「最近 N 条」行为画像 | `CountEvictor(50)` | 牺牲精确累计 |
| 滑动窗口减状态 | 配合自定义 Trigger | 运维复杂 |
| 实验性「采样窗口」 | TimeEvictor | 结果非精确，需标注 |

**原则**：生产环境 **优先用 Sliding 窗口 / 更小 Tumbling / 自定义 Trigger**，Evictor 因全量缓存 **性能差、慎用**。

### ③ 如何在不改窗口大小的前提下让结果更实时（Step 5 话术预备）

```
不改窗口大小（仍是 Tumbling 5min 语义）
  → 换 Trigger：CountOrTime / ContinuousEventTimeTrigger
  → 同一窗口 key 会多次输出「截至目前累计」
  → 下游用 (windowStart, windowEnd, key) 做 UPSERT 覆盖，而非 INSERT
```

---

## Step 4 陷阱

### ① early-fire → 同一窗口多次输出

```
窗口 [10:00, 10:05) 对 userId=U1：
  10:01:30  [EARLY-FIRE] count=100 sum=...
  10:03:00  [EARLY-FIRE] count=200 sum=...
  10:05:00  [FINAL-FIRE] count=220 sum=...   ← 终态

下游 MySQL / Redis / 大屏：
  ❌ 按 INSERT 累加 → 重复计数
  ✅ 按 (userId, windowStart) UPSERT 覆盖为最新值
  ✅ 或只消费 FINAL-FIRE（需业务区分 fire 类型，本 Demo 用 WM < window.end 判断）
```

### ② Evictor 破坏增量聚合

```
.aggregate(SumAggregator)  +  .evictor(...)  → ❌ 语义错误 / 运行时报错风险

原因：Aggregate 只存累加器；Evictor 需要 Iterable 全量元素才能剔除
正确做法：
  - 要增量：自定义 Trigger，不用 Evictor
  - 要剔除：ProcessWindowFunction 全量计算（本 Demo 支路 4）
```

### ③ early-fire 与 allowedLateness 叠加

early-fire 后窗口仍 OPEN，迟到数据仍会进入并可能再次 FIRE。计费类场景要定义「以哪次 FIRE 为准」。

### ④ ProcessingTimeTrigger vs EventTimeTrigger

| 类型 | 驱动 | 乱序敏感 |
|------|------|----------|
| EventTimeTrigger | WM 到 window.end | 是，与业务时间一致 |
| ProcessingTimeTrigger | 系统时钟 | 否，乱序下结果漂移 |

教育直播等业务 **必须用 EventTime + 自定义 early-fire**，不要用 ProcessingTime 冒充实时。

---

## Step 5 面试话术

### 如何在不改窗口大小的前提下让结果更实时？

> 「窗口仍用 Tumbling 5 分钟，保证统计口径不变；通过自定义 `CountOrTimeTrigger` 在窗口内每攒满 100 条或每 30 秒做一次 `FIRE`（不 `PURGE`），在 WM 到达窗口结束前就能输出中间累计。下游按 `(key, windowStart, windowEnd)` 做幂等 UPSERT，final-fire 时写入终态。这比把窗口改成 30 秒更小、比 ProcessingTime 更准，也比 Evictor 更省内存。」

### Trigger 与 Watermark 的分工？

> 「Watermark 决定 **事件时间进度** 和窗口 **能否关闭**；Trigger 决定窗口 **开放期间何时提前算一次**。默认 `EventTimeTrigger` 只在 WM ≥ window.end 时 FIRE 一次；early-fire 是 Trigger 层的能力，不改变窗口边界。」

### 四种 TriggerResult 一句话？

> 「`CONTINUE` 继续等；`FIRE` 算一次但保留数据；`PURGE` 清数据不算；`FIRE_AND_PURGE` 算完清空——Session 结束时常用最后一种。」

---

## 运行与验收

### 启动

```bash
# 1. 创建 topic
kafka-topics.sh --create --topic test_flink_trigger --partitions 1 \
  --bootstrap-server 192.168.1.124:9092

# 2. 启动 Job
org.example.job.trigger.FlinkTriggerEvictorDemoJob

# 3. 发送测试数据
org.example.job.trigger.FlinkTriggerEvictorDemoJobTest
```

### 预期日志

```
# Phase1 满 100 条后（WM 仍 < +5min）
自定义Trigger+聚合> [COUNT-OR-TIME+AGG][EARLY-FIRE] ... count=100 sum=100.0

# Phase2 满 200 条
自定义Trigger+聚合> [COUNT-OR-TIME+AGG][EARLY-FIRE] ... count=200 sum=200.0

# 默认 Trigger：上述阶段均无输出

# Phase4 flush（ts=+310s）后
默认Trigger> [DEFAULT-EventTimeTrigger][FINAL-FIRE] ... count=220 sum=220.0
自定义Trigger+聚合> [COUNT-OR-TIME+AGG][FINAL-FIRE] ... count=220 sum=220.0

# Evictor 支路：每次 FIRE count≤30
Evictor演示> [COUNT-OR-TIME+Evictor][EARLY-FIRE] ... count=30 sum=30.0
```

### 验收清单

| # | 验收项 | 验证方式 |
|---|--------|----------|
| ① | 说清 4 种 TriggerResult | 本文 Step1① + `FlinkTriggerEvictorDemoJobTest#triggerResult_fourKinds_documented` |
| ② | 跑通自定义 CountOrTimeTrigger | Job 控制台见 2 次 EARLY + 1 次 FINAL |
| ③ | 对比默认 Trigger 仅 final 一次 | 默认支路仅 flush 后 1 条 |
| ④ | 解释 Evictor 与 aggregate 冲突 | Step4② + Evictor 支路 count≤30 |
| ⑤ | 面试话术 | Step5 |

---

## 加分点：Flink SQL 的 `TABLE.EXEC.EMIT.EARLY-FIRE` 等价物

### Table API / SQL（Flink 1.14+）

DataStream 自定义 `CountOrTimeTrigger` 在 SQL 层的等价配置是 **Emit Strategy（早期发射策略）**：

```sql
-- Flink 1.14+ Table 配置（SQL Client / TableEnvironment）
SET 'table.exec.emit.early-fire.enabled' = 'true';
SET 'table.exec.emit.early-fire.delay' = '30 s';   -- 每 30s early-fire 一次
-- 或按处理时间间隔
SET 'table.exec.emit.early-fire.delay' = '10 s';

SELECT window_start, window_end, user_id, SUM(amount)
FROM TABLE(
  TUMBLE(TABLE orders, DESCRIPTOR(ts), INTERVAL '5' MINUTES)
)
GROUP BY window_start, window_end, user_id;
```

| DataStream API | Table API / SQL 等价 |
|----------------|---------------------|
| `CountOrTimeTrigger.of(100)` | `early-fire.enabled` + 自定义 UDF 场景较少，多用 **delay 周期** |
| `ContinuousEventTimeTrigger.of(interval)` | `table.exec.emit.early-fire.delay = interval` |
| 默认 `EventTimeTrigger` | `early-fire.enabled = false`（默认） |
| `allowedLateness` | 仍作用于 SQL 窗口，与 early-fire 正交 |

**对应关系**：SQL 的 early-fire 本质是 Planner 在窗口算子上配置了 **带周期的 early Trigger**；DataStream 上需手写 `Trigger` 才能达到完全相同的「满 N 条」语义——本 Demo 的 `CountOrTimeTrigger` 在 SQL 中没有一行配置完全等价，需 UDF 或继续用 DataStream API。

---

## Step 7 在线教育典型业务案例（Trigger / Evictor 三角）

> 以下三个场景是在线教育平台里 **early-fire 与 Evictor 最高发** 的业务，分别对应本 Demo 的三条主线：  
> **大窗口要预览** / **大屏秒级刷新** / **慎用 Evictor 的画像场景**。  
> 与《FlinkWatermarkDemoGuide》Step 7 互补：那边讲 WM 推进，这边讲 **WM 够慢时如何用 Trigger 补实时性**。

---

### 案例一：直播课分钟互动大屏 — 5min 窗口 + 秒级 early-fire

#### 业务背景

大班课运营大屏展示「本节课累计互动次数 / 答题参与率」，统计口径是 **Tumbling 5min**（与教务报表对齐），但产品要求 **每 10~30 秒刷新一次**「进行中」数字，不能等 5 分钟 WM 才跳变。

#### 数据模型

```json
{"liveRoomId":"L888","studentId":"S10001","eventType":"danmu|quiz|hand",
 "ts":1717654321000}
```

- `keyBy(liveRoomId)` + Tumbling 5min
- Event Time + `forBoundedOutOfOrderness(45s)`（参见 Watermark 指南案例一）

#### Trigger 配置

```java
stream.keyBy(LiveInteractEvent::getLiveRoomId)
    .window(TumblingEventTimeWindows.of(Time.minutes(5)))
    .trigger(CountOrTimeTrigger.of(500))   // 每 500 条互动 early-fire
    // 或 .trigger(ContinuousEventTimeTrigger.of(Time.seconds(30)))  // 每 30s early-fire
    .aggregate(new InteractCountAggregator(), new DashboardFormatter());
```

#### 为什么这样配（三要素）

| 维度 | 分析 |
|------|------|
| **统计口径** | 窗口仍 5min，与课后归档一致，不改 KPI 定义 |
| **实时性** | early-fire 每 30s 更新 Redis 大屏；final-fire 写 Doris 终态 |
| **下游** | Redis key=`roomId:windowStart` **覆盖写**；禁止 INSERT 累加 |

**与 Demo 映射**：灌入 100/200 条触发 EARLY-FIRE ≈ 直播高峰每攒够 N 条互动就刷新大屏；flush 事件 ≈ 下课推 WM 触发 FINAL-FIRE。

#### 生产架构要点

```
Kafka(live_interact) → Flink WM(45s) → Tumbling 5min + CountOrTimeTrigger
                     → Redis 大屏（消费 early + final，UPSERT）
                     → Doris 归档（仅 final 或取最后一次）
```

#### 踩坑与心得

1. **early-fire 不是免费午餐**：下游必须幂等；大屏组件要显示「进行中」与「已结束」两种状态。
2. **与 allowedLateness 叠加**：下课瞬间仍可能有弱网迟到互动，final-fire 后还可能增量更新——计费类要单独链路。
3. **心得**：这是 Step5 话术的生产版——**窗口口径不变，Trigger 补实时**。

---

### 案例二：录播课学习进度看板 — 1h 窗口 + 每 2min 预览（ContinuousEventTimeTrigger）

#### 业务背景

班主任看板展示「过去 1 小时班级平均学习进度」，窗口 **Tumbling 1h**，但班主任希望 **每 2 分钟**看到进度条变化（否则 1h 内像「卡死」）。

#### 数据模型

```json
{"classId":"CL001","studentId":"S10001","progress":65,"ts":1717654321000}
```

#### Trigger 配置

```java
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;

stream.keyBy(ProgressEvent::getClassId)
    .window(TumblingEventTimeWindows.of(Time.hours(1)))
    .trigger(ContinuousEventTimeTrigger.of(Time.minutes(2)))
    .aggregate(new AvgProgressAggregator(), new ProgressFormatter());
```

#### 为什么用 Continuous 而非 Count（三要素）

| 维度 | 分析 |
|------|------|
| **流量不均** | 小班课 1h 可能不足 100 条，CountTrigger 永不 early-fire |
| **时间语义** | 每 2min 固定刷新符合产品「刷新间隔」文案 |
| **WM 依赖** | 仍依赖 WM 推进；需保证心跳 WM 不卡死（见 Watermark 指南案例二 idleness） |

**与 Demo 映射**：`ContinuousEventTimeTrigger` 是 `CountOrTimeTrigger` 的「按时间 early-fire」兄弟；Demo 用 Count 便于单测精确控制条数。

#### 运维与告警

| 监控项 | 说明 |
|--------|------|
| early-fire 次数 / 小时 | 异常暴增可能是 Trigger 配置过密 |
| 同窗口多条 Redis 写 | 监控 UPSERT 延迟 P99 |
| final-fire 缺失 | WM 未推过 1h 边界，查 idle 分区 / union |

#### 踩坑与心得

1. **ContinuousEventTimeTrigger 首次 FIRE 也要等 WM > window.start**——不是 ProcessingTime 定时器。
2. **1h 窗口 + 2min early-fire** ≈ 每窗口最多 30 次输出，下游压力可估算。
3. **心得**：教育 B 端看板「窗口大、刷新密」= **ContinuousEventTimeTrigger 的标准场景**。

---

### 案例三：学员行为画像「最近 50 次答题」— Evictor 慎用对照

#### 业务背景

教研分析「某学员最近 50 次答题正确率」用于自适应推题。产品口述「1 天内的最近 50 次」，工程一度在 **Tumbling 1day** 上挂 `CountEvictor(50)` + `ProcessWindowFunction`。

#### 故障现象

```
窗口 [00:00, 24:00) 内共 200 次答题
early-fire 每 100 次触发 → Evictor 后每次只剩 50 条
正确率波动极大，与「全天 200 次」口径不一致
TaskManager heap 升高（全量缓存 200 条 × 百万学员）
```

#### 推荐架构（替代 Evictor）

```java
// 方案 A：滑动窗口直接表达「最近」
stream.keyBy(QuizEvent::getStudentId)
    .window(SlidingEventTimeWindows.of(Time.days(1), Time.minutes(30)))
    .aggregate(new QuizStatsAggregator());

// 方案 B：状态 + Timer（更省资源）
// KeyedProcessFunction 维护 LinkedList 最近 50 条，无需 Evictor
```

#### 为什么少用 Evictor（三要素）

| 维度 | 分析 |
|------|------|
| **语义** | Evictor 剔除的是窗口**存储元素**，不是「业务上最近 N 次」的精确表达 |
| **性能** | 必须全量缓存 → 无法 aggregate 增量 |
| **运维** | 与 Trigger 组合后调试困难，日志 count 每次 ≤50 难一眼看出 bug |

**与 Demo 映射**：支路 4 `CountEvictor(30)` 在 220 条灌入后，每次 FIRE `count≤30`——与生产画像故障同构。

#### 踩坑与心得

1. **Evictor + early-fire** = 双重复杂度，面试可说「我会优先 Sliding 或状态方案」。
2. **教育画像**多为「最近 N 次 / 最近 7 天」，用 **窗口类型表达口径** 比 Evictor 裁剪更可维护。
3. **心得**：本 Demo 支路 4 的价值是 **证明 Evictor 为何少用**，不是推荐用法。

---

### 三案例对照总表

| 案例 | Trigger/Evictor 主题 | 典型症状 | 核心配置 | 与 Demo 对应 |
|------|---------------------|----------|----------|--------------|
| 直播互动大屏 | **Count / 周期 early-fire** | 5min 窗口但大屏 5min 才跳 | `CountOrTimeTrigger(500)` 或 `Continuous...(30s)` | Phase1/2 EARLY-FIRE |
| 班级进度看板 | **ContinuousEventTimeTrigger** | 1h 窗口 1h 无反馈 | `ContinuousEventTimeTrigger.of(2min)` | 与 Count 支路互补 |
| 答题画像 | **Evictor 慎用** | 正确率漂移 / OOM | 改 Sliding 或 KeyedState | 支路 4 count≤30 |

### 与《FlinkWatermarkDemoGuide》《FlinkWindowDemoGuide》的关系

| 指南 | 本指南 |
|------|--------|
| WM 推进、乱序、idle | WM 到 end 前如何 **提前 FIRE** |
| Tumbling/Sliding/Session 选型 | 同一 Tumbling 如何用 **Trigger 提高实时性** |
| allowedLateness 迟到 | early-fire 多次输出 + 迟到叠加 → 下游幂等 |

**推荐学习顺序**：Watermark（WM 能推）→ Window（桶怎么切）→ **Trigger/Evictor（何时出数、出哪些）** → Late Data（迟到怎么办）。

---

## 与窗口 / Watermark Demo 的关系

| 组件 | 本 Demo 职责 |
|------|-------------|
| `TumblingEventTimeWindows(5min)` | 统计口径不变 |
| `forBoundedOutOfOrderness(5s)` | 保证 Event Time 语义 |
| `CountOrTimeTrigger(100)` | 满 100 条 early-fire |
| `CountEvictor(30)` | 演示剔除，非推荐 |
| `FlinkTriggerEvictorDemoJobTest` | 220 条 + flush，可重复验收 |

两者结合：先保证 **WM 能推过窗口 end**（Watermark Demo），再谈 **窗口内提前出数**（本 Demo）。
