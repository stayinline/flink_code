# Flink ProcessFunction 与定时器 — 学习指南

> 配套代码：`FlinkOrderTimeoutDemoJob` + `FlinkOrderTimeoutDemoJobTest`  
> 数据源：`OrderPaymentEvent` `(orderId, userId, eventType, amount, ts, tag)`  
> 核心：`KeyedProcessFunction` + **事件时间定时器**（演示超时 15s，生产 15min）

---

## 读前扫盲：定时器适合处理“某个 key 自己的倒计时”

窗口适合做一批数据的周期性聚合，例如“每 5 分钟 PV”。但订单超时这类问题不是固定窗口：每个订单都有自己的下单时间，也就有自己的超时时刻。`O1` 可能在 `+10s` 下单，`O2` 可能在 `+20s` 下单，它们的倒计时边界不同。

`KeyedProcessFunction + TimerService` 可以先这样理解：

| 组成 | 入门解释 | 本 Demo 例子 |
|------|----------|--------------|
| `keyBy(orderId)` | 让每个订单拥有独立状态和独立定时器 | `O1` 和 `O2` 互不影响 |
| `processElement` | 每来一条事件就执行一次 | 收到下单注册定时器，收到支付删除定时器 |
| `onTimer` | 定时器到期后执行 | 订单仍未支付则输出超时告警 |
| `ValueState` | 保存当前 key 的业务上下文 | 保存待支付订单和定时器时间戳 |

事件时间定时器和处理时间定时器的区别要先分清：

| 定时器类型 | 看什么时间 | 适合场景 |
|------------|------------|----------|
| EventTime Timer | 看 Watermark 是否推进到目标时间 | 按业务事件时间判断超时，例如订单下单后 15 分钟未支付 |
| ProcessingTime Timer | 看机器系统时钟是否到达目标时间 | 周期清理、心跳、非严格业务时间任务 |

本 Demo 选择事件时间定时器，所以“注册了定时器”不等于“现实世界 15 秒后一定立刻触发”。它还需要 Watermark 推进到对应时间点。读日志时如果暂时没有 `[TIMEOUT-ALERT]`，要同时看是否已经收到支付事件、是否删除了定时器、以及 Watermark 是否已经越过触发时间。

---

## Step 1 原理：processElement / onTimer / TimerService

### ① KeyedProcessFunction 生命周期

```
Kafka 事件
    │
    ▼
assignTimestampsAndWatermarks
    │
    ▼
keyBy(orderId)  ──→  每个 key 独立状态 + 独立定时器命名空间
    │
    ▼
KeyedProcessFunction
    ├── processElement()   每条事件到达
    └── onTimer()          注册的定时器到期
```

| 方法 | 职责 |
|------|------|
| `processElement` | 处理下单/支付，注册或删除定时器 |
| `onTimer` | 定时器触发，输出超时告警 |
| `open` | 初始化 `ValueState` / `ListState` 等 |

### ② TimerService 两类定时器

| API | 驱动 | 触发条件 |
|-----|------|----------|
| `registerEventTimeTimer(t)` | **Watermark** | `currentWatermark >= t` |
| `registerProcessingTimeTimer(t)` | **系统时钟** | 处理时间到达 `t` |

```
事件时间定时器（本 Demo）：
  下单 ts=+10s，超时 15s → registerEventTimeTimer(+25s)
  WM 必须推进到 ≥ +25s → onTimer 才执行

处理时间定时器（对比）：
  下单时刻 processingTime=T → registerProcessingTimeTimer(T+15s)
  不依赖 WM，机器挂钟走 15s 即触发（重启行为不同）
```

### ③ 定时器存在哪？

```
keyBy(orderId) 之后：
  ┌─────────────────────────────────────┐
  │ Key=O1  subtask-0                   │
  │   ValueState: pendingTimerTs=+15s   │
  │   TimerHeap: EventTimeTimer(+15s)   │  ← 与 key 绑定
  ├─────────────────────────────────────┤
  │ Key=O2  subtask-0                   │
  │   ValueState: pendingTimerTs=+25s   │
  │   TimerHeap: EventTimeTimer(+25s)   │
  └─────────────────────────────────────┘
           持久化到状态后端（RocksDB/Heap）
           Checkpoint 时状态 + 定时器元数据一起快照
```

**要点**：定时器不是 JVM `ScheduledExecutorService`，而是 Flink 状态后端管理的 **keyed state 附属结构**，随 Checkpoint 恢复。

### ④ register / delete 配对

```java
long fireTs = orderTs + timeoutMs;
pendingTimerTs.update(fireTs);
ctx.timerService().registerEventTimeTimer(fireTs);

// 支付成功必须删除，否则 onTimer 仍会触发 → 误告警 + 定时器泄漏
ctx.timerService().deleteEventTimeTimer(fireTs);
pendingTimerTs.clear();
```

---

## Step 2 手写代码对照

| 要求 | 实现 |
|------|------|
| 下单注册 15s 事件时间定时器 | `OrderTimeoutAlertFunction.handleOrderCreated()` |
| 支付删定时器 | `deleteEventTimeTimer` + `pendingTimerTs.clear()` |
| 超时 onTimer 告警 | `onTimer` → `[TIMEOUT-ALERT]` |
| 打印 WM | `OrderEventMonitorFunction` |
| 生产 15min | `PRODUCTION_TIMEOUT_MS`，Demo 用 `TIMEOUT_MS=15000` |

### 关键代码

```java
// 下单：注册事件时间定时器
long fireTs = event.getTs() + timeoutMs;
pendingOrder.update(event);
pendingTimerTs.update(fireTs);
ctx.timerService().registerEventTimeTimer(fireTs);

// 支付：删除定时器
Long timerTs = pendingTimerTs.value();
ctx.timerService().deleteEventTimeTimer(timerTs);
pendingTimerTs.clear();
pendingOrder.clear();

// 超时
public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) {
    if (Objects.equals(pendingTimerTs.value(), timestamp)) {
        out.collect("[TIMEOUT-ALERT] ...");
        pendingTimerTs.clear();
        pendingOrder.clear();
    }
}
```

---

## Step 3 场景：定时器典型用途 & 为何不用 Window

### ① 定时器典型用途

| 场景 | 实现方式 | 本 Demo 映射 |
|------|----------|--------------|
| **超时检测** | EventTimeTimer(orderTs + N) | 订单 15s 未支付 |
| **状态 TTL 手动实现** | ProcessingTimeTimer 定期清理 | 指南案例三 |
| **按 key 定期 flush** | ProcessingTimeTimer 滚动注册 | 班级进度上报 |
| **去重窗口** | 单定时器 + MapState | 指南案例二 |
| **A 后 N 分钟无 B** | 注册 Timer，B 到达删 Timer | 直播签到无互动 |

### ② ProcessFunction vs Window — 什么时候用谁？

| 维度 | Window | KeyedProcessFunction + Timer |
|------|--------|------------------------------|
| 统计口径 | 固定窗口边界（Tumbling 5min） | **任意**时间间隔（下单后 15min） |
| 触发条件 | WM 到 window.end | **自定义**（下单时刻 + offset） |
| 状态范围 | 窗口内元素 | 单 key 业务状态机 |
| 典型场景 | 聚合报表 | **超时、CEP 轻量替代、去重** |

```
订单超时：
  ❌ Tumbling 15min 窗口 — 窗口边界与「下单时刻」无关，无法表达「每笔订单独立 15min」
  ✅ KeyedProcessFunction — 每笔 orderId 独立注册 orderTs+15min 定时器
```

---

## Step 4 陷阱

### ① 海量 key → 定时器风暴

```
双 11 每秒 10 万下单 → 10 万个 EventTimeTimer
  → 状态后端 timer 堆膨胀
  → Checkpoint 变慢
  → 若支付后未 deleteTimer → 泄漏累积
```

**治理（加分点）**：

| 手段 | 说明 |
|------|------|
| **及时 deleteTimer** | 支付/取消必须删，本 Demo 核心 |
| **合并定时器** | 多订单按 `userId` 共用一个粗粒度 Timer（如每分钟扫一次 MapState） |
| **粗粒度时间轮** | 按秒/分钟桶注册，onTimer 时扫描桶内过期订单 |
| **TTL State** | `StateTtlConfig` 自动清理过期 key，减少长期堆积 |
| **降 key 基数** | 超时检测按 `userId` 而非 `orderId`（业务允许时） |

### ② 事件时间定时器依赖 WM（联动 D2）

```
O2 下单 ts=+10s → 定时器 +25s
Phase2 等待期 WM ≈ +5s → onTimer 不执行
Phase3 flush ts=+30s → WM ≥ +25s → [TIMEOUT-ALERT]

教训：EventTimeTimer 与 Window 一样，**WM 不推进则逻辑不前进**
```

### ③ 处理时间定时器重启行为

| 类型 | 故障重启后 |
|------|------------|
| EventTimeTimer | 从 Checkpoint 恢复，仍等 WM ≥ fireTs |
| ProcessingTimeTimer | 恢复后按**处理时间**重新计时，可能与故障前不一致 |

**选型**：计费/订单超时用 **EventTime**；纯运维扫尾（如每小时 flush 缓存）可用 **ProcessingTime**。

### ④ 乱序支付

支付事件 ts 早于下单（脏数据）→ `pendingTimerTs` 为空 → `[PAID-NO-PENDING]`。生产应做校验或侧输出。

---

## Step 5 面试话术

### 不用 CEP 库，怎么实现「A 事件后 N 分钟内没有 B 事件就告警」？

> 「`keyBy(业务键)` 后用 `KeyedProcessFunction`。收到 A 时把上下文写入 `ValueState`，并 `registerEventTimeTimer(A.ts + N)`。若 N 分钟内收到 B，则 `deleteEventTimeTimer` 并清空状态；否则 `onTimer` 触发告警。这是单模式 CEP 的轻量实现，比上 Flink CEP 库更轻、状态更可控。注意 EventTime 定时器依赖 Watermark 推进，海量 key 要及时删定时器防风暴，可合并为粗粒度时间轮。」

### 两类定时器差异与删除时机？

> 「EventTime 看 WM，与业务时间对齐，适合订单超时；ProcessingTime 看系统钟，适合周期性清理。删除时机：**条件达成立刻删**（支付成功、收到 B 事件、状态机转移），不要等 onTimer 才清理——否则会误触发且状态泄漏。」

---

## 运行与验收

### 启动

```bash
# 1. 创建 topic
kafka-topics.sh --create --topic test_flink_order_timeout --partitions 1 \
  --bootstrap-server 192.168.1.124:9092

# 2. 启动 Job
org.example.job.timer.FlinkOrderTimeoutDemoJob

# 3. 发送测试数据
org.example.job.timer.FlinkOrderTimeoutDemoJobTest
```

### 预期日志

```
# Phase1 O1
[TIMER-REGISTER] orderId=O1 ... fireAt=+15s
[PAID-IN-TIME] orderId=O1 ... deletedTimer=+15s

# Phase2~3 O2（WM 未推进前无 TIMEOUT-ALERT）
[TIMER-REGISTER] orderId=O2 ... fireAt=+25s
... 等待 ...
[TIMEOUT-ALERT] orderId=O2 ... fireAt=+25s   ← flush +30s 后

# Phase4~5 O3
[TIMER-REGISTER] orderId=O3 ... fireAt=+55s
... 等待 5s 仍无告警 ...
[TIMEOUT-ALERT] orderId=O3 ...               ← flush +65s 后
```

### 验收清单

| # | 验收项 | 验证方式 |
|---|--------|----------|
| ① | 能手写超时告警 | `OrderTimeoutAlertFunction` + Job 跑通 |
| ② | 说清两类定时器差异 | Step1② + 单测 `timerTypes_difference_documented` |
| ③ | 说清删除时机 | 支付后 `[PAID-IN-TIME]` + `deleteEventTimeTimer` 日志 |
| ④ | WM 不动定时器不触发 | Phase2 等待期无 O2 告警 |
| ⑤ | 面试话术 | Step5 |
| ⑥ | 定时器风暴治理 | Step4① 合并/时间轮 |

---

## 加分点：定时器风暴治理详解

### 问题量级

```
峰值 50k 订单/秒 × 超时 15min ≈ 4500 万并发定时器（极端估算）
RocksDB 中每个 timer 有元数据开销 → Checkpoint 超时
```

### 方案一：粗粒度时间轮（推荐口述）

```java
// 按分钟桶聚合：只注册「下一分钟边界」一个 Timer
long bucket = (orderTs / 60_000 + 1) * 60_000;
registerEventTimeTimer(bucket);
// MapState<bucket, List<orderId>> 存该分钟需检查的订单
// onTimer(bucket) 时扫描列表，对 ts+timeout < currentWM 的订单告警
```

定时器数量从 **O(订单数)** 降为 **O(时间桶数)**。

### 方案二：ProcessingTime 周期扫描

适合允许秒级误差的运维类超时（非精确计费）：

```java
// 每个 subtask 只注册一个 ProcessingTimeTimer，每 30s 扫 MapState
```

### 方案三：State TTL

```java
StateTtlConfig ttl = StateTtlConfig.newBuilder(Time.minutes(20))
    .setUpdateType(UpdateType.OnCreateAndWrite)
    .build();
```

超时未支付订单状态自动过期，避免无限增长（仍需 deleteTimer 防误触发）。

---

## Step 7 在线教育典型业务案例（定时器三角）

> 以下三个场景是在线教育平台里 **KeyedProcessFunction + Timer 最高发** 的业务。  
> 与《FlinkWatermarkDemoGuide》互补：WM 解决「时间进度」，本指南解决 **「单笔业务独立时序」**。

---

### 案例一：课程订单未支付催付 — EventTime 超时（本 Demo 生产版）

#### 业务背景

学员选课下单后需在 **15 分钟**内完成支付，否则：
- 释放课程名额
- 推送 App / 短信催付
- 班主任工作台标红「待催付」

每笔订单超时起点是 **各自下单时刻**，不能用固定 Tumbling 15min 窗口。

#### 数据模型

```json
{"orderId":"ORD888","userId":"S10001","eventType":"ORDER_CREATED","amount":299.0,"ts":1717654321000}
{"orderId":"ORD888","userId":"S10001","eventType":"PAYMENT","amount":299.0,"ts":1717655200000}
```

#### 实现

```java
stream.keyBy(OrderPaymentEvent::getOrderId)
    .process(new OrderTimeoutAlertFunction(Duration.ofMinutes(15).toMillis()));
```

#### 为什么用 ProcessFunction 而非 Window（三要素）

| 维度 | 分析 |
|------|------|
| **业务语义** | 「下单后 15min」是 **点对点超时**，与日历窗口无关 |
| **准确性** | EventTime 定时器与订单 ts 对齐，弱网补报可容错乱序 |
| **运维** | 支付成功必须 `deleteTimer`；日催付量 10 万需防定时器风暴 |

**与 Demo 映射**：`TIMEOUT_MS=15s` = 生产 `15min` 的缩小版；O1 支付 = 学员及时付款；O2/O3 flush = 大促结束推 WM。

#### 踩坑与心得

1. **大促峰值**：考虑时间轮合并，不要把定时器数量等同于订单数。
2. **重复下单**：同一 `orderId` 重试要先删旧 Timer（本 Demo `handleOrderCreated` 已处理）。
3. **心得**：教育订单催付是 **EventTime Timer 的标准教科书场景**。

---

### 案例二：直播课签到后 10 分钟无互动 —「A 后无 B」轻量 CEP

#### 业务背景

大班课要求学员 **签到后 10 分钟内至少有一次互动**（弹幕/答题），否则班主任介入「疑似挂机」。

模式：**A=签到，B=互动，N=10min，无 B 则告警** — 正是 Step5 面试题。

#### 数据模型

```json
{"liveRoomId":"L888","studentId":"S10001","eventType":"CHECK_IN","ts":1717654321000}
{"liveRoomId":"L888","studentId":"S10001","eventType":"INTERACT","ts":1717654800000}
```

#### 实现

```java
// keyBy(liveRoomId + studentId)
if (CHECK_IN) {
    state.update(ctx);
    timerService.registerEventTimeTimer(ts + 10min);
}
if (INTERACT) {
    timerService.deleteEventTimeTimer(pendingTs);
    state.clear();
}
onTimer → 告警「挂机嫌疑」
```

#### 为何不用 Flink CEP？

| CEP | ProcessFunction + Timer |
|-----|-------------------------|
| `followedBy(B).within(10min)` 表达力强 | 单模式 A→B 超时 **几十行搞定** |
| 状态复杂、调试难 | 状态仅 `pendingTimerTs` + 签到上下文 |
| 适合多模式序列 | 本场景 **一条规则** 足够 |

#### 运维与告警

| 监控项 | 说明 |
|--------|------|
| 定时器注册/删除比 | 删除偏低 → 泄漏 |
| WM 滞后 | 挂机告警延迟 |
| 误报率 | 互动事件定义是否含「切后台心跳」 |

#### 踩坑与心得

1. **B 事件定义要产品确认**：看视频进度算不算互动？
2. **WM 依赖**：下课时需 flush，否则最后一波签到告警滞后。
3. **心得**：这是 **不用 CEP 实现 A→B 超时** 的最佳业务例子。

---

### 案例三：录播课学习状态 TTL — ProcessingTime 定期清理

#### 业务背景

`KeyedProcessFunction` 维护学员 **当前学习会话**（开始时间、累计时长、课程 ID）。学员异常退出可能不发「结束学习」事件，状态会永久残留。

#### 实现思路

```java
// 每条心跳更新状态，并：
timerService.deleteProcessingTimeTimer(oldProcTimer);
long nextScan = ctx.timerService().currentProcessingTime() + 30_000;
timerService.registerProcessingTimeTimer(nextScan);

onTimer(processingTime) {
    if (now - lastHeartbeat > 5min) {
        flush 学习记录;
        state.clear();
    }
    registerProcessingTimeTimer(currentProcessingTime + 30s); // 滚动
}
```

#### EventTime vs ProcessingTime 选型

| 维度 | 本案例选 ProcessingTime 的原因 |
|------|--------------------------------|
| 检测目标 | **「多久没收到心跳」** — 到达延迟/系统静默 |
| 精确性 | 允许 30s 级误差，不需与业务 ts 严格对齐 |
| 重启 | 可接受重新计时（非计费） |

#### 与 Window 对比

```
Session 窗口（gap=5min）：
  可统计学习时长，但无法主动「5min 无心跳则强制结案写库」
ProcessFunction + ProcessingTimeTimer：
  主动扫描 + 写库 + 清状态 — 会话生命周期的 **主动收尾**
```

#### 踩坑与心得

1. **ProcessingTime 重启后定时器重新计时** — 非计费场景可接受。
2. **不要海量 register**：滚动单 Timer 扫描全 State（或分片），而非每学员一个 ProcessingTimeTimer。
3. **心得**：教育 **会话收尾 / 状态 TTL** 是 ProcessingTime 定时器的典型战场。

---

### 三案例对照总表

| 案例 | 定时器类型 | 典型症状 | 核心实现 | 与 Demo 对应 |
|------|-----------|----------|----------|--------------|
| 课程订单催付 | **EventTime** | 超时未释放名额 | `orderTs + 15min` | O1/O2/O3 全流程 |
| 签到后无互动 | **EventTime** | 挂机未告警 | A 注册 / B 删除 | Step5 面试题 |
| 学习会话 TTL | **ProcessingTime** | 状态泄漏 OOM | 滚动扫描 + clear | Step4③ 重启差异 |

### 与其他 Demo 的关系

| 指南 | 本指南 |
|------|--------|
| Watermark（D2） | EventTime 定时器 **依赖 WM 推进** |
| Window（D3） | 固定窗口 vs **点对点超时** |
| Trigger（D4） | 窗口内 early-fire vs **窗口外** 业务定时 |
| Late Data | 乱序支付 → `[PAID-NO-PENDING]` |

**推荐学习顺序**：Watermark → Window → Trigger → **Timer/ProcessFunction** → Late Data。

---

## 与窗口 / Watermark Demo 的关系

| 组件 | 本 Demo 职责 |
|------|-------------|
| `forBoundedOutOfOrderness(5s)` | 乱序支付仍正确删 Timer |
| `OrderEventMonitorFunction` | 观察 WM 与定时器触发关系 |
| `flush` 事件 | 推进 WM，触发 O2/O3 超时（联动 D2） |
| `OrderTimeoutAlertFunction` | 窗口之外的 **per-order** 时序逻辑 |

两者结合：先理解 **WM 如何推进**（Watermark Demo），再理解 **如何用 Timer 在窗口之外做超时**（本 Demo）。
