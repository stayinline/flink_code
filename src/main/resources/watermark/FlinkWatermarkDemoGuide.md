# Flink Watermark 传播与乱序 — 学习指南

> 配套代码：`FlinkWatermarkDemoJob` + `FlinkWatermarkDemoJobTest`  
> 数据源：`(eventId, userId, ts, amount, source, tag)`，Event Time + `forBoundedOutOfOrderness(5s)`

---

## Step 1 原理：Watermark = 事件时间进度的声明

### ① 单算子内 WM 如何推进

```
事件到达顺序（处理顺序）:  e1(ts=+10s) → e2(ts=+3s 乱序) → e3(ts=+8s)

maxEventTime 变化:           10s          10s(不回退!)      10s

当前 WM（outOfOrderness=5s）:
  e1 后: WM = 10 - 5 = +5s
  e2 后: WM = max(5s, 3-5) = 5s   ← ts 回退不会拉低 WM（加分点）
  e3 后: WM = max(5s, 8-5) = 5s

Tumbling 10s 窗口 [0,10):  当 WM ≥ 10s 时触发（需后续 +15s 类事件推进）
```

**公式**：`WM = max(上一WM, maxEventTime - outOfOrderness)`

### ② 多输入 union：下游取最小 WM

```
  Kafka(fast) ──→ WM_fast ──┐
                            ├── union ──→ WM_downstream = min(WM_fast, WM_slow)
  Kafka(slow) ──→ WM_slow ──┘

故障复现：
  fast 已到 WM=+17s，slow 从未收到数据 → WM_slow = Long.MIN_VALUE
  min(17s, MIN_VALUE) = MIN_VALUE → 下游 WM 不推进 → 窗口永不触发 ⚠️

修复：
  ① 向 slow 源补发数据（Phase4b）
  ② withIdleness(10s)：slow 10s 无数据则标记 idle，不再拖累 min(WM)
```

### ③ keyBy 后 WM 与 key 无关

```
union → WatermarkMonitor(parallelism=2) → keyBy(userId) → Window

- WM 在 subtask 级别传播，不按 key 拆分
- keyBy 只是 shuffle 数据；同一 subtask 上所有 key 共享同一 WM
- 窗口算子收到的 WM 来自上游 subtask 的输出 WM（取 min 对齐）
```

---

## Step 2 手写代码对照

| 要求 | 实现 |
|------|------|
| 乱序数据 ts 回退 | `FlinkWatermarkDemoJobTest` Phase1：+4s 后再发 +2s、+6s |
| forBoundedOutOfOrderness(5s) | `FlinkWatermarkDemoJob.buildWatermarkStrategy()` |
| 打印 currentWatermark | `WatermarkMonitorFunction` |
| 观察窗口触发 | `Tumbling 10s` + `WatermarkWindowLogFunction` |

### 关键代码

```java
// 打印每条事件到达时的 WM
stream.process(new WatermarkMonitorFunction());

// WM 策略
WatermarkStrategy.<WatermarkDemoEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
    .withTimestampAssigner((e, ts) -> e.getTs());
// 可选修复
// .withIdleness(Duration.ofSeconds(10))
```

---

## Step 3 复现故障 + 修复

### 故障 A：Kafka 空闲分区（Phase 2~3）

**现象**：只向 `test_flink_watermark` 的 **partition 0** 写数据，partition 1 沉默  
→ 该 consumer subtask 上 `WM = min(WM_p0, WM_p1) = MIN_VALUE`  
→ `[WINDOW-FIRED]` 长时间不出现

**修复方式 1**：`FlinkWatermarkDemoJob 10` 或 `-Dwatermark.idleness.sec=10`  
**修复方式 2**：测试 Phase4a 向 **partition 1** 发 `e09 ts=+25s`

### 故障 B：union 慢源沉默（Phase 3）

**现象**：`test_flink_watermark_slow` 无数据  
→ `WM_downstream = min(WM_fast, WM_slow) = MIN_VALUE`

**修复方式 1**：withIdleness(10s)  
**修复方式 2**：Phase4b 向 slow topic 发 `e10 ts=+28s`

### 对比实验

```bash
# 复现故障（无 idleness）
org.example.job.watermark.FlinkWatermarkDemoJob

# 自动修复（10s 空闲标记）
org.example.job.watermark.FlinkWatermarkDemoJob 10
# 或 -Dwatermark.idleness.sec=10
```

---

## Step 4 调优 / 陷阱

### ① 乱序度 out-of-orderness 权衡

| 设置 | 效果 |
|------|------|
| **太大**（如 30min） | WM 推进慢 → 窗口触发延迟高 → 大屏/告警滞后 |
| **太小**（如 0s） | 轻微乱序即判迟到 → 丢数据或侧输出暴增 |
| **经验** | 取 **P99 乱序延迟** + 20% buffer；Demo 用 5s |

### ② 周期性 WM vs 标点 WM

| 类型 | 机制 | 本 Demo |
|------|------|---------|
| **周期性** | `setAutoWatermarkInterval(1000)` 每 1s 基于 maxEventTime 发射 WM | ✅ 已启用 |
| **标点** | 数据源插入 `Watermark(ts)` 特殊记录（如 Kafka 自定义） | 未用 |

周期性 WM 在无新事件时也能「尝试」推进（基于已有 maxEventTime），但 **无法越过空闲分区/空闲源**——仍需 idleness。

### ③ WM 与 allowedLateness（预告）

```
窗口首次触发: WM ≥ window.end
allowedLateness > 0: WM ≥ window.end + lateness 前，迟到数据仍可更新窗口结果
本 Demo 未开 lateness → 迟到且 WM 已越过的事件被丢弃
```

---

## Step 5 面试话术：窗口不出数排查 Checklist

> **线上窗口不出数，我的排查顺序：**
> 1. **看 WM 是否推进** — Metrics / `[WM-MONITOR]` 日志，`currentWatermark` 是否长期 `MIN_VALUE` 或卡住  
> 2. **看是否有空闲分区/空闲源** — Kafka 某 partition 无数据、union 某支路沉默 → `min(WM)` 被拖死 → 加 `withIdleness`  
> 3. **看乱序设置** — `outOfOrderness` 过大导致 WM 滞后；过小导致有效数据被当迟到丢弃  
> 4. **看时间字段单位** — ts 是 **毫秒** 还是 **秒**（差 1000 倍会导致 WM 永远很小）  
> 5. **看是否误用 ProcessingTime** — 用了 `Time.milliseconds()` 窗口却走 ProcessingTime，与 Event Time 语义混用  
> 6. **看 keyBy 后是否有数据** — 某 key 无事件则该 key 无输出（不是 WM 问题但常混淆）

---

## 加分点：WM 为什么单调不减？

```
WM_new = max(WM_old, maxEventTime - outOfOrderness)
```

- `maxEventTime` 只增不减（取历史最大事件时间）  
- 乱序到达的 ts=+3s **不会**减小 maxEventTime（已是 +10s）  
- 因此 WM **永远不会回退**  
- 验证单测：`FlinkWatermarkDemoJobTest.outOfOrderness_watermarkIsMonotonic()`

---

## 测试数据发送计划

| Phase | 内容 | 目的 |
|-------|------|------|
| 1 | fast p0 乱序 +1,+4,+2,+8,+6 | WM 不回退 |
| 2 | 仅 p0 +12,+18 | 空闲 p1 卡住 WM |
| 3 | fast 继续，slow 沉默 | union min WM 卡住 |
| 4a | p1 发 +25s | 修复空闲分区 |
| 4b | slow 发 +28s | 修复 union 慢源 |
| 5 | p0 flush +35s | 触发 [10,20) [20,30) 窗口 |

---

## 运行步骤

### 0. 创建 Topic

```bash
kafka-topics.sh --create --topic test_flink_watermark --partitions 2 --bootstrap-server 192.168.1.124:9092
kafka-topics.sh --create --topic test_flink_watermark_slow --partitions 1 --bootstrap-server 192.168.1.124:9092
```

### 1. 启动 Job（二选一）

```bash
# 故障复现模式
org.example.job.watermark.FlinkWatermarkDemoJob

# 修复模式（withIdleness 10s）
org.example.job.watermark.FlinkWatermarkDemoJob 10
```

### 2. 发送测试数据

```bash
mvn test -Dtest=FlinkWatermarkDemoJobTest#sendWatermarkDemoEvents
```

### 3. 本地单测（无需 Kafka）

```bash
mvn test -Dtest=FlinkWatermarkDemoJobTest#outOfOrderness_watermarkIsMonotonic
```

---

## 预期日志样例

**乱序 WM 不回退**

```
[WM-MONITOR] ... eventId=e04 eventTs=...+8s | currentWM=...+5s
[WM-MONITOR] ... eventId=e05 tag=out-of-order eventTs=...+6s | currentWM=...+5s  ← 未回退到 +1s
```

**故障：窗口不出数（Phase 2~3，无 idleness）**

```
（仅有 [WM-MONITOR]，长时间无 [WINDOW-FIRED]）
```

**修复后窗口触发（Phase 5 或 idleness 超时后）**

```
窗口触发> [WINDOW-FIRED] userId=u001 | 窗口=[...+10s ~ ...+20s) | count=... sum=...
```

---

## 验收清单

| # | 验收项 | 验证方式 |
|---|--------|----------|
| ① | 能画 WM 多输入取 min 图 | 本文 Step1② + union 架构图 |
| ② | 复现并修复空闲分区不触发 | 无 idleness 复现 → 传参 `10` 或 Phase4a/b 修复 |
| ③ | 说清乱序度权衡 | Step4① 表格 + 面试话术 |

---

## 与窗口 Demo 的关系

| 窗口 Demo | Watermark Demo |
|-----------|----------------|
| 关注 Tumbling/Sliding/Session 差异 | 关注 WM 如何驱动窗口触发 |
| 假设 WM 正常推进 | 故意制造 WM 不推进故障 |
| `UserOrderEvent` | `WatermarkDemoEvent`（多 source/partition 标签） |

两者结合：先理解 **WM 传播**，再理解 **窗口类型**。
