# Flink 数据质量与补偿重跑 — 学习指南

> 配套代码：`FlinkDataQualityDemoJob` + `FlinkDataQualityDemoJobTest`  
> 链路：Kafka Source → **数据校验** → 主流聚合 → **幂等汇总表** + **脏数据侧输出 → DLQ**

---

## 读前扫盲：Exactly-Once ≠ 业务最终一致

| 概念 | 保证什么 | 不保证什么 |
|------|----------|------------|
| **Flink Exactly-Once** | Checkpoint 内算子状态、Source offset **技术一致性** | 脏数据自动变干净 |
| **业务最终一致** | 家长看到的时长、账单金额 **正确且可追溯** | 仅靠 EO 自动达成 |

实时链路还要处理：**脏了、漏了、重复了、晚到了** → 靠 **Side Output + DLQ + 幂等写 + 对账** 兜底。

---

## Step 1 原理

### ① 脏数据识别

| 类型 | 规则 | 本 Demo |
|------|------|---------|
| **解析失败** | JSON 不可解析 | `{invalid json` |
| **字段缺失** | studentId/courseId/ts 空 | `MISSING_FIELD` |
| **枚举非法** | eventType 不在白名单 | `INVALID_ENUM` |
| **时间异常** | 秒级 ts、超前、超 30 天 | `TIME_ANOMALY` |
| **业务主键缺失** | video_progress 无 eventId | `MISSING_BIZ_KEY` |

实现：`DataQualityValidator` + `DataQualityIngressFunction`

### ② Side Output — 主流与异常流分离

```java
public static final OutputTag<DirtyDataRecord> DIRTY_DATA_TAG =
        new OutputTag<DirtyDataRecord>("dirty-data") {};

validatedStream.getSideOutput(DIRTY_DATA_TAG)
        .addSink(new DlqSinkFunction());
```

### ③ DLQ — 可追踪、可修复、可回放

`DirtyDataRecord` 字段：

| 字段 | 用途 |
|------|------|
| `dlqId` | DLQ 主键 |
| `reason` | 失败原因枚举 |
| `rawPayload` | 原始 JSON，修复后回放 |
| `replayVersion` | 补数版本 |
| `detectedAtMs` | 审计：处理时间 |
| `replayable` | 是否可回放 |

### ④ 幂等补数

```
dedupKey = statDate | studentId | courseId
replayVersion：normal=1, replay-v2=2, backfill=3
→ ReplacingMergeTree / UPSERT 保留高版本
```

实现：`IdempotentSummarySinkFunction`

### ⑤ 对账

```
源端发送 count/sum
  ≟ 校验通过 count/sum + DLQ count
汇总层 hash/dedupKey 与明细层定期核对
```

实现：`ReconciliationReporter`（Job 日志 + 单测）

---

## Step 2 实操对照

| 要求 | 实现 |
|------|------|
| 校验 + 侧输出 | `DataQualityIngressFunction` |
| DLQ Sink | `DlqSinkFunction` |
| 幂等结果表 | `IdempotentSummarySinkFunction` |
| 脏/重/漏/回放 | `FlinkDataQualityDemoJobTest` Phase1~5 |
| 静默丢弃对照 | `drop hashmap` 模式 |

### 关键代码

```java
// 校验 + 侧输出
SingleOutputStreamOperator<StateDemoEvent> validated = source
    .process(new DataQualityIngressFunction(dlqEnabled));

validated.getSideOutput(DIRTY_DATA_TAG).addSink(new DlqSinkFunction());

// 主流 → 聚合 → 幂等写
validated.keyBy(StateDemoEvent::getStudentId)
    .process(new QualityAwareAggregateFunction())
    .addSink(new IdempotentSummarySinkFunction());
```

### 创建 Topic

```bash
kafka-topics.sh --create --topic test_flink_quality --partitions 2 \
  --bootstrap-server 192.168.1.124:9092
kafka-topics.sh --create --topic test_flink_quality_dlq --partitions 1 \
  --bootstrap-server 192.168.1.124:9092
```

### 启动 Job

```bash
# A：严格模式 + DLQ + 幂等（推荐）
org.example.job.quality.FlinkDataQualityDemoJob strict hashmap

# B：静默丢弃（对照）
org.example.job.quality.FlinkDataQualityDemoJob drop hashmap

# C：回放补数（独立 consumer group）
org.example.job.quality.FlinkDataQualityDemoJob replay hashmap
```

### 发送测试数据

```bash
mvn test -Dtest=FlinkDataQualityDemoJobTest#sendDataQualityDemoEvents
```

### DLQ 回放流程（运维）

```
1. 查询 DLQ：reason、rawPayload
2. 人工/脚本修复 JSON
3. 写入修复 topic 或原 topic，tag=replay-v2，限流
4. 幂等表按 dedupKey + 更高 replayVersion 覆盖
5. 对账确认 count/sum PASS
```

---

## Step 3 对比表

| 策略 | 可追溯 | 可修复 | 适用场景 | 不适用 |
|------|--------|--------|----------|--------|
| **直接丢弃** | ❌ | ❌ | 低价值 PV、采样监控 | 计费、学时、证书 |
| **侧输出** | ⚠️ 仅日志 | ⚠️ | 过渡方案 | 无 DLQ 存储 |
| **DLQ** | ✅ | ✅ | 所有有价数据 | — |
| **人工修复回放** | ✅ | ✅ | 少量脏数据 | 大批量需自动化 |
| **自动补偿重跑** | ✅ | ✅ | 漏数、对账差异 | 需幂等键先行 |

### 哪些可以丢、哪些必须 DLQ、哪些必须补数

| 数据类型 | 策略 |
|----------|------|
| 大屏 PV 估算 | 可丢弃 / 采样 |
| 学习时长、订单金额 | **必须 DLQ + 幂等 + 对账** |
| 重复上报 | 幂等 dedupKey / eventId 去重 |
| 历史漏数 | **补数 replay + 对账** |

---

## Step 4 调优 / 陷阱

### ① 侧输出不是最终兜底

侧输出只解决 **分流**；必须接 **DLQ 存储**（Kafka topic / CH 表）和 **回放入口**。

### ② 补数必须设计幂等键

不能假设 Exactly-Once 消灭重复：**重试、回放、at-least-once 都会产生重复写**。  
`dedupKey + replayVersion` 或 `eventId` 唯一约束。

### ③ 历史回放隔离

| 项 | 建议 |
|----|------|
| 消费者组 | `flink-quality-replay-consumer` 独立 group |
| 结果表 | 同表靠 version 覆盖，或 `_replay` 临时表再 merge |
| 限流 | 回放 QPS 限制，避免打满集群 |

### ④ 对账区分三种时间

| 时间 | 对账用途 |
|------|----------|
| **Event Time** | 业务日期窗口、学时归属日 |
| **Processing Time** | DLQ detectedAt、延迟监控 |
| **业务日期 statDate** | dedupKey 组成部分 |

### ⑤ 修复数据副作用

修复回放可能：**再次迟到**、**覆盖旧汇总**、**触发撤回** → 需 product 知晓 + 审计字段。

---

## Step 5 面试话术

> **我们如何通过 Side Output + DLQ + 幂等写 + 对账，把 Exactly-Once 落到业务最终一致？**

1. **脏数据**：Ingress 校验失败 → `OutputTag` 侧输出 → DLQ 存 rawPayload + reason，**不静默丢计费数据**。  
2. **重复**：汇总层 `dedupKey` UPSERT / ReplacingMergeTree；补数带 `replayVersion`。  
3. **漏数**：对账发现源端 count > 接受 count → DLQ + 补发 + 回放限流。  
4. **EO 边界**：EO 保证 **Flink 状态与 offset**；业务正确靠 **幂等 + 对账**。  
5. **恢复步骤**：查 DLQ → 修复 → replay topic → 对账 PASS → 告警关闭。

---

## 测试数据计划

| Phase | 内容 | 目的 |
|-------|------|------|
| 1 | 正常心跳 | 主流 + 幂等写 |
| 2 | 解析/字段/枚举/时间/主键错误 | 五类脏数据 → DLQ |
| 3 | 重复 eventId | 聚合 vs 幂等覆盖 |
| 4 | replay-v2 | 补数覆盖 |
| 5 | backfill 新学员 | 历史补数 |

---

## 预期日志样例

**正常**

```
[DQ-OK] eventId=e01 student=S80001 course=C_ENG tag=normal
[DQ-AGG] StudySummary{... total=60s ...}
[IDEMPOTENT-SUMMARY] dedupKey=...|S80001|C_ENG v=1 total=60s
```

**脏数据 → DLQ**

```
[DQ-DIRTY] reason=PARSE_FAIL eventId=null | Unexpected character...
[DLQ-SINK] dlqId=dlq-xxx reason=PARSE_FAIL replayable=true
```

**补数覆盖**

```
[IDEMPOTENT-SUMMARY] dedupKey=... v=2 total=150s tag=replay-v2
```

**静默丢弃（drop 模式）**

```
[DQ-DROP] 静默丢弃 reason=MISSING_FIELD | ...（无 DLQ，不可追溯）
```

---

## 验收清单

| # | 验收项 | 验证方式 |
|---|--------|----------|
| ① | 手写侧输出脏数据 | `DataQualityIngressFunction` |
| ② | DLQ 表结构 + 回放流程 | `DirtyDataRecord` + Step2 回放 |
| ③ | 幂等主键设计 | `dedupKey` + `replayVersion` |
| ④ | EO 不能替代对账 | Step1 扫盲 + 单测 |
| ⑤ | 漏数/重复排查恢复 | Step5 话术 + Phase3/4 |

---

## Step 7 在线教育典型业务案例（数据质量三角）

> **学时埋点脏了** / **重复扣费** / **漏统计补数**  
> 与 Late Data、Exactly-Once、Kafka Connector 指南互补。

---

### 案例一：App 埋点脏 JSON — 学时不能静默丢

#### 业务背景

学员观看心跳上报 `study_behavior`，字段缺失、秒级时间戳、空 `studentId` 日均 **0.3%**。

#### 故障（若静默丢弃）

```
家长端「今日学习 0 分钟」
客服：孩子明明看了 2 小时
→ 无法追溯原始 payload
```

#### 方案

```
Kafka → Flink DataQualityIngress
  ├─ 主流 → 学时聚合 → ClickHouse ReplacingMergeTree(dedupKey)
  └─ 侧输出 → DLQ 表（rawPayload, reason, detectedAtMs）
```

**与 Demo 映射**：Phase2 五类脏数据；`strict` 模式。

#### DLQ 表结构（ClickHouse 示例）

```sql
CREATE TABLE study_behavior_dlq (
  dlq_id String,
  reason String,
  raw_payload String,
  detected_at DateTime,
  replay_version UInt32,
  fixed_at Nullable(DateTime)
) ENGINE = MergeTree ORDER BY (detected_at, dlq_id);
```

#### 踩坑

1. **侧输出只 print 不落库 = 没做 DLQ**。  
2. 脏数据也要 **告警**（reason 分布突增）。  
3. **心得**：教育有价数据 **宁可进 DLQ 也不能丢**。

---

### 案例二：重复上报与补数 — 幂等键 + replayVersion

#### 业务背景

弱网导致同一条 `eventId` 上报 3 次；运维修复后 **回放** 正确时长。

#### 现象

| 层 | 无幂等 | 有幂等 |
|----|--------|--------|
| Flink 状态 | 累加 3 次 | 仍可能累加（靠下游兜底） |
| 汇总表 | 180min 错 | dedupKey UPSERT 最终 60min |
| 补数回放 | 再加一次 | replay-v2 **覆盖** |

**与 Demo 映射**：Phase3 重复 + Phase4 `replay-v2`；单测 `idempotentSummary_replayVersionOverrides`。

#### 主键设计

```
dedupKey = statDate + studentId + courseId   -- 日汇总粒度
eventId    -- 明细去重粒度（可选上游去重）
replayVersion -- 补数优先级
```

#### ReplacingMergeTree

```sql
CREATE TABLE study_daily_summary (
  stat_date Date,
  student_id String,
  course_id String,
  total_watch_sec UInt64,
  replay_version UInt32,
  updated_at DateTime
) ENGINE = ReplacingMergeTree(replay_version)
ORDER BY (stat_date, student_id, course_id);
```

---

### 案例三：漏数对账 — T+1 源汇 count/sum 告警

#### 业务背景

Kafka 集群故障 **丢 15 分钟** 心跳；Flink 消费正常但 **源端未收到**。

#### 对账任务（每日）

```sql
-- 源端（Kafka 估算 / ODS 落地 count）
SELECT stat_date, count(*) FROM ods_study_behavior GROUP BY stat_date;

-- 汇总层
SELECT stat_date, count(*), sum(total_watch_sec) FROM study_daily_summary GROUP BY stat_date;

-- 差异 > 0.1% → 告警 → 触发补数 job
```

#### 恢复步骤

1. 定位缺口时间段（对账 + Kafka 监控）。  
2. 从 **备份 / 离线日志** 补发到 `replay` topic。  
3. `replay hashmap` 独立 consumer group，**限流 5000 QPS**。  
4. `replayVersion=3` 写入。  
5. 对账 PASS 后关闭工单。

**与 Demo 映射**：`ReconciliationReporter`；`reconciliation_countMatchesSentMinusDlq` 单测。

#### 陷阱

- 对账用 **业务日期** 而非 processing date。  
- 补数可能改变 **已出报表** → 需审计字段 `updated_at` + 运营通知。

---

### 三案例对照总表

| 案例 | 问题 | 手段 | Demo 对应 |
|------|------|------|-----------|
| 脏 JSON | 学时丢失 | Side Output + DLQ | Phase2 |
| 重复/补数 | 金额/时长错 | dedupKey + replayVersion | Phase3/4 |
| 漏数 | 源汇不一致 | 对账 + 回放限流 | RECON + Phase5 |

---

## 加分点速查

| 主题 | 要点 |
|------|------|
| **ReplacingMergeTree / UPSERT** | version 列决定保留行 |
| **修复流与主流** | 同表 merge 或独立 replay 流 |
| **replayVersion** | 补数优先级 |
| **审计字段** | detectedAt、updatedAt、fixOperator |
| **回放限流** | 保护 Kafka/Flink |
| **技术 vs 业务一致性** | EO vs DLQ+幂等+对账 |
| **自动对账告警** | count/sum/hash 阈值 |

---

## 与其他 Demo 的关系

| Demo | 关系 |
|------|------|
| **Late Data** | 迟到进侧输出补偿（时间维度） |
| **Exactly-Once** | 技术一致性；本 Demo 补业务层 |
| **Kafka Connector** | Source offset + 回放 topic |
| **Savepoint** | 发布时保证状态；不替代对账 |

建议：**Exactly-Once → Kafka → 本指南（数据质量）**。
