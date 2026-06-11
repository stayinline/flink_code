# Flink CDC 选型与乱序治理 — 学习指南

> 配套代码：`FlinkCdcOutOfOrderDemoJob` + `FlinkCdcOutOfOrderDemoJobTest`  
> 前置：`FlinkCdcSnapshotDemoJob`（增量快照原理，见 `FlinkCdcSnapshotDemoGuide.md`）  
> 场景：在线教育教务 `student_enrollment` 同主键多次 UPDATE 乱序到达

---

## 读前扫盲：CDC 乱序 ≠ Watermark 迟到

| 维度 | D3 迟到数据（Event Time） | CDC 乱序（变更日志） |
|------|---------------------------|---------------------|
| **乱序原因** | 网络/客户端 ts 乱序 | 多分区 Kafka、重试、跨表延迟 |
| **表现** | 窗口算错 | **旧 UPDATE 覆盖新状态** |
| **治理** | allowedLateness + 侧输出 | **单调版本 + last-write-wins** |
| **下游** | 窗口重算 | ReplacingMergeTree / argMax |

两者都要做，解决的是**不同层面**的一致性问题。

---

## Step 1 原理：CDC 工具选型

### 四维对比表

| 工具 | 全量能力 | 分布式 | 生态 | 语义 |
|------|----------|--------|------|------|
| **Canal** | 无内置全量（常配合 dump） | Server 集群 HA 成熟 | 阿里系 → Kafka/RocketMQ | at-least-once，幂等靠下游 |
| **Maxwell** | 无全量（bootstrap 可选） | 轻量单进程 | Kafka JSON 简洁 | at-least-once |
| **Debezium** | 快照 initial/blocking | Kafka Connect 分布式 | 多 DB、Connect 生态 | EOS（Connect+Kafka EOS） |
| **Flink CDC** | **增量快照 FLIP-27 无锁** | **Flink 集群** | **Flink SQL/CK 一体** | **Flink CK exactly-once** |

代码：`CdcToolSelectionMatrix.build()`（单测可打印完整表）

### 选型一句话

```
要「全量+增量一体」进 Flink 实时计算     → Flink CDC
已有 Kafka 中枢、多下游、轻量采集         → Canal + Kafka（或 Maxwell）
要 Kafka Connect 统一运维、多库插件         → Debezium
```

---

## Step 2 实操：last-write-wins

### 单调版本字段（必选其一）

| 版本源 | 公式 | 优点 | 风险 |
|--------|------|------|------|
| **binlog file+pos**（推荐） | `fileNo×1e9+pos` | 与 MySQL 写入顺序一致 | 需解析 source 元数据 |
| **Debezium ts_ms** | 事件时间戳 | 简单 | 多源/时钟不一定单调 |
| **db updated_at** | 业务列毫秒 | 业务可读 | 须保证每次 UPDATE 递增 |

实现：`CdcVersionResolver` + `CdcLastWriteWinsFunction`

### 关键代码

```java
// 侧输出 / Kafka 解析后
stream.keyBy(CdcChangeEvent::getPrimaryKeyId)
    .process(new CdcLastWriteWinsFunction("binlog_pos"))
    .name("LastWriteWins");
```

```java
// 核心逻辑
if (incomingVersion < maxVersion) {
    // [LWW-SKIP] 旧值不能覆盖新值
    return;
}
maxVersionState.update(incomingVersion);
out.collect(snapshot);
```

### 乱序实验数据（Test 故意乱序发送）

```
到达顺序：pos=300 dropped → pos=150 enrolled → pos=400 completed → pos=200 enrolled
最终 LWW：status=completed（pos=400）
```

### 运行步骤

```bash
kafka-topics.sh --create --topic test_flink_cdc_outoforder --partitions 1 \
  --bootstrap-server 192.168.1.124:9092

org.example.job.cdc.FlinkCdcOutOfOrderDemoJob binlog_pos 2 hashmap

mvn test -Dtest=FlinkCdcOutOfOrderDemoJobTest#sendOutOfOrderCdcEvents
```

### 预期日志

```
[LWW-ACCEPT] pk=1001 version=... op=u status=dropped
[LWW-SKIP] pk=1001 incoming=... < max=... | 旧值不能覆盖新值
[LWW-ACCEPT] pk=1001 ... status=completed
[LWW-SKIP] pk=1001 ... status=enrolled
LWW结果> EnrollmentSnapshot{... status=completed ...}
```

---

## Step 3 场景：Flink CDC vs Canal+Kafka

### 选 Flink CDC

| 场景 | 原因 |
|------|------|
| MySQL → Flink 宽表/双流 Join | 全增一体，无需先 dump |
| 要 Flink CK 断点、与算子状态一体 | Source 位点在 CK |
| 增量快照大表无锁 | FLIP-27 chunk |
| SQL/Table API 为主 | 原生 `mysql-cdc` 连接器 |

### 选 Canal + Kafka

| 场景 | 原因 |
|------|------|
| 公司已有 **Kafka 数据中台** | 一份 binlog 多订阅 |
| 下游有 Java/Python/数仓批 | 与 Flink 解耦 |
| 只需采集，计算不在 Flink | 更轻 |
| 运维熟悉 Canal HA | 成熟案例多 |

### 混合架构（常见）

```
MySQL ──Canal──→ Kafka(ods_binlog) ──→ Flink 消费 + LWW
         │
         └── 离线仓、搜索索引同时订阅
```

Flink 侧仍要做 **LWW**，Canal 不保证消费顺序。

---

## Step 4 陷阱

### ① 无版本字段 → 旧覆盖新

```
T1: UPDATE status='paid'   (先到达)
T2: UPDATE status='refund' (后到达但更早版本)
无 LWW → 最终 paid ❌
```

**必须有单调 version**（binlog pos / updated_at / 业务 version 列）。

### ② DELETE 事件

- `op=d` 且版本最大 → 下游应 **逻辑删除** 或物理删除  
- 乱序：delete 先到、旧 update 后到 → LWW 必须 **拒绝旧 update**  
单测：`lastWriteWins_deleteWinsWhenNewest`

### ③ 多表关联延迟不一致

```
enrollment 表先到 status=enrolled
student 维表后到 grade=高三
Join 时维表为空 → 误用默认值
```

**对策**：维表 CDC 单独 LWW；Join 用 **处理时间/版本** 对齐；或数仓层 **延迟关联**。

### ④ 与 D3 迟到、ClickHouse 串联

| 层 | 手段 |
|----|------|
| Flink 实时 | `CdcLastWriteWinsFunction` |
| 窗口迟到 | `FlinkLateDataDemoJob` 侧输出补偿 |
| ClickHouse 落库 | `ReplacingMergeTree(replay_version)` 或 `argMax(status, version)` |
| T+1 对账 | 源表 count vs 湖仓 count |

```sql
-- ClickHouse 示例
CREATE TABLE enrollment_dwd (
  id UInt64,
  student_id String,
  enroll_status String,
  version UInt64
) ENGINE = ReplacingMergeTree(version)
ORDER BY id;

-- 查询最新
SELECT id, argMax(enroll_status, version) AS status
FROM enrollment_dwd GROUP BY id;
```

---

## Step 5 面试话术

> **CDC 数据到下游乱序了，怎么保证最终是最新值？**

1. **先定性**：是 Kafka 多分片乱序、重试重复，还是跨表延迟——不是 Event Time 窗口问题，是 **变更日志顺序** 问题。  
2. **版本字段**：优先 **binlog file+pos**；或业务 `updated_at` / `version` 列（须单调递增）。  
3. **Flink 治理**：`keyBy(主键)` + `ValueState<maxVersion>`，**last-write-wins**，旧事件 `[LWW-SKIP]`。  
4. **DELETE**：当作版本最大的一种 op，下游标记删除；旧 update 不能复活已删行。  
5. **落库兜底**：ClickHouse `ReplacingMergeTree(version)`，查询 `argMax`。  
6. **选型**：采集用 Canal+Kafka 或 Flink CDC 均可，**乱序治理在 Flink/湖仓层都要做**。

---

## 验收清单

| # | 验收项 | 验证方式 |
|---|--------|----------|
| ① | 产出选型表 | Step1 表 + `toolSelection_matrixCoversFourProducts` |
| ② | LWW 版本字段设计 | `CdcVersionResolver` + Step2 表 |
| ③ | 乱序实验 | `sendOutOfOrderCdcEvents` + Job 日志 |
| ④ | Flink CDC vs Canal | Step3 |
| ⑤ | 与 D3/CH 串联 | Step4④ + 单测 `integration_lateDataAndReplacingMergeTree` |

---

## Step 7 在线教育典型业务案例

> **选课状态同步** / **订单退费乱序** / **学员档案多表 Join**

---

### 案例一：选课状态 Canal→Kafka→Flink — 乱序 UPDATE

#### 业务背景

教务 MySQL `student_enrollment` 经 **Canal** 推 Kafka，Flink 维护「学员在学课程」Redis 缓存。  
弱网重试导致 **同一 id 的 UPDATE 乱序到达**。

#### 故障

```
先到：status=dropped（pos=500）
后到：status=enrolled（pos=200，更旧）
无 LWW → 缓存显示 enrolled，家长端仍显示已选课 ❌
```

#### 方案

```
Kafka → parse Debezium/Canal JSON
     → keyBy(id) → CdcLastWriteWinsFunction(binlog_pos)
     → Redis SET status
```

**与 Demo 映射**：`FlinkCdcOutOfOrderDemoJobTest` 乱序四条。

#### 运维

| 监控 | 说明 |
|------|------|
| `lww_skip_total` | 乱序被正确丢弃的次数 |
| Kafka lag | 与乱序无关但需联合看 |

---

### 案例二：订单退费 — DELETE 与 UPDATE 竞态

#### 业务背景

订单表：`paid` → `refund` → 逻辑删除。CDC 中 `u` 与 `d` 乱序。

#### 规则

- 版本最大为 `d` → 下游 **无此订单**  
- 旧 `u(paid)` 在 `d` 之后到达 → **LWW-SKIP**

**与 Demo 映射**：单测 `lastWriteWins_deleteWinsWhenNewest`。

#### ClickHouse 归档

```sql
ReplacingMergeTree(version) ORDER BY order_id
-- 退费报表查 argMax(amount, version)
```

---

### 案例三：学员表 + 选课表 — 多表 CDC 延迟不一致

#### 业务背景

`student` 表（年级）与 `student_enrollment`（选课）分别 CDC，Flink 实时 Join 出「高三学员选课列表」。

#### 故障

```
enrollment 先到：S1001 选 C_MATH
student 晚到 5min：S1001 grade=高三
Join 时 grade 为空 → 进错「初三」营销桶
```

#### 对策

1. 各表独立 **LWW + 独立 Kafka topic**  
2. Join 用 **interval join / 维表 TTL**（见 Dim Join Demo）  
3. 报表以 **T+1 数仓关联** 为准，实时仅近似  

**与 Demo 映射**：Step4③ 多表延迟。

---

### 三案例对照总表

| 案例 | 问题 | 治理 | Demo |
|------|------|------|------|
| 选课乱序 | 旧 UPDATE 覆盖 | binlog LWW | OutOfOrder Test |
| 退费 DELETE | u/d 竞态 | LWW + 逻辑删 | delete 单测 |
| 多表 Join | 表间延迟 | 分表 LWW + 延迟 Join | Step4③ |

---

## 与相关 Demo 的关系

| 指南 | 关系 |
|------|------|
| **FlinkCdcSnapshotDemoGuide** | 采集层：全量+增量怎么来 |
| **本指南** | 采集后：乱序怎么治、工具怎么选 |
| **FlinkLateDataDemoGuide** | Event Time 迟到（窗口维） |
| **FlinkDimJoinDemoGuide** | 多表 CDC 维表 Join |
| **数据质量 Demo** | DLQ + 对账 |

建议路径：**增量快照(D18) → 选型与乱序(本指南) → Dim Join**。
