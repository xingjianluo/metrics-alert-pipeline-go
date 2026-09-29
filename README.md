# metrics-alert-pipeline-go

一套本地可跑的「指标采集 → 窗口聚合 → 告警判定 → 状态流转」模拟链路，重点保证在
**并发写入、乱序/重复样本、规则热更新、背压与优雅退出** 下结论稳定、可解释。

- 纯标准库，无外部依赖（不需要数据库 / 消息队列）
- 所有关键路径线程安全，测试在 `-race` 下运行
- 判定输入与结果逐条打印日志，状态迁移与拒绝原因都可区分

## 快速开始

```bash
# 跑端到端演示（约 1 秒）：并发写入、基数拒绝、背压丢弃、热更新、触发/恢复、优雅退出
go run ./cmd/demo

# 全部测试（含竞态检测）
go test ./... -race -count=1
```

演示输出中四类前缀含义：

| 前缀 | 含义 |
| --- | --- |
| `[event ]` | 告警状态迁移事件（pending / fired / recovering / resolved） |
| `[reject]` | 样本被显式拒绝，带业务原因（基数超限 / 窗口已过期） |
| `[drop  ]` | 背压策略下的丢弃，带原因（queue_full / shutting_down） |
| `[rules ]` / `[stats ]` | 规则热更新提示、退出后的计数汇总 |

## 包结构

```
internal/model       样本 / 聚合 / 规则 / 事件等数据模型与纯函数
internal/aggregator  带标签写入、去重、乱序纠正、滚动窗口与基数限制
internal/evaluator   规则谓词判定与告警状态机（热更新保状态）
internal/pipeline    有界队列、背压策略、优雅退出的串联层
cmd/demo             本地端到端演示
```

## 数据模型

### 样本 Sample

```go
Sample{ Name, Labels map[string]string, Value float64, Timestamp time.Time }
```

一个样本由「指标名 + 标签集合」确定其所属**序列（series）**。序列标识是排序后拼接的
规范字符串（`model.Fingerprint`），因此标签 map 的迭代顺序不影响结果。

### 窗口聚合 Aggregate

按固定窗口长度（tumbling window）对每个序列聚合，输出：

```
Count, Sum, Min, Max, Last   （Last = 窗口内时间戳最大的那个样本的值）
WindowStart, WindowEnd
```

正确性约定：

1. **并发写入**：所有序列/窗口状态由互斥锁保护，同序列多 goroutine 并发写不会算错。
2. **乱序样本**：先到 00:50 再到 00:10 也没关系；`Min/Max` 基于值比较，
   `Last` 基于**时间戳**而不是到达顺序。
3. **重复上报**：同一「序列 + 样本时间戳（纳秒）」只计一次，首次值胜出；
   同时间戳但值不同也视为重复上报，避免重复计数（重复数内部记录）。
4. **水位线（watermark）**：窗口结束后再等待 `WatermarkLag` 才输出，容忍迟到；
   一旦窗口已输出，再到达该窗口的样本会被以 `sample_behind_watermark` 原因**显式拒绝**，
   而不是悄悄改写统计。
5. **基数限制**：活跃序列数达到 `MaxSeries` 时，新序列的写入返回
   `cardinality_limit_exceeded` 错误（含上限值和序列标识），**绝不静默丢弃、也不让内存无限膨胀**；
   已有序列继续正常写入。长期空闲（无在途窗口且超过 `IdleTTL`）的序列会被回收以释放基数。

### 规则 Rule

```go
Rule{
  ID, Metric, MatchLabels,           // 作用于哪些序列（MatchLabels 为子集匹配）
  Agg Aggregation,                   // last | avg | min | max | sum | count
  Op  ComparisonOp, Threshold,       // > >= < <= ==
  Window,                            // 对齐的判定窗口长度
  ForDuration,                       // 连续违规多久才触发
  KeepDuration,                      // 连续恢复多久才解除
}
```

判定时取聚合体上的 `Agg` 字段与 `Threshold` 比较，谓词为真记为该窗口「违规」。

### 告警身份与去重

告警实例键 = `RuleID + "|" + 序列指纹`（`model.AlertKey`）。因此「同一条规则作用于
不同标签序列」是不同告警；而同一告警在一次发作周期内**只会产生一次 `fired`**，
重复事件天然按身份去重（事件只在状态真正迁移时产生）。

## 告警状态机

```
                 违规窗口
   inactive ───────────────▶ pending
      ▲                        │  │
      │ 未持续够 For 就恢复     │  │ 连续违规累计 ≥ ForDuration
      │（计时清零，重新开始）   │  ▼
      └──────────────────────  fired
                                   │ 恢复窗口
                                   ▼
                               recovering
                                   │  │
              恢复期间又违规（抖动）│  │连续恢复累计 ≥ KeepDuration
                                   │  ▼
                                   └▶ (回到 firing，不产生新的 fired 事件)
                                      resolved（实例删除，回到 inactive）
```

要点：

- **ForDuration**：条件不是一碰阈值就报警，而是连续违规达到该时长才 `fired`；
  中途恢复会回到 inactive 并清零计时。
- **KeepDuration**：触发后不是一个好窗口就解除，需连续恢复达到该时长才 `resolved`。
- **抖动（flap）**：recovering 期间再次违规直接回到 firing，因为本次发作从未 resolved，
  所以**不会重复发 fired**；恢复计时重新开始。
- **计时口径（可解释）**：一个聚合窗口 `[WindowStart, WindowEnd)` 表示整个区间条件成立，
  连续时长按 `WindowEnd - streakStart` 计算。所以 `ForDuration=2 个窗口` 时，
  连续收到第 2 个违规窗口即触发。
- **静默恢复**：序列停止上报时，周期 tick（`EvaluatorTick`）会让 recovering 告警在
  KeepDuration 到期后照样 resolved，不依赖继续收到数据。

### 热更新（UpdateRules）

规则按 `ID` 整体替换：

- **ID 不变**（哪怕阈值/时长改了）：该规则下所有 pending/firing/recovering 实例的
  状态与计时**原样保留**，不会被清空、不会重复计数；下一个窗口起用新参数判定。
- **新增 ID**：从干净状态开始，不影响既有告警。
- **删除 ID**：该规则仍在活动中的告警被**恰好一次** force-resolved 后清理。

## pipeline：背压与优雅退出

### 有界队列与背压策略

采集入口是有界 channel，满时按策略处理（`BackpressurePolicy`）：

| 策略 | 行为 |
| --- | --- |
| `PolicyBlock` | 阻塞等待入队（对上游施加背压），不丢数据 |
| `PolicyDropOldest` | 挤掉队列中最旧的样本给新样本腾位 |
| `PolicyDropNewest` | 直接拒绝本次样本 |

任何丢弃都通过 `Drops()` 通道以 `DropEvent` 上报，原因可区分：

- `queue_full`：压力过大按策略丢弃；
- `shutting_down`：关闭开始后仍有提交（`Submit` 同时返回 `ErrShuttingDown`）。

注意它与聚合器拒绝是两条独立通道：`Rejects()` 上报业务拒绝
（`cardinality_limit_exceeded` / `sample_behind_watermark` / `aggregator_closed`），
`Drops()` 上报容量背压，二者不会混为一谈。

`Submit` 在整个入队期间持有读锁，`Shutdown` 持写锁关闭队列，因此不会出现
"向已关闭 channel 发送" 的 panic。

### 优雅退出（排队数据不丢一半）

`Shutdown(ctx)` 严格按顺序：

1. 置关闭位，拒绝新提交；
2. 关闭入队 channel，等待采集 worker **排空整个队列**（所有已接收样本都完成聚合）；
3. 停止水位线定时器，`aggregator.Close()` 把所有在途窗口**按最旧到最新 flush**；
4. 等判定 worker 消费完全部聚合，再关闭 events / drops / rejects 通道。

`ctx` 给出退出的最长等待时间。`Stats()` 可在退出后核对
`Submitted == 进入聚合的样本`（PolicyBlock 下无丢弃）。

### 延迟预算

`Config.LatencyBudget` 设定单个聚合块的判定耗时目标。每次判定都会测量耗时并记录
最大值（`EvalMaxMicros`）；超出预算时计数 `BudgetOverruns` 并打印告警日志，
而不是悄悄变慢。

## 配置项速查

### aggregator.Config

| 字段 | 含义 | 默认（0 值时） |
| --- | --- | --- |
| `Window` | 滚动窗口长度 | 1s |
| `MaxSeries` | 活跃序列基数上限，超限拒绝新序列 | 1000 |
| `WatermarkLag` | 窗口结束后再等多久输出，容忍迟到 | 1 个窗口 |
| `IdleTTL` | 空闲序列多久后被回收以释放基数 | 10 个窗口 |

### pipeline.Config

| 字段 | 含义 | 默认（0 值时） |
| --- | --- | --- |
| `IngestQueueSize` | 采集有界队列容量 | 1024 |
| `Backpressure` | 队列满策略：Block / DropOldest / DropNewest | Block |
| `EvaluatorTick` | 无数据时推进恢复计时的 tick 周期 | 100ms |
| `LatencyBudget` | 单块判定延迟目标，超期计数并告警 | 不检查 |

## 本地验证方法

```bash
# 1. 端到端演示
go run ./cmd/demo

# 2. 全部单测 + 竞态（建议多跑几轮）
go test ./... -race -count=3

# 3. 按场景单独验证
go test ./internal/aggregator/ -race -v   # 并发写/乱序/重复/基数/水位线/flush
go test ./internal/evaluator/ -race -v   # 状态机/抖动去重/热更新/判定日志/tick
go test ./internal/pipeline/  -race -v   # 并发+优雅退出/背压/拒绝原因/热更新/全链路
```

关键测试与需求对应：

| 场景 | 测试 |
| --- | --- |
| 同标签并发写入不乱算 | `TestConcurrentWritesSameSeries`、`TestConcurrentWritesThenGracefulShutdown` |
| 乱序 / 重复 / 冲突重复 | `TestOutOfOrderAndDuplicates` |
| 标签隔离 | `TestLabelIsolation` |
| 基数超限拒绝 + 原因 | `TestCardinalityRejection`、`TestCardinalityRejectedEndToEnd` |
| 水位线迟到拒绝 | `TestWatermarkEmitsAndRejectsLate` |
| 退出 flush 不丢窗口 | `TestCloseFlushesEveryWindow`、`TestConcurrentWritesThenGracefulShutdown` |
| 持续/恢复时长、状态迁移 | `TestFullStateMachineFlow`、`TestPendingInterruptResets` |
| 抖动不重复 fired | `TestFlapDoesNotDoubleFire`、`TestEndToEndAlertLifecycle` |
| 热更新保状态 / 不重复计数 | `TestHotUpdatePreservesState`、`TestHotUpdateDuringEvaluation` |
| 背压原因区分 | `TestBackpressureDropsDistinguishReason` |
| 判定输入与结果日志 | `TestDecisionLoggingShowsInputAndResult` |
| 延迟预算 | `TestEndToEndAlertLifecycle`（断言 `BudgetOverruns == 0`） |
