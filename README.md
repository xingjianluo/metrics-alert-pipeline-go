# metrics-alert-pipeline-go

本地可跑的指标采集与告警判定流程，模拟从采样、窗口聚合到告警产生与恢复的完整链路。
重点是在**并发写入、乱序/重复样本、规则热更新**下，结论稳定、可解释。

仅依赖 Go 标准库（含 `log/slog`），无外部服务依赖。

## 快速开始

```bash
# 跑全部测试（带竞态检测）
go test ./... -race -count=1

# 确定性端到端演示：触发 -> 热更新 -> 抖动不重复 -> 恢复 -> 基数拒绝 -> 优雅退出
go run ./cmd/demo -fast

# 带节奏的人类可读版本（每步 60ms）
go run ./cmd/demo
```

`demo` 结束时会自校验：必须恰好 3 个 Firing、3 个 Resolved，且观察到
`cardinality_limit_exceeded` 拒绝，否则以非 0 码退出。日志中每个判定周期
都打印 `eval input`（参与判定的窗口聚合输入）与 `eval result`（判定结果）。

## 目录结构

| 路径 | 职责 |
| --- | --- |
| `internal/model` | 核心数据类型：`Sample`、`SeriesKey`、`WindowAggregate`、`AlertState`、`AlertEvent`、拒绝原因 `RejectError` |
| `internal/store` | 按 series 存样本、按对齐窗口聚合；并发安全；基数上限；乱序/重复处理；窗口水位与 GC |
| `internal/rule` | 规则定义、标签过滤、字段选择与阈值谓词；规则快照校验 |
| `internal/engine` | 告警状态机（每 ruleID+series 一份状态）、持续时间判定、抖动去重、热更新保状态 |
| `internal/pipeline` | 有界队列 + ingest worker + 周期评估；背压/丢弃策略；优雅退出与最终一致性 flush |
| `cmd/demo` | 确定性本地演示 |

## 数据模型

### 样本与 series 标识

```
Sample{ Name, Labels map[string]string, Value, Timestamp }
SeriesKey = Name | k1=v1,k2=v2        // label key 排序后拼接，与 map 遍历顺序无关
```

同一 `(Name, Labels)` 即同一 series。写入时 label 会拷贝，调用方后续改 map
不影响存储。

### 窗口聚合

窗口按 `Timestamp.Truncate(WindowSize)` 对齐。窗口内按**样本时间戳去重**：

- 同一 series 且**同一时间戳**的重复上报 → last-write-wins，覆盖值，
  `Count/Sum/Min/Max` 绝不重复计数；
- 不同时间戳（含乱序到达）→ 正常计入对应窗口。

`WindowAggregate` 输出 `Count / Sum / Min / Max / Latest`（最新时间戳的值）。

### 乱序与水位

- 维护观察到的最新窗口 `latestWindow`；
- 样本窗口早于 `latestWindow - StaleWindows*WindowSize` 时拒绝，
  原因 `stale_sample`，错误信息带窗口与可接受下界；
- 在界内的迟到样本直接更新较早的窗口；
- 窗口只在 `windowEnd + StaleWindows*WindowSize <= now` 时才会被
  `Flush` 取出，因此 flush 出去的窗口不会再被合法迟到样本改变，结论可复现。
- 窗口数据**只由 Flush 删除**；GC 只兜底清理超出水位
  `(StaleWindows+1)` 个窗口的滞留窗口（停止上报的 series），不会回收
  尚未投递的窗口。

### 基数限制

`Store.Options.MaxSeries` 是当前持有窗口的不同 series 数硬上限。
新 series 会使计数超限时，写入返回：

```
cardinality_limit_exceeded: new series <key> rejected; at cardinality limit <n>
```

已存在的 series 仍可正常写（拒绝只针对新增）。拒绝进入 pipeline 统计
`Stats.RejectedBy[RejectCardinality]`，不会静默丢弃，也不会无限膨胀。

## 告警状态机

告警身份 = `ruleID + SeriesKey`。每个身份独立维护一份状态：

```
inactive ──条件成立──────────────> pending
pending  ──持续 ForDuration──────> firing     (发 Firing 事件，仅一次)
pending  ──条件不成立────────────> inactive   (待触发计时作废，不发事件)
firing   ──条件不成立────────────> resolving
resolving──恢复 KeepResolvedFor──> inactive   (发 Resolved 事件)
resolving──条件又成立────────────> firing     (抖动回归，不发新 Firing)
```

- **持续语义按窗口边界计量**：`ForDuration=2s`、窗口 1s 时，需要 3 个连续
  命中窗口（第 1 个命中窗口结束为 run 起点，第 3 个窗口结束满足 `>=2s`）。
  恢复同理。
- **事件去重**：一次 firing episode（首次 Firing 到 Resolved 之间）内无论
  抖动多少次，只产生一个 Firing；episode 结束才允许下一次 Firing。
  `AlertEvent` 带 `FiredAt/ResolvedAt`，恢复事件同时回填本 episode 的
  首次触发时间，可解释“这是哪一次告警恢复了”。
- 未命中不要求该周期有数据：该 rule 没参与某窗口判定的 series 不被推进
  （无数据不改变状态）。

### 规则表达

```go
rule.Rule{
    ID: "high_cpu", Metric: "cpu_usage",
    LabelFilter: map[string]string{"host": "a"}, // 可选；空=所有 series
    Field:  rule.FieldAvg, // avg|latest|sum|min|max
    Op:     rule.OpGT,     // gt|ge|lt|le|eq|ne
    Threshold: 0.8,
    ForDuration:     2 * time.Second,
    KeepResolvedFor: 2 * time.Second,
}
```

### 热更新

`Pipeline.UpdateRules(rule.Snapshot{Version, Rules})` 与评估串行：

- 新旧快照中 **ID 相同的规则，其全部 series 状态原样保留**（pending 计时、
  firing episode 都不重置、不重复计数），即使阈值/运算符被修改；
- 被删除规则的状态退役；新增规则从空状态起步；
- 新快照先 `Validate()`（字段合法、ID 无重复），失败则当前规则集不变。

## 背压、丢弃与优雅退出

- 采集侧是固定容量 FIFO 环形队列（不使用 channel，是为了支持队头驱逐）：
  - `PolicyBlock`：队列满时 `Submit` 阻塞等待空位，纯背压，不丢数据；
  - `PolicyDropNewest`：立即拒绝新样本，原因 `queue_full`；
  - `PolicyDropOldest`：驱逐最旧样本（计数到 `Stats.QueueDropped`，与
    其他拒绝原因区分），新样本入队。
- 拒绝原因统一为 `RejectError{Reason}`：`invalid_sample / stale_sample /
  cardinality_limit_exceeded / queue_full`，`Stats.RejectedBy` 分类计数。
- `Stop(ctx)`：关闭队列（新写入显式失败）→ 等所有 worker 把队列中剩余
  样本**全部 ingest**（Pop 在 Close 后仍可取完，返回 false 才退出）→
  最后再做一次确定性 flush + evaluate。排队数据不会丢一半；`ctx` 限定
  总排空时间。

### 延迟预算

设窗口大小 `W`、容忍迟到 `StaleWindows=k`、评估 tick 间隔 `F`：
一个窗口最晚在 `windowEnd + kW` 变为可投递，其判定结果最晚在其后一个
tick 产出，即端到端延迟上界约为 `kW + F`（另加队列等待与处理时间）。
`k` 是“迟到容忍”与“判定延迟”的显式权衡：`k=0` 延迟最低但不容迟到。

## 日志（判定输入与结果）

- `eval input`：series、labels、窗口起点、`count/sum/avg/latest`；
- `eval result`：rule、series、迁移后状态、判定值、`eval_time`、
  `fired_at/resolved_at`；
- `ALERT EVENT`（WARN）：只对 Firing/Resolved 输出；
- `sample rejected`（WARN）：被拒绝的样本与明确原因。

## 测试覆盖

```bash
go test ./... -race -count=1 -v          # 全部
go test ./internal/pipeline/ -race -v    # 端到端场景
```

- **并发写入**：20 goroutine × 同 series，`-race` 下窗口 Count 精确
  （重复时间戳合并），无重复计数（`store_test.go`、`pipeline_test.go`）。
- **乱序样本**：界内容忍并正确并入旧窗口；越界返回 `stale_sample`。
- **重复上报**：同时间戳 last-write-wins。
- **基数超限**：第 4 个 series 被拒，原因可区分，已有 series 不受影响。
- **判定中热更新**：pending 状态与计时保留，Firing 恰好 1 次，不重复计数。
- **告警反复抖动**：resolving 中命中不产生第二个 Firing；恢复计时被抖动
  重置，最终恰好一次 Firing + 一次 Resolved（`engine_test.go`）。
- **优雅退出**：50 个排队样本全部被 accepted，不丢一半。
- **队列策略**：FIFO、`drop_newest` 的 `queue_full`、`drop_oldest` 驱逐
  计数、Close 唤醒阻塞写者。

## 设计取舍

- 单把 `RWMutex` 保护 store、单 mutex 保护引擎：本地规模下简单且结论
  可解释；要扩展吞吐可按 series 分片，但同一 series 仍需串行。
- 无数据窗口不推进状态：避免“停报即误恢复”。是否需要专门的 no-data
  告警规则可在 rule 层另加语义。
- 基数限制按“当前持有窗口的 series”计数；停止上报的 series 在窗口
  滞留期后自然退役，长期 series 数仍受 `MaxSeries` 约束。
