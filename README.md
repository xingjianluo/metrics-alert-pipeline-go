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

# 启用状态落盘的演示：退出时写检查点；同一目录再跑一次可看到恢复日志
# （重放的旧时间戳样本会按水位被明确拒绝为 stale_sample，而不是重复计数）
go run ./cmd/demo -fast -ckpt-dir /tmp/ckpt-demo
go run ./cmd/demo -fast -ckpt-dir /tmp/ckpt-demo   # 第二次：checkpoint restored ...
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
| `internal/pipeline` | 有界队列 + ingest worker + 周期评估；背压/丢弃策略；优雅退出与最终一致性 flush；检查点调度 |
| `internal/checkpoint` | 状态落盘与重启恢复：原子写入、校验和、多份留存、损坏回退、版本检查 |
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

## 状态落盘与重启恢复

设置 `Pipeline.Config.CheckpointDir` 即开启持久化：进程重启后自动从最近的
可用检查点接着算，而不是从零开始。

### 落盘范围

每份检查点是一个自包含的 JSON 文件（`ckpt-<时间戳>.json`），包含：

- **告警状态机**：每个 `(ruleID, series)` 的状态（pending/firing/resolving/
  inactive）、当前 run 的起点 `since`、本 episode 的首次触发时间 `firedAt`、
  以及 `episode` 标志（该告警是否已对外通告过——这是重启后不重复发
  Firing 的关键）；
- **未判定完的窗口数据**：所有尚未 flush 的窗口（精确到每个样本时间戳的
  值，重复合并语义不变）和乱序水位 `latestWindow`；
- **规则集**：落盘时的规则快照（版本 + 全部规则），用于恢复时的兼容判定
  与日志解释；
- **事件历史**：最近的 Firing/Resolved 事件（有界，见下文上限）。

### 落盘时机与性能

- 周期落盘由**独立 goroutine** 执行（`CheckpointInterval`，默认 10s），
  采集与判定只被一次短暂的状态拷贝阻塞，磁盘 I/O 不在关键路径上；
- `Stop()` 在最终 flush + evaluate 之后写最后一份检查点，优雅退出的状态
  就是下次启动的起点；
- 每次写入都是 **临时文件 + 原子 rename**，进程在写入中途崩溃不会留下
  半个可用文件。

### 恢复语义

- 待触发计时**不清零**：pending 的 run 起点 `since` 原样恢复，重启前累计
  的持续时间继续有效；
- 已触发未恢复的告警**不重发**：`episode` 标志恢复后，同一 episode 内
  不会再产生第二个 Firing；恢复计时（resolving）同样延续，Resolved 不丢；
- 同一告警的重复事件仍按 `(rule, series, episode)` 标识去重，与重启前一致；
- 事件历史随检查点恢复（`Pipeline.EventHistory()`），有保留上限。

### 与规则热更新/变更的兼容范围

恢复时套用与热更新**完全相同**的兼容规则（按 rule ID 对齐）：

- 新旧规则集里 **ID 相同的规则：状态原样延续**，即使阈值/持续时间被修改
  （修改后的谓词从下一次判定起生效；若新条件下原状态不再成立，按状态机
  正常迁移，结论可解释）；
- **被删除的规则**：其状态退役（日志逐条打印 `checkpoint state retired`），
  不再参与判定；
- **新增的规则**：从干净状态起步，需要完整走完自己的 `ForDuration`；
- 落盘数据的**格式版本**与当前不一致时：旧版本走迁移表（当前只有 v1，
  无可迁移对象），更新或无法识别的版本**明确拒绝启动**并打印原因
  （如 `format version 99 is newer than supported 1`），绝不悄悄按空状态
  继续跑。

### 损坏与空间上限

- 每份文件带 CRC32 校验和；加载时从**新到旧**依次尝试，损坏/截断/版本
  不符的文件被跳过（日志 `checkpoint skipped` 带原因），自动回退到上一个
  可用点；
- 目录里有文件但**全部不可用**时，`New` 返回错误拒绝启动
  （`refusing to start empty`），避免空状态造成误报/漏报；目录为空才是
  正常的首次启动（`ErrNoCheckpoint`）；
- **留存上限**：`MaxCheckpoints`（默认 3）份，超出后最旧的被清理，磁盘
  占用不无限增长；**事件历史上限**：`MaxEvents`（默认 256）条，只保留
  最近的；
- 恢复时若检查点是在更大基数上限下写入的，超出当前 `MaxSeries` 的 series
  被跳过并计数告警，不静默超限。

### 本地验证

```bash
# 重启恢复专项测试（场景日志可见）
go test ./internal/pipeline/ -race -count=1 -v -run 'TestRestart|TestCorrupt|TestCheckpoint'
go test ./internal/checkpoint/ -race -count=1 -v

# 手动演示：第一次跑写检查点，Ctrl-C 或跑完后用同一目录再跑一次，
# 日志出现 "checkpoint restored"，水位延续（重放的旧样本被 stale_sample 拒绝）
go run ./cmd/demo -ckpt-dir /tmp/ckpt-demo
go run ./cmd/demo -ckpt-dir /tmp/ckpt-demo
```


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
- **重启恢复**（`recovery_test.go`）：待触发计时跨重启延续、第 3 个命中
  窗口按时触发且全程只触发一次；已触发 episode 重启后不重发 Firing、
  Resolved 不丢；未 flush 的窗口数据重启后参与判定；事件历史随检查点保留。
- **检查点 + 规则变更**：同 ID 规则（改了阈值）状态延续且不误触发、被删
  规则状态退役、新增规则从干净状态起步。
- **损坏检查点**：最新一份损坏/截断自动回退到上一份；全部损坏拒绝启动
  并打印原因；格式版本过新明确拒绝（`checkpoint_test.go`、
  `recovery_test.go`）。
- **留存上限**：检查点份数与事件历史条数均有上限，超限自动清理。
- **恢复过程并发稳定**：周期落盘与并发写入/判定同时进行，`-race` 下
  稳定，检查点始终可读。

## 设计取舍

- 单把 `RWMutex` 保护 store、单 mutex 保护引擎：本地规模下简单且结论
  可解释；要扩展吞吐可按 series 分片，但同一 series 仍需串行。
- 无数据窗口不推进状态：避免“停报即误恢复”。是否需要专门的 no-data
  告警规则可在 rule 层另加语义。
- 基数限制按“当前持有窗口的 series”计数；停止上报的 series 在窗口
  滞留期后自然退役，长期 series 数仍受 `MaxSeries` 约束。
- 检查点采用“多份留存 + 加载时回退”而非单文件 + 备份：实现简单，
  且天然覆盖“最新一份写了一半”的场景；代价是磁盘占用约为单份的
  `MaxCheckpoints` 倍（默认 3 份，本地规模可忽略）。
- 恢复按 rule ID 对齐而不是按规则内容哈希：与热更新语义保持一致，
  “改了阈值但 ID 不变”被视为同一规则的延续，结论可解释；如果需要
  “改阈值即重置状态”，应换一个新的 rule ID。
