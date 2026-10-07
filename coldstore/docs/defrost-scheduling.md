# 带时间窗的化霜排程

## 两个必须先定的策略（已固定并随每次修订记录）

| 决策点 | 取值 | 理由 |
| --- | --- | --- |
| 重排方式 | **滚动重排（rolling）** | 化霜是事件驱动的（提前完成、超时、温度变化、插单）。冻结窗内已下发/在执行的槽原样携带，只对冻结边界之后的尾部重排，避免一个过期计划跑到下一周期。每次重排生成新的、不可变的**修订号（revision）**。 |
| 相邻化霜 | **串行（serial），禁止重叠** | 同一房间下一次化霜不得早于上一次「化霜结束 + 回温等待」。该约束既作用于计划（冻结槽的 `recovered_at` 占用房间忙闲），也作用于真实执行（`ExecutionTracker.finish` 后，该房间的窗口开门时间被推到 `actual_finish + recovery_seconds`）。不同房间只受并发容量限流，允许并发。 |

两个策略值（`policy_reschedule=rolling`、`policy_adjacency=serial`）写在每个 `TimelinePlan` 上，审计和 CSV 导出都带，保证结果可按当时策略回看。

## 排队与约束

- **排队键**：`due_at` 升序（回温时间/窗口截止，最早必须开工的优先），`-temp`（更热优先）次之，`room_id` 兜底。温度只作为同回温优先级下的次序，不再是唯一依据。
- **每个作业**：`ready_at`（窗口开门）、`due_at`（窗口截止）、`defrost_seconds`（一次化多久）、`recovery_seconds`（化完等回温多久），均可按房间覆盖。
- **并发限流**：固定容量泳道，默认 `concurrency=2`，同一时刻重叠化霜数不得超过它。槽上记录 `reason`：`window-open` / `capacity-wait` / `room-recovery-wait` / `frozen-carried`。
- **晚于窗口**：`start_at > due_at` 标记 `late=true`，计划仍给出但显式可见。
- **计算量**：排序 + 泳道贪心，`O(n log n)`，单次修订硬上限 `MAX_JOBS=200`，历史保留 `MAX_HISTORY=50` 个修订。不引入 CP/SAT 求解器。
- **一致性**：每个修订生成后由 `verify_plan` 全量复核（窗口、并发、串行回温、冻结承诺、时间算术、规划视界），违例写入 `violations` 且 `consistent=false`，不会静默发布不一致排程。

## 冻结与执行一致性

- `/schedule/start` 只接受当前修订上的房间，并强制：
  - 未到窗口开门（超出容差 `tolerance_seconds`）→ 拒绝；
  - 在执行中重复开始 → 拒绝；
  - 并发数已满 → 拒绝（超容量不会发生）。
- 晚于 `due_at` 的开工允许但记 `late`，并写一条 `schedule-deviation` 审计。
- `/schedule/complete` 记录实际结束时间；提前/异常结束进入审计，并在下一次滚动重排时用于回温串行计算。
- 冻结来源以**执行记录为唯一真相**：已实际结束或从未启动且已过期的槽不冻结，滚动尾部重新排队。

## 审计与回看

审计 kind：
- `schedule-plan`：每次修订（revision、trigger、槽数、冻结数、late 数、consistent、violations）；
- `schedule-exec-start` / `schedule-exec-finish`：实际执行；
- `schedule-deviation`：错过窗口等偏差。

回看入口：
- `GET /schedule/timeline`：最新修订 + 配置 + 执行记录；
- `GET /schedule/history`：修订列表（`brief` 只看摘要）；
- `POST /schedule/revision {"revision": n}`：打开任一历史修订；
- `POST /export/schedule {"revision": n}`（缺省最新）：自描述 CSV，头部带修订/策略/容量/一致性，正文是排程表，可归档回看；
- `GET /export/audit`：审计 CSV（含排程原因与实际执行）。

## 触发原因（trigger）

`initial` / `manual` / `rolling-temp` / `rolling-complete` / `rolling-insert`。
重排入口为 `POST /schedule/generate`，可带 `trigger`、`reason`、`now`（确定性测试）、每房间 `defrost_seconds`/`recovery_seconds`/`ready_at`/`due_at` 覆盖、`concurrency`、`freeze_seconds`。

## 配置（均持久化，启动播种默认值）

`concurrency=2`、`recovery_seconds=600`、`response_seconds=3600`、
`freeze_seconds=300`、`tolerance_seconds=300`、`horizon_seconds=86400`。

对应端点：`/schedule/capacity`、`/schedule/recovery`、`/schedule/response-window`、
`/schedule/freeze`、`/schedule/tolerance`、`/schedule/horizon`。
