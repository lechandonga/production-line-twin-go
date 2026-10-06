# 产线数字孪生（本地离散事件仿真）

纯 Go 标准库实现的本地可运行产线数字孪生组件：用一份 JSON 配置描述工位、
缓冲容量、工序流转、带随机波动的加工时长、设备故障与质检返工；仿真器按
时间推进，推演在制品（WIP）流转、设备异常与返工，输出产能、在制品统计和
每件制品的完整流转轨迹。

不连接真实设备、消息队列或数据库，不需要联网；`GOPROXY=off go build ./...`
即可离线编译。

## 目录结构

- `internal/twin/config.go`：配置结构、JSON 加载、深拷贝
- `internal/twin/model.go`：模型构建与全部构建期校验、随机时长抽样
- `internal/twin/errors.go`：可区分的错误类别（`errors.Is` 判断）
- `internal/twin/simulator.go`：确定性离散事件仿真器、故障/返工、统计与轨迹
- `cmd/twin`：本地命令行入口
- `examples/line.json`：示例产线（2 台 CNC、质检返工、计划故障）

## 快速开始

```bash
go run ./cmd/twin -config examples/line.json -traces 2
go run ./cmd/twin -config examples/line.json -seeds 4   # 连续 4 个种子情景
go test ./... -v                                        # 带场景日志的测试
CGO_ENABLED=1 go test -race ./...                       # 竞态检测
GOPROXY=off go test ./...                               # 完全离线
```

## 配置格式

顶层字段：

| 字段 | 说明 |
| --- | --- |
| `name` | 产线名称 |
| `seed` | 随机种子；同种子重复运行结果完全一致 |
| `horizon` | 仿真时长（时间单位自定义，如秒）；时刻等于 horizon 的事件仍处理 |
| `source` | 投料描述 |
| `buffers` | 缓冲列表，恰好一个 `sink`（容量固定为 `-1` 无限） |
| `stations` | 工位列表 |

`source`：

- `output`：首缓冲 ID
- `count`：计划投料件数（必须 > 0）
- `firstAt`：首次投料时刻（>= 0）
- `interval`：相邻投料间隔，时长分布见下（可为 0 固定）

`buffers[*]`：`id`、`capacity`（普通缓冲必须 > 0；sink 必须 `-1`）、
`sink`。

`stations[*]`：

- `id` / `input` / `output`：工位标识与输入、输出缓冲
- `servers`：同构并行设备数（> 0），共享输入/输出缓冲
- `process`：单件加工时长分布
- `failure`（可选）：设备故障，见下
- `inspection`（可选）：质检返工，见下

时长分布（`kind`）：

- `fixed`：`{"kind":"fixed","mean":5}`
- `uniform`：`{"kind":"uniform","min":4,"max":7}`，要求 `0 <= min < max`
- `normal`：`{"kind":"normal","mean":5,"stddev":0.5}`，负值夹到 0

故障 `failure`：

- 计划故障：
  `{"kind":"scheduled","offset":30,"interval":80,"jitter":5,
  "repair":{"kind":"uniform","min":8,"max":12}}`
  从 `offset` 起每 `interval`（±`jitter` 均匀抖动）发生一次；
  `jitter < interval`。
- 随机故障：
  `{"kind":"random","mtbf":50,"repairMin":3,"repairMax":8}`
  相邻故障间隔为均值 `mtbf` 的指数分布，维修时长在
  `[repairMin, repairMax)` 均匀分布。

质检 `inspection`：

- `failRate`：每次加工完成的不合格概率（`[0,1]`）
- `maxRework`：单件允许的返工次数上限（>= 0）
- `reworkBuffer`：不合格品回流缓冲（必须是非 sink 且从投料可达）
- `onExceed`：超过返工上限后的处置：`scrap`（报废，默认）或 `accept`
  （让步接收为合格品进入正常输出）

构建阶段（`twin.New`）会拒绝所有非法配置，错误可用 `errors.Is` 区分类别：
`ErrDuplicateID`（重复标识）、`ErrUnknownReference`（未知引用）、
`ErrInvalidCapacity`、`ErrInvalidDuration`、`ErrInvalidParameter`、
`ErrInvalidRoute`（sink 不可达/返工回流非法等）、`ErrMissingField`。
校验失败时不会产生可运行模型。

## 时间推进与同刻事件规则

仿真器是事件驱动的单队列推进。事件按「时刻 → 固定优先级 →
工位 ID 字典序 → 设备编号 → 入队序号」排序，因此同一时刻多件事的先后
次序完全确定、可解释，且与配置中条目的书写顺序无关：

1. **故障结束**（维修完成，工位恢复）
2. **投料到达**（新制品到达首缓冲）
3. **加工完成**（先产出，再被流转消费）
4. **流转推进**（缓冲搬运、阻塞解除、空闲设备开工）
5. **故障开始**（同一时刻已到点的加工先完成，随后工位才停机）

流转规则：

- 缓冲为 FIFO；工位先尝试送走阻塞中的成品，再用空闲设备从输入缓冲拉料。
- 输出缓冲满时，完成的制品在工位上「阻塞等待」，仍占住该设备槽位，
  缓冲出现空位后在同一时刻按上述次序级联唤醒（每个工位每刻最多一次
  流转事件，重复唤醒合并）。
- 首缓冲满时已到投料时刻的物料进入待入线队列（计入 `Pending`），
  空位出现后按序补入，不丢弃、不跳时；投料时刻本身仍照常排定。

## 确定性

每个仿真情景拥有自己的 `math/rand` 源（`rand.New(rand.NewSource(seed))`）、
自己的模型与配置深拷贝，不共享任何可变状态。随机数只在明确的事件点按固定
顺序抽样（零刻故障武装 → 投料间隔 → 各工位加工时长 → 质检判定 →
故障/维修参数）。同一份配置与种子，任意次运行（串行或并发）的汇总统计与
每件制品轨迹逐字节一致。

## 故障与返工策略

**故障**：故障开始时工位立即停机，所有正在进行的加工被「冻结」——旧的
完成事件作废，故障期间不会产出任何加工完成结果；冻结只记录剩余时长，
不计入忙碌。维修结束后工位恢复，被冻结的设备以冻结时的剩余时长继续
加工。计划故障落在未结束的维修窗口内则跳过本次；随机故障在停机期间不
重复武装，维修恢复后再按 MTBF 抽样。成品在故障开始前已完成质检的，
允许在停机期间被搬入有空位的下游缓冲。

**返工**：质检不合格时，若该件返工次数 `< maxRework`，返工计数加一并
送入明确的 `reworkBuffer`（示例中回到质检前缓冲，再次经过质检）；
达到上限后按 `onExceed` 确定处置（`scrap` 报废或 `accept` 让步接收）。
返工次数随制品走，轨迹中有每次 `rework` 记录；返工环有严格上限，
不会死循环。

## 统计口径

物料账在每个事件后都满足：

```
Planned    = Released + Pending + NotArrived
Released   = Completed + Scrapped + WIP
```

- `Planned`：计划投料数
- `Released`：实际进入产线（首缓冲）的物料，账目的唯一来源
- `Pending`：已到投料时刻、因首缓冲满尚未入线
- `NotArrived`：投料时刻晚于 horizon 的计划物料
- `Completed`：合格进入 sink 的数量
- `Scrapped`：报废数量（附原因，当前为 `rework-limit-exceeded`）
- `WIP`：仿真结束时仍在制 = 非 sink 缓冲内 + 设备上加工中/阻塞等待的制品；
  sink 内成品只计入 `Completed`，绝不重复计数
- `AvgWIP`：WIP 的时间加权积分 / horizon
- 工位：`Processed`（完成加工次数，含返工件的重复加工）、`BusyTime`
  （实际加工时长，不含冻结）、`Downtime`、`Utilization =
  BusyTime / (horizon*servers - downtime)`

每件制品有一条 `PartTrace`：`released → started → (rework…) →
finished/scrapped`，保证恰好一次投入、至多一次终态、时间有序。
结果构建时会执行两层守恒校验（总账 + 逐件轨迹对账），不一致直接返回
`ErrConservation`，仿真器不会静默丢料；另有 2000 万事件上限防止任何
形式的活锁/卡死无限运行。

## 本地验证

```bash
go test ./... -v                       # 查看各场景与用例日志
CGO_ENABLED=1 go test -race -count=3 ./...
GOPROXY=off go test ./...              # 离线
go run ./cmd/twin -config examples/line.json -traces 5
```

测试覆盖：确定性复现（统计+轨迹完全一致）、长/短 horizon 物料守恒、
背压下的待入线物料、计划故障冻结-恢复时点、随机故障、返工上限与
scrap/accept 两种超限处置、轨迹合法性、20 类非法配置的可区分拒绝、
24 情景并发（结果与串行运行逐项一致）以及示例配置端到端运行。
