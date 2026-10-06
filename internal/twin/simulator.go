package twin

import (
	"container/heap"
	"math/rand"
	"sort"
	"strconv"
)

// 同刻事件优先级（数字越小越早执行），同优先级再按工位 ID 字典序、
// 设备编号排序，与配置输入顺序无关：
//
//	1 故障结束（维修完成，工位恢复）
//	2 投料到达（新制品进入首缓冲）
//	3 加工完成（先产出，再被后续流转消费）
//	4 流转推进（缓冲搬运、阻塞解除、空闲设备开工）
//	5 故障开始（同一时刻已到点的加工先完成，随后工位才停机）
const (
	priFaultEnd   = 1
	priArrival    = 2
	priComplete   = 3
	priFlow       = 4
	priFaultStart = 5
)

// TraceEvent 记录单个制品的一个流转轨迹点。
type TraceEvent struct {
	Time    float64
	Kind    string // released|started|completed|rework|moved|scrapped|finished
	Station string
	Buffer  string
	Detail  string
}

// PartTrace 是一件制品从投料到完工/报废的完整轨迹。
type PartTrace struct {
	PartID     int64
	ReleasedAt float64
	FinishedAt float64 // 0 表示仿真结束时仍在制
	Reworks    int
	Scrapped   bool
	Events     []TraceEvent
}

// StationStat 是工位级统计。
type StationStat struct {
	ID           string
	Processed    int64
	BusyTime     float64
	Downtime     float64
	CapacityTime float64
	Utilization  float64
}

// BufferLevel 是仿真结束时某缓冲的在制品数量。
type BufferLevel struct {
	Buffer string
	Items  int
}

// Result 是一次仿真的统计结果。物料账恒满足
// Released == Completed + Scrapped + WIP。
type Result struct {
	Seed           int64
	Horizon        float64
	Planned        int64
	Released       int64
	Completed      int64
	Scrapped       int64
	WIP            int64
	Pending        int64 // 已到投料时刻但首缓冲满、尚未入线的物料
	NotArrived     int64 // 投料时刻尚未到达（horizon 截断）的计划物料
	ScrapReasons   map[string]int64
	AvgWIP         float64
	BufferSnapshot []BufferLevel
	Stations       []StationStat
	Traces         []PartTrace
	LastEventTime  float64
}

type ev struct {
	time    float64
	pri     int
	tie1    int // 工位顺序号（source 用 -1）
	tie2    int // 设备号
	seq     int64
	kind    string
	part    *Part
	server  int
	gen     int64
	station *Station // flow/fault 事件目标
}

func (e *ev) stationID() string {
	if e.station != nil {
		return e.station.ID
	}
	return ""
}

type evHeap []*ev

func (h evHeap) Len() int { return len(h) }
func (h evHeap) Less(i, j int) bool {
	a, b := h[i], h[j]
	if a.time != b.time {
		return a.time < b.time
	}
	if a.pri != b.pri {
		return a.pri < b.pri
	}
	if a.tie1 != b.tie1 {
		return a.tie1 < b.tie1
	}
	if a.tie2 != b.tie2 {
		return a.tie2 < b.tie2
	}
	return a.seq < b.seq
}
func (h evHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *evHeap) Push(x interface{}) { *h = append(*h, x.(*ev)) }
func (h *evHeap) Pop() interface{} {
	old := *h
	n := len(old)
	e := old[n-1]
	*h = old[:n-1]
	return e
}

// Simulator 在一份独立模型副本上推进仿真；多个 Simulator 之间不共享状态，
// 可并发运行同一配置的不同情景。
type Simulator struct {
	cfg *Config
	m   *Model
	rng *rand.Rand

	h   evHeap
	seq int64
	now float64

	stations []*Station
	stIndex  map[string]int

	plannedLeft int64
	nextPartID  int64
	pending     []*Part
	nextArrival float64

	wakeStations map[*Station]bool

	released  int64
	completed int64
	scrapped  int64
	scrapWhy  map[string]int64

	wipArea   float64
	lastWIPAt float64

	traces      map[int64]*PartTrace
	orderTraces []int64

	lastEventTime float64
}

// New 基于配置构建模型并创建仿真器。配置非法时直接返回带类别的错误，
// 不会带着有问题的模型继续。
func New(cfg *Config) (*Simulator, error) {
	cfg = cfg.deepCopy()
	m, err := cfg.Build()
	if err != nil {
		return nil, err
	}
	m = m.clone()

	sts := append([]*Station(nil), m.order...)
	sort.Slice(sts, func(i, j int) bool { return sts[i].ID < sts[j].ID })

	s := &Simulator{
		cfg:          cfg,
		m:            m,
		rng:          rand.New(rand.NewSource(cfg.Seed)),
		stations:     sts,
		stIndex:      map[string]int{},
		plannedLeft:  int64(cfg.Source.Count),
		nextPartID:   1,
		nextArrival:  cfg.Source.FirstAt,
		wakeStations: map[*Station]bool{},
		scrapWhy:     map[string]int64{},
		traces:       map[int64]*PartTrace{},
	}
	for i, st := range sts {
		s.stIndex[st.ID] = i
	}
	heap.Init(&s.h)

	if s.nextArrival <= cfg.Horizon {
		s.push(&ev{time: s.nextArrival, pri: priArrival, tie1: -1, kind: "arrival"})
	}
	// 初始计划故障按工位 ID 固定顺序入队；随机故障的首次间隔
	// 在零刻流转中抽样，保证随机数调用次序确定。
	for i, st := range sts {
		if st.Failure == nil {
			continue
		}
		switch st.Failure.Kind {
		case "scheduled":
			s.push(&ev{time: st.Failure.Offset, pri: priFaultStart, tie1: i, kind: "faultStart", station: st})
		}
	}
	// 零刻流转：安排开工与随机故障。
	for _, st := range sts {
		s.wakeStations[st] = true
		s.push(&ev{time: 0, pri: priFlow, tie1: s.stIndex[st.ID], kind: "flow", station: st})
	}
	return s, nil
}

func (s *Simulator) push(e *ev) {
	e.seq = s.seq
	s.seq++
	heap.Push(&s.h, e)
}

const maxEvents = 20_000_000

// Run 推进仿真直到 horizon，返回产能、在制品与轨迹统计。
func (s *Simulator) Run() (*Result, error) {
	var processed int64
	for len(s.h) > 0 {
		e := heap.Pop(&s.h).(*ev)
		if e.time > s.cfg.Horizon || e.time < s.now {
			continue
		}
		if processed++; processed > maxEvents {
			return nil, buildErr(ErrConservation, "engine", "event limit %d exceeded (possible livelock)", maxEvents)
		}
		s.advanceWIPArea(e.time)
		s.now = e.time
		s.lastEventTime = e.time

		var dirty []*Station
		switch e.kind {
		case "arrival":
			dirty = s.onArrival(e)
		case "complete":
			dirty = s.onComplete(e)
		case "flow":
			if s.wakeStations[e.station] {
				delete(s.wakeStations, e.station)
				dirty = s.flowStation(e.station)
			}
		case "faultStart":
			dirty = s.onFaultStart(e)
		case "faultEnd":
			dirty = s.onFaultEnd(e)
		}
		s.scheduleWakes(dirty)
	}

	// horizon 处最终结算：忙碌时长与跨 horizon 的停机时长。
	for _, st := range s.stations {
		for i, p := range st.serving {
			if p != nil && !(st.blocked != nil && st.blockedSlot == i) {
				st.accBusy[i] += s.cfg.Horizon - st.startAt[i]
			}
		}
		var busy float64
		for _, v := range st.accBusy {
			busy += v
		}
		st.busyTime = busy
		if st.down {
			st.downTime += s.cfg.Horizon - st.downSince
		}
	}

	s.advanceWIPArea(s.cfg.Horizon)
	return s.buildResult()
}

func (s *Simulator) onArrival(e *ev) []*Station {
	if s.plannedLeft <= 0 {
		return nil
	}
	s.plannedLeft--
	p := &Part{ID: s.nextPartID, ReleasedAt: s.now}
	s.nextPartID++
	s.pending = append(s.pending, p)
	s.tryReleasePending()

	// 无论本次是否成功入线，后续投料时刻都已确定，必须照常调度；
	// 未入线的物料保留在 pending 队列，等首缓冲空位唤醒后重试。
	if s.plannedLeft > 0 {
		s.nextArrival = s.now + s.cfg.Source.Interval.sample(s.rng)
		if s.nextArrival <= s.cfg.Horizon {
			s.push(&ev{time: s.nextArrival, pri: priArrival, tie1: -1, kind: "arrival"})
		}
	}
	return s.consumersOf(s.m.Buffers[s.cfg.Source.Output])
}

func (s *Simulator) tryReleasePending() {
	buf := s.m.Buffers[s.cfg.Source.Output]
	for len(s.pending) > 0 {
		p := s.pending[0]
		if !buf.push(p) {
			return // 首缓冲满：队首之后都等待，物料不丢
		}
		s.pending = s.pending[1:]
		s.released++
		s.trace(p, TraceEvent{Time: s.now, Kind: "released", Buffer: buf.ID})
	}
}

func (s *Simulator) onComplete(e *ev) []*Station {
	idx := s.stIndex[e.stationID()]
	st := s.stations[idx]
	if e.gen != st.servingGen || e.server >= st.Servers || st.serving[e.server] != e.part {
		return nil // 故障冻结后旧完成事件作废
	}
	p := e.part
	st.remaining[e.server] = 0
	st.accBusy[e.server] += s.now - st.startAt[e.server]
	st.processed++
	// 注意：完成的工件仍占住 serving[server]（以 blocked 状态），
	// 该槽位不计入 busy，但其他工件不能复用，直到成品推走。
	st.busy--
	s.trace(p, TraceEvent{Time: s.now, Kind: "completed", Station: st.ID})

	target, scrap := s.inspect(st, p)
	if scrap {
		st.serving[e.server] = nil // 报废：槽位直接释放
		s.scrapPart(p, st, "rework-limit-exceeded")
		return s.consumersOf(st.Input)
	}
	st.blocked = p
	st.blockedTarget = target
	st.blockedSlot = e.server // 成品阻塞期间槽位保持占用，防止设备重复利用
	s.trace(p, TraceEvent{Time: s.now, Kind: "moved", Station: st.ID, Buffer: target.ID, Detail: "awaiting-buffer-space"})

	dirty := s.consumersOf(st.Input)
	dirty = append(dirty, st)
	return dirty
}

func (s *Simulator) inspect(st *Station, p *Part) (target *Buffer, scrap bool) {
	if st.Inspection == nil {
		return st.Output, false
	}
	insp := st.Inspection
	if s.rng.Float64() >= insp.FailRate {
		return st.Output, false
	}
	if p.Reworks >= insp.MaxRework {
		if insp.OnExceed == "accept" {
			return st.Output, false
		}
		return nil, true
	}
	p.Reworks++
	rb := s.m.Buffers[insp.ReworkBuffer]
	s.trace(p, TraceEvent{Time: s.now, Kind: "rework", Station: st.ID, Buffer: rb.ID, Detail: strconv.Itoa(p.Reworks)})
	return rb, false
}

func (s *Simulator) scrapPart(p *Part, st *Station, reason string) {
	s.scrapped++
	s.scrapWhy[reason]++
	t := s.trace(p, TraceEvent{Time: s.now, Kind: "scrapped", Station: st.ID, Detail: reason})
	t.Scrapped = true
	t.FinishedAt = s.now
}

func (s *Simulator) onFaultStart(e *ev) []*Station {
	st := s.stations[e.tie1]
	if st.down {
		// 故障落在尚未结束的维修窗口内：
		// 计划故障由 faultEnd 安排下一次；随机故障解除武装，
		// 由维修结束后的流转重新按 MTBF 抽样，避免永久停机。
		if st.Failure.Kind == "random" {
			st.nextFailureArmed = false
		}
		return nil
	}
	st.down = true
	st.downSince = s.now
	st.servingGen++
	st.nextFailureArmed = false

	// 冻结正在加工的设备：故障期间不得产出，旧完成事件全部作废，
	// 维修结束后用剩余时长重新调度。
	for i := range st.serving {
		if st.serving[i] != nil && !(st.blocked != nil && st.blockedSlot == i) {
			st.accBusy[i] += s.now - st.startAt[i]
			// 本次冻结区间内实际加工了 now-startAt，直接从剩余时长扣减。
			st.remaining[i] -= s.now - st.startAt[i]
			if st.remaining[i] < 0 {
				st.remaining[i] = 0
			}
			s.trace(st.serving[i], TraceEvent{Time: s.now, Kind: "moved", Station: st.ID, Detail: "processing-frozen"})
		}
	}

	var repair float64
	switch st.Failure.Kind {
	case "scheduled":
		repair = st.Failure.Repair.sample(s.rng)
	case "random":
		repair = st.Failure.RepairMin + s.rng.Float64()*(st.Failure.RepairMax-st.Failure.RepairMin)
	}
	s.push(&ev{time: s.now + repair, pri: priFaultEnd, tie1: e.tie1, kind: "faultEnd", station: st})
	return nil
}

func (s *Simulator) onFaultEnd(e *ev) []*Station {
	st := s.stations[e.tie1]
	if !st.down {
		return nil
	}
	st.down = false
	st.downTime += s.now - st.downSince

	// 恢复被冻结的加工，剩余时长保持不变。
	for i, p := range st.serving {
		if p != nil && st.remaining[i] > 0 && !(st.blocked != nil && st.blockedSlot == i) {
			st.startAt[i] = s.now
			s.push(&ev{
				time: s.now + st.remaining[i], pri: priComplete, tie1: e.tie1, tie2: i,
				kind: "complete", part: p, server: i, gen: st.servingGen, station: st,
			})
			s.trace(p, TraceEvent{Time: s.now, Kind: "moved", Station: st.ID, Detail: "processing-resumed"})
		}
	}

	// 计划故障在维修结束时确定下一次发生时刻（间隔含抖动）。
	if st.Failure.Kind == "scheduled" {
		next := s.now + st.Failure.Interval
		if st.Failure.Jitter > 0 {
			next += (s.rng.Float64()*2 - 1) * st.Failure.Jitter
		}
		if next <= s.cfg.Horizon {
			s.push(&ev{time: next, pri: priFaultStart, tie1: e.tie1, kind: "faultStart", station: st})
		}
	}
	return []*Station{st}
}

// flowStation 推进单个工位：先尝试送走阻塞中的成品，再用空闲设备拉料开工，
// 返回状态可能因此变化、需要在同一时刻继续推进的工位集合。
func (s *Simulator) flowStation(st *Station) []*Station {
	var dirty []*Station
	addDirty := func(x *Station) {
		for _, d := range dirty {
			if d == x {
				return
			}
		}
		dirty = append(dirty, x)
	}

	// 1. 送走等待输出缓冲空位的成品（设备故障也允许送走，因为加工已结束）。
	if st.blocked != nil {
		if st.blockedTarget.push(st.blocked) {
			p := st.blocked
			target := st.blockedTarget
			st.blocked = nil
			st.blockedTarget = nil
			if target.Sink {
				s.completed++
				t := s.trace(p, TraceEvent{Time: s.now, Kind: "finished", Buffer: target.ID})
				t.FinishedAt = s.now
			} else {
				s.trace(p, TraceEvent{Time: s.now, Kind: "moved", Station: st.ID, Buffer: target.ID})
				// 新物料进入非 sink 缓冲：其消费工位可以拉料。
				for _, c := range s.consumersOf(target) {
					addDirty(c)
				}
			}
			st.serving[st.blockedSlot] = nil
			st.blockedSlot = -1
			// 工位腾出阻塞位：自己可以继续拉料；
			// 同输入缓冲的其他工位可能也在等这件被推走…… 此处自己必查。
			addDirty(st)
		}
	}

	// 2. 空闲且未停机的设备从输入缓冲 FIFO 拉料并开工。
	if !st.down && st.blocked == nil {
		for {
			p := st.Input.pop()
			if p == nil {
				break
			}
			slot := -1
			for i := range st.serving {
				if st.serving[i] == nil {
					slot = i
					break
				}
			}
			if slot < 0 {
				// 没有真正空闲的槽位：把刚取出的物料放回缓冲队首。
				st.Input.items = append([]*Part{p}, st.Input.items...)
				break
			}
			dur := st.Process.sample(s.rng)
			st.serving[slot] = p
			st.remaining[slot] = dur
			st.procDur[slot] = dur
			st.startAt[slot] = s.now
			st.busy++
			s.push(&ev{
				time: s.now + dur, pri: priComplete, tie1: s.stIndex[st.ID], tie2: slot,
				kind: "complete", part: p, server: slot, gen: st.servingGen, station: st,
			})
			s.trace(p, TraceEvent{Time: s.now, Kind: "started", Station: st.ID})
			// 从输入缓冲取走一件：等待该缓冲空位的阻塞工位可以再推。
			for _, w := range s.producersInto(st.Input) {
				addDirty(w)
			}
			if len(s.pending) > 0 && s.m.Buffers[s.cfg.Source.Output] == st.Input {
				s.tryReleasePending()
			}
		}
	}

	// 3. 随机故障的工位在恢复正常后，按 MTBF 安排下一次故障。
	if st.Failure != nil && st.Failure.Kind == "random" && !st.down && !st.nextFailureArmed {
		st.nextFailureArmed = true
		at := s.now + s.rng.ExpFloat64()*st.Failure.MTBF
		s.push(&ev{time: at, pri: priFaultStart, tie1: s.stIndex[st.ID], kind: "faultStart", station: st})
	}

	return dirty
}

// producersInto 返回成品/返工目标可能是给定缓冲的所有工位，
// 即缓冲腾出空位时需要唤醒的阻塞生产者。
func (s *Simulator) producersInto(b *Buffer) []*Station {
	var out []*Station
	for _, st := range s.stations {
		if st.Output == b {
			out = append(out, st)
			continue
		}
		if st.Inspection != nil && s.m.Buffers[st.Inspection.ReworkBuffer] == b {
			out = append(out, st)
		}
	}
	return out
}

// scheduleWakes 为状态可能受影响的工位安排同一时刻的流转事件
// （每个工位每刻最多一个，重复唤醒合并）。
func (s *Simulator) scheduleWakes(dirty []*Station) {
	// source 首缓冲可能腾出位置，待投料需要重试。
	s.tryReleasePending()
	for _, st := range dirty {
		s.wakeStations[st] = true
	}
	// 按工位 ID 固定顺序入队，避免 map 遍历随机性。
	for _, st := range s.stations {
		if s.wakeStations[st] {
			s.push(&ev{time: s.now, pri: priFlow, tie1: s.stIndex[st.ID], kind: "flow", station: st})
		}
	}
}

// consumersOf 返回以给定缓冲为输入的工位（按固定工位顺序）。
func (s *Simulator) consumersOf(b *Buffer) []*Station {
	var out []*Station
	for _, st := range s.stations {
		if st.Input == b {
			out = append(out, st)
		}
	}
	return out
}

func (s *Simulator) trace(p *Part, e TraceEvent) *PartTrace {
	t, ok := s.traces[p.ID]
	if !ok {
		t = &PartTrace{PartID: p.ID, ReleasedAt: p.ReleasedAt}
		s.traces[p.ID] = t
		s.orderTraces = append(s.orderTraces, p.ID)
	}
	t.Reworks = p.Reworks
	t.Events = append(t.Events, e)
	return t
}

func (s *Simulator) advanceWIPArea(t float64) {
	if t < s.lastWIPAt {
		return
	}
	var wip int64
	for _, b := range s.m.Buffers {
		if b.Sink {
			continue // 成品已计入 completed，不在制
		}
		wip += int64(len(b.items))
	}
	for _, st := range s.stations {
		for _, p := range st.serving {
			if p != nil {
				wip++ // 含加工中与阻塞等空位的制品
			}
		}
	}
	s.wipArea += float64(wip) * (t - s.lastWIPAt)
	s.lastWIPAt = t
}

func (s *Simulator) buildResult() (*Result, error) {
	var wip int64
	var snapshot []BufferLevel
	for _, b := range s.m.Buffers {
		snapshot = append(snapshot, BufferLevel{Buffer: b.ID, Items: len(b.items)})
		if !b.Sink {
			wip += int64(len(b.items))
		}
	}
	for _, st := range s.stations {
		for _, p := range st.serving {
			if p != nil {
				wip++
			}
		}
	}
	sort.Slice(snapshot, func(i, j int) bool { return snapshot[i].Buffer < snapshot[j].Buffer })

	// 物料守恒：投入 == 产出 + 报废 + 在制。
	if s.released != s.completed+s.scrapped+wip {
		return nil, buildErr(ErrConservation, "result",
			"released=%d but completed=%d scrapped=%d wip=%d",
			s.released, s.completed, s.scrapped, wip)
	}

	// 逐件核对轨迹状态与总账一致（双重保险，防止重复计数）。
	var traceFinished, traceScrapped int64
	traces := make([]PartTrace, 0, len(s.orderTraces))
	for _, id := range s.orderTraces {
		t := s.traces[id]
		switch {
		case t.Scrapped:
			traceScrapped++
		case t.FinishedAt > 0:
			traceFinished++
		}
		traces = append(traces, *t)
	}
	if traceFinished != s.completed || traceScrapped != s.scrapped {
		return nil, buildErr(ErrConservation, "traces",
			"trace counts finished=%d scrapped=%d disagree with ledger completed=%d scrapped=%d",
			traceFinished, traceScrapped, s.completed, s.scrapped)
	}

	stats := make([]StationStat, 0, len(s.stations))
	for _, st := range s.stations {
		capTime := s.cfg.Horizon*float64(st.Servers) - st.downTime
		var util float64
		if capTime > 0 {
			util = st.busyTime / capTime
		}
		stats = append(stats, StationStat{
			ID: st.ID, Processed: st.processed, BusyTime: st.busyTime,
			Downtime: st.downTime, CapacityTime: capTime, Utilization: util,
		})
	}

	var avgWIP float64
	if s.cfg.Horizon > 0 {
		avgWIP = s.wipArea / s.cfg.Horizon
	}
	var pending int64
	pending = int64(len(s.pending))
	notArrived := s.plannedLeft

	return &Result{
		Seed:           s.cfg.Seed,
		Horizon:        s.cfg.Horizon,
		Planned:        int64(s.cfg.Source.Count),
		Released:       s.released,
		Completed:      s.completed,
		Scrapped:       s.scrapped,
		WIP:            wip,
		Pending:        pending,
		NotArrived:     notArrived,
		ScrapReasons:   s.scrapWhy,
		AvgWIP:         avgWIP,
		BufferSnapshot: snapshot,
		Stations:       stats,
		Traces:         traces,
		LastEventTime:  s.lastEventTime,
	}, nil
}
