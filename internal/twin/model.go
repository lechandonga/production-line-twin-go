package twin

import (
	"fmt"
	"math"
	"math/rand"
)

// Buffer 是产线缓冲（含末端 sink）。
type Buffer struct {
	ID       string
	Capacity int // -1 表示无限
	Sink     bool

	items []*Part
	// 等待该缓冲空位的工位（按工位 ID 固定顺序唤醒）
	waiters []*Station
}

func (b *Buffer) full() bool {
	return b.Capacity >= 0 && len(b.items) >= b.Capacity
}

// push 放入一件，容量满时返回 false。
func (b *Buffer) push(p *Part) bool {
	if b.full() {
		return false
	}
	b.items = append(b.items, p)
	return true
}

// pop 取出最旧的一件（FIFO）。
func (b *Buffer) pop() *Part {
	if len(b.items) == 0 {
		return nil
	}
	p := b.items[0]
	b.items = b.items[1:]
	return p
}

// Station 是工位：Servers 台并行设备，共享同一个输入/输出缓冲。
type Station struct {
	ID         string
	Input      *Buffer
	Output     *Buffer
	Servers    int
	Process    Duration
	Failure    *FailureSpec
	Inspection *InspectionSpec

	busy int
	down bool
	// 每台设备上正在加工的制品与剩余加工时长（故障冻结）；nil 表示空闲
	serving          []*Part
	remaining        []float64
	blocked          *Part // 已完成质检、等待输出缓冲空位的制品
	blockedTarget    *Buffer
	blockedSlot      int   // 该阻塞制品占用的设备槽位（-1 表示无）
	servingGen       int64 // 每次故障开始自增，作废旧的完成事件
	nextFailureArmed bool
	downSince        float64
	startAt          []float64 // 当前加工段起始时刻（故障恢复后重置）
	accBusy          []float64 // 已累计实际加工时长（扣除停机冻结）
	procDur          []float64 // 本次加工抽样得到的总时长（故障冻结时计算剩余量）
	busyTime         float64
	downTime         float64
	processed        int64
}

func (s *Station) availableServers() int {
	if s.down {
		return 0
	}
	return s.Servers - s.busy
}

// Part 是一件在制品。
type Part struct {
	ID         int64
	Reworks    int
	ReleasedAt float64
}

// Model 是构建并校验通过的产线模型。构建失败时不会产生可用模型。
type Model struct {
	cfg      *Config
	Stations map[string]*Station
	Buffers  map[string]*Buffer
	order    []*Station // 按 ID 排序，保证同刻事件次序固定
}

// Build 对配置做完整校验并构建模型；任何非法配置都会带可区分原因返回。
func (cfg *Config) Build() (*Model, error) {
	if cfg.Horizon <= 0 {
		return nil, buildErr(ErrInvalidDuration, "horizon", "horizon must be > 0, got %v", cfg.Horizon)
	}
	if cfg.Source.Count <= 0 {
		return nil, buildErr(ErrInvalidParameter, "source.count", "count must be > 0, got %d", cfg.Source.Count)
	}
	if err := validateDuration("source.interval", cfg.Source.Interval, true); err != nil {
		return nil, err
	}

	m := &Model{cfg: cfg, Stations: map[string]*Station{}, Buffers: map[string]*Buffer{}}

	seenBuf := map[string]bool{}
	var sinkCount int
	for _, bc := range cfg.Buffers {
		if bc.ID == "" {
			return nil, buildErr(ErrMissingField, "buffers", "buffer id is empty")
		}
		if seenBuf[bc.ID] {
			return nil, buildErr(ErrDuplicateID, "buffer:"+bc.ID, "duplicate buffer id")
		}
		seenBuf[bc.ID] = true
		if bc.Sink {
			sinkCount++
			if bc.Capacity != -1 {
				return nil, buildErr(ErrInvalidCapacity, "buffer:"+bc.ID, "sink buffer capacity must be -1 (unlimited)")
			}
		} else if bc.Capacity <= 0 {
			return nil, buildErr(ErrInvalidCapacity, "buffer:"+bc.ID, "capacity must be > 0 or -1, got %d", bc.Capacity)
		}
		m.Buffers[bc.ID] = &Buffer{ID: bc.ID, Capacity: bc.Capacity, Sink: bc.Sink}
	}
	if sinkCount != 1 {
		return nil, buildErr(ErrInvalidRoute, "buffers", "exactly one sink buffer is required, got %d", sinkCount)
	}

	if _, ok := m.Buffers[cfg.Source.Output]; !ok {
		return nil, buildErr(ErrUnknownReference, "source.output", "source output buffer %q does not exist", cfg.Source.Output)
	}

	seenSt := map[string]bool{}
	for _, sc := range cfg.Stations {
		if sc.ID == "" {
			return nil, buildErr(ErrMissingField, "stations", "station id is empty")
		}
		if seenSt[sc.ID] {
			return nil, buildErr(ErrDuplicateID, "station:"+sc.ID, "duplicate station id")
		}
		seenSt[sc.ID] = true

		in, ok := m.Buffers[sc.Input]
		if !ok {
			return nil, buildErr(ErrUnknownReference, "station:"+sc.ID+".input", "input buffer %q does not exist", sc.Input)
		}
		if in.Sink {
			return nil, buildErr(ErrInvalidRoute, "station:"+sc.ID, "station cannot consume from sink buffer %q", sc.Input)
		}
		out, ok := m.Buffers[sc.Output]
		if !ok {
			return nil, buildErr(ErrUnknownReference, "station:"+sc.ID+".output", "output buffer %q does not exist", sc.Output)
		}
		if sc.Servers <= 0 {
			return nil, buildErr(ErrInvalidParameter, "station:"+sc.ID+".servers", "servers must be > 0, got %d", sc.Servers)
		}
		if err := validateDuration("station:"+sc.ID+".process", sc.Process, false); err != nil {
			return nil, err
		}
		if sc.Failure != nil {
			if err := validateFailure(sc.ID, sc.Failure); err != nil {
				return nil, err
			}
		}
		if sc.Inspection != nil {
			insp := sc.Inspection
			if insp.FailRate < 0 || insp.FailRate > 1 {
				return nil, buildErr(ErrInvalidParameter, "station:"+sc.ID+".inspection.failRate", "failRate must be in [0,1], got %v", insp.FailRate)
			}
			if insp.MaxRework < 0 {
				return nil, buildErr(ErrInvalidParameter, "station:"+sc.ID+".inspection.maxRework", "maxRework must be >= 0, got %d", insp.MaxRework)
			}
			onExceed := insp.OnExceed
			if onExceed == "" {
				onExceed = "scrap"
			}
			if onExceed != "scrap" && onExceed != "accept" {
				return nil, buildErr(ErrInvalidParameter, "station:"+sc.ID+".inspection.onExceed", "onExceed must be scrap or accept, got %q", insp.OnExceed)
			}
			if _, ok := m.Buffers[insp.ReworkBuffer]; !ok {
				return nil, buildErr(ErrUnknownReference, "station:"+sc.ID+".inspection.reworkBuffer", "rework buffer %q does not exist", insp.ReworkBuffer)
			}
			if m.Buffers[insp.ReworkBuffer].Sink {
				return nil, buildErr(ErrInvalidRoute, "station:"+sc.ID+".inspection.reworkBuffer", "rework must return to a non-sink buffer")
			}
		}

		inspCopy := sc.Inspection
		if inspCopy != nil && inspCopy.OnExceed == "" {
			cp := *inspCopy
			cp.OnExceed = "scrap"
			inspCopy = &cp
		}
		st := &Station{
			ID: sc.ID, Input: in, Output: out, Servers: sc.Servers,
			Process: sc.Process, Failure: sc.Failure, Inspection: inspCopy,
			blockedSlot: -1,
			serving:     make([]*Part, sc.Servers), remaining: make([]float64, sc.Servers),
			startAt: make([]float64, sc.Servers), accBusy: make([]float64, sc.Servers),
			procDur: make([]float64, sc.Servers),
		}
		m.Stations[sc.ID] = st
		m.order = append(m.order, st)
	}

	if len(m.order) == 0 {
		return nil, buildErr(ErrMissingField, "stations", "at least one station is required")
	}

	// 图可达性：从 source.output 出发，必须能到达 sink；
	// 返工回流缓冲必须位于可达集合内（有工位会再次加工它）。
	reachable := map[string]bool{}
	frontier := []string{cfg.Source.Output}
	for len(frontier) > 0 {
		bid := frontier[len(frontier)-1]
		frontier = frontier[:len(frontier)-1]
		if reachable[bid] {
			continue
		}
		reachable[bid] = true
		for _, st := range m.order {
			if st.Input.ID == bid {
				frontier = append(frontier, st.Output.ID)
				if st.Inspection != nil && st.Inspection.FailRate > 0 {
					frontier = append(frontier, st.Inspection.ReworkBuffer)
				}
			}
		}
	}
	if !reachable[m.sinkID()] {
		return nil, buildErr(ErrInvalidRoute, "route", "sink %q is not reachable from source output %q", m.sinkID(), cfg.Source.Output)
	}
	for _, st := range m.order {
		if !reachable[st.Input.ID] {
			return nil, buildErr(ErrInvalidRoute, "station:"+st.ID, "station input buffer %q is not reachable", st.Input.ID)
		}
		if st.Inspection != nil && st.Inspection.FailRate > 0 && !reachable[st.Inspection.ReworkBuffer] {
			return nil, buildErr(ErrInvalidRoute, "station:"+st.ID+".inspection", "rework buffer %q is not reachable", st.Inspection.ReworkBuffer)
		}
	}

	return m, nil
}

func (m *Model) sinkID() string {
	for _, b := range m.Buffers {
		if b.Sink {
			return b.ID
		}
	}
	return ""
}

// clone 复制模型，使每个仿真情景拥有完全独立的运行态，互不干扰。
func (m *Model) clone() *Model {
	c := &Model{
		cfg:      m.cfg,
		Stations: map[string]*Station{},
		Buffers:  map[string]*Buffer{},
	}
	for id, b := range m.Buffers {
		c.Buffers[id] = &Buffer{ID: b.ID, Capacity: b.Capacity, Sink: b.Sink}
	}
	for _, st := range m.order {
		ns := &Station{
			ID:          st.ID,
			Input:       c.Buffers[st.Input.ID],
			Output:      c.Buffers[st.Output.ID],
			Servers:     st.Servers,
			Process:     st.Process,
			Failure:     st.Failure,
			Inspection:  st.Inspection,
			blockedSlot: -1,
			serving:     make([]*Part, st.Servers),
			remaining:   make([]float64, st.Servers),
			startAt:     make([]float64, st.Servers),
			accBusy:     make([]float64, st.Servers),
			procDur:     make([]float64, st.Servers),
		}
		c.Stations[st.ID] = ns
		c.order = append(c.order, ns)
	}
	return c
}

func validateDuration(where string, d Duration, zeroOK bool) error {
	minMean := 0.0
	if !zeroOK {
		minMean = math.SmallestNonzeroFloat64
	}
	switch d.Kind {
	case "":
		return buildErr(ErrMissingField, where, "duration kind is empty (fixed|uniform|normal)")
	case "fixed":
		if d.Mean < minMean {
			return buildErr(ErrInvalidDuration, where, "fixed mean must be >= 0, got %v", d.Mean)
		}
	case "uniform":
		if d.Min < 0 || d.Max <= d.Min {
			return buildErr(ErrInvalidDuration, where, "uniform requires 0 <= min < max, got [%v,%v]", d.Min, d.Max)
		}
	case "normal":
		if d.Mean <= 0 || d.StdDev < 0 {
			return buildErr(ErrInvalidDuration, where, "normal requires mean > 0 and stddev >= 0")
		}
	default:
		return buildErr(ErrInvalidDuration, where, "unknown duration kind %q", d.Kind)
	}
	return nil
}

func validateFailure(stID string, f *FailureSpec) error {
	where := "station:" + stID + ".failure"
	switch f.Kind {
	case "scheduled":
		if f.Interval <= 0 {
			return buildErr(ErrInvalidDuration, where, "scheduled interval must be > 0, got %v", f.Interval)
		}
		if f.Jitter < 0 || f.Jitter >= f.Interval {
			return buildErr(ErrInvalidDuration, where, "scheduled jitter must be in [0, interval), got %v", f.Jitter)
		}
		if f.Offset < 0 {
			return buildErr(ErrInvalidDuration, where, "scheduled offset must be >= 0, got %v", f.Offset)
		}
		return validateDuration(where+".repair", f.Repair, false)
	case "random":
		if f.MTBF <= 0 {
			return buildErr(ErrInvalidDuration, where, "random mtbf must be > 0, got %v", f.MTBF)
		}
		if f.RepairMin < 0 || f.RepairMax <= f.RepairMin {
			return buildErr(ErrInvalidDuration, where, "random repair requires 0 <= repairMin < repairMax")
		}
	default:
		return buildErr(ErrInvalidParameter, where, "failure kind must be scheduled or random, got %q", f.Kind)
	}
	return nil
}

func (d Duration) sample(r *rand.Rand) float64 {
	switch d.Kind {
	case "fixed":
		return d.Mean
	case "uniform":
		return d.Min + r.Float64()*(d.Max-d.Min)
	case "normal":
		v := r.NormFloat64()*d.StdDev + d.Mean
		if v < 0 {
			return 0
		}
		return v
	default:
		panic(fmt.Sprintf("twin: invalid duration kind %q", d.Kind))
	}
}
