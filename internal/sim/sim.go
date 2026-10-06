// Package sim 实现纯本地、离线、确定性的离散事件产线仿真器。
//
// 时间单位统一为毫秒(int64)。仿真器实例之间不共享任何状态，可在不同
// goroutine 中并发运行多个情景而互不干扰。
package sim

import (
	"container/heap"
	"fmt"
	"math"
	"math/rand"

	"github.com/lechandonga/production-line-twin-go/internal/config"
)

// Unit 是一个在制品（物料实例）。Reworks 记录它已经历的返工次数。
type Unit struct {
	ID      int64
	Reworks int
}

// TraceEntry 是制品轨迹上的一条记录。
type TraceEntry struct {
	TimeMs  int64
	UnitID  int64
	Station string // 相关工位（进料为第一个工位 id，报废为触发工位）
	Kind    string // feed/start/rework/pass/scrap/output/blocked/fault/resume
	Detail  string
}

// StationStat 是工位级统计。
type StationStat struct {
	ID          string
	Processed   int64 // 完成加工的件数（含质检不合格件）
	QualityFail int64
	DownMs      int64 // 累计故障维修时间
	FaultCount  int64
}

// Report 是一次仿真的完整结果。
type Report struct {
	Name        string
	Seed        int64
	EndTimeMs   int64
	Fed         int64 // 实际投入（进入产线）的件数
	Output      int64 // 成品产出件数
	Scrap       int64 // 报废件数
	WIP         int   // 结束时刻在制品数（缓冲 + 机台加工/滞留）
	BufferSizes []int // 结束时刻各工位缓冲占用
	Stations    []StationStat
	Traces      map[int64][]TraceEntry
}

type machineState int

const (
	mIdle    machineState = iota
	mBusy                 // 正在加工
	mDown                 // 故障停机，工件留在机台上
	mBlocked              // 加工完成但目标缓冲满，工件滞留在机台上
)

type machine struct {
	state       machineState
	job         *Unit
	startActive int64 // 当前活跃加工段起点
	remainingMs int64 // 还需的有效加工毫秒

	pendingTarget int
	pendingRework bool
}

// Simulator 是单情景仿真器。非并发安全；不同情景请各建一个实例。
type Simulator struct {
	cfg *config.Config
	rng *rand.Rand
	eq  eventQueue

	time       int64
	units      map[int64]*Unit
	buffers    [][]*Unit
	machines   []machine
	sinkIdx    int
	nextUnitID int64
	eventSeq   int64

	fed, output, scrap int64
	stats              []StationStat
	traces             map[int64][]TraceEntry

	faultAt []int64 // 各机台已计划故障时刻；0 表示无计划
}

// New 用已通过 Validate 的配置构建仿真器。
func New(cfg *config.Config) (*Simulator, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	n := len(cfg.Stations)
	s := &Simulator{
		cfg:      cfg,
		rng:      rand.New(rand.NewSource(cfg.Seed)),
		buffers:  make([][]*Unit, n),
		machines: make([]machine, n),
		sinkIdx:  n - 1,
		units:    make(map[int64]*Unit),
		stats:    make([]StationStat, n),
		traces:   make(map[int64][]TraceEntry),
		faultAt:  make([]int64, n),
	}
	for i := range cfg.Stations {
		s.stats[i].ID = cfg.Stations[i].ID
	}
	return s, nil
}

// 事件同刻优先级（数值小先处理）：完工释放机台 -> 故障 -> 修复 -> 投料。
const (
	evEndProcessing = 0
	evFault         = 1
	evRepairDone    = 2
	evFeed          = 3
)

type event struct {
	time     int64
	priority int
	station  int
	seq      int64
}

type eventQueue []event

func (q eventQueue) Len() int { return len(q) }
func (q eventQueue) Less(i, j int) bool {
	if q[i].time != q[j].time {
		return q[i].time < q[j].time
	}
	if q[i].priority != q[j].priority {
		return q[i].priority < q[j].priority
	}
	if q[i].station != q[j].station {
		return q[i].station < q[j].station
	}
	return q[i].seq < q[j].seq
}
func (q eventQueue) Swap(i, j int)       { q[i], q[j] = q[j], q[i] }
func (q *eventQueue) Push(x interface{}) { *q = append(*q, x.(event)) }
func (q *eventQueue) Pop() interface{} {
	old := *q
	n := len(old)
	e := old[n-1]
	*q = old[:n-1]
	return e
}

func (s *Simulator) schedule(t int64, priority, station int) {
	s.eventSeq++
	heap.Push(&s.eq, event{time: t, priority: priority, station: station, seq: s.eventSeq})
}

func (s *Simulator) trace(u *Unit, stationIdx int, kind, detail string) {
	sid := ""
	if stationIdx >= 0 {
		sid = s.cfg.Stations[stationIdx].ID
	}
	s.traces[u.ID] = append(s.traces[u.ID], TraceEntry{
		TimeMs: s.time, UnitID: u.ID, Station: sid, Kind: kind, Detail: detail,
	})
}

func (s *Simulator) uniform(lo, hi int64) int64 {
	if hi <= lo {
		return lo
	}
	return lo + s.rng.Int63n(hi-lo+1)
}

// Run 推进到 MaxTimeMs，返回统计报告。
func (s *Simulator) Run() *Report {
	s.schedule(0, evFeed, 0) // 首件 t=0 投料

	for s.eq.Len() > 0 {
		e := heap.Pop(&s.eq).(event)
		if e.time > s.cfg.MaxTimeMs {
			break
		}
		s.time = e.time
		switch e.priority {
		case evFeed:
			s.doFeed()
		case evEndProcessing:
			s.doEndProcessing(e.station)
		case evFault:
			s.doFault(e.station)
		case evRepairDone:
			s.doRepairDone(e.station)
		}
		s.dispatch()
	}
	return s.buildReport()
}

// doFeed 在工位 0 缓冲有空位时投入一件；否则本次投料跳过（不计投入）。
// 下一次按间隔照常发生；物料只有真正进入系统才计数。
func (s *Simulator) doFeed() {
	f := s.cfg.Feed
	next := s.time + s.uniform(f.IntervalMinMs, f.IntervalMaxMs)
	if next <= s.cfg.MaxTimeMs {
		s.schedule(next, evFeed, 0)
	}
	if len(s.buffers[0]) >= s.cfg.Stations[0].BufferCapacity {
		return
	}
	s.nextUnitID++
	u := &Unit{ID: s.nextUnitID}
	s.units[u.ID] = u
	s.buffers[0] = append(s.buffers[0], u)
	s.fed++
	s.trace(u, 0, "feed", "")
}

// dispatch 按工位索引升序反复消解“阻塞搬运”和“空闲上料”，直到无变化。
func (s *Simulator) dispatch() {
	changed := true
	for changed {
		changed = false
		for i := 0; i < len(s.machines); i++ {
			m := &s.machines[i]
			if m.state == mBlocked && s.movePending(i) {
				changed = true
			}
			if m.state == mIdle && i != s.sinkIdx && s.startProcessing(i) {
				changed = true
			}
		}
	}
}

func (s *Simulator) startProcessing(i int) bool {
	if len(s.buffers[i]) == 0 {
		return false
	}
	st := s.cfg.Stations[i]
	u := s.buffers[i][0]
	s.buffers[i] = s.buffers[i][1:]

	m := &s.machines[i]
	dur := s.uniform(st.DurationMinMs, st.DurationMaxMs)
	m.state = mBusy
	m.job = u
	m.startActive = s.time
	m.remainingMs = dur
	s.schedule(s.time+dur, evEndProcessing, i)
	s.armFault(i, dur)
	s.trace(u, i, "start", fmt.Sprintf("duration_ms=%d", dur))
	return true
}

// armFault 按故障率抽首次故障的有效加工时间并挂故障事件。
// lambda = fail_per_hour / 3_600_000（每毫秒），间隔服从指数分布。
func (s *Simulator) armFault(i int, windowMs int64) {
	s.faultAt[i] = 0
	st := s.cfg.Stations[i]
	if st.FailPerHour <= 0 {
		return
	}
	lambda := st.FailPerHour / 3_600_000.0
	ttf := int64(math.Ceil(-math.Log(1-s.rng.Float64()) / lambda))
	if ttf <= 0 {
		ttf = 1
	}
	if ttf < windowMs {
		s.faultAt[i] = s.time + ttf
		s.schedule(s.faultAt[i], evFault, i)
	}
}

func (s *Simulator) doFault(i int) {
	m := &s.machines[i]
	if s.faultAt[i] != s.time || m.state != mBusy {
		return // 故障计划已失效（工件已完工）
	}
	m.remainingMs -= s.time - m.startActive
	m.state = mDown
	st := s.cfg.Stations[i]
	repair := s.uniform(st.RepairMinMs, st.RepairMaxMs)
	s.stats[i].FaultCount++
	s.stats[i].DownMs += repair
	s.trace(m.job, i, "fault", fmt.Sprintf("repair_ms=%d", repair))
	s.schedule(s.time+repair, evRepairDone, i)
}

func (s *Simulator) doRepairDone(i int) {
	m := &s.machines[i]
	if m.state != mDown {
		return
	}
	m.state = mBusy
	m.startActive = s.time
	s.schedule(s.time+m.remainingMs, evEndProcessing, i)
	s.armFault(i, m.remainingMs)
	s.trace(m.job, i, "resume", fmt.Sprintf("remaining_ms=%d", m.remainingMs))
}

func (s *Simulator) doEndProcessing(i int) {
	m := &s.machines[i]
	if m.state != mBusy {
		return // 故障期间的旧完工事件作废
	}
	st := s.cfg.Stations[i]
	u := m.job
	s.stats[i].Processed++

	// 质检结论只在加工完成时抽取一次；故障/恢复不重抽。
	target := i + 1
	scrap := false
	rework := false
	if st.CheckQuality && s.rng.Float64() < st.QualityFailRate {
		s.stats[i].QualityFail++
		if u.Reworks >= st.ReworkMax {
			scrap = true
		} else {
			u.Reworks++
			rework = true
			for k := range s.cfg.Stations {
				if s.cfg.Stations[k].ID == st.ReworkTo {
					target = k
					break
				}
			}
		}
	}

	if scrap {
		m.state = mIdle
		m.job = nil
		s.scrap++
		delete(s.units, u.ID)
		s.trace(u, i, "scrap", fmt.Sprintf("reworks=%d max=%d", u.Reworks, st.ReworkMax))
		return
	}

	if target == s.sinkIdx {
		m.state = mIdle
		m.job = nil
		s.output++ // 成品出口容量无限，直接入账
		delete(s.units, u.ID)
		s.trace(u, s.sinkIdx, "output", "")
		return
	}

	m.pendingTarget = target
	m.pendingRework = rework
	if !s.movePending(i) {
		m.state = mBlocked // 目标缓冲满，滞留机台，等待后续 dispatch
		s.trace(u, i, "blocked", fmt.Sprintf("target=%s", s.cfg.Stations[target].ID))
	}
}

// movePending 尝试把滞留工件移入目标缓冲；成功则释放机台。
func (s *Simulator) movePending(i int) bool {
	m := &s.machines[i]
	t := m.pendingTarget
	if len(s.buffers[t]) >= s.cfg.Stations[t].BufferCapacity {
		return false
	}
	u := m.job
	s.buffers[t] = append(s.buffers[t], u)
	kind := "pass"
	if m.pendingRework {
		kind = "rework"
	}
	s.trace(u, t, kind, fmt.Sprintf("from=%s reworks=%d", s.cfg.Stations[i].ID, u.Reworks))
	m.state = mIdle
	m.job = nil
	m.pendingTarget = 0
	m.pendingRework = false
	return true
}

func (s *Simulator) buildReport() *Report {
	wip := 0
	sizes := make([]int, len(s.buffers))
	for i, b := range s.buffers {
		sizes[i] = len(b)
		wip += len(b)
	}
	for i := range s.machines {
		switch s.machines[i].state {
		case mBusy, mDown, mBlocked:
			wip++
		}
	}
	// 以最后一次处理的事件时间作为结束时间；无任何事件则为 0。
	end := s.time
	if end > s.cfg.MaxTimeMs {
		end = s.cfg.MaxTimeMs
	}
	return &Report{
		Name:        s.cfg.Name,
		Seed:        s.cfg.Seed,
		EndTimeMs:   end,
		Fed:         s.fed,
		Output:      s.output,
		Scrap:       s.scrap,
		WIP:         wip,
		BufferSizes: sizes,
		Stations:    s.stats,
		Traces:      s.traces,
	}
}
