package twin

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

// baseConfig 是含故障、质检、返工的完整产线，测试在此基础上裁剪。
func baseConfig(seed int64) *Config {
	return &Config{
		Name: "test-line", Seed: seed, Horizon: 300,
		Source: SourceConfig{
			Output: "B0", Count: 80, FirstAt: 0,
			Interval: Duration{Kind: "uniform", Min: 2, Max: 4},
		},
		Buffers: []BufferConfig{
			{ID: "B0", Capacity: 4},
			{ID: "B1", Capacity: 3},
			{ID: "B2", Capacity: 2},
			{ID: "SINK", Capacity: -1, Sink: true},
		},
		Stations: []StationConfig{
			{
				ID: "CNC", Input: "B0", Output: "B1", Servers: 2,
				Process: Duration{Kind: "uniform", Min: 4, Max: 7},
				Failure: &FailureSpec{
					Kind: "scheduled", Offset: 40, Interval: 100, Jitter: 0,
					Repair: Duration{Kind: "uniform", Min: 5, Max: 10},
				},
			},
			{
				ID: "QC", Input: "B1", Output: "SINK", Servers: 1,
				Process: Duration{Kind: "fixed", Mean: 2},
				Inspection: &InspectionSpec{
					FailRate: 0.3, MaxRework: 2, ReworkBuffer: "B2", OnExceed: "scrap",
				},
			},
			{
				ID: "RW", Input: "B2", Output: "B1", Servers: 1,
				Process: Duration{Kind: "uniform", Min: 3, Max: 5},
			},
		},
	}
}

func runCfg(t *testing.T, cfg *Config) *Result {
	t.Helper()
	s, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	r, err := s.Run()
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	return r
}

func checkConservation(t *testing.T, r *Result) {
	t.Helper()
	if r.Released != r.Completed+r.Scrapped+r.WIP {
		t.Fatalf("ledger broken: released=%d completed=%d scrapped=%d wip=%d",
			r.Released, r.Completed, r.Scrapped, r.WIP)
	}
	var traceWIP int64
	for _, tr := range r.Traces {
		if !tr.Scrapped && tr.FinishedAt == 0 {
			traceWIP++
		}
	}
	if traceWIP != r.WIP {
		t.Fatalf("trace WIP=%d disagrees with ledger wip=%d", traceWIP, r.WIP)
	}
}

func TestDeterministicReproduction(t *testing.T) {
	t.Log("场景: 确定性复现 -- 相同配置+种子两次运行的统计与每件轨迹必须完全一致")
	r1 := runCfg(t, baseConfig(123))
	r2 := runCfg(t, baseConfig(123))

	if r1.Completed != r2.Completed || r1.Scrapped != r2.Scrapped || r1.WIP != r2.WIP ||
		r1.Released != r2.Released || r1.AvgWIP != r2.AvgWIP || r1.LastEventTime != r2.LastEventTime {
		t.Fatalf("aggregate stats differ: %+v vs %+v", r1, r2)
	}
	if fmt.Sprint(r1.Stations) != fmt.Sprint(r2.Stations) {
		t.Fatalf("station stats differ")
	}
	if fmt.Sprint(r1.Traces) != fmt.Sprint(r2.Traces) {
		t.Fatalf("part traces differ between two runs with same seed")
	}
	t.Logf("用例通过: completed=%d scrapped=%d wip=%d avgWIP=%.3f, %d 件轨迹完全一致",
		r1.Completed, r1.Scrapped, r1.WIP, r1.AvgWIP, len(r1.Traces))
}

func TestMaterialConservation(t *testing.T) {
	t.Log("场景: 物料守恒 -- 长 horizon 下全部物料终态明确，投入=产出+报废")
	cfg := baseConfig(7)
	cfg.Horizon = 2000
	r := runCfg(t, cfg)
	if r.Released != r.Planned {
		t.Fatalf("expected all %d released, got %d (pending=%d)", r.Planned, r.Released, r.Pending)
	}
	if r.WIP != 0 {
		t.Fatalf("expected zero WIP at long horizon, got %d", r.WIP)
	}
	checkConservation(t, r)
	t.Logf("用例通过: 投入=%d = 产出=%d + 报废=%d + 在制=%d",
		r.Released, r.Completed, r.Scrapped, r.WIP)
}

func TestConservationWithBackpressure(t *testing.T) {
	t.Log("场景: 背压守恒 -- 短 horizon + 小缓冲, 未入线物料也不凭空消失")
	cfg := baseConfig(7)
	cfg.Horizon = 30
	r := runCfg(t, cfg)
	checkConservation(t, r)
	if r.Planned != r.Released+r.Pending+r.NotArrived {
		t.Fatalf("planned=%d != released=%d + pending=%d + notArrived=%d",
			r.Planned, r.Released, r.Pending, r.NotArrived)
	}
	t.Logf("用例通过: planned=%d released=%d pending=%d notArrived=%d completed=%d wip=%d",
		r.Planned, r.Released, r.Pending, r.NotArrived, r.Completed, r.WIP)
}

func TestFaultFreezesAndResumes(t *testing.T) {
	t.Log("场景: 设备故障 -- 故障期间无产出, 维修后按剩余时长继续")
	cfg := baseConfig(5)
	cfg.Horizon = 100
	cfg.Source = SourceConfig{Output: "B0", Count: 3, FirstAt: 0,
		Interval: Duration{Kind: "fixed", Mean: 40}}
	cfg.Buffers = []BufferConfig{
		{ID: "B0", Capacity: 10},
		{ID: "SINK", Capacity: -1, Sink: true},
	}
	cfg.Stations = []StationConfig{
		{
			ID: "M", Input: "B0", Output: "SINK", Servers: 1,
			Process: Duration{Kind: "fixed", Mean: 10},
			Failure: &FailureSpec{
				Kind: "scheduled", Offset: 3, Interval: 1000,
				Repair: Duration{Kind: "fixed", Mean: 4},
			},
		},
	}
	r := runCfg(t, cfg)
	checkConservation(t, r)

	tr := r.Traces[0]
	var frozenAt, resumedAt, completedAt float64 = -1, -1, -1
	for _, e := range tr.Events {
		switch e.Detail {
		case "processing-frozen":
			frozenAt = e.Time
		case "processing-resumed":
			resumedAt = e.Time
		}
		if e.Kind == "completed" {
			completedAt = e.Time
		}
	}
	// 0 开工 -> 3 冻结（已加工3）-> 7 恢复（剩余7）-> 14 完成。
	if frozenAt != 3 || resumedAt != 7 || completedAt != 14 {
		t.Fatalf("freeze/resume timing wrong: frozen=%v resumed=%v completed=%v",
			frozenAt, resumedAt, completedAt)
	}
	if completedAt < resumedAt {
		t.Fatalf("part completed during downtime")
	}
	if r.Completed < 2 {
		t.Fatalf("expected production to resume after repair, completed=%d wip=%d", r.Completed, r.WIP)
	}
	if got := r.Stations[0].Downtime; got != 4 {
		t.Fatalf("downtime = %v, want 4", got)
	}
	t.Logf("用例通过: 冻结@%.0f 恢复@%.0f 完成@%.0f, 维修后继续产出=%d, 停机=%.0f",
		frozenAt, resumedAt, completedAt, r.Completed, r.Stations[0].Downtime)
}

func TestRandomFault(t *testing.T) {
	t.Log("场景: 随机故障 -- MTBF 指数间隔, 多次故障且守恒")
	cfg := baseConfig(11)
	cfg.Stations[0].Failure = &FailureSpec{
		Kind: "random", MTBF: 20, RepairMin: 2, RepairMax: 6,
	}
	r := runCfg(t, cfg)
	checkConservation(t, r)
	var down float64
	for _, st := range r.Stations {
		if st.ID == "CNC" {
			down = st.Downtime
		}
	}
	if down <= 0 {
		t.Fatalf("expected some downtime under random failures, got 0")
	}
	t.Logf("用例通过: CNC 随机停机=%.2f, completed=%d scrapped=%d", down, r.Completed, r.Scrapped)
}

func TestReworkLoopAndScrap(t *testing.T) {
	t.Log("场景: 质检返工 -- 必不合格时返工次数有上限, 超限处置确定, 无死循环")
	cfg := baseConfig(1)
	cfg.Horizon = 500
	cfg.Source.Count = 5
	cfg.Stations[1].Inspection = &InspectionSpec{
		FailRate: 1, MaxRework: 2, ReworkBuffer: "B2", OnExceed: "scrap",
	}
	r := runCfg(t, cfg)
	checkConservation(t, r)
	if r.Completed != 0 || r.Scrapped != 5 {
		t.Fatalf("failRate=1 + maxRework=2 => all scrap, got completed=%d scrapped=%d",
			r.Completed, r.Scrapped)
	}
	for _, tr := range r.Traces {
		var reworks int
		for _, e := range tr.Events {
			if e.Kind == "rework" {
				reworks++
			}
		}
		if reworks != 2 {
			t.Fatalf("part %d reworks=%d, want 2", tr.PartID, reworks)
		}
		if !tr.Scrapped || tr.FinishedAt == 0 {
			t.Fatalf("part %d should be scrapped with finish time", tr.PartID)
		}
	}
	t.Logf("用例通过(scrap): 5 件均返工 2 次后报废, QC 处理=%d 次", r.Stations[1].Processed)

	t.Log("用例: onExceed=accept 时超限让步接收")
	cfg2 := baseConfig(1)
	cfg2.Horizon = 500
	cfg2.Source.Count = 3
	cfg2.Stations[1].Inspection = &InspectionSpec{
		FailRate: 1, MaxRework: 1, ReworkBuffer: "B2", OnExceed: "accept",
	}
	r2 := runCfg(t, cfg2)
	checkConservation(t, r2)
	if r2.Scrapped != 0 || r2.Completed != 3 {
		t.Fatalf("accept policy: completed=%d scrapped=%d, want 3/0", r2.Completed, r2.Scrapped)
	}
	t.Logf("用例通过(accept): completed=%d scrapped=%d", r2.Completed, r2.Scrapped)
}

func TestPartTracesCoherent(t *testing.T) {
	t.Log("场景: 轨迹一致 -- 每件单一投入、单一终态、时间有序")
	r := runCfg(t, baseConfig(42))
	checkConservation(t, r)
	for _, tr := range r.Traces {
		var released, finished, scrapped int
		last := -1.0
		for _, e := range tr.Events {
			if e.Time < last {
				t.Fatalf("part %d trace out of order", tr.PartID)
			}
			last = e.Time
			switch e.Kind {
			case "released":
				released++
			case "finished":
				finished++
			case "scrapped":
				scrapped++
			}
		}
		if released != 1 || finished > 1 || scrapped > 1 || (finished == 1 && scrapped == 1) {
			t.Fatalf("part %d bad terminal events: released=%d finished=%d scrapped=%d",
				tr.PartID, released, finished, scrapped)
		}
	}
	t.Logf("用例通过: %d 件轨迹均合法", len(r.Traces))
}

func TestInvalidConfigs(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*Config)
		want error
	}{
		{"重复缓冲标识", func(c *Config) {
			c.Buffers = append(c.Buffers, BufferConfig{ID: "B0", Capacity: 1})
		}, ErrDuplicateID},
		{"重复工位标识", func(c *Config) {
			c.Stations = append(c.Stations, c.Stations[0])
		}, ErrDuplicateID},
		{"未知输入缓冲", func(c *Config) {
			c.Stations[0].Input = "NOPE"
		}, ErrUnknownReference},
		{"未知返工缓冲", func(c *Config) {
			c.Stations[1].Inspection.ReworkBuffer = "NOPE"
		}, ErrUnknownReference},
		{"投料指向未知缓冲", func(c *Config) {
			c.Source.Output = "NOPE"
		}, ErrUnknownReference},
		{"非法缓冲容量0", func(c *Config) {
			c.Buffers[0].Capacity = 0
		}, ErrInvalidCapacity},
		{"非法缓冲容量负", func(c *Config) {
			c.Buffers[0].Capacity = -2
		}, ErrInvalidCapacity},
		{"sink 容量非无限", func(c *Config) {
			for i := range c.Buffers {
				if c.Buffers[i].Sink {
					c.Buffers[i].Capacity = 9
				}
			}
		}, ErrInvalidCapacity},
		{"加工时长为0", func(c *Config) {
			c.Stations[1].Process = Duration{Kind: "fixed", Mean: 0}
		}, ErrInvalidDuration},
		{"均匀分布上下界颠倒", func(c *Config) {
			c.Stations[0].Process = Duration{Kind: "uniform", Min: 9, Max: 1}
		}, ErrInvalidDuration},
		{"未知分布类型", func(c *Config) {
			c.Stations[1].Process = Duration{Kind: "weibull"}
		}, ErrInvalidDuration},
		{"负设备数", func(c *Config) {
			c.Stations[0].Servers = 0
		}, ErrInvalidParameter},
		{"质检概率越界", func(c *Config) {
			c.Stations[1].Inspection.FailRate = 1.5
		}, ErrInvalidParameter},
		{"返工次数为负", func(c *Config) {
			c.Stations[1].Inspection.MaxRework = -1
		}, ErrInvalidParameter},
		{"未知超限处置", func(c *Config) {
			c.Stations[1].Inspection.OnExceed = "explode"
		}, ErrInvalidParameter},
		{"故障间隔为0", func(c *Config) {
			c.Stations[0].Failure = &FailureSpec{Kind: "scheduled", Interval: 0,
				Repair: Duration{Kind: "fixed", Mean: 1}}
		}, ErrInvalidDuration},
		{"随机故障维修界非法", func(c *Config) {
			c.Stations[0].Failure = &FailureSpec{Kind: "random", MTBF: 10, RepairMin: 5, RepairMax: 5}
		}, ErrInvalidDuration},
		{"sink 不可达", func(c *Config) {
			c.Stations[1].Output = "B2"
		}, ErrInvalidRoute},
		{"返工回到sink", func(c *Config) {
			c.Stations[1].Inspection.ReworkBuffer = "SINK"
		}, ErrInvalidRoute},
		{"horizon非正", func(c *Config) {
			c.Horizon = 0
		}, ErrInvalidDuration},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := baseConfig(1)
			tc.mut(cfg)
			_, err := New(cfg)
			if err == nil {
				t.Fatalf("expected build failure %v, got nil", tc.want)
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want errors.Is %v", err, tc.want)
			}
			t.Logf("场景[非法配置/%-12s] 用例通过, 拒绝原因: %v", tc.name, err)
		})
	}
}

func TestConcurrentScenarios(t *testing.T) {
	t.Log("场景: 多情景并发 -- 24 个不同种子并发运行, 结果互不干扰且各自守恒")
	const n = 24
	var wg sync.WaitGroup
	errs := make(chan error, n)
	results := make([]*Result, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			cfg := baseConfig(int64(1000 + idx))
			r, err := New(cfg)
			if err != nil {
				errs <- err
				return
			}
			res, err := r.Run()
			if err != nil {
				errs <- err
				return
			}
			if res.Released != res.Completed+res.Scrapped+res.WIP {
				errs <- fmt.Errorf("seed %d conservation broken", cfg.Seed)
				return
			}
			results[idx] = res
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	// 并发结果必须与独立串行运行完全一致（证明无共享串扰）。
	for i := 0; i < n; i++ {
		serial := runCfg(t, baseConfig(int64(1000+i)))
		got := results[i]
		if got.Completed != serial.Completed || got.Scrapped != serial.Scrapped ||
			fmt.Sprint(got.Traces) != fmt.Sprint(serial.Traces) {
			t.Fatalf("scenario seed=%d concurrent result differs from serial run", 1000+i)
		}
	}
	t.Logf("用例通过: %d 个情景并发完成, 各自守恒且与串行结果一致 (样例: seed=1000 completed=%d scrapped=%d)",
		n, results[0].Completed, results[0].Scrapped)
}

func TestExampleConfig(t *testing.T) {
	t.Log("场景: 示例配置 examples/line.json 可构建并完成一次仿真")
	cfg, err := LoadConfig("../../examples/line.json")
	if err != nil {
		t.Fatal(err)
	}
	r := runCfg(t, cfg)
	checkConservation(t, r)
	if r.Released == 0 {
		t.Fatal("expected at least one released part")
	}
	t.Logf("用例通过: released=%d completed=%d scrapped=%d wip=%d",
		r.Released, r.Completed, r.Scrapped, r.WIP)
}
