package sim

import (
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	"github.com/lechandonga/production-line-twin-go/internal/config"
)

// 测试共用配置：三工位 + 出口，S2 有质检返工，S1/S2 有故障。
func testConfig(seed int64) *config.Config {
	return &config.Config{
		Name:      "test-line",
		Seed:      seed,
		MaxTimeMs: 30000,
		Feed:      config.Feed{IntervalMinMs: 900, IntervalMaxMs: 1300},
		Stations: []config.Station{
			{
				ID: "S1", BufferCapacity: 2,
				DurationMinMs: 800, DurationMaxMs: 1200,
				FailPerHour: 300, RepairMinMs: 1000, RepairMaxMs: 4000,
			},
			{
				ID: "S2", BufferCapacity: 2,
				DurationMinMs: 900, DurationMaxMs: 1400,
				FailPerHour: 200, RepairMinMs: 1000, RepairMaxMs: 3000,
				CheckQuality: true, QualityFailRate: 0.2,
				ReworkTo: "S1", ReworkMax: 2,
			},
			{
				ID: "S3", BufferCapacity: 1,
				DurationMinMs: 700, DurationMaxMs: 1100,
			},
			{ID: "SINK", BufferCapacity: 0, IsSink: true},
		},
	}
}

func run(t *testing.T, cfg *config.Config) *Report {
	t.Helper()
	s, err := New(cfg)
	if err != nil {
		t.Fatalf("构建仿真器失败: %v", err)
	}
	return s.Run()
}

// TestConservation 任意时刻物料账守恒：投入 == 产出 + 报废 + 在制。
func TestConservation(t *testing.T) {
	t.Log("场景[物料守恒]: 跑多个种子，校验投入/产出/报废/在制不重不漏")
	for _, seed := range []int64{1, 7, 42, 99, 123456} {
		cfg := testConfig(seed)
		r := run(t, cfg)
		account := r.Output + r.Scrap + int64(r.WIP)
		t.Logf("  用例 seed=%-6d => 投入=%d 产出=%d 报废=%d 在制=%d", seed, r.Fed, r.Output, r.Scrap, r.WIP)
		if r.Fed != account {
			t.Fatalf("seed=%d 物料不守恒: 投入=%d 但 产出+报废+在制=%d", seed, r.Fed, account)
		}
	}
}

// TestDeterminism 同配置同种子重复运行，统计与每件制品轨迹必须逐字节一致。
func TestDeterminism(t *testing.T) {
	t.Log("场景[确定性复现]: 同种子跑两次，比较统计与全部制品轨迹")
	mk := func() *Report { return run(t, testConfig(2024)) }
	r1, r2 := mk(), mk()
	if fmt.Sprint(r1) != fmt.Sprint(r2) {
		t.Fatalf("同种子重复运行结果不一致")
	}
	if len(r1.Traces) == 0 {
		t.Fatal("轨迹为空，无法校验")
	}
	t.Logf("  用例 => 制品数=%d 产出=%d 报废=%d，两次运行完全一致", len(r1.Traces), r1.Output, r1.Scrap)

	// 不同种子应产生不同结果（极不可能相同）。
	other := run(t, testConfig(2025))
	same := fmt.Sprint(other) == fmt.Sprint(r1)
	t.Logf("  用例 => 换种子后结果与原种子相同? %v (应为 false)", same)
	if same {
		t.Fatal("不同种子结果竟完全相同")
	}

	// 轨迹内部一致性：每件制品起点为 feed；产出/报废件以 output/scrap 结尾。
	for id, tr := range r1.Traces {
		if len(tr) == 0 || tr[0].Kind != "feed" {
			t.Fatalf("制品 %d 轨迹未以 feed 开始", id)
		}
		last := tr[len(tr)-1].Kind
		if last != "output" && last != "scrap" {
			// 结束时仍在制也是合法结尾
			if last != "feed" && last != "blocked" && last != "start" && last != "resume" && last != "fault" && last != "rework" && last != "pass" {
				t.Fatalf("制品 %d 轨迹结尾异常: %s", id, last)
			}
		}
		for k := 1; k < len(tr); k++ {
			if tr[k].TimeMs < tr[k-1].TimeMs {
				t.Fatalf("制品 %d 轨迹时间倒流", id)
			}
		}
	}
}

// TestFault 故障期间不得产出完工结果；维修后产线继续推进。
func TestFault(t *testing.T) {
	t.Log("场景[设备故障]: S1 高频故障，校验故障发生且维修后仍有产出")
	cfg := testConfig(77)
	r := run(t, cfg)
	var faults int64
	var downMs int64
	for _, st := range r.Stations {
		faults += st.FaultCount
		downMs += st.DownMs
	}
	t.Logf("  用例 => 故障总次数=%d 总维修时间=%dms 产出=%d", faults, downMs, r.Output)
	if faults == 0 {
		t.Fatal("期望出现故障但未出现")
	}
	if downMs <= 0 {
		t.Fatal("故障次数>0 但维修停机时间为0")
	}
	if r.Output == 0 {
		t.Fatal("维修后产线未恢复产出")
	}
	if r.Fed != r.Output+r.Scrap+int64(r.WIP) {
		t.Fatal("故障情景物料不守恒")
	}
}

// TestRework 返工回到明确的上游工位；超上限的制品被报废。
func TestRework(t *testing.T) {
	t.Log("场景[质检返工]: 100%% 不合格率 + rework_max=1，第二件起必报废")
	cfg := testConfig(5)
	for i := range cfg.Stations {
		cfg.Stations[i].FailPerHour = 0 // 排除故障干扰
	}
	s2 := &cfg.Stations[1]
	s2.QualityFailRate = 1.0
	s2.ReworkMax = 1
	r := run(t, cfg)

	rewound := 0
	scrapped := 0
	var maxReworks int
	for _, tr := range r.Traces {
		for _, e := range tr {
			if e.Kind == "rework" {
				rewound++
				if e.Station != "S1" {
					t.Fatalf("返工回流位置错误: %s", e.Station)
				}
			}
			if e.Kind == "scrap" {
				scrapped++
			}
		}
		// reworks 计数写在 detail 里，直接统计 rework 条数
		count := 0
		for _, e := range tr {
			if e.Kind == "rework" {
				count++
			}
		}
		if count > maxReworks {
			maxReworks = count
		}
	}
	t.Logf("  用例 => 回流次数=%d 报废件=%d 单件最大返工=%d (上限1)", rewound, scrapped, maxReworks)
	if rewound == 0 {
		t.Fatal("期望发生返工回流但未发生")
	}
	if scrapped == 0 {
		t.Fatal("期望超返工上限后报废但未发生")
	}
	if maxReworks > 1 {
		t.Fatalf("存在返工 %d 次的制品，超过上限 1", maxReworks)
	}
	if r.Output != 0 {
		t.Fatalf("100%%不合格率下不应有产出，实际 %d", r.Output)
	}
}

// TestInvalidConfigs 非法配置必须在构建阶段被拒绝，且错误类别可区分。
func TestInvalidConfigs(t *testing.T) {
	t.Log("场景[非法配置]: 各类问题配置必须在构建阶段被明确拒绝")
	cases := []struct {
		name string
		mut  func(*config.Config)
		want error
	}{
		{"空产线", func(c *config.Config) { c.Stations = nil }, config.ErrEmptyConfig},
		{"重复工位标识", func(c *config.Config) { c.Stations[1].ID = "S1" }, config.ErrDuplicateID},
		{"负缓冲容量", func(c *config.Config) { c.Stations[0].BufferCapacity = -1 }, config.ErrInvalidCapacity},
		{"加工时长反转", func(c *config.Config) {
			c.Stations[0].DurationMinMs, c.Stations[0].DurationMaxMs = 9, 1
		}, config.ErrInvalidDuration},
		{"加工时长非正", func(c *config.Config) { c.Stations[0].DurationMinMs = 0 }, config.ErrInvalidDuration},
		{"维修时长非法", func(c *config.Config) { c.Stations[0].RepairMinMs = 0 }, config.ErrInvalidDuration},
		{"返工引用不存在工位", func(c *config.Config) {
			c.Stations[1].ReworkTo = "GHOST"
		}, config.ErrUnknownReference},
		{"返工指向自己(成环)", func(c *config.Config) {
			c.Stations[1].ReworkTo = "S2"
		}, config.ErrReworkCycle},
		{"返工指向下游(成环)", func(c *config.Config) {
			c.Stations[1].ReworkTo = "S3"
		}, config.ErrReworkCycle},
		{"不合格率越界", func(c *config.Config) {
			c.Stations[1].QualityFailRate = 1.5
		}, config.ErrInvalidRate},
		{"负故障率", func(c *config.Config) { c.Stations[0].FailPerHour = -1 }, config.ErrInvalidRate},
		{"缺出口", func(c *config.Config) {
			c.Stations[3].IsSink = false
			c.Stations[3].DurationMinMs, c.Stations[3].DurationMaxMs = 1, 2
		}, config.ErrInvalidSink},
		{"质检缺回流工位", func(c *config.Config) { c.Stations[1].ReworkTo = "" }, config.ErrInvalidRework},
		{"投料间隔非法", func(c *config.Config) {
			c.Feed.IntervalMinMs, c.Feed.IntervalMaxMs = 10, 5
		}, config.ErrInvalidFeed},
		{"max_time 非正", func(c *config.Config) { c.MaxTimeMs = 0 }, config.ErrInvalidMaxTime},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig(1)
			tc.mut(cfg)
			err := cfg.Validate()
			if err == nil {
				t.Fatalf("用例 %q: 期望拒绝但通过了", tc.name)
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("用例 %q: 错误类别不符 got=%v want=%v", tc.name, err, tc.want)
			}
			t.Logf("  用例 %-22s 已拒绝: %v", tc.name, err)
		})
	}

	// 非法配置不得能构建仿真器。
	bad := testConfig(1)
	bad.Stations[0].BufferCapacity = -1
	if _, err := New(bad); err == nil {
		t.Fatal("非法配置竟构建出了仿真器")
	}
}

// TestLoadExampleConfig 仓库自带示例配置必须可加载、可运行、守恒。
func TestLoadExampleConfig(t *testing.T) {
	t.Log("场景[示例配置]: 加载 configs/line.json 并仿真")
	_, thisFile, _, _ := runtime.Caller(0)
	path := filepath.Join(filepath.Dir(thisFile), "..", "..", "configs", "line.json")
	cfg, err := config.LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	r := run(t, cfg)
	t.Logf("  用例 => 投入=%d 产出=%d 报废=%d 在制=%d", r.Fed, r.Output, r.Scrap, r.WIP)
	if r.Fed != r.Output+r.Scrap+int64(r.WIP) {
		t.Fatal("示例配置物料不守恒")
	}
}

// TestConcurrentScenarios 多个情景并发运行互不干扰，竞态检测下也须稳定。
func TestConcurrentScenarios(t *testing.T) {
	t.Log("场景[并发多情景]: 多 goroutine 各跑不同种子并互相独立复现")
	seeds := []int64{1, 2, 3, 4, 5, 6, 7, 8}
	results := make([]*Report, len(seeds))
	var wg sync.WaitGroup
	for i, seed := range seeds {
		wg.Add(1)
		go func(i int, seed int64) {
			defer wg.Done()
			results[i] = run(t, testConfig(seed))
		}(i, seed)
	}
	wg.Wait()
	for i, r := range results {
		if r.Fed != r.Output+r.Scrap+int64(r.WIP) {
			t.Fatalf("情景 seed=%d 物料不守恒", seeds[i])
		}
		// 独立复现：再跑一次必须一致
		again := run(t, testConfig(seeds[i]))
		if fmt.Sprint(again) != fmt.Sprint(r) {
			t.Fatalf("情景 seed=%d 并发后失去确定性", seeds[i])
		}
		t.Logf("  用例 seed=%d => 投入=%d 产出=%d 报废=%d 在制=%d", seeds[i], r.Fed, r.Output, r.Scrap, r.WIP)
	}
}
