package twin

import (
	"encoding/json"
	"os"
)

// Duration 描述带随机波动的时长（仿真时间单位，如秒）。
//
// Kind 支持:
//   - "fixed":   固定值 Mean
//   - "uniform": [Min, Max) 均匀分布
//   - "normal":  均值 Mean、标准差 StdDev 的正态分布（夹到 >=0）
type Duration struct {
	Kind   string  `json:"kind"`
	Mean   float64 `json:"mean,omitempty"`
	Min    float64 `json:"min,omitempty"`
	Max    float64 `json:"max,omitempty"`
	StdDev float64 `json:"stddev,omitempty"`
}

// FailureSpec 描述工位设备异常。两种触发方式二选一：
//
//   - 计划故障 Scheduled：按 Interval 周期发生，持续 Repair，
//     Interval/Repair 可带 Jitter 均匀波动。
//   - 随机故障 Random：相邻故障间隔服从均值 MTBF 的指数分布，
//     维修时长服从 [RepairMin, RepairMax) 均匀分布。
type FailureSpec struct {
	Kind      string   `json:"kind"`                // "scheduled" | "random"
	Interval  float64  `json:"interval,omitempty"`  // scheduled: 周期间隔
	Jitter    float64  `json:"jitter,omitempty"`    // scheduled: 间隔抖动幅度
	Offset    float64  `json:"offset,omitempty"`    // scheduled: 首次故障偏移
	MTBF      float64  `json:"mtbf,omitempty"`      // random: 平均故障间隔
	Repair    Duration `json:"repair,omitempty"`    // scheduled: 维修时长
	RepairMin float64  `json:"repairMin,omitempty"` // random: 最短维修
	RepairMax float64  `json:"repairMax,omitempty"` // random: 最长维修
}

// InspectionSpec 描述质检规则。
//
// 每次加工完成按 FailRate 概率判定为不合格：不合格且未超过 MaxRework 时
// 送往 ReworkBuffer 返工；超过 MaxRework 时按 OnExceed 处置
// （"scrap" 报废 | "accept" 让步接收为合格品）。
type InspectionSpec struct {
	FailRate     float64 `json:"failRate"`
	MaxRework    int     `json:"maxRework"`
	ReworkBuffer string  `json:"reworkBuffer"`
	OnExceed     string  `json:"onExceed"` // 默认 scrap
}

// StationConfig 描述一个工位（可含多台同构并行设备 Servers）。
type StationConfig struct {
	ID         string          `json:"id"`
	Input      string          `json:"input"`
	Output     string          `json:"output"`
	Servers    int             `json:"servers"`
	Process    Duration        `json:"process"`
	Failure    *FailureSpec    `json:"failure,omitempty"`
	Inspection *InspectionSpec `json:"inspection,omitempty"`
}

// BufferConfig 描述工位之间的缓冲。Sink 缓冲容量必须为 -1（无限）。
type BufferConfig struct {
	ID       string `json:"id"`
	Capacity int    `json:"capacity"` // -1 表示无限容量
	Sink     bool   `json:"sink,omitempty"`
}

// SourceConfig 描述投料：从首次投料 FirstAt 起，每 Interval（带 Jitter
// 均匀波动）投入一件，共 Count 件。
type SourceConfig struct {
	Output   string   `json:"output"`
	Count    int      `json:"count"`
	FirstAt  float64  `json:"firstAt"`
	Interval Duration `json:"interval"`
}

// Config 是一条产线的完整本地描述。
type Config struct {
	Name     string          `json:"name"`
	Seed     int64           `json:"seed"`
	Horizon  float64         `json:"horizon"`
	Source   SourceConfig    `json:"source"`
	Buffers  []BufferConfig  `json:"buffers"`
	Stations []StationConfig `json:"stations"`
}

// LoadConfig 从本地 JSON 文件读取配置（不访问网络）。
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// deepCopy 返回配置的完全独立副本（切片与指针都复制），
// 使多个仿真情景即使由同一份 Config 派生也不会共享可变状态。
func (cfg *Config) deepCopy() *Config {
	c := *cfg
	c.Buffers = append([]BufferConfig(nil), cfg.Buffers...)
	c.Stations = make([]StationConfig, len(cfg.Stations))
	for i, st := range cfg.Stations {
		c.Stations[i] = st
		if st.Failure != nil {
			f := *st.Failure
			c.Stations[i].Failure = &f
		}
		if st.Inspection != nil {
			in := *st.Inspection
			c.Stations[i].Inspection = &in
		}
	}
	return &c
}
