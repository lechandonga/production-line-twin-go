package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

// Config 描述一条本地产线。
//
// 产线由若干工位顺序组成（Station i -> Station i+1），其中第一个工位是
// 进料口，最后一个工位是成品出口（IsSink=true）。
// 返工通过每个工位的 ReworkTo 指定回流目标；ReworkMax 是单个制品允许的
// 总返工次数，超过后在触发返工的工位直接报废。
type Config struct {
	Name      string    `json:"name"`
	Seed      int64     `json:"seed"`
	MaxTimeMs int64     `json:"max_time_ms"`
	Feed      Feed      `json:"feed"`
	Stations  []Station `json:"stations"`
}

// Feed 描述进料口的投料间隔（毫秒，支持随机波动）。
type Feed struct {
	IntervalMinMs int64 `json:"interval_min_ms"`
	IntervalMaxMs int64 `json:"interval_max_ms"`
}

// Station 描述一个工位。
//
// BufferCapacity 是该工位入口缓冲（不含正在加工的 1 件）的容量。
// DurationMin/Max 是加工时长（毫秒），在区间内均匀分布。
// FailPerHour / RepairMinMs / RepairMaxMs 描述设备故障：按有效加工时间
// 折算故障率，故障后随机抽维修时长；三者全为 0 表示该工位无故障。
// CheckQuality=true 时在该工位加工结束做质检，失败的制品按 ReworkTo
// 回流；IsSink=true 的工位不做质检、不加工，只负责接收成品。
type Station struct {
	ID              string  `json:"id"`
	BufferCapacity  int     `json:"buffer_capacity"`
	DurationMinMs   int64   `json:"duration_min_ms"`
	DurationMaxMs   int64   `json:"duration_max_ms"`
	FailPerHour     float64 `json:"fail_per_hour"`
	RepairMinMs     int64   `json:"repair_min_ms"`
	RepairMaxMs     int64   `json:"repair_max_ms"`
	CheckQuality    bool    `json:"check_quality"`
	QualityFailRate float64 `json:"quality_fail_rate"`
	ReworkTo        string  `json:"rework_to"`
	ReworkMax       int     `json:"rework_max"`
	IsSink          bool    `json:"is_sink"`
}

// LoadFile 从本地 JSON 文件加载配置，文件不存在或 JSON 非法时返回错误。
func LoadFile(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %q: %w", path, err)
	}
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("parse config %q: %w", path, err)
	}
	return &cfg, nil
}

// 构建阶段可区分的错误类型。用 errors.Is 判定。
var (
	ErrEmptyConfig      = errors.New("config: empty production line (no stations)")
	ErrDuplicateID      = errors.New("config: duplicate station id")
	ErrUnknownReference = errors.New("config: unknown station reference")
	ErrInvalidCapacity  = errors.New("config: invalid buffer capacity")
	ErrInvalidDuration  = errors.New("config: invalid duration range")
	ErrInvalidRate      = errors.New("config: invalid probability/rate")
	ErrInvalidSink      = errors.New("config: invalid sink layout")
	ErrInvalidRework    = errors.New("config: invalid rework setup")
	ErrInvalidFeed      = errors.New("config: invalid feed interval")
	ErrInvalidMaxTime   = errors.New("config: max_time_ms must be positive")
	ErrReworkCycle      = errors.New("config: rework route must point to an earlier station (no forward cycles)")
)

// Validate 在构建阶段拒绝一切有问题的配置，返回带具体原因的错误。
// 错误用 fmt.Errorf("%w: ...", ErrXxx) 包装，可用 errors.Is 区分类别。
func (c *Config) Validate() error {
	if len(c.Stations) == 0 {
		return ErrEmptyConfig
	}
	if c.MaxTimeMs <= 0 {
		return fmt.Errorf("%w: got %d", ErrInvalidMaxTime, c.MaxTimeMs)
	}
	if c.Feed.IntervalMinMs <= 0 || c.Feed.IntervalMaxMs < c.Feed.IntervalMinMs {
		return fmt.Errorf("%w: min=%d max=%d", ErrInvalidFeed, c.Feed.IntervalMinMs, c.Feed.IntervalMaxMs)
	}

	index := make(map[string]int, len(c.Stations))
	sinkCount := 0
	for i, s := range c.Stations {
		if s.ID == "" {
			return fmt.Errorf("%w: station at index %d has empty id", ErrDuplicateID, i)
		}
		if _, ok := index[s.ID]; ok {
			return fmt.Errorf("%w: %q", ErrDuplicateID, s.ID)
		}
		index[s.ID] = i
		if s.IsSink {
			sinkCount++
		}
	}
	if sinkCount != 1 || !c.Stations[len(c.Stations)-1].IsSink {
		return fmt.Errorf("%w: require exactly one sink and it must be the last station, got %d sink(s)", ErrInvalidSink, sinkCount)
	}

	for i, s := range c.Stations {
		if s.BufferCapacity < 0 {
			return fmt.Errorf("%w: station %q buffer_capacity=%d", ErrInvalidCapacity, s.ID, s.BufferCapacity)
		}
		if !s.IsSink {
			if s.DurationMinMs <= 0 || s.DurationMaxMs < s.DurationMinMs {
				return fmt.Errorf("%w: station %q duration min=%d max=%d", ErrInvalidDuration, s.ID, s.DurationMinMs, s.DurationMaxMs)
			}
		}
		if s.FailPerHour < 0 {
			return fmt.Errorf("%w: station %q fail_per_hour=%v", ErrInvalidRate, s.ID, s.FailPerHour)
		}
		if s.FailPerHour > 0 {
			if s.RepairMinMs <= 0 || s.RepairMaxMs < s.RepairMinMs {
				return fmt.Errorf("%w: station %q repair min=%d max=%d", ErrInvalidDuration, s.ID, s.RepairMinMs, s.RepairMaxMs)
			}
		}
		if s.QualityFailRate < 0 || s.QualityFailRate > 1 {
			return fmt.Errorf("%w: station %q quality_fail_rate=%v", ErrInvalidRate, s.ID, s.QualityFailRate)
		}
		if s.CheckQuality {
			if s.QualityFailRate < 0 || s.QualityFailRate > 1 {
				return fmt.Errorf("%w: station %q quality_fail_rate=%v", ErrInvalidRate, s.ID, s.QualityFailRate)
			}
			if s.ReworkTo == "" {
				return fmt.Errorf("%w: station %q checks quality but rework_to is empty", ErrInvalidRework, s.ID)
			}
			if s.ReworkMax < 0 {
				return fmt.Errorf("%w: station %q rework_max=%d", ErrInvalidRework, s.ID, s.ReworkMax)
			}
			if s.IsSink {
				return fmt.Errorf("%w: sink station %q cannot check quality", ErrInvalidRework, s.ID)
			}
		} else if s.ReworkTo != "" || s.QualityFailRate != 0 {
			return fmt.Errorf("%w: station %q rework settings require check_quality=true", ErrInvalidRework, s.ID)
		}
		if s.ReworkTo != "" {
			j, ok := index[s.ReworkTo]
			if !ok {
				return fmt.Errorf("%w: station %q rework_to=%q", ErrUnknownReference, s.ID, s.ReworkTo)
			}
			if j >= i {
				return fmt.Errorf("%w: station %q -> %q", ErrReworkCycle, s.ID, s.ReworkTo)
			}
		}
		_ = i
	}
	return nil
}
