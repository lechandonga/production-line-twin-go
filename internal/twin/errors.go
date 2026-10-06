package twin

import (
	"errors"
	"fmt"
)

// 构建阶段可区分的错误原因。调用方使用 errors.Is 判断具体类别。
var (
	// ErrDuplicateID 表示工位或缓冲标识重复。
	ErrDuplicateID = errors.New("twin: duplicate id")
	// ErrUnknownReference 表示配置引用了不存在的工位或缓冲。
	ErrUnknownReference = errors.New("twin: unknown reference")
	// ErrInvalidCapacity 表示缓冲容量非法（负数或普通缓冲为 0）。
	ErrInvalidCapacity = errors.New("twin: invalid capacity")
	// ErrInvalidDuration 表示加工/间隔/维修时长非法（负数或下界大于上界）。
	ErrInvalidDuration = errors.New("twin: invalid duration")
	// ErrInvalidParameter 表示概率、数量、阈值等参数非法。
	ErrInvalidParameter = errors.New("twin: invalid parameter")
	// ErrInvalidRoute 表示工序流转或返工回流位置构不成合法路径。
	ErrInvalidRoute = errors.New("twin: invalid route")
	// ErrMissingField 表示必填字段缺失。
	ErrMissingField = errors.New("twin: missing field")
	// ErrConservation 表示仿真过程中物料账不平衡（属于内部不变量被破坏）。
	ErrConservation = errors.New("twin: material conservation violated")
)

// BuildError 携带错误类别与定位信息，便于在构建阶段明确拒绝问题配置。
type BuildError struct {
	Cause error
	Where string
	Msg   string
}

func (e *BuildError) Error() string {
	if e.Where != "" {
		return e.Cause.Error() + " (" + e.Where + "): " + e.Msg
	}
	return e.Cause.Error() + ": " + e.Msg
}

func (e *BuildError) Unwrap() error { return e.Cause }

func buildErr(cause error, where, format string, args ...any) error {
	return &BuildError{Cause: cause, Where: where, Msg: fmt.Sprintf(format, args...)}
}
