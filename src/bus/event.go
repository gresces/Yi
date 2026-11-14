package bus

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

// EventType 是发生的种类,格式为 "域.动作",过去时。
type EventType string

const (
	// TypeRawCaptured 表示用户主动丢进来的一条原始信息。
	TypeRawCaptured EventType = "raw.captured"
)

// ErrInvalidEvent 包裹所有校验失败。
var ErrInvalidEvent = errors.New("bus: invalid event")

// Event 是对一次已经发生之事的不可变记录。
// 它只有五个字段。位置和接收时间属于总线,不在这里。
type Event struct {
	ID     string          `json:"id"`
	Source string          `json:"source"`
	Types  []EventType     `json:"types"`
	Time   time.Time       `json:"time"`
	Data   json.RawMessage `json:"data"`
}

// Normalize 去掉空类型、去重并排序,保证同一组类型只有一种表示。
func (e *Event) Normalize() {
	if len(e.Types) == 0 {
		return
	}

	seen := make(map[EventType]struct{}, len(e.Types))
	out := make([]EventType, 0, len(e.Types))
	for _, t := range e.Types {
		t = EventType(strings.TrimSpace(string(t)))
		if t == "" {
			continue
		}
		if _, ok := seen[t]; ok {
			continue
		}
		seen[t] = struct{}{}
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	e.Types = out
}

// HasType 判断事件是否带有某个类型。
func (e Event) HasType(t EventType) bool {
	for _, x := range e.Types {
		if x == t {
			return true
		}
	}
	return false
}

// Validate 检查事件是否可发布。只校验结构,不解释 Data 的内容。
func (e Event) Validate() error {
	switch {
	case e.ID == "":
		return fmt.Errorf("%w: empty id", ErrInvalidEvent)
	case e.Source == "":
		return fmt.Errorf("%w: empty source", ErrInvalidEvent)
	case len(e.Types) == 0:
		return fmt.Errorf("%w: types is empty", ErrInvalidEvent)
	case e.Time.IsZero():
		return fmt.Errorf("%w: zero time", ErrInvalidEvent)
	}
	for _, t := range e.Types {
		if strings.TrimSpace(string(t)) == "" {
			return fmt.Errorf("%w: empty type", ErrInvalidEvent)
		}
	}
	d := bytes.TrimSpace(e.Data)
	if len(d) == 0 || d[0] != '{' || !json.Valid(d) {
		return fmt.Errorf("%w: data must be a JSON object", ErrInvalidEvent)
	}
	return nil
}
