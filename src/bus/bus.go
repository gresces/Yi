package bus

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

// ErrClosed 表示总线或消费者已经关闭。
var ErrClosed = errors.New("bus: closed")

// PublishResult 是一次发布的结果。
type PublishResult struct {
	Duplicate bool `json:"duplicate"`
	// Matched 是发布这一刻订阅了该类型的事件消费者数量。
	// 0 不代表事件成了孤儿:之后注册的消费者仍会读到它。
	Matched int `json:"matched"`
}

// Failure 是一次被显式放弃的事件。
type Failure struct {
	Consumer string    `json:"consumer"`
	EventID  string    `json:"event_id"`
	Source   string    `json:"source"`
	Error    string    `json:"error"`
	At       time.Time `json:"at"`
}

// Snapshot 是一次原子读取:事件、总数、消费者和失败记录来自同一个时刻。
type Snapshot struct {
	Events       []Event         `json:"events"`
	Total        int             `json:"total"`
	Consumers    []ConsumerStats `json:"consumers"`
	Failures     []Failure       `json:"failures"`
	FailureTotal int             `json:"failure_total"`
	Producers    []ProducerStat  `json:"producers"`
	Types        []TypeCount     `json:"types"`
}

// ConsumerStats 是一个消费者的进度快照。
type ConsumerStats struct {
	Name      string      `json:"name"`
	Types     []EventType `json:"types"`
	Patterns  []string    `json:"patterns,omitempty"`
	Cursor    int64       `json:"cursor"`
	Pending   int         `json:"pending"`
	Delivered uint64      `json:"delivered"`
	Failed    uint64      `json:"failed"`
	LastError string      `json:"last_error,omitempty"`
	LastEvent string      `json:"last_event,omitempty"`
	LastAt    string      `json:"last_at,omitempty"`
}

// ProducerStat 是一个输入来源的累计状态。
type ProducerStat struct {
	Source string `json:"source"`
	Count  int    `json:"count"`
	LastAt string `json:"last_at,omitempty"`
}

// TypeCount 是某个活跃类型的事件数量。
type TypeCount struct {
	Type  EventType `json:"type"`
	Count int       `json:"count"`
}

// Metrics 是最近一段时间的吞吐量,按 1 秒一个桶。
type Metrics struct {
	Step      int   `json:"step"`
	Window    int   `json:"window"`
	Published []int `json:"published"`
	Acked     []int `json:"acked"`
	Failed    []int `json:"failed"`
	Pending   int   `json:"pending"`
}

type metricBucket struct {
	published int
	acked     int
	failed    int
}

type producerState struct {
	count  int
	lastAt time.Time
}

// Message 是一条已经取出、等待确认的事件。
type Message struct {
	Event Event

	seq int64
	c   *Consumer
}

// Ack 确认这条事件,游标推进到它。
func (m *Message) Ack() error { return m.c.settle(m.seq, nil) }

// Nack 放弃这条事件:记录一次失败,然后推进游标。
func (m *Message) Nack(err error) error {
	if err == nil {
		err = errors.New("nacked")
	}
	return m.c.settle(m.seq, err)
}

// Consumer 是注册在总线上的拉取方。它只有一个游标,不持有 goroutine。
type Consumer struct {
	bus      *Bus
	name     string
	types    map[EventType]struct{}
	patterns []string
	compiled []*regexp.Regexp

	cursor   int64 // 最后确认的 seq
	inflight int64 // 已取出、尚未确认的 seq;0 表示没有
	pending  int   // 尚未确认、且类型匹配的事件数

	notify chan struct{}
	done   chan struct{}
	closed bool

	delivered uint64
	failed    uint64
	lastErr   string
	lastEvent string
	lastAt    time.Time
}

// Next 阻塞直到事件可用、消费者关闭或 ctx 结束。
// 未确认之前重复调用会返回同一条事件,这就是重试。
func (c *Consumer) Next(ctx context.Context) (*Message, error) {
	for {
		m, err := c.tryNext()
		if err != nil {
			return nil, err
		}
		if m != nil {
			return m, nil
		}

		select {
		case <-c.notify:
		case <-c.done:
			return nil, ErrClosed
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Close 注销这个消费者,并唤醒所有阻塞中的 Next。
func (c *Consumer) Close() error {
	b := c.bus
	b.mu.Lock()
	defer b.mu.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true
	close(c.done)
	delete(b.subs, c.name)
	b.notifyLocked()

	if b.store != nil {
		if err := b.store.deleteConsumer(c.name); err != nil {
			return err
		}
	}
	b.maybeArchiveLocked()
	return nil
}

// Stats 返回这个消费者的进度。
func (c *Consumer) Stats() ConsumerStats {
	b := c.bus
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.statsLocked(c)
}

// Types 返回这个消费者订阅的事件类型。
func (c *Consumer) Types() []EventType {
	b := c.bus
	b.mu.Lock()
	defer b.mu.Unlock()
	return sortedTypes(c.types)
}

// Patterns 返回这个消费者订阅的正则表达式。
func (c *Consumer) Patterns() []string {
	b := c.bus
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), c.patterns...)
}

// Inflight 返回已取出但尚未确认的事件。
func (c *Consumer) Inflight() (Event, bool) {
	b := c.bus
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed || c.closed || c.inflight == 0 {
		return Event{}, false
	}
	e := b.log[c.inflight-b.base-1].event
	e.Data = bytes.Clone(e.Data)
	return e, true
}

// Ack 确认当前 inflight 的事件;没有时是一个空操作。
func (c *Consumer) Ack() error {
	b := c.bus
	b.mu.Lock()
	seq := c.inflight
	b.mu.Unlock()
	return c.settle(seq, nil)
}

// Nack 放弃当前 inflight 的事件;没有时是一个空操作。
func (c *Consumer) Nack(err error) error {
	if err == nil {
		err = errors.New("nacked")
	}
	b := c.bus
	b.mu.Lock()
	seq := c.inflight
	b.mu.Unlock()
	return c.settle(seq, err)
}

// Bus 是一条只追加的事件日志。它不持有 goroutine,也不调用任何回调。
type Bus struct {
	mu       sync.Mutex
	base     int64 // 已归档的最大 seq;活跃日志从 base+1 开始
	log      []storedEvent
	active   int // log 中未删除的事件数
	seen     map[key]int64
	subs     map[string]*Consumer
	failures []Failure
	closed   bool
	store    store // nil 表示纯内存

	done     chan struct{}
	watchers map[chan struct{}]struct{}
	byType   map[EventType][]int64

	metrics   map[int64]*metricBucket
	producers map[string]*producerState
	// hidden 是从生产者列表里移除、但事件仍保留的来源。发布新事件时自动恢复。
	hidden map[string]struct{}

	publishedTotal uint64
	ackedTotal     uint64
	nackedTotal    uint64
}

type key struct{ source, id string }

// storedEvent 是事件加上总线自己的信封。不导出,信封因此无法泄漏。
type storedEvent struct {
	seq        int64
	receivedAt time.Time
	event      Event
	deleted    bool
}

// New 创建一条空的内存总线。
func New() *Bus {
	return newBus(nil)
}

func newBus(s store) *Bus {
	return &Bus{
		seen:      make(map[key]int64),
		subs:      make(map[string]*Consumer),
		store:     s,
		done:      make(chan struct{}),
		watchers:  make(map[chan struct{}]struct{}),
		byType:    make(map[EventType][]int64),
		metrics:   make(map[int64]*metricBucket),
		producers: make(map[string]*producerState),
		hidden:    make(map[string]struct{}),
	}
}

// Open 打开一个持久化的总线。dir 下会生成 yi.db。
// 事件只追加;消费者游标随每次确认落盘。
func Open(dir string) (*Bus, error) {
	s, err := openSQLiteStore(dir)
	if err != nil {
		return nil, err
	}

	b := newBus(s)

	events, base, err := s.loadEvents()
	if err != nil {
		s.close()
		return nil, err
	}
	b.base = base
	for i, se := range events {
		if se.seq != base+int64(i)+1 {
			s.close()
			return nil, fmt.Errorf("bus: event log gap at seq %d (want %d)", se.seq, base+int64(i)+1)
		}
		b.log = append(b.log, se)
		b.seen[key{source: se.event.Source, id: se.event.ID}] = se.seq
		if se.deleted {
			continue
		}
		b.active++
		for _, t := range se.event.Types {
			b.byType[t] = append(b.byType[t], se.seq)
		}
	}
	for _, se := range b.log {
		b.producerLocked(se.event.Source, se.receivedAt)
	}

	hidden, err := s.loadHiddenProducers()
	if err != nil {
		s.close()
		return nil, err
	}
	for _, source := range hidden {
		b.hidden[source] = struct{}{}
	}

	state, err := s.loadState()
	if err != nil {
		s.close()
		return nil, err
	}
	b.failures = state.Failures

	for _, cs := range state.Consumers {
		if cs.Name == "" {
			continue
		}
		types := make(map[EventType]struct{}, len(cs.Types))
		for _, t := range cs.Types {
			if t != "" {
				types[t] = struct{}{}
			}
		}
		patterns, compiled, err := compilePatterns(cs.Patterns)
		if err != nil {
			s.close()
			return nil, fmt.Errorf("bus: consumer %q patterns: %w", cs.Name, err)
		}
		if len(types) == 0 && len(patterns) == 0 {
			continue
		}
		if _, exists := b.subs[cs.Name]; exists {
			continue
		}

		cursor := cs.Cursor
		// seq 的上界是 base + 活跃日志长度,不是活跃日志长度。
		if last := b.base + int64(len(b.log)); cursor > last {
			cursor = last
		}

		c := &Consumer{
			bus:       b,
			name:      cs.Name,
			types:     types,
			patterns:  patterns,
			compiled:  compiled,
			cursor:    cursor,
			delivered: cs.Delivered,
			failed:    cs.Failed,
			lastErr:   cs.LastError,
			lastEvent: cs.LastEvent,
			lastAt:    parseTime(cs.LastAt),
			notify:    make(chan struct{}, 1),
			done:      make(chan struct{}),
		}
		c.pending = b.countPendingLocked(c)
		b.subs[cs.Name] = c
	}

	return b, nil
}

// Len 返回日志中未删除的事件总数。
func (b *Bus) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.active
}

// Snapshot 原子地返回最近 limit 条事件和全部消费者状态。limit <= 0 时返回全部。
func (b *Bus) Snapshot(limit int) Snapshot {
	b.mu.Lock()
	defer b.mu.Unlock()

	if limit <= 0 || limit > len(b.log) {
		limit = len(b.log)
	}

	events, total := b.pageLocked(limit, 0, "")
	out := Snapshot{
		Events:       events,
		Total:        total,
		Consumers:    make([]ConsumerStats, 0, len(b.subs)),
		Failures:     append([]Failure{}, b.failures...),
		FailureTotal: len(b.failures),
	}
	for _, c := range b.subs {
		out.Consumers = append(out.Consumers, b.statsLocked(c))
	}
	sort.Slice(out.Consumers, func(i, j int) bool { return out.Consumers[i].Name < out.Consumers[j].Name })

	out.Producers = make([]ProducerStat, 0, len(b.producers))
	for source, p := range b.producers {
		if p.count <= 0 {
			continue
		}
		if _, hidden := b.hidden[source]; hidden {
			continue
		}
		out.Producers = append(out.Producers, ProducerStat{
			Source: source,
			Count:  p.count,
			LastAt: formatTime(p.lastAt),
		})
	}
	sort.Slice(out.Producers, func(i, j int) bool {
		if out.Producers[i].Count != out.Producers[j].Count {
			return out.Producers[i].Count > out.Producers[j].Count
		}
		return out.Producers[i].Source < out.Producers[j].Source
	})

	out.Types = make([]TypeCount, 0, len(b.byType))
	for typ, seqs := range b.byType {
		if len(seqs) > 0 {
			out.Types = append(out.Types, TypeCount{Type: typ, Count: len(seqs)})
		}
	}
	sort.Slice(out.Types, func(i, j int) bool {
		if out.Types[i].Count != out.Types[j].Count {
			return out.Types[i].Count > out.Types[j].Count
		}
		return out.Types[i].Type < out.Types[j].Type
	})

	return out
}

// Metrics 返回最近 window 秒的吞吐量,按 1 秒一个桶。
func (b *Bus) Metrics(window int) Metrics {
	b.mu.Lock()
	defer b.mu.Unlock()

	if window <= 0 || window > 600 {
		window = 120
	}
	now := time.Now().Unix()

	for ts := range b.metrics {
		if ts < now-600 {
			delete(b.metrics, ts)
		}
	}

	out := Metrics{
		Step:      1,
		Window:    window,
		Published: make([]int, window),
		Acked:     make([]int, window),
		Failed:    make([]int, window),
	}
	for i := 0; i < window; i++ {
		ts := now - int64(window-1-i)
		if bkt, ok := b.metrics[ts]; ok {
			out.Published[i] = bkt.published
			out.Acked[i] = bkt.acked
			out.Failed[i] = bkt.failed
		}
	}
	for _, c := range b.subs {
		out.Pending += c.pending
	}
	return out
}

// PrometheusStats 是 /metrics 所需的进程内快照。计数器从进程启动开始累计;
// Prometheus 会在重启时自然识别 counter reset。
type PrometheusStats struct {
	Active         int
	PublishedTotal uint64
	AckedTotal     uint64
	NackedTotal    uint64
	Failures       int
	Consumers      []ConsumerStats
	Producers      []ProducerStat
	Types          []TypeCount
}

func (b *Bus) PrometheusStats() PrometheusStats {
	b.mu.Lock()
	defer b.mu.Unlock()

	out := PrometheusStats{
		Active:         b.active,
		PublishedTotal: b.publishedTotal,
		AckedTotal:     b.ackedTotal,
		NackedTotal:    b.nackedTotal,
		Failures:       len(b.failures),
		Consumers:      make([]ConsumerStats, 0, len(b.subs)),
		Producers:      make([]ProducerStat, 0, len(b.producers)),
		Types:          make([]TypeCount, 0, len(b.byType)),
	}
	for _, c := range b.subs {
		out.Consumers = append(out.Consumers, b.statsLocked(c))
	}
	sort.Slice(out.Consumers, func(i, j int) bool {
		return out.Consumers[i].Name < out.Consumers[j].Name
	})

	for source, producer := range b.producers {
		if producer.count <= 0 {
			continue
		}
		if _, hidden := b.hidden[source]; hidden {
			continue
		}
		out.Producers = append(out.Producers, ProducerStat{
			Source: source,
			Count:  producer.count,
			LastAt: formatTime(producer.lastAt),
		})
	}
	sort.Slice(out.Producers, func(i, j int) bool {
		return out.Producers[i].Source < out.Producers[j].Source
	})

	for typ, seqs := range b.byType {
		if len(seqs) > 0 {
			out.Types = append(out.Types, TypeCount{Type: typ, Count: len(seqs)})
		}
	}
	sort.Slice(out.Types, func(i, j int) bool {
		if out.Types[i].Count != out.Types[j].Count {
			return out.Types[i].Count > out.Types[j].Count
		}
		return out.Types[i].Type < out.Types[j].Type
	})
	return out
}

// Watch 返回一个在总线状态变化时收到通知的通道,以及取消函数。
// 通道容量为 1,多次变化会合并成一次唤醒。
func (b *Bus) Watch() (<-chan struct{}, func()) {
	b.mu.Lock()
	defer b.mu.Unlock()

	ch := make(chan struct{}, 1)
	b.watchers[ch] = struct{}{}

	unwatch := func() {
		b.mu.Lock()
		delete(b.watchers, ch)
		b.mu.Unlock()
	}
	return ch, unwatch
}

// Done 在总线关闭时关闭。
func (b *Bus) Done() <-chan struct{} { return b.done }

// Publish 追加一条事件,并唤醒所有订阅了该类型的消费者。
// 同一个 (source, id) 重复发布会被忽略,此时 Duplicate 为 true。
func (b *Bus) Publish(e Event) (PublishResult, error) {
	e.Normalize()
	if err := e.Validate(); err != nil {
		return PublishResult{}, err
	}

	// 总线持有自己的副本:json.RawMessage 是 []byte,不复制会与调用方共享底层数组。
	e.Data = bytes.Clone(e.Data)

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return PublishResult{}, ErrClosed
	}

	k := key{source: e.Source, id: e.ID}
	if seq, ok := b.seen[k]; ok && seq > b.base {
		return PublishResult{Duplicate: true, Matched: b.matchedLocked(e.Types)}, nil
	}

	now := time.Now()
	seq := b.base + int64(len(b.log)) + 1
	se := storedEvent{seq: seq, receivedAt: now, event: e}

	inserted := true
	if b.store != nil {
		var err error
		inserted, err = b.store.appendEvent(se)
		if err != nil {
			return PublishResult{}, err
		}
	}
	if !inserted {
		// 和已归档的事件重复:events 表的 UNIQUE(source, id) 挡住了。
		return PublishResult{Duplicate: true, Matched: b.matchedLocked(e.Types)}, nil
	}

	b.log = append(b.log, se)
	b.seen[k] = seq
	b.active++
	for _, t := range e.Types {
		b.byType[t] = append(b.byType[t], seq)
	}

	b.metricLocked(now).published++
	b.publishedTotal++
	b.producerLocked(e.Source, now)

	// 隐藏过的来源又发布了,说明它重新活跃,放回生产者列表。
	if _, ok := b.hidden[e.Source]; ok {
		delete(b.hidden, e.Source)
		if b.store != nil {
			_ = b.store.unhideProducer(e.Source)
		}
	}

	matched := 0
	for _, c := range b.subs {
		if !c.matches(e) {
			continue
		}
		matched++
		c.pending++
		select {
		case c.notify <- struct{}{}:
		default:
		}
	}

	b.notifyLocked()
	return PublishResult{Matched: matched}, nil
}

// EventRef 按 (source, id) 指向一条事件。事件 id 只在同一个 source 内唯一。
type EventRef struct {
	ID     string `json:"id"`
	Source string `json:"source"`
}

// DeleteEvents 把指定事件从活跃视图删除。底层日志保留 seq 以维持游标语义,
// 但事件不再出现在分页、统计和消费者投递中。
func (b *Bus) DeleteEvents(refs []EventRef) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return 0, ErrClosed
	}
	seqs := make(map[int64]struct{}, len(refs))
	for _, ref := range refs {
		seq, ok := b.seen[key{source: ref.Source, id: ref.ID}]
		if !ok || seq <= b.base || seq > b.base+int64(len(b.log)) {
			continue
		}
		if b.log[seq-b.base-1].deleted {
			continue
		}
		seqs[seq] = struct{}{}
	}
	return b.deleteSeqsLocked(seqs)
}

// DeleteProducers 删除指定来源的全部活跃事件。
func (b *Bus) DeleteProducers(sources []string) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return 0, ErrClosed
	}
	wanted := make(map[string]struct{}, len(sources))
	for _, source := range sources {
		if source != "" {
			wanted[source] = struct{}{}
		}
	}
	seqs := make(map[int64]struct{})
	for i := range b.log {
		se := b.log[i]
		if se.deleted {
			continue
		}
		if _, ok := wanted[se.event.Source]; ok {
			seqs[se.seq] = struct{}{}
		}
	}
	return b.deleteSeqsLocked(seqs)
}

// HideProducers 把来源从生产者列表里移除,但保留它们的事件。
// 返回确实被隐藏的数量;之后某个来源再次发布事件,它会自动回到列表里。
func (b *Bus) HideProducers(sources []string) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return 0, ErrClosed
	}

	hidden := 0
	var firstErr error
	for _, source := range sources {
		if source == "" {
			continue
		}
		if _, ok := b.hidden[source]; ok {
			continue
		}
		if p := b.producers[source]; p == nil || p.count <= 0 {
			continue
		}
		if b.store != nil {
			if err := b.store.hideProducer(source); err != nil {
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
		}
		b.hidden[source] = struct{}{}
		hidden++
	}
	if hidden > 0 {
		b.notifyLocked()
	}
	return hidden, firstErr
}

// DeleteConsumers 删除多个消费者注册。返回成功删除的数量。
func (b *Bus) DeleteConsumers(names []string) (int, error) {
	deleted := 0
	var firstErr error
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		c, ok := b.Consumer(name)
		if !ok {
			continue
		}
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		deleted++
	}
	return deleted, firstErr
}

func (b *Bus) deleteSeqsLocked(seqs map[int64]struct{}) (int, error) {
	if len(seqs) == 0 {
		return 0, nil
	}
	ordered := make([]int64, 0, len(seqs))
	for seq := range seqs {
		ordered = append(ordered, seq)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })

	if b.store != nil {
		if err := b.store.deleteEvents(ordered); err != nil {
			return 0, err
		}
	}

	for _, seq := range ordered {
		b.log[seq-b.base-1].deleted = true
		if b.active > 0 {
			b.active--
		}
	}
	for _, c := range b.subs {
		if _, ok := seqs[c.inflight]; ok {
			c.inflight = 0
		}
		c.pending = b.countPendingLocked(c)
		select {
		case c.notify <- struct{}{}:
		default:
		}
	}
	for typ, seqsByType := range b.byType {
		kept := seqsByType[:0]
		for _, seq := range seqsByType {
			if _, deleted := seqs[seq]; !deleted {
				kept = append(kept, seq)
			}
		}
		if len(kept) == 0 {
			delete(b.byType, typ)
		} else {
			b.byType[typ] = kept
		}
	}
	b.rebuildProducersLocked()
	b.notifyLocked()
	return len(ordered), nil
}

func (b *Bus) rebuildProducersLocked() {
	b.producers = make(map[string]*producerState)
	for _, se := range b.log {
		if se.deleted {
			continue
		}
		b.producerLocked(se.event.Source, se.receivedAt)
	}
}

// Subscribe 注册一个按精确类型订阅的消费者。
func (b *Bus) Subscribe(name string, types ...EventType) (*Consumer, error) {
	return b.SubscribePatterns(name, types, nil)
}

// SubscribePatterns 注册一个消费者。types 是精确类型,patterns 是 Go RE2
// 正则表达式,匹配事件的任意一个类型;两类条件取并集。
func (b *Bus) SubscribePatterns(name string, types []EventType, patterns []string) (*Consumer, error) {
	if name == "" {
		return nil, errors.New("bus: consumer needs a name")
	}

	match := make(map[EventType]struct{}, len(types))
	for _, t := range types {
		if t == "" {
			return nil, errors.New("bus: empty event type")
		}
		match[t] = struct{}{}
	}

	normalizedPatterns, compiled, err := compilePatterns(patterns)
	if err != nil {
		return nil, err
	}
	if len(match) == 0 && len(normalizedPatterns) == 0 {
		return nil, errors.New("bus: consumer needs at least one type or pattern")
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return nil, ErrClosed
	}
	if _, ok := b.subs[name]; ok {
		return nil, fmt.Errorf("bus: consumer %q already exists", name)
	}

	c := &Consumer{
		bus:      b,
		name:     name,
		types:    match,
		patterns: normalizedPatterns,
		compiled: compiled,
		notify:   make(chan struct{}, 1),
		done:     make(chan struct{}),
	}
	c.pending = b.countPendingLocked(c)
	b.subs[name] = c
	if err := b.saveConsumerLocked(c); err != nil {
		delete(b.subs, name)
		c.closed = true
		close(c.done)
		return nil, err
	}
	b.notifyLocked()
	return c, nil
}

// archiveBatch 是触发归档的最小积压条数。关闭时会把剩余的也归档。
var archiveBatch int64 = 256

// SetArchiveBatch 设置归档批量;小于等于 0 表示禁用归档。
// 必须在创建总线之前调用。
func SetArchiveBatch(n int64) { archiveBatch = n }

// watermarkLocked 返回所有消费者游标的最小值。没有消费者时返回 0。
// 它是内部值,只用于决定哪些事件可以归档。
func (b *Bus) watermarkLocked() int64 {
	if len(b.subs) == 0 {
		return 0
	}

	last := b.base + int64(len(b.log))
	min := last
	for _, c := range b.subs {
		if c.cursor < min {
			min = c.cursor
		}
	}
	if min > last {
		return last
	}
	return min
}

// maybeArchiveLocked 在积压超过阈值时把水位以下的事件归档。
// 失败不影响 ack——下次确认时会重试。
func (b *Bus) maybeArchiveLocked() {
	if b.store == nil || len(b.subs) == 0 || archiveBatch <= 0 {
		return
	}
	upto := b.watermarkLocked()
	if upto-b.base < archiveBatch {
		return
	}
	_ = b.archiveLocked(upto)
}

// archiveLocked 把 seq <= upto 的事件标记为已归档,并从活跃日志里移除。
func (b *Bus) archiveLocked(upto int64) error {
	if b.store == nil || upto <= b.base {
		return nil
	}
	if err := b.store.archiveEvents(upto); err != nil {
		return err
	}

	n := int(upto - b.base)
	if n > len(b.log) {
		n = len(b.log)
	}
	for i := 0; i < n; i++ {
		if b.log[i].deleted {
			continue
		}
		b.active--
		src := b.log[i].event.Source
		if p := b.producers[src]; p != nil {
			p.count--
			if p.count <= 0 {
				delete(b.producers, src)
			}
		}
	}
	b.base = upto
	b.log = b.log[n:]
	// 底层数组可能一直变大;空闲容量过多时复制一份释放。
	if cap(b.log) > 2*len(b.log)+1024 {
		b.log = append([]storedEvent(nil), b.log...)
	}

	for typ, seqs := range b.byType {
		i := sort.Search(len(seqs), func(i int) bool { return seqs[i] > upto })
		if i > 0 {
			b.byType[typ] = append([]int64(nil), seqs[i:]...)
		}
		if len(b.byType[typ]) == 0 {
			delete(b.byType, typ)
		}
	}

	// seen 里已归档的键可以丢掉;超过阈值时重建,避免无限增长。
	if len(b.seen) > 2*len(b.log)+1024 {
		fresh := make(map[key]int64, len(b.log))
		for _, se := range b.log {
			fresh[key{source: se.event.Source, id: se.event.ID}] = se.seq
		}
		b.seen = fresh
	}
	return nil
}

// Consumer 按名字返回已注册的消费者。
func (b *Bus) Consumer(name string) (*Consumer, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	c, ok := b.subs[name]
	return c, ok
}

// Recent 返回最近 limit 条事件,按发生顺序排列。limit <= 0 时返回全部。
func (b *Bus) Recent(limit int) []Event {
	if limit <= 0 {
		limit = 1 << 30
	}
	events, _ := b.Page(limit, 0, "")
	return events
}

// Page 返回一页事件(按发生顺序)以及过滤后的总数。
// offset 从最新一页开始:0 是最新的一页,limit 是这一页的条数。
// typ 为空表示不过滤。
func (b *Bus) Page(limit, offset int, typ EventType) ([]Event, int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.pageLocked(limit, offset, typ)
}

func (b *Bus) pageLocked(limit, offset int, typ EventType) ([]Event, int) {
	if limit < 0 {
		limit = 0
	}
	if offset < 0 {
		offset = 0
	}

	if typ == "" {
		total := b.active
		end := total - offset
		if end < 0 {
			end = 0
		}
		start := end - limit
		if start < 0 {
			start = 0
		}

		out := make([]Event, 0, end-start)
		seen := 0
		for _, se := range b.log {
			if se.deleted {
				continue
			}
			seen++
			if seen > start && seen <= end {
				e := se.event
				e.Data = bytes.Clone(e.Data)
				out = append(out, e)
			}
			if seen >= end {
				break
			}
		}
		return out, total
	}

	seqs := b.byType[typ]
	total := len(seqs)
	end := total - offset
	if end < 0 {
		end = 0
	}
	start := end - limit
	if start < 0 {
		start = 0
	}

	out := make([]Event, 0, end-start)
	for _, seq := range seqs[start:end] {
		e := b.log[seq-b.base-1].event
		e.Data = bytes.Clone(e.Data)
		out = append(out, e)
	}
	return out, total
}

// Stats 返回所有消费者的进度,按名字排序。
func (b *Bus) Stats() []ConsumerStats {
	b.mu.Lock()
	defer b.mu.Unlock()

	out := make([]ConsumerStats, 0, len(b.subs))
	for _, c := range b.subs {
		out = append(out, b.statsLocked(c))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Failures 返回被显式放弃事件的只读副本。
func (b *Bus) Failures() []Failure {
	b.mu.Lock()
	defer b.mu.Unlock()

	out := make([]Failure, len(b.failures))
	copy(out, b.failures)
	return out
}

// FailuresPage 返回一页失败记录(按发生顺序)以及总数。
// offset 从最新一页开始:0 是最新的一页。
func (b *Bus) FailuresPage(limit, offset int) ([]Failure, int) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if limit < 0 {
		limit = 0
	}
	if offset < 0 {
		offset = 0
	}

	total := len(b.failures)
	end := total - offset
	if end < 0 {
		end = 0
	}
	start := end - limit
	if start < 0 {
		start = 0
	}

	out := make([]Failure, end-start)
	copy(out, b.failures[start:end])
	return out, total
}

// Reset 把消费者的游标和统计清零,让它重新读取全部历史。
func (b *Bus) Reset(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	c, ok := b.subs[name]
	if !ok {
		return fmt.Errorf("bus: unknown consumer %q", name)
	}
	c.cursor = 0
	c.inflight = 0
	c.delivered = 0
	c.failed = 0
	c.lastErr = ""
	c.lastEvent = ""
	c.lastAt = time.Time{}
	c.pending = b.countPendingLocked(c)

	select {
	case c.notify <- struct{}{}:
	default:
	}
	b.notifyLocked()
	return b.saveConsumerLocked(c)
}

// Close 关闭总线,并唤醒所有阻塞中的 Next。
func (b *Bus) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return nil
	}
	b.closed = true
	for _, c := range b.subs {
		if !c.closed {
			c.closed = true
			close(c.done)
		}
	}
	close(b.done)

	if b.store != nil {
		// 关闭时把水位以下剩余的也归档,避免下次启动又全量载入。
		if archiveBatch > 0 && len(b.subs) > 0 {
			if upto := b.watermarkLocked(); upto > b.base {
				_ = b.archiveLocked(upto)
			}
		}
		return b.store.close()
	}
	return nil
}

// ---- 内部 ----

func (c *Consumer) matches(e Event) bool {
	return c.matchesTypes(e.Types)
}

func (c *Consumer) matchesTypes(types []EventType) bool {
	for _, t := range types {
		if _, ok := c.types[t]; ok {
			return true
		}
	}
	for _, pattern := range c.compiled {
		for _, t := range types {
			if pattern.MatchString(string(t)) {
				return true
			}
		}
	}
	return false
}

const (
	maxSubscriptionPatterns   = 64
	maxSubscriptionPatternLen = 512
)

// NormalizePatterns trims, validates, de-duplicates and sorts subscription
// patterns. Patterns are Go RE2 regular expressions and match any one event
// type when applied with MatchString (add ^/$ for a full match).
func NormalizePatterns(patterns []string) ([]string, error) {
	if len(patterns) > maxSubscriptionPatterns {
		return nil, fmt.Errorf("bus: too many subscription patterns (max %d)", maxSubscriptionPatterns)
	}

	seen := make(map[string]struct{}, len(patterns))
	out := make([]string, 0, len(patterns))
	for _, raw := range patterns {
		pattern := strings.TrimSpace(raw)
		if pattern == "" {
			return nil, errors.New("bus: empty subscription pattern")
		}
		if len(pattern) > maxSubscriptionPatternLen {
			return nil, fmt.Errorf("bus: subscription pattern is too long (max %d bytes)", maxSubscriptionPatternLen)
		}
		if _, err := regexp.Compile(pattern); err != nil {
			return nil, fmt.Errorf("bus: invalid subscription pattern %q: %w", pattern, err)
		}
		if _, ok := seen[pattern]; ok {
			continue
		}
		seen[pattern] = struct{}{}
		out = append(out, pattern)
	}
	sort.Strings(out)
	return out, nil
}

func compilePatterns(patterns []string) ([]string, []*regexp.Regexp, error) {
	normalized, err := NormalizePatterns(patterns)
	if err != nil {
		return nil, nil, err
	}
	compiled := make([]*regexp.Regexp, 0, len(normalized))
	for _, pattern := range normalized {
		re, err := regexp.Compile(pattern)
		if err != nil {
			return nil, nil, fmt.Errorf("bus: invalid subscription pattern %q: %w", pattern, err)
		}
		compiled = append(compiled, re)
	}
	return normalized, compiled, nil
}

// metricLocked 返回当前秒的吞吐量桶。
func (b *Bus) metricLocked(now time.Time) *metricBucket {
	ts := now.Unix()
	bkt := b.metrics[ts]
	if bkt == nil {
		bkt = &metricBucket{}
		b.metrics[ts] = bkt
	}
	return bkt
}

// producerLocked 记录一次发布来源。
func (b *Bus) producerLocked(source string, at time.Time) {
	p := b.producers[source]
	if p == nil {
		p = &producerState{}
		b.producers[source] = p
	}
	p.count++
	if at.After(p.lastAt) {
		p.lastAt = at
	}
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339Nano)
}

func parseTime(raw string) time.Time {
	if raw == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}
	}
	return t
}

// countPendingLocked 从头数一遍消费者尚未确认、且类型匹配的事件。
// 只在注册、重置和启动时调用。
func (b *Bus) countPendingLocked(c *Consumer) int {
	n := 0
	for _, se := range b.log {
		if !se.deleted && se.seq > c.cursor && c.matches(se.event) {
			n++
		}
	}
	return n
}

// tryNext 返回 nil, nil 表示暂时没有事件,调用方应该等待。
func (c *Consumer) tryNext() (*Message, error) {
	b := c.bus
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed || c.closed {
		return nil, ErrClosed
	}

	if c.inflight != 0 {
		return c.messageLocked(c.inflight), nil
	}

	// seq == base + index + 1;cursor 可能落在归档区间里。
	start := c.cursor - b.base
	if start < 0 {
		start = 0
	}

	for i := start; i < int64(len(b.log)); i++ {
		se := b.log[i]
		if se.deleted || !c.matches(se.event) {
			continue
		}
		c.inflight = se.seq
		return c.messageLocked(se.seq), nil
	}

	// 扫描到底没有任何匹配:全部是别的类型,可以安全越过,避免反复扫描。
	last := b.base + int64(len(b.log))
	if last > c.cursor {
		c.cursor = last
	}
	return nil, nil
}

func (c *Consumer) messageLocked(seq int64) *Message {
	e := c.bus.log[seq-c.bus.base-1].event
	e.Data = bytes.Clone(e.Data)
	return &Message{Event: e, seq: seq, c: c}
}

// settle 处理一次确认。failure 为 nil 表示成功,否则记录并放弃。
func (c *Consumer) settle(seq int64, failure error) error {
	b := c.bus
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed || c.closed {
		return ErrClosed
	}
	if seq == 0 || c.inflight != seq {
		// 没有 inflight、重复或过期的确认,忽略。
		return nil
	}
	if seq <= b.base || seq > b.base+int64(len(b.log)) {
		// 这条已经被归档,理论上不会发生(归档只在水位之下)。
		return nil
	}

	e := b.log[seq-b.base-1].event
	c.inflight = 0
	c.cursor = seq
	c.lastEvent = e.ID
	if c.pending > 0 {
		c.pending--
	}
	now := time.Now()
	c.lastAt = now

	if failure == nil {
		c.delivered++
		b.metricLocked(now).acked++
		b.ackedTotal++
		b.notifyLocked()
		err := b.saveConsumerLocked(c)
		b.maybeArchiveLocked()
		return err
	}

	c.failed++
	c.lastErr = failure.Error()
	c.lastAt = now
	b.metricLocked(now).failed++
	b.nackedTotal++
	f := Failure{
		Consumer: c.name,
		EventID:  e.ID,
		Source:   e.Source,
		Error:    failure.Error(),
		At:       now,
	}
	b.failures = append(b.failures, f)
	b.notifyLocked()

	var err error
	if b.store != nil {
		err = b.store.appendFailure(f)
	}
	if saveErr := b.saveConsumerLocked(c); saveErr != nil && err == nil {
		err = saveErr
	}
	b.maybeArchiveLocked()
	return err
}

func (b *Bus) statsLocked(c *Consumer) ConsumerStats {
	return ConsumerStats{
		Name:      c.name,
		Types:     sortedTypes(c.types),
		Patterns:  append([]string(nil), c.patterns...),
		Cursor:    c.cursor,
		Pending:   c.pending,
		Delivered: c.delivered,
		Failed:    c.failed,
		LastError: c.lastErr,
		LastEvent: c.lastEvent,
		LastAt:    formatTime(c.lastAt),
	}
}

// pendingLocked 只统计该消费者关心、且尚未确认的事件。
// 消费者不订阅的类型不计入,否则多个消费者累加后会超过事件总数。
func sortedTypes(m map[EventType]struct{}) []EventType {
	out := make([]EventType, 0, len(m))
	for t := range m {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func (b *Bus) matchedLocked(types []EventType) int {
	n := 0
	for _, c := range b.subs {
		if c.matchesTypes(types) {
			n++
		}
	}
	return n
}

// notifyLocked 唤醒所有 Watch 通道。调用方必须持有 b.mu。
func (b *Bus) notifyLocked() {
	for ch := range b.watchers {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// saveConsumerLocked 把单个消费者的游标与统计写入存储。调用方必须持有 b.mu。
func (b *Bus) saveConsumerLocked(c *Consumer) error {
	if b.store == nil {
		return nil
	}
	return b.store.saveConsumer(ConsumerState{
		Name:      c.name,
		Types:     sortedTypes(c.types),
		Patterns:  append([]string(nil), c.patterns...),
		Cursor:    c.cursor,
		Delivered: c.delivered,
		Failed:    c.failed,
		LastError: c.lastErr,
		LastEvent: c.lastEvent,
		LastAt:    formatTime(c.lastAt),
	})
}
