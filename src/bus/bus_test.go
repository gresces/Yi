package bus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"
)

const typeOther EventType = "test.other"

func eventOf(id string, typ EventType) Event {
	return Event{
		ID:     id,
		Source: "test",
		Types:  []EventType{typ},
		Time:   time.Now(),
		Data:   json.RawMessage(`{"text":"hello"}`),
	}
}

func event(id string) Event { return eventOf(id, TypeRawCaptured) }

func mustSubscribe(t *testing.T, b *Bus, name string, types ...EventType) *Consumer {
	t.Helper()
	c, err := b.Subscribe(name, types...)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func mustPublish(t *testing.T, b *Bus, e Event) PublishResult {
	t.Helper()
	res, err := b.Publish(e)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func mustNext(t *testing.T, c *Consumer) *Message {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	m, err := c.Next(ctx)
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	return m
}

func TestPullAckInOrder(t *testing.T) {
	b := New()
	defer b.Close()
	c := mustSubscribe(t, b, "c", TypeRawCaptured)
	defer c.Close()

	for i := 0; i < 3; i++ {
		if res := mustPublish(t, b, event(fmt.Sprintf("e%d", i))); res.Matched != 1 {
			t.Fatalf("matched = %d, want 1", res.Matched)
		}
	}

	for i := 0; i < 3; i++ {
		m := mustNext(t, c)
		if want := fmt.Sprintf("e%d", i); m.Event.ID != want {
			t.Fatalf("got %s, want %s", m.Event.ID, want)
		}
		if err := m.Ack(); err != nil {
			t.Fatal(err)
		}
	}

	if got := c.Stats(); got.Delivered != 3 || got.Pending != 0 {
		t.Fatalf("stats = %+v", got)
	}
}

func TestHistoryIsDelivered(t *testing.T) {
	b := New()
	defer b.Close()

	mustPublish(t, b, event("before"))

	c := mustSubscribe(t, b, "c", TypeRawCaptured)
	defer c.Close()

	if m := mustNext(t, c); m.Event.ID != "before" {
		t.Fatalf("got %s, want before", m.Event.ID)
	}
}

func TestRetryWithoutAck(t *testing.T) {
	b := New()
	defer b.Close()
	c := mustSubscribe(t, b, "c", TypeRawCaptured)
	defer c.Close()

	mustPublish(t, b, event("a"))
	mustPublish(t, b, event("b"))

	first := mustNext(t, c)
	second := mustNext(t, c)
	if first.Event.ID != second.Event.ID {
		t.Fatalf("unacked Next returned %s then %s", first.Event.ID, second.Event.ID)
	}
	if err := second.Ack(); err != nil {
		t.Fatal(err)
	}

	if m := mustNext(t, c); m.Event.ID != "b" {
		t.Fatalf("got %s, want b", m.Event.ID)
	}
}

func TestNackSkipsAndRecords(t *testing.T) {
	b := New()
	defer b.Close()
	c := mustSubscribe(t, b, "c", TypeRawCaptured)
	defer c.Close()

	mustPublish(t, b, event("a"))
	mustPublish(t, b, event("b"))

	m := mustNext(t, c)
	if err := m.Nack(errors.New("boom")); err != nil {
		t.Fatal(err)
	}

	if next := mustNext(t, c); next.Event.ID != "b" {
		t.Fatalf("got %s, want b", next.Event.ID)
	}

	f := b.Failures()
	if len(f) != 1 || f[0].EventID != "a" || f[0].Consumer != "c" {
		t.Fatalf("unexpected failures: %+v", f)
	}
}

func TestNoConsumerStillStored(t *testing.T) {
	b := New()
	defer b.Close()

	res := mustPublish(t, b, event("lonely"))
	if res.Matched != 0 {
		t.Fatalf("matched = %d, want 0", res.Matched)
	}
	if got := b.Recent(0); len(got) != 1 {
		t.Fatalf("event not stored: %+v", got)
	}

	// 之后注册的消费者仍能读到它。
	c := mustSubscribe(t, b, "late", TypeRawCaptured)
	defer c.Close()
	if m := mustNext(t, c); m.Event.ID != "lonely" {
		t.Fatalf("got %s, want lonely", m.Event.ID)
	}
}

func TestNextBlocksUntilPublish(t *testing.T) {
	b := New()
	defer b.Close()
	c := mustSubscribe(t, b, "c", TypeRawCaptured)
	defer c.Close()

	go func() {
		time.Sleep(50 * time.Millisecond)
		_, _ = b.Publish(event("late"))
	}()

	if m := mustNext(t, c); m.Event.ID != "late" {
		t.Fatalf("got %s, want late", m.Event.ID)
	}
}

func TestCloseUnblocksNext(t *testing.T) {
	b := New()
	defer b.Close()
	c := mustSubscribe(t, b, "c", TypeRawCaptured)

	go func() {
		time.Sleep(50 * time.Millisecond)
		_ = c.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if _, err := c.Next(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("got %v, want ErrClosed", err)
	}
}

func TestDuplicatePublish(t *testing.T) {
	b := New()
	defer b.Close()
	c := mustSubscribe(t, b, "c", TypeRawCaptured)
	defer c.Close()

	if res := mustPublish(t, b, event("same")); res.Duplicate {
		t.Fatal("first publish marked duplicate")
	}
	if res := mustPublish(t, b, event("same")); !res.Duplicate {
		t.Fatal("second publish not marked duplicate")
	}
	if got := b.Recent(0); len(got) != 1 {
		t.Fatalf("log has %d events, want 1", len(got))
	}

	if m := mustNext(t, c); m.Event.ID != "same" {
		t.Fatalf("got %s", m.Event.ID)
	}
}

func TestResetReplays(t *testing.T) {
	b := New()
	defer b.Close()
	c := mustSubscribe(t, b, "c", TypeRawCaptured)
	defer c.Close()

	mustPublish(t, b, event("a"))
	m := mustNext(t, c)
	if err := m.Ack(); err != nil {
		t.Fatal(err)
	}

	if err := b.Reset("c"); err != nil {
		t.Fatal(err)
	}
	if got := c.Stats(); got.Delivered != 0 || got.Cursor != 0 {
		t.Fatalf("stats not reset: %+v", got)
	}
	if m := mustNext(t, c); m.Event.ID != "a" {
		t.Fatalf("got %s, want a", m.Event.ID)
	}
}

func TestNonMatchingSkipped(t *testing.T) {
	b := New()
	defer b.Close()
	c := mustSubscribe(t, b, "c", TypeRawCaptured)
	defer c.Close()

	if res := mustPublish(t, b, eventOf("other", typeOther)); res.Matched != 0 {
		t.Fatalf("matched = %d, want 0", res.Matched)
	}
	mustPublish(t, b, event("raw"))

	if m := mustNext(t, c); m.Event.ID != "raw" {
		t.Fatalf("got %s, want raw", m.Event.ID)
	}
}

func TestStats(t *testing.T) {
	b := New()
	defer b.Close()
	c := mustSubscribe(t, b, "c", TypeRawCaptured)
	defer c.Close()

	mustPublish(t, b, event("a"))
	mustPublish(t, b, event("b"))

	m := mustNext(t, c)
	if m.Event.ID != "a" {
		t.Fatal("unexpected order")
	}
	if err := m.Ack(); err != nil {
		t.Fatal(err)
	}

	stats := b.Stats()
	if len(stats) != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
	got := stats[0]
	if got.Name != "c" || got.Delivered != 1 || got.Failed != 0 || got.Cursor != 1 || got.Pending != 1 {
		t.Fatalf("unexpected stats: %+v", got)
	}
}

func TestDuplicateNameRejected(t *testing.T) {
	b := New()
	defer b.Close()

	mustSubscribe(t, b, "c", TypeRawCaptured)
	if _, err := b.Subscribe("c", TypeRawCaptured); err == nil {
		t.Fatal("expected duplicate name to fail")
	}
}

func TestValidate(t *testing.T) {
	cases := map[string]Event{
		"empty id":     {Source: "s", Types: []EventType{TypeRawCaptured}, Time: time.Now(), Data: json.RawMessage(`{}`)},
		"empty source": {ID: "i", Types: []EventType{TypeRawCaptured}, Time: time.Now(), Data: json.RawMessage(`{}`)},
		"zero time":    {ID: "i", Source: "s", Types: []EventType{TypeRawCaptured}, Data: json.RawMessage(`{}`)},
		"array data":   {ID: "i", Source: "s", Types: []EventType{TypeRawCaptured}, Time: time.Now(), Data: json.RawMessage(`[]`)},
		"invalid json": {ID: "i", Source: "s", Types: []EventType{TypeRawCaptured}, Time: time.Now(), Data: json.RawMessage(`{`)},
	}
	for name, e := range cases {
		if err := e.Validate(); !errors.Is(err, ErrInvalidEvent) {
			t.Errorf("%s: got %v, want ErrInvalidEvent", name, err)
		}
	}
}

func TestPage(t *testing.T) {
	b := New()
	defer b.Close()

	for i := 1; i <= 5; i++ {
		mustPublish(t, b, event(fmt.Sprintf("e%d", i)))
	}

	cases := []struct {
		limit, offset int
		want          []string
	}{
		{2, 0, []string{"e4", "e5"}},
		{2, 2, []string{"e2", "e3"}},
		{2, 4, []string{"e1"}},
		{2, 10, nil},
	}
	for _, tc := range cases {
		got, total := b.Page(tc.limit, tc.offset, "")
		if total != 5 {
			t.Fatalf("Page(%d,%d) total = %d, want 5", tc.limit, tc.offset, total)
		}
		if len(got) != len(tc.want) {
			t.Fatalf("Page(%d,%d) len = %d, want %d", tc.limit, tc.offset, len(got), len(tc.want))
		}
		for i, id := range tc.want {
			if got[i].ID != id {
				t.Fatalf("Page(%d,%d)[%d] = %s, want %s", tc.limit, tc.offset, i, got[i].ID, id)
			}
		}
	}

	mustPublish(t, b, eventOf("o1", typeOther))
	mustPublish(t, b, eventOf("o2", typeOther))

	got, total := b.Page(10, 0, typeOther)
	if total != 2 || len(got) != 2 || got[0].ID != "o1" || got[1].ID != "o2" {
		t.Fatalf("filtered page = %+v total %d", got, total)
	}
}

func TestFailuresPage(t *testing.T) {
	b := New()
	defer b.Close()

	c := mustSubscribe(t, b, "c", TypeRawCaptured)
	for i := 1; i <= 3; i++ {
		mustPublish(t, b, event(fmt.Sprintf("e%d", i)))
	}
	if err := mustNext(t, c).Nack(errors.New("bad-1")); err != nil {
		t.Fatal(err)
	}
	if err := mustNext(t, c).Nack(errors.New("bad-2")); err != nil {
		t.Fatal(err)
	}

	got, total := b.FailuresPage(1, 0)
	if total != 2 || len(got) != 1 || got[0].Error != "bad-2" {
		t.Fatalf("newest page: %+v total %d", got, total)
	}

	got, total = b.FailuresPage(5, 0)
	if total != 2 || len(got) != 2 || got[0].Error != "bad-1" || got[1].Error != "bad-2" {
		t.Fatalf("all: %+v total %d", got, total)
	}

	got, total = b.FailuresPage(1, 2)
	if total != 2 || len(got) != 0 {
		t.Fatalf("past end: %+v total %d", got, total)
	}
}

func TestRecent(t *testing.T) {
	b := New()
	defer b.Close()

	for _, id := range []string{"a", "b", "c"} {
		mustPublish(t, b, event(id))
	}

	all := b.Recent(0)
	if len(all) != 3 || all[0].ID != "a" || all[2].ID != "c" {
		t.Fatalf("unexpected events: %+v", all)
	}
	last := b.Recent(1)
	if len(last) != 1 || last[0].ID != "c" {
		t.Fatalf("unexpected tail: %+v", last)
	}
}

func sumInts(values []int) int {
	total := 0
	for _, v := range values {
		total += v
	}
	return total
}

func TestMultiTypeMatchingAndPending(t *testing.T) {
	b := New()
	defer b.Close()

	both := mustSubscribe(t, b, "both", TypeRawCaptured, typeOther)
	raw := mustSubscribe(t, b, "raw", TypeRawCaptured)

	e := eventOf("multi", typeOther)
	e.Types = []EventType{TypeRawCaptured, typeOther}
	res := mustPublish(t, b, e)
	if res.Matched != 2 {
		t.Fatalf("matched = %d, want 2", res.Matched)
	}

	// 一条事件同时命中两个类型,但每个消费者只应计一次。
	if got := both.Stats().Pending; got != 1 {
		t.Fatalf("both pending = %d, want 1", got)
	}
	if got := raw.Stats().Pending; got != 1 {
		t.Fatalf("raw pending = %d, want 1", got)
	}

	m := mustNext(t, both)
	if !m.Event.HasType(TypeRawCaptured) || !m.Event.HasType(typeOther) {
		t.Fatalf("unexpected types: %+v", m.Event.Types)
	}
	if err := m.Ack(); err != nil {
		t.Fatal(err)
	}
	if got := both.Stats().Pending; got != 0 {
		t.Fatalf("both pending after ack = %d, want 0", got)
	}
	if got := raw.Stats().Pending; got != 1 {
		t.Fatalf("raw pending after other ack = %d, want 1", got)
	}
}

func TestRegexSubscriptionMatching(t *testing.T) {
	b := New()
	defer b.Close()

	c, err := b.SubscribePatterns("regex", nil, []string{`^mail\.`, `^note$`, `^note$`})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if got := c.Patterns(); len(got) != 2 || got[0] != "^mail\\." || got[1] != "^note$" {
		t.Fatalf("patterns = %#v", got)
	}

	mail := eventOf("mail", "mail.received")
	note := eventOf("note", "note")
	other := eventOf("other", TypeRawCaptured)
	for _, e := range []Event{mail, note, other} {
		res := mustPublish(t, b, e)
		want := 0
		if e.Types[0] == "mail.received" || e.Types[0] == "note" {
			want = 1
		}
		if res.Matched != want {
			t.Fatalf("%s matched = %d, want %d", e.ID, res.Matched, want)
		}
	}

	if got := c.Stats().Pending; got != 2 {
		t.Fatalf("pending = %d, want 2", got)
	}
	if got := c.Stats().Patterns; len(got) != 2 {
		t.Fatalf("stats patterns = %#v", got)
	}

	for _, want := range []string{"mail", "note"} {
		m := mustNext(t, c)
		if m.Event.ID != want {
			t.Fatalf("next = %s, want %s", m.Event.ID, want)
		}
		if err := m.Ack(); err != nil {
			t.Fatal(err)
		}
	}
	if got := c.Stats().Pending; got != 0 {
		t.Fatalf("pending after ack = %d", got)
	}
}

func TestRegexAndExactSubscriptionUnion(t *testing.T) {
	b := New()
	defer b.Close()

	c, err := b.SubscribePatterns("mixed", []EventType{TypeRawCaptured}, []string{`^note$`})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	multi := event("multi")
	multi.Types = []EventType{TypeRawCaptured, "note"}
	if res := mustPublish(t, b, multi); res.Matched != 1 {
		t.Fatalf("matched = %d, want 1", res.Matched)
	}
	if got := c.Stats().Pending; got != 1 {
		t.Fatalf("multi-type event must count once, pending = %d", got)
	}
	mustNext(t, c).Ack()

	if res := mustPublish(t, b, eventOf("note", "note")); res.Matched != 1 {
		t.Fatalf("pattern match = %d, want 1", res.Matched)
	}
	mustNext(t, c).Ack()

	if res := mustPublish(t, b, eventOf("other", "other.type")); res.Matched != 0 {
		t.Fatalf("non-match = %d, want 0", res.Matched)
	}
}

func TestInvalidRegexSubscription(t *testing.T) {
	b := New()
	defer b.Close()

	for _, patterns := range [][]string{
		{`(`},
		{`   `},
		{""},
	} {
		if _, err := b.SubscribePatterns("bad", nil, patterns); err == nil {
			t.Fatalf("patterns %#v should be rejected", patterns)
		}
	}
	if _, err := b.SubscribePatterns("empty", nil, nil); err == nil {
		t.Fatal("empty subscription should be rejected")
	}
}

func TestMetrics(t *testing.T) {
	b := New()
	defer b.Close()

	c := mustSubscribe(t, b, "c", TypeRawCaptured)
	mustPublish(t, b, event("a"))
	mustPublish(t, b, event("b"))
	if err := mustNext(t, c).Ack(); err != nil {
		t.Fatal(err)
	}
	if err := mustNext(t, c).Nack(errors.New("bad")); err != nil {
		t.Fatal(err)
	}

	m := b.Metrics(60)
	if len(m.Published) != 60 || len(m.Acked) != 60 || len(m.Failed) != 60 {
		t.Fatalf("unexpected series lengths: %d/%d/%d", len(m.Published), len(m.Acked), len(m.Failed))
	}
	if got := sumInts(m.Published); got != 2 {
		t.Fatalf("published sum = %d, want 2", got)
	}
	if got := sumInts(m.Acked); got != 1 {
		t.Fatalf("acked sum = %d, want 1", got)
	}
	if got := sumInts(m.Failed); got != 1 {
		t.Fatalf("failed sum = %d, want 1", got)
	}
	if m.Pending != 0 {
		t.Fatalf("pending = %d, want 0", m.Pending)
	}
}

func TestProducers(t *testing.T) {
	b := New()
	defer b.Close()

	mustPublish(t, b, event("a"))
	fromPhone := event("b")
	fromPhone.Source = "phone"
	mustPublish(t, b, fromPhone)

	snap := b.Snapshot(0)
	if len(snap.Producers) != 2 {
		t.Fatalf("producers = %+v", snap.Producers)
	}
	for _, p := range snap.Producers {
		if p.Count != 1 || p.LastAt == "" {
			t.Fatalf("unexpected producer: %+v", p)
		}
	}
	if snap.Producers[0].Source != "phone" {
		t.Fatalf("producers not sorted: %+v", snap.Producers)
	}
}

func TestSnapshotTypes(t *testing.T) {
	b := New()
	defer b.Close()

	mustPublish(t, b, eventOf("r", TypeRawCaptured))
	mustPublish(t, b, eventOf("o", typeOther))

	multi := eventOf("m", TypeRawCaptured)
	multi.Types = []EventType{TypeRawCaptured, typeOther}
	mustPublish(t, b, multi)

	snap := b.Snapshot(0)
	if len(snap.Types) != 2 {
		t.Fatalf("types = %+v", snap.Types)
	}
	got := map[EventType]int{}
	for _, tc := range snap.Types {
		got[tc.Type] = tc.Count
	}
	if got[TypeRawCaptured] != 2 || got[typeOther] != 2 {
		t.Fatalf("type counts = %+v", got)
	}
}
