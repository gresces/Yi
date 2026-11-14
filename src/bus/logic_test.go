package bus

import (
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
)

func TestDedupeConcurrent(t *testing.T) {
	b := New()
	defer b.Close()

	e := event("same")
	var inserted, duplicates atomic.Int64
	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := b.Publish(e)
			if err != nil {
				t.Error(err)
				return
			}
			if res.Duplicate {
				duplicates.Add(1)
			} else {
				inserted.Add(1)
			}
		}()
	}
	wg.Wait()

	if got := inserted.Load(); got != 1 {
		t.Fatalf("inserted = %d, want 1", got)
	}
	if got := duplicates.Load(); got != 49 {
		t.Fatalf("duplicates = %d, want 49", got)
	}
	if got := b.Len(); got != 1 {
		t.Fatalf("len = %d, want 1", got)
	}
}

func TestTypesNormalized(t *testing.T) {
	b := New()
	defer b.Close()

	e := event("x")
	e.Types = []EventType{" b ", "a", "a", ""}
	if _, err := b.Publish(e); err != nil {
		t.Fatal(err)
	}

	got := b.Recent(1)[0].Types
	want := []EventType{"a", "b"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("types = %v, want %v", got, want)
	}
}

func TestAckIdempotent(t *testing.T) {
	b := New()
	defer b.Close()
	c := mustSubscribe(t, b, "c", TypeRawCaptured)

	mustPublish(t, b, event("a"))
	m := mustNext(t, c)
	if err := m.Ack(); err != nil {
		t.Fatal(err)
	}
	if err := m.Ack(); err != nil {
		t.Fatal(err)
	}

	stats := c.Stats()
	if stats.Delivered != 1 || stats.Failed != 0 || stats.Pending != 0 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestStaleAckIgnored(t *testing.T) {
	b := New()
	defer b.Close()
	c := mustSubscribe(t, b, "c", TypeRawCaptured)

	mustPublish(t, b, event("a"))
	mustPublish(t, b, event("b"))

	first := mustNext(t, c)
	if err := first.Ack(); err != nil {
		t.Fatal(err)
	}

	second := mustNext(t, c)
	if err := first.Ack(); err != nil { // 过期确认,应被忽略
		t.Fatal(err)
	}
	if got := c.Stats().Delivered; got != 1 {
		t.Fatalf("delivered after stale ack = %d, want 1", got)
	}

	if err := second.Ack(); err != nil {
		t.Fatal(err)
	}
	if got := c.Stats().Delivered; got != 2 {
		t.Fatalf("delivered = %d, want 2", got)
	}
}

func TestMetricsWindowClamp(t *testing.T) {
	b := New()
	defer b.Close()

	if got := b.Metrics(0).Window; got != 120 {
		t.Fatalf("window(0) = %d, want 120", got)
	}
	if got := b.Metrics(1000).Window; got != 120 {
		t.Fatalf("window(1000) = %d, want 120", got)
	}
	if got := b.Metrics(30).Window; got != 30 {
		t.Fatalf("window(30) = %d, want 30", got)
	}
}

func TestPaginationBoundaries(t *testing.T) {
	b := New()
	defer b.Close()

	for i := 1; i <= 5; i++ {
		mustPublish(t, b, event(fmt.Sprintf("e%d", i)))
	}

	cases := []struct {
		limit, offset, want int
	}{
		{0, 0, 0},
		{-1, 0, 0},
		{10, 10, 0},
		{2, 4, 1},
	}
	for _, tc := range cases {
		got, total := b.Page(tc.limit, tc.offset, "")
		if total != 5 || len(got) != tc.want {
			t.Fatalf("Page(%d,%d) = len %d total %d, want len %d total 5",
				tc.limit, tc.offset, len(got), total, tc.want)
		}
	}
}

func TestMultiTypeFiltering(t *testing.T) {
	b := New()
	defer b.Close()

	e := eventOf("multi", TypeRawCaptured)
	e.Types = []EventType{TypeRawCaptured, typeOther}
	mustPublish(t, b, e)

	if got, total := b.Page(10, 0, TypeRawCaptured); total != 1 || len(got) != 1 {
		t.Fatalf("raw filter: %d/%d", len(got), total)
	}
	if got, total := b.Page(10, 0, typeOther); total != 1 || len(got) != 1 {
		t.Fatalf("other filter: %d/%d", len(got), total)
	}
	if got, total := b.Page(10, 0, "missing"); total != 0 || len(got) != 0 {
		t.Fatalf("missing filter: %d/%d", len(got), total)
	}
}
