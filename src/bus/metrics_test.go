package bus

import (
	"errors"
	"testing"
)

func TestPrometheusStats(t *testing.T) {
	b := New()
	defer b.Close()

	c := mustSubscribe(t, b, "c", TypeRawCaptured)
	mustPublish(t, b, event("e1"))
	mustPublish(t, b, event("e2"))
	if err := mustNext(t, c).Ack(); err != nil {
		t.Fatal(err)
	}
	if err := mustNext(t, c).Nack(errors.New("bad")); err != nil {
		t.Fatal(err)
	}
	mustPublish(t, b, event("e3"))

	stats := b.PrometheusStats()
	if stats.Active != 3 {
		t.Fatalf("active = %d, want 3", stats.Active)
	}
	if stats.PublishedTotal != 3 || stats.AckedTotal != 1 || stats.NackedTotal != 1 {
		t.Fatalf("counters = published %d acked %d nacked %d",
			stats.PublishedTotal, stats.AckedTotal, stats.NackedTotal)
	}
	if stats.Failures != 1 {
		t.Fatalf("failures = %d, want 1", stats.Failures)
	}
	if len(stats.Consumers) != 1 || stats.Consumers[0].Name != "c" ||
		stats.Consumers[0].Pending != 1 {
		t.Fatalf("consumers = %+v", stats.Consumers)
	}
	if len(stats.Producers) != 1 || stats.Producers[0].Count != 3 {
		t.Fatalf("producers = %+v", stats.Producers)
	}
	if len(stats.Types) != 1 || stats.Types[0].Type != TypeRawCaptured ||
		stats.Types[0].Count != 3 {
		t.Fatalf("types = %+v", stats.Types)
	}

	// Soft-deleted events leave the active gauge but do not rewrite process
	// lifetime counters or the failure log.
	if _, err := b.DeleteEvents([]EventRef{{ID: "e1", Source: "test"}}); err != nil {
		t.Fatal(err)
	}
	stats = b.PrometheusStats()
	if stats.Active != 2 {
		t.Fatalf("active after delete = %d, want 2", stats.Active)
	}
	if stats.PublishedTotal != 3 || stats.AckedTotal != 1 || stats.NackedTotal != 1 {
		t.Fatalf("counters changed after delete: %+v", stats)
	}
}
