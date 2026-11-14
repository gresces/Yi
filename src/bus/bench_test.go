package bus

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

func benchEvent(i int) Event {
	return Event{
		ID:     fmt.Sprintf("bench-%d", i),
		Source: "bench",
		Types:  []EventType{TypeRawCaptured},
		Time:   time.Now(),
		Data:   json.RawMessage(`{"text":"benchmark payload"}`),
	}
}

func BenchmarkPublishMemory(b *testing.B) {
	bus := New()
	defer bus.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := bus.Publish(benchEvent(i)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPublishWithConsumer(b *testing.B) {
	bus := New()
	defer bus.Close()
	if _, err := bus.Subscribe("c", TypeRawCaptured); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := bus.Publish(benchEvent(i)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPublishSQLite(b *testing.B) {
	bus, err := Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer bus.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := bus.Publish(benchEvent(i)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAckMemory(b *testing.B) {
	bus := New()
	defer bus.Close()
	c, err := bus.Subscribe("c", TypeRawCaptured)
	if err != nil {
		b.Fatal(err)
	}

	b.StopTimer()
	for i := 0; i < b.N; i++ {
		if _, err := bus.Publish(benchEvent(i)); err != nil {
			b.Fatal(err)
		}
	}
	ctx := context.Background()
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		m, err := c.Next(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if err := m.Ack(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPage10k(b *testing.B) {
	bus := New()
	defer bus.Close()
	for i := 0; i < 10000; i++ {
		if _, err := bus.Publish(benchEvent(i)); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bus.Page(50, (i*37)%9000, "")
	}
}

func BenchmarkSnapshot10k(b *testing.B) {
	bus := New()
	defer bus.Close()
	for _, name := range []string{"a", "b", "c", "d"} {
		if _, err := bus.Subscribe(name, TypeRawCaptured); err != nil {
			b.Fatal(err)
		}
	}
	for i := 0; i < 10000; i++ {
		if _, err := bus.Publish(benchEvent(i)); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bus.Snapshot(50)
	}
}

func BenchmarkMetrics(b *testing.B) {
	bus := New()
	defer bus.Close()
	for i := 0; i < 600; i++ {
		bus.metricLocked(time.Now().Add(-time.Duration(i)*time.Second)).published++
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bus.Metrics(120)
	}
}
