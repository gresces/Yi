package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"yi/src/bus"
)

func TestEventsPagination(t *testing.T) {
	b := bus.New()
	defer b.Close()

	for i := 1; i <= 3; i++ {
		if _, err := b.Publish(bus.Event{
			ID:     fmt.Sprintf("e%d", i),
			Source: "test",
			Types:  []bus.EventType{bus.TypeRawCaptured},
			Time:   time.Now(),
			Data:   []byte(`{"text":"x"}`),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := b.Publish(bus.Event{
		ID:     "m1",
		Source: "test",
		Types:  []bus.EventType{"mail.received"},
		Time:   time.Now(),
		Data:   []byte(`{"subject":"x"}`),
	}); err != nil {
		t.Fatal(err)
	}

	srv := httptest.NewServer(frontendHandler(b, newAuth("")))
	defer srv.Close()

	type page struct {
		Events []bus.Event `json:"events"`
		Total  int         `json:"total"`
		Offset int         `json:"offset"`
		Limit  int         `json:"limit"`
	}
	get := func(query string) page {
		t.Helper()
		resp, err := srv.Client().Get(srv.URL + "/api/events" + query)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d", resp.StatusCode)
		}
		var p page
		if err := json.NewDecoder(resp.Body).Decode(&p); err != nil {
			t.Fatal(err)
		}
		return p
	}

	p := get("?limit=2&offset=1")
	if p.Total != 4 || p.Offset != 1 || p.Limit != 2 || len(p.Events) != 2 {
		t.Fatalf("unexpected page: %+v", p)
	}
	if p.Events[0].ID != "e2" || p.Events[1].ID != "e3" {
		t.Fatalf("unexpected order: %+v", p.Events)
	}

	filtered := get("?type=mail.received")
	if filtered.Total != 1 || len(filtered.Events) != 1 || filtered.Events[0].ID != "m1" {
		t.Fatalf("unexpected filtered page: %+v", filtered)
	}
}

func TestFailuresPagination(t *testing.T) {
	b := bus.New()
	defer b.Close()

	c, err := b.Subscribe("c", bus.TypeRawCaptured)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := b.Publish(bus.Event{
			ID:     fmt.Sprintf("e%d", i),
			Source: "test",
			Types:  []bus.EventType{bus.TypeRawCaptured},
			Time:   time.Now(),
			Data:   []byte(`{}`),
		}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 2; i++ {
		m, err := c.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if err := m.Nack(fmt.Errorf("bad-%d", i)); err != nil {
			t.Fatal(err)
		}
	}

	srv := httptest.NewServer(frontendHandler(b, newAuth("")))
	defer srv.Close()

	resp, err := srv.Client().Get(srv.URL + "/api/failures?limit=1&offset=0")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var page struct {
		Failures []bus.Failure `json:"failures"`
		Total    int           `json:"total"`
		Offset   int           `json:"offset"`
		Limit    int           `json:"limit"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
		t.Fatal(err)
	}
	if page.Total != 2 || page.Offset != 0 || page.Limit != 1 || len(page.Failures) != 1 {
		t.Fatalf("unexpected page: %+v", page)
	}
	if page.Failures[0].Error != "bad-1" {
		t.Fatalf("unexpected newest failure: %+v", page.Failures[0])
	}
}

func TestStreamPushesSnapshots(t *testing.T) {
	b := bus.New()
	defer b.Close()

	srv := httptest.NewServer(frontendHandler(b, newAuth("")))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/api/stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("content-type = %q", ct)
	}

	scanner := bufio.NewScanner(resp.Body)
	readSnapshot := func() bus.Snapshot {
		t.Helper()
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "data: ") {
				var s bus.Snapshot
				if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &s); err != nil {
					t.Fatalf("decode snapshot: %v", err)
				}
				return s
			}
		}
		t.Fatal("stream ended without a snapshot")
		return bus.Snapshot{}
	}

	if s := readSnapshot(); s.Total != 0 || len(s.Events) != 0 {
		t.Fatalf("unexpected initial snapshot: %+v", s)
	}

	if _, err := b.Publish(bus.Event{
		ID:     "e1",
		Source: "test",
		Types:  []bus.EventType{bus.TypeRawCaptured},
		Time:   time.Now(),
		Data:   []byte(`{"text":"hello"}`),
	}); err != nil {
		t.Fatal(err)
	}

	s := readSnapshot()
	if s.Total != 1 || len(s.Events) != 1 || s.Events[0].ID != "e1" {
		t.Fatalf("unexpected snapshot after publish: %+v", s)
	}
}

func TestMetricsEndpoint(t *testing.T) {
	b := bus.New()
	defer b.Close()

	c, err := b.Subscribe("c", bus.TypeRawCaptured)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err := b.Publish(bus.Event{
			ID:     fmt.Sprintf("e%d", i),
			Source: "test",
			Types:  []bus.EventType{bus.TypeRawCaptured},
			Time:   time.Now(),
			Data:   []byte(`{}`),
		}); err != nil {
			t.Fatal(err)
		}
	}
	m, err := c.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Ack(); err != nil {
		t.Fatal(err)
	}

	srv := httptest.NewServer(frontendHandler(b, newAuth("")))
	defer srv.Close()

	resp, err := srv.Client().Get(srv.URL + "/api/metrics?window=10")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var metrics bus.Metrics
	if err := json.NewDecoder(resp.Body).Decode(&metrics); err != nil {
		t.Fatal(err)
	}
	if metrics.Window != 10 || len(metrics.Published) != 10 || len(metrics.Acked) != 10 {
		t.Fatalf("unexpected metrics shape: %+v", metrics)
	}
	published, acked := 0, 0
	for i := range metrics.Published {
		published += metrics.Published[i]
		acked += metrics.Acked[i]
	}
	if published != 2 || acked != 1 {
		t.Fatalf("published=%d acked=%d, want 2/1", published, acked)
	}
}

func readSnapshotTotal(t *testing.T, sc *bufio.Scanner) int {
	t.Helper()
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "data: ") {
			var s bus.Snapshot
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &s); err != nil {
				t.Fatalf("decode snapshot: %v", err)
			}
			return s.Total
		}
	}
	t.Fatal("stream ended without a snapshot")
	return 0
}

func TestStreamMultipleClients(t *testing.T) {
	b := bus.New()
	defer b.Close()

	srv := httptest.NewServer(frontendHandler(b, newAuth("")))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const clients = 3
	readers := make([]*bufio.Scanner, 0, clients)
	for i := 0; i < clients; i++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/api/stream", nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		readers = append(readers, bufio.NewScanner(resp.Body))
	}

	for i, sc := range readers {
		if total := readSnapshotTotal(t, sc); total != 0 {
			t.Fatalf("client %d initial total = %d, want 0", i, total)
		}
	}

	if _, err := b.Publish(bus.Event{
		ID:     "e1",
		Source: "test",
		Types:  []bus.EventType{bus.TypeRawCaptured},
		Time:   time.Now(),
		Data:   []byte(`{"text":"x"}`),
	}); err != nil {
		t.Fatal(err)
	}

	for i, sc := range readers {
		if total := readSnapshotTotal(t, sc); total != 1 {
			t.Fatalf("client %d total = %d, want 1", i, total)
		}
	}
}
