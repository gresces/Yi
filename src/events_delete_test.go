package main

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"yi/src/bus"
)

func publishDeleteTestEvent(t *testing.T, b *bus.Bus, id, source string, types ...bus.EventType) {
	t.Helper()
	if len(types) == 0 {
		types = []bus.EventType{bus.TypeRawCaptured}
	}
	if _, err := b.Publish(bus.Event{
		ID:     id,
		Source: source,
		Types:  types,
		Time:   time.Now(),
		Data:   []byte(`{"text":"x"}`),
	}); err != nil {
		t.Fatal(err)
	}
}

func TestFrontendDeleteEvents(t *testing.T) {
	b, srv := frontendAuthTestServer(t, "")
	publishDeleteTestEvent(t, b, "e1", "phone")
	publishDeleteTestEvent(t, b, "e2", "phone")
	publishDeleteTestEvent(t, b, "e3", "laptop")

	code, body := do(t, srv, http.MethodPost, "/api/events/delete", map[string]any{
		"events": []bus.EventRef{{ID: "e2", Source: "phone"}},
	})
	if code != http.StatusOK {
		t.Fatalf("delete: %d %s", code, body)
	}
	var result struct {
		Deleted int `json:"deleted"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatal(err)
	}
	if result.Deleted != 1 {
		t.Fatalf("deleted = %d", result.Deleted)
	}

	code, body = do(t, srv, http.MethodGet, "/api/events?limit=0", nil)
	if code != http.StatusOK {
		t.Fatalf("events: %d %s", code, body)
	}
	var page struct {
		Events []bus.Event `json:"events"`
		Total  int         `json:"total"`
	}
	if err := json.Unmarshal(body, &page); err != nil {
		t.Fatal(err)
	}
	if page.Total != 2 || len(page.Events) != 2 {
		t.Fatalf("unexpected page: %+v", page)
	}
	for _, event := range page.Events {
		if event.ID == "e2" {
			t.Fatal("deleted event still in page")
		}
	}

	// Verify persistence of the deletion through a real store in the bus test;
	// here verify idempotence and batch validation.
	code, body = do(t, srv, http.MethodPost, "/api/events/delete", map[string]any{
		"events": []bus.EventRef{{ID: "e2", Source: "phone"}},
	})
	if code != http.StatusOK {
		t.Fatalf("delete again: %d %s", code, body)
	}
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatal(err)
	}
	if result.Deleted != 0 {
		t.Fatalf("second deleted = %d", result.Deleted)
	}

	code, body = do(t, srv, http.MethodPost, "/api/events/delete", map[string]any{
		"events": []bus.EventRef{},
	})
	if code != http.StatusBadRequest {
		t.Fatalf("empty delete: %d %s", code, body)
	}
}

func TestFrontendDeleteProducersAndConsumers(t *testing.T) {
	b, srv := frontendAuthTestServer(t, "")
	publishDeleteTestEvent(t, b, "e1", "phone")
	publishDeleteTestEvent(t, b, "e2", "laptop")
	if _, err := b.Subscribe("c1", bus.TypeRawCaptured); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Subscribe("c2", bus.TypeRawCaptured); err != nil {
		t.Fatal(err)
	}

	code, body := do(t, srv, http.MethodPost, "/api/producers/delete", map[string]any{
		"sources": []string{"phone"},
	})
	if code != http.StatusOK {
		t.Fatalf("delete producer: %d %s", code, body)
	}
	if b.Len() != 1 {
		t.Fatalf("len = %d, want 1", b.Len())
	}
	for _, producer := range b.Snapshot(10).Producers {
		if producer.Source == "phone" {
			t.Fatal("phone producer still present")
		}
	}

	code, body = do(t, srv, http.MethodPost, "/api/consumers/delete", map[string]any{
		"names": []string{"c1", "c2"},
	})
	if code != http.StatusOK {
		t.Fatalf("delete consumers: %d %s", code, body)
	}
	if _, ok := b.Consumer("c1"); ok {
		t.Fatal("c1 still registered")
	}
	if _, ok := b.Consumer("c2"); ok {
		t.Fatal("c2 still registered")
	}
	if len(b.Stats()) != 0 {
		t.Fatalf("stats = %+v", b.Stats())
	}
}
