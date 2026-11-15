package main

import (
	"encoding/json"
	"net/http"
	"testing"

	"yi/src/bus"
)

func TestFrontendHideProducers(t *testing.T) {
	b, srv := frontendAuthTestServer(t, "")
	publishDeleteTestEvent(t, b, "h1", "phone")
	publishDeleteTestEvent(t, b, "h2", "phone")
	publishDeleteTestEvent(t, b, "h3", "laptop")

	code, body := do(t, srv, http.MethodPost, "/api/producers/hide", map[string]any{
		"sources": []string{"phone"},
	})
	if code != http.StatusOK {
		t.Fatalf("hide: %d %s", code, body)
	}
	var result struct {
		Hidden int `json:"hidden"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatal(err)
	}
	if result.Hidden != 1 {
		t.Fatalf("hidden = %d, want 1", result.Hidden)
	}

	// 事件还在。
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
	if page.Total != 3 || len(page.Events) != 3 {
		t.Fatalf("events total=%d len=%d, want 3", page.Total, len(page.Events))
	}

	// 生产者列表里只剩 laptop。
	snapshot := b.Snapshot(10)
	if len(snapshot.Producers) != 1 || snapshot.Producers[0].Source != "laptop" {
		t.Fatalf("producers = %+v", snapshot.Producers)
	}

	code, body = do(t, srv, http.MethodPost, "/api/producers/hide", map[string]any{
		"sources": []string{},
	})
	if code != http.StatusBadRequest {
		t.Fatalf("empty sources: %d %s", code, body)
	}
}
