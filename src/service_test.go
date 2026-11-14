package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"yi/src/bus"
)

func newTestServer(t *testing.T) (*bus.Bus, *httptest.Server) {
	t.Helper()

	b := bus.New()
	srv := httptest.NewServer(serviceHandler(b))
	t.Cleanup(func() {
		srv.Close()
		_ = b.Close()
	})
	return b, srv
}

func do(t *testing.T, srv *httptest.Server, method, path string, body any) (int, []byte) {
	t.Helper()

	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(raw)
	}

	req, err := http.NewRequest(method, srv.URL+path, reader)
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	out, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, out
}

func register(t *testing.T, srv *httptest.Server, name string, types ...string) {
	t.Helper()
	code, body := do(t, srv, http.MethodPut, "/consumers/"+name, map[string]any{"types": types})
	if code != http.StatusCreated {
		t.Fatalf("register %s: %d %s", name, code, body)
	}
}

func publishRaw(t *testing.T, srv *httptest.Server, id string) {
	t.Helper()
	code, body := do(t, srv, http.MethodPost, "/events", map[string]any{
		"id":     id,
		"source": "test",
		"types":  []string{"raw.captured"},
		"time":   "2026-11-14T10:00:00+08:00",
		"data":   map[string]string{"text": "hello"},
	})
	if code != http.StatusCreated {
		t.Fatalf("publish: %d %s", code, body)
	}
}

func nextEvent(t *testing.T, srv *httptest.Server, name string) (int, bus.Event) {
	t.Helper()
	code, body := do(t, srv, http.MethodGet, "/consumers/"+name+"/next?timeout=1", nil)
	var resp struct {
		Event bus.Event `json:"event"`
	}
	if code == http.StatusOK {
		if err := json.Unmarshal(body, &resp); err != nil {
			t.Fatalf("decode next: %v (%s)", err, body)
		}
	}
	return code, resp.Event
}

func TestConsumerRegisterIsIdempotent(t *testing.T) {
	_, srv := newTestServer(t)

	code, body := do(t, srv, http.MethodPut, "/consumers/c", map[string]any{"types": []string{"raw.captured"}})
	if code != http.StatusCreated {
		t.Fatalf("create: %d %s", code, body)
	}

	code, body = do(t, srv, http.MethodPut, "/consumers/c", map[string]any{"types": []string{"raw.captured"}})
	if code != http.StatusOK {
		t.Fatalf("repeat: %d %s", code, body)
	}

	code, body = do(t, srv, http.MethodPut, "/consumers/c", map[string]any{"types": []string{"other.type"}})
	if code != http.StatusConflict {
		t.Fatalf("conflict: %d %s", code, body)
	}

	code, body = do(t, srv, http.MethodGet, "/consumers", nil)
	if code != http.StatusOK {
		t.Fatalf("list: %d %s", code, body)
	}
	var stats []bus.ConsumerStats
	if err := json.Unmarshal(body, &stats); err != nil {
		t.Fatal(err)
	}
	if len(stats) != 1 || stats[0].Name != "c" || len(stats[0].Types) != 1 {
		t.Fatalf("unexpected stats: %s", body)
	}

	if code, _ = do(t, srv, http.MethodDelete, "/consumers/c", nil); code != http.StatusNoContent {
		t.Fatalf("delete: %d", code)
	}
	if code, _ = do(t, srv, http.MethodDelete, "/consumers/c", nil); code != http.StatusNotFound {
		t.Fatalf("second delete: %d", code)
	}
}

func TestConsumerPullAck(t *testing.T) {
	_, srv := newTestServer(t)
	register(t, srv, "c", "raw.captured")
	publishRaw(t, srv, "e1")

	code, ev := nextEvent(t, srv, "c")
	if code != http.StatusOK || ev.ID != "e1" {
		t.Fatalf("next: %d %+v", code, ev)
	}

	// 未确认之前,再取还是同一条。
	code, again := nextEvent(t, srv, "c")
	if code != http.StatusOK || again.ID != "e1" {
		t.Fatalf("retry: %d %+v", code, again)
	}

	if code, body := do(t, srv, http.MethodPost, "/consumers/c/ack", map[string]any{"id": "wrong"}); code != http.StatusConflict {
		t.Fatalf("ack wrong id: %d %s", code, body)
	}

	if code, body := do(t, srv, http.MethodPost, "/consumers/c/ack", map[string]any{"id": "e1"}); code != http.StatusNoContent {
		t.Fatalf("ack: %d %s", code, body)
	}

	code, body := do(t, srv, http.MethodGet, "/consumers/c/next?timeout=0.1", nil)
	if code != http.StatusNoContent {
		t.Fatalf("next after ack: %d %s", code, body)
	}
}

func TestConsumerNackRecordsFailure(t *testing.T) {
	_, srv := newTestServer(t)
	register(t, srv, "c", "raw.captured")
	publishRaw(t, srv, "e1")

	code, ev := nextEvent(t, srv, "c")
	if code != http.StatusOK {
		t.Fatalf("next: %d", code)
	}

	if code, body := do(t, srv, http.MethodPost, "/consumers/c/nack", map[string]any{
		"id": ev.ID, "error": "boom",
	}); code != http.StatusNoContent {
		t.Fatalf("nack: %d %s", code, body)
	}

	code, body := do(t, srv, http.MethodGet, "/failures", nil)
	if code != http.StatusOK {
		t.Fatalf("failures: %d", code)
	}
	var failures []bus.Failure
	if err := json.Unmarshal(body, &failures); err != nil {
		t.Fatal(err)
	}
	if len(failures) != 1 || failures[0].EventID != "e1" || failures[0].Error != "boom" {
		t.Fatalf("unexpected failures: %s", body)
	}

	if code, _ := do(t, srv, http.MethodGet, "/consumers/c/next?timeout=0.1", nil); code != http.StatusNoContent {
		t.Fatalf("nack did not advance: %d", code)
	}
}

func TestConsumerNextTimesOut(t *testing.T) {
	_, srv := newTestServer(t)
	register(t, srv, "c", "raw.captured")

	code, body := do(t, srv, http.MethodGet, "/consumers/c/next?timeout=0.1", nil)
	if code != http.StatusNoContent {
		t.Fatalf("got %d %s, want 204", code, body)
	}
}

func TestUnknownConsumer(t *testing.T) {
	_, srv := newTestServer(t)

	for _, tc := range []struct{ method, path string }{
		{http.MethodGet, "/consumers/ghost/next?timeout=0.1"},
		{http.MethodPost, "/consumers/ghost/ack"},
		{http.MethodPost, "/consumers/ghost/nack"},
		{http.MethodDelete, "/consumers/ghost"},
	} {
		if code, _ := do(t, srv, tc.method, tc.path, nil); code != http.StatusNotFound {
			t.Errorf("%s %s: got %d, want 404", tc.method, tc.path, code)
		}
	}
}

func TestPublishMatchedCount(t *testing.T) {
	_, srv := newTestServer(t)

	code, body := do(t, srv, http.MethodPost, "/events", map[string]any{
		"id": "e1", "source": "test", "types": []string{"raw.captured"},
		"time": "2026-11-14T10:00:00+08:00", "data": map[string]string{"text": "x"},
	})
	if code != http.StatusCreated {
		t.Fatalf("publish: %d %s", code, body)
	}
	var res bus.PublishResult
	if err := json.Unmarshal(body, &res); err != nil {
		t.Fatal(err)
	}
	if res.Matched != 0 {
		t.Fatalf("matched = %d, want 0", res.Matched)
	}

	register(t, srv, "c", "raw.captured")

	code, body = do(t, srv, http.MethodPost, "/events", map[string]any{
		"id": "e2", "source": "test", "types": []string{"raw.captured"},
		"time": "2026-11-14T10:00:00+08:00", "data": map[string]string{"text": "x"},
	})
	if code != http.StatusCreated {
		t.Fatalf("publish: %d %s", code, body)
	}
	if err := json.Unmarshal(body, &res); err != nil {
		t.Fatal(err)
	}
	if res.Matched != 1 {
		t.Fatalf("matched = %d, want 1", res.Matched)
	}
}
