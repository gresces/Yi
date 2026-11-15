package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"yi/src/bus"
)

func TestPrometheusMetricsEndpoint(t *testing.T) {
	b := bus.New()
	defer b.Close()
	if _, err := b.Subscribe("worker", bus.TypeRawCaptured); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publish(bus.Event{
		ID:     "e1",
		Source: "phone",
		Types:  []bus.EventType{bus.TypeRawCaptured},
		Time:   time.Now(),
		Data:   []byte(`{"text":"x"}`),
	}); err != nil {
		t.Fatal(err)
	}

	srv := httptest.NewServer(serviceHandler(b, newAuth("")))
	defer srv.Close()

	resp, err := srv.Client().Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Type"); !strings.HasPrefix(got, "text/plain; version=0.0.4") {
		t.Fatalf("content type = %q", got)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	body := string(raw)
	for _, want := range []string{
		"# TYPE yi_up gauge",
		"yi_up 1\n",
		"yi_events_active 1\n",
		"yi_events_published_total 1\n",
		"yi_events_acked_total 0\n",
		"yi_events_nacked_total 0\n",
		"yi_consumers 1\n",
		"yi_consumer_pending{consumer=\"worker\"} 1\n",
		"yi_producer_events{source=\"phone\"} 1\n",
		"yi_events_by_type{type=\"raw.captured\"} 1\n",
		"yi_build_info{version=",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics body does not contain %q:\n%s", want, body)
		}
	}
}

func TestMetricsEndpointRequiresAuth(t *testing.T) {
	b := bus.New()
	defer b.Close()
	srv := httptest.NewServer(serviceHandler(b, newAuth("test-token-0123456789")))
	defer srv.Close()

	resp, err := srv.Client().Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("anonymous metrics status = %d", resp.StatusCode)
	}

	req, err := http.NewRequest(http.MethodGet, srv.URL+"/metrics", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer test-token-0123456789")
	resp, err = srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("authenticated metrics status = %d", resp.StatusCode)
	}
}

func TestPrometheusLabelEscaping(t *testing.T) {
	if got := escapePrometheusLabel("a\"b\\c\nd"); got != `a\"b\\c\nd` {
		t.Fatalf("escaped label = %q", got)
	}
}
