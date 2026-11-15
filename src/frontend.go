package main

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"strings"
	"time"

	"yi/src/bus"
)

//go:embed web
var webFS embed.FS

const (
	pageSize    = 50
	maxPageSize = 200
)

// frontendHandler 提供静态页面和人看的 JSON 接口。
func frontendHandler(b *bus.Bus, a *auth) http.Handler {
	sub, err := fs.Sub(webFS, "web")
	if err != nil {
		panic(err)
	}

	mux := http.NewServeMux()

	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		page, err := fs.ReadFile(sub, "index.html")
		if err != nil {
			http.Error(w, "index.html missing", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		_, _ = w.Write(page)
	})

	mux.Handle("GET /static/", http.StripPrefix("/static/", http.FileServer(http.FS(sub))))

	// ---- 认证 ----

	mux.HandleFunc("POST /api/login", a.login)
	mux.HandleFunc("POST /api/logout", a.logout)
	mux.HandleFunc("GET /api/session", a.session)

	// ---- 页面用的 JSON ----

	mux.HandleFunc("GET /api/events", func(w http.ResponseWriter, r *http.Request) {
		limit := queryInt(r, "limit", pageSize)
		if limit <= 0 || limit > maxPageSize {
			limit = pageSize
		}
		offset := queryInt(r, "offset", 0)
		if offset < 0 {
			offset = 0
		}
		typ := bus.EventType(strings.TrimSpace(r.URL.Query().Get("type")))

		events, total := b.Page(limit, offset, typ)
		writeJSON(w, http.StatusOK, map[string]any{
			"events": events,
			"total":  total,
			"offset": offset,
			"limit":  limit,
		})
	})

	mux.HandleFunc("GET /api/failures", func(w http.ResponseWriter, r *http.Request) {
		limit := queryInt(r, "limit", pageSize)
		if limit <= 0 || limit > maxPageSize {
			limit = pageSize
		}
		offset := queryInt(r, "offset", 0)
		if offset < 0 {
			offset = 0
		}

		failures, total := b.FailuresPage(limit, offset)
		writeJSON(w, http.StatusOK, map[string]any{
			"failures": failures,
			"total":    total,
			"offset":   offset,
			"limit":    limit,
		})
	})

	mux.HandleFunc("POST /api/capture", func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

		var body struct {
			Text   string `json:"text"`
			Source string `json:"source"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
			return
		}

		text := strings.TrimSpace(body.Text)
		if text == "" {
			writeErr(w, http.StatusBadRequest, "text is required")
			return
		}
		source := strings.TrimSpace(body.Source)
		if source == "" {
			source = "web"
		}

		data, err := json.Marshal(map[string]string{"text": text})
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}

		e := bus.Event{
			ID:     newID(),
			Source: source,
			Types:  []bus.EventType{bus.TypeRawCaptured},
			Time:   time.Now(),
			Data:   data,
		}
		res, err := b.Publish(e)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}

		status := http.StatusCreated
		if res.Duplicate {
			status = http.StatusOK
		}
		writeJSON(w, status, map[string]any{
			"id":        e.ID,
			"duplicate": res.Duplicate,
			"matched":   res.Matched,
		})
	})

	// ---- 管理操作 ----

	const maxDeleteBatch = 1000

	mux.HandleFunc("POST /api/events/delete", func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		var body struct {
			Events []bus.EventRef `json:"events"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
			return
		}
		if len(body.Events) == 0 || len(body.Events) > maxDeleteBatch {
			writeErr(w, http.StatusBadRequest, "events must contain 1-1000 items")
			return
		}
		deleted, err := b.DeleteEvents(body.Events)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"deleted": deleted})
	})

	mux.HandleFunc("POST /api/producers/delete", func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		var body struct {
			Sources []string `json:"sources"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
			return
		}
		if len(body.Sources) == 0 || len(body.Sources) > maxDeleteBatch {
			writeErr(w, http.StatusBadRequest, "sources must contain 1-1000 items")
			return
		}
		deleted, err := b.DeleteProducers(body.Sources)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"deleted": deleted})
	})

	mux.HandleFunc("POST /api/producers/hide", func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		var body struct {
			Sources []string `json:"sources"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
			return
		}
		if len(body.Sources) == 0 || len(body.Sources) > maxDeleteBatch {
			writeErr(w, http.StatusBadRequest, "sources must contain 1-1000 items")
			return
		}
		hidden, err := b.HideProducers(body.Sources)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"hidden": hidden})
	})

	mux.HandleFunc("POST /api/consumers/delete", func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		var body struct {
			Names []string `json:"names"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
			return
		}
		if len(body.Names) == 0 || len(body.Names) > maxDeleteBatch {
			writeErr(w, http.StatusBadRequest, "names must contain 1-1000 items")
			return
		}
		deleted, err := b.DeleteConsumers(body.Names)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"deleted": deleted})
	})

	mux.HandleFunc("GET /api/stats", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"events":    b.Len(),
			"consumers": b.Stats(),
			"failures":  b.Failures(),
		})
	})

	mux.HandleFunc("GET /api/metrics", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, b.Metrics(queryInt(r, "window", 120)))
	})

	// SSE:连接建立时推一次快照,之后每次状态变化再推一次。
	mux.HandleFunc("GET /api/stream", func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			writeErr(w, http.StatusInternalServerError, "streaming unsupported")
			return
		}

		// 先订阅再取快照,避免两者之间漏掉一次变化。
		changes, unwatch := b.Watch()
		defer unwatch()

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("X-Accel-Buffering", "no")
		w.WriteHeader(http.StatusOK)

		send := func() bool {
			raw, err := json.Marshal(b.Snapshot(pageSize))
			if err != nil {
				return false
			}
			if _, err := fmt.Fprintf(w, "event: snapshot\ndata: %s\n\n", raw); err != nil {
				return false
			}
			flusher.Flush()
			return true
		}

		if !send() {
			return
		}

		ping := time.NewTicker(20 * time.Second)
		defer ping.Stop()

		for {
			select {
			case <-r.Context().Done():
				return
			case <-b.Done():
				return
			case <-changes:
				if !send() {
					return
				}
			case <-ping.C:
				if _, err := fmt.Fprint(w, ": ping\n\n"); err != nil {
					return
				}
				flusher.Flush()
			}
		}
	})

	return a.wrap(mux, authPublicFrontend, true)
}
