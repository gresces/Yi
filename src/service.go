package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"yi/src/bus"
)

// serviceHandler 是机器接口:JSON 进,JSON 出。
func serviceHandler(b *bus.Bus) http.Handler {
	mux := http.NewServeMux()

	// ---- 事件 ----

	mux.HandleFunc("POST /events", func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

		var e bus.Event
		if err := json.NewDecoder(r.Body).Decode(&e); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
			return
		}

		res, err := b.Publish(e)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}

		status := http.StatusCreated
		if res.Duplicate {
			status = http.StatusOK
		}
		writeJSON(w, status, res)
	})

	mux.HandleFunc("GET /events", func(w http.ResponseWriter, r *http.Request) {
		limit := queryInt(r, "limit", 50)
		if limit <= 0 {
			limit = 1 << 30
		}
		events, _ := b.Page(limit, queryInt(r, "offset", 0), "")
		writeJSON(w, http.StatusOK, events)
	})

	// ---- 消费者 ----

	mux.HandleFunc("GET /consumers", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, b.Stats())
	})

	mux.HandleFunc("PUT /consumers/{name}", func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")

		var body struct {
			Types []bus.EventType `json:"types"`
		}
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
			return
		}
		if len(body.Types) == 0 {
			writeErr(w, http.StatusBadRequest, "types is required")
			return
		}

		if c, ok := b.Consumer(name); ok {
			if sameTypes(c.Types(), body.Types) {
				writeJSON(w, http.StatusOK, c.Stats())
				return
			}
			writeErr(w, http.StatusConflict, "consumer exists with different types")
			return
		}

		c, err := b.Subscribe(name, body.Types...)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		writeJSON(w, http.StatusCreated, c.Stats())
	})

	mux.HandleFunc("DELETE /consumers/{name}", func(w http.ResponseWriter, r *http.Request) {
		c, ok := b.Consumer(r.PathValue("name"))
		if !ok {
			writeErr(w, http.StatusNotFound, "unknown consumer")
			return
		}
		_ = c.Close()
		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc("GET /consumers/{name}/next", func(w http.ResponseWriter, r *http.Request) {
		c, ok := b.Consumer(r.PathValue("name"))
		if !ok {
			writeErr(w, http.StatusNotFound, "unknown consumer")
			return
		}

		timeout := queryFloat(r, "timeout", 25)
		if timeout <= 0 {
			timeout = 25
		}
		if timeout > 60 {
			timeout = 60
		}

		ctx, cancel := context.WithTimeout(r.Context(), time.Duration(timeout*float64(time.Second)))
		defer cancel()

		m, err := c.Next(ctx)
		switch {
		case err == nil:
			writeJSON(w, http.StatusOK, map[string]any{"event": m.Event})
		case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
			w.WriteHeader(http.StatusNoContent)
		case errors.Is(err, bus.ErrClosed):
			writeErr(w, http.StatusNotFound, "consumer closed")
		default:
			writeErr(w, http.StatusInternalServerError, err.Error())
		}
	})

	mux.HandleFunc("POST /consumers/{name}/ack", func(w http.ResponseWriter, r *http.Request) {
		settle(w, r, b, false)
	})

	mux.HandleFunc("POST /consumers/{name}/nack", func(w http.ResponseWriter, r *http.Request) {
		settle(w, r, b, true)
	})

	// ---- 诊断 ----

	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, b.Stats())
	})

	mux.HandleFunc("GET /failures", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, b.Failures())
	})

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	})

	return mux
}

// settle 处理 ack / nack。body 可选,但带上 id 会校验它确实是当前 inflight 的事件。
func settle(w http.ResponseWriter, r *http.Request, b *bus.Bus, nack bool) {
	c, ok := b.Consumer(r.PathValue("name"))
	if !ok {
		writeErr(w, http.StatusNotFound, "unknown consumer")
		return
	}

	ev, ok := c.Inflight()
	if !ok {
		writeErr(w, http.StatusConflict, "no inflight message")
		return
	}

	var body struct {
		ID    string `json:"id"`
		Error string `json:"error"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<16)
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil && !errors.Is(err, io.EOF) {
		writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if body.ID != "" && body.ID != ev.ID {
		writeErr(w, http.StatusConflict, "id does not match inflight message")
		return
	}

	if nack {
		if body.Error == "" {
			body.Error = "nacked"
		}
		_ = c.Nack(errors.New(body.Error))
	} else {
		_ = c.Ack()
	}
	w.WriteHeader(http.StatusNoContent)
}

func sameTypes(a, b []bus.EventType) bool {
	if len(a) != len(b) {
		return false
	}
	set := make(map[bus.EventType]struct{}, len(a))
	for _, t := range a {
		set[t] = struct{}{}
	}
	for _, t := range b {
		if _, ok := set[t]; !ok {
			return false
		}
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func queryInt(r *http.Request, name string, def int) int {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return def
	}
	return n
}

func queryFloat(r *http.Request, name string, def float64) float64 {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return def
	}
	n, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return def
	}
	return n
}
