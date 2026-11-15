package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"yi/src/bus"
)

func authTestServer(t *testing.T, token string) (*bus.Bus, *httptest.Server) {
	t.Helper()

	b := bus.New()
	srv := httptest.NewServer(serviceHandler(b, newAuth(token)))
	t.Cleanup(func() {
		srv.Close()
		_ = b.Close()
	})
	return b, srv
}

func frontendAuthTestServer(t *testing.T, token string) (*bus.Bus, *httptest.Server) {
	t.Helper()

	b := bus.New()
	srv := httptest.NewServer(frontendHandler(b, newAuth(token)))
	t.Cleanup(func() {
		srv.Close()
		_ = b.Close()
	})
	return b, srv
}

func requestWithToken(t *testing.T, srv *httptest.Server, method, path, token string) *http.Response {
	t.Helper()

	req, err := http.NewRequest(method, srv.URL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestServiceAuth(t *testing.T) {
	_, srv := authTestServer(t, "test-token-0123456789")

	resp := requestWithToken(t, srv, http.MethodGet, "/healthz", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("health without auth: %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp = requestWithToken(t, srv, http.MethodGet, "/events?limit=1", "")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("events without auth: %d", resp.StatusCode)
	}
	if got := resp.Header.Get("WWW-Authenticate"); !strings.Contains(got, "Bearer") {
		t.Fatalf("WWW-Authenticate = %q", got)
	}
	resp.Body.Close()

	resp = requestWithToken(t, srv, http.MethodGet, "/events?limit=1", "wrong")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("events wrong token: %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp = requestWithToken(t, srv, http.MethodGet, "/events?limit=1", "test-token-0123456789")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("events with bearer: %d", resp.StatusCode)
	}
	resp.Body.Close()

	// Cookie auth is frontend-only; the machine API must not accept it.
	req, err := http.NewRequest(http.MethodGet, srv.URL+"/events?limit=1", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.AddCookie(&http.Cookie{Name: authCookieName, Value: "test-token-0123456789"})
	resp, err = srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("service cookie auth should be rejected: %d", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestServiceAuthDisabled(t *testing.T) {
	_, srv := authTestServer(t, "")

	resp := requestWithToken(t, srv, http.MethodGet, "/events?limit=1", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("events without auth on disabled server: %d", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestFrontendLoginFlow(t *testing.T) {
	_, srv := frontendAuthTestServer(t, "test-token-0123456789")

	resp, err := srv.Client().Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("index without auth: %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp, err = srv.Client().Get(srv.URL + "/static/app.js")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("static asset without auth: %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp, err = srv.Client().Get(srv.URL + "/api/stats")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("stats without auth: %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp, err = srv.Client().Get(srv.URL + "/api/session")
	if err != nil {
		t.Fatal(err)
	}
	var session struct {
		Authenticated bool `json:"authenticated"`
		AuthEnabled   bool `json:"auth_enabled"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&session); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if session.Authenticated || !session.AuthEnabled {
		t.Fatalf("unexpected anonymous session: %+v", session)
	}

	login := func(token string) *http.Response {
		t.Helper()
		body := strings.NewReader(`{"token":"` + token + `"}`)
		resp, err := srv.Client().Post(srv.URL+"/api/login", "application/json", body)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}

	resp = login("wrong")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("bad login: %d", resp.StatusCode)
	}
	resp.Body.Close()

	malformed := strings.NewReader(`{"token":`)
	resp, err = srv.Client().Post(srv.URL+"/api/login", "application/json", malformed)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("malformed login: %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp = login("test-token-0123456789")
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("login: %d", resp.StatusCode)
	}
	var sessionCookie *http.Cookie
	for _, cookie := range resp.Cookies() {
		if cookie.Name == authCookieName {
			sessionCookie = cookie
			break
		}
	}
	resp.Body.Close()
	if sessionCookie == nil {
		t.Fatal("login did not set a session cookie")
	}
	if sessionCookie.Value != "test-token-0123456789" || !sessionCookie.HttpOnly ||
		sessionCookie.SameSite != http.SameSiteStrictMode {
		t.Fatalf("unexpected cookie: %+v", sessionCookie)
	}

	req, err := http.NewRequest(http.MethodGet, srv.URL+"/api/stats", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.AddCookie(sessionCookie)
	resp, err = srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("stats with cookie: %d", resp.StatusCode)
	}
	if got := resp.Header.Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control = %q", got)
	}
	resp.Body.Close()

	// A wrong bearer header is authoritative and must not fall back to the
	// cookie.
	req, err = http.NewRequest(http.MethodGet, srv.URL+"/api/stats", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.AddCookie(sessionCookie)
	req.Header.Set("Authorization", "Bearer wrong-token-0123456789")
	resp, err = srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("wrong bearer with cookie: %d", resp.StatusCode)
	}
	resp.Body.Close()

	// Cookie-authenticated writes need a same-origin Origin/Referer.
	capture := func(origin string) *http.Response {
		req, err := http.NewRequest(http.MethodPost, srv.URL+"/api/capture",
			strings.NewReader(`{"text":"csrf check","source":"test"}`))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.AddCookie(sessionCookie)
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}

	resp = capture("")
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("cookie write without Origin: %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp = capture("http://evil.example")
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("cookie write with foreign Origin: %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp = capture(srv.URL)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("cookie write with same Origin: %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp = requestWithToken(t, srv, http.MethodGet, "/api/stream", "")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("stream without auth: %d", resp.StatusCode)
	}
	resp.Body.Close()

	req, err = http.NewRequest(http.MethodGet, srv.URL+"/api/stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.AddCookie(sessionCookie)
	resp, err = srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK ||
		!strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		t.Fatalf("stream with cookie: %d %q", resp.StatusCode,
			resp.Header.Get("Content-Type"))
	}
	resp.Body.Close()

	req, err = http.NewRequest(http.MethodGet, srv.URL+"/api/session", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.AddCookie(sessionCookie)
	resp, err = srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.NewDecoder(resp.Body).Decode(&session); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if !session.Authenticated || !session.AuthEnabled {
		t.Fatalf("unexpected authenticated session: %+v", session)
	}

	req, err = http.NewRequest(http.MethodPost, srv.URL+"/api/logout", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.AddCookie(sessionCookie)
	resp, err = srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("logout: %d", resp.StatusCode)
	}
	foundClear := false
	for _, cookie := range resp.Cookies() {
		if cookie.Name == authCookieName && cookie.MaxAge < 0 {
			foundClear = true
		}
	}
	resp.Body.Close()
	if !foundClear {
		t.Fatal("logout did not clear the session cookie")
	}
}

func TestLoginSecureCookieBehindTLSProxy(t *testing.T) {
	_, srv := frontendAuthTestServer(t, "test-token-0123456789")

	req, err := http.NewRequest(http.MethodPost, srv.URL+"/api/login",
		strings.NewReader(`{"token":"test-token-0123456789"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Forwarded-Proto", "https")
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("login: %d", resp.StatusCode)
	}

	for _, cookie := range resp.Cookies() {
		if cookie.Name == authCookieName {
			if !cookie.Secure {
				t.Fatal("session cookie should be Secure behind an HTTPS proxy")
			}
			return
		}
	}
	t.Fatal("login did not set a session cookie")
}

func TestFrontendAuthDisabled(t *testing.T) {
	_, srv := frontendAuthTestServer(t, "")

	resp, err := srv.Client().Post(srv.URL+"/api/login", "application/json",
		strings.NewReader(`{"token":"anything"}`))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusNoContent || len(resp.Cookies()) != 0 {
		t.Fatalf("disabled login: %d cookies=%d", resp.StatusCode, len(resp.Cookies()))
	}
	resp.Body.Close()

	resp, err = srv.Client().Get(srv.URL + "/api/stats")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("stats without auth on disabled server: %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp, err = srv.Client().Get(srv.URL + "/api/session")
	if err != nil {
		t.Fatal(err)
	}
	var session struct {
		Authenticated bool `json:"authenticated"`
		AuthEnabled   bool `json:"auth_enabled"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&session); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if !session.Authenticated || session.AuthEnabled {
		t.Fatalf("unexpected disabled session: %+v", session)
	}
}
