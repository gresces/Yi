package main

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	authCookieName   = "yi_session"
	authCookieMaxAge = 30 * 24 * 60 * 60
)

// auth是服务端唯一的认证配置:一个共享的 bearer token。
// token 为空时认证关闭,保持旧配置可直接运行。
type auth struct {
	token string
}

func newAuth(token string) *auth {
	return &auth{token: strings.TrimSpace(token)}
}

func (a *auth) enabled() bool {
	return a != nil && a.token != ""
}

func secureEqual(left, right string) bool {
	return subtle.ConstantTimeCompare([]byte(left), []byte(right)) == 1
}

func bearerToken(header string) string {
	const prefix = "bearer "
	if len(header) <= len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) {
		return ""
	}
	return strings.TrimSpace(header[len(prefix):])
}

func (a *auth) headerAuthorized(r *http.Request) bool {
	header := r.Header.Get("Authorization")
	if header == "" {
		return false
	}
	token := bearerToken(header)
	return token != "" && secureEqual(token, a.token)
}

func (a *auth) cookieAuthorized(r *http.Request) bool {
	cookie, err := r.Cookie(authCookieName)
	return err == nil && secureEqual(cookie.Value, a.token)
}

func (a *auth) authorized(r *http.Request, allowCookie bool) bool {
	if !a.enabled() {
		return true
	}
	// A present Authorization header is authoritative: do not silently fall
	// back to the cookie when a client sends the wrong bearer token.
	if r.Header.Get("Authorization") != "" {
		return a.headerAuthorized(r)
	}
	return allowCookie && a.cookieAuthorized(r)
}

// wrap protects a handler. public may be nil; paths it accepts bypass auth.
// allowCookie is true only on the frontend listener; the machine API is
// bearer-only, which also removes cookie CSRF from write endpoints there.
func (a *auth) wrap(next http.Handler, public func(*http.Request) bool, allowCookie bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if public != nil && public(r) {
			next.ServeHTTP(w, r)
			return
		}
		if !a.authorized(r, allowCookie) {
			w.Header().Set("WWW-Authenticate", `Bearer realm="yi"`)
			writeErr(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		if a.enabled() && allowCookie && !a.headerAuthorized(r) &&
			!safeHTTPMethod(r.Method) && !sameOrigin(r) {
			writeErr(w, http.StatusForbidden, "cross-origin request rejected")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func safeHTTPMethod(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return true
	default:
		return false
	}
}

func sameOrigin(r *http.Request) bool {
	source := r.Header.Get("Origin")
	if source == "" {
		source = r.Header.Get("Referer")
	}
	if source == "" {
		return false
	}
	parsed, err := url.Parse(source)
	if err != nil || parsed.Host == "" {
		return false
	}
	return strings.EqualFold(parsed.Host, r.Host)
}

func secureCookieRequest(r *http.Request) bool {
	return r.TLS != nil ||
		strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https")
}

func (a *auth) setCookie(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{
		Name:     authCookieName,
		Value:    a.token,
		Path:     "/",
		HttpOnly: true,
		Secure:   secureCookieRequest(r),
		SameSite: http.SameSiteStrictMode,
		MaxAge:   authCookieMaxAge,
	})
}

func (a *auth) clearCookie(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{
		Name:     authCookieName,
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		Secure:   secureCookieRequest(r),
		SameSite: http.SameSiteStrictMode,
		MaxAge:   -1,
		Expires:  time.Unix(1, 0),
	})
}

func (a *auth) login(w http.ResponseWriter, r *http.Request) {
	if !a.enabled() {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	var body struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if !secureEqual(strings.TrimSpace(body.Token), a.token) {
		w.Header().Set("WWW-Authenticate", `Bearer realm="yi"`)
		writeErr(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	a.setCookie(w, r)
	w.WriteHeader(http.StatusNoContent)
}

func (a *auth) logout(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Origin") != "" && !sameOrigin(r) {
		writeErr(w, http.StatusForbidden, "cross-origin request rejected")
		return
	}
	a.clearCookie(w, r)
	w.WriteHeader(http.StatusNoContent)
}

func (a *auth) session(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"authenticated": a.authorized(r, true),
		"auth_enabled":  a.enabled(),
	})
}

// validateAuthToken restricts tokens to the URL-safe / base64 alphabet so the
// same value can safely be placed in the config file and in a cookie.
func validateAuthToken(token string) error {
	if token == "" {
		return nil
	}
	if len(token) < 16 {
		return fmt.Errorf("auth-token must be at least 16 characters")
	}
	for _, char := range token {
		switch {
		case char >= 'a' && char <= 'z',
			char >= 'A' && char <= 'Z',
			char >= '0' && char <= '9',
			char == '-', char == '.', char == '_', char == '~',
			char == '+', char == '/', char == '=':
			continue
		default:
			return fmt.Errorf("auth-token contains unsupported character %q", char)
		}
	}
	return nil
}

func newAuthToken() (string, error) {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

func authPublicService(r *http.Request) bool {
	return r.URL.Path == "/healthz"
}

func authPublicFrontend(r *http.Request) bool {
	path := r.URL.Path
	return path == "/" ||
		strings.HasPrefix(path, "/static/") ||
		path == "/api/login" ||
		path == "/api/logout" ||
		path == "/api/session"
}
