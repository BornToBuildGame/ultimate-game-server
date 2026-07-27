package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRequestIDMiddleware(t *testing.T) {
	h := RequestIDMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, _ := r.Context().Value(RequestIDKey).(string)
		if id == "" {
			t.Fatal("missing request id in context")
		}
		w.WriteHeader(http.StatusOK)
	}))

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthcheck", nil)
	req.Header.Set("X-Request-Id", "client-trace-1")
	h.ServeHTTP(rr, req)
	if got := rr.Header().Get("X-Request-Id"); got != "client-trace-1" {
		t.Fatalf("X-Request-Id=%q", got)
	}

	rr2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodGet, "/healthcheck", nil)
	h.ServeHTTP(rr2, req2)
	if got := rr2.Header().Get("X-Request-Id"); got == "" {
		t.Fatal("expected generated X-Request-Id")
	}
}

func TestRateLimitHeaders(t *testing.T) {
	lim := NewIPRateLimiter(2, 0.001)
	h := RateLimitMiddleware(lim)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	var last *httptest.ResponseRecorder
	for i := 0; i < 3; i++ {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.RemoteAddr = "10.0.0.1:1234"
		h.ServeHTTP(rr, req)
		last = rr
		if rr.Header().Get("X-RateLimit-Limit") == "" {
			t.Fatal("missing X-RateLimit-Limit")
		}
		if rr.Header().Get("X-RateLimit-Remaining") == "" {
			t.Fatal("missing X-RateLimit-Remaining")
		}
		if rr.Header().Get("X-RateLimit-Reset") == "" {
			t.Fatal("missing X-RateLimit-Reset")
		}
	}
	if last.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d", last.Code)
	}
	if last.Header().Get("Retry-After") == "" {
		t.Fatal("expected Retry-After on 429")
	}
}

func TestHealthcheckJSON(t *testing.T) {
	srv := &Server{cfg: Config{Version: "1.2.3"}}
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthcheck", nil)
	srv.handleHealthcheck(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d", rr.Code)
	}
	var body map[string]interface{}
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body["status"] != "ok" || body["version"] != "1.2.3" {
		t.Fatalf("body=%v", body)
	}
	if _, ok := body["timestamp"].(string); !ok {
		t.Fatal("missing timestamp")
	}

	rr2 := httptest.NewRecorder()
	srv.handleHealthDeprecated(rr2, httptest.NewRequest(http.MethodGet, "/health", nil))
	if rr2.Header().Get("Deprecation") != "true" {
		t.Fatal("expected Deprecation header")
	}
}

func TestReadyProbeUnavailableWithoutPool(t *testing.T) {
	srv := &Server{}
	rr := httptest.NewRecorder()
	srv.handleReady(rr, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d", rr.Code)
	}
	var body map[string]string
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body["status"] != "not_ready" || body["error"] != "database unreachable" {
		t.Fatalf("body=%v", body)
	}
}

func TestCORSAllowlist(t *testing.T) {
	h := CORSMiddleware([]string{"https://game.example"})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Origin", "https://game.example")
	h.ServeHTTP(rr, req)
	if rr.Header().Get("Access-Control-Allow-Origin") != "https://game.example" {
		t.Fatalf("origin=%q", rr.Header().Get("Access-Control-Allow-Origin"))
	}

	rr2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodGet, "/", nil)
	req2.Header.Set("Origin", "https://evil.example")
	h.ServeHTTP(rr2, req2)
	if rr2.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatal("evil origin should be rejected")
	}
}

func TestEnvelopeParseHelpers(t *testing.T) {
	_ = time.Now()
	if got := splitCSV(" a, b , ,c "); strings.Join(got, ",") != "a,b,c" {
		t.Fatalf("splitCSV=%v", got)
	}
}
