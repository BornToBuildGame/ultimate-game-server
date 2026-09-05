package api

import (
	"bufio"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
)

type hijackableRecorder struct {
	http.ResponseWriter
	hijacked bool
}

func (h *hijackableRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h.hijacked = true
	c1, c2 := net.Pipe()
	_ = c2.Close()
	return c1, bufio.NewReadWriter(bufio.NewReader(c1), bufio.NewWriter(c1)), nil
}

func TestStatusRecorderHijack(t *testing.T) {
	base := &hijackableRecorder{ResponseWriter: httptest.NewRecorder()}
	rec := &statusRecorder{ResponseWriter: base, status: http.StatusOK}
	conn, _, err := rec.Hijack()
	if err != nil {
		t.Fatalf("Hijack: %v", err)
	}
	defer conn.Close()
	if !base.hijacked {
		t.Fatal("expected underlying Hijacker to be called")
	}
}

func TestMetricsMiddlewareSkipsWSWrap(t *testing.T) {
	var sawHijacker bool
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, ok := w.(http.Hijacker)
		sawHijacker = ok
	})
	h := MetricsMiddleware(inner)
	req := httptest.NewRequest(http.MethodGet, "/ws", nil)
	base := &hijackableRecorder{ResponseWriter: httptest.NewRecorder()}
	h.ServeHTTP(base, req)
	if !sawHijacker {
		t.Fatal("/ws handler should receive Hijacker-capable writer (unwrapped)")
	}
}
