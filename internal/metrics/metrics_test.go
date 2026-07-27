package metrics

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestHandlerExposesCounters(t *testing.T) {
	ObserveHTTP("GET", "/healthcheck", "200")
	ObserveGRPC("RpcFunc", "OK")
	IncWSOpened()
	SetWSSessions(3)

	rr := httptest.NewRecorder()
	Handler().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d", rr.Code)
	}
	body := rr.Body.String()
	if !contains(body, "uge_http_requests_total") {
		t.Fatal("missing http counter")
	}
	if n := testutil.CollectAndCount(httpRequestsTotal); n < 1 {
		t.Fatalf("expected counter samples, got %d", n)
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(sub) == 0 || indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
