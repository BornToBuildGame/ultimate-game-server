package api

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/BornToBuildGame/ultimate-game-server/internal/auth"
	"github.com/BornToBuildGame/ultimate-game-server/internal/runtime"

	"go.uber.org/zap"
)

type rpcTestLogger struct{}

func (rpcTestLogger) Debug(format string, args ...interface{}) {}
func (rpcTestLogger) Info(format string, args ...interface{})  {}
func (rpcTestLogger) Warn(format string, args ...interface{})  {}
func (rpcTestLogger) Error(format string, args ...interface{}) {}

func TestRPC_HTTP_WrapUnwrapAndHTTPKey(t *testing.T) {
	logger := zap.NewNop()
	cfg := Config{
		HTTPAddr:  "127.0.0.1:0",
		GRPCAddr:  "127.0.0.1:0",
		JWTSecret: []byte("super_secret_signing_key_at_least_32_bytes_long_1234567"),
		JWTExpiry: time.Hour,
		RPCHTTPKey: "testkey",
	}
	srv, err := NewServer(logger, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	rm := runtime.NewGoRuntimeManager(&rpcTestLogger{}, nil, nil)
	rm.Registry().RegisterRPC("echo", func(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule, payload string) (string, error) {
		return "out:" + payload, nil
	})
	srv.SetRuntimeManager(rm)
	srv.rpcCfg.HTTPKey = "testkey"

	mux := http.NewServeMux()
	mux.HandleFunc("GET /v2/rpc/{id}", srv.handleRPC)
	mux.HandleFunc("POST /v2/rpc/{id}", srv.handleRPC)

	// wrapped POST with http_key
	body, _ := json.Marshal("hello")
	req := httptest.NewRequest(http.MethodPost, "/v2/rpc/echo?http_key=testkey", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	var resp map[string]string
	_ = json.Unmarshal(rr.Body.Bytes(), &resp)
	if resp["payload"] != "out:hello" {
		t.Fatalf("payload=%q", resp["payload"])
	}

	// unwrap
	req2 := httptest.NewRequest(http.MethodPost, "/v2/rpc/echo?http_key=testkey&unwrap", bytes.NewReader([]byte("raw")))
	rr2 := httptest.NewRecorder()
	mux.ServeHTTP(rr2, req2)
	if rr2.Body.String() != "out:raw" {
		t.Fatalf("unwrap body=%q", rr2.Body.String())
	}

	// bad key
	req3 := httptest.NewRequest(http.MethodGet, "/v2/rpc/echo?http_key=bad", nil)
	rr3 := httptest.NewRecorder()
	mux.ServeHTTP(rr3, req3)
	if rr3.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr3.Code)
	}

	// bearer
	tm, _ := auth.NewTokenManager(cfg.JWTSecret, cfg.JWTExpiry)
	tok, _, _ := tm.GenerateSession("user-1", "alice")
	req4 := httptest.NewRequest(http.MethodGet, "/v2/rpc/echo", nil)
	req4.Header.Set("Authorization", "Bearer "+tok)
	rr4 := httptest.NewRecorder()
	mux.ServeHTTP(rr4, req4)
	if rr4.Code != http.StatusOK {
		t.Fatalf("bearer status=%d body=%s", rr4.Code, rr4.Body.String())
	}

	// not found
	req5 := httptest.NewRequest(http.MethodGet, "/v2/rpc/missing?http_key=testkey", nil)
	rr5 := httptest.NewRecorder()
	mux.ServeHTTP(rr5, req5)
	if rr5.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rr5.Code)
	}
}
