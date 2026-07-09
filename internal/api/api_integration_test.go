//go:build integration

package api

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"ultimate-game-server/internal/auth"
	"ultimate-game-server/internal/database"
	"ultimate-game-server/internal/runtime"

	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"go.uber.org/zap"
)

func TestAPI_Integration(t *testing.T) {
	ctx := context.Background()

	// 1. Spin up PostgreSQL container
	postgresContainer, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("ultimate_game_db"),
		postgres.WithUsername("game_admin"),
		postgres.WithPassword("game_password"),
	)
	if err != nil {
		t.Fatalf("failed to start postgres container: %v", err)
	}
	defer func() {
		if err := postgresContainer.Terminate(ctx); err != nil {
			t.Errorf("failed to terminate postgres container: %v", err)
		}
	}()

	dsn, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("failed to get container DSN: %v", err)
	}

	logger := zap.NewNop()
	dbCfg := database.Config{
		DSN:             dsn,
		MaxOpenConns:    5,
		MaxRetries:      5,
		RetryDelay:      500 * time.Millisecond,
	}

	pool, err := database.ConnectWithBackoff(ctx, logger, dbCfg)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}
	defer pool.Close()

	// Run migrations
	err = database.RunMigrations(ctx, logger, pool)
	if err != nil {
		t.Fatalf("failed to run database migrations: %v", err)
	}

	// 2. Start API Server on local test ports
	serverCfg := Config{
		HTTPAddr:        "127.0.0.1:17350",
		GRPCAddr:        "127.0.0.1:17349",
		JWTSecret:       []byte("super_secret_signing_key_at_least_32_bytes_long_1234567"),
		JWTExpiry:       10 * time.Minute,
		RateLimitMax:    5,
		RateLimitRefill: 1,
	}

	srv, err := NewServer(logger, serverCfg, pool)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	err = srv.Start(ctx)
	if err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := srv.Stop(shutdownCtx); err != nil {
			t.Errorf("failed to stop server: %v", err)
		}
	}()

	// Wait brief moment for servers to start listening
	time.Sleep(100 * time.Millisecond)

	// 3. Verify Health Endpoint
	resp, err := http.Get("http://127.0.0.1:17350/health")
	if err != nil {
		t.Fatalf("failed to query health endpoint: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected health status 200, got: %d", resp.StatusCode)
	}

	// 4. Test Body Limit Middleware (exceeding 4KB)
	largeBody := make([]byte, 5000)
	respLarge, err := http.Post("http://127.0.0.1:17350/v2/account/authenticate/email", "application/json", bytes.NewReader(largeBody))
	if err == nil {
		respLarge.Body.Close()
		// Depending on net/http server configuration, requests exceeding body size constraints
		// may return HTTP 400 Bad Request or 413 Payload Too Large.
		if respLarge.StatusCode != http.StatusBadRequest && respLarge.StatusCode != http.StatusRequestEntityTooLarge {
			t.Errorf("expected request body to be blocked, but got status: %d", respLarge.StatusCode)
		}
	}

	// 5. Test Rate Limiter Middleware (HTTP 429)
	// We configured srv with RateLimitMax: 5. Let's make 6 quick requests to /health
	client := &http.Client{}
	rateLimited := false
	for i := 0; i < 7; i++ {
		req, _ := http.NewRequest("GET", "http://127.0.0.1:17350/health", nil)
		req.Header.Set("X-Forwarded-For", "8.8.8.8") // Mock different remote ip or rely on localhost
		r, err := client.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		if r.StatusCode == http.StatusTooManyRequests {
			rateLimited = true
			r.Body.Close()
			break
		}
		r.Body.Close()
	}
	if !rateLimited {
		t.Error("expected rate limiter to block subsequent requests with HTTP 429")
	}

	// 6. Test JWT Authentication Middleware (direct middleware test)
	tm, _ := auth.NewTokenManager(serverCfg.JWTSecret, serverCfg.JWTExpiry)
	authMW := AuthMiddleware(tm)

	testHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		userID := r.Context().Value(UserIDKey).(string)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(userID))
	})

	// Case A: Missing Authorization Header
	reqAuthA := httptest.NewRequest("GET", "/test-protected", nil)
	rrA := httptest.NewRecorder()
	authMW(testHandler).ServeHTTP(rrA, reqAuthA)
	if rrA.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized for missing token, got: %d", rrA.Code)
	}

	// Case B: Valid Token
	expectedUserID := "user-abc-123"
	accessToken, _, _ := tm.GenerateSession(expectedUserID, "testuser")
	reqAuthB := httptest.NewRequest("GET", "/test-protected", nil)
	reqAuthB.Header.Set("Authorization", fmt.Sprintf("Bearer %s", accessToken))
	rrB := httptest.NewRecorder()
	authMW(testHandler).ServeHTTP(rrB, reqAuthB)
	if rrB.Code != http.StatusOK {
		t.Errorf("expected 200 OK for valid token, got: %d", rrB.Code)
	}
	if rrB.Body.String() != expectedUserID {
		t.Errorf("expected response body to contain user ID %q, got %q", expectedUserID, rrB.Body.String())
	}
}

// testLogger implements Logger for testing purposes
type testLogger struct {
	t *testing.T
}

func (l *testLogger) Debug(format string, args ...interface{}) { l.t.Logf("[DEBUG] "+format, args...) }
func (l *testLogger) Info(format string, args ...interface{})  { l.t.Logf("[INFO] "+format, args...) }
func (l *testLogger) Warn(format string, args ...interface{})  { l.t.Logf("[WARN] "+format, args...) }
func (l *testLogger) Error(format string, args ...interface{}) { l.t.Logf("[ERROR] "+format, args...) }

func TestAPI_RuntimeHooks_Integration(t *testing.T) {
	ctx := context.Background()

	// 1. Spin up PostgreSQL container
	postgresContainer, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("ultimate_game_db"),
		postgres.WithUsername("game_admin"),
		postgres.WithPassword("game_password"),
	)
	if err != nil {
		t.Fatalf("failed to start postgres container: %v", err)
	}
	defer func() {
		if err := postgresContainer.Terminate(ctx); err != nil {
			t.Errorf("failed to terminate postgres container: %v", err)
		}
	}()

	dsn, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("failed to get container DSN: %v", err)
	}

	logger := zap.NewNop()
	dbCfg := database.Config{
		DSN:             dsn,
		MaxOpenConns:    10,
		MaxRetries:      10,
		RetryDelay:      500 * time.Millisecond,
	}

	pool, err := database.ConnectWithBackoff(ctx, logger, dbCfg)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}
	defer pool.Close()

	// Run migrations
	err = database.RunMigrations(ctx, logger, pool)
	if err != nil {
		t.Fatalf("failed to run database migrations: %v", err)
	}

	// 2. Start API Server on local test ports
	serverCfg := Config{
		HTTPAddr:        "127.0.0.1:18350",
		GRPCAddr:        "127.0.0.1:18349",
		JWTSecret:       []byte("super_secret_signing_key_at_least_32_bytes_long_1234567"),
		JWTExpiry:       10 * time.Minute,
		RateLimitMax:    5,
		RateLimitRefill: 1,
	}

	srv, err := NewServer(logger, serverCfg, pool)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	// Setup runtime manager with mock hook registrations
	rm := runtime.NewGoRuntimeManager(&testLogger{t: t}, nil, nil)

	// Register before hook that mutates the username
	rm.Registry().RegisterBefore("AuthenticateEmail", func(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule, in interface{}) (interface{}, error) {
		req := in.(*authEmailRequest)
		req.Username = "intercepted_user"
		return req, nil
	})

	// Register after hook that flags completion
	var afterCalled bool
	var afterMu sync.Mutex
	afterCond := sync.NewCond(&afterMu)

	rm.Registry().RegisterAfter("AuthenticateEmail", func(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule, out interface{}, in interface{}) error {
		afterMu.Lock()
		afterCalled = true
		afterCond.Signal()
		afterMu.Unlock()
		return nil
	})

	srv.SetRuntimeManager(rm)

	err = srv.Start(ctx)
	if err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		srv.Stop(shutdownCtx)
	}()

	time.Sleep(100 * time.Millisecond)

	// 3. Post Authenticate Request
	payload := []byte(`{
		"email": "hook@test.com",
		"password": "Password123!",
		"username": "original_user",
		"display_name": "Original User",
		"register": true
	}`)

	resp, err := http.Post("http://127.0.0.1:18350/v2/account/authenticate/email", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("auth request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		buf := new(bytes.Buffer)
		buf.ReadFrom(resp.Body)
		t.Fatalf("expected status 200 OK, got: %d. Body: %s", resp.StatusCode, buf.String())
	}

	// 4. Verify username in database was intercepted and changed to "intercepted_user"
	var dbUsername string
	err = pool.QueryRow(ctx, "SELECT username FROM users WHERE email = $1", "hook@test.com").Scan(&dbUsername)
	if err != nil {
		t.Fatalf("failed to query user from DB: %v", err)
	}

	if dbUsername != "intercepted_user" {
		t.Errorf("expected username in database to be 'intercepted_user', got: %s", dbUsername)
	}

	// 5. Verify after hook was asynchronously called
	afterMu.Lock()
	if !afterCalled {
		// Wait brief moment
		afterCond.Wait()
	}
	isAfterCalled := afterCalled
	afterMu.Unlock()

	if !isAfterCalled {
		t.Error("expected after hook to be invoked asynchronously, but it was not")
	}
}

