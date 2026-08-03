package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/BornToBuildGame/ultimate-game-server/internal/api/apipb"
	"github.com/BornToBuildGame/ultimate-game-server/internal/auth"
	"github.com/BornToBuildGame/ultimate-game-server/internal/match"
	"github.com/BornToBuildGame/ultimate-game-server/internal/runtime"

	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type mockGoMatch struct {
	initCalled bool
	loopCalled bool
}

func (m *mockGoMatch) MatchInit(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule, params map[string]interface{}) (interface{}, int, string) {
	m.initCalled = true
	return map[string]interface{}{"status": "ready"}, 30, `{"mode": "test"}`
}

func (m *mockGoMatch) MatchJoinAttempt(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule, dispatcher interface{}, tick int64, state interface{}, presence runtime.Presence, metadata map[string]string) (interface{}, bool, string) {
	return state, true, ""
}

func (m *mockGoMatch) MatchJoin(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule, dispatcher interface{}, tick int64, state interface{}, presences []runtime.Presence) interface{} {
	return state
}

func (m *mockGoMatch) MatchLeave(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule, dispatcher interface{}, tick int64, state interface{}, presences []runtime.Presence) interface{} {
	return state
}

func (m *mockGoMatch) MatchLoop(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule, dispatcher interface{}, tick int64, state interface{}, messages []runtime.MatchData) interface{} {
	m.loopCalled = true
	return state
}

func (m *mockGoMatch) MatchTerminate(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule, dispatcher interface{}, tick int64, state interface{}, graceSeconds int) interface{} {
	return state
}

func (m *mockGoMatch) MatchSignal(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule, dispatcher interface{}, tick int64, state interface{}, data string) (interface{}, string) {
	return state, "signal_response: " + data
}

func TestRealtimeServer_GrpcAndRest(t *testing.T) {
	logger := zap.NewNop()
	secret := []byte("super_secret_signing_key_at_least_32_bytes_long_1234567")
	tm, _ := auth.NewTokenManager(secret, 10*time.Minute)

	token, _, _ := tm.GenerateSession("user-1", "gamer_1")

	// Set up match router and hook registry
	hr := runtime.NewHookRegistry()
	mockMatch := &mockGoMatch{}
	hr.RegisterMatch("mock_module", func(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule) (runtime.Match, error) {
		return mockMatch, nil
	})

	matchRouter := match.NewRouter()
	matchRouter.SetDependencies(hr, nil, logger, nil, nil)

	// Create RealtimeServer
	rtServer := NewRealtimeServer(logger, matchRouter, nil, tm)

	// Context with grpc auth metadata
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+token))

	// 1. Create match via router
	matchID := match.NewAuthoritativeMatchID()
	err := matchRouter.CreateAndRegisterMatch(ctx, matchID, "mock_module", map[string]interface{}{"foo": "bar"})
	if err != nil {
		t.Fatalf("failed to create match: %v", err)
	}

	// Give match goroutine a moment to start
	time.Sleep(30 * time.Millisecond)

	if !mockMatch.initCalled {
		t.Error("expected mockMatch.MatchInit to have been called")
	}

	// 2. Test ListMatches (gRPC)
	listReq := &apipb.ListMatchesRequest{Authoritative: wrapperspb.Bool(true)}
	matchesList, err := rtServer.ListMatches(ctx, listReq)
	if err != nil {
		t.Fatalf("failed to list matches: %v", err)
	}
	if len(matchesList.Matches) == 0 {
		t.Error("expected at least one active match in list")
	}

	// 5. Test REST API Handlers using Server
	srv := &Server{
		logger:      logger,
		tokenMgr:    tm,
		MatchRouter: matchRouter,
	}

	// Test REST CreateMatch
	body := `{"module": "mock_module", "params": {"x": 1}}`
	req := httptest.NewRequest("POST", "/v2/match", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()

	srv.handleCreateMatch(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200, got: %d (%s)", w.Code, w.Body.String())
	}

	var createResp map[string]string
	_ = json.Unmarshal(w.Body.Bytes(), &createResp)
	restMatchID := createResp["match_id"]
	if restMatchID == "" {
		t.Error("expected non-empty match ID from REST response")
	}
	if !strings.Contains(restMatchID, ".") {
		t.Errorf("expected REST match ID with node suffix, got: %s", restMatchID)
	}

	// Give new match loop a moment to start
	time.Sleep(30 * time.Millisecond)

	// Test REST ListMatches
	reqList := httptest.NewRequest("GET", "/v2/match", nil)
	reqList.Header.Set("Authorization", "Bearer "+token)
	wList := httptest.NewRecorder()

	srv.handleListMatches(wList, reqList)

	if wList.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200, got: %d", wList.Code)
	}

	var listResp map[string]interface{}
	_ = json.Unmarshal(wList.Body.Bytes(), &listResp)
	matches := listResp["matches"].([]interface{})
	if len(matches) < 2 {
		t.Errorf("expected at least 2 matches in list, got: %d", len(matches))
	}

	// Test REST GetMatch
	reqGet := httptest.NewRequest("GET", "/v2/match/"+restMatchID, nil)
	// Mock PathValue for Go 1.22 routing
	reqGet.SetPathValue("match_id", restMatchID)
	reqGet.Header.Set("Authorization", "Bearer "+token)
	wGet := httptest.NewRecorder()

	srv.handleGetMatch(wGet, reqGet)

	if wGet.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200, got: %d (%s)", wGet.Code, wGet.Body.String())
	}

	var getResp map[string]interface{}
	_ = json.Unmarshal(wGet.Body.Bytes(), &getResp)
	if getResp["match_id"] != restMatchID {
		t.Errorf("expected match ID %s, got: %v", restMatchID, getResp["match_id"])
	}
}
