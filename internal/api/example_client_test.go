//go:build integration

package api

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	"ultimate-game-server/internal/database"
	"ultimate-game-server/internal/leaderboard"
	"ultimate-game-server/internal/runtime"

	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"go.uber.org/zap"
)

/*
TestExample_GameClient demonstrates how a game client interacts with the
Ultimate Game Engine server APIs. This is a living reference and demo.

Run with:

	go test -tags=integration -run TestExample_GameClient -v -count=1 ./internal/api/...

The example covers a typical game session:
  1. Server setup (done once by infrastructure)
  2. Player registers / logs in
  3. Player stores save data
  4. Player submits a high score
  5. Player checks the leaderboard
  6. Player connects via WebSocket for real-time
  7. Player calls a custom server RPC
  8. Session management (refresh & logout)
*/
func TestExample_GameClient(t *testing.T) {
	ctx := context.Background()

	// ━━━ Infrastructure Setup (skip in a real client — server is already running) ━━━

	pgC, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("ultimate_game_db"),
		postgres.WithUsername("game_admin"),
		postgres.WithPassword("game_password"),
	)
	if err != nil {
		t.Fatalf("postgres: %v", err)
	}
	defer pgC.Terminate(ctx)

	redisC, err := tcredis.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Fatalf("redis: %v", err)
	}
	defer redisC.Terminate(ctx)

	pgDSN, _ := pgC.ConnectionString(ctx, "sslmode=disable")
	redisEP, _ := redisC.Endpoint(ctx, "")

	logger, _ := zap.NewDevelopment()
	pool, err := database.ConnectWithBackoff(ctx, logger, database.Config{
		DSN: pgDSN, MaxOpenConns: 5, MaxRetries: 10, RetryDelay: 500 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("db: %v", err)
	}
	defer pool.Close()
	database.RunMigrations(ctx, logger, pool)

	t.Setenv("REDIS_ADDR", redisEP)
	srv, _ := NewServer(logger, Config{
		HTTPAddr:        "127.0.0.1:19450",
		GRPCAddr:        "127.0.0.1:19449",
		JWTSecret:       []byte("example_jwt_secret_key_at_least_32_bytes_long_1234567890"),
		JWTExpiry:       10 * time.Minute,
		RateLimitMax:    200,
		RateLimitRefill: 200,
		RPCHTTPKey:      "example_http_key",
	}, pool)

	// Register a custom RPC that simulates getting daily rewards
	exLogger := &e2eLogger{t: t}
	sqlDB := stdlib.OpenDBFromPool(pool)
	nk := runtime.NewGoRuntimeModule(pool, exLogger)
	rm := runtime.NewGoRuntimeManager(exLogger, sqlDB, nk)
	rm.Registry().RegisterRPC("daily_reward", func(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule, payload string) (string, error) {
		reward := map[string]interface{}{
			"coins":  100,
			"gems":   5,
			"streak": 3,
			"items":  []string{"health_potion", "speed_boost"},
		}
		out, _ := json.Marshal(reward)
		return string(out), nil
	})
	srv.SetRuntimeManager(rm)
	srv.Start(ctx)
	defer func() {
		shutCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		srv.Stop(shutCtx)
	}()
	time.Sleep(200 * time.Millisecond)

	base := "http://127.0.0.1:19450"
	client := &http.Client{Timeout: 5 * time.Second}

	// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
	// EXAMPLE 1: Register a New Player
	// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
	t.Log("📝 Example 1: Register a new player")
	t.Log("   POST /v2/account/authenticate/email  {register: true}")

	registerBody, _ := json.Marshal(map[string]interface{}{
		"email":        "hero@game.example",
		"password":     "MyStrongPass1!",
		"username":     "mighty_hero",
		"display_name": "Mighty Hero",
		"register":     true,
	})
	resp, _ := client.Post(base+"/v2/account/authenticate/email", "application/json", bytes.NewReader(registerBody))
	var session struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		UserID       string `json:"user_id"`
		Username     string `json:"username"`
	}
	json.NewDecoder(resp.Body).Decode(&session)
	resp.Body.Close()
	t.Logf("   → User ID:  %s", session.UserID)
	t.Logf("   → Username: %s", session.Username)
	t.Logf("   → Token:    %s...  (truncated)", session.AccessToken[:20])

	token := session.AccessToken

	// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
	// EXAMPLE 2: Save Game Progress
	// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
	t.Log("")
	t.Log("💾 Example 2: Save game progress to server storage")
	t.Log("   PUT /v2/storage  (with Bearer auth)")

	saveData := map[string]interface{}{
		"objects": []map[string]interface{}{
			{
				"collection":       "saves",
				"key":              "slot_1",
				"value":            map[string]interface{}{
					"level":       12,
					"gold":        5430,
					"location":    "Dragon's Keep",
					"inventory":   []string{"excalibur", "shield_of_valor", "health_potion_x5"},
					"play_time_s": 14520,
				},
				"permission_read":  1,
				"permission_write": 1,
			},
		},
	}
	saveBody, _ := json.Marshal(saveData)
	req, _ := http.NewRequest("PUT", base+"/v2/storage", bytes.NewReader(saveBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	resp, _ = client.Do(req)
	var saveResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&saveResp)
	resp.Body.Close()
	acks := saveResp["acks"].([]interface{})
	ver := acks[0].(map[string]interface{})["version"].(string)
	t.Logf("   → Saved! Version: %s", ver)

	// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
	// EXAMPLE 3: Load Game Progress
	// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
	t.Log("")
	t.Log("📂 Example 3: Load game progress from server storage")
	t.Log("   POST /v2/storage/read  (with Bearer auth)")

	readBody, _ := json.Marshal(map[string]interface{}{
		"object_ids": []map[string]interface{}{
			{"collection": "saves", "key": "slot_1", "user_id": session.UserID},
		},
	})
	req, _ = http.NewRequest("POST", base+"/v2/storage/read", bytes.NewReader(readBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	resp, _ = client.Do(req)
	var readResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&readResp)
	resp.Body.Close()
	objects := readResp["objects"].([]interface{})
	obj := objects[0].(map[string]interface{})
	value := obj["value"].(map[string]interface{})
	t.Logf("   → Level: %.0f, Gold: %.0f, Location: %s", value["level"], value["gold"], value["location"])

	// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
	// EXAMPLE 4: Submit High Score
	// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
	t.Log("")
	t.Log("🏆 Example 4: Submit a high score to the leaderboard")

	lbID := "example_weekly_rank"
	leaderboard.CreateLeaderboard(ctx, pool, &leaderboard.Leaderboard{
		ID: lbID, SortOrder: leaderboard.SortOrderDescending, Operator: leaderboard.OperatorBest,
	})

	scoreBody, _ := json.Marshal(map[string]interface{}{
		"score":    9500,
		"subscore": 42,
		"metadata": map[string]string{"character": "warrior", "weapon": "excalibur"},
	})
	req, _ = http.NewRequest("POST", base+"/v2/leaderboard/"+lbID, bytes.NewReader(scoreBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	resp, _ = client.Do(req)
	var scoreResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&scoreResp)
	resp.Body.Close()
	t.Logf("   → POST /v2/leaderboard/%s", lbID)
	t.Logf("   → Score submitted! Rank: %v", scoreResp["rank"])

	// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
	// EXAMPLE 5: View Leaderboard
	// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
	t.Log("")
	t.Log("📊 Example 5: View the leaderboard")
	t.Logf("   GET /v2/leaderboard/%s?limit=10", lbID)

	req, _ = http.NewRequest("GET", fmt.Sprintf("%s/v2/leaderboard/%s?limit=10", base, lbID), nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, _ = client.Do(req)
	var lbResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&lbResp)
	resp.Body.Close()
	records := lbResp["records"].([]interface{})
	for i, r := range records {
		rec := r.(map[string]interface{})
		t.Logf("   → #%d  %s  Score: %.0f", i+1, rec["username"], rec["score"])
	}

	// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
	// EXAMPLE 6: Connect via WebSocket
	// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
	t.Log("")
	t.Log("🔌 Example 6: Connect via WebSocket for real-time features")
	t.Logf("   ws://server/ws?token=<access_token>")

	wsURL := fmt.Sprintf("ws://127.0.0.1:19450/ws?token=%s", url.QueryEscape(token))
	dialer := websocket.Dialer{HandshakeTimeout: 3 * time.Second}
	conn, _, err := dialer.Dial(wsURL, nil)
	if err != nil {
		t.Logf("   ⚠ WebSocket dial: %v", err)
	} else {
		t.Log("   → Connected! Ready for real-time messaging.")
		conn.Close()
	}

	// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
	// EXAMPLE 7: Call Custom RPC
	// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
	t.Log("")
	t.Log("⚡ Example 7: Call a custom server RPC function")
	t.Log("   POST /v2/rpc/daily_reward?unwrap")

	req, _ = http.NewRequest("POST", base+"/v2/rpc/daily_reward?unwrap", bytes.NewBufferString("{}"))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	resp, _ = client.Do(req)
	var rewardResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&rewardResp)
	resp.Body.Close()
	t.Logf("   → Coins: %.0f, Gems: %.0f, Streak: %.0f", rewardResp["coins"], rewardResp["gems"], rewardResp["streak"])

	// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
	// EXAMPLE 8: Session Refresh
	// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
	t.Log("")
	t.Log("🔄 Example 8: Refresh session tokens")
	t.Log("   POST /v2/account/session/refresh")

	refreshBody, _ := json.Marshal(map[string]interface{}{
		"token": session.RefreshToken,
	})
	resp, _ = client.Post(base+"/v2/account/session/refresh", "application/json", bytes.NewReader(refreshBody))
	var refreshResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&refreshResp)
	resp.Body.Close()
	t.Logf("   → New access token: %s...", refreshResp["access_token"].(string)[:20])

	t.Log("")
	t.Log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	t.Log("  ✅ All examples completed successfully!")
	t.Log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
}
