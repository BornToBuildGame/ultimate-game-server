//go:build integration

package taixiu

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/BornToBuildGame/ultimate-game-server/internal/api"
	"github.com/BornToBuildGame/ultimate-game-server/internal/database"
	"github.com/BornToBuildGame/ultimate-game-server/pkg/runtime"

	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"go.uber.org/zap"
)

type integrationLogger struct {
	t *testing.T
}

func (l *integrationLogger) Debug(format string, args ...interface{}) { l.t.Logf("[DEBUG] "+format, args...) }
func (l *integrationLogger) Info(format string, args ...interface{})  { l.t.Logf("[INFO] "+format, args...) }
func (l *integrationLogger) Warn(format string, args ...interface{})  { l.t.Logf("[WARN] "+format, args...) }
func (l *integrationLogger) Error(format string, args ...interface{}) { l.t.Logf("[ERROR] "+format, args...) }

// TestTaiXiuMatch_E2E_Integration tests the complete Tài-Xỉu game loop using a real PostgreSQL database,
// Redis instance, HTTP REST authentication, WebSocket real-time connection, and database wallet persistence.
func TestTaiXiuMatch_E2E_Integration(t *testing.T) {
	ctx := context.Background()

	// 1. Start Postgres & Redis Testcontainers
	pgC, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("ultimate_taixiu_db"),
		postgres.WithUsername("game_admin"),
		postgres.WithPassword("game_password"),
	)
	if err != nil {
		t.Fatalf("postgres container: %v", err)
	}
	defer pgC.Terminate(ctx)

	redisC, err := tcredis.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Fatalf("redis container: %v", err)
	}
	defer redisC.Terminate(ctx)

	pgDSN, _ := pgC.ConnectionString(ctx, "sslmode=disable")
	redisEP, _ := redisC.Endpoint(ctx, "")
	t.Setenv("REDIS_ADDR", redisEP)

	logger, _ := zap.NewDevelopment()
	pool, err := database.ConnectWithBackoff(ctx, logger, database.Config{
		DSN: pgDSN, MaxOpenConns: 5, MaxRetries: 10, RetryDelay: 500 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("database pool: %v", err)
	}
	defer pool.Close()

	if err := database.RunMigrations(ctx, logger, pool); err != nil {
		t.Fatalf("run database migrations: %v", err)
	}

	// 2. Initialize Server and GoRuntime
	httpAddr := "127.0.0.1:19650"
	grpcAddr := "127.0.0.1:19649"

	srv, err := api.NewServer(logger, api.Config{
		HTTPAddr:        httpAddr,
		GRPCAddr:        grpcAddr,
		JWTSecret:       []byte("taixiu_e2e_integration_secret_key_32_bytes!"),
		JWTExpiry:       10 * time.Minute,
		RateLimitMax:    500,
		RateLimitRefill: 500,
		RPCHTTPKey:      "taixiu_http_key",
	}, pool)
	if err != nil {
		t.Fatalf("create server: %v", err)
	}

	e2eLog := &integrationLogger{t: t}
	sqlDB := stdlib.OpenDBFromPool(pool)
	nk := runtime.NewGoRuntimeModule(pool, e2eLog)
	rm := runtime.NewGoRuntimeManager(e2eLog, sqlDB, nk)

	// Register TaiXiuMatch module with server runtime registry
	err = rm.Registry().RegisterMatch("tai_xiu", func(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule) (runtime.Match, error) {
		return NewTaiXiuMatch(), nil
	})
	if err != nil {
		t.Fatalf("register tai_xiu match module: %v", err)
	}

	srv.SetRuntimeManager(rm)
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start server: %v", err)
	}
	defer func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Stop(shutCtx)
	}()

	time.Sleep(300 * time.Millisecond)

	// Setup dedicated test WebSocket server directly bound to srv.SocketGateway.Upgrade
	wsServer := httptest.NewServer(http.HandlerFunc(srv.SocketGateway.Upgrade))
	defer wsServer.Close()

	wsURLParsed, err := url.Parse(wsServer.URL)
	if err != nil {
		t.Fatalf("parse ws test server URL: %v", err)
	}
	wsURLParsed.Scheme = "ws"
	wsBaseURL := wsURLParsed.String()

	baseURL := "http://" + httpAddr
	client := &http.Client{Timeout: 5 * time.Second}

	// 3. Register Player 1 & Player 2 via HTTP Authentication REST API
	p1Session := registerPlayer(t, client, baseURL, "player1_taixiu@game.test", "Pass1234!", "taixiu_p1")
	p2Session := registerPlayer(t, client, baseURL, "player2_taixiu@game.test", "Pass1234!", "taixiu_p2")

	// 4. Initialize Player Wallets with 1,000 Coins in Real Postgres Database
	_, _, err = nk.WalletUpdate(ctx, p1Session.UserID, map[string]int64{"coins": 1000}, map[string]interface{}{"reason": "initial_balance"}, true)
	if err != nil {
		t.Fatalf("wallet init player 1: %v", err)
	}
	_, _, err = nk.WalletUpdate(ctx, p2Session.UserID, map[string]int64{"coins": 1000}, map[string]interface{}{"reason": "initial_balance"}, true)
	if err != nil {
		t.Fatalf("wallet init player 2: %v", err)
	}

	t.Logf("Initialized Postgres wallets for %s and %s with 1,000 coins", p1Session.UserID, p2Session.UserID)

	// 5. Create a TaiXiu Match Room via REST API
	createMatchBody, _ := json.Marshal(map[string]interface{}{
		"module": "tai_xiu",
		"params": map[string]interface{}{
			"betting_ticks": 2, // 2-second betting window for fast test execution
		},
	})
	req, _ := http.NewRequest("POST", baseURL+"/v2/match", bytes.NewReader(createMatchBody))
	req.Header.Set("Authorization", "Bearer "+p1Session.AccessToken)
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("create match room failed: %v (code %d)", err, resp.StatusCode)
	}

	var matchResp map[string]string
	json.NewDecoder(resp.Body).Decode(&matchResp)
	resp.Body.Close()
	matchID := matchResp["match_id"]
	if matchID == "" {
		t.Fatal("expected non-empty match ID from REST response")
	}
	t.Logf("🎮 Authoritative match created with ID: %s", matchID)

	// 6. Connect Real WebSocket Clients for Player 1 and Player 2
	ws1URL := fmt.Sprintf("%s/ws?token=%s", wsBaseURL, url.QueryEscape(p1Session.AccessToken))
	ws1, _, err := websocket.DefaultDialer.Dial(ws1URL, nil)
	if err != nil {
		t.Fatalf("WebSocket dial player 1: %v", err)
	}
	defer ws1.Close()

	ws2URL := fmt.Sprintf("%s/ws?token=%s", wsBaseURL, url.QueryEscape(p2Session.AccessToken))
	ws2, _, err := websocket.DefaultDialer.Dial(ws2URL, nil)
	if err != nil {
		t.Fatalf("WebSocket dial player 2: %v", err)
	}
	defer ws2.Close()

	// 7. Join Match Room via WebSocket
	joinEnv1 := fmt.Sprintf(`{"cid": "1", "match_join": {"match_id": "%s"}}`, matchID)
	if err := ws1.WriteMessage(websocket.TextMessage, []byte(joinEnv1)); err != nil {
		t.Fatalf("ws1 send match_join: %v", err)
	}

	joinEnv2 := fmt.Sprintf(`{"cid": "2", "match_join": {"match_id": "%s"}}`, matchID)
	if err := ws2.WriteMessage(websocket.TextMessage, []byte(joinEnv2)); err != nil {
		t.Fatalf("ws2 send match_join: %v", err)
	}

	// 8. Player 1 Bets 100 on TAI, Player 2 Bets 200 on XIU over WebSocket
	betData1, _ := json.Marshal(BetRequest{Choice: ChoiceTai, Amount: 100})
	betEnv1 := map[string]interface{}{
		"cid": "3",
		"match_data_send": map[string]interface{}{
			"match_id": matchID,
			"op_code":  OpCodeBet,
			"data":     string(betData1),
		},
	}
	betBytes1, _ := json.Marshal(betEnv1)
	if err := ws1.WriteMessage(websocket.TextMessage, betBytes1); err != nil {
		t.Fatalf("ws1 send bet: %v", err)
	}

	betData2, _ := json.Marshal(BetRequest{Choice: ChoiceXiu, Amount: 200})
	betEnv2 := map[string]interface{}{
		"cid": "4",
		"match_data_send": map[string]interface{}{
			"match_id": matchID,
			"op_code":  OpCodeBet,
			"data":     string(betData2),
		},
	}
	betBytes2, _ := json.Marshal(betEnv2)
	if err := ws2.WriteMessage(websocket.TextMessage, betBytes2); err != nil {
		t.Fatalf("ws2 send bet: %v", err)
	}

	t.Log("🎲 Player 1 placed bet of 100 on TAI via WebSocket")
	t.Log("🎲 Player 2 placed bet of 200 on XIU via WebSocket")

	// 9. Read WebSocket Messages to Assert RoundResult Broadcast
	var roundResult *RoundResultBroadcast

	for {
		_ = ws1.SetReadDeadline(time.Now().Add(15 * time.Second))
		_, raw, err := ws1.ReadMessage()
		if err != nil {
			t.Fatalf("WebSocket read error while waiting for RoundResult: %v", err)
		}

		var envelope struct {
			MatchData struct {
				OpCode int64  `json:"op_code"`
				Data   string `json:"data"`
			} `json:"match_data"`
		}
		if err := json.Unmarshal(raw, &envelope); err != nil {
			continue
		}

		if envelope.MatchData.OpCode == OpCodeRoundResult {
			decoded, err := base64.StdEncoding.DecodeString(envelope.MatchData.Data)
			if err != nil {
				decoded = []byte(envelope.MatchData.Data)
			}
			var res RoundResultBroadcast
			if json.Unmarshal(decoded, &res) == nil {
				roundResult = &res
				t.Logf("✨ Received OpCodeRoundResult on WebSocket: Dice=%v Sum=%d WinningSide=%s",
					res.Dice, res.Sum, res.WinningSide)
				break
			}
		}
	}

	if roundResult == nil {
		t.Fatal("expected non-nil round result broadcast")
	}

	// Wait 500ms for async wallet updates to complete in Postgres DB
	time.Sleep(500 * time.Millisecond)

	// 10. Query Postgres DB directly to verify wallet balances persisted correctly
	p1Wallet, _, err := nk.WalletUpdate(ctx, p1Session.UserID, map[string]int64{"coins": 0}, nil, false)
	if err != nil {
		t.Fatalf("read p1 wallet: %v", err)
	}
	p2Wallet, _, err := nk.WalletUpdate(ctx, p2Session.UserID, map[string]int64{"coins": 0}, nil, false)
	if err != nil {
		t.Fatalf("read p2 wallet: %v", err)
	}

	t.Logf("📊 Final Database Wallet Balance - Player 1 (%s): %d coins", p1Session.UserID, p1Wallet["coins"])
	t.Logf("📊 Final Database Wallet Balance - Player 2 (%s): %d coins", p2Session.UserID, p2Wallet["coins"])

	if roundResult.WinningSide == ChoiceTai {
		if p1Wallet["coins"] != 1100 {
			t.Errorf("expected Player 1 wallet balance 1100 (won 100), got %d", p1Wallet["coins"])
		}
		if p2Wallet["coins"] != 800 {
			t.Errorf("expected Player 2 wallet balance 800 (lost 200), got %d", p2Wallet["coins"])
		}
	} else if roundResult.WinningSide == ChoiceXiu {
		if p1Wallet["coins"] != 900 {
			t.Errorf("expected Player 1 wallet balance 900 (lost 100), got %d", p1Wallet["coins"])
		}
		if p2Wallet["coins"] != 1200 {
			t.Errorf("expected Player 2 wallet balance 1200 (won 200), got %d", p2Wallet["coins"])
		}
	}
}

type authSession struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	UserID       string `json:"user_id"`
	Username     string `json:"username"`
}

func registerPlayer(t *testing.T, client *http.Client, baseURL, email, password, username string) authSession {
	body, _ := json.Marshal(map[string]interface{}{
		"email":        email,
		"password":     password,
		"username":     username,
		"display_name": username,
		"register":     true,
	})
	resp, err := client.Post(baseURL+"/v2/account/authenticate/email", "application/json", bytes.NewReader(body))
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("register player %s failed: %v (code %d)", email, err, resp.StatusCode)
	}
	defer resp.Body.Close()

	var sess authSession
	if err := json.NewDecoder(resp.Body).Decode(&sess); err != nil {
		t.Fatalf("decode auth session: %v", err)
	}
	return sess
}
