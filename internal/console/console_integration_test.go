//go:build integration

package console

import (
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/BornToBuildGame/ultimate-game-server/internal/database"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"go.uber.org/zap"
	"golang.org/x/crypto/bcrypt"
)

type mockLogger struct{}

func (m *mockLogger) Info(msg string, fields ...any)  {}
func (m *mockLogger) Error(msg string, fields ...any) {}

func TestConsole_TOTPUnit(t *testing.T) {
	secret := []byte("secret_key_12345")

	// Generate correct code for current time step
	currentTime := time.Now().Unix()
	step := currentTime / 30
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(step))
	mac := hmac.New(sha1.New, secret)
	mac.Write(buf)
	hash := mac.Sum(nil)
	offset := hash[len(hash)-1] & 0xf
	binaryCode := (int32(hash[offset])&0x7f)<<24 |
		(int32(hash[offset+1])&0xff)<<16 |
		(int32(hash[offset+2])&0xff)<<8 |
		(int32(hash[offset+3])&0xff)
	correctCode := fmt.Sprintf("%06d", binaryCode%1000000)

	// Verify valid TOTP
	if !ValidateTOTP(secret, correctCode) {
		t.Errorf("expected TOTP verification to succeed for correct code")
	}

	// Verify invalid TOTP
	if ValidateTOTP(secret, "999999") {
		t.Errorf("expected TOTP verification to fail for invalid code")
	}
}

func TestConsole_Integration(t *testing.T) {
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

	// 2. Seed Console Admin User
	consoleUserID := uuid.New().String()
	pwdHash, _ := bcrypt.GenerateFromPassword([]byte("adminpassword"), bcrypt.DefaultCost)
	insertConsoleUser := `INSERT INTO console_user (id, username, email, password, mfa_required, mfa_secret, acl) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)`
	_, err = pool.Exec(ctx, insertConsoleUser, consoleUserID, "admin_operator", "admin@game.com", pwdHash, true, []byte("mysecret"), `{"admin":true}`)
	if err != nil {
		t.Fatalf("failed to insert console user: %v", err)
	}

	// Restricted operator without write_players
	restrictedID := uuid.New().String()
	pwdHash2, _ := bcrypt.GenerateFromPassword([]byte("readonly"), bcrypt.DefaultCost)
	_, err = pool.Exec(ctx, insertConsoleUser, restrictedID, "readonly_op", "ro@game.com", pwdHash2, false, nil, `{"read_players":true}`)
	if err != nil {
		t.Fatalf("failed to insert readonly console user: %v", err)
	}

	// Seed game user profile to ban later
	gameUserID := uuid.New().String()
	insertGameUser := `INSERT INTO users (id, username, email, password, display_name) VALUES ($1, $2, $3, $4, $5)`
	_, err = pool.Exec(ctx, insertGameUser, gameUserID, "player_one", "player@test.com", []byte("hash"), "Player One")
	if err != nil {
		t.Fatalf("failed to insert game user: %v", err)
	}

	// Initialize Server
	srv, err := NewServer(&mockLogger{}, pool, []byte("jwtsecretkey"))
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	defer srv.Close()

	// 3. Test Authentication Endpoint
	// Generate current TOTP code for "mysecret"
	currentTime := time.Now().Unix()
	step := currentTime / 30
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(step))
	mac := hmac.New(sha1.New, []byte("mysecret"))
	mac.Write(buf)
	hash := mac.Sum(nil)
	offset := hash[len(hash)-1] & 0xf
	binaryCode := (int32(hash[offset])&0x7f)<<24 |
		(int32(hash[offset+1])&0xff)<<16 |
		(int32(hash[offset+2])&0xff)<<8 |
		(int32(hash[offset+3])&0xff)
	totpCode := fmt.Sprintf("%06d", binaryCode%1000000)

	authForm := url.Values{}
	authForm.Set("username", "admin_operator")
	authForm.Set("password", "adminpassword")
	authForm.Set("totp", totpCode)

	reqAuth := httptest.NewRequest("POST", "/console/authenticate", strings.NewReader(authForm.Encode()))
	reqAuth.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rrAuth := httptest.NewRecorder()

	srv.handleAuthenticate(rrAuth, reqAuth)
	if rrAuth.Code != http.StatusOK {
		t.Fatalf("auth endpoint returned bad status: %d (%s)", rrAuth.Code, rrAuth.Body.String())
	}

	// Retrieve session cookie
	cookies := rrAuth.Result().Cookies()
	var sessionCookie *http.Cookie
	for _, c := range cookies {
		if c.Name == "session_token" {
			sessionCookie = c
			break
		}
	}
	if sessionCookie == nil {
		t.Fatal("expected session_token cookie to be set")
	}

	// Verify Cookie Properties: HttpOnly, SameSite=Strict
	if !sessionCookie.HttpOnly {
		t.Errorf("expected session cookie to have HttpOnly=true")
	}
	if sessionCookie.SameSite != http.SameSiteStrictMode {
		t.Errorf("expected session cookie to have SameSite=Strict")
	}

	// 4. Test Ban User Endpoint
	banForm := url.Values{}
	banForm.Set("user_id", gameUserID)

	reqBan := httptest.NewRequest("POST", "/console/api/users/ban", strings.NewReader(banForm.Encode()))
	reqBan.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	reqBan.AddCookie(sessionCookie)
	rrBan := httptest.NewRecorder()

	srv.handleBanUser(rrBan, reqBan)
	if rrBan.Code != http.StatusOK {
		t.Fatalf("ban endpoint returned bad status: %d (%s)", rrBan.Code, rrBan.Body.String())
	}

	// Wait brief moment for async audit logger queue to execute insertion
	time.Sleep(100 * time.Millisecond)

	// Verify audit log entry in PostgreSQL
	var auditCount int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM console_audit_log WHERE action = 'ban_player'").Scan(&auditCount)
	if err != nil {
		t.Fatalf("failed to query audit log table: %v", err)
	}
	if auditCount != 1 {
		t.Errorf("expected 1 audit log entry, got: %d", auditCount)
	}

	// 5. Test Bleve Search Endpoint
	reqSearch := httptest.NewRequest("GET", "/console/api/search?q=Banned", nil)
	reqSearch.AddCookie(sessionCookie)
	rrSearch := httptest.NewRecorder()

	srv.handleSearch(rrSearch, reqSearch)
	if rrSearch.Code != http.StatusOK {
		t.Fatalf("search endpoint returned bad status: %d (%s)", rrSearch.Code, rrSearch.Body.String())
	}

	// Verify Bleve returned hits
	if !strings.Contains(rrSearch.Body.String(), `"hits":`) {
		t.Errorf("expected hits count in search results, got: %s", rrSearch.Body.String())
	}

	// 6. Player search (DB)
	reqPlayers := httptest.NewRequest("GET", "/console/api/users?q=player", nil)
	reqPlayers.AddCookie(sessionCookie)
	rrPlayers := httptest.NewRecorder()
	srv.handleSearchPlayers(rrPlayers, reqPlayers)
	if rrPlayers.Code != http.StatusOK {
		t.Fatalf("player search status %d: %s", rrPlayers.Code, rrPlayers.Body.String())
	}
	if !strings.Contains(rrPlayers.Body.String(), "player_one") {
		t.Errorf("expected player_one in search: %s", rrPlayers.Body.String())
	}

	// 7. Audit list
	reqAudit := httptest.NewRequest("GET", "/console/api/audit?limit=10", nil)
	reqAudit.AddCookie(sessionCookie)
	rrAudit := httptest.NewRecorder()
	srv.handleListAudit(rrAudit, reqAudit)
	if rrAudit.Code != http.StatusOK {
		t.Fatalf("audit list status %d: %s", rrAudit.Code, rrAudit.Body.String())
	}

	// 8. ACL deny: readonly cannot ban
	authRO := url.Values{}
	authRO.Set("username", "readonly_op")
	authRO.Set("password", "readonly")
	reqRO := httptest.NewRequest("POST", "/console/authenticate", strings.NewReader(authRO.Encode()))
	reqRO.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rrRO := httptest.NewRecorder()
	srv.handleAuthenticate(rrRO, reqRO)
	if rrRO.Code != http.StatusOK {
		t.Fatalf("readonly auth status %d: %s", rrRO.Code, rrRO.Body.String())
	}
	var roCookie *http.Cookie
	for _, c := range rrRO.Result().Cookies() {
		if c.Name == "session_token" {
			roCookie = c
			break
		}
	}
	if roCookie == nil {
		t.Fatal("expected readonly session cookie")
	}
	ban2 := url.Values{}
	ban2.Set("user_id", gameUserID)
	reqBan2 := httptest.NewRequest("POST", "/console/api/users/ban", strings.NewReader(ban2.Encode()))
	reqBan2.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	reqBan2.AddCookie(roCookie)
	rrBan2 := httptest.NewRecorder()
	srv.handleBanUser(rrBan2, reqBan2)
	if rrBan2.Code != http.StatusForbidden {
		t.Fatalf("expected forbidden for readonly ban, got %d", rrBan2.Code)
	}

	// 9. Logout revokes token
	reqLogout := httptest.NewRequest("POST", "/console/logout", nil)
	reqLogout.AddCookie(sessionCookie)
	rrLogout := httptest.NewRecorder()
	srv.handleLogout(rrLogout, reqLogout)
	if rrLogout.Code != http.StatusOK {
		t.Fatalf("logout status %d", rrLogout.Code)
	}
	reqAfter := httptest.NewRequest("GET", "/console/api/audit", nil)
	reqAfter.AddCookie(sessionCookie)
	rrAfter := httptest.NewRecorder()
	srv.handleListAudit(rrAfter, reqAfter)
	if rrAfter.Code != http.StatusUnauthorized {
		t.Fatalf("expected unauthorized after logout, got %d", rrAfter.Code)
	}
}
