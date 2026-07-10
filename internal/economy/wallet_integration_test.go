//go:build integration

package economy

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"ultimate-game-server/internal/database"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"go.uber.org/zap"
)

func TestWallet_Integration(t *testing.T) {
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
		MaxOpenConns:    10, // increase conns for concurrency
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

	// 2. Insert test user with initial wallet balance
	userID := uuid.New().String()
	initialWallet := `{"coins": 1000}`
	insertUser := `INSERT INTO users (id, username, email, password, display_name, wallet) VALUES ($1, $2, $3, $4, $5, $6)`
	_, err = pool.Exec(ctx, insertUser, userID, "wallet_user", "wallet@test.com", []byte("hash"), "Wallet User", initialWallet)
	if err != nil {
		t.Fatalf("failed to insert user: %v", err)
	}

	// 3. Concurrently deduct 100 coins 10 times (total 1000)
	var wg sync.WaitGroup
	errorsChan := make(chan error, 10)
	changeset := map[string]int64{"coins": -100}
	metadata := map[string]interface{}{"reason": "purchase"}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := UpdateWallet(ctx, pool, userID, changeset, metadata)
			if err != nil {
				errorsChan <- err
			}
		}()
	}

	wg.Wait()
	close(errorsChan)

	// Assert no errors occurred during the 10 parallel deductions
	for err := range errorsChan {
		t.Errorf("unexpected error during concurrent deductions: %v", err)
	}

	// 4. Try one more deduction -> should fail due to insufficient funds (0 coins left)
	_, err = UpdateWallet(ctx, pool, userID, changeset, metadata)
	if !errors.Is(err, ErrInsufficientFunds) {
		t.Errorf("expected ErrInsufficientFunds, got: %v", err)
	}

	// 5. Verify final balance in database is 0
	var walletBytes []byte
	err = pool.QueryRow(ctx, "SELECT wallet FROM users WHERE id = $1", userID).Scan(&walletBytes)
	if err != nil {
		t.Fatalf("failed to query user wallet: %v", err)
	}

	wallet := make(map[string]int64)
	json.Unmarshal(walletBytes, &wallet)
	if balance := wallet["coins"]; balance != 0 {
		t.Errorf("expected final coins balance to be 0, got: %d", balance)
	}

	// 6. Verify 10 ledger rows were created
	var ledgerCount int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM wallet_ledger WHERE user_id = $1", userID).Scan(&ledgerCount)
	if err != nil {
		t.Fatalf("failed to count ledger entries: %v", err)
	}
	if ledgerCount != 10 {
		t.Errorf("expected 10 ledger entries, got: %d", ledgerCount)
	}
}

func TestIAP_Integration(t *testing.T) {
	ctx := context.Background()

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
		DSN:          dsn,
		MaxOpenConns: 5,
		MaxRetries:   5,
		RetryDelay:   500 * time.Millisecond,
	}

	pool, err := database.ConnectWithBackoff(ctx, logger, dbCfg)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer pool.Close()

	err = database.RunMigrations(ctx, logger, pool)
	if err != nil {
		t.Fatalf("migrations failed: %v", err)
	}

	// Create user
	userID := uuid.New().String()
	_, err = pool.Exec(ctx, "INSERT INTO users (id, username, password, display_name) VALUES ($1, 'iap_user', 'pass', 'IAP')", userID)
	if err != nil {
		t.Fatalf("failed to insert user: %v", err)
	}

	p := StorePurchase{
		TransactionID: "tx_apple_123",
		ProductID:     "gems_pack_100",
		PurchaseTime:  time.Now(),
		Environment:   1, // Sandbox
	}

	// 1. Process Apple IAP -> should succeed
	err = ProcessAppleValidationTx(ctx, pool, userID, p, `{"status": 0}`)
	if err != nil {
		t.Fatalf("failed to process Apple IAP: %v", err)
	}

	// 2. Re-process Apple IAP -> should fail (duplicate check)
	err = ProcessAppleValidationTx(ctx, pool, userID, p, `{"status": 0}`)
	if err != ErrTransactionSeenBefore {
		t.Fatalf("expected ErrTransactionSeenBefore, got: %v", err)
	}

	// 3. Process Google IAP -> should succeed
	p2 := StorePurchase{
		TransactionID: "tx_google_456",
		ProductID:     "gems_pack_100",
		PurchaseTime:  time.Now(),
		Environment:   1, // Sandbox
	}
	err = ProcessGoogleValidationTx(ctx, pool, userID, p2, `{"purchaseState": 0}`)
	if err != nil {
		t.Fatalf("failed to process Google IAP: %v", err)
	}

	// 4. Verify wallet balance
	var walletBytes []byte
	err = pool.QueryRow(ctx, "SELECT wallet FROM users WHERE id = $1", userID).Scan(&walletBytes)
	if err != nil {
		t.Fatalf("failed to query user wallet: %v", err)
	}

	var w map[string]int64
	_ = json.Unmarshal(walletBytes, &w)
	if w["gems"] != 200 {
		t.Errorf("expected 200 gems, got: %d", w["gems"])
	}
}
