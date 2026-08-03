//go:build integration

package economy

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/BornToBuildGame/ultimate-game-server/internal/database"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"go.uber.org/zap"
)

func TestWallet_Integration(t *testing.T) {
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
		MaxOpenConns: 10,
		MaxRetries:   5,
		RetryDelay:   500 * time.Millisecond,
	}

	pool, err := database.ConnectWithBackoff(ctx, logger, dbCfg)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}
	defer pool.Close()

	err = database.RunMigrations(ctx, logger, pool)
	if err != nil {
		t.Fatalf("failed to run database migrations: %v", err)
	}

	userID := uuid.New().String()
	initialWallet := `{"coins": 1000}`
	insertUser := `INSERT INTO users (id, username, email, password, display_name, wallet) VALUES ($1, $2, $3, $4, $5, $6)`
	_, err = pool.Exec(ctx, insertUser, userID, "wallet_user", "wallet@test.com", []byte("hash"), "Wallet User", initialWallet)
	if err != nil {
		t.Fatalf("failed to insert user: %v", err)
	}

	var wg sync.WaitGroup
	errorsChan := make(chan error, 10)
	changeset := map[string]int64{"coins": -100}
	metadata := map[string]interface{}{"reason": "purchase"}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := UpdateWallet(ctx, pool, userID, changeset, metadata, true)
			if err != nil {
				errorsChan <- err
			}
		}()
	}

	wg.Wait()
	close(errorsChan)

	for err := range errorsChan {
		t.Errorf("unexpected error during concurrent deductions: %v", err)
	}

	_, _, err = UpdateWallet(ctx, pool, userID, changeset, metadata, true)
	if !errors.Is(err, ErrInsufficientFunds) {
		t.Errorf("expected ErrInsufficientFunds, got: %v", err)
	}

	var walletBytes []byte
	err = pool.QueryRow(ctx, "SELECT wallet FROM users WHERE id = $1", userID).Scan(&walletBytes)
	if err != nil {
		t.Fatalf("failed to query user wallet: %v", err)
	}

	wallet := make(map[string]int64)
	_ = json.Unmarshal(walletBytes, &wallet)
	if balance := wallet["coins"]; balance != 0 {
		t.Errorf("expected final coins balance to be 0, got: %d", balance)
	}

	var ledgerCount int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM wallet_ledger WHERE user_id = $1", userID).Scan(&ledgerCount)
	if err != nil {
		t.Fatalf("failed to count ledger entries: %v", err)
	}
	if ledgerCount != 10 {
		t.Errorf("expected 10 ledger entries, got: %d", ledgerCount)
	}

	// Batch update + updateLedger=false
	user2 := uuid.New().String()
	_, err = pool.Exec(ctx, insertUser, user2, "wallet_user2", "wallet2@test.com", []byte("hash"), "W2", `{"coins": 50}`)
	if err != nil {
		t.Fatalf("insert user2: %v", err)
	}
	results, err := UpdateWallets(ctx, pool, []WalletUpdate{
		{UserID: userID, Changeset: map[string]int64{"coins": 10}},
		{UserID: user2, Changeset: map[string]int64{"coins": 5}},
	}, false)
	if err != nil {
		t.Fatalf("batch update: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	var ledgerAfter int
	_ = pool.QueryRow(ctx, "SELECT COUNT(*) FROM wallet_ledger WHERE user_id = $1", userID).Scan(&ledgerAfter)
	if ledgerAfter != 10 {
		t.Errorf("updateLedger=false should not add ledger rows, got %d", ledgerAfter)
	}

	list, err := ListWalletLedger(ctx, pool, userID, 5, "")
	if err != nil {
		t.Fatalf("list ledger: %v", err)
	}
	if len(list.Items) != 5 || list.NextCursor == "" {
		t.Errorf("expected 5 items with cursor, got %d cursor=%q", len(list.Items), list.NextCursor)
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

	userID := uuid.New().String()
	_, err = pool.Exec(ctx, "INSERT INTO users (id, username, password, display_name) VALUES ($1, 'iap_user', 'pass', 'IAP')", userID)
	if err != nil {
		t.Fatalf("failed to insert user: %v", err)
	}

	receipt := `{"product_id":"gems_pack_100","transaction_id":"tx_apple_123","environment":1}`
	vp, err := ValidatePurchaseApple(ctx, pool, IAPConfig{}, userID, receipt, true)
	if err != nil {
		t.Fatalf("failed to process Apple IAP: %v", err)
	}
	if vp.SeenBefore {
		t.Fatal("first purchase should not be seen_before")
	}

	vp2, err := ValidatePurchaseApple(ctx, pool, IAPConfig{}, userID, receipt, true)
	if err != nil {
		t.Fatalf("duplicate validate should succeed: %v", err)
	}
	if !vp2.SeenBefore {
		t.Fatal("expected seen_before=true on duplicate")
	}

	// persist=false should not create a new transaction row
	noPersist := `{"product_id":"gems_pack_100","transaction_id":"tx_nopersist","environment":1}`
	_, err = ValidatePurchaseApple(ctx, pool, IAPConfig{}, userID, noPersist, false)
	if err != nil {
		t.Fatalf("persist=false validate: %v", err)
	}
	var count int
	_ = pool.QueryRow(ctx, `SELECT COUNT(*) FROM purchase WHERE transaction_id = 'tx_nopersist'`).Scan(&count)
	if count != 0 {
		t.Errorf("persist=false should not write row, count=%d", count)
	}

	// No auto-credit
	var walletBytes []byte
	err = pool.QueryRow(ctx, "SELECT wallet FROM users WHERE id = $1", userID).Scan(&walletBytes)
	if err != nil {
		t.Fatalf("failed to query user wallet: %v", err)
	}
	var w map[string]int64
	_ = json.Unmarshal(walletBytes, &w)
	if w["gems"] != 0 {
		t.Errorf("expected no auto-credit gems, got: %d", w["gems"])
	}

	subReceipt := `{"product_id":"vip_month","transaction_id":"sub_orig_1","expire_time":"2099-01-01T00:00:00Z","environment":1}`
	sub, err := ValidateSubscriptionApple(ctx, pool, IAPConfig{}, userID, subReceipt, true)
	if err != nil {
		t.Fatalf("subscription: %v", err)
	}
	if !sub.Active {
		t.Error("expected active subscription")
	}
	sub2, err := ValidateSubscriptionApple(ctx, pool, IAPConfig{}, userID, subReceipt, true)
	if err != nil {
		t.Fatalf("subscription upsert: %v", err)
	}
	if !sub2.SeenBefore {
		t.Error("expected seen_before on subscription upsert")
	}
}
