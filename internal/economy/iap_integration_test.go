package economy

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"testing"
	"time"

	"ultimate-game-server/internal/database"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"go.uber.org/zap"
)

func TestIAP_HuaweiRSA_RejectsBadSignature(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	pubDER, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	cfg := IAPConfig{HuaweiPublicKey: base64.StdEncoding.EncodeToString(pubDER)}
	data := `{"productId":"sku","orderId":"ord-1","purchaseTime":1700000000000}`
	_, err = ValidatePurchaseHuawei(context.Background(), nil, cfg, uuid.New().String(), data, "not-a-signature", false)
	if err == nil {
		t.Fatal("expected signature failure")
	}
}

func TestIAP_HuaweiRSA_AcceptsValidSignature_PersistFalse(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	pubDER, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	data := `{"productId":"sku","orderId":"ord-rsa-1","purchaseTime":1700000000000}`
	hash := sha256.Sum256([]byte(data))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, hash[:])
	if err != nil {
		t.Fatal(err)
	}
	cfg := IAPConfig{HuaweiPublicKey: base64.StdEncoding.EncodeToString(pubDER)}
	vp, err := ValidatePurchaseHuawei(context.Background(), nil, cfg, uuid.New().String(), data, base64.StdEncoding.EncodeToString(sig), false)
	if err != nil {
		t.Fatal(err)
	}
	if vp.ProductID != "sku" || vp.TransactionID != "ord-rsa-1" {
		t.Fatalf("unexpected purchase: %+v", vp)
	}
}

func TestIAP_GoogleCredsWithoutPackage_Error(t *testing.T) {
	cfg := IAPConfig{
		GoogleClientEmail: "svc@example.com",
		GooglePrivateKey:  "-----BEGIN PRIVATE KEY-----\nMIIE\n-----END PRIVATE KEY-----",
	}
	_, err := ValidatePurchaseGoogle(context.Background(), nil, cfg, uuid.New().String(), "sku", "plain-token", false)
	if err == nil {
		t.Fatal("expected package name error when credentials set without package")
	}
}

func TestIAP_ListSubscriptions_Cursor_Integration(t *testing.T) {
	ctx := context.Background()
	postgresContainer, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("ultimate_game_db"),
		postgres.WithUsername("game_admin"),
		postgres.WithPassword("game_password"),
	)
	if err != nil {
		t.Fatalf("postgres: %v", err)
	}
	defer func() { _ = postgresContainer.Terminate(ctx) }()

	dsn, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	logger := zap.NewNop()
	pool, err := database.ConnectWithBackoff(ctx, logger, database.Config{
		DSN: dsn, MaxOpenConns: 5, MaxRetries: 5, RetryDelay: 500 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := database.RunMigrations(ctx, logger, pool); err != nil {
		t.Fatal(err)
	}

	userID := uuid.New().String()
	_, err = pool.Exec(ctx, "INSERT INTO users (id, username, password, display_name) VALUES ($1, 'iap_cursor', 'pass', 'IAP')", userID)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		receipt := fmt.Sprintf(`{"product_id":"sub_%d","transaction_id":"otx_%d","expire_time":"%s","environment":1}`,
			i, i, time.Now().UTC().Add(48*time.Hour).Format(time.RFC3339))
		if _, err := ValidateSubscriptionApple(ctx, pool, IAPConfig{}, userID, receipt, true); err != nil {
			t.Fatalf("upsert sub %d: %v", i, err)
		}
	}

	page1, err := ListSubscriptions(ctx, pool, userID, 2, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(page1.Subscriptions) != 2 {
		t.Fatalf("expected 2, got %d", len(page1.Subscriptions))
	}
	if page1.Cursor == "" {
		t.Fatal("expected next cursor")
	}
	page2, err := ListSubscriptions(ctx, pool, userID, 2, page1.Cursor)
	if err != nil {
		t.Fatal(err)
	}
	if len(page2.Subscriptions) != 1 {
		t.Fatalf("expected 1 remaining, got %d", len(page2.Subscriptions))
	}
}

func TestResolveLoadIAPConfigFromEnv(t *testing.T) {
	t.Setenv("APPLE_SHARED_PASSWORD", "pw")
	t.Setenv("GOOGLE_IAP_PACKAGE_NAME", "com.example")
	cfg := LoadIAPConfigFromEnv()
	if cfg.AppleSharedPassword != "pw" || cfg.GooglePackageName != "com.example" {
		t.Fatalf("unexpected cfg: %+v", cfg)
	}
}
