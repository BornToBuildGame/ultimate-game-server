//go:build integration

package storage

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"ultimate-game-server/internal/database"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"go.uber.org/zap"
)

func TestStorage_Integration(t *testing.T) {
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

	// 2. Insert test user
	userID := uuid.New().String()
	insertUser := `INSERT INTO users (id, username, email, password, display_name) VALUES ($1, $2, $3, $4, $5)`
	_, err = pool.Exec(ctx, insertUser, userID, "storage_user", "storage@test.com", []byte("hash"), "Storage User")
	if err != nil {
		t.Fatalf("failed to insert user: %v", err)
	}

	// 3. Write object initially
	obj := &StorageObject{
		Collection: "progress",
		Key:        "save_data",
		UserID:     userID,
		Value:      `{"level": 1, "exp": 10}`,
		Read:       1,
		Write:      1,
	}

	err = WriteStorageObjects(ctx, pool, []*StorageObject{obj})
	if err != nil {
		t.Fatalf("initial write failed: %v", err)
	}

	initialVersion := obj.Version
	if initialVersion == "" {
		t.Fatal("expected version to be calculated on write")
	}

	// 4. Try updating with an incorrect old version hash -> should return ErrOCCConflict
	objConflict := &StorageObject{
		Collection: "progress",
		Key:        "save_data",
		UserID:     userID,
		Value:      `{"level": 2, "exp": 20}`,
		Version:    "wrong_version_hash_123456",
		Read:       1,
		Write:      1,
	}

	err = WriteStorageObjects(ctx, pool, []*StorageObject{objConflict})
	if !errors.Is(err, ErrOCCConflict) {
		t.Errorf("expected ErrOCCConflict on version mismatch, got: %v", err)
	}

	// 5. Update with the correct version hash -> should succeed
	objSuccess := &StorageObject{
		Collection: "progress",
		Key:        "save_data",
		UserID:     userID,
		Value:      `{"level": 2, "exp": 20}`,
		Version:    initialVersion,
		Read:       1,
		Write:      1,
	}

	err = WriteStorageObjects(ctx, pool, []*StorageObject{objSuccess})
	if err != nil {
		t.Fatalf("write with correct version failed: %v", err)
	}

	// 6. Read back and verify values
	readReqs := []ReadRequest{
		{Collection: "progress", Key: "save_data", UserID: userID},
	}
	results, err := ReadStorageObjects(ctx, pool, readReqs)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 storage object, got: %d", len(results))
	}

	readObj := results[0]
	var valMap map[string]interface{}
	if err := json.Unmarshal([]byte(readObj.Value), &valMap); err != nil {
		t.Fatalf("failed to unmarshal read value: %v", err)
	}
	if valMap["level"] != 2.0 || valMap["exp"] != 20.0 {
		t.Errorf("unexpected read value fields: %v", valMap)
	}
	if readObj.Version != objSuccess.Version {
		t.Errorf("version mismatch: %s != %s", readObj.Version, objSuccess.Version)
	}
}
