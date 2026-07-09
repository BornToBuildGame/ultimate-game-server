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
		DSN:          dsn,
		MaxOpenConns: 5,
		MaxRetries:   5,
		RetryDelay:   500 * time.Millisecond,
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

	// Verify system user seed migration (0008_seed_system_user) was executed
	var systemUserExists bool
	err = pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM users WHERE id = '00000000-0000-0000-0000-000000000000')").Scan(&systemUserExists)
	if err != nil {
		t.Fatalf("failed to check system user: %v", err)
	}
	if !systemUserExists {
		t.Error("expected system user 00000000-0000-0000-0000-000000000000 to be seeded in users table")
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

	// 7. Wildcard "*" write validation
	wildcardObj1 := &StorageObject{
		Collection: "progress",
		Key:        "wildcard_key",
		UserID:     userID,
		Value:      `{"data": "first"}`,
		Version:    "*",
		Read:       1,
		Write:      1,
	}

	// First write with version "*" (does not exist) -> should succeed
	err = WriteStorageObjects(ctx, pool, []*StorageObject{wildcardObj1})
	if err != nil {
		t.Fatalf("wildcard initial write failed: %v", err)
	}

	// Second write with version "*" (already exists) -> should fail with ErrOCCConflict
	wildcardObj2 := &StorageObject{
		Collection: "progress",
		Key:        "wildcard_key",
		UserID:     userID,
		Value:      `{"data": "second"}`,
		Version:    "*",
		Read:       1,
		Write:      1,
	}
	err = WriteStorageObjects(ctx, pool, []*StorageObject{wildcardObj2})
	if !errors.Is(err, ErrOCCConflict) {
		t.Errorf("expected ErrOCCConflict on wildcard overwrite, got: %v", err)
	}

	// 8. DeleteStorageObjects validation
	// Test conditional delete with wrong version -> should fail
	deleteReqConflict := DeleteRequest{
		Collection: "progress",
		Key:        "save_data",
		UserID:     userID,
		Version:    "wrong_version",
	}
	err = DeleteStorageObjects(ctx, pool, []DeleteRequest{deleteReqConflict})
	if !errors.Is(err, ErrOCCConflict) {
		t.Errorf("expected ErrOCCConflict on conditional delete version mismatch, got: %v", err)
	}

	// Test conditional delete with correct version -> should succeed
	deleteReqSuccess := DeleteRequest{
		Collection: "progress",
		Key:        "save_data",
		UserID:     userID,
		Version:    objSuccess.Version,
	}
	err = DeleteStorageObjects(ctx, pool, []DeleteRequest{deleteReqSuccess})
	if err != nil {
		t.Fatalf("conditional delete failed: %v", err)
	}

	// Verify actually deleted
	checkResults, err := ReadStorageObjects(ctx, pool, []ReadRequest{{Collection: "progress", Key: "save_data", UserID: userID}})
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if len(checkResults) != 0 {
		t.Error("expected storage object to be deleted, but still exists")
	}

	// Test unconditional delete -> should succeed
	deleteUnconditional := DeleteRequest{
		Collection: "progress",
		Key:        "wildcard_key",
		UserID:     userID,
	}
	err = DeleteStorageObjects(ctx, pool, []DeleteRequest{deleteUnconditional})
	if err != nil {
		t.Fatalf("unconditional delete failed: %v", err)
	}

	// 9. ListStorageObjects pagination validation
	// Write 5 test objects
	for i := 1; i <= 5; i++ {
		o := &StorageObject{
			Collection: "list_test",
			Key:        uuid.New().String(),
			UserID:     userID,
			Value:      `{"num":` + string(rune('0'+i)) + `}`,
			Read:       1,
			Write:      1,
		}
		err = WriteStorageObjects(ctx, pool, []*StorageObject{o})
		if err != nil {
			t.Fatalf("failed to write list test object %d: %v", i, err)
		}
	}

	// Page 1: limit = 2
	listRes1, cursor1, err := ListStorageObjects(ctx, pool, userID, "list_test", 2, "")
	if err != nil {
		t.Fatalf("List page 1 failed: %v", err)
	}
	if len(listRes1) != 2 {
		t.Errorf("expected 2 list results, got %d", len(listRes1))
	}
	if cursor1 == "" {
		t.Error("expected non-empty pagination cursor for next page")
	}

	// Page 2: limit = 2 with cursor1
	listRes2, cursor2, err := ListStorageObjects(ctx, pool, userID, "list_test", 2, cursor1)
	if err != nil {
		t.Fatalf("List page 2 failed: %v", err)
	}
	if len(listRes2) != 2 {
		t.Errorf("expected 2 list results on page 2, got %d", len(listRes2))
	}
	if cursor2 == "" {
		t.Error("expected non-empty pagination cursor on page 2")
	}

	// Page 3: limit = 2 with cursor2
	listRes3, cursor3, err := ListStorageObjects(ctx, pool, userID, "list_test", 2, cursor2)
	if err != nil {
		t.Fatalf("List page 3 failed: %v", err)
	}
	if len(listRes3) != 1 {
		t.Errorf("expected 1 list result on page 3 (last element), got %d", len(listRes3))
	}
	if cursor3 != "" {
		t.Errorf("expected empty cursor for last page, got %q", cursor3)
	}

	// 10. List public objects across multiple users (empty user_id)
	user2ID := uuid.New().String()
	_, err = pool.Exec(ctx, insertUser, user2ID, "storage_user2", "storage2@test.com", []byte("hash"), "Storage User 2")
	if err != nil {
		t.Fatalf("failed to insert user 2: %v", err)
	}

	// Write public record for user 1 (read = 2)
	oPub1 := &StorageObject{
		Collection: "pub_test",
		Key:        "key1",
		UserID:     userID,
		Value:      `{"owner": "user1"}`,
		Read:       2,
		Write:      1,
	}
	// Write public record for user 2 (read = 2)
	oPub2 := &StorageObject{
		Collection: "pub_test",
		Key:        "key2",
		UserID:     user2ID,
		Value:      `{"owner": "user2"}`,
		Read:       2,
		Write:      1,
	}
	// Write private record for user 1 (read = 1) -> should NOT be returned on public listing
	oPriv := &StorageObject{
		Collection: "pub_test",
		Key:        "key3-private",
		UserID:     userID,
		Value:      `{"owner": "user1-private"}`,
		Read:       1,
		Write:      1,
	}

	err = WriteStorageObjects(ctx, pool, []*StorageObject{oPub1, oPub2, oPriv})
	if err != nil {
		t.Fatalf("failed to write public/private test objects: %v", err)
	}

	// List with empty user_id -> should return public records key1 (user1) and key2 (user2)
	pubList, _, err := ListStorageObjects(ctx, pool, "", "pub_test", 10, "")
	if err != nil {
		t.Fatalf("list public storage objects failed: %v", err)
	}
	if len(pubList) != 2 {
		t.Errorf("expected 2 public records, got %d", len(pubList))
	}
	for _, o := range pubList {
		if o.Read != 2 {
			t.Errorf("expected public record only, got object with read = %d", o.Read)
		}
	}

	// 11. Search index Bleve validation
	err = InitSearchIndex()
	if err != nil {
		t.Fatalf("failed to initialize search index: %v", err)
	}

	searchObj1 := &StorageObject{
		Collection: "search_test",
		Key:        "char_mage",
		UserID:     userID,
		Value:      `{"class": "mage", "level": 15, "active": true}`,
		Read:       1,
		Write:      1,
	}
	searchObj2 := &StorageObject{
		Collection: "search_test",
		Key:        "char_warrior",
		UserID:     userID,
		Value:      `{"class": "warrior", "level": 5, "active": false}`,
		Read:       1,
		Write:      1,
	}

	err = WriteStorageObjects(ctx, pool, []*StorageObject{searchObj1, searchObj2})
	if err != nil {
		t.Fatalf("failed to write search test objects: %v", err)
	}

	// Search level >= 10
	searchRes, err := SearchStorageObjects(ctx, pool, "value.level:>=10", 10)
	if err != nil {
		t.Fatalf("search query failed: %v", err)
	}
	if len(searchRes) != 1 {
		t.Fatalf("expected 1 search result, got %d", len(searchRes))
	}
	if searchRes[0].Key != "char_mage" {
		t.Errorf("expected key to be 'char_mage', got %q", searchRes[0].Key)
	}

	// Search class:warrior
	searchResWarrior, err := SearchStorageObjects(ctx, pool, "value.class:warrior", 10)
	if err != nil {
		t.Fatalf("search warrior query failed: %v", err)
	}
	if len(searchResWarrior) != 1 {
		t.Fatalf("expected 1 search result, got %d", len(searchResWarrior))
	}
	if searchResWarrior[0].Key != "char_warrior" {
		t.Errorf("expected key to be 'char_warrior', got %q", searchResWarrior[0].Key)
	}
}
