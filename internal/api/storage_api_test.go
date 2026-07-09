//go:build integration

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"ultimate-game-server/internal/auth"
	"ultimate-game-server/internal/database"
	"ultimate-game-server/internal/runtime"
	"ultimate-game-server/internal/api/storagepb"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

func TestStorageAPI_Integration(t *testing.T) {
	ctx := context.Background()

	// 1. Spin up PostgreSQL container
	postgresContainer, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("ultimate_game_db"),
		postgres.WithUsername("game_admin"),
		postgres.WithPassword("game_password"),
	)
	require.NoError(t, err)
	defer func() {
		err := postgresContainer.Terminate(ctx)
		assert.NoError(t, err)
	}()

	dsn, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	logger := zap.NewNop()
	dbCfg := database.Config{
		DSN:          dsn,
		MaxOpenConns: 5,
		MaxRetries:   5,
		RetryDelay:   100 * time.Millisecond,
	}

	pool, err := database.ConnectWithBackoff(ctx, logger, dbCfg)
	require.NoError(t, err)
	defer pool.Close()

	// Run migrations
	err = database.RunMigrations(ctx, logger, pool)
	require.NoError(t, err)

	// Seed system user to resolve constraints
	_, err = pool.Exec(ctx, `INSERT INTO users (id, username, display_name) VALUES ('00000000-0000-0000-0000-000000000000', 'system', 'System User') ON CONFLICT (id) DO NOTHING`)
	require.NoError(t, err)

	// Register a test user
	_, err = auth.RegisterEmail(ctx, pool, "test_player", "player@game.com", "Password123", "Test Player")
	require.NoError(t, err)

	// 2. Start API Server
	serverCfg := Config{
		HTTPAddr:        "127.0.0.1:17360",
		GRPCAddr:        "127.0.0.1:17359",
		JWTSecret:       []byte("super_secret_signing_key_at_least_32_bytes_long_1234567"),
		JWTExpiry:       10 * time.Minute,
		RateLimitMax:    100,
		RateLimitRefill: 10,
	}

	srv, err := NewServer(logger, serverCfg, pool)
	require.NoError(t, err)

	err = srv.Start(ctx)
	require.NoError(t, err)
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Stop(shutdownCtx)
	}()

	time.Sleep(100 * time.Millisecond)

	// 3. Login to get JWT
	loginBody := map[string]interface{}{
		"email":    "player@game.com",
		"password": "Password123",
	}
	loginBytes, _ := json.Marshal(loginBody)
	resp, err := http.Post("http://127.0.0.1:17360/v2/account/authenticate/email", "application/json", bytes.NewBuffer(loginBytes))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var authResp struct {
		AccessToken string `json:"access_token"`
		UserID      string `json:"user_id"`
	}
	err = json.NewDecoder(resp.Body).Decode(&authResp)
	require.NoError(t, err)
	resp.Body.Close()

	bearerToken := authResp.AccessToken

	// ==================== REST INTEGRATION TESTS ====================

	// Write objects
	writeBody := map[string]interface{}{
		"objects": []map[string]interface{}{
			{
				"collection":       "inventory",
				"key":              "weapons",
				"value":            map[string]interface{}{"swords": []string{"excalibur"}},
				"permission_read":  1,
				"permission_write": 1,
			},
		},
	}
	writeBytes, _ := json.Marshal(writeBody)
	req, err := http.NewRequest("POST", "http://127.0.0.1:17360/v2/storage", bytes.NewBuffer(writeBytes))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+bearerToken)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err = client.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var writeAck struct {
		Acks []struct {
			Collection string `json:"collection"`
			Key        string `json:"key"`
			UserID     string `json:"user_id"`
			Version    string `json:"version"`
		} `json:"acks"`
	}
	err = json.NewDecoder(resp.Body).Decode(&writeAck)
	require.NoError(t, err)
	resp.Body.Close()
	require.Len(t, writeAck.Acks, 1)
	assert.Equal(t, "weapons", writeAck.Acks[0].Key)
	version := writeAck.Acks[0].Version

	// Read objects
	readBody := map[string]interface{}{
		"object_ids": []map[string]interface{}{
			{
				"collection": "inventory",
				"key":        "weapons",
				"user_id":    authResp.UserID,
			},
		},
	}
	readBytes, _ := json.Marshal(readBody)
	req, err = http.NewRequest("POST", "http://127.0.0.1:17360/v2/storage/read", bytes.NewBuffer(readBytes))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+bearerToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err = client.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var readRes struct {
		Objects []struct {
			Collection string                 `json:"collection"`
			Key        string                 `json:"key"`
			UserID     string                 `json:"user_id"`
			Value      map[string]interface{} `json:"value"`
			Version    string                 `json:"version"`
		} `json:"objects"`
	}
	err = json.NewDecoder(resp.Body).Decode(&readRes)
	require.NoError(t, err)
	resp.Body.Close()
	require.Len(t, readRes.Objects, 1)
	assert.Equal(t, version, readRes.Objects[0].Version)

	// List objects
	req, err = http.NewRequest("GET", "http://127.0.0.1:17360/v2/storage/inventory?user_id="+authResp.UserID, nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+bearerToken)

	resp, err = client.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var listRes struct {
		Objects []struct {
			Collection string `json:"collection"`
			Key        string `json:"key"`
		} `json:"objects"`
	}
	err = json.NewDecoder(resp.Body).Decode(&listRes)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Len(t, listRes.Objects, 1)

	// Delete objects (with OCC version conflict)
	deleteBodyConflict := map[string]interface{}{
		"object_ids": []map[string]interface{}{
			{
				"collection": "inventory",
				"key":        "weapons",
				"version":    "wrong_version",
			},
		},
	}
	deleteBytesConflict, _ := json.Marshal(deleteBodyConflict)
	req, err = http.NewRequest("POST", "http://127.0.0.1:17360/v2/storage/delete", bytes.NewBuffer(deleteBytesConflict))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+bearerToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err = client.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusConflict, resp.StatusCode)
	resp.Body.Close()

	// Delete objects (success)
	deleteBody := map[string]interface{}{
		"object_ids": []map[string]interface{}{
			{
				"collection": "inventory",
				"key":        "weapons",
				"version":    version,
			},
		},
	}
	deleteBytes, _ := json.Marshal(deleteBody)
	req, err = http.NewRequest("POST", "http://127.0.0.1:17360/v2/storage/delete", bytes.NewBuffer(deleteBytes))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+bearerToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err = client.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	// ==================== gRPC INTEGRATION TESTS ====================

	conn, err := grpc.NewClient("127.0.0.1:17359", grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	gClient := storagepb.NewStorageServiceClient(conn)

	// Build authenticated context
	grpcCtx := metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer "+bearerToken))

	// gRPC Write
	gWriteResp, err := gClient.WriteStorageObjects(grpcCtx, &storagepb.WriteStorageObjectsRequest{
		Objects: []*storagepb.WriteStorageObjectsRequest_WriteOp{
			{
				Collection:      "inventory",
				Key:             "armor",
				Value:           `{"type":"plate"}`,
				PermissionRead:  1,
				PermissionWrite: 1,
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, gWriteResp.Acks, 1)
	assert.Equal(t, "armor", gWriteResp.Acks[0].Key)
	gVersion := gWriteResp.Acks[0].Version

	// gRPC Read
	gReadResp, err := gClient.ReadStorageObjects(grpcCtx, &storagepb.ReadStorageObjectsRequest{
		ObjectIds: []*storagepb.ReadStorageObjectsRequest_ReadOp{
			{
				Collection: "inventory",
				Key:        "armor",
				UserId:     authResp.UserID,
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, gReadResp.Objects, 1)
	assert.Equal(t, gVersion, gReadResp.Objects[0].Version)

	// gRPC List
	gListResp, err := gClient.ListStorageObjects(grpcCtx, &storagepb.ListStorageObjectsRequest{
		Collection: "inventory",
		UserId:     authResp.UserID,
		Limit:      10,
	})
	require.NoError(t, err)
	assert.Len(t, gListResp.Objects, 1)

	// gRPC Delete
	_, err = gClient.DeleteStorageObjects(grpcCtx, &storagepb.DeleteStorageObjectsRequest{
		ObjectIds: []*storagepb.DeleteStorageObjectsRequest_DeleteOp{
			{
				Collection: "inventory",
				Key:        "armor",
				Version:    gVersion,
			},
		},
	})
	require.NoError(t, err)

	// ==================== RUNTIME MODULE TESTS ====================

	goLogger := &testGoLogger{}
	goNK := runtime.NewGoRuntimeModule(pool, goLogger)

	// Runtime StorageWrite
	wAcks, err := goNK.StorageWrite(ctx, []*runtime.StorageWrite{
		{
			Collection:      "inventory",
			Key:             "boots",
			UserID:          authResp.UserID,
			Value:           `{"speed":10}`,
			PermissionRead:  1,
			PermissionWrite: 1,
		},
	})
	require.NoError(t, err)
	require.Len(t, wAcks, 1)
	rtVersion := wAcks[0].Version

	// Runtime StorageRead
	rObjs, err := goNK.StorageRead(ctx, []*runtime.StorageRead{
		{
			Collection: "inventory",
			Key:        "boots",
			UserID:     authResp.UserID,
		},
	})
	require.NoError(t, err)
	require.Len(t, rObjs, 1)
	assert.Equal(t, rtVersion, rObjs[0].Version)

	// Runtime StorageDelete
	err = goNK.StorageDelete(ctx, []*runtime.StorageDelete{
		{
			Collection: "inventory",
			Key:        "boots",
			UserID:     authResp.UserID,
			Version:    rtVersion,
		},
	})
	require.NoError(t, err)

	// Verify it's deleted
	rObjsDeleted, err := goNK.StorageRead(ctx, []*runtime.StorageRead{
		{
			Collection: "inventory",
			Key:        "boots",
			UserID:     authResp.UserID,
		},
	})
	require.NoError(t, err)
	assert.Empty(t, rObjsDeleted)
}

type testGoLogger struct{}

func (l *testGoLogger) Debug(format string, args ...interface{}) {}
func (l *testGoLogger) Info(format string, args ...interface{})  {}
func (l *testGoLogger) Warn(format string, args ...interface{})  {}
func (l *testGoLogger) Error(format string, args ...interface{}) {}
