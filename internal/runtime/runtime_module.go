package runtime

import (
	"context"
	"fmt"
	"time"

	"ultimate-game-server/internal/storage"

	"github.com/jackc/pgx/v5/pgxpool"
)

type GoRuntimeModule struct {
	dbPool *pgxpool.Pool
	logger Logger
}

func NewGoRuntimeModule(dbPool *pgxpool.Pool, logger Logger) *GoRuntimeModule {
	return &GoRuntimeModule{
		dbPool: dbPool,
		logger: logger,
	}
}

func (m *GoRuntimeModule) StorageRead(ctx context.Context, reads []*StorageRead) ([]*StorageObject, error) {
	reqs := make([]storage.ReadRequest, len(reads))
	for i, r := range reads {
		reqs[i] = storage.ReadRequest{
			Collection: r.Collection,
			Key:        r.Key,
			UserID:     r.UserID,
		}
	}

	objs, err := storage.ReadStorageObjects(ctx, m.dbPool, reqs)
	if err != nil {
		return nil, fmt.Errorf("failed to read storage objects: %w", err)
	}

	res := make([]*StorageObject, len(objs))
	for i, o := range objs {
		res[i] = &StorageObject{
			Collection:      o.Collection,
			Key:             o.Key,
			UserID:          o.UserID,
			Value:           o.Value,
			Version:         o.Version,
			PermissionRead:  int32(o.Read),
			PermissionWrite: int32(o.Write),
			CreateTime:      time.Now(),
			UpdateTime:      time.Now(),
		}
	}
	return res, nil
}

func (m *GoRuntimeModule) StorageWrite(ctx context.Context, writes []*StorageWrite) ([]*StorageObjectAck, error) {
	objs := make([]*storage.StorageObject, len(writes))
	for i, w := range writes {
		objs[i] = &storage.StorageObject{
			Collection: w.Collection,
			Key:        w.Key,
			UserID:     w.UserID,
			Value:      w.Value,
			Version:    w.Version,
			Read:       int16(w.PermissionRead),
			Write:      int16(w.PermissionWrite),
		}
	}

	err := storage.WriteStorageObjects(ctx, m.dbPool, objs)
	if err != nil {
		return nil, fmt.Errorf("failed to write storage objects: %w", err)
	}

	res := make([]*StorageObjectAck, len(objs))
	for i, o := range objs {
		res[i] = &StorageObjectAck{
			Collection: o.Collection,
			Key:        o.Key,
			UserID:     o.UserID,
			Version:    o.Version,
			CreateTime: time.Now(),
			UpdateTime: time.Now(),
		}
	}
	return res, nil
}

func (m *GoRuntimeModule) StorageDelete(ctx context.Context, deletes []*StorageDelete) error {
	reqs := make([]storage.DeleteRequest, len(deletes))
	for i, d := range deletes {
		reqs[i] = storage.DeleteRequest{
			Collection: d.Collection,
			Key:        d.Key,
			UserID:     d.UserID,
			Version:    d.Version,
		}
	}

	err := storage.DeleteStorageObjects(ctx, m.dbPool, reqs)
	if err != nil {
		return fmt.Errorf("failed to delete storage objects: %w", err)
	}
	return nil
}

// Unimplemented operations returning error/default

func (m *GoRuntimeModule) WalletUpdate(ctx context.Context, userID string, changeset map[string]int64, metadata map[string]interface{}, updateLedger bool) error {
	return fmt.Errorf("wallet operations not implemented in runtime module")
}

func (m *GoRuntimeModule) AccountGetId(ctx context.Context, userID string) (*Account, error) {
	return nil, fmt.Errorf("account operations not implemented in runtime module")
}

func (m *GoRuntimeModule) LeaderboardRecordWrite(ctx context.Context, id, ownerID, username string, score, subscore int64, metadata map[string]interface{}) (*LeaderboardRecord, error) {
	return nil, fmt.Errorf("leaderboard operations not implemented in runtime module")
}

func (m *GoRuntimeModule) NotificationSend(ctx context.Context, userID, subject string, content map[string]interface{}, code int, senderID string, persistent bool) error {
	return fmt.Errorf("notification operations not implemented in runtime module")
}

func (m *GoRuntimeModule) MatchCreate(ctx context.Context, module string, params map[string]interface{}) (string, error) {
	return "", fmt.Errorf("match operations not implemented in runtime module")
}
