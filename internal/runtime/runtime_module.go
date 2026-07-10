package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"ultimate-game-server/internal/economy"
	"ultimate-game-server/internal/leaderboard"
	"ultimate-game-server/internal/notification"
	"ultimate-game-server/internal/storage"
	"ultimate-game-server/internal/tournament"

	"github.com/google/uuid"
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

func (m *GoRuntimeModule) WalletUpdate(ctx context.Context, userID string, changeset map[string]int64, metadata map[string]interface{}, updateLedger bool) (map[string]int64, error) {
	return economy.UpdateWallet(ctx, m.dbPool, userID, changeset, metadata)
}

func (m *GoRuntimeModule) AccountGetId(ctx context.Context, userID string) (*Account, error) {
	query := `SELECT username, create_time, update_time FROM users WHERE id = $1`
	var username string
	var createTime, updateTime time.Time
	err := m.dbPool.QueryRow(ctx, query, userID).Scan(&username, &createTime, &updateTime)
	if err != nil {
		return nil, err
	}
	return &Account{
		ID:         userID,
		Username:   username,
		CreateTime: createTime,
		UpdateTime: updateTime,
	}, nil
}

func (m *GoRuntimeModule) LeaderboardRecordWrite(ctx context.Context, id, ownerID, username string, score, subscore int64, metadata map[string]interface{}) (*LeaderboardRecord, error) {
	metadataStr := "{}"
	if len(metadata) > 0 {
		bytes, _ := json.Marshal(metadata)
		metadataStr = string(bytes)
	}

	rec, err := leaderboard.SubmitScore(ctx, m.dbPool, nil, id, ownerID, username, score, subscore, metadataStr, false)
	if err != nil {
		return nil, err
	}

	return &LeaderboardRecord{
		LeaderboardID: rec.LeaderboardID,
		OwnerID:       rec.OwnerID,
		Username:      rec.Username,
		Score:         rec.Score,
		Subscore:      rec.Subscore,
		NumScore:      rec.NumScore,
		MaxNumScore:   rec.MaxNumScore,
		Metadata:      rec.Metadata,
		CreateTime:    rec.CreateTime,
		UpdateTime:    rec.UpdateTime,
		ExpiryTime:    rec.ExpiryTime,
		Rank:          rec.Rank,
	}, nil
}

func (m *GoRuntimeModule) NotificationSend(ctx context.Context, userID, subject string, content map[string]interface{}, code int, senderID string, persistent bool) error {
	contentBytes, _ := json.Marshal(content)
	notif := &notification.Notification{
		UserID:   userID,
		Subject:  subject,
		Content:  string(contentBytes),
		Code:     int16(code),
		SenderID: senderID,
	}
	return notification.CreateNotification(ctx, m.dbPool, notif)
}

func (m *GoRuntimeModule) MatchCreate(ctx context.Context, module string, params map[string]interface{}) (string, error) {
	matchID := uuid.New().String()
	return matchID, nil
}

func (m *GoRuntimeModule) LeaderboardCreate(ctx context.Context, id string, authoritative bool, sortOrder int, operator int, resetSchedule string, metadata map[string]interface{}, enableRanks bool) error {
	metadataStr := "{}"
	if len(metadata) > 0 {
		bytes, _ := json.Marshal(metadata)
		metadataStr = string(bytes)
	}
	lb := &leaderboard.Leaderboard{
		ID:            id,
		Authoritative: authoritative,
		SortOrder:     sortOrder,
		Operator:      operator,
		ResetSchedule: resetSchedule,
		Metadata:      metadataStr,
		EnableRanks:   enableRanks,
	}
	return leaderboard.CreateLeaderboard(ctx, m.dbPool, lb)
}

func (m *GoRuntimeModule) LeaderboardDelete(ctx context.Context, id string) error {
	return leaderboard.DeleteLeaderboard(ctx, m.dbPool, id)
}

func (m *GoRuntimeModule) TournamentCreate(ctx context.Context, id string, authoritative bool, sortOrder, operator int, resetSchedule string, metadata map[string]interface{}, title, description string, category int, startTime, endTime int64, duration, maxSize, maxNumScore int, joinRequired, enableRanks bool) error {
	metadataStr := "{}"
	if len(metadata) > 0 {
		bytes, _ := json.Marshal(metadata)
		metadataStr = string(bytes)
	}
	lb := &leaderboard.Leaderboard{
		ID:            id,
		Authoritative: authoritative,
		SortOrder:     sortOrder,
		Operator:      operator,
		ResetSchedule: resetSchedule,
		Metadata:      metadataStr,
		Title:         title,
		Description:   description,
		Category:      category,
		StartTime:     time.Unix(startTime, 0).UTC(),
		EndTime:       time.Unix(endTime, 0).UTC(),
		Duration:      duration,
		MaxSize:       maxSize,
		MaxNumScore:   maxNumScore,
		JoinRequired:  joinRequired,
		EnableRanks:   enableRanks,
	}
	return leaderboard.CreateLeaderboard(ctx, m.dbPool, lb)
}

func (m *GoRuntimeModule) TournamentDelete(ctx context.Context, id string) error {
	return leaderboard.DeleteLeaderboard(ctx, m.dbPool, id)
}

func (m *GoRuntimeModule) TournamentJoin(ctx context.Context, id, ownerID, username string) error {
	return tournament.JoinTournament(ctx, m.dbPool, id, ownerID, username)
}
