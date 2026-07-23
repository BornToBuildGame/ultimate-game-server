package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"ultimate-game-server/internal/economy"
	"ultimate-game-server/internal/leaderboard"
	"ultimate-game-server/internal/notification"
	"ultimate-game-server/internal/social"
	"ultimate-game-server/internal/storage"
	"ultimate-game-server/internal/tournament"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type MatchRegistry interface {
	CreateAndRegisterMatch(ctx context.Context, matchID string, module string, params map[string]interface{}) error
}

// PartyLister lists discoverable parties for runtime party_list.
type PartyLister interface {
	List(limit int, open *bool, showHidden bool, query, cursor string) ([]*PartyListEntry, string, error)
}

type GoRuntimeModule struct {
	dbPool       *pgxpool.Pool
	logger       Logger
	registry     MatchRegistry
	partyLister  PartyLister
}

func NewGoRuntimeModule(dbPool *pgxpool.Pool, logger Logger) *GoRuntimeModule {
	return &GoRuntimeModule{
		dbPool: dbPool,
		logger: logger,
	}
}

func (m *GoRuntimeModule) SetMatchRegistry(reg MatchRegistry) {
	m.registry = reg
}

func (m *GoRuntimeModule) SetPartyLister(l PartyLister) {
	m.partyLister = l
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
	// Format matches match.NewAuthoritativeMatchID — cannot import match (cycle via runtime).
	node := os.Getenv("UGE_NODE_ID")
	if node == "" {
		if h, err := os.Hostname(); err == nil && h != "" {
			node = h
		} else {
			node = "node-local"
		}
	}
	matchID := uuid.New().String() + "." + node
	if m.registry != nil {
		err := m.registry.CreateAndRegisterMatch(ctx, matchID, module, params)
		if err != nil {
			return "", fmt.Errorf("failed to create and register match: %w", err)
		}
	}
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

func (m *GoRuntimeModule) LeaderboardList(ctx context.Context, limit int, cursor string) ([]*Leaderboard, string, error) {
	list, next, err := leaderboard.ListLeaderboards(ctx, m.dbPool, limit, cursor)
	if err != nil {
		return nil, "", err
	}
	return toRuntimeLeaderboards(list), next, nil
}

func (m *GoRuntimeModule) LeaderboardsGetId(ctx context.Context, ids []string) ([]*Leaderboard, error) {
	list, err := leaderboard.LeaderboardsGetId(ctx, m.dbPool, ids)
	if err != nil {
		return nil, err
	}
	return toRuntimeLeaderboards(list), nil
}

func (m *GoRuntimeModule) LeaderboardRanksDisable(ctx context.Context, id string) error {
	return leaderboard.DisableRanks(ctx, m.dbPool, id, false)
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

func (m *GoRuntimeModule) TournamentList(ctx context.Context, categoryStart, categoryEnd int, startTime, endTime int64, limit int, cursor string, active bool) ([]*TournamentView, string, error) {
	st, et := time.Time{}, time.Time{}
	if startTime > 0 {
		st = time.Unix(startTime, 0).UTC()
	}
	if endTime > 0 {
		et = time.Unix(endTime, 0).UTC()
	}
	list, next, err := tournament.ListTournaments(ctx, m.dbPool, categoryStart, categoryEnd, st, et, limit, cursor, active)
	if err != nil {
		return nil, "", err
	}
	out := make([]*TournamentView, len(list))
	for i, v := range list {
		out[i] = toRuntimeTournamentView(v)
	}
	return out, next, nil
}

func (m *GoRuntimeModule) TournamentsGetId(ctx context.Context, ids []string) ([]*Leaderboard, error) {
	list, err := leaderboard.TournamentsGetId(ctx, m.dbPool, ids)
	if err != nil {
		return nil, err
	}
	return toRuntimeLeaderboards(list), nil
}

func (m *GoRuntimeModule) TournamentRanksDisable(ctx context.Context, id string) error {
	return leaderboard.DisableRanks(ctx, m.dbPool, id, true)
}

func (m *GoRuntimeModule) TournamentJoin(ctx context.Context, id, ownerID, username string) error {
	return tournament.JoinTournament(ctx, m.dbPool, id, ownerID, username)
}

func (m *GoRuntimeModule) leaderboardRecordsList(ctx context.Context, id string, ownerIDs []string, limit int, cursor string, expiry int64) ([]*LeaderboardRecord, string, string, error) {
	expiryTime := time.Time{}
	if expiry != 0 {
		expiryTime = leaderboard.ResolveExpiryTime(expiry)
	}
	if len(ownerIDs) > 0 {
		recs, err := leaderboard.GetOwnerRecords(ctx, m.dbPool, id, ownerIDs, expiryTime)
		if err != nil {
			return nil, "", "", err
		}
		return toRuntimeRecords(recs), "", "", nil
	}
	recs, next, prev, err := leaderboard.GetLeaderboardRecordsPaged(ctx, m.dbPool, id, limit, cursor, expiryTime, expiry)
	if err != nil {
		return nil, "", "", err
	}
	return toRuntimeRecords(recs), next, prev, nil
}

func (m *GoRuntimeModule) LeaderboardRecordsList(ctx context.Context, id string, ownerIDs []string, limit int, cursor string, expiry int64) ([]*LeaderboardRecord, string, string, error) {
	return m.leaderboardRecordsList(ctx, id, ownerIDs, limit, cursor, expiry)
}

func (m *GoRuntimeModule) LeaderboardRecordsAroundOwner(ctx context.Context, id, ownerID string, limit int, expiry int64) ([]*LeaderboardRecord, error) {
	expiryTime := time.Time{}
	if expiry != 0 {
		expiryTime = leaderboard.ResolveExpiryTime(expiry)
	}
	recs, err := leaderboard.GetLeaderboardRecordsAroundPlayer(ctx, m.dbPool, nil, id, ownerID, limit, expiryTime)
	if err != nil {
		return nil, err
	}
	return toRuntimeRecords(recs), nil
}

func (m *GoRuntimeModule) LeaderboardRecordsHaystack(ctx context.Context, id, ownerID string, limit int, cursor string, expiry int64) ([]*LeaderboardRecord, string, string, error) {
	recs, next, prev, err := leaderboard.RecordsHaystack(ctx, m.dbPool, id, ownerID, limit, cursor, expiry)
	if err != nil {
		return nil, "", "", err
	}
	return toRuntimeRecords(recs), next, prev, nil
}

func (m *GoRuntimeModule) LeaderboardRecordsListCursorFromRank(ctx context.Context, leaderboardID string, rank, expiry int64) (string, error) {
	return leaderboard.RecordsListCursorFromRank(ctx, m.dbPool, leaderboardID, rank, expiry)
}

func (m *GoRuntimeModule) LeaderboardRecordDelete(ctx context.Context, id, ownerID string) error {
	return leaderboard.DeleteRecord(ctx, m.dbPool, id, ownerID)
}

func (m *GoRuntimeModule) TournamentRecordWrite(ctx context.Context, id, ownerID, username string, score, subscore int64, metadata map[string]interface{}) (*LeaderboardRecord, error) {
	return m.LeaderboardRecordWrite(ctx, id, ownerID, username, score, subscore, metadata)
}

func (m *GoRuntimeModule) TournamentRecordsList(ctx context.Context, id string, ownerIDs []string, limit int, cursor string, expiry int64) ([]*LeaderboardRecord, string, string, error) {
	return m.leaderboardRecordsList(ctx, id, ownerIDs, limit, cursor, expiry)
}

func (m *GoRuntimeModule) TournamentRecordsAroundOwner(ctx context.Context, id, ownerID string, limit int, expiry int64) ([]*LeaderboardRecord, error) {
	return m.LeaderboardRecordsAroundOwner(ctx, id, ownerID, limit, expiry)
}

func (m *GoRuntimeModule) TournamentRecordsHaystack(ctx context.Context, id, ownerID string, limit int, cursor string, expiry int64) ([]*LeaderboardRecord, string, string, error) {
	return m.LeaderboardRecordsHaystack(ctx, id, ownerID, limit, cursor, expiry)
}

func (m *GoRuntimeModule) TournamentRecordDelete(ctx context.Context, id, ownerID string) error {
	return leaderboard.DeleteRecord(ctx, m.dbPool, id, ownerID)
}

func (m *GoRuntimeModule) TournamentAddAttempt(ctx context.Context, id, ownerID string, count int) error {
	return tournament.AddAttempt(ctx, m.dbPool, id, ownerID, count)
}

func (m *GoRuntimeModule) FriendsList(ctx context.Context, userID string, limit int, state *int, cursor string) ([]*FriendEdge, string, error) {
	filter := 0
	if state != nil {
		filter = *state
	}
	list, next, err := social.ListFriends(ctx, m.dbPool, userID, filter, limit, cursor)
	if err != nil {
		return nil, "", err
	}
	out := make([]*FriendEdge, len(list))
	for i, f := range list {
		out[i] = &FriendEdge{
			UserID: f.User.ID, Username: f.User.Username, DisplayName: f.User.DisplayName,
			State: f.State, UpdateTime: f.UpdateTime, Metadata: f.Metadata,
		}
	}
	return out, next, nil
}

func (m *GoRuntimeModule) FriendsAdd(ctx context.Context, userID string, ids, usernames []string, metadata map[string]any) error {
	resolved, err := social.ResolveUserIDs(ctx, m.dbPool, ids, usernames)
	if err != nil {
		return err
	}
	meta := "{}"
	if metadata != nil {
		b, err := json.Marshal(metadata)
		if err != nil {
			return err
		}
		meta = string(b)
	}
	notifier := apiFriendNotifier{m: m}
	for _, dest := range resolved {
		if err := social.AddFriendWithOpts(ctx, m.dbPool, userID, dest, meta, social.DefaultConfig(), notifier); err != nil {
			return err
		}
	}
	return nil
}

func (m *GoRuntimeModule) FriendsDelete(ctx context.Context, userID string, ids, usernames []string) error {
	resolved, err := social.ResolveUserIDs(ctx, m.dbPool, ids, usernames)
	if err != nil {
		return err
	}
	notifier := apiFriendNotifier{m: m}
	for _, dest := range resolved {
		if err := social.DeleteFriend(ctx, m.dbPool, userID, dest, notifier); err != nil {
			return err
		}
	}
	return nil
}

func (m *GoRuntimeModule) FriendsBlock(ctx context.Context, userID string, ids, usernames []string) error {
	resolved, err := social.ResolveUserIDs(ctx, m.dbPool, ids, usernames)
	if err != nil {
		return err
	}
	for _, dest := range resolved {
		if err := social.BlockUser(ctx, m.dbPool, userID, dest); err != nil {
			return err
		}
	}
	return nil
}

func (m *GoRuntimeModule) FriendsOfFriendsList(ctx context.Context, userID string, limit int, cursor string) ([]*FriendOfFriendEdge, string, error) {
	list, next, err := social.ListFriendsOfFriends(ctx, m.dbPool, userID, limit, cursor)
	if err != nil {
		return nil, "", err
	}
	out := make([]*FriendOfFriendEdge, len(list))
	for i, f := range list {
		out[i] = &FriendOfFriendEdge{Referrer: f.Referrer, UserID: f.User.ID, Username: f.User.Username}
	}
	return out, next, nil
}

func (m *GoRuntimeModule) UsersGetFriendStatus(ctx context.Context, userID string, friendIDs []string) (map[string]int, error) {
	return social.UsersGetFriendStatus(ctx, m.dbPool, userID, friendIDs)
}

func (m *GoRuntimeModule) FriendMetadataUpdate(ctx context.Context, userID, friendID string, metadata map[string]any) error {
	return social.FriendMetadataUpdate(ctx, m.dbPool, userID, friendID, metadata)
}

func (m *GoRuntimeModule) PartyList(ctx context.Context, limit int, open *bool, showHidden bool, query, cursor string) ([]*PartyListEntry, string, error) {
	if m.partyLister == nil {
		return []*PartyListEntry{}, "", nil
	}
	return m.partyLister.List(limit, open, showHidden, query, cursor)
}

type apiFriendNotifier struct {
	m *GoRuntimeModule
}

func (n apiFriendNotifier) Notify(ctx context.Context, userID, subject, content string, code int16, senderID string) error {
	var contentMap map[string]interface{}
	_ = json.Unmarshal([]byte(content), &contentMap)
	return n.m.NotificationSend(ctx, userID, subject, contentMap, int(code), senderID, true)
}

func toRuntimeRecords(recs []*leaderboard.LeaderboardRecord) []*LeaderboardRecord {
	out := make([]*LeaderboardRecord, len(recs))
	for i, r := range recs {
		out[i] = &LeaderboardRecord{
			LeaderboardID: r.LeaderboardID, OwnerID: r.OwnerID, Username: r.Username,
			Score: r.Score, Subscore: r.Subscore, NumScore: r.NumScore, MaxNumScore: r.MaxNumScore,
			Metadata: r.Metadata, CreateTime: r.CreateTime, UpdateTime: r.UpdateTime,
			ExpiryTime: r.ExpiryTime, Rank: r.Rank,
		}
	}
	return out
}

func toRuntimeLeaderboards(list []*leaderboard.Leaderboard) []*Leaderboard {
	out := make([]*Leaderboard, len(list))
	for i, lb := range list {
		out[i] = &Leaderboard{
			ID: lb.ID, Authoritative: lb.Authoritative, SortOrder: lb.SortOrder, Operator: lb.Operator,
			ResetSchedule: lb.ResetSchedule, Metadata: lb.Metadata, CreateTime: lb.CreateTime,
			Category: lb.Category, Description: lb.Description, Duration: lb.Duration, EndTime: lb.EndTime,
			JoinRequired: lb.JoinRequired, MaxSize: lb.MaxSize, MaxNumScore: lb.MaxNumScore,
			Title: lb.Title, Size: lb.Size, StartTime: lb.StartTime, EnableRanks: lb.EnableRanks,
		}
	}
	return out
}

func toRuntimeTournamentView(v *tournament.TournamentView) *TournamentView {
	return &TournamentView{
		Leaderboard: toRuntimeLeaderboards([]*leaderboard.Leaderboard{v.Leaderboard})[0],
		CanEnter:    v.CanEnter,
		StartActive: v.StartActive,
		EndActive:   v.EndActive,
		PrevReset:   v.PrevReset,
		NextReset:   v.NextReset,
	}
}
