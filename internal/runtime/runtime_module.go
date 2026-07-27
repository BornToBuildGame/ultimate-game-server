package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"ultimate-game-server/internal/auth"
	"ultimate-game-server/internal/chat"
	"ultimate-game-server/internal/cronexpr"
	"ultimate-game-server/internal/economy"
	"ultimate-game-server/internal/leaderboard"
	"ultimate-game-server/internal/notification"
	"ultimate-game-server/internal/fleet"
	"ultimate-game-server/internal/satori"
	"ultimate-game-server/internal/social"
	"ultimate-game-server/internal/storage"
	"ultimate-game-server/internal/tournament"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type MatchRegistry interface {
	CreateAndRegisterMatch(ctx context.Context, matchID string, module string, params map[string]interface{}) error
	ListMatches(ctx context.Context, limit int, authoritative bool, label string, minSize, maxSize int) ([]*MatchInfo, error)
	GetMatch(ctx context.Context, matchID string) (*MatchInfo, error)
	MatchSignal(ctx context.Context, matchID, data string) (string, error)
}

// PartyLister lists discoverable parties for runtime party_list.
type PartyLister interface {
	List(limit int, open *bool, showHidden bool, query, cursor string) ([]*PartyListEntry, string, error)
}

// StatusFollower follows/unfollows status presence for a session (runtime nk.status_follow).
type StatusFollower interface {
	StatusFollow(sessionID string, userIDs []string) error
	StatusUnfollow(sessionID string, userIDs []string) error
}

type GoRuntimeModule struct {
	dbPool         *pgxpool.Pool
	logger         Logger
	registry       MatchRegistry
	partyLister    PartyLister
	statusFollower StatusFollower
	streamManager  StreamManager
	rpcDispatcher  RPCDispatcherFunc
	storageIndex   *storage.BlugeStorageIndex
	satoriClient   *satori.Client
	fleetManager   fleet.Manager
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

func (m *GoRuntimeModule) SetStatusFollower(sf StatusFollower) {
	m.statusFollower = sf
}

func (m *GoRuntimeModule) SetStreamManager(sm StreamManager) {
	m.streamManager = sm
}

func (m *GoRuntimeModule) SetRPCDispatcher(fn RPCDispatcherFunc) {
	m.rpcDispatcher = fn
}

func (m *GoRuntimeModule) SetStorageIndex(idx *storage.BlugeStorageIndex) {
	m.storageIndex = idx
	storage.SetDefaultIndex(idx)
}

func (m *GoRuntimeModule) RegisterStorageIndex(name, collection, key string, fields, sortableFields []string, maxEntries int, indexOnly bool) error {
	if m.storageIndex == nil {
		return fmt.Errorf("storage index not configured")
	}
	return m.storageIndex.CreateIndex(storage.StorageIndexDefinition{
		Name: name, Collection: collection, Key: key,
		Fields: fields, SortableFields: sortableFields,
		MaxEntries: maxEntries, IndexOnly: indexOnly,
	})
}

func (m *GoRuntimeModule) RegisterStorageIndexFilter(indexName string, fn func(ctx context.Context, write *storage.StorageObject) (bool, error)) error {
	if m.storageIndex == nil {
		return fmt.Errorf("storage index not configured")
	}
	m.storageIndex.RegisterFilter(indexName, fn)
	return nil
}

func (m *GoRuntimeModule) SetSatoriClient(c *satori.Client) {
	m.satoriClient = c
}

func (m *GoRuntimeModule) GetSatori() satori.Satori {
	if m.satoriClient == nil {
		return nil
	}
	return m.satoriClient
}

func (m *GoRuntimeModule) SetFleetManager(fm fleet.Manager) {
	m.fleetManager = fm
}

func (m *GoRuntimeModule) GetFleetManager() fleet.Manager {
	return m.fleetManager
}

func (m *GoRuntimeModule) MultiUpdate(ctx context.Context, accountUpdates []*AccountUpdateParams, storageWrites []*StorageWrite, storageDeletes []*StorageDelete, walletUpdates []*WalletUpdateParams, updateLedger bool) ([]*StorageObjectAck, []*WalletUpdateResultView, error) {
	acc := make([]auth.AccountUpdateParams, 0, len(accountUpdates))
	for _, a := range accountUpdates {
		if a == nil {
			continue
		}
		acc = append(acc, auth.AccountUpdateParams{
			UserID: a.UserID, Username: a.Username, DisplayName: a.DisplayName,
			AvatarURL: a.AvatarURL, LangTag: a.LangTag, Location: a.Location,
			Timezone: a.Timezone, Metadata: a.Metadata,
		})
	}
	objs := make([]*storage.StorageObject, 0, len(storageWrites))
	for _, w := range storageWrites {
		if w == nil {
			continue
		}
		objs = append(objs, &storage.StorageObject{
			Collection: w.Collection, Key: w.Key, UserID: w.UserID, Value: w.Value, Version: w.Version,
			Read: int16(w.PermissionRead), Write: int16(w.PermissionWrite),
		})
	}
	dels := make([]storage.DeleteRequest, 0, len(storageDeletes))
	for _, d := range storageDeletes {
		if d == nil {
			continue
		}
		dels = append(dels, storage.DeleteRequest{Collection: d.Collection, Key: d.Key, UserID: d.UserID, Version: d.Version})
	}
	wallets := make([]economy.WalletUpdate, 0, len(walletUpdates))
	for _, u := range walletUpdates {
		if u == nil {
			continue
		}
		wallets = append(wallets, economy.WalletUpdate{UserID: u.UserID, Changeset: u.Changeset, Metadata: u.Metadata})
	}
	var idx storage.IndexWriter
	if m.storageIndex != nil {
		idx = m.storageIndex
	}
	acks, results, err := economy.MultiUpdate(ctx, m.dbPool, idx, economy.MultiUpdateParams{
		AccountUpdates: acc, StorageWrites: objs, StorageDeletes: dels,
		WalletUpdates: wallets, UpdateLedger: updateLedger,
	})
	if err != nil {
		return nil, nil, err
	}
	outAcks := make([]*StorageObjectAck, len(acks))
	for i, a := range acks {
		outAcks[i] = &StorageObjectAck{
			Collection: a.Collection, Key: a.Key, UserID: a.UserID, Version: a.Version,
			CreateTime: a.CreateTime, UpdateTime: a.UpdateTime,
		}
	}
	outWallets := make([]*WalletUpdateResultView, len(results))
	for i, r := range results {
		outWallets[i] = &WalletUpdateResultView{UserID: r.UserID, Updated: r.Updated, Previous: r.Previous}
	}
	return outAcks, outWallets, nil
}

func (m *GoRuntimeModule) StorageIndexList(ctx context.Context, callerID, indexName, query string, limit int, order []string, cursor string) ([]*StorageObject, string, error) {
	if m.storageIndex == nil {
		return nil, "", fmt.Errorf("storage index not configured")
	}
	objs, next, err := m.storageIndex.List(ctx, callerID, indexName, query, limit, order, cursor)
	if err != nil {
		return nil, "", err
	}
	out := make([]*StorageObject, len(objs))
	for i, o := range objs {
		out[i] = &StorageObject{
			Collection: o.Collection, Key: o.Key, UserID: o.UserID, Value: o.Value, Version: o.Version,
			CreateTime: o.CreateTime, UpdateTime: o.UpdateTime,
		}
	}
	return out, next, nil
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

	objs, err := storage.ReadStorageObjects(ctx, m.dbPool, uuid.Nil, reqs)
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
			CreateTime:      o.CreateTime,
			UpdateTime:      o.UpdateTime,
		}
	}
	return res, nil
}

func (m *GoRuntimeModule) StorageWrite(ctx context.Context, writes []*StorageWrite) ([]*StorageObjectAck, error) {
	objs := make([]*storage.StorageObject, len(writes))
	for i, w := range writes {
		uid := w.UserID
		if uid == "" {
			uid = uuid.Nil.String()
		}
		objs[i] = &storage.StorageObject{
			Collection: w.Collection,
			Key:        w.Key,
			UserID:     uid,
			Value:      w.Value,
			Version:    w.Version,
			Read:       int16(w.PermissionRead),
			Write:      int16(w.PermissionWrite),
		}
	}

	acks, err := storage.WriteStorageObjects(ctx, m.dbPool, true, objs)
	if err != nil {
		return nil, fmt.Errorf("failed to write storage objects: %w", err)
	}

	res := make([]*StorageObjectAck, len(acks))
	for i, a := range acks {
		res[i] = &StorageObjectAck{
			Collection: a.Collection,
			Key:        a.Key,
			UserID:     a.UserID,
			Version:    a.Version,
			CreateTime: a.CreateTime,
			UpdateTime: a.UpdateTime,
		}
	}
	return res, nil
}

func (m *GoRuntimeModule) StorageDelete(ctx context.Context, deletes []*StorageDelete) error {
	reqs := make([]storage.DeleteRequest, len(deletes))
	for i, d := range deletes {
		uid := d.UserID
		if uid == "" {
			uid = uuid.Nil.String()
		}
		reqs[i] = storage.DeleteRequest{
			Collection: d.Collection,
			Key:        d.Key,
			UserID:     uid,
			Version:    d.Version,
		}
	}

	err := storage.DeleteStorageObjects(ctx, m.dbPool, true, reqs)
	if err != nil {
		return fmt.Errorf("failed to delete storage objects: %w", err)
	}
	return nil
}

func (m *GoRuntimeModule) StorageList(ctx context.Context, callerID, userID, collection string, limit int, cursor string) ([]*StorageObject, string, error) {
	caller := uuid.Nil
	if callerID != "" {
		parsed, err := uuid.Parse(callerID)
		if err != nil {
			return nil, "", fmt.Errorf("invalid caller id: %w", err)
		}
		caller = parsed
	}
	var owner *uuid.UUID
	if userID != "" {
		parsed, err := uuid.Parse(userID)
		if err != nil {
			return nil, "", fmt.Errorf("invalid user id: %w", err)
		}
		owner = &parsed
	}
	if limit <= 0 {
		limit = 100
	}
	list, err := storage.ListStorageObjects(ctx, m.dbPool, caller, owner, collection, limit, cursor)
	if err != nil {
		return nil, "", err
	}
	res := make([]*StorageObject, len(list.Objects))
	for i, o := range list.Objects {
		res[i] = &StorageObject{
			Collection: o.Collection, Key: o.Key, UserID: o.UserID, Value: o.Value, Version: o.Version,
			PermissionRead: int32(o.Read), PermissionWrite: int32(o.Write),
			CreateTime: o.CreateTime, UpdateTime: o.UpdateTime,
		}
	}
	return res, list.Cursor, nil
}

func (m *GoRuntimeModule) StorageWriteRetry(ctx context.Context, reads []*StorageRead, updateFn func([]*StorageObject) ([]*StorageWrite, error), maxRetries int) ([]*StorageObjectAck, error) {
	reqs := make([]storage.ReadRequest, len(reads))
	for i, r := range reads {
		reqs[i] = storage.ReadRequest{Collection: r.Collection, Key: r.Key, UserID: r.UserID}
	}
	acks, err := storage.WriteStorageObjectsRetry(ctx, m.dbPool, reqs, func(objs []*storage.StorageObject) ([]*storage.StorageObject, error) {
		views := make([]*StorageObject, len(objs))
		for i, o := range objs {
			views[i] = &StorageObject{
				Collection: o.Collection, Key: o.Key, UserID: o.UserID, Value: o.Value, Version: o.Version,
				PermissionRead: int32(o.Read), PermissionWrite: int32(o.Write),
				CreateTime: o.CreateTime, UpdateTime: o.UpdateTime,
			}
		}
		writes, err := updateFn(views)
		if err != nil {
			return nil, err
		}
		out := make([]*storage.StorageObject, len(writes))
		for i, w := range writes {
			uid := w.UserID
			if uid == "" {
				uid = uuid.Nil.String()
			}
			out[i] = &storage.StorageObject{
				Collection: w.Collection, Key: w.Key, UserID: uid, Value: w.Value, Version: w.Version,
				Read: int16(w.PermissionRead), Write: int16(w.PermissionWrite),
			}
		}
		return out, nil
	}, maxRetries)
	if err != nil {
		return nil, err
	}
	res := make([]*StorageObjectAck, len(acks))
	for i, a := range acks {
		res[i] = &StorageObjectAck{
			Collection: a.Collection, Key: a.Key, UserID: a.UserID, Version: a.Version,
			CreateTime: a.CreateTime, UpdateTime: a.UpdateTime,
		}
	}
	return res, nil
}

// Unimplemented operations returning error/default

func (m *GoRuntimeModule) WalletUpdate(ctx context.Context, userID string, changeset map[string]int64, metadata map[string]interface{}, updateLedger bool) (map[string]int64, map[string]int64, error) {
	return economy.UpdateWallet(ctx, m.dbPool, userID, changeset, metadata, updateLedger)
}

func (m *GoRuntimeModule) WalletsUpdate(ctx context.Context, updates []*WalletUpdateParams, updateLedger bool) ([]*WalletUpdateResultView, error) {
	in := make([]economy.WalletUpdate, 0, len(updates))
	for _, u := range updates {
		if u == nil {
			continue
		}
		in = append(in, economy.WalletUpdate{UserID: u.UserID, Changeset: u.Changeset, Metadata: u.Metadata})
	}
	results, err := economy.UpdateWallets(ctx, m.dbPool, in, updateLedger)
	if err != nil {
		return nil, err
	}
	out := make([]*WalletUpdateResultView, 0, len(results))
	for _, r := range results {
		out = append(out, &WalletUpdateResultView{UserID: r.UserID, Updated: r.Updated, Previous: r.Previous})
	}
	return out, nil
}

func (m *GoRuntimeModule) WalletLedgerList(ctx context.Context, userID string, limit int, cursor string) ([]*WalletLedgerView, string, error) {
	list, err := economy.ListWalletLedger(ctx, m.dbPool, userID, limit, cursor)
	if err != nil {
		return nil, "", err
	}
	out := make([]*WalletLedgerView, 0, len(list.Items))
	for _, item := range list.Items {
		out = append(out, &WalletLedgerView{
			ID: item.ID, UserID: item.UserID, Changeset: item.Changeset, Metadata: item.Metadata,
			CreateTime: item.CreateTime, UpdateTime: item.UpdateTime,
		})
	}
	return out, list.NextCursor, nil
}

func (m *GoRuntimeModule) WalletLedgerUpdate(ctx context.Context, ledgerID, userID string, metadata map[string]interface{}) error {
	return economy.UpdateWalletLedgerMetadata(ctx, m.dbPool, ledgerID, userID, metadata)
}

func purchaseView(vp *economy.ValidatedPurchase) *ValidatedPurchaseView {
	if vp == nil {
		return nil
	}
	return &ValidatedPurchaseView{
		UserID: vp.UserID, ProductID: vp.ProductID, TransactionID: vp.TransactionID,
		Store: vp.Store, PurchaseTime: vp.PurchaseTime, SeenBefore: vp.SeenBefore, Environment: vp.Environment,
	}
}

func subView(sub *economy.ValidatedSubscription) *ValidatedSubscriptionView {
	if sub == nil {
		return nil
	}
	return &ValidatedSubscriptionView{
		UserID: sub.UserID, ProductID: sub.ProductID, OriginalTransactionID: sub.OriginalTransactionID,
		Store: sub.Store, PurchaseTime: sub.PurchaseTime, ExpireTime: sub.ExpireTime,
		Active: sub.Active, SeenBefore: sub.SeenBefore, Environment: sub.Environment,
	}
}

func (m *GoRuntimeModule) PurchaseValidateApple(ctx context.Context, userID, receipt string, persist bool) (*ValidatedPurchaseView, error) {
	vp, err := economy.ValidatePurchaseApple(ctx, m.dbPool, economy.DefaultIAPConfig, userID, receipt, persist)
	return purchaseView(vp), err
}

func (m *GoRuntimeModule) PurchaseValidateGoogle(ctx context.Context, userID, productID, purchaseToken string, persist bool) (*ValidatedPurchaseView, error) {
	vp, err := economy.ValidatePurchaseGoogle(ctx, m.dbPool, economy.DefaultIAPConfig, userID, productID, purchaseToken, persist)
	return purchaseView(vp), err
}

func (m *GoRuntimeModule) PurchaseValidateHuawei(ctx context.Context, userID, purchaseData, signature string, persist bool) (*ValidatedPurchaseView, error) {
	vp, err := economy.ValidatePurchaseHuawei(ctx, m.dbPool, economy.DefaultIAPConfig, userID, purchaseData, signature, persist)
	return purchaseView(vp), err
}

func (m *GoRuntimeModule) PurchaseValidateFacebookInstant(ctx context.Context, userID, signedRequest string, persist bool) (*ValidatedPurchaseView, error) {
	vp, err := economy.ValidatePurchaseFacebookInstant(ctx, m.dbPool, economy.DefaultIAPConfig, userID, signedRequest, persist)
	return purchaseView(vp), err
}

func (m *GoRuntimeModule) PurchaseValidateSamsung(ctx context.Context, userID, purchaseID string, persist bool) (*ValidatedPurchaseView, error) {
	vp, err := economy.ValidatePurchaseSamsung(ctx, m.dbPool, economy.DefaultIAPConfig, userID, purchaseID, persist)
	return purchaseView(vp), err
}

func (m *GoRuntimeModule) PurchasesList(ctx context.Context, userID string, limit int) ([]*ValidatedPurchaseView, error) {
	list, err := economy.ListPurchases(ctx, m.dbPool, userID, limit)
	if err != nil {
		return nil, err
	}
	out := make([]*ValidatedPurchaseView, 0, len(list))
	for _, vp := range list {
		out = append(out, purchaseView(vp))
	}
	return out, nil
}

func (m *GoRuntimeModule) SubscriptionValidateApple(ctx context.Context, userID, receipt string, persist bool) (*ValidatedSubscriptionView, error) {
	sub, err := economy.ValidateSubscriptionApple(ctx, m.dbPool, economy.DefaultIAPConfig, userID, receipt, persist)
	return subView(sub), err
}

func (m *GoRuntimeModule) SubscriptionValidateGoogle(ctx context.Context, userID, productID, purchaseToken string, persist bool) (*ValidatedSubscriptionView, error) {
	sub, err := economy.ValidateSubscriptionGoogle(ctx, m.dbPool, economy.DefaultIAPConfig, userID, productID, purchaseToken, persist)
	return subView(sub), err
}

func (m *GoRuntimeModule) SubscriptionsList(ctx context.Context, userID string, limit int) ([]*ValidatedSubscriptionView, error) {
	list, err := economy.ListSubscriptions(ctx, m.dbPool, userID, limit, "")
	if err != nil {
		return nil, err
	}
	out := make([]*ValidatedSubscriptionView, 0, len(list.Subscriptions))
	for _, sub := range list.Subscriptions {
		out = append(out, subView(sub))
	}
	return out, nil
}

func (m *GoRuntimeModule) SubscriptionGetProductID(ctx context.Context, userID, productID string) (*ValidatedSubscriptionView, error) {
	sub, err := economy.GetSubscriptionByProductID(ctx, m.dbPool, userID, productID)
	return subView(sub), err
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

func userToView(u *auth.User) *UserView {
	if u == nil {
		return nil
	}
	return &UserView{
		ID: u.ID.String(), Username: u.Username, DisplayName: u.DisplayName, AvatarURL: u.AvatarURL,
		LangTag: u.LangTag, Location: u.Location, Timezone: u.Timezone, Metadata: u.Metadata,
		CreateTime: u.CreateTime, UpdateTime: u.UpdateTime,
	}
}

func (m *GoRuntimeModule) UsersGetId(ctx context.Context, userIDs []string) ([]*UserView, error) {
	users, err := auth.GetUsersPublic(ctx, m.dbPool, userIDs, nil, nil)
	if err != nil {
		return nil, err
	}
	out := make([]*UserView, 0, len(users))
	for _, u := range users {
		out = append(out, userToView(u))
	}
	return out, nil
}

func (m *GoRuntimeModule) UsersGetUsername(ctx context.Context, usernames []string) ([]*UserView, error) {
	users, err := auth.GetUsersPublic(ctx, m.dbPool, nil, usernames, nil)
	if err != nil {
		return nil, err
	}
	out := make([]*UserView, 0, len(users))
	for _, u := range users {
		out = append(out, userToView(u))
	}
	return out, nil
}

func (m *GoRuntimeModule) UsersGetRandom(ctx context.Context, count int) ([]*UserView, error) {
	if count <= 0 {
		count = 1
	}
	if count > 100 {
		count = 100
	}
	rows, err := m.dbPool.Query(ctx, `
		SELECT id, username, COALESCE(display_name,''), COALESCE(avatar_url,''), lang_tag,
		       COALESCE(location,''), COALESCE(timezone,''), COALESCE(metadata::text,'{}'),
		       create_time, update_time
		FROM users WHERE disable_time <= '1970-01-01 00:00:01 UTC'
		ORDER BY random() LIMIT $1`, count)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*UserView
	for rows.Next() {
		var u UserView
		var id uuid.UUID
		if err := rows.Scan(&id, &u.Username, &u.DisplayName, &u.AvatarURL, &u.LangTag, &u.Location, &u.Timezone, &u.Metadata, &u.CreateTime, &u.UpdateTime); err != nil {
			return nil, err
		}
		u.ID = id.String()
		out = append(out, &u)
	}
	return out, rows.Err()
}

func (m *GoRuntimeModule) UsersBanId(ctx context.Context, userIDs []string) error {
	for _, idStr := range userIDs {
		uid, err := uuid.Parse(idStr)
		if err != nil {
			continue
		}
		if _, err := m.dbPool.Exec(ctx, `UPDATE users SET disable_time = now(), update_time = now() WHERE id = $1`, uid); err != nil {
			return err
		}
	}
	return nil
}

func (m *GoRuntimeModule) UsersUnbanId(ctx context.Context, userIDs []string) error {
	for _, idStr := range userIDs {
		uid, err := uuid.Parse(idStr)
		if err != nil {
			continue
		}
		if _, err := m.dbPool.Exec(ctx, `UPDATE users SET disable_time = '1970-01-01 00:00:00 UTC', update_time = now() WHERE id = $1`, uid); err != nil {
			return err
		}
	}
	return nil
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
	if err := notification.ValidateRuntimeCode(int16(code)); err != nil {
		return err
	}
	contentBytes, _ := json.Marshal(content)
	if senderID == "" {
		senderID = uuid.Nil.String()
	}
	n := &notification.Notification{
		UserID:     userID,
		Subject:    subject,
		Content:    string(contentBytes),
		Code:       int16(code),
		SenderID:   senderID,
		Persistent: persistent,
	}
	return notification.NotificationSend(ctx, m.dbPool, nil, map[string][]*notification.Notification{userID: {n}})
}

func (m *GoRuntimeModule) NotificationsSend(ctx context.Context, notifications []*NotificationSendParams) error {
	batch := make(map[string][]*notification.Notification)
	for _, p := range notifications {
		if p == nil {
			continue
		}
		if err := notification.ValidateRuntimeCode(int16(p.Code)); err != nil {
			return err
		}
		contentBytes, _ := json.Marshal(p.Content)
		sender := p.SenderID
		if sender == "" {
			sender = uuid.Nil.String()
		}
		n := &notification.Notification{
			UserID: p.UserID, Subject: p.Subject, Content: string(contentBytes),
			Code: int16(p.Code), SenderID: sender, Persistent: p.Persistent,
		}
		batch[p.UserID] = append(batch[p.UserID], n)
	}
	return notification.NotificationSend(ctx, m.dbPool, nil, batch)
}

func (m *GoRuntimeModule) NotificationSendAll(ctx context.Context, subject string, content map[string]interface{}, code int, persistent bool) error {
	if code <= 0 {
		return notification.ErrNotificationCodeInvalid
	}
	contentBytes, _ := json.Marshal(content)
	n := &notification.Notification{
		Subject: subject, Content: string(contentBytes), Code: int16(code),
		SenderID: uuid.Nil.String(), Persistent: persistent,
	}
	return notification.NotificationSendAll(ctx, m.dbPool, nil, n)
}

func toNotificationView(n *notification.Notification) *NotificationView {
	if n == nil {
		return nil
	}
	return &NotificationView{
		ID: n.ID, UserID: n.UserID, Subject: n.Subject, Content: n.Content,
		Code: n.Code, SenderID: n.SenderID, CreateTime: n.CreateTime, Persistent: n.Persistent,
	}
}

func (m *GoRuntimeModule) NotificationsList(ctx context.Context, userID string, limit int, cursor string) ([]*NotificationView, string, error) {
	if limit <= 0 {
		limit = 100
	}
	list, err := notification.NotificationList(ctx, m.dbPool, userID, limit, cursor)
	if err != nil {
		return nil, "", err
	}
	out := make([]*NotificationView, len(list.Notifications))
	for i, n := range list.Notifications {
		out[i] = toNotificationView(n)
	}
	return out, list.CacheableCursor, nil
}

func (m *GoRuntimeModule) NotificationsDelete(ctx context.Context, userID string, ids []string) error {
	return notification.NotificationDelete(ctx, m.dbPool, userID, ids)
}

func (m *GoRuntimeModule) NotificationsUpdate(ctx context.Context, updates []*NotificationUpdateParams) error {
	us := make([]notification.NotificationUpdate, 0, len(updates))
	for _, u := range updates {
		if u == nil {
			continue
		}
		us = append(us, notification.NotificationUpdate{ID: u.ID, Subject: u.Subject, Content: u.Content, SenderID: u.SenderID})
	}
	return notification.NotificationsUpdate(ctx, m.dbPool, us...)
}

func (m *GoRuntimeModule) NotificationsGetId(ctx context.Context, userID string, ids []string) ([]*NotificationView, error) {
	list, err := notification.NotificationsGetId(ctx, m.dbPool, userID, ids...)
	if err != nil {
		return nil, err
	}
	out := make([]*NotificationView, len(list))
	for i, n := range list {
		out[i] = toNotificationView(n)
	}
	return out, nil
}

func (m *GoRuntimeModule) NotificationsDeleteId(ctx context.Context, userID string, ids []string) error {
	return notification.NotificationsDeleteId(ctx, m.dbPool, userID, ids...)
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

func (m *GoRuntimeModule) MatchList(ctx context.Context, limit int, authoritative bool, label string, minSize, maxSize int) ([]*MatchInfo, error) {
	if m.registry == nil {
		return nil, nil
	}
	return m.registry.ListMatches(ctx, limit, authoritative, label, minSize, maxSize)
}

func (m *GoRuntimeModule) MatchGet(ctx context.Context, matchID string) (*MatchInfo, error) {
	if m.registry == nil {
		return nil, fmt.Errorf("match not found")
	}
	return m.registry.GetMatch(ctx, matchID)
}

func (m *GoRuntimeModule) MatchSignal(ctx context.Context, matchID, data string) (string, error) {
	if m.registry == nil {
		return "", fmt.Errorf("match registry not configured")
	}
	return m.registry.MatchSignal(ctx, matchID, data)
}

func (m *GoRuntimeModule) StatusFollow(sessionID string, userIDs []string) error {
	if m.statusFollower == nil {
		return fmt.Errorf("status registry not configured")
	}
	if sessionID == "" {
		return fmt.Errorf("expects a valid session id")
	}
	return m.statusFollower.StatusFollow(sessionID, userIDs)
}

func (m *GoRuntimeModule) StatusUnfollow(sessionID string, userIDs []string) error {
	if m.statusFollower == nil {
		return fmt.Errorf("status registry not configured")
	}
	if sessionID == "" {
		return fmt.Errorf("expects a valid session id")
	}
	return m.statusFollower.StatusUnfollow(sessionID, userIDs)
}

func (m *GoRuntimeModule) StreamUserList(mode int16, subject, subcontext, label string, includeHidden, includeNotHidden bool) ([]StreamPresenceView, error) {
	if m.streamManager == nil {
		return nil, fmt.Errorf("stream manager not configured")
	}
	return m.streamManager.StreamUserList(mode, subject, subcontext, label, includeHidden, includeNotHidden)
}

func (m *GoRuntimeModule) StreamUserJoin(mode int16, subject, subcontext, label, userID, sessionID string, hidden, persistence bool, status string) (bool, error) {
	if m.streamManager == nil {
		return false, fmt.Errorf("stream manager not configured")
	}
	return m.streamManager.StreamUserJoin(mode, subject, subcontext, label, userID, sessionID, hidden, persistence, status)
}

func (m *GoRuntimeModule) StreamUserLeave(mode int16, subject, subcontext, label, userID, sessionID string) error {
	if m.streamManager == nil {
		return fmt.Errorf("stream manager not configured")
	}
	return m.streamManager.StreamUserLeave(mode, subject, subcontext, label, userID, sessionID)
}

func (m *GoRuntimeModule) StreamUserGet(mode int16, subject, subcontext, label, userID, sessionID string) (*StreamPresenceView, error) {
	if m.streamManager == nil {
		return nil, fmt.Errorf("stream manager not configured")
	}
	return m.streamManager.StreamUserGet(mode, subject, subcontext, label, userID, sessionID)
}

func (m *GoRuntimeModule) StreamClose(mode int16, subject, subcontext, label string) error {
	if m.streamManager == nil {
		return fmt.Errorf("stream manager not configured")
	}
	return m.streamManager.StreamClose(mode, subject, subcontext, label)
}

func (m *GoRuntimeModule) StreamCount(mode int16, subject, subcontext, label string) (int, error) {
	if m.streamManager == nil {
		return 0, fmt.Errorf("stream manager not configured")
	}
	return m.streamManager.StreamCount(mode, subject, subcontext, label)
}

func (m *GoRuntimeModule) StreamSend(mode int16, subject, subcontext, label, data string, sessionIDs []string, reliable bool) error {
	if m.streamManager == nil {
		return fmt.Errorf("stream manager not configured")
	}
	return m.streamManager.StreamSend(mode, subject, subcontext, label, data, sessionIDs, reliable)
}

func (m *GoRuntimeModule) SessionDisconnect(sessionID string) error {
	if m.streamManager == nil {
		return fmt.Errorf("stream manager not configured")
	}
	return m.streamManager.SessionDisconnect(sessionID)
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

func toGroupView(g *social.Group) *GroupView {
	if g == nil {
		return nil
	}
	return &GroupView{
		ID: g.ID, CreatorID: g.CreatorID, Name: g.Name, Description: g.Description,
		AvatarURL: g.AvatarURL, LangTag: g.LangTag, Metadata: g.Metadata,
		Open: g.State == social.GroupStateOpen, EdgeCount: g.EdgeCount, MaxCount: g.MaxCount,
	}
}

func (m *GoRuntimeModule) GroupsGetId(ctx context.Context, groupIDs []string) ([]*GroupView, error) {
	list, err := social.GroupsGetID(ctx, m.dbPool, groupIDs)
	if err != nil {
		return nil, err
	}
	out := make([]*GroupView, len(list))
	for i, g := range list {
		out[i] = toGroupView(g)
	}
	return out, nil
}

func (m *GoRuntimeModule) GroupCreate(ctx context.Context, userID, name, description, avatarURL, langTag, metadata string, open bool, maxCount int) (*GroupView, error) {
	g, err := social.CreateGroupWithParams(ctx, m.dbPool, userID, social.CreateGroupParams{
		Name: name, Description: description, AvatarURL: avatarURL, LangTag: langTag,
		Metadata: metadata, Open: open, MaxCount: maxCount,
	})
	if err != nil {
		return nil, err
	}
	return toGroupView(g), nil
}

func (m *GoRuntimeModule) GroupUpdate(ctx context.Context, groupID, userID, name, description, avatarURL, langTag, metadata string, open bool, maxCount int) error {
	if err := social.UpdateGroup(ctx, m.dbPool, userID, groupID, name, description, avatarURL, langTag, open, metadata); err != nil {
		return err
	}
	if maxCount > 0 {
		return social.UpdateGroupMaxCount(ctx, m.dbPool, groupID, maxCount)
	}
	return nil
}

func (m *GoRuntimeModule) GroupDelete(ctx context.Context, groupID, userID string) error {
	return social.DeleteGroup(ctx, m.dbPool, userID, groupID)
}

func (m *GoRuntimeModule) GroupUsersAdd(ctx context.Context, groupID, callerID string, userIDs []string) error {
	return social.AddGroupUsers(ctx, m.dbPool, callerID, groupID, userIDs, nil)
}

func (m *GoRuntimeModule) GroupUsersBan(ctx context.Context, groupID, callerID string, userIDs []string) error {
	return social.BanGroupUsers(ctx, m.dbPool, callerID, groupID, userIDs)
}

func (m *GoRuntimeModule) GroupUsersKick(ctx context.Context, groupID, callerID string, userIDs []string) error {
	for _, uid := range userIDs {
		if err := social.KickMember(ctx, m.dbPool, callerID, uid, groupID); err != nil {
			return err
		}
	}
	return nil
}

func (m *GoRuntimeModule) GroupUsersPromote(ctx context.Context, groupID, callerID string, userIDs []string) error {
	for _, uid := range userIDs {
		if err := social.PromoteMember(ctx, m.dbPool, callerID, uid, groupID); err != nil {
			return err
		}
	}
	return nil
}

func (m *GoRuntimeModule) GroupUsersDemote(ctx context.Context, groupID, callerID string, userIDs []string) error {
	for _, uid := range userIDs {
		if err := social.DemoteMember(ctx, m.dbPool, callerID, uid, groupID); err != nil {
			return err
		}
	}
	return nil
}

func (m *GoRuntimeModule) GroupUsersList(ctx context.Context, groupID string, limit int, cursor string) ([]*GroupUserView, string, error) {
	list, next, err := social.ListGroupMembers(ctx, m.dbPool, groupID, limit, cursor)
	if err != nil {
		return nil, "", err
	}
	out := make([]*GroupUserView, len(list))
	for i, mbr := range list {
		out[i] = &GroupUserView{UserID: mbr.UserID, Username: mbr.Username, State: mbr.Role}
	}
	return out, next, nil
}

func (m *GoRuntimeModule) GroupsList(ctx context.Context, name, langTag string, open *bool, members, limit int, cursor string) ([]*GroupView, string, error) {
	list, next, err := social.ListGroups(ctx, m.dbPool, name, langTag, open, members, limit, cursor)
	if err != nil {
		return nil, "", err
	}
	out := make([]*GroupView, len(list))
	for i, g := range list {
		out[i] = toGroupView(g)
	}
	return out, next, nil
}

func (m *GoRuntimeModule) UserGroupsList(ctx context.Context, userID string, limit int, cursor string) ([]*UserGroupView, string, error) {
	list, next, err := social.ListUserGroups(ctx, m.dbPool, userID, limit, cursor)
	if err != nil {
		return nil, "", err
	}
	out := make([]*UserGroupView, len(list))
	for i, r := range list {
		out[i] = &UserGroupView{Group: toGroupView(r.Group), State: r.Role}
	}
	return out, next, nil
}

func (m *GoRuntimeModule) GroupsGetRandom(ctx context.Context, count int) ([]*GroupView, error) {
	list, err := social.GroupsGetRandom(ctx, m.dbPool, count)
	if err != nil {
		return nil, err
	}
	out := make([]*GroupView, len(list))
	for i, g := range list {
		out[i] = toGroupView(g)
	}
	return out, nil
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

func toChannelAckView(ack *chat.ChannelMessageAck) *ChannelMessageAckView {
	if ack == nil {
		return nil
	}
	return &ChannelMessageAckView{
		ChannelID: ack.ChannelID, MessageID: ack.MessageID, Code: ack.Code, Username: ack.Username,
		CreateTime: ack.CreateTime, UpdateTime: ack.UpdateTime, Persistent: ack.Persistent,
		RoomName: ack.RoomName, GroupID: ack.GroupID, UserIDOne: ack.UserIDOne, UserIDTwo: ack.UserIDTwo,
	}
}

func (m *GoRuntimeModule) ChannelIdBuild(ctx context.Context, userID, target string, chanType int) (string, error) {
	id, _, err := chat.BuildChannelId(ctx, m.dbPool, userID, target, chanType)
	return id, err
}

func (m *GoRuntimeModule) ChannelMessageSend(ctx context.Context, channelID string, content map[string]interface{}, senderID, senderUsername string, persist bool) (*ChannelMessageAckView, error) {
	stream, err := chat.ChannelIdToStream(channelID)
	if err != nil {
		return nil, err
	}
	contentBytes, _ := json.Marshal(content)
	ack, msg, err := chat.ChannelMessageSend(ctx, m.dbPool, stream, channelID, string(contentBytes), senderID, senderUsername, persist)
	if err != nil {
		return nil, err
	}
	if chat.DefaultRouter != nil {
		chat.DefaultRouter.BroadcastChannelMessage(channelID, msg)
	}
	return toChannelAckView(ack), nil
}

func (m *GoRuntimeModule) ChannelMessageUpdate(ctx context.Context, channelID, messageID string, content map[string]interface{}, senderID, senderUsername string, persist bool) (*ChannelMessageAckView, error) {
	stream, err := chat.ChannelIdToStream(channelID)
	if err != nil {
		return nil, err
	}
	contentBytes, _ := json.Marshal(content)
	ack, msg, err := chat.ChannelMessageUpdate(ctx, m.dbPool, stream, channelID, messageID, string(contentBytes), senderID, senderUsername, persist)
	if err != nil {
		return nil, err
	}
	if chat.DefaultRouter != nil {
		chat.DefaultRouter.BroadcastChannelMessage(channelID, msg)
	}
	return toChannelAckView(ack), nil
}

func (m *GoRuntimeModule) ChannelMessageRemove(ctx context.Context, channelID, messageID, senderID, senderUsername string, persist bool) (*ChannelMessageAckView, error) {
	stream, err := chat.ChannelIdToStream(channelID)
	if err != nil {
		return nil, err
	}
	ack, msg, err := chat.ChannelMessageRemove(ctx, m.dbPool, stream, channelID, messageID, senderID, senderUsername, persist)
	if err != nil {
		return nil, err
	}
	if chat.DefaultRouter != nil {
		chat.DefaultRouter.BroadcastChannelMessage(channelID, msg)
	}
	return toChannelAckView(ack), nil
}

func (m *GoRuntimeModule) ChannelMessagesList(ctx context.Context, channelID string, limit int, forward bool, cursor string) ([]*ChannelMessageView, string, string, string, error) {
	stream, err := chat.ChannelIdToStream(channelID)
	if err != nil {
		return nil, "", "", "", err
	}
	list, err := chat.ChannelMessagesList(ctx, m.dbPool, "", stream, channelID, limit, forward, cursor)
	if err != nil {
		return nil, "", "", "", err
	}
	out := make([]*ChannelMessageView, len(list.Messages))
	for i, msg := range list.Messages {
		out[i] = &ChannelMessageView{
			ChannelID: msg.ChannelID, MessageID: msg.MessageID, Code: msg.Code,
			SenderID: msg.SenderID, Username: msg.Username, Content: msg.Content,
			CreateTime: msg.CreateTime, UpdateTime: msg.UpdateTime, Persistent: msg.Persistent,
			RoomName: msg.RoomName, GroupID: msg.GroupID, UserIDOne: msg.UserIDOne, UserIDTwo: msg.UserIDTwo,
		}
	}
	return out, list.NextCursor, list.PrevCursor, list.CacheableCursor, nil
}

// CronNext returns the next UTC unix timestamp matching expression after timestamp (ADR-0025).
func (m *GoRuntimeModule) CronNext(expression string, timestamp int64) (int64, error) {
	expr, err := cronexpr.Parse(expression)
	if err != nil {
		return 0, fmt.Errorf("expects a valid cron string")
	}
	t := time.Unix(timestamp, 0).UTC()
	return expr.Next(t).UTC().Unix(), nil
}

// CronPrev returns the previous UTC unix timestamp matching expression before timestamp (ADR-0025).
func (m *GoRuntimeModule) CronPrev(expression string, timestamp int64) (int64, error) {
	expr, err := cronexpr.Parse(expression)
	if err != nil {
		return 0, fmt.Errorf("expects a valid cron string")
	}
	t := time.Unix(timestamp, 0).UTC()
	return expr.Last(t).UTC().Unix(), nil
}
