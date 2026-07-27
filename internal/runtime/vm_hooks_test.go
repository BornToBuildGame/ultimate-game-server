package runtime

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/dop251/goja"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yuin/gopher-lua"
)

// Mock runtime module that holds storage in memory
type mockRuntimeModule struct {
	mu      sync.RWMutex
	storage map[string]*StorageObject
}

func newMockRuntimeModule() *mockRuntimeModule {
	return &mockRuntimeModule{
		storage: make(map[string]*StorageObject),
	}
}

func (m *mockRuntimeModule) StorageRead(ctx context.Context, reads []*StorageRead) ([]*StorageObject, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var res []*StorageObject
	for _, r := range reads {
		k := fmt.Sprintf("%s:%s:%s", r.Collection, r.UserID, r.Key)
		if obj, ok := m.storage[k]; ok {
			res = append(res, obj)
		}
	}
	return res, nil
}

func (m *mockRuntimeModule) StorageWrite(ctx context.Context, writes []*StorageWrite) ([]*StorageObjectAck, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var acks []*StorageObjectAck
	for _, w := range writes {
		k := fmt.Sprintf("%s:%s:%s", w.Collection, w.UserID, w.Key)
		obj := &StorageObject{
			Collection:      w.Collection,
			Key:             w.Key,
			UserID:          w.UserID,
			Value:           w.Value,
			Version:         "v1",
			PermissionRead:  w.PermissionRead,
			PermissionWrite: w.PermissionWrite,
			CreateTime:      time.Now(),
			UpdateTime:      time.Now(),
		}
		m.storage[k] = obj
		acks = append(acks, &StorageObjectAck{
			Collection: w.Collection,
			Key:        w.Key,
			UserID:     w.UserID,
			Version:    "v1",
			CreateTime: obj.CreateTime,
			UpdateTime: obj.UpdateTime,
		})
	}
	return acks, nil
}

func (m *mockRuntimeModule) StorageDelete(ctx context.Context, deletes []*StorageDelete) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, d := range deletes {
		k := fmt.Sprintf("%s:%s:%s", d.Collection, d.UserID, d.Key)
		delete(m.storage, k)
	}
	return nil
}

func (m *mockRuntimeModule) StorageList(ctx context.Context, callerID, userID, collection string, limit int, cursor string) ([]*StorageObject, string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var res []*StorageObject
	for _, obj := range m.storage {
		if obj.Collection == collection && (userID == "" || obj.UserID == userID) {
			res = append(res, obj)
		}
	}
	return res, "", nil
}

func (m *mockRuntimeModule) StorageWriteRetry(ctx context.Context, reads []*StorageRead, updateFn func([]*StorageObject) ([]*StorageWrite, error), maxRetries int) ([]*StorageObjectAck, error) {
	objs, err := m.StorageRead(ctx, reads)
	if err != nil {
		return nil, err
	}
	writes, err := updateFn(objs)
	if err != nil {
		return nil, err
	}
	return m.StorageWrite(ctx, writes)
}

func (m *mockRuntimeModule) WalletUpdate(ctx context.Context, userID string, changeset map[string]int64, metadata map[string]interface{}, updateLedger bool) (map[string]int64, map[string]int64, error) {
	previous := map[string]int64{"gold": 100, "gems": 50}
	updated := map[string]int64{"gold": 100, "gems": 50}
	for k, v := range changeset {
		updated[k] += v
	}
	return updated, previous, nil
}

func (m *mockRuntimeModule) WalletsUpdate(ctx context.Context, updates []*WalletUpdateParams, updateLedger bool) ([]*WalletUpdateResultView, error) {
	return nil, nil
}
func (m *mockRuntimeModule) WalletLedgerList(ctx context.Context, userID string, limit int, cursor string) ([]*WalletLedgerView, string, error) {
	return nil, "", nil
}
func (m *mockRuntimeModule) WalletLedgerUpdate(ctx context.Context, ledgerID, userID string, metadata map[string]interface{}) error {
	return nil
}
func (m *mockRuntimeModule) PurchaseValidateApple(ctx context.Context, userID, receipt string, persist bool) (*ValidatedPurchaseView, error) {
	return &ValidatedPurchaseView{UserID: userID, ProductID: "test", TransactionID: "tx"}, nil
}
func (m *mockRuntimeModule) PurchaseValidateGoogle(ctx context.Context, userID, productID, purchaseToken string, persist bool) (*ValidatedPurchaseView, error) {
	return &ValidatedPurchaseView{UserID: userID, ProductID: productID, TransactionID: purchaseToken}, nil
}
func (m *mockRuntimeModule) PurchaseValidateHuawei(ctx context.Context, userID, purchaseData, signature string, persist bool) (*ValidatedPurchaseView, error) {
	return &ValidatedPurchaseView{UserID: userID}, nil
}
func (m *mockRuntimeModule) PurchaseValidateFacebookInstant(ctx context.Context, userID, signedRequest string, persist bool) (*ValidatedPurchaseView, error) {
	return &ValidatedPurchaseView{UserID: userID}, nil
}
func (m *mockRuntimeModule) PurchaseValidateSamsung(ctx context.Context, userID, purchaseID string, persist bool) (*ValidatedPurchaseView, error) {
	return &ValidatedPurchaseView{UserID: userID, TransactionID: purchaseID}, nil
}
func (m *mockRuntimeModule) PurchasesList(ctx context.Context, userID string, limit int) ([]*ValidatedPurchaseView, error) {
	return nil, nil
}
func (m *mockRuntimeModule) SubscriptionValidateApple(ctx context.Context, userID, receipt string, persist bool) (*ValidatedSubscriptionView, error) {
	return &ValidatedSubscriptionView{UserID: userID, Active: true}, nil
}
func (m *mockRuntimeModule) SubscriptionValidateGoogle(ctx context.Context, userID, productID, purchaseToken string, persist bool) (*ValidatedSubscriptionView, error) {
	return &ValidatedSubscriptionView{UserID: userID, ProductID: productID, Active: true}, nil
}
func (m *mockRuntimeModule) SubscriptionsList(ctx context.Context, userID string, limit int) ([]*ValidatedSubscriptionView, error) {
	return nil, nil
}
func (m *mockRuntimeModule) SubscriptionGetProductID(ctx context.Context, userID, productID string) (*ValidatedSubscriptionView, error) {
	return &ValidatedSubscriptionView{UserID: userID, ProductID: productID}, nil
}

func (m *mockRuntimeModule) MatchList(ctx context.Context, limit int, authoritative bool, label string, minSize, maxSize int) ([]*MatchInfo, error) {
	return nil, nil
}
func (m *mockRuntimeModule) MatchGet(ctx context.Context, matchID string) (*MatchInfo, error) {
	return &MatchInfo{MatchID: matchID}, nil
}
func (m *mockRuntimeModule) MatchSignal(ctx context.Context, matchID, data string) (string, error) {
	return "ok", nil
}
func (m *mockRuntimeModule) StatusFollow(sessionID string, userIDs []string) error {
	return nil
}
func (m *mockRuntimeModule) StatusUnfollow(sessionID string, userIDs []string) error {
	return nil
}
func (m *mockRuntimeModule) StreamUserList(mode int16, subject, subcontext, label string, includeHidden, includeNotHidden bool) ([]StreamPresenceView, error) {
	return nil, nil
}
func (m *mockRuntimeModule) StreamUserJoin(mode int16, subject, subcontext, label, userID, sessionID string, hidden, persistence bool, status string) (bool, error) {
	return false, nil
}
func (m *mockRuntimeModule) StreamUserLeave(mode int16, subject, subcontext, label, userID, sessionID string) error {
	return nil
}
func (m *mockRuntimeModule) StreamCount(mode int16, subject, subcontext, label string) (int, error) {
	return 0, nil
}
func (m *mockRuntimeModule) StreamSend(mode int16, subject, subcontext, label, data string, sessionIDs []string, reliable bool) error {
	return nil
}
func (m *mockRuntimeModule) SessionDisconnect(sessionID string) error {
	return nil
}
func (m *mockRuntimeModule) RpcCall(ctx context.Context, id, payload string) (string, error) {
	return payload, nil
}
func (m *mockRuntimeModule) MultiUpdate(ctx context.Context, accountUpdates []*AccountUpdateParams, storageWrites []*StorageWrite, storageDeletes []*StorageDelete, walletUpdates []*WalletUpdateParams, updateLedger bool) ([]*StorageObjectAck, []*WalletUpdateResultView, error) {
	return nil, nil, nil
}
func (m *mockRuntimeModule) StorageIndexList(ctx context.Context, callerID, indexName, query string, limit int, order []string, cursor string) ([]*StorageObject, string, error) {
	return nil, "", nil
}
func (m *mockRuntimeModule) CronNext(expression string, timestamp int64) (int64, error) {
	return timestamp, nil
}
func (m *mockRuntimeModule) CronPrev(expression string, timestamp int64) (int64, error) {
	return timestamp, nil
}

func (m *mockRuntimeModule) AccountGetId(ctx context.Context, userID string) (*Account, error) {
	return &Account{ID: userID, Username: "test_user"}, nil
}

func (m *mockRuntimeModule) LeaderboardRecordWrite(ctx context.Context, id, ownerID, username string, score, subscore int64, metadata map[string]interface{}) (*LeaderboardRecord, error) {
	return &LeaderboardRecord{
		LeaderboardID: id,
		OwnerID:       ownerID,
		Username:      username,
		Score:         score,
		Subscore:      subscore,
		Rank:          1,
	}, nil
}

func (m *mockRuntimeModule) NotificationSend(ctx context.Context, userID, subject string, content map[string]interface{}, code int, senderID string, persistent bool) error {
	return nil
}
func (m *mockRuntimeModule) NotificationsSend(ctx context.Context, notifications []*NotificationSendParams) error {
	return nil
}
func (m *mockRuntimeModule) NotificationSendAll(ctx context.Context, subject string, content map[string]interface{}, code int, persistent bool) error {
	return nil
}
func (m *mockRuntimeModule) NotificationsList(ctx context.Context, userID string, limit int, cursor string) ([]*NotificationView, string, error) {
	return nil, "", nil
}
func (m *mockRuntimeModule) NotificationsDelete(ctx context.Context, userID string, ids []string) error {
	return nil
}
func (m *mockRuntimeModule) NotificationsUpdate(ctx context.Context, updates []*NotificationUpdateParams) error {
	return nil
}
func (m *mockRuntimeModule) NotificationsGetId(ctx context.Context, userID string, ids []string) ([]*NotificationView, error) {
	return nil, nil
}
func (m *mockRuntimeModule) NotificationsDeleteId(ctx context.Context, userID string, ids []string) error {
	return nil
}

func (m *mockRuntimeModule) FriendsList(ctx context.Context, userID string, limit int, state *int, cursor string) ([]*FriendEdge, string, error) {
	return nil, "", nil
}

func (m *mockRuntimeModule) FriendsAdd(ctx context.Context, userID string, ids, usernames []string, metadata map[string]any) error {
	return nil
}

func (m *mockRuntimeModule) FriendsDelete(ctx context.Context, userID string, ids, usernames []string) error {
	return nil
}

func (m *mockRuntimeModule) FriendsBlock(ctx context.Context, userID string, ids, usernames []string) error {
	return nil
}

func (m *mockRuntimeModule) FriendsOfFriendsList(ctx context.Context, userID string, limit int, cursor string) ([]*FriendOfFriendEdge, string, error) {
	return nil, "", nil
}

func (m *mockRuntimeModule) UsersGetFriendStatus(ctx context.Context, userID string, friendIDs []string) (map[string]int, error) {
	return map[string]int{}, nil
}

func (m *mockRuntimeModule) FriendMetadataUpdate(ctx context.Context, userID, friendID string, metadata map[string]any) error {
	return nil
}

func (m *mockRuntimeModule) PartyList(ctx context.Context, limit int, open *bool, showHidden bool, query, cursor string) ([]*PartyListEntry, string, error) {
	return nil, "", nil
}

func (m *mockRuntimeModule) GroupsGetId(ctx context.Context, groupIDs []string) ([]*GroupView, error) {
	return nil, nil
}
func (m *mockRuntimeModule) GroupCreate(ctx context.Context, userID, name, description, avatarURL, langTag, metadata string, open bool, maxCount int) (*GroupView, error) {
	return &GroupView{ID: "g1", Name: name, Open: open}, nil
}
func (m *mockRuntimeModule) GroupUpdate(ctx context.Context, groupID, userID, name, description, avatarURL, langTag, metadata string, open bool, maxCount int) error {
	return nil
}
func (m *mockRuntimeModule) GroupDelete(ctx context.Context, groupID, userID string) error { return nil }
func (m *mockRuntimeModule) GroupUsersAdd(ctx context.Context, groupID, callerID string, userIDs []string) error {
	return nil
}
func (m *mockRuntimeModule) GroupUsersBan(ctx context.Context, groupID, callerID string, userIDs []string) error {
	return nil
}
func (m *mockRuntimeModule) GroupUsersKick(ctx context.Context, groupID, callerID string, userIDs []string) error {
	return nil
}
func (m *mockRuntimeModule) GroupUsersPromote(ctx context.Context, groupID, callerID string, userIDs []string) error {
	return nil
}
func (m *mockRuntimeModule) GroupUsersDemote(ctx context.Context, groupID, callerID string, userIDs []string) error {
	return nil
}
func (m *mockRuntimeModule) GroupUsersList(ctx context.Context, groupID string, limit int, cursor string) ([]*GroupUserView, string, error) {
	return nil, "", nil
}
func (m *mockRuntimeModule) GroupsList(ctx context.Context, name, langTag string, open *bool, members, limit int, cursor string) ([]*GroupView, string, error) {
	return nil, "", nil
}
func (m *mockRuntimeModule) UserGroupsList(ctx context.Context, userID string, limit int, cursor string) ([]*UserGroupView, string, error) {
	return nil, "", nil
}
func (m *mockRuntimeModule) GroupsGetRandom(ctx context.Context, count int) ([]*GroupView, error) {
	return nil, nil
}

func (m *mockRuntimeModule) ChannelIdBuild(ctx context.Context, userID, target string, chanType int) (string, error) {
	return "2..test", nil
}
func (m *mockRuntimeModule) ChannelMessageSend(ctx context.Context, channelID string, content map[string]interface{}, senderID, senderUsername string, persist bool) (*ChannelMessageAckView, error) {
	return &ChannelMessageAckView{ChannelID: channelID, MessageID: "m1", Persistent: persist}, nil
}
func (m *mockRuntimeModule) ChannelMessageUpdate(ctx context.Context, channelID, messageID string, content map[string]interface{}, senderID, senderUsername string, persist bool) (*ChannelMessageAckView, error) {
	return &ChannelMessageAckView{ChannelID: channelID, MessageID: messageID}, nil
}
func (m *mockRuntimeModule) ChannelMessageRemove(ctx context.Context, channelID, messageID, senderID, senderUsername string, persist bool) (*ChannelMessageAckView, error) {
	return &ChannelMessageAckView{ChannelID: channelID, MessageID: messageID, Code: 2}, nil
}
func (m *mockRuntimeModule) ChannelMessagesList(ctx context.Context, channelID string, limit int, forward bool, cursor string) ([]*ChannelMessageView, string, string, string, error) {
	return nil, "", "", "", nil
}

func (m *mockRuntimeModule) MatchCreate(ctx context.Context, module string, params map[string]interface{}) (string, error) {
	return "match_12345", nil
}

func (m *mockRuntimeModule) LeaderboardCreate(ctx context.Context, id string, authoritative bool, sortOrder int, operator int, resetSchedule string, metadata map[string]interface{}, enableRanks bool) error {
	return nil
}

func (m *mockRuntimeModule) LeaderboardDelete(ctx context.Context, id string) error {
	return nil
}

func (m *mockRuntimeModule) LeaderboardList(ctx context.Context, limit int, cursor string) ([]*Leaderboard, string, error) {
	return nil, "", nil
}

func (m *mockRuntimeModule) LeaderboardsGetId(ctx context.Context, ids []string) ([]*Leaderboard, error) {
	return nil, nil
}

func (m *mockRuntimeModule) LeaderboardRanksDisable(ctx context.Context, id string) error {
	return nil
}

func (m *mockRuntimeModule) TournamentCreate(ctx context.Context, id string, authoritative bool, sortOrder, operator int, resetSchedule string, metadata map[string]interface{}, title, description string, category int, startTime, endTime int64, duration, maxSize, maxNumScore int, joinRequired, enableRanks bool) error {
	return nil
}

func (m *mockRuntimeModule) TournamentDelete(ctx context.Context, id string) error {
	return nil
}

func (m *mockRuntimeModule) TournamentList(ctx context.Context, categoryStart, categoryEnd int, startTime, endTime int64, limit int, cursor string, active bool) ([]*TournamentView, string, error) {
	return nil, "", nil
}

func (m *mockRuntimeModule) TournamentsGetId(ctx context.Context, ids []string) ([]*Leaderboard, error) {
	return nil, nil
}

func (m *mockRuntimeModule) TournamentRanksDisable(ctx context.Context, id string) error {
	return nil
}

func (m *mockRuntimeModule) TournamentJoin(ctx context.Context, id, ownerID, username string) error {
	return nil
}

func (m *mockRuntimeModule) LeaderboardRecordsList(ctx context.Context, id string, ownerIDs []string, limit int, cursor string, expiry int64) ([]*LeaderboardRecord, string, string, error) {
	return []*LeaderboardRecord{}, "", "", nil
}

func (m *mockRuntimeModule) LeaderboardRecordsAroundOwner(ctx context.Context, id, ownerID string, limit int, expiry int64) ([]*LeaderboardRecord, error) {
	return []*LeaderboardRecord{}, nil
}

func (m *mockRuntimeModule) LeaderboardRecordDelete(ctx context.Context, id, ownerID string) error {
	return nil
}

func (m *mockRuntimeModule) LeaderboardRecordsHaystack(ctx context.Context, id, ownerID string, limit int, cursor string, expiry int64) ([]*LeaderboardRecord, string, string, error) {
	return nil, "", "", nil
}

func (m *mockRuntimeModule) LeaderboardRecordsListCursorFromRank(ctx context.Context, leaderboardID string, rank, expiry int64) (string, error) {
	return "", nil
}

func (m *mockRuntimeModule) TournamentRecordWrite(ctx context.Context, id, ownerID, username string, score, subscore int64, metadata map[string]interface{}) (*LeaderboardRecord, error) {
	return m.LeaderboardRecordWrite(ctx, id, ownerID, username, score, subscore, metadata)
}

func (m *mockRuntimeModule) TournamentRecordsList(ctx context.Context, id string, ownerIDs []string, limit int, cursor string, expiry int64) ([]*LeaderboardRecord, string, string, error) {
	return m.LeaderboardRecordsList(ctx, id, ownerIDs, limit, cursor, expiry)
}

func (m *mockRuntimeModule) TournamentRecordsAroundOwner(ctx context.Context, id, ownerID string, limit int, expiry int64) ([]*LeaderboardRecord, error) {
	return m.LeaderboardRecordsAroundOwner(ctx, id, ownerID, limit, expiry)
}

func (m *mockRuntimeModule) TournamentRecordsHaystack(ctx context.Context, id, ownerID string, limit int, cursor string, expiry int64) ([]*LeaderboardRecord, string, string, error) {
	return nil, "", "", nil
}

func (m *mockRuntimeModule) TournamentRecordDelete(ctx context.Context, id, ownerID string) error {
	return nil
}

func (m *mockRuntimeModule) TournamentAddAttempt(ctx context.Context, id, ownerID string, count int) error {
	return nil
}

// Test Gopher-Lua bindings and execution
func TestLuaVM_NK_Bindings(t *testing.T) {
	nk := newMockRuntimeModule()
	L := lua.NewState()
	defer L.Close()

	// Map nk table to Lua VM
	MapLuaNK(L, nk)

	// Execute a script that writes to storage
	script := `
		local writes = {
			{
				collection = "inventory",
				key = "sword",
				user_id = "user1",
				value = '{"damage": 50}',
				permission_read = 1,
				permission_write = 1
			}
		}
		local acks = nk.storage_write(writes)
		assert(#acks == 1)
		assert(acks[1].key == "sword")

		local reads = {
			{
				collection = "inventory",
				key = "sword",
				user_id = "user1"
			}
		}
		local objs = nk.storage_read(reads)
		assert(#objs == 1)
		assert(objs[1].value == '{"damage": 50}')

		-- Test wallet update
		local wallet = nk.wallet_update("user1", {gold = 50})
		assert(wallet.gold == 150)
		assert(wallet.gems == 50)

		-- Test leaderboard record write
		local rec = nk.leaderboard_record_write("lb1", "user1", "gamer1", 1000, 0, {tag = "pro"})
		assert(rec.leaderboard_id == "lb1")
		assert(rec.score == 1000)

		-- Test notification send
		nk.notification_send("user1", "Welcome", {msg = "Hello"}, 1, "sys", true)

		-- Test match create
		local match_id = nk.match_create("battle", {map = "desert"})
		assert(match_id == "match_12345")

		-- Test leaderboard create/delete
		nk.leaderboard_create("lb_temp", true, 1, 0, "", {}, true)
		nk.leaderboard_delete("lb_temp")

		-- Test tournament create/join/delete
		nk.tournament_create("tour_temp", true, 1, 0, "", {}, "Title", "Desc", 1, 0, 0, 3600, 100, 3, true, true)
		nk.tournament_join("tour_temp", "user1", "gamer1")
		nk.tournament_delete("tour_temp")
	`

	err := L.DoString(script)
	require.NoError(t, err)
}

// Test Goja JavaScript bindings and execution
func TestGojaVM_NK_Bindings(t *testing.T) {
	nk := newMockRuntimeModule()
	vm := goja.New()

	// Map nk table to JS VM
	MapJSNK(vm, nk, 5*time.Second)

	// Execute a JS script that writes to storage
	script := `
		var writes = [
			{
				collection: "inventory",
				key: "shield",
				user_id: "user1",
				value: '{"defense": 20}',
				permission_read: 1,
				permission_write: 1
			}
		];
		var acks = nk.storage_write(writes);
		if (acks.length !== 1 || acks[0].key !== "shield") {
			throw new Error("storage_write failed");
		}

		var reads = [
			{
				collection: "inventory",
				key: "shield",
				user_id: "user1"
			}
		];
		var objs = nk.storage_read(reads);
		if (objs.length !== 1 || objs[0].value !== '{"defense": 20}') {
			throw new Error("storage_read failed");
		}

		// Test wallet update
		var wallet = nk.wallet_update("user1", {gold: 50});
		if (wallet.gold !== 150 || wallet.gems !== 50) {
			throw new Error("wallet_update failed");
		}

		// Test leaderboard record write
		var rec = nk.leaderboard_record_write("lb1", "user1", "gamer1", 1000, 0, {tag: "pro"});
		if (rec.leaderboard_id !== "lb1" || rec.score !== 1000) {
			throw new Error("leaderboard_record_write failed");
		}

		// Test notification send
		nk.notification_send("user1", "Welcome", {msg: "Hello"}, 1, "sys", true);

		// Test match create
		var match_id = nk.match_create("battle", {map: "desert"});
		if (match_id !== "match_12345") {
			throw new Error("match_create failed");
		}

		// Test leaderboard create/delete
		nk.leaderboard_create("lb_temp", true, 1, 0, "", {}, true);
		nk.leaderboard_delete("lb_temp");

		// Test tournament create/join/delete
		nk.tournament_create("tour_temp", true, 1, 0, "", {}, "Title", "Desc", 1, 0, 0, 3600, 100, 3, true, true);
		nk.tournament_join("tour_temp", "user1", "gamer1");
		nk.tournament_delete("tour_temp");
	`

	_, err := vm.RunString(script)
	require.NoError(t, err)
}

// Test Precedence Resolution (Go > Lua > JS)
func TestHookPrecedenceResolution(t *testing.T) {
	registry := NewHookRegistry()

	// Register before hooks in all runtimes
	registry.RegisterBefore("WriteStorageObjects", func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in interface{}) (interface{} , error) {
		return "go_native", nil
	})
	registry.RegisterLuaBefore("WriteStorageObjects", "luaBeforeWrite")
	registry.RegisterJSBefore("WriteStorageObjects", "jsBeforeWrite")

	// Verify Go hook is chosen
	hook, runtimeType, fnName, found := registry.GetBeforeHook("WriteStorageObjects")
	assert.True(t, found)
	assert.Equal(t, "go", runtimeType)
	assert.Equal(t, "", fnName)
	assert.NotNil(t, hook)

	res, err := hook(context.Background(), nil, nil, nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, "go_native", res)

	// Clear Go hook and check if Lua hook is chosen next
	registry.mu.Lock()
	delete(registry.beforeHooks, "WriteStorageObjects")
	registry.mu.Unlock()

	hook, runtimeType, fnName, found = registry.GetBeforeHook("WriteStorageObjects")
	assert.True(t, found)
	assert.Equal(t, "lua", runtimeType)
	assert.Equal(t, "luaBeforeWrite", fnName)
	assert.Nil(t, hook)

	// Clear Lua hook and check if JS hook is chosen next
	registry.mu.Lock()
	delete(registry.luaBeforeHooks, "WriteStorageObjects")
	registry.mu.Unlock()

	hook, runtimeType, fnName, found = registry.GetBeforeHook("WriteStorageObjects")
	assert.True(t, found)
	assert.Equal(t, "js", runtimeType)
	assert.Equal(t, "jsBeforeWrite", fnName)
	assert.Nil(t, hook)
}

// Test Watchdog timeouts for Gopher-Lua and Goja VM
func TestScriptVM_WatchdogTimeout(t *testing.T) {
	nk := newMockRuntimeModule()

	t.Run("Lua Instruction watchdog", func(t *testing.T) {
		L := lua.NewState()
		defer L.Close()

		MapLuaNK(L, nk)

		// Set context timeout to emulate CPU limit
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		L.SetContext(ctx)

		// Infinite loop
		script := `
			local count = 0
			while true do
				count = count + 1
			end
		`
		err := L.DoString(script)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "deadline exceeded")
	})

	t.Run("Goja Watchdog Timeout", func(t *testing.T) {
		vm := goja.New()

		// Limit runtime to 50ms for testing watchdog quickly
		MapJSNK(vm, nk, 50*time.Millisecond)

		script := `
			var count = 0;
			while (true) {
				count++;
			}
		`
		_, err := vm.RunString(script)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "timeout exceeded")
	})
}
