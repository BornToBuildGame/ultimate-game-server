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

func (m *mockRuntimeModule) WalletUpdate(ctx context.Context, userID string, changeset map[string]int64, metadata map[string]interface{}, updateLedger bool) (map[string]int64, error) {
	newWallet := map[string]int64{"gold": 100, "gems": 50}
	for k, v := range changeset {
		newWallet[k] += v
	}
	return newWallet, nil
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

func (m *mockRuntimeModule) MatchCreate(ctx context.Context, module string, params map[string]interface{}) (string, error) {
	return "match_12345", nil
}

func (m *mockRuntimeModule) LeaderboardCreate(ctx context.Context, id string, authoritative bool, sortOrder int, operator int, resetSchedule string, metadata map[string]interface{}, enableRanks bool) error {
	return nil
}

func (m *mockRuntimeModule) LeaderboardDelete(ctx context.Context, id string) error {
	return nil
}

func (m *mockRuntimeModule) TournamentCreate(ctx context.Context, id string, authoritative bool, sortOrder, operator int, resetSchedule string, metadata map[string]interface{}, title, description string, category int, startTime, endTime int64, duration, maxSize, maxNumScore int, joinRequired, enableRanks bool) error {
	return nil
}

func (m *mockRuntimeModule) TournamentDelete(ctx context.Context, id string) error {
	return nil
}

func (m *mockRuntimeModule) TournamentJoin(ctx context.Context, id, ownerID, username string) error {
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
