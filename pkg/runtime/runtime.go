package runtime

import (
	"context"
	"database/sql"
	"time"

	internalruntime "github.com/BornToBuildGame/ultimate-game-server/internal/runtime"
	"github.com/dop251/goja"
	"github.com/redis/go-redis/v9"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/yuin/gopher-lua"
)

// Logger defines the runtime logging interface.
type Logger = internalruntime.Logger

// RuntimeModule defines the interface exposed to Go game logic (nk).
type RuntimeModule = internalruntime.RuntimeModule

// Initializer defines the handler registration interface for Go modules.
type Initializer = internalruntime.Initializer

// Match defines the authoritative multiplayer match interface.
type Match = internalruntime.Match

// Presence defines client presence within a match.
type Presence = internalruntime.Presence

// MatchData defines match payload messages passed into MatchLoop.
type MatchData = internalruntime.MatchData

// Storage types
type StorageRead = internalruntime.StorageRead
type StorageWrite = internalruntime.StorageWrite
type StorageDelete = internalruntime.StorageDelete
type StorageObject = internalruntime.StorageObject
type StorageObjectAck = internalruntime.StorageObjectAck
type MatchInfo = internalruntime.MatchInfo

// User, Leaderboard, and Domain types
type Account = internalruntime.Account
type UserView = internalruntime.UserView
type LeaderboardRecord = internalruntime.LeaderboardRecord
type Leaderboard = internalruntime.Leaderboard
type TournamentView = internalruntime.TournamentView
type Friend = internalruntime.Friend
type GroupView = internalruntime.GroupView
type NotificationView = internalruntime.NotificationView
type RPCHandler = internalruntime.RPCHandler
type BeforeHook = internalruntime.BeforeHook
type AfterHook = internalruntime.AfterHook

// InitModuleFunc is the entry point function signature for Go game modules.
type InitModuleFunc = internalruntime.InitModuleFunc

// GoRuntimeManager manages Go runtime plugins and native registrations.
type GoRuntimeManager = internalruntime.GoRuntimeManager

// HookRegistry holds all registered RPCs, hooks, and match factories.
type HookRegistry = internalruntime.HookRegistry

// CronScheduler manages cron jobs registered in the server runtime.
type CronScheduler = internalruntime.CronScheduler

// NewGoRuntimeModule constructs a new GoRuntimeModule instance backed by a DB pool.
func NewGoRuntimeModule(dbPool *pgxpool.Pool, logger Logger) *internalruntime.GoRuntimeModule {
	return internalruntime.NewGoRuntimeModule(dbPool, logger)
}

// NewGoRuntimeManager constructs a Go runtime manager.
func NewGoRuntimeManager(logger Logger, db *sql.DB, nk RuntimeModule) *GoRuntimeManager {
	return internalruntime.NewGoRuntimeManager(logger, db, nk)
}

// NewCronScheduler constructs a new runtime CronScheduler.
func NewCronScheduler(registry *HookRegistry, logger Logger, db *sql.DB, nk RuntimeModule) *CronScheduler {
	return internalruntime.NewCronScheduler(registry, logger, db, nk)
}

// NewCronClusterLock creates a cluster lock backed by Redis.
func NewCronClusterLock(client *redis.Client, nodeID string) *internalruntime.CronClusterLock {
	return internalruntime.NewCronClusterLock(client, nodeID)
}

// MapLuaNK registers Lua nk bindings.
func MapLuaNK(l *lua.LState, nk RuntimeModule, reg *HookRegistry) {
	internalruntime.MapLuaNK(l, nk, reg)
}

// MapJSNK registers JavaScript nk bindings.
func MapJSNK(r *goja.Runtime, nk RuntimeModule, timeout time.Duration, reg *HookRegistry) {
	internalruntime.MapJSNK(r, nk, timeout, reg)
}

// LoadLuaModules loads Lua scripts from runtime path.
func LoadLuaModules(ctx context.Context, path string, l *lua.LState, logger Logger) error {
	return internalruntime.LoadLuaModules(ctx, path, l, logger)
}

// LoadJSModules loads JavaScript scripts from runtime path.
func LoadJSModules(ctx context.Context, path string, r *goja.Runtime, logger Logger) error {
	return internalruntime.LoadJSModules(ctx, path, r, logger)
}
