package runtime

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"
)

// Logger provides structured logging for runtime modules.
type Logger interface {
	Debug(format string, args ...interface{})
	Info(format string, args ...interface{})
	Warn(format string, args ...interface{})
	Error(format string, args ...interface{})
}

// RuntimeModule provides access to all server-side APIs from runtime code.
// This interface is injected into all Go native runtime handler invocations.
type RuntimeModule interface {
	// Storage operations
	StorageRead(ctx context.Context, reads []*StorageRead) ([]*StorageObject, error)
	StorageWrite(ctx context.Context, writes []*StorageWrite) ([]*StorageObjectAck, error)
	StorageDelete(ctx context.Context, deletes []*StorageDelete) error

	// Wallet operations
	WalletUpdate(ctx context.Context, userID string, changeset map[string]int64, metadata map[string]interface{}, updateLedger bool) (map[string]int64, error)

	// Account operations
	AccountGetId(ctx context.Context, userID string) (*Account, error)

	// Leaderboard operations
	LeaderboardRecordWrite(ctx context.Context, id, ownerID, username string, score, subscore int64, metadata map[string]interface{}) (*LeaderboardRecord, error)

	// Notification operations
	NotificationSend(ctx context.Context, userID, subject string, content map[string]interface{}, code int, senderID string, persistent bool) error

	// Match operations
	MatchCreate(ctx context.Context, module string, params map[string]interface{}) (string, error)
}

type StorageRead struct {
	Collection string `json:"collection"`
	Key        string `json:"key"`
	UserID     string `json:"user_id"`
}

type StorageWrite struct {
	Collection      string `json:"collection"`
	Key             string `json:"key"`
	UserID          string `json:"user_id"`
	Value           string `json:"value"`
	Version         string `json:"version"`
	PermissionRead  int32  `json:"permission_read"`
	PermissionWrite int32  `json:"permission_write"`
}

type StorageDelete struct {
	Collection string `json:"collection"`
	Key        string `json:"key"`
	UserID     string `json:"user_id"`
	Version    string `json:"version"`
}

type StorageObject struct {
	Collection      string    `json:"collection"`
	Key             string    `json:"key"`
	UserID          string    `json:"user_id"`
	Value           string    `json:"value"`
	Version         string    `json:"version"`
	PermissionRead  int32     `json:"permission_read"`
	PermissionWrite int32     `json:"permission_write"`
	CreateTime      time.Time `json:"create_time"`
	UpdateTime      time.Time `json:"update_time"`
}

type StorageObjectAck struct {
	Collection string    `json:"collection"`
	Key        string    `json:"key"`
	UserID     string    `json:"user_id"`
	Version    string    `json:"version"`
	CreateTime time.Time `json:"create_time"`
	UpdateTime time.Time `json:"update_time"`
}

type Account struct {
	ID         string    `json:"id"`
	Username   string    `json:"username"`
	CreateTime time.Time `json:"create_time"`
	UpdateTime time.Time `json:"update_time"`
}

type LeaderboardRecord struct {
	LeaderboardID string    `json:"leaderboard_id"`
	OwnerID       string    `json:"owner_id"`
	Username      string    `json:"username"`
	Score         int64     `json:"score"`
	Subscore      int64     `json:"subscore"`
	NumScore      int       `json:"num_score"`
	MaxNumScore   int       `json:"max_num_score"`
	Metadata      string    `json:"metadata"`
	CreateTime    time.Time `json:"create_time"`
	UpdateTime    time.Time `json:"update_time"`
	ExpiryTime    time.Time `json:"expiry_time"`
	Rank          int64     `json:"rank"`
}

// RPCHandler represents a custom client-callable RPC endpoint handler.
// Go native handlers receive logger, db, and nk for full server API access.
type RPCHandler func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, payload string) (string, error)

// BeforeHook represents an HTTP/gRPC before request interceptor.
// Go native before hooks receive logger, db, and nk for full server API access.
type BeforeHook func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in interface{}) (interface{}, error)

// AfterHook represents an HTTP/gRPC after request interceptor.
// Go native after hooks receive logger, db, and nk for full server API access.
type AfterHook func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out interface{}, in interface{}) error

// EventHandler represents an asynchronous event handler.
type EventHandler func(ctx context.Context, logger Logger, evt *Event)

// Event represents a server-side event (session start/end, matchmaker match, etc.).
type Event struct {
	Name       string
	Properties map[string]string
	Timestamp  int64
}

// MatchHandlerFactory creates a new match handler instance.
type MatchHandlerFactory func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule) (Match, error)

// Match represents an authoritative match handler.
type Match interface {
	MatchInit(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, params map[string]interface{}) (interface{}, int, string)
	MatchJoin(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, dispatcher interface{}, tick int64, state interface{}, presences []interface{}) interface{}
	MatchLeave(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, dispatcher interface{}, tick int64, state interface{}, presences []interface{}) interface{}
	MatchLoop(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, dispatcher interface{}, tick int64, state interface{}, messages []interface{}) interface{}
	MatchTerminate(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, dispatcher interface{}, tick int64, state interface{}, graceSeconds int) interface{}
}

// MatchmakerMatchedHandler handles matchmaker match events.
type MatchmakerMatchedHandler func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, entries []interface{}) (string, error)

// LeaderboardResetHandler handles leaderboard reset events.
type LeaderboardResetHandler func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, leaderboardID string, reset int64) error

// TournamentEndHandler handles tournament end events.
type TournamentEndHandler func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, tournamentID string, end int64, reset int64) error

// TournamentResetHandler handles tournament reset events.
type TournamentResetHandler func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, tournamentID string, end int64, reset int64) error

// CronJob represents a scheduled event job.
type CronJob struct {
	Schedule string
	Handler  func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule) error
}

// Initializer provides registration methods during module initialization.
// Available only during InitModule execution — do not store or cache globally.
type Initializer interface {
	RegisterRpc(id string, fn RPCHandler) error
	RegisterBeforeRt(id string, fn BeforeHook) error
	RegisterAfterRt(id string, fn AfterHook) error
	RegisterMatch(name string, fn MatchHandlerFactory) error
	RegisterMatchmakerMatched(fn MatchmakerMatchedHandler) error
	RegisterLeaderboardReset(fn LeaderboardResetHandler) error
	RegisterTournamentEnd(fn TournamentEndHandler) error
	RegisterTournamentReset(fn TournamentResetHandler) error
	RegisterEvent(fn EventHandler) error
}

// HookRegistry stores registered custom RPCs, before/after hooks, and cron jobs.
type HookRegistry struct {
	mu             sync.RWMutex
	beforeHooks    map[string]BeforeHook
	afterHooks     map[string]AfterHook
	rpcHooks       map[string]RPCHandler
	luaBeforeHooks map[string]string
	luaAfterHooks  map[string]string
	luaRpcHooks    map[string]string
	jsBeforeHooks  map[string]string
	jsAfterHooks   map[string]string
	jsRpcHooks     map[string]string
	eventHandlers  []EventHandler
	matchHandlers  map[string]MatchHandlerFactory
	cronJobs       map[string]*CronJob
}

// NewHookRegistry creates a new instance of HookRegistry.
func NewHookRegistry() *HookRegistry {
	return &HookRegistry{
		beforeHooks:    make(map[string]BeforeHook),
		afterHooks:     make(map[string]AfterHook),
		rpcHooks:       make(map[string]RPCHandler),
		luaBeforeHooks: make(map[string]string),
		luaAfterHooks:  make(map[string]string),
		luaRpcHooks:    make(map[string]string),
		jsBeforeHooks:  make(map[string]string),
		jsAfterHooks:   make(map[string]string),
		jsRpcHooks:     make(map[string]string),
		eventHandlers:  make([]EventHandler, 0),
		matchHandlers:  make(map[string]MatchHandlerFactory),
		cronJobs:       make(map[string]*CronJob),
	}
}

// RegisterBefore registers a request before hook.
func (hr *HookRegistry) RegisterBefore(name string, hook BeforeHook) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	hr.beforeHooks[name] = hook
}

// GetBefore retrieves a before hook.
func (hr *HookRegistry) GetBefore(name string) (BeforeHook, bool) {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	hook, ok := hr.beforeHooks[name]
	return hook, ok
}

// RegisterAfter registers a response after hook.
func (hr *HookRegistry) RegisterAfter(name string, hook AfterHook) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	hr.afterHooks[name] = hook
}

// GetAfter retrieves an after hook.
func (hr *HookRegistry) GetAfter(name string) (AfterHook, bool) {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	hook, ok := hr.afterHooks[name]
	return hook, ok
}

// RegisterRPC registers a custom RPC endpoint handler.
func (hr *HookRegistry) RegisterRPC(rpcName string, handler RPCHandler) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	hr.rpcHooks[rpcName] = handler
}

// GetRPC retrieves an RPC handler.
func (hr *HookRegistry) GetRPC(rpcName string) (RPCHandler, bool) {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	handler, ok := hr.rpcHooks[rpcName]
	return handler, ok
}

// RegisterEvent registers an event handler.
func (hr *HookRegistry) RegisterEvent(handler EventHandler) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	hr.eventHandlers = append(hr.eventHandlers, handler)
}

// RegisterMatch registers a match handler factory.
func (hr *HookRegistry) RegisterMatch(name string, factory MatchHandlerFactory) error {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	if _, exists := hr.matchHandlers[name]; exists {
		return fmt.Errorf("match handler %q already registered", name)
	}
	hr.matchHandlers[name] = factory
	return nil
}

// RegisterCron registers a scheduled background cron job.
func (hr *HookRegistry) RegisterCron(jobName string, cron *CronJob) error {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	if _, exists := hr.cronJobs[jobName]; exists {
		return fmt.Errorf("cron job %q already registered", jobName)
	}
	hr.cronJobs[jobName] = cron
	return nil
}

// GetBeforeHook retrieves a before hook, returning runtime type and script function name if script-based.
func (hr *HookRegistry) GetBeforeHook(name string) (BeforeHook, string, string, bool) {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	// 1. Go Native Hook (highest precedence)
	if hook, ok := hr.beforeHooks[name]; ok {
		return hook, "go", "", true
	}
	// 2. Gopher-Lua Hook
	if fnName, ok := hr.luaBeforeHooks[name]; ok {
		return nil, "lua", fnName, true
	}
	// 3. Goja JS Hook
	if fnName, ok := hr.jsBeforeHooks[name]; ok {
		return nil, "js", fnName, true
	}
	return nil, "", "", false
}

// GetAfterHook retrieves an after hook.
func (hr *HookRegistry) GetAfterHook(name string) (AfterHook, string, string, bool) {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	if hook, ok := hr.afterHooks[name]; ok {
		return hook, "go", "", true
	}
	if fnName, ok := hr.luaAfterHooks[name]; ok {
		return nil, "lua", fnName, true
	}
	if fnName, ok := hr.jsAfterHooks[name]; ok {
		return nil, "js", fnName, true
	}
	return nil, "", "", false
}

// GetRPCHook retrieves an RPC handler.
func (hr *HookRegistry) GetRPCHook(name string) (RPCHandler, string, string, bool) {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	if hook, ok := hr.rpcHooks[name]; ok {
		return hook, "go", "", true
	}
	if fnName, ok := hr.luaRpcHooks[name]; ok {
		return nil, "lua", fnName, true
	}
	if fnName, ok := hr.jsRpcHooks[name]; ok {
		return nil, "js", fnName, true
	}
	return nil, "", "", false
}

func (hr *HookRegistry) RegisterLuaBefore(name, fnName string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	hr.luaBeforeHooks[name] = fnName
}

func (hr *HookRegistry) RegisterLuaAfter(name, fnName string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	hr.luaAfterHooks[name] = fnName
}

func (hr *HookRegistry) RegisterLuaRPC(name, fnName string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	hr.luaRpcHooks[name] = fnName
}

func (hr *HookRegistry) RegisterJSBefore(name, fnName string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	hr.jsBeforeHooks[name] = fnName
}

func (hr *HookRegistry) RegisterJSAfter(name, fnName string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	hr.jsAfterHooks[name] = fnName
}

func (hr *HookRegistry) RegisterJSRPC(name, fnName string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	hr.jsRpcHooks[name] = fnName
}
