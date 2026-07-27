package runtime

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"plugin"
	"reflect"
	"runtime/debug"
	"strings"
	"sync"

	"ultimate-game-server/internal/storage"
)

// RuntimeType identifies the execution runtime for a registered handler.
type RuntimeType int

const (
	// RuntimeGo is the Go native runtime (highest precedence).
	RuntimeGo RuntimeType = iota
	// RuntimeLua is the Lua VM sandbox runtime.
	RuntimeLua
	// RuntimeJS is the JavaScript VM sandbox runtime (lowest precedence).
	RuntimeJS
)

// InitModuleFunc is the required entry point signature for Go runtime modules.
// Go plugins (.so files) must export a function with this exact signature named "InitModule".
type InitModuleFunc func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, initializer Initializer) error

// GoRuntimeManager manages Go native runtime modules loaded from .so plugins.
// It handles plugin loading, initialization, panic recovery, and runtime precedence.
type GoRuntimeManager struct {
	mu     sync.RWMutex
	logger Logger
	db     *sql.DB
	nk     RuntimeModule

	// Registry holds all Go-registered handlers
	registry *HookRegistry

	// loaded tracks successfully loaded plugin paths
	loaded []string
}

// NewGoRuntimeManager creates a new Go runtime manager with the given dependencies.
func NewGoRuntimeManager(logger Logger, db *sql.DB, nk RuntimeModule) *GoRuntimeManager {
	return &GoRuntimeManager{
		logger:   logger,
		db:       db,
		nk:       nk,
		registry: NewHookRegistry(),
		loaded:   make([]string, 0),
	}
}

// LoadPlugins scans the specified directory for .so files and loads them as Go plugins.
// Each plugin must export an InitModule function matching InitModuleFunc.
// Plugins are loaded in alphabetical order. If a plugin fails to load, the error is
// logged and the next plugin is attempted — the server does not crash.
func (m *GoRuntimeManager) LoadPlugins(ctx context.Context, dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			m.logger.Info("Go runtime modules directory not found: %s (skipping)", dir)
			return nil
		}
		return fmt.Errorf("failed to read runtime modules directory %s: %w", dir, err)
	}

	// Try to load plugin manifest for checksum verification
	var manifest map[string]string
	manifestPath := filepath.Join(dir, "plugin_manifest.json")
	if manifestData, err := os.ReadFile(manifestPath); err == nil {
		if err := json.Unmarshal(manifestData, &manifest); err != nil {
			m.logger.Warn("Failed to parse plugin manifest: %v. Checksum verification will be skipped.", err)
		} else {
			m.logger.Info("Loaded plugin manifest for checksum verification. Found %d entries.", len(manifest))
		}
	} else if !os.IsNotExist(err) {
		m.logger.Warn("Failed to read plugin manifest: %v. Checksum verification will be skipped.", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if filepath.Ext(entry.Name()) != ".so" {
			continue
		}

		path := filepath.Join(dir, entry.Name())

		// Perform checksum verification if manifest is available
		if manifest != nil {
			if err := m.verifyPluginChecksum(path, manifest); err != nil {
				m.logger.Error("Plugin verification failed for %s: %v. Rejecting module.", entry.Name(), err)
				continue
			}
		}

		if err := m.loadPlugin(ctx, path); err != nil {
			m.logger.Error("Failed to load Go plugin %s: %v", path, err)
			continue // Do not crash; skip and continue loading other modules
		}
		m.loaded = append(m.loaded, path)
		m.logger.Info("Loaded Go runtime module: %s", entry.Name())
	}

	m.logger.Info("Go runtime: loaded %d module(s)", len(m.loaded))
	return nil
}

// verifyPluginChecksum computes the SHA-256 hash of a file and compares it to the expected hash in the manifest.
func (m *GoRuntimeManager) verifyPluginChecksum(path string, manifest map[string]string) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open file for checksum: %w", err)
	}
	defer file.Close()

	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return fmt.Errorf("failed to compute file checksum: %w", err)
	}

	actualHash := hex.EncodeToString(hasher.Sum(nil))
	filename := filepath.Base(path)
	expectedHash, exists := manifest[filename]
	if !exists {
		// Also check by full path in case manifest uses absolute paths
		expectedHash, exists = manifest[path]
	}

	if !exists {
		return fmt.Errorf("file not registered in plugin manifest")
	}

	if actualHash != expectedHash {
		return fmt.Errorf("checksum mismatch: expected %s, got %s", expectedHash, actualHash)
	}

	return nil
}

// loadPlugin loads a single .so plugin file and executes its InitModule function.
func (m *GoRuntimeManager) loadPlugin(ctx context.Context, path string) error {
	// 1. Open the shared object plugin
	p, err := plugin.Open(path)
	if err != nil {
		return fmt.Errorf("plugin.Open failed: %w", err)
	}

	// 2. Look up the InitModule symbol
	sym, err := p.Lookup("InitModule")
	if err != nil {
		return fmt.Errorf("plugin missing InitModule export: %w", err)
	}

	// 3. Type-assert to the required signature
	initFn, ok := sym.(func(context.Context, Logger, *sql.DB, RuntimeModule, Initializer) error)
	if !ok {
		return fmt.Errorf("plugin InitModule has wrong signature (expected InitModuleFunc)")
	}

	// 4. Create an initializer that registers into our HookRegistry
	init := &goInitializer{registry: m.registry, nk: m.nk}
	if grm, ok := m.nk.(*GoRuntimeModule); ok {
		init.storageIndex = grm.storageIndex
	}

	// 5. Execute InitModule with panic recovery
	if err := m.safeCall(func() error {
		return initFn(ctx, m.logger, m.db, m.nk, init)
	}); err != nil {
		return fmt.Errorf("InitModule execution failed: %w", err)
	}

	return nil
}

// InvokeRPC invokes a Go-registered RPC handler by function ID.
// All invocations are wrapped with panic recovery.
func (m *GoRuntimeManager) InvokeRPC(ctx context.Context, id string, payload string) (result string, err error) {
	handler, exists := m.registry.GetRPC(id)
	if !exists {
		return "", fmt.Errorf("Go RPC %q not found", id)
	}

	err = m.safeCall(func() error {
		var rpcErr error
		result, rpcErr = handler(ctx, m.logger, m.db, m.nk, payload)
		return rpcErr
	})
	return result, err
}

// InvokeBeforeHook invokes a Go-registered before hook by name.
// All invocations are wrapped with panic recovery.
func (m *GoRuntimeManager) InvokeBeforeHook(ctx context.Context, name string, in interface{}) (out interface{}, err error) {
	hook, exists := m.registry.GetBefore(name)
	if !exists {
		return nil, fmt.Errorf("Go before hook %q not found", name)
	}

	err = m.safeCall(func() error {
		var hookErr error
		out, hookErr = hook(ctx, m.logger, m.db, m.nk, in)
		return hookErr
	})
	return out, err
}

// InvokeAfterHook invokes a Go-registered after hook by name.
// All invocations are wrapped with panic recovery.
func (m *GoRuntimeManager) InvokeAfterHook(ctx context.Context, name string, response interface{}, request interface{}) error {
	hook, exists := m.registry.GetAfter(name)
	if !exists {
		return fmt.Errorf("Go after hook %q not found", name)
	}

	return m.safeCall(func() error {
		return hook(ctx, m.logger, m.db, m.nk, response, request)
	})
}

// HasRPC checks if a Go RPC handler is registered for the given function ID.
func (m *GoRuntimeManager) HasRPC(id string) bool {
	_, exists := m.registry.GetRPC(id)
	return exists
}

// HasBeforeHook checks if a before hook is registered (Go → Lua → JS).
func (m *GoRuntimeManager) HasBeforeHook(name string) bool {
	_, _, _, found := m.registry.GetBeforeHook(name)
	return found
}

// HasAfterHook checks if an after hook is registered (Go → Lua → JS).
func (m *GoRuntimeManager) HasAfterHook(name string) bool {
	_, _, _, found := m.registry.GetAfterHook(name)
	return found
}

func (m *GoRuntimeManager) safeCall(fn func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("runtime panic recovered: %v\n%s", r, debug.Stack())
			m.logger.Error("Go runtime panic: %v", r)
		}
	}()
	return fn()
}

// Logger returns the logger.
func (m *GoRuntimeManager) Logger() Logger {
	return m.logger
}

// DB returns the database connection.
func (m *GoRuntimeManager) DB() *sql.DB {
	return m.db
}

// NK returns the runtime module.
func (m *GoRuntimeManager) NK() RuntimeModule {
	return m.nk
}

// Registry returns the hook registry.
func (m *GoRuntimeManager) Registry() *HookRegistry {
	return m.registry
}

// goInitializer implements the Initializer interface for Go modules.
// It captures registrations into the HookRegistry during InitModule execution.
type goInitializer struct {
	registry     *HookRegistry
	storageIndex *storage.BlugeStorageIndex
	nk           RuntimeModule
}

func (i *goInitializer) RegisterRpc(id string, fn RPCHandler) error {
	if id == "" {
		return fmt.Errorf("RPC id must not be empty")
	}
	i.registry.RegisterRPC(strings.ToLower(id), fn)
	return nil
}

func (i *goInitializer) RegisterBeforeRt(id string, fn BeforeHook) error {
	if id == "" {
		return fmt.Errorf("before hook id must not be empty")
	}
	i.registry.RegisterBefore(id, fn)
	return nil
}

func (i *goInitializer) RegisterAfterRt(id string, fn AfterHook) error {
	if id == "" {
		return fmt.Errorf("after hook id must not be empty")
	}
	i.registry.RegisterAfter(id, fn)
	return nil
}

func (i *goInitializer) RegisterMatch(name string, fn MatchHandlerFactory) error {
	return i.registry.RegisterMatch(name, fn)
}

func (i *goInitializer) RegisterMatchmakerMatched(fn MatchmakerMatchedHandler) error {
	i.registry.RegisterMatchmakerMatched(fn)
	return nil
}

func (i *goInitializer) RegisterMatchmakerOverride(fn MatchmakerOverrideHandler) error {
	i.registry.RegisterMatchmakerOverride(fn)
	return nil
}

func (i *goInitializer) RegisterMatchmakerProcessor(fn MatchmakerProcessorHandler) error {
	i.registry.RegisterMatchmakerProcessor(fn)
	return nil
}

func (i *goInitializer) RegisterLeaderboardReset(fn LeaderboardResetHandler) error {
	i.registry.RegisterLeaderboardReset(fn)
	return nil
}

func (i *goInitializer) RegisterTournamentEnd(fn TournamentEndHandler) error {
	i.registry.RegisterTournamentEnd(fn)
	return nil
}

func (i *goInitializer) RegisterTournamentReset(fn TournamentResetHandler) error {
	i.registry.RegisterTournamentReset(fn)
	return nil
}// Helper to adapt hook parameters dynamically
func convertHookParam(src interface{}, dst interface{}) error {
	bytes, err := json.Marshal(src)
	if err != nil {
		return err
	}
	return json.Unmarshal(bytes, dst)
}

func convertHookParamBack(src interface{}, orig interface{}) (interface{}, error) {
	bytes, err := json.Marshal(src)
	if err != nil {
		return nil, err
	}
	origType := reflect.TypeOf(orig)
	if origType.Kind() == reflect.Ptr {
		newVal := reflect.New(origType.Elem()).Interface()
		if err := json.Unmarshal(bytes, newVal); err != nil {
			return nil, err
		}
		return newVal, nil
	}
	newVal := reflect.New(origType).Interface()
	if err := json.Unmarshal(bytes, newVal); err != nil {
		return nil, err
	}
	return reflect.ValueOf(newVal).Elem().Interface(), nil
}

// Specific type-safe before hooks
func (i *goInitializer) RegisterBeforeAuthenticateEmail(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *AuthenticateEmailRequest) (*AuthenticateEmailRequest, error)) error {
	wrapped := func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in interface{}) (interface{}, error) {
		var req AuthenticateEmailRequest
		if err := convertHookParam(in, &req); err != nil {
			return nil, err
		}
		res, err := fn(ctx, logger, db, nk, &req)
		if err != nil {
			return nil, err
		}
		return convertHookParamBack(res, in)
	}
	i.registry.RegisterBefore("AuthenticateEmail", wrapped)
	return nil
}

func (i *goInitializer) RegisterBeforeWriteStorageObjects(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *WriteStorageObjectsRequest) (*WriteStorageObjectsRequest, error)) error {
	wrapped := func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in interface{}) (interface{}, error) {
		var req WriteStorageObjectsRequest
		if err := convertHookParam(in, &req); err != nil {
			return nil, err
		}
		res, err := fn(ctx, logger, db, nk, &req)
		if err != nil {
			return nil, err
		}
		return convertHookParamBack(res, in)
	}
	i.registry.RegisterBefore("WriteStorageObjects", wrapped)
	return nil
}

func (i *goInitializer) RegisterBeforeAddFriends(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *AddFriendsRequest) (*AddFriendsRequest, error)) error {
	wrapped := func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in interface{}) (interface{}, error) {
		var req AddFriendsRequest
		if err := convertHookParam(in, &req); err != nil {
			return nil, err
		}
		res, err := fn(ctx, logger, db, nk, &req)
		if err != nil {
			return nil, err
		}
		return convertHookParamBack(res, in)
	}
	i.registry.RegisterBefore("AddFriends", wrapped)
	return nil
}

func (i *goInitializer) RegisterBeforeJoinGroup(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *JoinGroupRequest) (*JoinGroupRequest, error)) error {
	wrapped := func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in interface{}) (interface{}, error) {
		var req JoinGroupRequest
		if err := convertHookParam(in, &req); err != nil {
			return nil, err
		}
		res, err := fn(ctx, logger, db, nk, &req)
		if err != nil {
			return nil, err
		}
		return convertHookParamBack(res, in)
	}
	i.registry.RegisterBefore("JoinGroup", wrapped)
	return nil
}

// Specific type-safe after hooks
func (i *goInitializer) RegisterAfterAuthenticateEmail(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *Session, in *AuthenticateEmailRequest) error) error {
	wrapped := func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out interface{}, in interface{}) error {
		var outStruct Session
		var inStruct AuthenticateEmailRequest
		if err := convertHookParam(out, &outStruct); err != nil {
			return err
		}
		if err := convertHookParam(in, &inStruct); err != nil {
			return err
		}
		return fn(ctx, logger, db, nk, &outStruct, &inStruct)
	}
	i.registry.RegisterAfter("AuthenticateEmail", wrapped)
	return nil
}

func (i *goInitializer) RegisterAfterWriteStorageObjects(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out *StorageObjectAcks, in *WriteStorageObjectsRequest) error) error {
	wrapped := func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out interface{}, in interface{}) error {
		var outStruct StorageObjectAcks
		var inStruct WriteStorageObjectsRequest
		if err := convertHookParam(out, &outStruct); err != nil {
			return err
		}
		if err := convertHookParam(in, &inStruct); err != nil {
			return err
		}
		return fn(ctx, logger, db, nk, &outStruct, &inStruct)
	}
	i.registry.RegisterAfter("WriteStorageObjects", wrapped)
	return nil
}

func (i *goInitializer) RegisterAfterAddFriends(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *AddFriendsRequest) error) error {
	wrapped := func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out interface{}, in interface{}) error {
		var inStruct AddFriendsRequest
		if err := convertHookParam(in, &inStruct); err != nil {
			return err
		}
		return fn(ctx, logger, db, nk, &inStruct)
	}
	i.registry.RegisterAfter("AddFriends", wrapped)
	return nil
}

func (i *goInitializer) RegisterAfterJoinGroup(fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in *JoinGroupRequest) error) error {
	wrapped := func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out interface{}, in interface{}) error {
		var inStruct JoinGroupRequest
		if err := convertHookParam(in, &inStruct); err != nil {
			return err
		}
		return fn(ctx, logger, db, nk, &inStruct)
	}
	i.registry.RegisterAfter("JoinGroup", wrapped)
	return nil
}

func (i *goInitializer) RegisterEvent(fn EventHandler) error {
	i.registry.RegisterEvent(fn)
	return nil
}

func (i *goInitializer) RegisterEventSessionStart(fn EventHandler) error {
	i.registry.RegisterEventSessionStart(fn)
	return nil
}

func (i *goInitializer) RegisterEventSessionEnd(fn EventHandler) error {
	i.registry.RegisterEventSessionEnd(fn)
	return nil
}

func (i *goInitializer) RegisterShutdown(fn ShutdownHandler) error {
	i.registry.RegisterShutdown(fn)
	return nil
}

func (i *goInitializer) RegisterHttp(pathPattern string, handler func(http.ResponseWriter, *http.Request), methods ...string) error {
	i.registry.RegisterHttp(pathPattern, handler, methods...)
	return nil
}

func (i *goInitializer) RegisterConsoleHttp(pathPattern string, handler func(http.ResponseWriter, *http.Request), methods ...string) error {
	i.registry.RegisterConsoleHttp(pathPattern, handler, methods...)
	return nil
}

func (i *goInitializer) RegisterCron(name, schedule string, fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule) error) error {
	return i.registry.RegisterCron(name, &CronJob{Schedule: schedule, Handler: fn})
}

func (i *goInitializer) RegisterStorageIndex(name, collection, key string, fields, sortableFields []string, maxEntries int, indexOnly bool) error {
	if i.storageIndex == nil {
		return fmt.Errorf("storage index not configured")
	}
	return i.storageIndex.CreateIndex(storage.StorageIndexDefinition{
		Name: name, Collection: collection, Key: key,
		Fields: fields, SortableFields: sortableFields,
		MaxEntries: maxEntries, IndexOnly: indexOnly,
	})
}

func (i *goInitializer) RegisterStorageIndexFilter(indexName string, fn func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, write *StorageWrite) bool) error {
	if i.storageIndex == nil {
		return fmt.Errorf("storage index not configured")
	}
	i.storageIndex.RegisterFilter(indexName, func(ctx context.Context, obj *storage.StorageObject) (bool, error) {
		sw := &StorageWrite{
			Collection: obj.Collection, Key: obj.Key, UserID: obj.UserID, Value: obj.Value, Version: obj.Version,
			PermissionRead: int32(obj.Read), PermissionWrite: int32(obj.Write),
		}
		return fn(ctx, nil, nil, nil, sw), nil
	})
	return nil
}
