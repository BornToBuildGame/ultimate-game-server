package runtime

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"plugin"
	"runtime/debug"
	"sync"
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
	init := &goInitializer{registry: m.registry}

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

// HasBeforeHook checks if a Go before hook is registered for the given name.
func (m *GoRuntimeManager) HasBeforeHook(name string) bool {
	_, exists := m.registry.GetBefore(name)
	return exists
}

// HasAfterHook checks if a Go after hook is registered for the given name.
func (m *GoRuntimeManager) HasAfterHook(name string) bool {
	_, exists := m.registry.GetAfter(name)
	return exists
}

// Registry returns the underlying HookRegistry for direct access.
func (m *GoRuntimeManager) Registry() *HookRegistry {
	return m.registry
}

// safeCall wraps a function invocation with panic recovery to prevent
// Go module panics from crashing the server process.
func (m *GoRuntimeManager) safeCall(fn func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			stack := debug.Stack()
			m.logger.Error("Go runtime panic recovered: %v\n%s", r, string(stack))
			err = fmt.Errorf("runtime panic recovered: %v", r)
		}
	}()
	return fn()
}

// goInitializer implements the Initializer interface for Go modules.
// It captures registrations into the HookRegistry during InitModule execution.
type goInitializer struct {
	registry *HookRegistry
}

func (i *goInitializer) RegisterRpc(id string, fn RPCHandler) error {
	if id == "" {
		return fmt.Errorf("RPC id must not be empty")
	}
	i.registry.RegisterRPC(id, fn)
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
	// TODO: Store matchmaker matched handler in registry
	return nil
}

func (i *goInitializer) RegisterLeaderboardReset(fn LeaderboardResetHandler) error {
	// TODO: Store leaderboard reset handler in registry
	return nil
}

func (i *goInitializer) RegisterTournamentEnd(fn TournamentEndHandler) error {
	// TODO: Store tournament end handler in registry
	return nil
}

func (i *goInitializer) RegisterTournamentReset(fn TournamentResetHandler) error {
	// TODO: Store tournament reset handler in registry
	return nil
}

func (i *goInitializer) RegisterEvent(fn EventHandler) error {
	i.registry.RegisterEvent(fn)
	return nil
}
