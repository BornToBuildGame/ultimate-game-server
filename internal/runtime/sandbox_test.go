package runtime

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSandbox_RunSuccess(t *testing.T) {
	// 64MB memory limit, 2 second timeout
	sb := NewSandbox(64*1024*1024, 2*time.Second)
	defer sb.Close()

	ctx := context.Background()
	result, err := sb.Run(ctx, "return 5 + 15")
	if err != nil {
		t.Fatalf("unexpected error running script: %v", err)
	}

	// Gopher-lua float formats 20 as "20"
	if result != "20" {
		t.Errorf("expected return value '20', got: %q", result)
	}
}

func TestSandbox_InstructionLimitExceeded(t *testing.T) {
	// 64MB memory, 100ms timeout
	sb := NewSandbox(64*1024*1024, 100*time.Millisecond)
	defer sb.Close()

	ctx := context.Background()
	// Loop that exceeds 10M instructions quickly or times out context
	_, err := sb.Run(ctx, "while true do end")
	if err == nil {
		t.Fatal("expected execution of infinite loop to fail, but it succeeded")
	}

	// The error should contain either instruction budget exceeded or context deadline exceeded
	errStr := err.Error()
	if !strings.Contains(errStr, "instruction") && !strings.Contains(errStr, "context deadline") && !strings.Contains(errStr, "deadline exceeded") {
		t.Errorf("expected limit/cancellation error, got: %v", err)
	}
}

// testLogger implements Logger for testing purposes.
type testLogger struct {
	t *testing.T
}

func (l *testLogger) Debug(format string, args ...interface{}) { l.t.Logf("[DEBUG] "+format, args...) }
func (l *testLogger) Info(format string, args ...interface{})  { l.t.Logf("[INFO] "+format, args...) }
func (l *testLogger) Warn(format string, args ...interface{})  { l.t.Logf("[WARN] "+format, args...) }
func (l *testLogger) Error(format string, args ...interface{}) { l.t.Logf("[ERROR] "+format, args...) }

func TestHookRegistry(t *testing.T) {
	registry := NewHookRegistry()
	logger := &testLogger{t: t}

	// 1. Test RPC registry with full Go native signature
	rpcName := "rpc_test"
	registry.RegisterRPC(rpcName, func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, payload string) (string, error) {
		return "hello " + payload, nil
	})

	handler, exists := registry.GetRPC(rpcName)
	if !exists {
		t.Fatal("expected RPC handler to exist")
	}

	res, err := handler(context.Background(), logger, nil, nil, "world")
	if err != nil {
		t.Fatalf("unexpected error running rpc: %v", err)
	}
	if res != "hello world" {
		t.Errorf("expected 'hello world', got %q", res)
	}

	// 2. Test Before Hooks with full Go native signature
	beforeName := "beforeWriteStorage"
	registry.RegisterBefore(beforeName, func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in interface{}) (interface{}, error) {
		return "modified", nil
	})

	beforeHook, exists := registry.GetBefore(beforeName)
	if !exists {
		t.Fatal("expected before hook to exist")
	}
	resBefore, _ := beforeHook(context.Background(), logger, nil, nil, "input")
	if resBefore != "modified" {
		t.Errorf("expected 'modified', got %v", resBefore)
	}

	// 3. Test After Hooks with full Go native signature
	afterName := "afterWriteStorage"
	registry.RegisterAfter(afterName, func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out interface{}, in interface{}) error {
		return nil
	})

	afterHook, exists := registry.GetAfter(afterName)
	if !exists {
		t.Fatal("expected after hook to exist")
	}
	err = afterHook(context.Background(), logger, nil, nil, "response", "request")
	if err != nil {
		t.Fatalf("unexpected error from after hook: %v", err)
	}
}

func TestGoInitializer(t *testing.T) {
	registry := NewHookRegistry()
	init := &goInitializer{registry: registry}

	// 1. Test RegisterRpc
	err := init.RegisterRpc("test_rpc", func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, payload string) (string, error) {
		return "result:" + payload, nil
	})
	if err != nil {
		t.Fatalf("unexpected error registering RPC: %v", err)
	}

	handler, exists := registry.GetRPC("test_rpc")
	if !exists {
		t.Fatal("expected RPC handler to be registered via Initializer")
	}

	logger := &testLogger{t: t}
	res, err := handler(context.Background(), logger, nil, nil, "data")
	if err != nil {
		t.Fatalf("unexpected error calling RPC: %v", err)
	}
	if res != "result:data" {
		t.Errorf("expected 'result:data', got %q", res)
	}

	// 2. Test RegisterRpc with empty ID fails
	err = init.RegisterRpc("", func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, payload string) (string, error) {
		return "", nil
	})
	if err == nil {
		t.Fatal("expected error when registering RPC with empty ID")
	}

	// 3. Test RegisterBeforeRt
	err = init.RegisterBeforeRt("AuthenticateEmail", func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in interface{}) (interface{}, error) {
		return in, nil
	})
	if err != nil {
		t.Fatalf("unexpected error registering before hook: %v", err)
	}

	_, exists = registry.GetBefore("AuthenticateEmail")
	if !exists {
		t.Fatal("expected before hook to be registered via Initializer")
	}

	// 4. Test RegisterAfterRt
	err = init.RegisterAfterRt("WriteStorageObjects", func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out interface{}, in interface{}) error {
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error registering after hook: %v", err)
	}

	_, exists = registry.GetAfter("WriteStorageObjects")
	if !exists {
		t.Fatal("expected after hook to be registered via Initializer")
	}
}

func TestGoRuntimeManager_PanicRecovery(t *testing.T) {
	logger := &testLogger{t: t}
	manager := NewGoRuntimeManager(logger, nil, nil)

	// Register an RPC that panics
	manager.registry.RegisterRPC("panic_rpc", func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, payload string) (string, error) {
		panic("deliberate test panic")
	})

	// InvokeRPC should recover the panic and return an error
	_, err := manager.InvokeRPC(context.Background(), "panic_rpc", "{}")
	if err == nil {
		t.Fatal("expected error from panicking RPC, got nil")
	}
	if !strings.Contains(err.Error(), "panic recovered") {
		t.Errorf("expected panic recovery error, got: %v", err)
	}
}

func TestGoRuntimeManager_InvokeRPC(t *testing.T) {
	logger := &testLogger{t: t}
	manager := NewGoRuntimeManager(logger, nil, nil)

	// Register a normal RPC
	manager.registry.RegisterRPC("echo", func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, payload string) (string, error) {
		return "echo:" + payload, nil
	})

	result, err := manager.InvokeRPC(context.Background(), "echo", "hello")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "echo:hello" {
		t.Errorf("expected 'echo:hello', got %q", result)
	}

	// Non-existent RPC
	_, err = manager.InvokeRPC(context.Background(), "nonexistent", "")
	if err == nil {
		t.Fatal("expected error for non-existent RPC")
	}
}

func TestGoRuntimeManager_HasChecks(t *testing.T) {
	logger := &testLogger{t: t}
	manager := NewGoRuntimeManager(logger, nil, nil)

	// Nothing registered yet
	if manager.HasRPC("test") {
		t.Fatal("expected HasRPC to return false before registration")
	}
	if manager.HasBeforeHook("test") {
		t.Fatal("expected HasBeforeHook to return false before registration")
	}
	if manager.HasAfterHook("test") {
		t.Fatal("expected HasAfterHook to return false before registration")
	}

	// Register handlers
	manager.registry.RegisterRPC("test", func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, payload string) (string, error) {
		return "", nil
	})
	manager.registry.RegisterBefore("test", func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, in interface{}) (interface{}, error) {
		return nil, nil
	})
	manager.registry.RegisterAfter("test", func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule, out interface{}, in interface{}) error {
		return nil
	})

	if !manager.HasRPC("test") {
		t.Fatal("expected HasRPC to return true after registration")
	}
	if !manager.HasBeforeHook("test") {
		t.Fatal("expected HasBeforeHook to return true after registration")
	}
	if !manager.HasAfterHook("test") {
		t.Fatal("expected HasAfterHook to return true after registration")
	}
}

func TestGoRuntimeManager_VerifyPluginChecksum(t *testing.T) {
	logger := &testLogger{t: t}
	manager := NewGoRuntimeManager(logger, nil, nil)

	// Create a temporary directory for our dummy plugin file
	tempDir, err := os.MkdirTemp("", "plugin_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	pluginPath := filepath.Join(tempDir, "my_plugin.so")
	pluginContent := []byte("dummy plugin binary content")
	if err := os.WriteFile(pluginPath, pluginContent, 0644); err != nil {
		t.Fatalf("failed to write dummy plugin: %v", err)
	}

	// Compute actual hash
	hasher := sha256.New()
	hasher.Write(pluginContent)
	actualHash := hex.EncodeToString(hasher.Sum(nil))

	// Test case 1: Correct hash matching filename
	manifest := map[string]string{
		"my_plugin.so": actualHash,
	}
	if err := manager.verifyPluginChecksum(pluginPath, manifest); err != nil {
		t.Errorf("expected checksum verification to succeed: %v", err)
	}

	// Test case 2: Correct hash matching full path
	manifestWithPath := map[string]string{
		pluginPath: actualHash,
	}
	if err := manager.verifyPluginChecksum(pluginPath, manifestWithPath); err != nil {
		t.Errorf("expected checksum verification by path to succeed: %v", err)
	}

	// Test case 3: Incorrect hash
	badManifest := map[string]string{
		"my_plugin.so": "invalid_hash_value_here_1234567890abcdef",
	}
	if err := manager.verifyPluginChecksum(pluginPath, badManifest); err == nil {
		t.Error("expected checksum verification to fail with mismatched hash, but it succeeded")
	}

	// Test case 4: Missing manifest entry
	emptyManifest := map[string]string{}
	if err := manager.verifyPluginChecksum(pluginPath, emptyManifest); err == nil {
		t.Error("expected checksum verification to fail with missing manifest entry, but it succeeded")
	}
}
