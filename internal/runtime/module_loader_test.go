package runtime

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/dop251/goja"
	lua "github.com/yuin/gopher-lua"
)

type silentLogger struct{}

func (silentLogger) Debug(string, ...interface{}) {}
func (silentLogger) Info(string, ...interface{})  {}
func (silentLogger) Warn(string, ...interface{})  {}
func (silentLogger) Error(string, ...interface{}) {}

func TestLoadLuaModules_RegistersRPC(t *testing.T) {
	dir := t.TempDir()
	src := `
nk.register_rpc(function(context, payload)
  return payload
end, "echo_init")
`
	if err := os.WriteFile(filepath.Join(dir, "test_init.lua"), []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	reg := NewHookRegistry()
	L := lua.NewState()
	defer L.Close()
	MapLuaNK(L, &mockRuntimeModule{}, reg)
	if err := LoadLuaModules(context.Background(), dir, L, silentLogger{}); err != nil {
		t.Fatal(err)
	}
	_, kind, fn, ok := reg.GetRPCHook("echo_init")
	if !ok || kind != "lua" || fn == "" {
		t.Fatalf("expected lua rpc registered: ok=%v kind=%s fn=%s", ok, kind, fn)
	}
}

func TestLoadJSModules_RegistersRPC(t *testing.T) {
	dir := t.TempDir()
	src := `
nk.register_rpc("echo_js", function(context, payload) { return payload; });
`
	if err := os.WriteFile(filepath.Join(dir, "test_init.js"), []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	reg := NewHookRegistry()
	vm := goja.New()
	MapJSNK(vm, &mockRuntimeModule{}, 0, reg)
	if err := LoadJSModules(context.Background(), dir, vm, silentLogger{}); err != nil {
		t.Fatal(err)
	}
	_, kind, fn, ok := reg.GetRPCHook("echo_js")
	if !ok || kind != "js" || fn == "" {
		t.Fatalf("expected js rpc registered: ok=%v kind=%s fn=%s", ok, kind, fn)
	}
}

func TestLoadLuaModules_MissingDir(t *testing.T) {
	L := lua.NewState()
	defer L.Close()
	if err := LoadLuaModules(context.Background(), filepath.Join(t.TempDir(), "missing"), L, silentLogger{}); err != nil {
		t.Fatalf("missing dir should be ok: %v", err)
	}
}
