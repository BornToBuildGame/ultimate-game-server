package runtime

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/dop251/goja"
	lua "github.com/yuin/gopher-lua"
)

// LoadLuaModules scans dir for *.lua files and evaluates them in alphabetical order.
// Modules register RPCs/hooks via nk.register_* at load time (reference-style init).
// Match handler files that only define match_* functions are safe to load — they do not start matches.
func LoadLuaModules(ctx context.Context, dir string, L *lua.LState, logger Logger) error {
	_ = ctx
	if L == nil || dir == "" {
		return nil
	}
	paths, err := listRuntimeFiles(dir, ".lua")
	if err != nil {
		if os.IsNotExist(err) {
			if logger != nil {
				logger.Info("Lua runtime modules directory not found: %s (skipping)", dir)
			}
			return nil
		}
		return err
	}
	var firstErr error
	for _, path := range paths {
		if err := L.DoFile(path); err != nil {
			if logger != nil {
				logger.Error("Failed to load Lua module %s: %v", path, err)
			}
			if firstErr == nil {
				firstErr = fmt.Errorf("lua module %s: %w", path, err)
			}
			continue
		}
		if logger != nil {
			logger.Info("Loaded Lua runtime module: %s", path)
		}
	}
	return firstErr
}

// LoadJSModules scans dir for *.js files and evaluates them in alphabetical order.
func LoadJSModules(ctx context.Context, dir string, vm *goja.Runtime, logger Logger) error {
	_ = ctx
	if vm == nil || dir == "" {
		return nil
	}
	paths, err := listRuntimeFiles(dir, ".js")
	if err != nil {
		if os.IsNotExist(err) {
			if logger != nil {
				logger.Info("JS runtime modules directory not found: %s (skipping)", dir)
			}
			return nil
		}
		return err
	}
	var firstErr error
	for _, path := range paths {
		src, err := os.ReadFile(path)
		if err != nil {
			if logger != nil {
				logger.Error("Failed to read JS module %s: %v", path, err)
			}
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if _, err := vm.RunString(string(src)); err != nil {
			if logger != nil {
				logger.Error("Failed to load JS module %s: %v", path, err)
			}
			if firstErr == nil {
				firstErr = fmt.Errorf("js module %s: %w", path, err)
			}
			continue
		}
		if logger != nil {
			logger.Info("Loaded JS runtime module: %s", path)
		}
	}
	return firstErr
}

func listRuntimeFiles(dir, ext string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var paths []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if strings.EqualFold(filepath.Ext(name), ext) {
			paths = append(paths, filepath.Join(dir, name))
		}
	}
	sort.Strings(paths)
	return paths, nil
}
