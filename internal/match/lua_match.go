package match

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"ultimate-game-server/internal/runtime"
)

// SetLuaModulePath configures the directory used to resolve Lua match modules by name.
func (r *Router) SetLuaModulePath(path string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.LuaModulePath = path
}

// RegisterLuaMatchSource registers in-memory Lua match module source (tests / hot reload).
func (r *Router) RegisterLuaMatchSource(name, source string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.luaMatchSources == nil {
		r.luaMatchSources = make(map[string]string)
	}
	r.luaMatchSources[name] = source
}

func (r *Router) resolveLuaMatchSource(module string) (string, string, error) {
	r.mu.RLock()
	src := ""
	if r.luaMatchSources != nil {
		src = r.luaMatchSources[module]
	}
	base := r.LuaModulePath
	r.mu.RUnlock()

	if src != "" {
		return src, "memory:" + module, nil
	}
	if base == "" {
		return "", "", fmt.Errorf("lua match module %q not found (no module path)", module)
	}
	candidates := []string{
		filepath.Join(base, module+".lua"),
		filepath.Join(base, module),
		filepath.Join(base, filepath.FromSlash(module)+".lua"),
	}
	for _, p := range candidates {
		data, err := os.ReadFile(p)
		if err == nil {
			return string(data), p, nil
		}
	}
	return "", "", fmt.Errorf("lua match module %q not found under %s", module, base)
}

func (r *Router) createAndRegisterLuaMatch(matchID, module string, params map[string]interface{}) error {
	r.mu.RLock()
	logger := r.Logger
	zapLogger := r.ZapLogger
	nk := r.NK
	hr := r.HookRegistry
	registry := r.Registry
	matchCount := len(r.matches)
	r.mu.RUnlock()

	if matchCount >= maxConcurrentMatches {
		return fmt.Errorf("max concurrent matches (%d) reached", maxConcurrentMatches)
	}

	source, origin, err := r.resolveLuaMatchSource(module)
	if err != nil {
		return err
	}

	sb := runtime.NewSandbox(64*1024*1024, 5*time.Second)
	if nk != nil {
		runtime.MapLuaNK(sb.L, nk, hr)
	}
	if err := sb.L.DoString(source); err != nil {
		sb.Close()
		return fmt.Errorf("load lua match module %q (%s): %w", module, origin, err)
	}

	loop := NewMatchLoop(matchID, nil, 10, zapLogger, registry)
	loop.SetSandbox(sb)
	tickRate, label, err := loop.callLuaMatchInit(params)
	if err != nil {
		sb.Close()
		return fmt.Errorf("match_init for %q: %w", module, err)
	}
	loop.tickRate = tickRate
	loop.tickDuration = time.Second / time.Duration(tickRate)
	loop.label = label
	if logger != nil {
		loop.goLogger = logger
	}
	loop.goNK = nk
	loop.onMetadataUpdate = func(matchID string, label string, playerCount int) {
		r.UpdateMetadata(matchID, label, playerCount)
	}
	loop.onEnd = func(matchID string, finalState MatchState) {
		r.Unregister(matchID)
		sb.Close()
	}

	r.Register(matchID, loop)
	go loop.Start(context.Background())
	return nil
}
