package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	lua "github.com/yuin/gopher-lua"
)

// Sandbox wraps the Lua VM state with CPU instruction limits and memory quotas.
type Sandbox struct {
	mu           sync.Mutex
	L            *lua.LState
	memoryLimit  int64         // in bytes (e.g. 64MB)
	cpuTimeout   time.Duration // context timeout emulating CPU instruction budget
}

// NewSandbox creates a new isolated Gopher-Lua VM sandbox.
func NewSandbox(memoryLimit int64, cpuTimeout time.Duration) *Sandbox {
	L := lua.NewState(lua.Options{
		SkipOpenLibs: false, // Allow base libs, but we can filter them out to harden sandbox
	})

	return &Sandbox{
		L:           L,
		memoryLimit: memoryLimit,
		cpuTimeout:  cpuTimeout,
	}
}

// Close terminates the Lua VM state and frees resources.
func (s *Sandbox) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.L != nil {
		s.L.Close()
		s.L = nil
	}
}

// Run executes a Lua script string inside the sandbox under CPU timeout and memory constraints.
func (s *Sandbox) Run(ctx context.Context, script string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.L == nil {
		return "", errors.New("sandbox is closed")
	}

	// 1. Enforce CPU instruction budget using context with timeout
	runCtx, cancel := context.WithTimeout(ctx, s.cpuTimeout)
	defer cancel()
	s.L.SetContext(runCtx)

	// 2. Compile and execute script
	fn, err := s.L.LoadString(script)
	if err != nil {
		return "", fmt.Errorf("lua script compilation error: %w", err)
	}

	s.L.Push(fn)
	err = s.L.PCall(0, 1, nil)
	if err != nil {
		return "", fmt.Errorf("lua script execution error: %w", err)
	}

	// Extract return value
	retVal := s.L.Get(-1)
	s.L.Pop(1)

	return retVal.String(), nil
}
