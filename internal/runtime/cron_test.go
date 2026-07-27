package runtime

import (
	"context"
	"database/sql"
	"sync/atomic"
	"testing"
	"time"
)

func TestCronNextPrev(t *testing.T) {
	m := NewGoRuntimeModule(nil, &mockLogger{})
	// Daily midnight UTC: next after 2020-01-01 12:00 should be 2020-01-02 00:00
	ts := time.Date(2020, 1, 1, 12, 0, 0, 0, time.UTC).Unix()
	next, err := m.CronNext("0 0 * * *", ts)
	if err != nil {
		t.Fatal(err)
	}
	wantNext := time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC).Unix()
	if next != wantNext {
		t.Fatalf("CronNext got %d want %d", next, wantNext)
	}
	prev, err := m.CronPrev("0 0 * * *", ts)
	if err != nil {
		t.Fatal(err)
	}
	wantPrev := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	if prev != wantPrev {
		t.Fatalf("CronPrev got %d want %d", prev, wantPrev)
	}
	if _, err := m.CronNext("not a cron", ts); err == nil {
		t.Fatal("expected invalid cron error")
	}
}

func TestRegisterCronRejectsInvalid(t *testing.T) {
	reg := NewHookRegistry()
	err := reg.RegisterCron("bad", &CronJob{
		Schedule: "not-cron",
		Handler:  func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule) error { return nil },
	})
	if err == nil {
		t.Fatal("expected invalid schedule error")
	}
}

func TestCronSchedulerFiresAndSkipIfRunning(t *testing.T) {
	reg := NewHookRegistry()
	var fires atomic.Int32
	block := make(chan struct{})
	err := reg.RegisterCron("job", &CronJob{
		Schedule: "0 0 * * *", // daily midnight
		Handler: func(ctx context.Context, logger Logger, db *sql.DB, nk RuntimeModule) error {
			fires.Add(1)
			select {
			case <-block:
			case <-ctx.Done():
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	base := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	var now atomic.Pointer[time.Time]
	t0 := base
	now.Store(&t0)

	s := NewCronScheduler(reg, &mockLogger{}, nil, &mockRuntimeModule{})
	s.handlerTimeout = 2 * time.Second
	s.now = func() time.Time { return *now.Load() }

	ctx := context.Background()
	// Establish nextFire = Next(base) = 2020-01-02 00:00
	s.tickOnce(ctx)
	s.mu.Lock()
	nf := s.nextFire["job"]
	s.mu.Unlock()
	if nf.IsZero() {
		t.Fatal("expected nextFire set")
	}

	// Jump to exactly next fire time
	t1 := nf
	now.Store(&t1)
	s.tickOnce(ctx)

	deadline := time.Now().Add(time.Second)
	for fires.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if fires.Load() < 1 {
		t.Fatalf("expected at least one cron fire; nextFire was %v", nf)
	}

	before := fires.Load()
	t2 := nf.Add(24 * time.Hour)
	now.Store(&t2)
	s.tickOnce(ctx)
	time.Sleep(50 * time.Millisecond)
	if fires.Load() != before {
		t.Fatalf("expected skip-if-running; fires grew from %d to %d", before, fires.Load())
	}

	close(block)
	time.Sleep(20 * time.Millisecond)
}

func TestHasBeforeHookIncludesLua(t *testing.T) {
	reg := NewHookRegistry()
	reg.RegisterLuaBefore("AuthenticateEmail", "__before_AuthenticateEmail")
	m := NewGoRuntimeManager(&mockLogger{}, nil, &mockRuntimeModule{})
	// replace registry
	m.registry = reg
	if !m.HasBeforeHook("AuthenticateEmail") {
		t.Fatal("expected HasBeforeHook true for Lua registration")
	}
	if m.HasBeforeHook("Missing") {
		t.Fatal("expected false for missing hook")
	}
}

type mockLogger struct{}

func (m *mockLogger) Debug(format string, args ...interface{}) {}
func (m *mockLogger) Info(format string, args ...interface{})  {}
func (m *mockLogger) Warn(format string, args ...interface{})  {}
func (m *mockLogger) Error(format string, args ...interface{}) {}
