package runtime

import (
	"context"
	"database/sql"
	"sync"
	"time"
)

const defaultCronHandlerTimeout = 60 * time.Second

// CronScheduler runs RegisterCron jobs on a single node (ADR-0025).
// Overlap policy: skip if the previous invocation is still running.
type CronScheduler struct {
	registry       *HookRegistry
	logger         Logger
	db             *sql.DB
	nk             RuntimeModule
	tick           time.Duration
	handlerTimeout time.Duration
	now            func() time.Time

	mu       sync.Mutex
	nextFire map[string]time.Time
	inflight map[string]bool

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewCronScheduler creates a scheduler bound to the given registry and runtime deps.
func NewCronScheduler(registry *HookRegistry, logger Logger, db *sql.DB, nk RuntimeModule) *CronScheduler {
	return &CronScheduler{
		registry:       registry,
		logger:         logger,
		db:             db,
		nk:             nk,
		tick:           time.Second,
		handlerTimeout: defaultCronHandlerTimeout,
		now:            func() time.Time { return time.Now().UTC() },
		nextFire:       make(map[string]time.Time),
		inflight:       make(map[string]bool),
	}
}

// Start begins the background tick loop. Safe to call once.
func (s *CronScheduler) Start(parent context.Context) {
	if s == nil || s.registry == nil {
		return
	}
	ctx, cancel := context.WithCancel(parent)
	s.cancel = cancel
	s.wg.Add(1)
	go s.loop(ctx)
}

// Stop cancels the loop and waits for in-flight handlers to finish or context cancel.
func (s *CronScheduler) Stop() {
	if s == nil || s.cancel == nil {
		return
	}
	s.cancel()
	s.wg.Wait()
}

func (s *CronScheduler) loop(ctx context.Context) {
	defer s.wg.Done()
	ticker := time.NewTicker(s.tick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.tickOnce(ctx)
		}
	}
}

func (s *CronScheduler) tickOnce(ctx context.Context) {
	now := s.now()
	jobs := s.registry.ListCronJobs()
	for name, job := range jobs {
		if job == nil || job.Expr == nil || job.Handler == nil {
			continue
		}
		s.mu.Lock()
		next, ok := s.nextFire[name]
		if !ok {
			next = job.Expr.Next(now)
			s.nextFire[name] = next
		}
		due := !now.Before(next)
		running := s.inflight[name]
		if due {
			s.nextFire[name] = job.Expr.Next(now)
		}
		s.mu.Unlock()

		if !due {
			continue
		}
		if running {
			if s.logger != nil {
				s.logger.Warn("cron job %q skipped: previous still running", name)
			}
			continue
		}

		s.mu.Lock()
		s.inflight[name] = true
		s.mu.Unlock()

		s.wg.Add(1)
		go s.runJob(ctx, name, job)
	}
}

func (s *CronScheduler) runJob(parent context.Context, name string, job *CronJob) {
	defer s.wg.Done()
	defer func() {
		s.mu.Lock()
		s.inflight[name] = false
		s.mu.Unlock()
	}()
	defer func() {
		if r := recover(); r != nil {
			if s.logger != nil {
				s.logger.Error("cron job %q panic: %v", name, r)
			}
		}
	}()

	ctx, cancel := context.WithTimeout(parent, s.handlerTimeout)
	defer cancel()
	if err := job.Handler(ctx, s.logger, s.db, s.nk); err != nil && s.logger != nil {
		s.logger.Error("cron job %q failed: %v", name, err)
	}
}
