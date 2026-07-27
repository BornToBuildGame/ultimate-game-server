package database

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestApplyPoolSettingsMinConns(t *testing.T) {
	poolCfg, err := pgxpool.ParseConfig("postgres://unused@localhost:1/db")
	if err != nil {
		t.Fatal(err)
	}
	ApplyPoolSettings(poolCfg, Config{
		MaxOpenConns:    10,
		MaxIdleConns:    3,
		MaxConnLifetime: time.Minute,
		MaxConnIdleTime: time.Minute,
	})
	if poolCfg.MinConns != 3 {
		t.Fatalf("MinConns=%d want 3", poolCfg.MinConns)
	}
	if poolCfg.MaxConns != 10 {
		t.Fatalf("MaxConns=%d want 10", poolCfg.MaxConns)
	}
	if poolCfg.MaxConnLifetime != time.Minute {
		t.Fatalf("MaxConnLifetime=%v", poolCfg.MaxConnLifetime)
	}
	if poolCfg.MaxConnIdleTime != time.Minute {
		t.Fatalf("MaxConnIdleTime=%v", poolCfg.MaxConnIdleTime)
	}
}
