package database

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

// Config holds the database configuration options.
type Config struct {
	DSN             string        `json:"dsn" yaml:"dsn"`
	MaxOpenConns    int32         `json:"max_open_conns" yaml:"max_open_conns"`
	MaxIdleConns    int32         `json:"max_idle_conns" yaml:"max_idle_conns"`
	MaxConnLifetime time.Duration `json:"max_conn_lifetime" yaml:"max_conn_lifetime"`
	MaxConnIdleTime time.Duration `json:"max_conn_idle_time" yaml:"max_conn_idle_time"`
	MaxRetries      int           `json:"max_retries" yaml:"max_retries"`
	RetryDelay      time.Duration `json:"retry_delay" yaml:"retry_delay"`
}

// DefaultConfig returns the default database configuration.
func DefaultConfig() Config {
	return Config{
		DSN:             "postgres://game_admin:game_password@localhost:5432/ultimate_game_db?sslmode=disable",
		MaxOpenConns:    20,
		MaxIdleConns:    5,
		MaxConnLifetime: 30 * time.Minute,
		MaxConnIdleTime: 10 * time.Minute,
		MaxRetries:      5,
		RetryDelay:      1 * time.Second,
	}
}

// ConnectWithBackoff establishes a connection pool to PostgreSQL with exponential backoff retry.
func ConnectWithBackoff(ctx context.Context, logger *zap.Logger, cfg Config) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("failed to parse database DSN: %w", err)
	}

	// Apply connection pool settings
	if cfg.MaxOpenConns > 0 {
		poolCfg.MaxConns = cfg.MaxOpenConns
	}
	if cfg.MaxConnLifetime > 0 {
		poolCfg.MaxConnLifetime = cfg.MaxConnLifetime
	}
	if cfg.MaxConnIdleTime > 0 {
		poolCfg.MaxConnIdleTime = cfg.MaxConnIdleTime
	}

	var pool *pgxpool.Pool
	retryCount := 0
	maxRetries := cfg.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 5
	}
	delay := cfg.RetryDelay
	if delay <= 0 {
		delay = 1 * time.Second
	}

	for retryCount < maxRetries {
		logger.Info("Attempting to connect to PostgreSQL database...",
			zap.Int("attempt", retryCount+1),
			zap.Int("max_retries", maxRetries))

		pool, err = pgxpool.NewWithConfig(ctx, poolCfg)
		if err == nil {
			// Test if the connection is actually valid by pinging
			err = pool.Ping(ctx)
			if err == nil {
				logger.Info("Successfully connected to PostgreSQL database")
				return pool, nil
			}
		}

		// Connection or Ping failed
		logger.Warn("Failed to connect to PostgreSQL database",
			zap.Int("attempt", retryCount+1),
			zap.Error(err))

		// If pool was created, close it before next attempt
		if pool != nil {
			pool.Close()
		}

		retryCount++
		if retryCount >= maxRetries {
			break
		}

		logger.Info("Retrying database connection in backoff window", zap.Duration("delay", delay))
		
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}

		delay *= 2
	}

	return nil, fmt.Errorf("failed to connect to PostgreSQL database after %d attempts: %w", maxRetries, err)
}
