package database

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// RunMigrations runs all embedded migrations against the database.
func RunMigrations(ctx context.Context, logger *zap.Logger, pool *pgxpool.Pool) error {
	logger.Info("Starting database migrations...")

	// 1. Ensure schema_version table exists first
	err := bootstrapSchemaVersion(ctx, logger, pool)
	if err != nil {
		return fmt.Errorf("failed to bootstrap schema_version table: %w", err)
	}

	// 2. Load already applied migrations
	applied, err := getAppliedMigrations(ctx, pool)
	if err != nil {
		return fmt.Errorf("failed to fetch applied migrations: %w", err)
	}

	// 3. Read and sort migration files from embedded FS
	files, err := fs.ReadDir(migrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("failed to read embedded migrations directory: %w", err)
	}

	var migrationFiles []string
	for _, f := range files {
		if !f.IsDir() && strings.HasSuffix(f.Name(), ".sql") {
			migrationFiles = append(migrationFiles, f.Name())
		}
	}
	sort.Strings(migrationFiles)

	// 4. Apply pending migrations sequentially in transactions
	for _, fileName := range migrationFiles {
		versionName := strings.TrimSuffix(fileName, ".sql")
		if applied[versionName] {
			logger.Debug("Migration already applied", zap.String("version", versionName))
			continue
		}

		logger.Info("Applying database migration", zap.String("version", versionName))

		filePath := filepath.Join("migrations", fileName)
		content, err := migrationsFS.ReadFile(filePath)
		if err != nil {
			return fmt.Errorf("failed to read migration file %s: %w", fileName, err)
		}

		err = runMigrationTransaction(ctx, logger, pool, versionName, string(content))
		if err != nil {
			return fmt.Errorf("failed to execute migration %s: %w", versionName, err)
		}

		logger.Info("Migration applied successfully", zap.String("version", versionName))
	}

	logger.Info("Database migrations completed successfully")
	return nil
}

func bootstrapSchemaVersion(ctx context.Context, logger *zap.Logger, pool *pgxpool.Pool) error {
	query := `
		CREATE TABLE IF NOT EXISTS schema_version (
			version          VARCHAR(64) PRIMARY KEY,
			migration_time   TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL
		);
	`
	_, err := pool.Exec(ctx, query)
	if err != nil {
		return err
	}
	return nil
}

func getAppliedMigrations(ctx context.Context, pool *pgxpool.Pool) (map[string]bool, error) {
	query := "SELECT version FROM schema_version"
	rows, err := pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	applied := make(map[string]bool)
	for rows.Next() {
		var version string
		if err := rows.Scan(&version); err != nil {
			return nil, err
		}
		applied[version] = true
	}
	return applied, rows.Err()
}

func runMigrationTransaction(ctx context.Context, logger *zap.Logger, pool *pgxpool.Pool, version string, sqlContent string) error {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
				logger.Error("Failed to rollback transaction", zap.Error(rollbackErr))
			}
		}
	}()

	// Execute migration SQL content
	if strings.TrimSpace(sqlContent) != "" {
		_, err = tx.Exec(ctx, sqlContent)
		if err != nil {
			return fmt.Errorf("failed to execute sql statements: %w", err)
		}
	}

	// Insert into schema_version
	_, err = tx.Exec(ctx, "INSERT INTO schema_version (version) VALUES ($1)", version)
	if err != nil {
		return fmt.Errorf("failed to insert migration version record: %w", err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}
