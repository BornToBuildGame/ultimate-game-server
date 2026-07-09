package storage

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrOCCConflict is returned when an optimistic concurrency control version mismatch occurs.
var ErrOCCConflict = errors.New("optimistic concurrency control version conflict")

// StorageObject represents a record in the NoSQL-style storage engine.
type StorageObject struct {
	Collection string `json:"collection"`
	Key        string `json:"key"`
	UserID     string `json:"user_id"`
	Value      string `json:"value"` // JSON string representation of value
	Version    string `json:"version"`
	Read       int16  `json:"read"`
	Write      int16  `json:"write"`
}

// ReadRequest defines a collection, key, and user lookup.
type ReadRequest struct {
	Collection string
	Key        string
	UserID     string
}

// CalculateVersion calculates the MD5 hash representation of the value string.
func CalculateVersion(value string) string {
	hasher := md5.New()
	hasher.Write([]byte(value))
	return hex.EncodeToString(hasher.Sum(nil))
}

// WriteStorageObjects writes multiple storage objects transactionally, enforcing OCC.
func WriteStorageObjects(ctx context.Context, pool *pgxpool.Pool, objects []*StorageObject) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	for _, obj := range objects {
		newVersion := CalculateVersion(obj.Value)

		// 1. Enforce OCC if an existing version is specified by the client
		if obj.Version != "" && obj.Version != "*" {
			var currentVersion string
			queryVer := `SELECT version FROM storage WHERE collection = $1 AND key = $2 AND user_id = $3`
			err = tx.QueryRow(ctx, queryVer, obj.Collection, obj.Key, obj.UserID).Scan(&currentVersion)
			if err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return fmt.Errorf("%w: object does not exist", ErrOCCConflict)
				}
				return err
			}
			if currentVersion != obj.Version {
				return fmt.Errorf("%w: expected %q, got %q", ErrOCCConflict, obj.Version, currentVersion)
			}
		}

		// 2. Perform Upsert with new version hash
		upsertQuery := `INSERT INTO storage (collection, key, user_id, value, version, read, write, update_time)
		                VALUES ($1, $2, $3, $4, $5, $6, $7, now())
		                ON CONFLICT (collection, key, user_id)
		                DO UPDATE SET value = $4, version = $5, read = $6, write = $7, update_time = now()`
		
		_, err = tx.Exec(ctx, upsertQuery,
			obj.Collection,
			obj.Key,
			obj.UserID,
			obj.Value,
			newVersion,
			obj.Read,
			obj.Write,
		)
		if err != nil {
			return err
		}

		obj.Version = newVersion
	}

	return tx.Commit(ctx)
}

// ReadStorageObjects reads a set of storage objects from PostgreSQL.
func ReadStorageObjects(ctx context.Context, pool *pgxpool.Pool, reqs []ReadRequest) ([]*StorageObject, error) {
	var objects []*StorageObject

	for _, req := range reqs {
		query := `SELECT collection, key, user_id, value, version, read, write 
		          FROM storage 
		          WHERE collection = $1 AND key = $2 AND user_id = $3`
		
		var obj StorageObject
		var valBytes []byte
		err := pool.QueryRow(ctx, query, req.Collection, req.Key, req.UserID).Scan(
			&obj.Collection,
			&obj.Key,
			&obj.UserID,
			&valBytes,
			&obj.Version,
			&obj.Read,
			&obj.Write,
		)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				continue // Skip nonexistent objects, return what was found
			}
			return nil, err
		}
		obj.Value = string(valBytes)
		objects = append(objects, &obj)
	}

	return objects, nil
}
