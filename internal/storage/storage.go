package storage

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/blevesearch/bleve/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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

// DeleteRequest defines a collection, key, user, and optional expected version lookup.
type DeleteRequest struct {
	Collection string
	Key        string
	UserID     string
	Version    string // optional, for OCC
}

// ListCursor represents the pagination cursor for storage listing.
type ListCursor struct {
	Collection string `json:"collection"`
	UserID     string `json:"user_id"`
	Key        string `json:"key"`
}

var (
	searchIndex   bleve.Index
	searchIndexMu sync.RWMutex
)

// CalculateVersion calculates the MD5 hash representation of the value string.
func CalculateVersion(value string) string {
	hasher := md5.New()
	hasher.Write([]byte(value))
	return hex.EncodeToString(hasher.Sum(nil))
}

// InitSearchIndex initializes a memory-only Bleve search index for testing/local querying.
func InitSearchIndex() error {
	searchIndexMu.Lock()
	defer searchIndexMu.Unlock()

	mapping := bleve.NewIndexMapping()
	idx, err := bleve.NewMemOnly(mapping)
	if err != nil {
		return fmt.Errorf("failed to initialize bleve search index: %w", err)
	}
	searchIndex = idx
	return nil
}

// IndexStorageObject indexes a storage object in Bleve if the search index is initialized.
func IndexStorageObject(obj *StorageObject) {
	searchIndexMu.RLock()
	idx := searchIndex
	searchIndexMu.RUnlock()

	if idx == nil {
		return
	}

	var valMap map[string]interface{}
	if err := json.Unmarshal([]byte(obj.Value), &valMap); err == nil {
		indexedObj := map[string]interface{}{
			"collection": obj.Collection,
			"key":        obj.Key,
			"user_id":    obj.UserID,
			"value":      valMap,
		}
		docID := fmt.Sprintf("%s:%s:%s", obj.Collection, obj.UserID, obj.Key)
		_ = idx.Index(docID, indexedObj)
	}
}

// DeleteIndexedStorageObject deletes a storage object from the Bleve index.
func DeleteIndexedStorageObject(collection, userID, key string) {
	searchIndexMu.RLock()
	idx := searchIndex
	searchIndexMu.RUnlock()

	if idx == nil {
		return
	}

	docID := fmt.Sprintf("%s:%s:%s", collection, userID, key)
	_ = idx.Delete(docID)
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

		// 1. Enforce OCC if wildcard "*" (insert-only) or an existing version is specified
		if obj.Version == "*" {
			// Wildcard OCC: fail if the object already exists.
			// Under concurrency, a direct INSERT will fail with unique constraint violation.
			insertQuery := `INSERT INTO storage (collection, key, user_id, value, version, read, write, update_time)
			                VALUES ($1, $2, $3, $4, $5, $6, $7, now())`
			_, err = tx.Exec(ctx, insertQuery,
				obj.Collection,
				obj.Key,
				obj.UserID,
				obj.Value,
				newVersion,
				obj.Read,
				obj.Write,
			)
			if err != nil {
				var pgErr *pgconn.PgError
				if errors.As(err, &pgErr) && pgErr.Code == "23505" { // unique_violation code
					return fmt.Errorf("%w: object already exists for wildcard write", ErrOCCConflict)
				}
				return err
			}
		} else {
			if obj.Version != "" {
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
		}

		obj.Version = newVersion
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}

	// 3. Index objects in Bleve after successful commit
	for _, obj := range objects {
		IndexStorageObject(obj)
	}

	return nil
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

// DeleteStorageObjects deletes multiple storage objects transactionally, enforcing OCC.
func DeleteStorageObjects(ctx context.Context, pool *pgxpool.Pool, reqs []DeleteRequest) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	for _, req := range reqs {
		if req.Version != "" {
			query := `DELETE FROM storage WHERE collection = $1 AND key = $2 AND user_id = $3 AND version = $4`
			res, err := tx.Exec(ctx, query, req.Collection, req.Key, req.UserID, req.Version)
			if err != nil {
				return err
			}
			if res.RowsAffected() == 0 {
				var currentVer string
				err = tx.QueryRow(ctx, `SELECT version FROM storage WHERE collection = $1 AND key = $2 AND user_id = $3`, req.Collection, req.Key, req.UserID).Scan(&currentVer)
				if err != nil {
					if errors.Is(err, pgx.ErrNoRows) {
						return fmt.Errorf("%w: object does not exist", ErrOCCConflict)
					}
					return err
				}
				return fmt.Errorf("%w: expected %q, got %q", ErrOCCConflict, req.Version, currentVer)
			}
		} else {
			query := `DELETE FROM storage WHERE collection = $1 AND key = $2 AND user_id = $3`
			_, err := tx.Exec(ctx, query, req.Collection, req.Key, req.UserID)
			if err != nil {
				return err
			}
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}

	// Remove deleted objects from search index
	for _, req := range reqs {
		DeleteIndexedStorageObject(req.Collection, req.UserID, req.Key)
	}

	return nil
}

// EncodeCursor serializes and base64 encodes the ListCursor.
func EncodeCursor(c *ListCursor) (string, error) {
	bytes, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(bytes), nil
}

// DecodeCursor base64 decodes and deserializes the ListCursor.
func DecodeCursor(cursorStr string) (*ListCursor, error) {
	bytes, err := base64.URLEncoding.DecodeString(cursorStr)
	if err != nil {
		return nil, fmt.Errorf("invalid cursor: %w", err)
	}
	var c ListCursor
	if err := json.Unmarshal(bytes, &c); err != nil {
		return nil, fmt.Errorf("invalid cursor format: %w", err)
	}
	return &c, nil
}

// ListStorageObjects lists storage records, using keyset pagination.
func ListStorageObjects(ctx context.Context, pool *pgxpool.Pool, userID string, collection string, limit int, cursor string) ([]*StorageObject, string, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}

	var cursorObj *ListCursor
	if cursor != "" {
		var err error
		cursorObj, err = DecodeCursor(cursor)
		if err != nil {
			return nil, "", err
		}
		if cursorObj.Collection != collection {
			return nil, "", errors.New("cursor collection mismatch")
		}
	}

	var rows pgx.Rows
	var err error

	if userID == "" {
		// List public records across all users (where read = 2)
		if cursorObj != nil {
			query := `SELECT collection, key, user_id, value, version, read, write 
			          FROM storage 
			          WHERE collection = $1 AND read = 2 AND (user_id, key) > ($2, $3)
			          ORDER BY user_id ASC, key ASC LIMIT $4`
			rows, err = pool.Query(ctx, query, collection, cursorObj.UserID, cursorObj.Key, limit)
		} else {
			query := `SELECT collection, key, user_id, value, version, read, write 
			          FROM storage 
			          WHERE collection = $1 AND read = 2
			          ORDER BY user_id ASC, key ASC LIMIT $2`
			rows, err = pool.Query(ctx, query, collection, limit)
		}
	} else {
		// List records for a specific user
		if cursorObj != nil {
			query := `SELECT collection, key, user_id, value, version, read, write 
			          FROM storage 
			          WHERE collection = $1 AND user_id = $2 AND key > $3
			          ORDER BY key ASC LIMIT $4`
			rows, err = pool.Query(ctx, query, collection, userID, cursorObj.Key, limit)
		} else {
			query := `SELECT collection, key, user_id, value, version, read, write 
			          FROM storage 
			          WHERE collection = $1 AND user_id = $2
			          ORDER BY key ASC LIMIT $3`
			rows, err = pool.Query(ctx, query, collection, userID, limit)
		}
	}

	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var objects []*StorageObject
	for rows.Next() {
		var obj StorageObject
		var valBytes []byte
		err := rows.Scan(
			&obj.Collection,
			&obj.Key,
			&obj.UserID,
			&valBytes,
			&obj.Version,
			&obj.Read,
			&obj.Write,
		)
		if err != nil {
			return nil, "", err
		}
		obj.Value = string(valBytes)
		objects = append(objects, &obj)
	}

	if err = rows.Err(); err != nil {
		return nil, "", err
	}

	nextCursor := ""
	if len(objects) == limit {
		lastObj := objects[len(objects)-1]
		nextCursor, err = EncodeCursor(&ListCursor{
			Collection: lastObj.Collection,
			UserID:     lastObj.UserID,
			Key:        lastObj.Key,
		})
		if err != nil {
			return nil, "", err
		}
	}

	return objects, nextCursor, nil
}

// SearchStorageObjects searches storage objects using Lucene-like query syntax on indexed fields.
func SearchStorageObjects(ctx context.Context, pool *pgxpool.Pool, queryString string, limit int) ([]*StorageObject, error) {
	searchIndexMu.RLock()
	idx := searchIndex
	searchIndexMu.RUnlock()

	if idx == nil {
		return nil, errors.New("search index not initialized")
	}

	query := bleve.NewQueryStringQuery(queryString)
	searchReq := bleve.NewSearchRequest(query)
	if limit > 0 {
		searchReq.Size = limit
	} else {
		searchReq.Size = 20
	}

	searchRes, err := idx.Search(searchReq)
	if err != nil {
		return nil, fmt.Errorf("search execution failed: %w", err)
	}

	var readReqs []ReadRequest
	for _, hit := range searchRes.Hits {
		parts := strings.SplitN(hit.ID, ":", 3)
		if len(parts) == 3 {
			readReqs = append(readReqs, ReadRequest{
				Collection: parts[0],
				UserID:     parts[1],
				Key:        parts[2],
			})
		}
	}

	if len(readReqs) == 0 {
		return []*StorageObject{}, nil
	}

	return ReadStorageObjects(ctx, pool, readReqs)
}
