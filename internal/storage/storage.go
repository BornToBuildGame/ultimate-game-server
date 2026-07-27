package storage

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/blevesearch/bleve/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	// ErrStorageRejectedVersion is returned when OCC version checks fail.
	ErrStorageRejectedVersion = errors.New("storage write rejected: version")
	// ErrStorageRejectedPermission is returned when client lacks write permission.
	ErrStorageRejectedPermission = errors.New("storage write rejected: permission")
	// ErrStorageWriteExhaustedRetries is returned by WriteStorageObjectsRetry.
	ErrStorageWriteExhaustedRetries = errors.New("storage write exhausted retries")
	// ErrOCCConflict aliases version reject for older call sites/tests.
	ErrOCCConflict = ErrStorageRejectedVersion
	// ErrListCursorInvalid is returned for malformed list cursors.
	ErrListCursorInvalid = errors.New("storage list cursor invalid")
)

// StorageObject represents a record in the storage engine.
type StorageObject struct {
	Collection string    `json:"collection"`
	Key        string    `json:"key"`
	UserID     string    `json:"user_id"`
	Value      string    `json:"value"`
	Version    string    `json:"version"`
	Read       int16     `json:"read"`
	Write      int16     `json:"write"`
	CreateTime time.Time `json:"create_time"`
	UpdateTime time.Time `json:"update_time"`
}

// StorageObjectAck is returned after a successful write.
type StorageObjectAck struct {
	Collection string
	Key        string
	UserID     string
	Version    string
	CreateTime time.Time
	UpdateTime time.Time
}

// ReadRequest defines a collection, key, and user lookup.
type ReadRequest struct {
	Collection string
	Key        string
	UserID     string
}

// DeleteRequest defines a collection, key, user, and optional expected version.
type DeleteRequest struct {
	Collection string
	Key        string
	UserID     string
	Version    string
}

// storageCursor is the gob+base64 list pagination cursor (reference-aligned).
type storageCursor struct {
	Key    string
	UserID uuid.UUID
	Read   int32
}

// List is a paginated list response.
type List struct {
	Objects []*StorageObject
	Cursor  string
}

// ListCursor maps to storageCursor for helpers.
type ListCursor struct {
	Key    string `json:"key"`
	UserID string `json:"user_id"`
	Read   int32  `json:"read"`
}

var (
	searchIndex   bleve.Index
	searchIndexMu sync.RWMutex
)

// CalculateVersion calculates the MD5 hash of the value string.
func CalculateVersion(value string) string {
	hasher := md5.New()
	hasher.Write([]byte(value))
	return hex.EncodeToString(hasher.Sum(nil))
}

// InitSearchIndex initializes a memory-only Bleve index (test-only; production indexes deferred).
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

// IndexStorageObject indexes a storage object if the search index is initialized.
func IndexStorageObject(obj *StorageObject) {
	searchIndexMu.RLock()
	idx := searchIndex
	searchIndexMu.RUnlock()
	if idx == nil || obj == nil {
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
	_ = idx.Delete(fmt.Sprintf("%s:%s:%s", collection, userID, key))
}

func encodeStorageCursor(c *storageCursor) (string, error) {
	if c == nil {
		return "", nil
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(c); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf.Bytes()), nil
}

func decodeStorageCursor(cursor string) (*storageCursor, error) {
	if cursor == "" {
		return nil, nil
	}
	b, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		b, err = base64.URLEncoding.DecodeString(cursor)
		if err != nil {
			return nil, ErrListCursorInvalid
		}
	}
	out := &storageCursor{}
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(out); err != nil {
		return nil, ErrListCursorInvalid
	}
	return out, nil
}

// EncodeCursor encodes a ListCursor (gob+base64).
func EncodeCursor(c *ListCursor) (string, error) {
	if c == nil {
		return "", nil
	}
	uid, err := uuid.Parse(c.UserID)
	if err != nil && c.UserID != "" {
		uid = uuid.Nil
	}
	return encodeStorageCursor(&storageCursor{Key: c.Key, UserID: uid, Read: c.Read})
}

// DecodeCursor decodes a list cursor.
func DecodeCursor(cursorStr string) (*ListCursor, error) {
	sc, err := decodeStorageCursor(cursorStr)
	if err != nil {
		return nil, err
	}
	if sc == nil {
		return nil, nil
	}
	return &ListCursor{Key: sc.Key, UserID: sc.UserID.String(), Read: sc.Read}, nil
}

type writeOp struct {
	obj   *StorageObject
	index int
}

// WriteStorageObjects writes objects transactionally with atomic OCC.
func WriteStorageObjects(ctx context.Context, pool *pgxpool.Pool, authoritative bool, objects []*StorageObject) ([]*StorageObjectAck, error) {
	if len(objects) == 0 {
		return []*StorageObjectAck{}, nil
	}
	ops := make([]writeOp, len(objects))
	for i, o := range objects {
		ops[i] = writeOp{obj: o, index: i}
	}
	sort.SliceStable(ops, func(i, j int) bool {
		a, b := ops[i].obj, ops[j].obj
		if a.Collection != b.Collection {
			return a.Collection < b.Collection
		}
		if a.Key != b.Key {
			return a.Key < b.Key
		}
		return a.UserID < b.UserID
	})

	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	acks := make([]*StorageObjectAck, len(objects))
	for _, op := range ops {
		obj := op.obj
		if obj.UserID == "" {
			obj.UserID = uuid.Nil.String()
		}
		if obj.Read < 0 {
			obj.Read = 1
		}
		if obj.Write < 0 {
			obj.Write = 1
		}
		newVersion := CalculateVersion(obj.Value)
		ack, err := writeOne(ctx, tx, authoritative, obj, newVersion)
		if err != nil {
			return nil, err
		}
		obj.Version = ack.Version
		obj.CreateTime = ack.CreateTime
		obj.UpdateTime = ack.UpdateTime
		acks[op.index] = ack
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	for _, obj := range objects {
		IndexStorageObject(obj)
	}
	if idx := defaultIndexWriter(); idx != nil {
		idx.WriteStorageAcks(ctx, acks, objects)
	}
	return acks, nil
}

func writeOne(ctx context.Context, tx pgx.Tx, authoritative bool, obj *StorageObject, newVersion string) (*StorageObjectAck, error) {
	writeCheck := ""
	if !authoritative {
		writeCheck = " AND storage.write = 1"
	}
	params := []interface{}{obj.Collection, obj.Key, obj.UserID, obj.Value, newVersion, obj.Read, obj.Write}

	switch {
	case obj.Version != "" && obj.Version != "*":
		query := `
		WITH upd AS (
			UPDATE storage SET value = $4, version = $5, read = $6, write = $7, update_time = now()
			WHERE collection = $1 AND key = $2 AND user_id = $3 AND version = $8` + writeCheck + `
			RETURNING read, write, version, create_time, update_time
		)
		(SELECT read, write, version, create_time, update_time, true AS ok FROM upd)
		UNION ALL
		(SELECT read, write, version, create_time, update_time, false AS ok FROM storage
		 WHERE collection = $1 AND key = $2 AND user_id = $3 AND NOT EXISTS (SELECT 1 FROM upd))
		LIMIT 1`
		params = append(params, obj.Version)
		var resultRead, resultWrite int16
		var resultVersion string
		var createTime, updateTime time.Time
		var ok bool
		err := tx.QueryRow(ctx, query, params...).Scan(&resultRead, &resultWrite, &resultVersion, &createTime, &updateTime, &ok)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrStorageRejectedVersion
		}
		if err != nil {
			return nil, err
		}
		if !ok {
			if !authoritative && resultWrite != 1 {
				return nil, ErrStorageRejectedPermission
			}
			return nil, ErrStorageRejectedVersion
		}
		return &StorageObjectAck{
			Collection: obj.Collection, Key: obj.Key, UserID: obj.UserID,
			Version: resultVersion, CreateTime: createTime, UpdateTime: updateTime,
		}, nil

	case obj.Version == "*":
		query := `
		INSERT INTO storage (collection, key, user_id, value, version, read, write, create_time, update_time)
		VALUES ($1, $2, $3, $4, $5, $6, $7, now(), now())
		RETURNING read, write, version, create_time, update_time`
		var resultRead, resultWrite int16
		var resultVersion string
		var createTime, updateTime time.Time
		err := tx.QueryRow(ctx, query, params...).Scan(&resultRead, &resultWrite, &resultVersion, &createTime, &updateTime)
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23505" {
				return nil, ErrStorageRejectedVersion
			}
			return nil, err
		}
		return &StorageObjectAck{
			Collection: obj.Collection, Key: obj.Key, UserID: obj.UserID,
			Version: resultVersion, CreateTime: createTime, UpdateTime: updateTime,
		}, nil

	default:
		query := `
		WITH upd AS (
			INSERT INTO storage (collection, key, user_id, value, version, read, write, create_time, update_time)
				VALUES ($1, $2, $3, $4, $5, $6, $7, now(), now())
			ON CONFLICT (collection, key, user_id) DO
				UPDATE SET value = $4, version = $5, read = $6, write = $7, update_time = now()
				WHERE TRUE` + writeCheck + `
				AND NOT (storage.version = $5 AND storage.read = $6 AND storage.write = $7)
			RETURNING read, write, version, create_time, update_time
		)
		(SELECT read, write, version, create_time, update_time, true AS ok FROM upd)
		UNION ALL
		(SELECT read, write, version, create_time, update_time, false AS ok FROM storage
		 WHERE collection = $1 AND key = $2 AND user_id = $3 AND NOT EXISTS (SELECT 1 FROM upd))
		LIMIT 1`
		var resultRead, resultWrite int16
		var resultVersion string
		var createTime, updateTime time.Time
		var ok bool
		err := tx.QueryRow(ctx, query, params...).Scan(&resultRead, &resultWrite, &resultVersion, &createTime, &updateTime, &ok)
		if err != nil {
			return nil, err
		}
		if !ok {
			if !authoritative && resultWrite != 1 {
				return nil, ErrStorageRejectedPermission
			}
		}
		return &StorageObjectAck{
			Collection: obj.Collection, Key: obj.Key, UserID: obj.UserID,
			Version: resultVersion, CreateTime: createTime, UpdateTime: updateTime,
		}, nil
	}
}

// ReadStorageObjects reads objects; when caller != uuid.Nil, applies permission filter.
func ReadStorageObjects(ctx context.Context, pool *pgxpool.Pool, caller uuid.UUID, reqs []ReadRequest) ([]*StorageObject, error) {
	var objects []*StorageObject
	for _, req := range reqs {
		userID := req.UserID
		if userID == "" {
			userID = uuid.Nil.String()
		}
		query := `SELECT collection, key, user_id, value, version, read, write, create_time, update_time
		          FROM storage WHERE collection = $1 AND key = $2 AND user_id = $3`
		args := []interface{}{req.Collection, req.Key, userID}
		if caller != uuid.Nil {
			query += ` AND (read = 2 OR (read = 1 AND user_id = $4))`
			args = append(args, caller.String())
		}
		var obj StorageObject
		var valBytes []byte
		err := pool.QueryRow(ctx, query, args...).Scan(
			&obj.Collection, &obj.Key, &obj.UserID, &valBytes, &obj.Version,
			&obj.Read, &obj.Write, &obj.CreateTime, &obj.UpdateTime,
		)
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, err
		}
		obj.Value = string(valBytes)
		objects = append(objects, &obj)
	}
	return objects, nil
}

// DeleteStorageObjects deletes objects transactionally.
func DeleteStorageObjects(ctx context.Context, pool *pgxpool.Pool, authoritative bool, reqs []DeleteRequest) error {
	if len(reqs) == 0 {
		return nil
	}
	sorted := make([]DeleteRequest, len(reqs))
	copy(sorted, reqs)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Collection != sorted[j].Collection {
			return sorted[i].Collection < sorted[j].Collection
		}
		if sorted[i].Key != sorted[j].Key {
			return sorted[i].Key < sorted[j].Key
		}
		return sorted[i].UserID < sorted[j].UserID
	})

	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	for _, req := range sorted {
		userID := req.UserID
		if userID == "" {
			userID = uuid.Nil.String()
		}
		params := []interface{}{req.Collection, req.Key, userID}
		query := `DELETE FROM storage WHERE collection = $1 AND key = $2 AND user_id = $3`
		if !authoritative {
			query += ` AND write > 0`
		}
		if req.Version != "" {
			query += fmt.Sprintf(` AND version = $%d`, len(params)+1)
			params = append(params, req.Version)
		}
		res, err := tx.Exec(ctx, query, params...)
		if err != nil {
			return err
		}
		if res.RowsAffected() == 0 {
			if authoritative && req.Version == "" {
				continue
			}
			return ErrStorageRejectedVersion
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	for _, req := range reqs {
		DeleteIndexedStorageObject(req.Collection, req.UserID, req.Key)
	}
	if idx := defaultIndexWriter(); idx != nil {
		idx.DeleteStorage(ctx, reqs)
	}
	return nil
}

// ListStorageObjects lists with caller×owner permission modes.
func ListStorageObjects(ctx context.Context, pool *pgxpool.Pool, caller uuid.UUID, ownerID *uuid.UUID, collection string, limit int, cursor string) (*List, error) {
	if limit <= 0 {
		limit = 1
	}
	if limit > 100 {
		limit = 100
	}
	sc, err := decodeStorageCursor(cursor)
	if err != nil {
		return nil, err
	}

	authoritative := caller == uuid.Nil
	fetch := limit + 1

	var rows pgx.Rows
	switch {
	case authoritative && ownerID == nil:
		rows, err = listAll(ctx, pool, true, collection, fetch, sc)
	case authoritative && ownerID != nil:
		rows, err = listUser(ctx, pool, true, *ownerID, collection, fetch, sc)
	case !authoritative && ownerID == nil:
		rows, err = listAll(ctx, pool, false, collection, fetch, sc)
	case !authoritative && ownerID != nil && *ownerID == caller:
		rows, err = listUser(ctx, pool, false, *ownerID, collection, fetch, sc)
	default:
		rows, err = listPublicUser(ctx, pool, *ownerID, collection, fetch, sc)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	objects := make([]*StorageObject, 0, limit)
	for rows.Next() {
		obj, err := scanObject(rows)
		if err != nil {
			return nil, err
		}
		objects = append(objects, obj)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	out := &List{Objects: objects}
	if len(objects) > limit {
		objects = objects[:limit]
		out.Objects = objects
		last := objects[len(objects)-1]
		uid, _ := uuid.Parse(last.UserID)
		out.Cursor, err = encodeStorageCursor(&storageCursor{Key: last.Key, UserID: uid, Read: int32(last.Read)})
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

func scanObject(rows pgx.Rows) (*StorageObject, error) {
	var obj StorageObject
	var valBytes []byte
	err := rows.Scan(&obj.Collection, &obj.Key, &obj.UserID, &valBytes, &obj.Version, &obj.Read, &obj.Write, &obj.CreateTime, &obj.UpdateTime)
	if err != nil {
		return nil, err
	}
	obj.Value = string(valBytes)
	return &obj, nil
}

func listAll(ctx context.Context, pool *pgxpool.Pool, authoritative bool, collection string, limit int, sc *storageCursor) (pgx.Rows, error) {
	if authoritative {
		if sc != nil {
			return pool.Query(ctx, `
SELECT collection, key, user_id, value, version, read, write, create_time, update_time FROM storage
WHERE collection = $1 AND (read, key, user_id) > ($2, $3, $4)
ORDER BY read ASC, key ASC, user_id ASC LIMIT $5`,
				collection, sc.Read, sc.Key, sc.UserID, limit)
		}
		return pool.Query(ctx, `
SELECT collection, key, user_id, value, version, read, write, create_time, update_time FROM storage
WHERE collection = $1 ORDER BY read ASC, key ASC, user_id ASC LIMIT $2`, collection, limit)
	}
	if sc != nil {
		return pool.Query(ctx, `
SELECT collection, key, user_id, value, version, read, write, create_time, update_time FROM storage
WHERE collection = $1 AND read = 2 AND (key, user_id) > ($2, $3)
ORDER BY key ASC, user_id ASC LIMIT $4`,
			collection, sc.Key, sc.UserID, limit)
	}
	return pool.Query(ctx, `
SELECT collection, key, user_id, value, version, read, write, create_time, update_time FROM storage
WHERE collection = $1 AND read = 2 ORDER BY key ASC, user_id ASC LIMIT $2`, collection, limit)
}

func listUser(ctx context.Context, pool *pgxpool.Pool, authoritative bool, owner uuid.UUID, collection string, limit int, sc *storageCursor) (pgx.Rows, error) {
	if authoritative {
		if sc != nil {
			return pool.Query(ctx, `
SELECT collection, key, user_id, value, version, read, write, create_time, update_time FROM storage
WHERE collection = $1 AND user_id = $2 AND (read, key) > ($3, $4)
ORDER BY read ASC, key ASC LIMIT $5`,
				collection, owner, sc.Read, sc.Key, limit)
		}
		return pool.Query(ctx, `
SELECT collection, key, user_id, value, version, read, write, create_time, update_time FROM storage
WHERE collection = $1 AND user_id = $2 ORDER BY read ASC, key ASC LIMIT $3`, collection, owner, limit)
	}
	if sc != nil {
		return pool.Query(ctx, `
SELECT collection, key, user_id, value, version, read, write, create_time, update_time FROM storage
WHERE collection = $1 AND user_id = $2 AND read >= 1 AND (read, key) > ($3, $4)
ORDER BY read ASC, key ASC LIMIT $5`,
			collection, owner, sc.Read, sc.Key, limit)
	}
	return pool.Query(ctx, `
SELECT collection, key, user_id, value, version, read, write, create_time, update_time FROM storage
WHERE collection = $1 AND user_id = $2 AND read >= 1 ORDER BY read ASC, key ASC LIMIT $3`,
		collection, owner, limit)
}

func listPublicUser(ctx context.Context, pool *pgxpool.Pool, owner uuid.UUID, collection string, limit int, sc *storageCursor) (pgx.Rows, error) {
	if sc != nil {
		return pool.Query(ctx, `
SELECT collection, key, user_id, value, version, read, write, create_time, update_time FROM storage
WHERE collection = $1 AND user_id = $2 AND read = 2 AND key > $3
ORDER BY key ASC LIMIT $4`, collection, owner, sc.Key, limit)
	}
	return pool.Query(ctx, `
SELECT collection, key, user_id, value, version, read, write, create_time, update_time FROM storage
WHERE collection = $1 AND user_id = $2 AND read = 2 ORDER BY key ASC LIMIT $3`, collection, owner, limit)
}

// WriteStorageObjectsRetry reads, applies updateFn, and retries on version reject.
func WriteStorageObjectsRetry(ctx context.Context, pool *pgxpool.Pool, reqs []ReadRequest, updateFn func([]*StorageObject) ([]*StorageObject, error), maxRetries int) ([]*StorageObjectAck, error) {
	if maxRetries < 0 {
		maxRetries = 0
	}
	if maxRetries > 10 {
		maxRetries = 10
	}
	for attempt := 0; attempt <= maxRetries; attempt++ {
		objs, err := ReadStorageObjects(ctx, pool, uuid.Nil, reqs)
		if err != nil {
			return nil, err
		}
		writes, err := updateFn(objs)
		if err != nil {
			return nil, err
		}
		acks, err := WriteStorageObjects(ctx, pool, true, writes)
		if err != nil {
			if errors.Is(err, ErrStorageRejectedVersion) && attempt < maxRetries {
				time.Sleep(time.Duration(2<<attempt)*time.Millisecond + time.Millisecond)
				continue
			}
			return nil, err
		}
		return acks, nil
	}
	return nil, ErrStorageWriteExhaustedRetries
}

// SearchStorageObjects searches via in-memory Bleve (test-only).
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
			readReqs = append(readReqs, ReadRequest{Collection: parts[0], UserID: parts[1], Key: parts[2]})
		}
	}
	if len(readReqs) == 0 {
		return []*StorageObject{}, nil
	}
	return ReadStorageObjects(ctx, pool, uuid.Nil, readReqs)
}
