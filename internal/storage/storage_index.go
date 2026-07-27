package storage

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/search"
	"github.com/google/uuid"
)

// DefaultIndex is an optional Bluge index updated on durable storage writes.
var (
	defaultIndexMu sync.RWMutex
	defaultIndex   IndexWriter
)

// SetDefaultIndex attaches a global index updated by Write/DeleteStorageObjects.
func SetDefaultIndex(idx IndexWriter) {
	defaultIndexMu.Lock()
	defaultIndex = idx
	defaultIndexMu.Unlock()
}

func defaultIndexWriter() IndexWriter {
	defaultIndexMu.RLock()
	defer defaultIndexMu.RUnlock()
	return defaultIndex
}

// IndexWriter updates secondary storage indexes after durable writes.
type IndexWriter interface {
	WriteStorageAcks(ctx context.Context, acks []*StorageObjectAck, objects []*StorageObject)
	DeleteStorage(ctx context.Context, deletes []DeleteRequest)
}

// StorageIndexFilter decides whether a write is indexed (true) or removed (false).
type StorageIndexFilter func(ctx context.Context, write *StorageObject) (bool, error)

// StorageIndexDefinition describes a named Bluge index over a collection/key.
type StorageIndexDefinition struct {
	Name           string
	Collection     string
	Key            string
	Fields         []string
	SortableFields []string
	MaxEntries     int
	IndexOnly      bool
}

type indexEntry struct {
	def    StorageIndexDefinition
	writer *bluge.Writer
}

// BlugeStorageIndex is a per-node in-memory Bluge index registry.
type BlugeStorageIndex struct {
	mu      sync.RWMutex
	indices map[string]*indexEntry
	filters map[string]StorageIndexFilter
}

// NewBlugeStorageIndex creates an empty index registry.
func NewBlugeStorageIndex() *BlugeStorageIndex {
	return &BlugeStorageIndex{
		indices: make(map[string]*indexEntry),
		filters: make(map[string]StorageIndexFilter),
	}
}

// RegisterFilter attaches a custom filter to an index name.
func (si *BlugeStorageIndex) RegisterFilter(indexName string, fn StorageIndexFilter) {
	si.mu.Lock()
	defer si.mu.Unlock()
	si.filters[indexName] = fn
}

// CreateIndex registers a new named index.
func (si *BlugeStorageIndex) CreateIndex(def StorageIndexDefinition) error {
	if def.Name == "" || def.Collection == "" {
		return errors.New("index name and collection are required")
	}
	if def.MaxEntries <= 0 {
		def.MaxEntries = 100000
	}
	si.mu.Lock()
	defer si.mu.Unlock()
	if _, exists := si.indices[def.Name]; exists {
		return fmt.Errorf("index %q already exists", def.Name)
	}
	writer, err := bluge.OpenWriter(bluge.InMemoryOnlyConfig())
	if err != nil {
		return err
	}
	si.indices[def.Name] = &indexEntry{def: def, writer: writer}
	return nil
}

func (si *BlugeStorageIndex) matchingIndices(collection, key string) []*indexEntry {
	si.mu.RLock()
	defer si.mu.RUnlock()
	var out []*indexEntry
	for _, e := range si.indices {
		if e.def.Collection != collection {
			continue
		}
		if e.def.Key != "" && e.def.Key != key {
			continue
		}
		out = append(out, e)
	}
	return out
}

// WriteStorageAcks indexes written storage objects.
func (si *BlugeStorageIndex) WriteStorageAcks(ctx context.Context, acks []*StorageObjectAck, objects []*StorageObject) {
	byKey := make(map[string]*StorageObject, len(objects))
	for _, o := range objects {
		if o == nil {
			continue
		}
		byKey[o.Collection+"\x00"+o.Key+"\x00"+o.UserID] = o
	}
	for _, ack := range acks {
		if ack == nil {
			continue
		}
		obj := byKey[ack.Collection+"\x00"+ack.Key+"\x00"+ack.UserID]
		if obj == nil {
			obj = &StorageObject{
				Collection: ack.Collection, Key: ack.Key, UserID: ack.UserID,
				Version: ack.Version, CreateTime: ack.CreateTime, UpdateTime: ack.UpdateTime,
			}
		}
		for _, idx := range si.matchingIndices(obj.Collection, obj.Key) {
			if fn, ok := si.filters[idx.def.Name]; ok {
				keep, err := fn(ctx, obj)
				if err != nil || !keep {
					si.deleteDoc(idx, obj.Collection, obj.Key, obj.UserID)
					continue
				}
			}
			doc, err := mapStorageDoc(obj, idx.def)
			if err != nil || doc == nil {
				continue
			}
			batch := bluge.NewBatch()
			batch.Update(storageIndexDocID(obj.Collection, obj.Key, obj.UserID), doc)
			_ = idx.writer.Batch(batch)
		}
	}
}

// DeleteStorage removes objects from matching indexes.
func (si *BlugeStorageIndex) DeleteStorage(ctx context.Context, deletes []DeleteRequest) {
	for _, req := range deletes {
		for _, idx := range si.matchingIndices(req.Collection, req.Key) {
			si.deleteDoc(idx, req.Collection, req.Key, req.UserID)
		}
	}
}

func (si *BlugeStorageIndex) deleteDoc(idx *indexEntry, collection, key, userID string) {
	batch := bluge.NewBatch()
	batch.Delete(storageIndexDocID(collection, key, userID))
	_ = idx.writer.Batch(batch)
}

func storageIndexDocID(collection, key, userID string) bluge.Identifier {
	return bluge.Identifier(fmt.Sprintf("%s.%s.%s", collection, key, userID))
}

func mapStorageDoc(obj *StorageObject, def StorageIndexDefinition) (*bluge.Document, error) {
	fields := map[string]interface{}{}
	if len(def.Fields) > 0 {
		if err := json.Unmarshal([]byte(obj.Value), &fields); err != nil {
			return nil, err
		}
	}
	doc := bluge.NewDocument(string(storageIndexDocID(obj.Collection, obj.Key, obj.UserID)))
	doc.AddField(bluge.NewKeywordField("collection", obj.Collection).StoreValue())
	doc.AddField(bluge.NewKeywordField("key", obj.Key).StoreValue())
	doc.AddField(bluge.NewKeywordField("user_id", obj.UserID).StoreValue())
	doc.AddField(bluge.NewKeywordField("version", obj.Version).StoreValue())
	doc.AddField(bluge.NewDateTimeField("create_time", obj.CreateTime).StoreValue().Sortable())
	doc.AddField(bluge.NewDateTimeField("update_time", obj.UpdateTime).StoreValue().Sortable())
	for _, f := range def.Fields {
		if v, ok := fields[f]; ok {
			doc.AddField(bluge.NewKeywordField(f, fmt.Sprint(v)).StoreValue())
		}
	}
	for _, f := range def.SortableFields {
		if v, ok := fields[f]; ok {
			doc.AddField(bluge.NewKeywordField("sort_"+f, fmt.Sprint(v)).StoreValue().Sortable())
		}
	}
	if def.IndexOnly {
		doc.AddField(bluge.NewStoredOnlyField("json", []byte(obj.Value)))
	}
	return doc, nil
}

type indexListCursor struct {
	Query  string
	Offset int
	Limit  int
	Order  []string
}

type indexResult struct {
	Collection string
	Key        string
	UserID     string
	Version    string
	Value      string
	CreateTime time.Time
	UpdateTime time.Time
}

// List queries a named index and returns storage objects plus next cursor.
func (si *BlugeStorageIndex) List(ctx context.Context, callerID, indexName, query string, limit int, order []string, cursor string) ([]*StorageObject, string, error) {
	si.mu.RLock()
	idx, ok := si.indices[indexName]
	si.mu.RUnlock()
	if !ok {
		return nil, "", fmt.Errorf("index %q not found", indexName)
	}
	if limit <= 0 {
		limit = 10
	}
	if limit > 100 {
		limit = 100
	}
	if query == "" {
		query = "*"
	}

	var idxCursor *indexListCursor
	if cursor != "" {
		idxCursor = &indexListCursor{}
		cb, err := base64.RawURLEncoding.DecodeString(cursor)
		if err != nil {
			return nil, "", errors.New("invalid cursor")
		}
		if err := gob.NewDecoder(bytes.NewReader(cb)).Decode(idxCursor); err != nil {
			return nil, "", errors.New("invalid cursor")
		}
		if query != idxCursor.Query || limit != idxCursor.Limit || !slices.Equal(order, idxCursor.Order) {
			return nil, "", errors.New("invalid cursor")
		}
	}

	var parsedQuery bluge.Query = bluge.NewMatchAllQuery()
	if query != "*" {
		parsedQuery = bluge.NewMatchQuery(query)
	}
	searchReq := bluge.NewTopNSearch(limit+1, parsedQuery)
	if len(order) > 0 {
		searchReq.SortBy(order)
	} else {
		searchReq.SortBy([]string{"-update_time"})
	}
	if idxCursor != nil {
		searchReq.SetFrom(idxCursor.Offset)
	}

	reader, err := idx.writer.Reader()
	if err != nil {
		return nil, "", err
	}
	defer reader.Close()

	results, err := reader.Search(ctx, searchReq)
	if err != nil {
		return nil, "", err
	}
	indexResults, err := queryMatchesToIndexResults(results)
	if err != nil {
		return nil, "", err
	}

	var next string
	if len(indexResults) > limit {
		indexResults = indexResults[:limit]
		offset := 0
		if idxCursor != nil {
			offset = idxCursor.Offset
		}
		newCursor := &indexListCursor{Query: query, Offset: offset + limit, Limit: limit, Order: order}
		buf := new(bytes.Buffer)
		if err := gob.NewEncoder(buf).Encode(newCursor); err == nil {
			next = base64.RawURLEncoding.EncodeToString(buf.Bytes())
		}
	}

	objects := make([]*StorageObject, 0, len(indexResults))
	for _, r := range indexResults {
		if callerID != "" && callerID != uuid.Nil.String() && r.UserID != callerID {
			continue
		}
		objects = append(objects, &StorageObject{
			Collection: r.Collection, Key: r.Key, UserID: r.UserID, Value: r.Value,
			Version: r.Version, CreateTime: r.CreateTime, UpdateTime: r.UpdateTime,
		})
	}
	return objects, next, nil
}

func queryMatchesToIndexResults(dmi search.DocumentMatchIterator) ([]*indexResult, error) {
	out := make([]*indexResult, 0)
	next, err := dmi.Next()
	for err == nil && next != nil {
		res := &indexResult{}
		err = next.VisitStoredFields(func(field string, value []byte) bool {
			switch field {
			case "collection":
				res.Collection = string(value)
			case "key":
				res.Key = string(value)
			case "user_id":
				res.UserID = string(value)
			case "version":
				res.Version = string(value)
			case "json":
				res.Value = string(value)
			case "create_time":
				if t, parseErr := bluge.DecodeDateTime(value); parseErr == nil {
					res.CreateTime = t
				}
			case "update_time":
				if t, parseErr := bluge.DecodeDateTime(value); parseErr == nil {
					res.UpdateTime = t
				}
			}
			return true
		})
		if err != nil {
			return nil, err
		}
		out = append(out, res)
		next, err = dmi.Next()
	}
	if err != nil {
		return nil, err
	}
	return out, nil
}
