package storage

import (
	"context"
	"testing"
	"time"
)

func TestBlugeStorageIndex_CreateAndList(t *testing.T) {
	idx := NewBlugeStorageIndex()
	if err := idx.CreateIndex(StorageIndexDefinition{
		Name: "player_idx", Collection: "player", Fields: []string{"level"}, IndexOnly: true,
	}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	obj := &StorageObject{
		Collection: "player", Key: "p1", UserID: "u1",
		Value: `{"level":"5"}`, Version: "v1", CreateTime: now, UpdateTime: now,
	}
	ack := &StorageObjectAck{Collection: obj.Collection, Key: obj.Key, UserID: obj.UserID, Version: obj.Version, CreateTime: now, UpdateTime: now}
	idx.WriteStorageAcks(context.Background(), []*StorageObjectAck{ack}, []*StorageObject{obj})

	objs, _, err := idx.List(context.Background(), "", "player_idx", "*", 10, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(objs) != 1 {
		t.Fatalf("expected 1 object, got %d", len(objs))
	}
}
