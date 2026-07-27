package storage

import (
	"context"
	"sort"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// WriteStorageObjectsTx writes storage objects inside an existing transaction.
func WriteStorageObjectsTx(ctx context.Context, tx pgx.Tx, authoritative bool, objects []*StorageObject) ([]*StorageObjectAck, error) {
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
	return acks, nil
}

// DeleteStorageObjectsTx deletes storage objects inside an existing transaction.
func DeleteStorageObjectsTx(ctx context.Context, tx pgx.Tx, authoritative bool, reqs []DeleteRequest) error {
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
			query += ` AND version = $4`
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
	return nil
}
