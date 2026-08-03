package economy

import (
	"context"

	"github.com/BornToBuildGame/ultimate-game-server/internal/auth"
	"github.com/BornToBuildGame/ultimate-game-server/internal/storage"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// MultiUpdateParams groups atomic account, storage, and wallet mutations.
type MultiUpdateParams struct {
	AccountUpdates []auth.AccountUpdateParams
	StorageWrites  []*storage.StorageObject
	StorageDeletes []storage.DeleteRequest
	WalletUpdates  []WalletUpdate
	UpdateLedger   bool
}

// MultiUpdate executes account, storage, and wallet mutations in one transaction.
func MultiUpdate(ctx context.Context, pool *pgxpool.Pool, idx storage.IndexWriter, p MultiUpdateParams) ([]*storage.StorageObjectAck, []WalletUpdateResult, error) {
	if len(p.AccountUpdates) == 0 && len(p.StorageWrites) == 0 && len(p.StorageDeletes) == 0 && len(p.WalletUpdates) == 0 {
		return nil, nil, nil
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback(ctx)

	if err := auth.UpdateAccountsTx(ctx, tx, p.AccountUpdates); err != nil {
		return nil, nil, err
	}

	var acks []*storage.StorageObjectAck
	if len(p.StorageWrites) > 0 {
		acks, err = storage.WriteStorageObjectsTx(ctx, tx, true, p.StorageWrites)
		if err != nil {
			return nil, nil, err
		}
	}
	if len(p.StorageDeletes) > 0 {
		if err := storage.DeleteStorageObjectsTx(ctx, tx, true, p.StorageDeletes); err != nil {
			return nil, nil, err
		}
	}

	var walletResults []WalletUpdateResult
	if len(p.WalletUpdates) > 0 {
		walletResults, err = UpdateWalletsTx(ctx, tx, p.WalletUpdates, p.UpdateLedger)
		if err != nil {
			return nil, walletResults, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, walletResults, err
	}

	if idx != nil {
		if len(acks) > 0 {
			idx.WriteStorageAcks(ctx, acks, p.StorageWrites)
		}
		if len(p.StorageDeletes) > 0 {
			idx.DeleteStorage(ctx, p.StorageDeletes)
		}
	}
	for _, obj := range p.StorageWrites {
		storage.IndexStorageObject(obj)
	}
	for _, req := range p.StorageDeletes {
		storage.DeleteIndexedStorageObject(req.Collection, req.UserID, req.Key)
	}
	return acks, walletResults, nil
}

// UpdateWalletsTx applies wallet updates inside an existing transaction.
func UpdateWalletsTx(ctx context.Context, tx pgx.Tx, updates []WalletUpdate, updateLedger bool) ([]WalletUpdateResult, error) {
	return updateWalletsTx(ctx, tx, updates, updateLedger)
}
