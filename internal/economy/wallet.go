package economy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrInsufficientFunds is returned when a wallet mutation would cause a balance to fall below zero.
var ErrInsufficientFunds = errors.New("insufficient wallet balance")

// UpdateWallet performs an atomic wallet mutation and logs the change to the ledger.
func UpdateWallet(
	ctx context.Context,
	pool *pgxpool.Pool,
	userID string,
	changeset map[string]int64,
	metadata map[string]interface{},
) (map[string]int64, error) {
	if len(changeset) == 0 {
		return nil, errors.New("changeset cannot be empty")
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	// 1. Lock user row and fetch wallet JSONB
	var walletBytes []byte
	selectQuery := `SELECT wallet FROM users WHERE id = $1 FOR UPDATE`
	err = tx.QueryRow(ctx, selectQuery, userID).Scan(&walletBytes)
	if err != nil {
		return nil, err
	}

	currentWallet := make(map[string]int64)
	if len(walletBytes) > 0 {
		err = json.Unmarshal(walletBytes, &currentWallet)
		if err != nil {
			return nil, fmt.Errorf("failed to parse wallet JSONB: %w", err)
		}
	}

	// 2. Apply changes and validate non-negative constraints
	for currency, delta := range changeset {
		currentBalance := currentWallet[currency]
		newBalance := currentBalance + delta
		if newBalance < 0 {
			return nil, fmt.Errorf("%w: currency %q balance cannot be negative (%d + %d = %d)",
				ErrInsufficientFunds, currency, currentBalance, delta, newBalance)
		}
		currentWallet[currency] = newBalance
	}

	// 3. Marshal and update users table
	newWalletBytes, err := json.Marshal(currentWallet)
	if err != nil {
		return nil, err
	}

	updateQuery := `UPDATE users SET wallet = $1, update_time = now() WHERE id = $2`
	_, err = tx.Exec(ctx, updateQuery, newWalletBytes, userID)
	if err != nil {
		return nil, err
	}

	// 4. Record to ledger
	ledgerID := uuid.New().String()
	changesetBytes, err := json.Marshal(changeset)
	if err != nil {
		return nil, err
	}

	metaBytes, err := json.Marshal(metadata)
	if err != nil {
		return nil, err
	}

	insertLedger := `INSERT INTO wallet_ledger (id, user_id, changeset, metadata) VALUES ($1, $2, $3, $4)`
	_, err = tx.Exec(ctx, insertLedger, ledgerID, userID, changesetBytes, metaBytes)
	if err != nil {
		return nil, err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return nil, err
	}

	return currentWallet, nil
}
