package economy

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type StorePurchase struct {
	TransactionID string
	ProductID     string
	PurchaseTime  time.Time
	Environment   int // 0=Unknown, 1=Sandbox, 2=Production
}

var ErrTransactionSeenBefore = errors.New("TRANSACTION_SEEN_BEFORE")

func ProcessAppleValidationTx(ctx context.Context, pool *pgxpool.Pool, userID string, p StorePurchase, rawResponse string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// 1. Check duplicate
	var existingUser string
	err = tx.QueryRow(ctx, "SELECT user_id FROM purchase WHERE transaction_id = $1", p.TransactionID).Scan(&existingUser)
	if err == nil {
		return ErrTransactionSeenBefore
	}

	// 2. Insert validated transaction log (store = 0 for AppleAppStore)
	_, err = tx.Exec(ctx, `
		INSERT INTO purchase (user_id, product_id, transaction_id, store, raw_response, purchase_time, environment)
		VALUES ($1, $2, $3, 0, $4, $5, $6)`,
		userID, p.ProductID, p.TransactionID, rawResponse, p.PurchaseTime, p.Environment)
	if err != nil {
		return err
	}

	// 3. Trigger wallet credit (e.g. 100 gems for gems_pack_100)
	var coinsDelta int64 = 0
	if p.ProductID == "gems_pack_100" {
		coinsDelta = 100
	}
	if coinsDelta > 0 {
		walletQuery := `
			UPDATE users 
			SET wallet = jsonb_set(COALESCE(wallet, '{}'::jsonb), '{gems}', (COALESCE((wallet->>'gems')::int, 0) + $1)::text::jsonb),
			    update_time = now()
			WHERE id = $2`
		_, err = tx.Exec(ctx, walletQuery, coinsDelta, userID)
		if err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

func ProcessGoogleValidationTx(ctx context.Context, pool *pgxpool.Pool, userID string, p StorePurchase, rawResponse string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// 1. Check duplicate
	var existingUser string
	err = tx.QueryRow(ctx, "SELECT user_id FROM purchase WHERE transaction_id = $1", p.TransactionID).Scan(&existingUser)
	if err == nil {
		return ErrTransactionSeenBefore
	}

	// 2. Insert validated transaction log (store = 1 for GooglePlay)
	_, err = tx.Exec(ctx, `
		INSERT INTO purchase (user_id, product_id, transaction_id, store, raw_response, purchase_time, environment)
		VALUES ($1, $2, $3, 1, $4, $5, $6)`,
		userID, p.ProductID, p.TransactionID, rawResponse, p.PurchaseTime, p.Environment)
	if err != nil {
		return err
	}

	// 3. Trigger wallet credit (e.g. 100 gems for gems_pack_100)
	var coinsDelta int64 = 0
	if p.ProductID == "gems_pack_100" {
		coinsDelta = 100
	}
	if coinsDelta > 0 {
		walletQuery := `
			UPDATE users 
			SET wallet = jsonb_set(COALESCE(wallet, '{}'::jsonb), '{gems}', (COALESCE((wallet->>'gems')::int, 0) + $1)::text::jsonb),
			    update_time = now()
			WHERE id = $2`
		_, err = tx.Exec(ctx, walletQuery, coinsDelta, userID)
		if err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}
