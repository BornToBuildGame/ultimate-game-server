package economy

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrInsufficientFunds aliases negative-balance rejection for older call sites.
var ErrInsufficientFunds = errors.New("insufficient wallet balance")

// ErrWalletNegative is returned when a wallet mutation would make a balance negative.
type ErrWalletNegative struct {
	UserID  string
	Path    string
	Current int64
	Amount  int64
}

func (e *ErrWalletNegative) Error() string {
	return fmt.Sprintf("wallet update rejected: user %s currency %q would be negative (%d + %d)",
		e.UserID, e.Path, e.Current, e.Amount)
}

func (e *ErrWalletNegative) Is(target error) bool {
	return target == ErrInsufficientFunds
}

// ErrLedgerCursorInvalid is returned for malformed ledger cursors.
var ErrLedgerCursorInvalid = errors.New("wallet ledger cursor invalid")

// WalletUpdate is a single wallet mutation request.
type WalletUpdate struct {
	UserID    string
	Changeset map[string]int64
	Metadata  map[string]interface{}
}

// WalletUpdateResult is the outcome of a successful wallet update for one user entry.
type WalletUpdateResult struct {
	UserID   string
	Updated  map[string]int64
	Previous map[string]int64
}

// LedgerItem is a wallet_ledger row.
type LedgerItem struct {
	ID         string
	UserID     string
	Changeset  map[string]int64
	Metadata   map[string]interface{}
	CreateTime time.Time
	UpdateTime time.Time
}

// LedgerList is a paginated ledger response.
type LedgerList struct {
	Items      []*LedgerItem
	NextCursor string
}

type ledgerCursor struct {
	UserID     string
	CreateTime time.Time
	ID         string
}

// GetWallet returns the user's current wallet map (empty map if unset).
func GetWallet(ctx context.Context, pool *pgxpool.Pool, userID string) (map[string]int64, error) {
	var walletBytes []byte
	err := pool.QueryRow(ctx, `SELECT wallet FROM users WHERE id = $1`, userID).Scan(&walletBytes)
	if err != nil {
		return nil, err
	}
	out := make(map[string]int64)
	if len(walletBytes) > 0 {
		if err := json.Unmarshal(walletBytes, &out); err != nil {
			return nil, fmt.Errorf("failed to parse wallet JSONB: %w", err)
		}
	}
	return out, nil
}

// UpdateWallet performs a single atomic wallet mutation.
func UpdateWallet(
	ctx context.Context,
	pool *pgxpool.Pool,
	userID string,
	changeset map[string]int64,
	metadata map[string]interface{},
	updateLedger bool,
) (updated, previous map[string]int64, err error) {
	results, err := UpdateWallets(ctx, pool, []WalletUpdate{{
		UserID: userID, Changeset: changeset, Metadata: metadata,
	}}, updateLedger)
	if err != nil {
		return nil, nil, err
	}
	if len(results) == 0 {
		return nil, nil, fmt.Errorf("user not found: %s", userID)
	}
	return results[0].Updated, results[0].Previous, nil
}

// UpdateWallets applies batch wallet updates in one transaction (all-or-nothing on negative).
func UpdateWallets(ctx context.Context, pool *pgxpool.Pool, updates []WalletUpdate, updateLedger bool) ([]WalletUpdateResult, error) {
	if len(updates) == 0 {
		return nil, nil
	}
	for _, u := range updates {
		if len(u.Changeset) == 0 {
			return nil, errors.New("changeset cannot be empty")
		}
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	results, err := updateWalletsTx(ctx, tx, updates, updateLedger)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return results, nil
}

func updateWalletsTx(ctx context.Context, tx pgx.Tx, updates []WalletUpdate, updateLedger bool) ([]WalletUpdateResult, error) {
	idSet := make(map[string]struct{}, len(updates))
	ids := make([]string, 0, len(updates))
	for _, u := range updates {
		if _, ok := idSet[u.UserID]; !ok {
			idSet[u.UserID] = struct{}{}
			ids = append(ids, u.UserID)
		}
	}
	sort.Strings(ids)

	rows, err := tx.Query(ctx, `SELECT id, wallet FROM users WHERE id = ANY($1::UUID[]) FOR UPDATE`, ids)
	if err != nil {
		return nil, err
	}
	wallets := make(map[string]map[string]int64, len(ids))
	for rows.Next() {
		var id string
		var walletBytes []byte
		if err := rows.Scan(&id, &walletBytes); err != nil {
			rows.Close()
			return nil, err
		}
		wm := make(map[string]int64)
		if len(walletBytes) > 0 {
			if err := json.Unmarshal(walletBytes, &wm); err != nil {
				rows.Close()
				return nil, fmt.Errorf("failed to parse wallet JSONB: %w", err)
			}
		}
		wallets[id] = wm
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	results := make([]WalletUpdateResult, 0, len(updates))
	type pendingWrite struct {
		userID string
		bytes  []byte
	}
	writes := make([]pendingWrite, 0, len(updates))
	seenWrite := make(map[string]int) // userID -> index in writes

	for _, update := range updates {
		walletMap, ok := wallets[update.UserID]
		if !ok {
			continue // missing user skipped
		}
		previous := make(map[string]int64, len(walletMap))
		for k, v := range walletMap {
			previous[k] = v
		}
		for currency, delta := range update.Changeset {
			newBal := walletMap[currency] + delta
			if newBal < 0 {
				return nil, &ErrWalletNegative{
					UserID: update.UserID, Path: currency,
					Current: walletMap[currency], Amount: delta,
				}
			}
			walletMap[currency] = newBal
		}
		updatedCopy := make(map[string]int64, len(walletMap))
		for k, v := range walletMap {
			updatedCopy[k] = v
		}
		results = append(results, WalletUpdateResult{
			UserID: update.UserID, Updated: updatedCopy, Previous: previous,
		})

		b, err := json.Marshal(walletMap)
		if err != nil {
			return nil, err
		}
		if idx, ok := seenWrite[update.UserID]; ok {
			writes[idx].bytes = b
		} else {
			seenWrite[update.UserID] = len(writes)
			writes = append(writes, pendingWrite{userID: update.UserID, bytes: b})
		}

		if updateLedger {
			ledgerID := uuid.New().String()
			csBytes, err := json.Marshal(update.Changeset)
			if err != nil {
				return nil, err
			}
			meta := update.Metadata
			if meta == nil {
				meta = map[string]interface{}{}
			}
			metaBytes, err := json.Marshal(meta)
			if err != nil {
				return nil, err
			}
			_, err = tx.Exec(ctx,
				`INSERT INTO wallet_ledger (id, user_id, changeset, metadata) VALUES ($1, $2, $3, $4)`,
				ledgerID, update.UserID, csBytes, metaBytes)
			if err != nil {
				return nil, err
			}
		}
	}

	for _, w := range writes {
		_, err := tx.Exec(ctx, `UPDATE users SET wallet = $1, update_time = now() WHERE id = $2`, w.bytes, w.userID)
		if err != nil {
			return nil, err
		}
	}
	return results, nil
}

func encodeLedgerCursor(c *ledgerCursor) (string, error) {
	if c == nil {
		return "", nil
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(c); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf.Bytes()), nil
}

func decodeLedgerCursor(cursor string) (*ledgerCursor, error) {
	if cursor == "" {
		return nil, nil
	}
	b, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		b, err = base64.URLEncoding.DecodeString(cursor)
		if err != nil {
			return nil, ErrLedgerCursorInvalid
		}
	}
	out := &ledgerCursor{}
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(out); err != nil {
		return nil, ErrLedgerCursorInvalid
	}
	return out, nil
}

// ListWalletLedger lists ledger entries newest-first with gob cursor pagination.
func ListWalletLedger(ctx context.Context, pool *pgxpool.Pool, userID string, limit int, cursor string) (*LedgerList, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 100 {
		limit = 100
	}
	sc, err := decodeLedgerCursor(cursor)
	if err != nil {
		return nil, err
	}

	fetch := limit + 1
	var rows pgx.Rows
	if sc != nil {
		rows, err = pool.Query(ctx, `
SELECT id, user_id, changeset, metadata, create_time, update_time FROM wallet_ledger
WHERE user_id = $1 AND (create_time, id) < ($2::TIMESTAMPTZ, $3::UUID)
ORDER BY create_time DESC, id DESC LIMIT $4`,
			userID, sc.CreateTime, sc.ID, fetch)
	} else {
		rows, err = pool.Query(ctx, `
SELECT id, user_id, changeset, metadata, create_time, update_time FROM wallet_ledger
WHERE user_id = $1 ORDER BY create_time DESC, id DESC LIMIT $2`, userID, fetch)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]*LedgerItem, 0, limit)
	for rows.Next() {
		item := &LedgerItem{}
		var csBytes, metaBytes []byte
		if err := rows.Scan(&item.ID, &item.UserID, &csBytes, &metaBytes, &item.CreateTime, &item.UpdateTime); err != nil {
			return nil, err
		}
		_ = json.Unmarshal(csBytes, &item.Changeset)
		_ = json.Unmarshal(metaBytes, &item.Metadata)
		if item.Changeset == nil {
			item.Changeset = map[string]int64{}
		}
		if item.Metadata == nil {
			item.Metadata = map[string]interface{}{}
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	out := &LedgerList{Items: items}
	if len(items) > limit {
		items = items[:limit]
		out.Items = items
		last := items[len(items)-1]
		out.NextCursor, err = encodeLedgerCursor(&ledgerCursor{
			UserID: last.UserID, CreateTime: last.CreateTime, ID: last.ID,
		})
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// UpdateWalletLedgerMetadata merges metadata into an existing ledger row.
func UpdateWalletLedgerMetadata(ctx context.Context, pool *pgxpool.Pool, ledgerID, userID string, metadata map[string]interface{}) error {
	if metadata == nil {
		metadata = map[string]interface{}{}
	}
	metaBytes, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	res, err := pool.Exec(ctx, `
UPDATE wallet_ledger SET metadata = metadata || $1::jsonb, update_time = now()
WHERE id = $2 AND user_id = $3`, metaBytes, ledgerID, userID)
	if err != nil {
		return err
	}
	if res.RowsAffected() == 0 {
		return errors.New("ledger item not found")
	}
	return nil
}
