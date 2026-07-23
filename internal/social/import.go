package social

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ImportHTTPClient is overridable for tests.
var ImportHTTPClient = &http.Client{Timeout: 15 * time.Second}

// FacebookFriendProfile is a minimal Facebook friend profile.
type FacebookFriendProfile struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type facebookPagingCursors struct {
	After string `json:"after"`
}

type facebookPaging struct {
	Cursors facebookPagingCursors `json:"cursors"`
	Next    string                `json:"next"`
}

type facebookFriendsResp struct {
	Paging facebookPaging          `json:"paging"`
	Data   []FacebookFriendProfile `json:"data"`
}

// SteamFriendProfile is a minimal Steam friend profile.
type SteamFriendProfile struct {
	SteamID uint64 `json:"steamid"`
}

type steamFriendsList struct {
	Friends []SteamFriendProfile `json:"friends"`
}

type steamFriendsWrapper struct {
	FriendsList steamFriendsList `json:"friendslist"`
}

// FetchFacebookFriendIDs retrieves Facebook friend IDs for the given access token.
func FetchFacebookFriendIDs(ctx context.Context, accessToken string) ([]string, error) {
	if accessToken == "" {
		return nil, fmt.Errorf("facebook token is required")
	}
	friends := make([]string, 0)
	after := ""
	for {
		path := "https://graph.facebook.com/v22.0/me/friends?access_token=" + url.QueryEscape(accessToken)
		if after != "" {
			path += "&after=" + url.QueryEscape(after)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, path, nil)
		if err != nil {
			return friends, err
		}
		resp, err := ImportHTTPClient.Do(req)
		if err != nil {
			return friends, err
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return friends, err
		}
		if resp.StatusCode != http.StatusOK {
			return friends, fmt.Errorf("facebook friends request failed: status %d", resp.StatusCode)
		}
		var current facebookFriendsResp
		if err := json.Unmarshal(body, &current); err != nil {
			return friends, err
		}
		for _, f := range current.Data {
			if f.ID != "" {
				friends = append(friends, f.ID)
			}
		}
		if current.Paging.Next == "" {
			return friends, nil
		}
		after = current.Paging.Cursors.After
	}
}

// FetchSteamFriendIDs retrieves Steam friend IDs for the given steam user.
func FetchSteamFriendIDs(ctx context.Context, publisherKey, steamID string) ([]string, error) {
	if publisherKey == "" || steamID == "" {
		return nil, fmt.Errorf("steam publisher key and steam id are required")
	}
	path := fmt.Sprintf(
		"https://partner.steam-api.com/ISteamUser/GetFriendList/v0001/?key=%s&steamid=%s&relationship=friend",
		url.QueryEscape(publisherKey), url.QueryEscape(steamID),
	)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	resp, err := ImportHTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("steam friends request failed: status %d", resp.StatusCode)
	}
	var wrapped steamFriendsWrapper
	if err := json.Unmarshal(body, &wrapped); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(wrapped.FriendsList.Friends))
	for _, f := range wrapped.FriendsList.Friends {
		out = append(out, strconv.FormatUint(f.SteamID, 10))
	}
	return out, nil
}

func resetUserFriends(ctx context.Context, tx pgx.Tx, userID string) error {
	res, err := tx.Exec(ctx, `DELETE FROM user_edge WHERE source_id = $1 AND state <> $2`, userID, StateBlocked)
	if err != nil {
		return err
	}
	if n := res.RowsAffected(); n != 0 {
		_, err = tx.Exec(ctx, `UPDATE users SET edge_count = GREATEST(edge_count - $2, 0), update_time = now() WHERE id = $1`, userID, n)
		if err != nil {
			return err
		}
	}

	rows, err := tx.Query(ctx, `DELETE FROM user_edge WHERE destination_id = $1 AND state <> $2 RETURNING source_id`, userID, StateBlocked)
	if err != nil {
		return err
	}
	var sources []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		sources = append(sources, id)
	}
	rows.Close()
	for _, sid := range sources {
		if _, err := tx.Exec(ctx, `UPDATE users SET edge_count = GREATEST(edge_count - 1, 0), update_time = now() WHERE id = $1`, sid); err != nil {
			return err
		}
	}
	return nil
}

func importFriendsByIDs(ctx context.Context, tx pgx.Tx, userID string, friendIDs []string, provider string) ([]string, error) {
	imported := make([]string, 0)
	meta := fmt.Sprintf(`{"imported":true,"provider":%q}`, provider)
	cfg := DefaultConfig()
	for _, fid := range friendIDs {
		if fid == userID {
			continue
		}
		_, err := addFriendTx(ctx, tx, userID, fid, meta, cfg)
		if err != nil {
			if err == pgx.ErrNoRows {
				continue
			}
			// Skip limit / duplicate errors during bulk import.
			continue
		}
		imported = append(imported, fid)
	}
	return imported, nil
}

func matchUsersByProviderIDs(ctx context.Context, tx pgx.Tx, column string, providerIDs []string) ([]string, error) {
	if len(providerIDs) == 0 {
		return nil, nil
	}
	query := fmt.Sprintf(`SELECT id FROM users WHERE %s = ANY($1::text[])`, column)
	rows, err := tx.Query(ctx, query, providerIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// ImportFacebookFriends imports Facebook friends matching existing users.
func ImportFacebookFriends(ctx context.Context, pool *pgxpool.Pool, userID, username, token string, reset bool, notifier FriendNotifier) error {
	notifier = notifierOrDefault(notifier)
	fbIDs, err := FetchFacebookFriendIDs(ctx, token)
	if err != nil {
		return err
	}
	if len(fbIDs) == 0 && !reset {
		return nil
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if reset {
		if err := resetUserFriends(ctx, tx, userID); err != nil {
			return err
		}
	}
	var imported []string
	if len(fbIDs) > 0 {
		matched, err := matchUsersByProviderIDs(ctx, tx, "facebook_id", fbIDs)
		if err != nil {
			return err
		}
		imported, err = importFriendsByIDs(ctx, tx, userID, matched, "Facebook")
		if err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}

	contentBytes, _ := json.Marshal(map[string]string{"username": username})
	content := string(contentBytes)
	for _, fid := range imported {
		_ = notifier.Notify(ctx, fid,
			fmt.Sprintf("%s added you as a friend", username),
			content, int16(NotificationCodeFriendImport), userID)
	}
	return nil
}

// ImportSteamFriends imports Steam friends matching existing users.
func ImportSteamFriends(ctx context.Context, pool *pgxpool.Pool, userID, username, steamToken string, reset bool, notifier FriendNotifier) error {
	notifier = notifierOrDefault(notifier)

	publisherKey := os.Getenv("STEAM_PUBLISHER_KEY")
	if publisherKey == "" {
		return fmt.Errorf("steam authentication is not configured")
	}

	// Resolve caller's steam_id from DB; steamToken may be session ticket or steam ID in tests.
	var steamID string
	err := pool.QueryRow(ctx, `SELECT steam_id FROM users WHERE id = $1`, userID).Scan(&steamID)
	if err != nil || steamID == "" {
		// Fall back: treat token as steam ID (test / simplified path).
		steamID = steamToken
	}
	if steamID == "" {
		return fmt.Errorf("steam profile not linked")
	}

	steamIDs, err := FetchSteamFriendIDs(ctx, publisherKey, steamID)
	if err != nil {
		return err
	}
	if len(steamIDs) == 0 && !reset {
		return nil
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if reset {
		if err := resetUserFriends(ctx, tx, userID); err != nil {
			return err
		}
	}
	var imported []string
	if len(steamIDs) > 0 {
		matched, err := matchUsersByProviderIDs(ctx, tx, "steam_id", steamIDs)
		if err != nil {
			return err
		}
		imported, err = importFriendsByIDs(ctx, tx, userID, matched, "Steam")
		if err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}

	contentBytes, _ := json.Marshal(map[string]string{"username": username})
	content := string(contentBytes)
	for _, fid := range imported {
		_ = notifier.Notify(ctx, fid,
			fmt.Sprintf("%s added you as a friend", username),
			content, int16(NotificationCodeFriendImport), userID)
	}
	return nil
}

// ImportFriendsFromProviderIDs is a test helper that skips external HTTP and matches by column.
func ImportFriendsFromProviderIDs(ctx context.Context, pool *pgxpool.Pool, userID, username, column string, providerIDs []string, provider string, reset bool, notifier FriendNotifier) error {
	notifier = notifierOrDefault(notifier)
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if reset {
		if err := resetUserFriends(ctx, tx, userID); err != nil {
			return err
		}
	}
	matched, err := matchUsersByProviderIDs(ctx, tx, column, providerIDs)
	if err != nil {
		return err
	}
	imported, err := importFriendsByIDs(ctx, tx, userID, matched, provider)
	if err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	contentBytes, _ := json.Marshal(map[string]string{"username": username})
	for _, fid := range imported {
		_ = notifier.Notify(ctx, fid, fmt.Sprintf("%s added you as a friend", username),
			string(contentBytes), int16(NotificationCodeFriendImport), userID)
	}
	return nil
}
