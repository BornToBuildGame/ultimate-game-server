package auth

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"
)

// GetAccount loads the full account profile including linked devices.
func GetAccount(ctx context.Context, pool *pgxpool.Pool, userID uuid.UUID) (*User, error) {
	query := `
		SELECT id, username, COALESCE(display_name,''), COALESCE(avatar_url,''), lang_tag,
		       COALESCE(location,''), COALESCE(timezone,''), COALESCE(metadata::text,'{}'),
		       email, custom_id, apple_id, google_id, facebook_id, gamecenter_id, steam_id,
		       facebook_instant_game_id, disable_time, create_time, update_time
		FROM users WHERE id = $1
	`
	var u User
	err := pool.QueryRow(ctx, query, userID).Scan(
		&u.ID, &u.Username, &u.DisplayName, &u.AvatarURL, &u.LangTag,
		&u.Location, &u.Timezone, &u.Metadata,
		&u.Email, &u.CustomID, &u.AppleID, &u.GoogleID, &u.FacebookID, &u.GamecenterID, &u.SteamID,
		&u.FacebookIGID, &u.DisableTime, &u.CreateTime, &u.UpdateTime,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("user not found")
		}
		return nil, err
	}

	rows, err := pool.Query(ctx, `SELECT id FROM user_device WHERE user_id = $1`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		u.Devices = append(u.Devices, id)
	}
	return &u, rows.Err()
}

// AccountUpdate holds mutable account profile fields.
type AccountUpdate struct {
	Username    *string
	DisplayName *string
	AvatarURL   *string
	LangTag     *string
	Location    *string
	Timezone    *string
	Metadata    *string
}

// UpdateAccount updates profile fields for a user.
func UpdateAccount(ctx context.Context, pool *pgxpool.Pool, userID uuid.UUID, upd AccountUpdate) error {
	if upd.Username != nil {
		u := strings.TrimSpace(*upd.Username)
		if len(u) < 3 || len(u) > 64 {
			return errors.New("username must be between 3 and 64 characters")
		}
		upd.Username = &u
	}
	if upd.Metadata != nil {
		var js json.RawMessage
		if err := json.Unmarshal([]byte(*upd.Metadata), &js); err != nil {
			return errors.New("metadata must be valid JSON")
		}
	}

	query := `
		UPDATE users SET
			username = COALESCE($2, username),
			display_name = COALESCE($3, display_name),
			avatar_url = COALESCE($4, avatar_url),
			lang_tag = COALESCE($5, lang_tag),
			location = COALESCE($6, location),
			timezone = COALESCE($7, timezone),
			metadata = COALESCE($8::jsonb, metadata),
			update_time = now()
		WHERE id = $1
	`
	cmd, err := pool.Exec(ctx, query, userID, upd.Username, upd.DisplayName, upd.AvatarURL,
		upd.LangTag, upd.Location, upd.Timezone, upd.Metadata)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return errors.New("username already taken")
		}
		return err
	}
	if cmd.RowsAffected() == 0 {
		return errors.New("user not found")
	}
	return nil
}

// LinkEmail sets email+password on an existing account.
func LinkEmail(ctx context.Context, pool *pgxpool.Pool, userID uuid.UUID, email, password string) error {
	if !ValidateEmailAddress(email) {
		return errors.New("invalid email address")
	}
	if err := ValidatePasswordPolicy(password); err != nil {
		return err
	}
	hashed, err := bcrypt.GenerateFromPassword([]byte(password), 12)
	if err != nil {
		return err
	}
	cmd, err := pool.Exec(ctx, `
		UPDATE users SET email = $1, password = $2, update_time = now() WHERE id = $3
	`, email, hashed, userID)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return errors.New("email already taken")
		}
		return err
	}
	if cmd.RowsAffected() == 0 {
		return errors.New("user not found")
	}
	return nil
}

// ChangePassword updates password and is intended to be paired with JWT revocation by the caller.
func ChangePassword(ctx context.Context, pool *pgxpool.Pool, userID uuid.UUID, oldPassword, newPassword string) error {
	if err := ValidatePasswordPolicy(newPassword); err != nil {
		return err
	}
	var stored []byte
	err := pool.QueryRow(ctx, `SELECT password FROM users WHERE id = $1`, userID).Scan(&stored)
	if err != nil {
		return errors.New("user not found")
	}
	if len(stored) == 0 {
		return errors.New("account has no password")
	}
	if err := bcrypt.CompareHashAndPassword(stored, []byte(oldPassword)); err != nil {
		return errors.New("invalid credentials")
	}
	hashed, err := bcrypt.GenerateFromPassword([]byte(newPassword), 12)
	if err != nil {
		return err
	}
	_, err = pool.Exec(ctx, `UPDATE users SET password = $1, update_time = now() WHERE id = $2`, hashed, userID)
	return err
}

// CountIdentities returns how many login methods the user currently has.
func CountIdentities(ctx context.Context, pool *pgxpool.Pool, userID uuid.UUID) (int, error) {
	var email, custom, apple, google, facebook, gc, steam, fig sql.NullString
	var hasPassword bool
	err := pool.QueryRow(ctx, `
		SELECT email, custom_id, apple_id, google_id, facebook_id, gamecenter_id, steam_id,
		       facebook_instant_game_id,
		       (password IS NOT NULL AND length(password) > 0)
		FROM users WHERE id = $1
	`, userID).Scan(&email, &custom, &apple, &google, &facebook, &gc, &steam, &fig, &hasPassword)
	if err != nil {
		return 0, err
	}
	n := 0
	if email.Valid && email.String != "" && hasPassword {
		n++
	}
	for _, v := range []*sql.NullString{&custom, &apple, &google, &facebook, &gc, &steam, &fig} {
		if v.Valid && v.String != "" {
			n++
		}
	}
	var deviceCount int
	_ = pool.QueryRow(ctx, `SELECT COUNT(*) FROM user_device WHERE user_id = $1`, userID).Scan(&deviceCount)
	n += deviceCount
	return n, nil
}

// UnlinkProvider clears a provider column after enforcing last-identity guard.
func UnlinkProvider(ctx context.Context, pool *pgxpool.Pool, userID uuid.UUID, provider string) error {
	count, err := CountIdentities(ctx, pool, userID)
	if err != nil {
		return err
	}
	if count <= 1 {
		return errors.New("cannot unlink last identity")
	}

	var col string
	switch strings.ToLower(provider) {
	case "apple":
		col = "apple_id"
	case "google":
		col = "google_id"
	case "facebook":
		col = "facebook_id"
	case "gamecenter":
		col = "gamecenter_id"
	case "steam":
		col = "steam_id"
	case "custom":
		col = "custom_id"
	case "facebookinstantgame", "facebook_instant_game", "facebookinstant":
		col = "facebook_instant_game_id"
	case "email":
		cmd, err := pool.Exec(ctx, `
			UPDATE users SET email = NULL, password = NULL, update_time = now() WHERE id = $1
		`, userID)
		if err != nil {
			return err
		}
		if cmd.RowsAffected() == 0 {
			return errors.New("user not found")
		}
		return nil
	default:
		return fmt.Errorf("unsupported provider: %s", provider)
	}

	cmd, err := pool.Exec(ctx, fmt.Sprintf(`UPDATE users SET %s = NULL, update_time = now() WHERE id = $1`, col), userID)
	if err != nil {
		return err
	}
	if cmd.RowsAffected() == 0 {
		return errors.New("user not found")
	}
	return nil
}

// UnlinkDevice removes a device identity with last-identity guard.
func UnlinkDevice(ctx context.Context, pool *pgxpool.Pool, userID uuid.UUID, deviceID string) error {
	count, err := CountIdentities(ctx, pool, userID)
	if err != nil {
		return err
	}
	if count <= 1 {
		return errors.New("cannot unlink last identity")
	}
	cmd, err := pool.Exec(ctx, `DELETE FROM user_device WHERE user_id = $1 AND id = $2`, userID, deviceID)
	if err != nil {
		return err
	}
	if cmd.RowsAffected() == 0 {
		return errors.New("device not found")
	}
	return nil
}

// AuthenticateDevice finds or creates a user via user_device.
func AuthenticateDevice(ctx context.Context, pool *pgxpool.Pool, deviceID string, opts AuthOptions) (*User, bool, error) {
	if deviceID == "" {
		return nil, false, errors.New("device ID cannot be empty")
	}
	create := opts.Create

	var userID uuid.UUID
	err := pool.QueryRow(ctx, `SELECT user_id FROM user_device WHERE id = $1`, deviceID).Scan(&userID)
	if err == nil {
		u, err := loadUserBasic(ctx, pool, userID)
		return u, false, err
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, false, err
	}
	if !create {
		return nil, false, errors.New("device not found")
	}

	username := opts.Username
	if username == "" {
		username = randomUsername()
	}
	newID := uuid.New()
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback(ctx)

	var u User
	err = tx.QueryRow(ctx, `
		INSERT INTO users (id, username, display_name)
		VALUES ($1, $2, $3)
		RETURNING id, username, display_name, email, disable_time
	`, newID, username, username).Scan(&u.ID, &u.Username, &u.DisplayName, &u.Email, &u.DisableTime)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return nil, false, errors.New("username already taken")
		}
		return nil, false, err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO user_device (id, user_id) VALUES ($1, $2)
	`, deviceID, newID)
	if err != nil {
		return nil, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, false, err
	}
	return &u, true, nil
}

func loadUserBasic(ctx context.Context, pool *pgxpool.Pool, userID uuid.UUID) (*User, error) {
	var u User
	err := pool.QueryRow(ctx, `
		SELECT id, username, display_name, email, disable_time FROM users WHERE id = $1
	`, userID).Scan(&u.ID, &u.Username, &u.DisplayName, &u.Email, &u.DisableTime)
	if err != nil {
		return nil, err
	}
	if u.DisableTime.After(time.Unix(0, 0)) {
		return nil, errors.New("account disabled")
	}
	return &u, nil
}

// GetUsersPublic returns public profile fields for users matching ids, usernames, or facebook IDs.
// Disabled accounts are omitted. Metadata is returned as stored JSON text.
func GetUsersPublic(ctx context.Context, pool *pgxpool.Pool, ids, usernames, facebookIDs []string) ([]*User, error) {
	seen := make(map[uuid.UUID]struct{})
	var out []*User

	appendUser := func(u *User) {
		if u == nil {
			return
		}
		if u.DisableTime.After(time.Unix(0, 0)) {
			return
		}
		if _, ok := seen[u.ID]; ok {
			return
		}
		seen[u.ID] = struct{}{}
		out = append(out, u)
	}

	queryPublic := `
		SELECT id, username, COALESCE(display_name,''), COALESCE(avatar_url,''), lang_tag,
		       COALESCE(location,''), COALESCE(timezone,''), COALESCE(metadata::text,'{}'),
		       disable_time, create_time, update_time
		FROM users WHERE `

	for _, idStr := range ids {
		idStr = strings.TrimSpace(idStr)
		if idStr == "" {
			continue
		}
		uid, err := uuid.Parse(idStr)
		if err != nil {
			continue
		}
		var u User
		err = pool.QueryRow(ctx, queryPublic+`id = $1`, uid).Scan(
			&u.ID, &u.Username, &u.DisplayName, &u.AvatarURL, &u.LangTag,
			&u.Location, &u.Timezone, &u.Metadata, &u.DisableTime, &u.CreateTime, &u.UpdateTime,
		)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				continue
			}
			return nil, err
		}
		appendUser(&u)
	}

	for _, name := range usernames {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		var u User
		err := pool.QueryRow(ctx, queryPublic+`LOWER(username) = LOWER($1)`, name).Scan(
			&u.ID, &u.Username, &u.DisplayName, &u.AvatarURL, &u.LangTag,
			&u.Location, &u.Timezone, &u.Metadata, &u.DisableTime, &u.CreateTime, &u.UpdateTime,
		)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				continue
			}
			return nil, err
		}
		appendUser(&u)
	}

	for _, fb := range facebookIDs {
		fb = strings.TrimSpace(fb)
		if fb == "" {
			continue
		}
		var u User
		err := pool.QueryRow(ctx, queryPublic+`facebook_id = $1`, fb).Scan(
			&u.ID, &u.Username, &u.DisplayName, &u.AvatarURL, &u.LangTag,
			&u.Location, &u.Timezone, &u.Metadata, &u.DisableTime, &u.CreateTime, &u.UpdateTime,
		)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				continue
			}
			return nil, err
		}
		appendUser(&u)
	}

	return out, nil
}

func randomUsername() string {
	return fmt.Sprintf("user_%s", strings.ReplaceAll(uuid.New().String()[:8], "-", ""))
}

