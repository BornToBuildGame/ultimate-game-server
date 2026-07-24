package auth

import (
	"context"
	"database/sql"
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

// User represents a user profile retrieved from database.
type User struct {
	ID           uuid.UUID
	Username     string
	DisplayName  string
	AvatarURL    string
	LangTag      string
	Location     string
	Timezone     string
	Metadata     string
	Email        *string
	CustomID     *string
	AppleID      *string
	GoogleID     *string
	FacebookID   *string
	GamecenterID *string
	SteamID      *string
	FacebookIGID *string
	DisableTime  time.Time
	CreateTime   time.Time
	UpdateTime   time.Time
	Devices      []string
}

// AuthOptions controls create-or-login semantics shared by authenticate endpoints.
type AuthOptions struct {
	Create   bool
	Username string
	Vars     map[string]string
}

// RegisterEmail creates a new user account with an email and password.
func RegisterEmail(ctx context.Context, pool *pgxpool.Pool, username, email, password, displayName string) (*User, error) {
	if !ValidateEmailAddress(email) {
		return nil, errors.New("invalid email address")
	}
	if err := ValidatePasswordPolicy(password); err != nil {
		return nil, err
	}
	if len(username) < 3 || len(username) > 64 {
		return nil, errors.New("username must be between 3 and 64 characters")
	}

	// 2. Hash password with bcrypt work factor 12
	hashed, err := bcrypt.GenerateFromPassword([]byte(password), 12)
	if err != nil {
		return nil, fmt.Errorf("failed to hash password: %w", err)
	}

	// 3. Insert user record
	userID := uuid.New()
	query := `
		INSERT INTO users (id, username, email, password, display_name)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id, username, display_name, email, disable_time
	`

	var user User
	err = pool.QueryRow(ctx, query, userID, username, email, hashed, displayName).Scan(
		&user.ID, &user.Username, &user.DisplayName, &user.Email, &user.DisableTime,
	)

	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" { // unique violation
			if strings.Contains(pgErr.ConstraintName, "email") {
				return nil, errors.New("email already taken")
			}
			return nil, errors.New("username already taken")
		}
		return nil, fmt.Errorf("failed to register user: %w", err)
	}

	return &user, nil
}

// AuthenticateEmail verifies a user email and password.
func AuthenticateEmail(ctx context.Context, pool *pgxpool.Pool, email, password string) (*User, error) {
	if !ValidateEmailAddress(email) {
		return nil, errors.New("invalid credentials")
	}
	query := `
		SELECT id, username, display_name, email, password, disable_time
		FROM users
		WHERE email = $1
	`

	var user User
	var dbPassword []byte
	err := pool.QueryRow(ctx, query, email).Scan(
		&user.ID, &user.Username, &user.DisplayName, &user.Email, &dbPassword, &user.DisableTime,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("invalid credentials")
		}
		return nil, fmt.Errorf("failed to query user: %w", err)
	}

	// Check if account is disabled (disable_time > 1970-01-01)
	if user.DisableTime.After(time.Unix(0, 0)) {
		return nil, errors.New("account disabled")
	}

	// Verify bcrypt hash
	err = bcrypt.CompareHashAndPassword(dbPassword, []byte(password))
	if err != nil {
		return nil, errors.New("invalid credentials")
	}

	return &user, nil
}

// AuthenticateCustom authenticates a custom ID, creating a user if they do not exist.
func AuthenticateCustom(ctx context.Context, pool *pgxpool.Pool, customID string) (*User, error) {
	u, _, err := AuthenticateCustomWithOpts(ctx, pool, customID, AuthOptions{Create: true})
	return u, err
}

// AuthenticateCustomWithOpts supports create=false and optional username.
func AuthenticateCustomWithOpts(ctx context.Context, pool *pgxpool.Pool, customID string, opts AuthOptions) (*User, bool, error) {
	if customID == "" {
		return nil, false, errors.New("custom ID cannot be empty")
	}

	query := `
		SELECT id, username, display_name, email, disable_time
		FROM users
		WHERE custom_id = $1
	`

	var user User
	err := pool.QueryRow(ctx, query, customID).Scan(
		&user.ID, &user.Username, &user.DisplayName, &user.Email, &user.DisableTime,
	)

	if err == nil {
		if user.DisableTime.After(time.Unix(0, 0)) {
			return nil, false, errors.New("account disabled")
		}
		return &user, false, nil
	}

	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, false, fmt.Errorf("failed to query custom_id: %w", err)
	}
	if !opts.Create {
		return nil, false, errors.New("user not found")
	}

	userID := uuid.New()
	username := opts.Username
	if username == "" {
		username = randomUsername()
	}

	insertQuery := `
		INSERT INTO users (id, username, custom_id, display_name)
		VALUES ($1, $2, $3, $4)
		RETURNING id, username, display_name, email, disable_time
	`

	err = pool.QueryRow(ctx, insertQuery, userID, username, customID, username).Scan(
		&user.ID, &user.Username, &user.DisplayName, &user.Email, &user.DisableTime,
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return nil, false, errors.New("username already taken")
		}
		return nil, false, fmt.Errorf("failed to create custom user: %w", err)
	}

	return &user, true, nil
}

// AuthenticateSocial authenticates a social provider ID, creating a user if they do not exist.
func AuthenticateSocial(ctx context.Context, pool *pgxpool.Pool, provider string, providerID string) (*User, error) {
	u, _, err := AuthenticateSocialWithOpts(ctx, pool, provider, providerID, AuthOptions{Create: true})
	return u, err
}

// AuthenticateSocialWithOpts supports create=false and optional username.
func AuthenticateSocialWithOpts(ctx context.Context, pool *pgxpool.Pool, provider, providerID string, opts AuthOptions) (*User, bool, error) {
	if providerID == "" {
		return nil, false, errors.New("provider ID cannot be empty")
	}

	var providerColumn string
	switch strings.ToLower(provider) {
	case "apple":
		providerColumn = "apple_id"
	case "google":
		providerColumn = "google_id"
	case "facebook":
		providerColumn = "facebook_id"
	case "gamecenter":
		providerColumn = "gamecenter_id"
	case "steam":
		providerColumn = "steam_id"
	case "facebookinstantgame", "facebook_instant_game", "facebookinstant":
		providerColumn = "facebook_instant_game_id"
	default:
		return nil, false, fmt.Errorf("unsupported provider: %s", provider)
	}

	query := fmt.Sprintf(`
		SELECT id, username, display_name, email, disable_time
		FROM users
		WHERE %s = $1
	`, providerColumn)

	var user User
	err := pool.QueryRow(ctx, query, providerID).Scan(
		&user.ID, &user.Username, &user.DisplayName, &user.Email, &user.DisableTime,
	)

	if err == nil {
		if user.DisableTime.After(time.Unix(0, 0)) {
			return nil, false, errors.New("account disabled")
		}
		return &user, false, nil
	}

	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, false, fmt.Errorf("failed to query social provider: %w", err)
	}
	if !opts.Create {
		return nil, false, errors.New("user not found")
	}

	userID := uuid.New()
	username := opts.Username
	if username == "" {
		username = randomUsername()
	}

	insertQuery := fmt.Sprintf(`
		INSERT INTO users (id, username, %s, display_name)
		VALUES ($1, $2, $3, $4)
		RETURNING id, username, display_name, email, disable_time
	`, providerColumn)

	err = pool.QueryRow(ctx, insertQuery, userID, username, providerID, username).Scan(
		&user.ID, &user.Username, &user.DisplayName, &user.Email, &user.DisableTime,
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return nil, false, errors.New("username already taken")
		}
		return nil, false, fmt.Errorf("failed to create social user: %w", err)
	}

	return &user, true, nil
}

// LinkProvider links a social provider ID to an existing user profile.
func LinkProvider(ctx context.Context, pool *pgxpool.Pool, userID uuid.UUID, provider string, providerID string) error {
	var providerColumn string
	switch strings.ToLower(provider) {
	case "apple":
		providerColumn = "apple_id"
	case "google":
		providerColumn = "google_id"
	case "facebook":
		providerColumn = "facebook_id"
	case "gamecenter":
		providerColumn = "gamecenter_id"
	case "steam":
		providerColumn = "steam_id"
	case "custom":
		providerColumn = "custom_id"
	case "facebookinstantgame", "facebook_instant_game", "facebookinstant":
		providerColumn = "facebook_instant_game_id"
	default:
		return fmt.Errorf("unsupported provider: %s", provider)
	}

	query := fmt.Sprintf(`
		UPDATE users
		SET %s = $1, update_time = now()
		WHERE id = $2
	`, providerColumn)

	cmdTag, err := pool.Exec(ctx, query, providerID, userID)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" { // unique constraint violation
			return errors.New("provider identity already linked to another account")
		}
		return fmt.Errorf("failed to link provider: %w", err)
	}

	if cmdTag.RowsAffected() == 0 {
		return errors.New("user not found")
	}

	return nil
}

// AddDevice registers a device ID associated with a user.
func AddDevice(ctx context.Context, pool *pgxpool.Pool, userID uuid.UUID, deviceID string, preferences string, pushTokens map[string]string) error {
	query := `
		INSERT INTO user_device (id, user_id, preferences, push_token_amazon, push_token_android, push_token_huawei, push_token_ios, push_token_web)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (id) DO UPDATE
		SET user_id = EXCLUDED.user_id,
		    preferences = EXCLUDED.preferences,
		    push_token_amazon = EXCLUDED.push_token_amazon,
		    push_token_android = EXCLUDED.push_token_android,
		    push_token_huawei = EXCLUDED.push_token_huawei,
		    push_token_ios = EXCLUDED.push_token_ios,
		    push_token_web = EXCLUDED.push_token_web
	`

	prefJSON := preferences
	if prefJSON == "" {
		prefJSON = "{}"
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, query,
		deviceID,
		userID,
		prefJSON,
		pushTokens["amazon"],
		pushTokens["android"],
		pushTokens["huawei"],
		pushTokens["ios"],
		pushTokens["web"],
	)
	if err != nil {
		return err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// SoftDeleteUser soft deletes a user account by disabling the profile and writing a tombstone.
func SoftDeleteUser(ctx context.Context, pool *pgxpool.Pool, userID uuid.UUID) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// 1. Write user_tombstone record
	tombstoneQuery := `
		INSERT INTO user_tombstone (user_id)
		VALUES ($1)
		ON CONFLICT (user_id) DO NOTHING
	`
	_, err = tx.Exec(ctx, tombstoneQuery, userID)
	if err != nil {
		return fmt.Errorf("failed to insert user tombstone: %w", err)
	}

	// 2. Disable user in users table
	disableQuery := `
		UPDATE users
		SET disable_time = now(), update_time = now()
		WHERE id = $1
	`
	cmdTag, err := tx.Exec(ctx, disableQuery, userID)
	if err != nil {
		return fmt.Errorf("failed to disable user: %w", err)
	}

	if cmdTag.RowsAffected() == 0 {
		return sql.ErrNoRows
	}

	return tx.Commit(ctx)
}
