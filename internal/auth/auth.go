package auth

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"
)

// User represents a user profile retrieved from database.
type User struct {
	ID          uuid.UUID
	Username    string
	DisplayName string
	Email       *string
	DisableTime time.Time
}

// SessionRegistry handles refresh token storage and rotation (token family tracking).
type SessionRegistry struct {
	mu           sync.RWMutex
	tokens       map[string]string // key: refresh_token, value: user_id
	tokenFamily  map[string]string // key: refresh_token, value: parent_refresh_token (for rotation tracking)
	usedTokens   map[string]bool   // key: refresh_token, value: true if already rotated
	revokedUsers map[string]bool   // key: user_id, value: true if all sessions are revoked
}

// NewSessionRegistry creates a new instance of SessionRegistry.
func NewSessionRegistry() *SessionRegistry {
	return &SessionRegistry{
		tokens:       make(map[string]string),
		tokenFamily:  make(map[string]string),
		usedTokens:   make(map[string]bool),
		revokedUsers: make(map[string]bool),
	}
}

// RegisterSession registers a new refresh token for a user.
func (sr *SessionRegistry) RegisterSession(userID, token string, parentToken string) {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	sr.tokens[token] = userID
	if parentToken != "" {
		sr.tokenFamily[token] = parentToken
		sr.usedTokens[parentToken] = true
	}
}

// ValidateAndRotateSession validates a refresh token and performs single-use rotation.
// Returns the associated User ID and a boolean indicating if reuse/theft was detected.
func (sr *SessionRegistry) ValidateAndRotateSession(token string) (string, bool, error) {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	userID, exists := sr.tokens[token]
	if !exists {
		return "", false, errors.New("refresh token not found")
	}

	if sr.revokedUsers[userID] {
		return "", false, errors.New("user sessions are revoked")
	}

	// Token reuse detection (theft/replay attack prevention)
	if sr.usedTokens[token] {
		// Revoke all tokens in this user's registry
		for t, uid := range sr.tokens {
			if uid == userID {
				delete(sr.tokens, t)
				delete(sr.tokenFamily, t)
				delete(sr.usedTokens, t)
			}
		}
		sr.revokedUsers[userID] = true
		return userID, true, errors.New("refresh token already used: token family revoked")
	}

	return userID, false, nil
}

// RevokeAllSessions revokes all sessions associated with a user ID.
func (sr *SessionRegistry) RevokeAllSessions(userID string) {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	sr.revokedUsers[userID] = true
	for t, uid := range sr.tokens {
		if uid == userID {
			delete(sr.tokens, t)
			delete(sr.tokenFamily, t)
			delete(sr.usedTokens, t)
		}
	}
}

// RegisterEmail creates a new user account with an email and password.
func RegisterEmail(ctx context.Context, pool *pgxpool.Pool, username, email, password, displayName string) (*User, error) {
	if len(password) < 8 || len(password) > 128 {
		return nil, errors.New("password must be between 8 and 128 characters")
	}
	if len(username) < 3 || len(username) > 64 {
		return nil, errors.New("username must be between 3 and 64 characters")
	}

	// 1. Check password strength
	hasUpper := false
	hasLower := false
	hasDigit := false
	for _, char := range password {
		if char >= 'A' && char <= 'Z' {
			hasUpper = true
		} else if char >= 'a' && char <= 'z' {
			hasLower = true
		} else if char >= '0' && char <= '9' {
			hasDigit = true
		}
	}
	if !hasUpper || !hasLower || !hasDigit {
		return nil, errors.New("password must contain at least one uppercase letter, one lowercase letter, and one digit")
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

// AuthenticateCustom authenticates a custom device ID, creating a user if they do not exist.
func AuthenticateCustom(ctx context.Context, pool *pgxpool.Pool, customID string) (*User, error) {
	if customID == "" {
		return nil, errors.New("custom ID cannot be empty")
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
		// Found user
		if user.DisableTime.After(time.Unix(0, 0)) {
			return nil, errors.New("account disabled")
		}
		return &user, nil
	}

	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("failed to query custom_id: %w", err)
	}

	// Generate a unique random username
	userID := uuid.New()
	username := fmt.Sprintf("user_%s", strings.ReplaceAll(uuid.New().String()[:8], "-", ""))

	// Insert new user linked to custom ID
	insertQuery := `
		INSERT INTO users (id, username, custom_id, display_name)
		VALUES ($1, $2, $3, $4)
		RETURNING id, username, display_name, email, disable_time
	`

	err = pool.QueryRow(ctx, insertQuery, userID, username, customID, username).Scan(
		&user.ID, &user.Username, &user.DisplayName, &user.Email, &user.DisableTime,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create custom user: %w", err)
	}

	return &user, nil
}

// AuthenticateSocial authenticates a social provider ID, creating a user if they do not exist.
func AuthenticateSocial(ctx context.Context, pool *pgxpool.Pool, provider string, providerID string) (*User, error) {
	if providerID == "" {
		return nil, errors.New("provider ID cannot be empty")
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
	default:
		return nil, fmt.Errorf("unsupported provider: %s", provider)
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
		// Found user
		if user.DisableTime.After(time.Unix(0, 0)) {
			return nil, errors.New("account disabled")
		}
		return &user, nil
	}

	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("failed to query social provider: %w", err)
	}

	// Register new user linked to social provider
	userID := uuid.New()
	username := fmt.Sprintf("user_%s", strings.ReplaceAll(uuid.New().String()[:8], "-", ""))

	insertQuery := fmt.Sprintf(`
		INSERT INTO users (id, username, %s, display_name)
		VALUES ($1, $2, $3, $4)
		RETURNING id, username, display_name, email, disable_time
	`, providerColumn)

	err = pool.QueryRow(ctx, insertQuery, userID, username, providerID, username).Scan(
		&user.ID, &user.Username, &user.DisplayName, &user.Email, &user.DisableTime,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create social user: %w", err)
	}

	return &user, nil
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
