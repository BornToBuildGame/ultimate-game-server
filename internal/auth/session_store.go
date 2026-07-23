package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	redisRefreshPrefix   = "session:refresh:"
	redisUserSessPrefix  = "session:user:"
	redisUsedPrefix      = "session:used:"
	redisRevokedPrefix   = "session:revoked:"
	redisJWTDenyPrefix   = "session:jwt_deny:"
	defaultRefreshTTL    = 7 * 24 * time.Hour
)

// SessionStore abstracts refresh-token + JWT denial storage.
type SessionStore interface {
	RegisterSession(ctx context.Context, userID, token, parentToken string) error
	ValidateAndRotateSession(ctx context.Context, token string) (userID string, theftDetected bool, err error)
	RevokeAllSessions(ctx context.Context, userID string) error
	RevokeRefreshToken(ctx context.Context, token string) error
	BlacklistAccessJTI(ctx context.Context, jti string, until time.Time) error
	IsAccessJTIBlacklisted(ctx context.Context, jti string) (bool, error)
	IsUserRevoked(ctx context.Context, userID string) (bool, error)
}

// MemorySessionStore is the in-process implementation (tests / no Redis).
type MemorySessionStore struct {
	mu           sync.RWMutex
	tokens       map[string]string
	tokenFamily  map[string]string
	usedTokens   map[string]bool
	revokedUsers map[string]bool
	jwtDeny      map[string]time.Time
}

// NewSessionRegistry returns a memory-backed store compatible with historical API.
func NewSessionRegistry() *SessionRegistry {
	return &SessionRegistry{store: NewMemorySessionStore()}
}

// SessionRegistry wraps SessionStore with the historical sync API used by handlers/tests.
type SessionRegistry struct {
	store SessionStore
}

// NewSessionRegistryWithStore builds a registry over an arbitrary store.
func NewSessionRegistryWithStore(store SessionStore) *SessionRegistry {
	if store == nil {
		store = NewMemorySessionStore()
	}
	return &SessionRegistry{store: store}
}

func NewMemorySessionStore() *MemorySessionStore {
	return &MemorySessionStore{
		tokens:       make(map[string]string),
		tokenFamily:  make(map[string]string),
		usedTokens:   make(map[string]bool),
		revokedUsers: make(map[string]bool),
		jwtDeny:      make(map[string]time.Time),
	}
}

func (sr *SessionRegistry) Store() SessionStore { return sr.store }

func (sr *SessionRegistry) RegisterSession(userID, token string, parentToken string) {
	_ = sr.store.RegisterSession(context.Background(), userID, token, parentToken)
}

func (sr *SessionRegistry) ValidateAndRotateSession(token string) (string, bool, error) {
	return sr.store.ValidateAndRotateSession(context.Background(), token)
}

func (sr *SessionRegistry) RevokeAllSessions(userID string) {
	_ = sr.store.RevokeAllSessions(context.Background(), userID)
}

func (m *MemorySessionStore) RegisterSession(ctx context.Context, userID, token, parentToken string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.revokedUsers, userID)
	m.tokens[token] = userID
	if parentToken != "" {
		m.tokenFamily[token] = parentToken
		m.usedTokens[parentToken] = true
	}
	return nil
}

func (m *MemorySessionStore) ValidateAndRotateSession(ctx context.Context, token string) (string, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	userID, exists := m.tokens[token]
	if !exists {
		return "", false, errors.New("refresh token not found")
	}
	if m.revokedUsers[userID] {
		return "", false, errors.New("user sessions are revoked")
	}
	if m.usedTokens[token] {
		for t, uid := range m.tokens {
			if uid == userID {
				delete(m.tokens, t)
				delete(m.tokenFamily, t)
				delete(m.usedTokens, t)
			}
		}
		m.revokedUsers[userID] = true
		return userID, true, errors.New("refresh token already used: token family revoked")
	}
	return userID, false, nil
}

func (m *MemorySessionStore) RevokeAllSessions(ctx context.Context, userID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.revokedUsers[userID] = true
	for t, uid := range m.tokens {
		if uid == userID {
			delete(m.tokens, t)
			delete(m.tokenFamily, t)
			delete(m.usedTokens, t)
		}
	}
	return nil
}

func (m *MemorySessionStore) RevokeRefreshToken(ctx context.Context, token string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.tokens, token)
	delete(m.tokenFamily, token)
	delete(m.usedTokens, token)
	return nil
}

func (m *MemorySessionStore) BlacklistAccessJTI(ctx context.Context, jti string, until time.Time) error {
	if jti == "" {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jwtDeny[jti] = until
	return nil
}

func (m *MemorySessionStore) IsAccessJTIBlacklisted(ctx context.Context, jti string) (bool, error) {
	if jti == "" {
		return false, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	until, ok := m.jwtDeny[jti]
	if !ok {
		return false, nil
	}
	if time.Now().After(until) {
		delete(m.jwtDeny, jti)
		return false, nil
	}
	return true, nil
}

func (m *MemorySessionStore) IsUserRevoked(ctx context.Context, userID string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.revokedUsers[userID], nil
}

// RedisSessionStore persists refresh tokens and JWT deny list in Redis.
type RedisSessionStore struct {
	rdb *redis.Client
	ttl time.Duration
	mem *MemorySessionStore // fallback when redis ops fail mid-flight for local consistency
}

// NewRedisSessionStore creates a Redis-backed store. Falls back operations return error if rdb nil.
func NewRedisSessionStore(rdb *redis.Client, ttl time.Duration) *RedisSessionStore {
	if ttl <= 0 {
		ttl = defaultRefreshTTL
	}
	return &RedisSessionStore{rdb: rdb, ttl: ttl, mem: NewMemorySessionStore()}
}

type refreshPayload struct {
	UserID string `json:"user_id"`
}

func (r *RedisSessionStore) RegisterSession(ctx context.Context, userID, token, parentToken string) error {
	if r.rdb == nil {
		return r.mem.RegisterSession(ctx, userID, token, parentToken)
	}
	_ = r.rdb.Del(ctx, redisRevokedPrefix+userID)
	payload, _ := json.Marshal(refreshPayload{UserID: userID})
	pipe := r.rdb.Pipeline()
	pipe.Set(ctx, redisRefreshPrefix+token, payload, r.ttl)
	pipe.SAdd(ctx, redisUserSessPrefix+userID, token)
	pipe.Expire(ctx, redisUserSessPrefix+userID, r.ttl)
	if parentToken != "" {
		pipe.Set(ctx, redisUsedPrefix+parentToken, "1", r.ttl)
	}
	_, err := pipe.Exec(ctx)
	return err
}

func (r *RedisSessionStore) ValidateAndRotateSession(ctx context.Context, token string) (string, bool, error) {
	if r.rdb == nil {
		return r.mem.ValidateAndRotateSession(ctx, token)
	}
	raw, err := r.rdb.Get(ctx, redisRefreshPrefix+token).Bytes()
	if err == redis.Nil {
		return "", false, errors.New("refresh token not found")
	}
	if err != nil {
		return "", false, err
	}
	var p refreshPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return "", false, err
	}
	revoked, _ := r.rdb.Exists(ctx, redisRevokedPrefix+p.UserID).Result()
	if revoked > 0 {
		return "", false, errors.New("user sessions are revoked")
	}
	used, _ := r.rdb.Exists(ctx, redisUsedPrefix+token).Result()
	if used > 0 {
		_ = r.RevokeAllSessions(ctx, p.UserID)
		return p.UserID, true, errors.New("refresh token already used: token family revoked")
	}
	return p.UserID, false, nil
}

func (r *RedisSessionStore) RevokeAllSessions(ctx context.Context, userID string) error {
	if r.rdb == nil {
		return r.mem.RevokeAllSessions(ctx, userID)
	}
	tokens, err := r.rdb.SMembers(ctx, redisUserSessPrefix+userID).Result()
	if err != nil && err != redis.Nil {
		return err
	}
	pipe := r.rdb.Pipeline()
	pipe.Set(ctx, redisRevokedPrefix+userID, "1", r.ttl)
	for _, t := range tokens {
		pipe.Del(ctx, redisRefreshPrefix+t)
		pipe.Del(ctx, redisUsedPrefix+t)
	}
	pipe.Del(ctx, redisUserSessPrefix+userID)
	_, err = pipe.Exec(ctx)
	return err
}

func (r *RedisSessionStore) RevokeRefreshToken(ctx context.Context, token string) error {
	if r.rdb == nil {
		return r.mem.RevokeRefreshToken(ctx, token)
	}
	raw, err := r.rdb.Get(ctx, redisRefreshPrefix+token).Bytes()
	if err == redis.Nil {
		return nil
	}
	if err != nil {
		return err
	}
	var p refreshPayload
	_ = json.Unmarshal(raw, &p)
	pipe := r.rdb.Pipeline()
	pipe.Del(ctx, redisRefreshPrefix+token)
	if p.UserID != "" {
		pipe.SRem(ctx, redisUserSessPrefix+p.UserID, token)
	}
	_, err = pipe.Exec(ctx)
	return err
}

func (r *RedisSessionStore) BlacklistAccessJTI(ctx context.Context, jti string, until time.Time) error {
	if jti == "" {
		return nil
	}
	if r.rdb == nil {
		return r.mem.BlacklistAccessJTI(ctx, jti, until)
	}
	ttl := time.Until(until)
	if ttl <= 0 {
		return nil
	}
	return r.rdb.Set(ctx, redisJWTDenyPrefix+jti, "1", ttl).Err()
}

func (r *RedisSessionStore) IsAccessJTIBlacklisted(ctx context.Context, jti string) (bool, error) {
	if jti == "" {
		return false, nil
	}
	if r.rdb == nil {
		return r.mem.IsAccessJTIBlacklisted(ctx, jti)
	}
	n, err := r.rdb.Exists(ctx, redisJWTDenyPrefix+jti).Result()
	return n > 0, err
}

func (r *RedisSessionStore) IsUserRevoked(ctx context.Context, userID string) (bool, error) {
	if r.rdb == nil {
		return r.mem.IsUserRevoked(ctx, userID)
	}
	n, err := r.rdb.Exists(ctx, redisRevokedPrefix+userID).Result()
	return n > 0, err
}

// NewSessionStoreFromRedis picks Redis when available, else memory.
func NewSessionStoreFromRedis(rdb *redis.Client) SessionStore {
	if rdb == nil {
		return NewMemorySessionStore()
	}
	return NewRedisSessionStore(rdb, defaultRefreshTTL)
}

// Ensure MemorySessionStore implements SessionStore.
var _ SessionStore = (*MemorySessionStore)(nil)
var _ SessionStore = (*RedisSessionStore)(nil)

// Logout clears refresh token and optionally blacklists access JWT by jti.
func Logout(ctx context.Context, store SessionStore, tm *TokenManager, accessToken, refreshToken string) error {
	if refreshToken != "" {
		_ = store.RevokeRefreshToken(ctx, refreshToken)
	}
	if accessToken != "" && tm != nil {
		claims, err := tm.VerifyTokenIgnoreExpiry(accessToken)
		if err == nil && claims != nil {
			until := time.Now().Add(tm.Expiry())
			if claims.ExpiresAt != nil {
				until = claims.ExpiresAt.Time
			}
			_ = store.BlacklistAccessJTI(ctx, claims.ID, until)
			if claims.UserID != "" && refreshToken == "" && accessToken != "" {
				// empty refresh + empty? caller may want all — handled by LogoutAll
			}
		}
	}
	return nil
}

// LogoutAll revokes every refresh for the user and blacklists the current access jti.
func LogoutAll(ctx context.Context, store SessionStore, tm *TokenManager, userID, accessToken string) error {
	_ = store.RevokeAllSessions(ctx, userID)
	if accessToken != "" && tm != nil {
		claims, err := tm.VerifyTokenIgnoreExpiry(accessToken)
		if err == nil && claims != nil {
			until := time.Now().Add(tm.Expiry())
			if claims.ExpiresAt != nil {
				until = claims.ExpiresAt.Time
			}
			_ = store.BlacklistAccessJTI(ctx, claims.ID, until)
		}
	}
	return nil
}

// Format helper kept for redis key introspection in tests.
func RedisRefreshKey(token string) string {
	return fmt.Sprintf("%s%s", redisRefreshPrefix, token)
}
