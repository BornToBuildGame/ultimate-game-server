package auth

import (
	"net/mail"
	"strings"
	"sync"
	"time"
	"unicode"
)

var commonPasswords = map[string]struct{}{
	"password": {}, "12345678": {}, "123456789": {},
	"qwerty123": {}, "letmein": {}, "welcome1": {}, "admin123": {}, "iloveyou": {},
	"abc12345": {}, "monkey12": {}, "dragon12": {}, "master12": {}, "login123": {},
}

// ValidateEmailAddress checks RFC 5322-ish email via net/mail.
func ValidateEmailAddress(email string) bool {
	email = strings.TrimSpace(email)
	if email == "" || len(email) > 255 {
		return false
	}
	addr, err := mail.ParseAddress(email)
	if err != nil {
		return false
	}
	return addr.Address == email || strings.EqualFold(addr.Address, email)
}

// IsCommonPassword returns true if password is on the deny list (case-insensitive).
func IsCommonPassword(password string) bool {
	_, ok := commonPasswords[strings.ToLower(strings.TrimSpace(password))]
	return ok
}

// ValidatePasswordPolicy enforces length, complexity, and common-password checks.
func ValidatePasswordPolicy(password string) error {
	if len(password) < 8 || len(password) > 128 {
		return errPasswordLength
	}
	if IsCommonPassword(password) {
		return errCommonPassword
	}
	var hasUpper, hasLower, hasDigit bool
	for _, r := range password {
		switch {
		case unicode.IsUpper(r):
			hasUpper = true
		case unicode.IsLower(r):
			hasLower = true
		case unicode.IsDigit(r):
			hasDigit = true
		}
	}
	if !hasUpper || !hasLower || !hasDigit {
		return errPasswordComplexity
	}
	return nil
}

var (
	errPasswordLength     = errString("password must be between 8 and 128 characters")
	errCommonPassword     = errString("password is too common")
	errPasswordComplexity = errString("password must contain at least one uppercase letter, one lowercase letter, and one digit")
)

type errString string

func (e errString) Error() string { return string(e) }

// LoginLockout tracks failed authentication attempts per account and IP.
type LoginLockout struct {
	mu      sync.Mutex
	account map[string]*failBucket // email/custom/device key
	ip      map[string]*failBucket
}

type failBucket struct {
	fails  int
	reset  time.Time
	locked time.Time
}

const (
	accountFailLimit = 5
	accountWindow    = 15 * time.Minute
	ipFailLimit      = 20
	ipWindow         = 10 * time.Minute
)

// NewLoginLockout creates an in-memory lockout tracker.
func NewLoginLockout() *LoginLockout {
	return &LoginLockout{
		account: make(map[string]*failBucket),
		ip:      make(map[string]*failBucket),
	}
}

// Check returns an error if the identity or IP is currently locked out.
func (l *LoginLockout) Check(identityKey, ip string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	if b := l.account[identityKey]; b != nil {
		if now.Before(b.locked) {
			return errString("too many failed login attempts; try again later")
		}
		if now.After(b.reset) {
			delete(l.account, identityKey)
		}
	}
	if b := l.ip[ip]; b != nil {
		if now.Before(b.locked) {
			return errString("too many failed login attempts from this IP; try again later")
		}
		if now.After(b.reset) {
			delete(l.ip, ip)
		}
	}
	return nil
}

// RecordFailure increments fail counters; locks when thresholds are exceeded.
func (l *LoginLockout) RecordFailure(identityKey, ip string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	l.bump(l.account, identityKey, now, accountFailLimit, accountWindow)
	l.bump(l.ip, ip, now, ipFailLimit, ipWindow)
}

// ClearSuccess resets counters after a successful authentication.
func (l *LoginLockout) ClearSuccess(identityKey, ip string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.account, identityKey)
	// Do not clear IP on success — other identities may still be brute-forcing.
	_ = ip
}

func (l *LoginLockout) bump(m map[string]*failBucket, key string, now time.Time, limit int, window time.Duration) {
	if key == "" {
		return
	}
	b := m[key]
	if b == nil || now.After(b.reset) {
		b = &failBucket{reset: now.Add(window)}
		m[key] = b
	}
	b.fails++
	if b.fails >= limit {
		b.locked = now.Add(window)
	}
}
