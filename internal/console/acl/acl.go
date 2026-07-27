package acl

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"math"
)

// Permission levels — bit positions within each resource's 3-bit slot (reference-aligned).
const (
	PermissionRead   = PermissionLevel(0)
	PermissionWrite  = PermissionLevel(1)
	PermissionDelete = PermissionLevel(2)
)

type PermissionLevel int

// Resource IDs — order must match reference AclResources enum (append-only).
const (
	ResourceAccount Resource = iota
	ResourceAccountWallet
	ResourceAccountExport
	ResourceAccountFriends
	ResourceAccountGroups
	ResourceAccountNotes
	ResourceACLTemplate
	ResourceAllAccounts
	ResourceAllData
	ResourceAllStorage
	ResourceAPIExplorer
	ResourceAuditLog
	ResourceConfiguration
	ResourceChannelMessage
	ResourceUser
	ResourceGroup
	ResourceInAppPurchase
	ResourceLeaderboard
	ResourceLeaderboardRecord
	ResourceMatch
	ResourceNotification
	ResourceSatoriMessage
	ResourceSettings
	ResourceStorageData
	ResourceStorageDataImport
	ResourceHiroInventory
	ResourceHiroProgression
	ResourceHiroEconomy
	ResourceHiroStats
	ResourceHiroEnergy
	resourceCount
)

type Resource int

var byteCount = int(math.Ceil(float64(int(resourceCount)*3) / 8.0))

// Permission is a resource×R/W/D bitmap.
type Permission struct {
	Bitmap []byte
}

func None() Permission {
	return Permission{Bitmap: make([]byte, byteCount)}
}

func Admin() Permission {
	return Permission{Bitmap: bytes.Repeat([]byte{0xFF}, byteCount)}
}

func (p Permission) IsAdmin() bool {
	if len(p.Bitmap) == 0 {
		return false
	}
	for _, b := range p.Bitmap {
		if b != 0xFF {
			return false
		}
	}
	return true
}

func (p Permission) IsNone() bool {
	for _, b := range p.Bitmap {
		if b != 0x00 {
			return false
		}
	}
	return true
}

func (p Permission) HasAccess(required Permission) bool {
	if required.IsNone() || p.IsAdmin() {
		return true
	}
	n := len(p.Bitmap)
	if len(required.Bitmap) < n {
		n = len(required.Bitmap)
	}
	for i := 0; i < n; i++ {
		if (p.Bitmap[i] & required.Bitmap[i]) != required.Bitmap[i] {
			return false
		}
	}
	return true
}

func (p Permission) Compose(other Permission) Permission {
	out := None()
	copy(out.Bitmap, p.Bitmap)
	for i := range out.Bitmap {
		if i < len(other.Bitmap) {
			out.Bitmap[i] |= other.Bitmap[i]
		}
	}
	return out
}

func (p Permission) String() string {
	if p.IsAdmin() {
		p = Admin()
	}
	return base64.RawURLEncoding.EncodeToString(p.Bitmap)
}

func Parse(s string) (Permission, error) {
	if s == "" {
		return None(), nil
	}
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		b, err = base64.URLEncoding.DecodeString(s)
		if err != nil {
			return None(), err
		}
	}
	out := None()
	copy(out.Bitmap, b)
	return out, nil
}

func NewPermission(resource Resource, level PermissionLevel) Permission {
	out := None()
	targetBitIdx := int(resource)*3 + int(level)
	bitIdx := 0
	for i := range out.Bitmap {
		for j := 0; j < 8; j++ {
			if bitIdx == targetBitIdx {
				out.Bitmap[i] |= 1 << (7 - j)
			}
			bitIdx++
		}
	}
	return out
}

// FromDBACL converts console_user.acl JSONB into a Permission.
// Supports: {"admin":true}, legacy flags write_players/read_players, or {"bitmap":"..."}.
func FromDBACL(raw []byte) Permission {
	if len(raw) == 0 {
		return None()
	}
	// Try plain string (bitmap) first if JSON string.
	var asString string
	if err := json.Unmarshal(raw, &asString); err == nil && asString != "" {
		if p, err := Parse(asString); err == nil {
			return p
		}
	}
	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		return None()
	}
	if v, ok := m["bitmap"].(string); ok && v != "" {
		if p, err := Parse(v); err == nil {
			return p
		}
	}
	if v, ok := m["admin"].(bool); ok && v {
		return Admin()
	}
	p := None()
	if v, ok := m["write_players"].(bool); ok && v {
		p = p.Compose(NewPermission(ResourceAccount, PermissionWrite))
		p = p.Compose(NewPermission(ResourceAccount, PermissionRead))
		p = p.Compose(NewPermission(ResourceAccount, PermissionDelete))
		p = p.Compose(NewPermission(ResourceAccountNotes, PermissionWrite))
		p = p.Compose(NewPermission(ResourceAccountWallet, PermissionWrite))
		p = p.Compose(NewPermission(ResourceStorageData, PermissionWrite))
		p = p.Compose(NewPermission(ResourceGroup, PermissionWrite))
		p = p.Compose(NewPermission(ResourceNotification, PermissionWrite))
		p = p.Compose(NewPermission(ResourceChannelMessage, PermissionWrite))
	}
	if v, ok := m["read_players"].(bool); ok && v {
		p = p.Compose(NewPermission(ResourceAccount, PermissionRead))
		p = p.Compose(NewPermission(ResourceAccountNotes, PermissionRead))
		p = p.Compose(NewPermission(ResourceAccountWallet, PermissionRead))
		p = p.Compose(NewPermission(ResourceGroup, PermissionRead))
		p = p.Compose(NewPermission(ResourceNotification, PermissionRead))
		p = p.Compose(NewPermission(ResourceChannelMessage, PermissionRead))
		p = p.Compose(NewPermission(ResourceMatch, PermissionRead))
		p = p.Compose(NewPermission(ResourceStorageData, PermissionRead))
		p = p.Compose(NewPermission(ResourceInAppPurchase, PermissionRead))
	}
	return p
}

// LegacyFlagAccess maps old JSON flag names onto bitmap checks.
func LegacyFlagAccess(p Permission, flag string) bool {
	if p.IsAdmin() {
		return true
	}
	switch flag {
	case "admin":
		return p.IsAdmin()
	case "write_players":
		return p.HasAccess(NewPermission(ResourceAccount, PermissionWrite))
	case "read_players":
		return p.HasAccess(NewPermission(ResourceAccount, PermissionRead))
	default:
		return false
	}
}
