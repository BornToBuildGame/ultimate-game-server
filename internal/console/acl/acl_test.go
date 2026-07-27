package acl

import "testing"

func TestAdminHasAll(t *testing.T) {
	admin := Admin()
	req := NewPermission(ResourceGroup, PermissionDelete)
	if !admin.HasAccess(req) {
		t.Fatal("admin should have group delete")
	}
}

func TestWritePlayersLegacy(t *testing.T) {
	p := FromDBACL([]byte(`{"admin":false,"write_players":true}`))
	if !LegacyFlagAccess(p, "write_players") {
		t.Fatal("expected write_players")
	}
	if LegacyFlagAccess(p, "admin") {
		t.Fatal("should not be admin")
	}
	if !p.HasAccess(NewPermission(ResourceAccount, PermissionWrite)) {
		t.Fatal("expected account write")
	}
}

func TestBitmapRoundTrip(t *testing.T) {
	p := NewPermission(ResourceNotification, PermissionWrite).Compose(NewPermission(ResourceNotification, PermissionRead))
	s := p.String()
	got, err := Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	if !got.HasAccess(NewPermission(ResourceNotification, PermissionWrite)) {
		t.Fatal("round-trip lost write")
	}
}

func TestAdminJSON(t *testing.T) {
	p := FromDBACL([]byte(`{"admin":true}`))
	if !p.IsAdmin() {
		t.Fatal("expected admin")
	}
}
