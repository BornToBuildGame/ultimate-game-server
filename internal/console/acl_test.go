package console

import (
	"testing"
)

func TestHasACL(t *testing.T) {
	admin := &ConsoleClaims{ACL: map[string]interface{}{"admin": true}}
	if !hasACL(admin, "write_players") {
		t.Fatal("admin should satisfy any flag")
	}
	writer := &ConsoleClaims{ACL: map[string]interface{}{"write_players": true}}
	if !hasACL(writer, "write_players") {
		t.Fatal("write_players should pass")
	}
	if hasACL(writer, "admin") {
		t.Fatal("write_players should not imply admin")
	}
	reader := &ConsoleClaims{ACL: map[string]interface{}{"read_players": true}}
	if hasACL(reader, "write_players") {
		t.Fatal("read_players should not write")
	}
	if hasACL(nil, "admin") {
		t.Fatal("nil claims should deny")
	}
	empty := &ConsoleClaims{ACL: map[string]interface{}{}}
	if hasACL(empty, "admin") {
		t.Fatal("empty acl should deny")
	}
}

func TestParseACL(t *testing.T) {
	acl := parseACL([]byte(`{"admin":false,"write_players":true}`))
	if acl["admin"] == true {
		t.Fatal("admin should be false")
	}
	if v, ok := acl["write_players"].(bool); !ok || !v {
		t.Fatal("write_players expected true")
	}
	empty := parseACL(nil)
	if len(empty) != 0 {
		t.Fatalf("expected empty map, got %v", empty)
	}
}
