package social

import (
	"testing"
)

func TestEdgeCursorRoundTrip(t *testing.T) {
	enc, err := encodeCursor(edgeListCursor{State: 0, Position: 12345})
	if err != nil {
		t.Fatal(err)
	}
	dec, err := decodeEdgeCursor(enc)
	if err != nil {
		t.Fatal(err)
	}
	if dec.State != 0 || dec.Position != 12345 {
		t.Fatalf("got %+v", dec)
	}
	if _, err := decodeEdgeCursor("not-valid"); err != ErrInvalidCursor {
		t.Fatalf("expected ErrInvalidCursor, got %v", err)
	}
}

func TestFoFCursorRoundTrip(t *testing.T) {
	enc, err := encodeCursor(friendsOfFriendsCursor{SourceID: "a", DestinationID: "b"})
	if err != nil {
		t.Fatal(err)
	}
	dec, err := decodeFoFCursor(enc)
	if err != nil {
		t.Fatal(err)
	}
	if dec.SourceID != "a" || dec.DestinationID != "b" {
		t.Fatalf("got %+v", dec)
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.MaxFriends != 1000 || cfg.MaxPending != 100 {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
}
