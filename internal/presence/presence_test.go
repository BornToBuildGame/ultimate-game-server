package presence

import "testing"

func TestOnlineIndex_FriendIntersection(t *testing.T) {
	oi := NewOnlineIndex()
	userID1 := "user-111"
	userID2 := "user-222"
	userID3 := "user-333"

	oi.SetOnline(userID1)
	oi.SetOnline(userID2)

	friends := []string{userID1, userID2, userID3}
	online := oi.GetOnlineFriends(friends)
	if len(online) != 2 {
		t.Fatalf("expected 2 online friends, got %d: %v", len(online), online)
	}

	oi.ClearOnline(userID1)
	online2 := oi.GetOnlineFriends(friends)
	if len(online2) != 1 || online2[0] != userID2 {
		t.Fatalf("expected only user 2 online, got: %v", online2)
	}
}

func TestOnlineIndex_MultiSession(t *testing.T) {
	oi := NewOnlineIndex()
	uid := "user-ms"
	oi.SetOnline(uid)
	oi.SetOnline(uid)
	if !oi.IsOnline(uid) {
		t.Fatal("expected online")
	}
	oi.ClearOnline(uid)
	if !oi.IsOnline(uid) {
		t.Fatal("expected still online with one session")
	}
	oi.ClearOnline(uid)
	if oi.IsOnline(uid) {
		t.Fatal("expected offline")
	}
}
