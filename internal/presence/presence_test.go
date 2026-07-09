package presence

import (
	"testing"
	"time"
)

func TestPresenceTracker_OnlineStatusAndBitmap(t *testing.T) {
	pt := NewPresenceTracker()

	userID1 := "user-111"
	userID2 := "user-222"
	userID3 := "user-333"

	record1 := PresenceRecord{
		UserID:    userID1,
		SessionID: "sess-111",
		Username:  "player_1",
		JoinedAt:  time.Now(),
	}

	record2 := PresenceRecord{
		UserID:    userID2,
		SessionID: "sess-222",
		Username:  "player_2",
		JoinedAt:  time.Now(),
	}

	// 1. Set presence for user 1 & 2
	pt.SetPresence(record1)
	pt.SetPresence(record2)

	// 2. Fetch online friends out of list [user1, user2, user3]
	friends := []string{userID1, userID2, userID3}
	online := pt.GetOnlineFriends(friends)

	if len(online) != 2 {
		t.Fatalf("expected 2 online friends, got %d: %v", len(online), online)
	}

	hasUser1 := false
	hasUser2 := false
	for _, u := range online {
		if u == userID1 {
			hasUser1 = true
		}
		if u == userID2 {
			hasUser2 = true
		}
	}

	if !hasUser1 || !hasUser2 {
		t.Error("expected user 1 and user 2 to be online")
	}

	// 3. Remove presence of user 1
	userID, fullyOffline, _ := pt.RemovePresence("sess-111")
	if userID != userID1 {
		t.Errorf("expected removed user ID %q, got %q", userID1, userID)
	}
	if !fullyOffline {
		t.Error("expected user 1 to be fully offline")
	}

	// Verify online friends list again
	online2 := pt.GetOnlineFriends(friends)
	if len(online2) != 1 || online2[0] != userID2 {
		t.Fatalf("expected only user 2 to be online, got: %v", online2)
	}
}

func TestPresenceTracker_Subscriptions(t *testing.T) {
	pt := NewPresenceTracker()

	userID := "user-target"
	subscriberSess := "sess-sub-999"

	// 1. Follow target
	online := pt.Follow(subscriberSess, []string{userID})
	if len(online) != 0 {
		t.Error("expected online records to be empty initially")
	}

	// 2. Target goes online: should return the subscriber session ID to be notified
	record := PresenceRecord{
		UserID:    userID,
		SessionID: "sess-target-1",
		Username:  "target",
		JoinedAt:  time.Now(),
	}

	subs := pt.SetPresence(record)
	if len(subs) != 1 || subs[0] != subscriberSess {
		t.Fatalf("expected subscriber %q to be in notify list, got: %v", subscriberSess, subs)
	}

	// 3. Target goes offline: should return subscriber session ID to be notified
	_, offline, subsOff := pt.RemovePresence("sess-target-1")
	if !offline {
		t.Error("expected target to be fully offline")
	}
	if len(subsOff) != 1 || subsOff[0] != subscriberSess {
		t.Fatalf("expected subscriber %q to be notified of offline, got: %v", subscriberSess, subsOff)
	}
}
