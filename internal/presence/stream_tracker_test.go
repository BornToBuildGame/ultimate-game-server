package presence

import "testing"

func TestLocalTracker_PartyMode(t *testing.T) {
	st := NewLocalTracker()
	key := PartyStream("uuid.node")
	if !st.TrackMembership("s1", key) {
		t.Fatal("expected new track")
	}
	if st.TrackMembership("s1", key) {
		t.Fatal("duplicate track should be false")
	}
	st.TrackMembership("s2", key)
	if st.Count(key) != 2 {
		t.Fatalf("count=%d", st.Count(key))
	}
	_, _ = st.Untrack("s1", key)
	if st.Count(key) != 1 {
		t.Fatalf("after untrack count=%d", st.Count(key))
	}
	st.UntrackByMode("s2", StreamModeParty)
	if st.Count(key) != 0 {
		t.Fatalf("expected empty after UntrackByMode")
	}
	if StreamModeParty != 7 {
		t.Fatalf("expected party mode 7, got %d", StreamModeParty)
	}
}

func TestLocalTracker_StatusMeta(t *testing.T) {
	st := NewLocalTracker()
	key := StatusStream("user-a")
	if !st.Track("sess-a", key, "user-a", PresenceMeta{Username: "alice", Status: "hello"}) {
		t.Fatal("expected new track")
	}
	list := st.ListByStream(key)
	if len(list) != 1 || list[0].Meta.Status != "hello" {
		t.Fatalf("list=%+v", list)
	}
	if !st.Update("sess-a", key, "user-a", PresenceMeta{Username: "alice", Status: "world"}) {
		t.Fatal("expected update ok")
	}
	p, ok := st.GetPresence("sess-a", key)
	if !ok || p.Meta.Status != "world" {
		t.Fatalf("got=%+v ok=%v", p, ok)
	}
	removed := st.UntrackAll("sess-a")
	if len(removed) != 1 || removed[0].UserID != "user-a" {
		t.Fatalf("removed=%+v", removed)
	}
	if st.Count(key) != 0 {
		t.Fatal("expected empty stream")
	}
}
