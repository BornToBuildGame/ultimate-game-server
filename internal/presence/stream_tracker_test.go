package presence

import "testing"

func TestStreamTracker_PartyMode(t *testing.T) {
	st := NewStreamTracker()
	key := PartyStream("uuid.node")
	if !st.Track("s1", key) {
		t.Fatal("expected new track")
	}
	if st.Track("s1", key) {
		t.Fatal("duplicate track should be false")
	}
	st.Track("s2", key)
	if st.Count(key) != 2 {
		t.Fatalf("count=%d", st.Count(key))
	}
	st.Untrack("s1", key)
	if st.Count(key) != 1 {
		t.Fatalf("after untrack count=%d", st.Count(key))
	}
	st.UntrackByMode("s2", StreamModeParty)
	if st.Count(key) != 0 {
		t.Fatalf("expected empty after UntrackByMode")
	}
}
