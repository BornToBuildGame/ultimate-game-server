package party

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestParty_CreateJoinLeavePromote(t *testing.T) {
	reg := NewRegistryWithConfig(Config{
		Node: "node1", SingleParty: true, DefaultMaxSize: 4, AbsoluteMaxSize: 256, IdleCheckMs: 0,
	})
	defer reg.StopIdleSweep()

	leaderID := uuid.New().String()
	userA := uuid.New().String()
	userB := uuid.New().String()

	p, err := reg.Create(leaderID, "leader", "sess_lead", true, false, 4, `{"mode":"ranked"}`)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if !strings.HasSuffix(p.PartyID, ".node1") {
		t.Fatalf("expected {uuid}.node1 id, got %s", p.PartyID)
	}
	if p.LeaderID != leaderID || len(p.Members) != 1 {
		t.Fatalf("unexpected party state: %+v", p)
	}

	out, err := reg.Join(p.PartyID, userA, "user_a", "sess_a")
	if err != nil || !out.Joined {
		t.Fatalf("open join: err=%v joined=%v", err, out != nil && out.Joined)
	}
	if len(out.Party.Members) != 2 {
		t.Fatalf("expected 2 members, got %d", len(out.Party.Members))
	}

	// Full party at max_size=2
	p2, err := reg.Create(uuid.New().String(), "l2", "sess_l2", true, false, 2, "{}")
	if err != nil {
		t.Fatalf("create p2: %v", err)
	}
	userX := uuid.New().String()
	userY := uuid.New().String()
	if _, err := reg.Join(p2.PartyID, userX, "x", "sx"); err != nil {
		t.Fatalf("join x: %v", err)
	}
	if _, err := reg.Join(p2.PartyID, userY, "y", "sy"); err != ErrPartyFull {
		t.Fatalf("expected ErrPartyFull, got %v", err)
	}

	// Fresh users for promote/dissolve path
	leaderID = uuid.New().String()
	userA = uuid.New().String()
	userB = uuid.New().String()
	p3, err := reg.Create(leaderID, "leader", "sess_lead", true, false, 4, "{}")
	if err != nil {
		t.Fatalf("create p3: %v", err)
	}
	if _, err := reg.Join(p3.PartyID, userA, "user_a", "sess_a"); err != nil {
		t.Fatalf("join a: %v", err)
	}
	if _, err := reg.Join(p3.PartyID, userB, "user_b", "sess_b"); err != nil {
		t.Fatalf("join b: %v", err)
	}

	leave, err := reg.Leave(p3.PartyID, leaderID)
	if err != nil {
		t.Fatalf("leave: %v", err)
	}
	if leave.PromotedLeader == nil || leave.PromotedLeader.UserID != userA {
		t.Fatalf("expected promote to userA, got %+v", leave.PromotedLeader)
	}

	leave, err = reg.Leave(p3.PartyID, userA)
	if err != nil {
		t.Fatalf("leave a: %v", err)
	}
	leave, err = reg.Leave(p3.PartyID, userB)
	if err != nil {
		t.Fatalf("leave b: %v", err)
	}
	if !leave.Dissolved {
		t.Fatalf("expected dissolve")
	}
}

func TestParty_ClosedJoinRequestAcceptReject(t *testing.T) {
	reg := NewRegistryWithConfig(Config{Node: "n", SingleParty: true, AbsoluteMaxSize: 256, IdleCheckMs: 0})
	defer reg.StopIdleSweep()

	leaderID := uuid.New().String()
	guestID := uuid.New().String()
	p, err := reg.Create(leaderID, "lead", "s1", false, false, 4, "{}")
	if err != nil {
		t.Fatal(err)
	}

	out, err := reg.Join(p.PartyID, guestID, "guest", "s2")
	if err != nil {
		t.Fatal(err)
	}
	if out.Joined {
		t.Fatal("closed join should queue request")
	}
	if len(out.Party.JoinRequests) != 1 {
		t.Fatalf("expected 1 join request, got %d", len(out.Party.JoinRequests))
	}

	list, err := reg.JoinRequestList(p.PartyID, "s1")
	if err != nil || len(list) != 1 {
		t.Fatalf("join request list: %v len=%d", err, len(list))
	}

	_, member, err := reg.Accept(p.PartyID, "s1", Presence{UserID: guestID, Username: "guest", SessionID: "s2"})
	if err != nil {
		t.Fatal(err)
	}
	if member.UserID != guestID {
		t.Fatalf("accepted wrong member")
	}
	got, _ := reg.GetParty(p.PartyID)
	if len(got.Members) != 2 || len(got.JoinRequests) != 0 {
		t.Fatalf("after accept members=%d requests=%d", len(got.Members), len(got.JoinRequests))
	}

	guest2 := uuid.New().String()
	_, _ = reg.Join(p.PartyID, guest2, "g2", "s3")
	rem, err := reg.Remove(p.PartyID, "s1", Presence{UserID: guest2, Username: "g2", SessionID: "s3"})
	if err != nil || !rem.RejectedRequest {
		t.Fatalf("reject request: err=%v rejected=%v", err, rem != nil && rem.RejectedRequest)
	}
}

func TestParty_PromoteKickCloseUpdateList(t *testing.T) {
	reg := NewRegistryWithConfig(Config{Node: "n", SingleParty: false, AbsoluteMaxSize: 256, IdleCheckMs: 0})
	defer reg.StopIdleSweep()

	lead := uuid.New().String()
	a := uuid.New().String()
	p, _ := reg.Create(lead, "l", "sl", true, false, 4, `{"tier":1}`)
	_, _ = reg.Join(p.PartyID, a, "a", "sa")

	_, newLead, err := reg.Promote(p.PartyID, "sl", Presence{UserID: a, Username: "a", SessionID: "sa"})
	if err != nil || newLead.UserID != a {
		t.Fatalf("promote: %v %+v", err, newLead)
	}

	// a is leader now; kick original lead
	rem, err := reg.Remove(p.PartyID, "sa", Presence{UserID: lead, Username: "l", SessionID: "sl"})
	if err != nil || rem.Kicked == nil {
		t.Fatalf("kick: %v %+v", err, rem)
	}

	upd, err := reg.Update(p.PartyID, "sa", `{"tier":2}`, false, false)
	if err != nil || upd.Open || upd.Label != `{"tier":2}` {
		t.Fatalf("update: %v %+v", err, upd)
	}

	openTrue := true
	list, _, err := reg.List(10, &openTrue, false, "tier", "")
	if err != nil {
		t.Fatal(err)
	}
	// party is now closed (open=false), so open=true filter should exclude
	for _, e := range list {
		if e.ID == p.PartyID {
			t.Fatal("closed party should not appear in open=true list")
		}
	}
	openFalse := false
	list, _, err = reg.List(10, &openFalse, false, "tier", "")
	if err != nil || len(list) == 0 {
		t.Fatalf("expected closed party in list: %v len=%d", err, len(list))
	}

	_, err = reg.Close(p.PartyID, "sa")
	if err != nil {
		t.Fatal(err)
	}
	_, err = reg.GetParty(p.PartyID)
	if err != ErrPartyNotFound {
		t.Fatalf("expected not found after close, got %v", err)
	}
}

func TestParty_LeaveSessionAndMMHook(t *testing.T) {
	reg := NewRegistryWithConfig(Config{Node: "n", SingleParty: true, AbsoluteMaxSize: 256, IdleCheckMs: 0})
	defer reg.StopIdleSweep()

	var cancelled []string
	reg.SetMembershipChangeHook(func(partyID string) {
		cancelled = append(cancelled, partyID)
	})

	lead := uuid.New().String()
	a := uuid.New().String()
	p, _ := reg.Create(lead, "l", "sl", true, false, 4, "{}")
	_, _ = reg.Join(p.PartyID, a, "a", "sa")
	if len(cancelled) == 0 {
		t.Fatal("expected MM cancel on join")
	}

	out, err := reg.LeaveSession("sa")
	if err != nil || !out.WasMember {
		t.Fatalf("leave session: %v %+v", err, out)
	}
	got, _ := reg.GetParty(p.PartyID)
	if _, ok := got.Members[a]; ok {
		t.Fatal("member should be gone after LeaveSession")
	}
}

func TestParty_MaxSizeBoundsAndHidden(t *testing.T) {
	reg := NewRegistryWithConfig(Config{Node: "n", AbsoluteMaxSize: 256, IdleCheckMs: 0})
	defer reg.StopIdleSweep()

	lead := uuid.New().String()
	_, err := reg.Create(lead, "l", "s", true, false, 0, "{}")
	if err != nil {
		t.Fatalf("default max size create: %v", err)
	}
	_, err = reg.Create(uuid.New().String(), "l", "s2", true, false, 257, "{}")
	if err != ErrInvalidMaxSize {
		t.Fatalf("expected invalid max size, got %v", err)
	}
	_, err = reg.Create(uuid.New().String(), "l", "s3", true, true, 4, `{"x":1}`)
	if err != ErrHiddenNonEmptyLabel {
		t.Fatalf("expected hidden label error, got %v", err)
	}
	p, err := reg.Create(uuid.New().String(), "l", "s4", true, true, 4, "{}")
	if err != nil {
		t.Fatal(err)
	}
	list, _, _ := reg.List(10, nil, false, "", "")
	for _, e := range list {
		if e.ID == p.PartyID {
			t.Fatal("hidden party should be excluded from client list")
		}
	}
}

func TestParty_SweepIdle(t *testing.T) {
	reg := NewRegistryWithConfig(Config{Node: "n", IdleCheckMs: 0})
	defer reg.StopIdleSweep()
	p, _ := reg.Create(uuid.New().String(), "l", "s", true, false, 4, "{}")
	reg.mu.Lock()
	reg.parties[p.PartyID].Members = map[string]*PartyMember{}
	reg.mu.Unlock()
	reg.SweepIdle()
	time.Sleep(10 * time.Millisecond)
	if _, err := reg.GetParty(p.PartyID); err != ErrPartyNotFound {
		t.Fatalf("empty party should be swept, err=%v", err)
	}
}
