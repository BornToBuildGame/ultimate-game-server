package party

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestParty_UnitAndLogic(t *testing.T) {
	reg := NewRegistry()

	leaderID := uuid.New().String()
	userA := uuid.New().String()
	userB := uuid.New().String()

	// 1. Create Party
	p, err := reg.CreateParty(leaderID, "leader", "session_lead", true, 4)
	if err != nil {
		t.Fatalf("failed to create party: %v", err)
	}

	if p.LeaderID != leaderID {
		t.Errorf("expected leader to be %s, got %s", leaderID, p.LeaderID)
	}
	if len(p.Members) != 1 {
		t.Errorf("expected 1 member in party, got: %d", len(p.Members))
	}

	// 2. Test Invalid Max Size bounds
	_, err = reg.CreateParty(leaderID, "leader", "session_lead", true, 1)
	if err != ErrInvalidMaxSize {
		t.Errorf("expected ErrInvalidMaxSize, got: %v", err)
	}
	_, err = reg.CreateParty(leaderID, "leader", "session_lead", true, 20)
	if err != ErrInvalidMaxSize {
		t.Errorf("expected ErrInvalidMaxSize, got: %v", err)
	}

	// 3. Invite Target User
	err = reg.SendInvitation(p.PartyID, leaderID, userA)
	if err != nil {
		t.Fatalf("failed to send invitation: %v", err)
	}

	// Inviting again should be fine
	err = reg.SendInvitation(p.PartyID, leaderID, userA)
	if err != nil {
		t.Fatalf("failed to send duplicate invitation: %v", err)
	}

	// Non-leader inviting should fail
	err = reg.SendInvitation(p.PartyID, userA, userB)
	if err != ErrNotLeader {
		t.Errorf("expected ErrNotLeader, got: %v", err)
	}

	// 4. Join Party
	p, err = reg.JoinParty(p.PartyID, userA, "user_a", "session_a")
	if err != nil {
		t.Fatalf("userA failed to join party: %v", err)
	}
	if len(p.Members) != 2 {
		t.Errorf("expected 2 members, got: %d", len(p.Members))
	}

	// 5. Test Closed Party restriction
	p.Open = false
	_, err = reg.JoinParty(p.PartyID, userB, "user_b", "session_b")
	if err != ErrClosedNoInvitation {
		t.Errorf("expected ErrClosedNoInvitation, got: %v", err)
	}

	// Invite userB
	err = reg.SendInvitation(p.PartyID, leaderID, userB)
	if err != nil {
		t.Fatalf("failed to invite userB: %v", err)
	}
	p, err = reg.JoinParty(p.PartyID, userB, "user_b", "session_b")
	if err != nil {
		t.Fatalf("userB failed to join closed party after invitation: %v", err)
	}

	// 6. Test Max Size Full checks
	p.MaxSize = 3
	userC := uuid.New().String()
	err = reg.SendInvitation(p.PartyID, leaderID, userC)
	if err != nil {
		t.Fatalf("failed to invite userC: %v", err)
	}
	_, err = reg.JoinParty(p.PartyID, userC, "user_c", "session_c")
	if err != ErrPartyFull {
		t.Errorf("expected ErrPartyFull, got: %v", err)
	}

	// 7. Test Member Property Updates
	props := map[string]interface{}{
		"ready": true,
		"hero":  "warrior",
	}
	p, err = reg.UpdateMemberProperties(p.PartyID, userA, props)
	if err != nil {
		t.Fatalf("failed to update member properties: %v", err)
	}
	mProps := p.Members[userA].Properties
	if mProps["ready"] != true || mProps["hero"] != "warrior" {
		t.Errorf("properties not updated correctly")
	}

	// 8. Test Leader Promotion on Leave
	// Join order: leader first, then userA, then userB.
	// When leader leaves, userA (longest serving) should be promoted.
	p, err = reg.LeaveParty(p.PartyID, leaderID)
	if err != nil {
		t.Fatalf("leader failed to leave party: %v", err)
	}
	if p.LeaderID != userA {
		t.Errorf("expected userA to be promoted to leader, got %s", p.LeaderID)
	}

	// 9. Evicting everyone deletes the party
	p, err = reg.LeaveParty(p.PartyID, userA)
	if err != nil {
		t.Fatalf("userA failed to leave: %v", err)
	}
	p, err = reg.LeaveParty(p.PartyID, userB)
	if err != nil {
		t.Fatalf("userB failed to leave: %v", err)
	}
	if p != nil {
		t.Errorf("expected returned party to be nil after last user leaves")
	}
}

func TestParty_Sweep(t *testing.T) {
	reg := NewRegistry()
	leaderID := uuid.New().String()
	userA := uuid.New().String()

	p, _ := reg.CreateParty(leaderID, "leader", "session_lead", false, 4)
	reg.SendInvitation(p.PartyID, leaderID, userA)

	// Manually set invitation time to 10 minutes ago
	p.Invitations[userA] = time.Now().Add(-10 * time.Minute)

	reg.SweepInvitations()
	if _, ok := p.Invitations[userA]; ok {
		t.Errorf("expected invitation to be swept")
	}
}
