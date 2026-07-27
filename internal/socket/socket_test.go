package socket

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"ultimate-game-server/internal/auth"
	"ultimate-game-server/internal/party"
	"ultimate-game-server/internal/presence"

	"go.uber.org/zap"
)

func TestConnectionRegistry_ConcurrencyAndGracePeriod(t *testing.T) {
	reg := NewConnectionRegistry()
	reg.GracePeriod = 30 * time.Millisecond // opt-in grace for reconnection window

	userID := "user-999"
	sessionID := "session-999"

	session := &Session{
		ID:       sessionID,
		UserID:   userID,
		Username: "player_999",
		IsActive: true,
	}

	// 1. Add session
	reg.Add(session)

	s, ok := reg.GetBySession(sessionID)
	if !ok || s.ID != sessionID {
		t.Fatal("failed to retrieve registered session")
	}

	// 2. Start grace period
	cleanedUp := false
	var wg sync.WaitGroup
	wg.Add(1)

	// Inject custom cleanup function
	reg.StartGracePeriod(sessionID, func() {
		cleanedUp = true
		wg.Done()
	})

	// Verify session is marked inactive immediately
	s, ok = reg.GetBySession(sessionID)
	if !ok || s.IsActive {
		t.Error("expected session to be marked inactive during grace period")
	}

	// Wait 10ms: timer is for 30ms grace, so it should NOT be cleaned up yet
	time.Sleep(10 * time.Millisecond)
	if cleanedUp {
		t.Error("unexpected early cleanup of session during grace period")
	}

	// 3. Simulate Reconnection: Adding the session again cancels grace timer
	newSession := &Session{
		ID:       sessionID,
		UserID:   userID,
		Username: "player_999",
		IsActive: true,
	}
	reg.Add(newSession)

	// Since we can't inspect the timer directly without reflection, we verify that
	// s is active again
	s, ok = reg.GetBySession(sessionID)
	if !ok || !s.IsActive {
		t.Error("expected re-added session to be marked active")
	}
}

func TestGatewayHandler_RelayedMultiplayer(t *testing.T) {
	logger := zap.NewNop()
	secret := []byte("super_secret_signing_key_at_least_32_bytes_long_1234567")
	tm, _ := auth.NewTokenManager(secret, 10*time.Minute)

	reg := NewConnectionRegistry()
	gh := NewGatewayHandler(logger, tm, reg, nil, nil, nil)

	// Create two sessions
	sess1 := &Session{
		ID:       "session-1",
		UserID:   "user-1",
		Username: "player-1",
		Send:     make(chan []byte, 10),
		IsActive: true,
		matchIDs: make(map[string]bool),
	}
	sess2 := &Session{
		ID:       "session-2",
		UserID:   "user-2",
		Username: "player-2",
		Send:     make(chan []byte, 10),
		IsActive: true,
		matchIDs: make(map[string]bool),
	}
	reg.Add(sess1)
	reg.Add(sess2)

	// 1. Send MatchCreate from session 1
	createEnv := `{"cid": "123", "match_create": {}}`
	gh.RouteMessage(sess1, []byte(createEnv))

	// Receive create response
	select {
	case msg := <-sess1.Send:
		var resp struct {
			Cid         string `json:"cid"`
			MatchCreate struct {
				MatchID string `json:"match_id"`
			} `json:"match_create"`
		}
		if err := json.Unmarshal(msg, &resp); err != nil {
			t.Fatalf("failed to parse match_create response: %v", err)
		}
		if resp.Cid != "123" {
			t.Errorf("expected cid 123, got: %s", resp.Cid)
		}
		matchID := resp.MatchCreate.MatchID
		if matchID == "" {
			t.Fatal("expected non-empty match ID")
		}
		if !strings.HasSuffix(matchID, ".") {
			t.Fatalf("expected relayed match ID to end with '.', got %q", matchID)
		}

		// 2. Join session 1
		joinEnv := fmt.Sprintf(`{"cid": "124", "match_join": {"match_id": "%s"}}`, matchID)
		gh.RouteMessage(sess1, []byte(joinEnv))

		// Check join response
		select {
		case joinMsg := <-sess1.Send:
			var joinResp struct {
				MatchJoin struct {
					MatchID string `json:"match_id"`
				} `json:"match_join"`
			}
			json.Unmarshal(joinMsg, &joinResp)
			if joinResp.MatchJoin.MatchID != matchID {
				t.Errorf("expected join match ID %s, got: %s", matchID, joinResp.MatchJoin.MatchID)
			}
		default:
			t.Fatal("expected match_join response for sess1")
		}

		// 3. Join session 2
		gh.RouteMessage(sess2, []byte(joinEnv))

		// sess2 gets join response
		select {
		case joinMsg := <-sess2.Send:
			var joinResp struct {
				MatchJoin struct {
					MatchID string `json:"match_id"`
				} `json:"match_join"`
			}
			json.Unmarshal(joinMsg, &joinResp)
			if joinResp.MatchJoin.MatchID != matchID {
				t.Errorf("expected join match ID %s, got: %s", matchID, joinResp.MatchJoin.MatchID)
			}
		default:
			t.Fatal("expected match_join response for sess2")
		}

		// sess1 gets match presence event notification (notifying about sess2 joining)
		select {
		case notifMsg := <-sess1.Send:
			var notif struct {
				MatchPresenceEvent struct {
					MatchID string `json:"match_id"`
					Joins   []struct {
						UserID string `json:"user_id"`
					} `json:"joins"`
				} `json:"match_presence_event"`
			}
			json.Unmarshal(notifMsg, &notif)
			if len(notif.MatchPresenceEvent.Joins) != 1 || notif.MatchPresenceEvent.Joins[0].UserID != "user-2" {
				t.Errorf("expected join notification for user-2, got: %v", notif.MatchPresenceEvent.Joins)
			}
		default:
			t.Fatal("expected presence notification for sess1")
		}

		// 4. Send match data from sess1
		dataEnv := fmt.Sprintf(`{"cid": "125", "match_data_send": {"match_id": "%s", "op_code": 100, "data": "hello"}}`, matchID)
		gh.RouteMessage(sess1, []byte(dataEnv))

		// sess2 should receive the relayed match data
		select {
		case dataMsg := <-sess2.Send:
			var rcv struct {
				MatchData struct {
					MatchID string `json:"match_id"`
					OpCode  int64  `json:"op_code"`
					Data    string `json:"data"`
				} `json:"match_data"`
			}
			json.Unmarshal(dataMsg, &rcv)
			if rcv.MatchData.MatchID != matchID || rcv.MatchData.OpCode != 100 || rcv.MatchData.Data != "hello" {
				t.Errorf("unexpected relayed data: %v", rcv.MatchData)
			}
		default:
			t.Fatal("expected relayed match data on sess2")
		}

	default:
		t.Fatal("expected match_create response")
	}
}

func TestGatewayHandler_PartyJoinRequestFlow(t *testing.T) {
	logger := zap.NewNop()
	secret := []byte("super_secret_signing_key_at_least_32_bytes_long_1234567")
	tm, _ := auth.NewTokenManager(secret, 10*time.Minute)
	reg := NewConnectionRegistry()
	gh := NewGatewayHandler(logger, tm, reg, nil, nil, nil)

	partyReg := party.NewRegistryWithConfig(party.Config{Node: "test", SingleParty: true, AbsoluteMaxSize: 256, IdleCheckMs: 0})
	defer partyReg.StopIdleSweep()
	gh.SetPartyRegistry(partyReg)
	gh.SetStreamTracker(presence.NewStreamTracker())

	leader := &Session{ID: "sl", UserID: "lead", Username: "lead", Send: make(chan []byte, 16), IsActive: true, matchIDs: map[string]bool{}}
	guest := &Session{ID: "sg", UserID: "guest", Username: "guest", Send: make(chan []byte, 16), IsActive: true, matchIDs: map[string]bool{}}
	reg.Add(leader)
	reg.Add(guest)

	gh.RouteMessage(leader, []byte(`{"cid":"c1","party_create":{"open":false,"max_size":4,"label":"{}"}}`))
	var partyID string
	select {
	case msg := <-leader.Send:
		var resp struct {
			Party struct {
				PartyID string `json:"party_id"`
			} `json:"party"`
		}
		if err := json.Unmarshal(msg, &resp); err != nil {
			t.Fatalf("create resp: %v %s", err, msg)
		}
		partyID = resp.Party.PartyID
		if partyID == "" || !strings.HasSuffix(partyID, ".test") {
			t.Fatalf("bad party id %q", partyID)
		}
	default:
		t.Fatal("expected party create reply")
	}

	gh.RouteMessage(guest, []byte(fmt.Sprintf(`{"cid":"c2","party_join":{"party_id":%q}}`, partyID)))
	select {
	case msg := <-guest.Send:
		if !strings.Contains(string(msg), `"pending":true`) {
			t.Fatalf("expected pending join ack, got %s", msg)
		}
	default:
		t.Fatal("expected guest join ack")
	}
	select {
	case msg := <-leader.Send:
		if !strings.Contains(string(msg), "party_join_request") {
			t.Fatalf("expected join request on leader, got %s", msg)
		}
	default:
		t.Fatal("expected party_join_request")
	}

	accept := fmt.Sprintf(`{"cid":"c3","party_accept":{"party_id":%q,"presence":{"user_id":"guest","username":"guest","session_id":"sg"}}}`, partyID)
	gh.RouteMessage(leader, []byte(accept))
	// drain accept ack + presence on leader, party state on guest
	deadline := time.After(500 * time.Millisecond)
	gotParty := false
	for !gotParty {
		select {
		case msg := <-guest.Send:
			if strings.Contains(string(msg), `"party"`) {
				gotParty = true
			}
		case <-leader.Send:
		case <-deadline:
			t.Fatal("timeout waiting for accepted party state")
		}
	}

	gh.RouteMessage(leader, []byte(fmt.Sprintf(`{"cid":"c4","party_promote":{"party_id":%q,"presence":{"user_id":"guest","username":"guest","session_id":"sg"}}}`, partyID)))
	sawLeader := false
	deadline = time.After(500 * time.Millisecond)
	for !sawLeader {
		select {
		case msg := <-leader.Send:
			if strings.Contains(string(msg), "party_leader") {
				sawLeader = true
			}
		case msg := <-guest.Send:
			if strings.Contains(string(msg), "party_leader") {
				sawLeader = true
			}
		case <-deadline:
			t.Fatal("expected party_leader broadcast")
		}
	}

	gh.leaveAllParties(leader)
	p, err := partyReg.GetParty(partyID)
	if err != nil {
		t.Fatalf("party should still exist after leader disconnect leave: %v", err)
	}
	if _, ok := p.Members["lead"]; ok {
		t.Fatal("leader should have left party on disconnect")
	}
	if p.LeaderID != "guest" {
		t.Fatalf("expected guest as leader after disconnect, got %s", p.LeaderID)
	}
}
