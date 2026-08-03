package socket

import (
	"context"
	"encoding/json"

	"github.com/BornToBuildGame/ultimate-game-server/internal/cluster"
	"github.com/BornToBuildGame/ultimate-game-server/internal/party"
	"github.com/BornToBuildGame/ultimate-game-server/internal/presence"
)

func (gh *GatewayHandler) trackParty(sessionID, partyID string) {
	if gh.StreamTracker != nil {
		gh.StreamTracker.TrackMembership(sessionID, presence.PartyStream(partyID))
	}
}

func (gh *GatewayHandler) untrackParty(sessionID, partyID string) {
	if gh.StreamTracker != nil {
		_, _ = gh.StreamTracker.Untrack(sessionID, presence.PartyStream(partyID))
	}
}

func (gh *GatewayHandler) handlePartyCreate(s *Session, cid string, req *PartyCreatePayload) {
	if gh.PartyRegistry == nil {
		res, _ := json.Marshal(map[string]interface{}{"cid": cid, "error": "party registry not configured"})
		s.TrySend(res)
		return
	}
	maxSize := req.MaxSize
	if maxSize == 0 {
		maxSize = 4
	}
	p, err := gh.PartyRegistry.Create(s.UserID, s.Username, s.ID, req.Open, req.Hidden, maxSize, req.Label)
	if err != nil {
		res, _ := json.Marshal(map[string]interface{}{"cid": cid, "error": err.Error()})
		s.TrySend(res)
		return
	}
	gh.trackParty(s.ID, p.PartyID)
	s.TrySend(gh.marshalPartyReply(cid, p, s))
}

func (gh *GatewayHandler) handlePartyJoin(s *Session, cid string, req *PartyJoinPayload) {
	if gh.PartyRegistry == nil {
		res, _ := json.Marshal(map[string]interface{}{"cid": cid, "error": "party registry not configured"})
		s.TrySend(res)
		return
	}
	out, err := gh.PartyRegistry.Join(req.PartyID, s.UserID, s.Username, s.ID)
	if err != nil {
		res, _ := json.Marshal(map[string]interface{}{"cid": cid, "error": err.Error()})
		s.TrySend(res)
		return
	}
	p := out.Party
	if !out.Joined {
		leader := p.Members[p.LeaderID]
		if leader != nil {
			payload, _ := json.Marshal(map[string]interface{}{
				"party_join_request": map[string]interface{}{
					"party_id": p.PartyID,
					"presences": []map[string]interface{}{{
						"user_id":    s.UserID,
						"username":   s.Username,
						"session_id": s.ID,
					}},
				},
			})
			gh.registry.SendToSession(leader.SessionID, payload)
		}
		ack, _ := json.Marshal(map[string]interface{}{
			"cid": cid,
			"party_join": map[string]interface{}{
				"party_id": p.PartyID,
				"pending":  true,
			},
		})
		s.TrySend(ack)
		return
	}

	gh.trackParty(s.ID, p.PartyID)
	s.TrySend(gh.marshalPartyReply(cid, p, s))
	join := map[string]interface{}{
		"user_id":    s.UserID,
		"username":   s.Username,
		"session_id": s.ID,
	}
	gh.broadcastPartyPresence(p.PartyID, p.Members, []map[string]interface{}{join}, nil, s.ID)
}

func (gh *GatewayHandler) handlePartyLeave(s *Session, cid string, req *PartyLeavePayload) {
	if gh.PartyRegistry == nil {
		res, _ := json.Marshal(map[string]interface{}{"cid": cid, "error": "party registry not configured"})
		s.TrySend(res)
		return
	}
	out, err := gh.PartyRegistry.Leave(req.PartyID, s.UserID)
	if err != nil {
		res, _ := json.Marshal(map[string]interface{}{"cid": cid, "error": err.Error()})
		s.TrySend(res)
		return
	}
	gh.untrackParty(s.ID, req.PartyID)
	leave := map[string]interface{}{
		"user_id":    s.UserID,
		"username":   s.Username,
		"session_id": s.ID,
	}
	if out.Party != nil {
		gh.broadcastPartyPresence(req.PartyID, out.Party.Members, nil, []map[string]interface{}{leave}, s.ID)
		if out.PromotedLeader != nil {
			gh.broadcastPartyLeader(req.PartyID, out.Party.Members, out.PromotedLeader)
		}
	}
	res, _ := json.Marshal(map[string]interface{}{
		"cid": cid,
		"party_leave": map[string]interface{}{
			"party_id": req.PartyID,
		},
	})
	s.TrySend(res)
}

func (gh *GatewayHandler) handlePartyPromote(s *Session, cid string, req *PartyPromotePayload) {
	if gh.PartyRegistry == nil {
		res, _ := json.Marshal(map[string]interface{}{"cid": cid, "error": "party registry not configured"})
		s.TrySend(res)
		return
	}
	p, leader, err := gh.PartyRegistry.Promote(req.PartyID, s.ID, party.Presence{
		UserID: req.Presence.UserID, Username: req.Presence.Username, SessionID: req.Presence.SessionID,
	})
	if err != nil {
		res, _ := json.Marshal(map[string]interface{}{"cid": cid, "error": err.Error()})
		s.TrySend(res)
		return
	}
	gh.broadcastPartyLeader(req.PartyID, p.Members, leader)
	ack, _ := json.Marshal(map[string]interface{}{"cid": cid, "party_promote": map[string]interface{}{"party_id": req.PartyID}})
	s.TrySend(ack)
}

func (gh *GatewayHandler) handlePartyAccept(s *Session, cid string, req *PartyAcceptPayload) {
	if gh.PartyRegistry == nil {
		res, _ := json.Marshal(map[string]interface{}{"cid": cid, "error": "party registry not configured"})
		s.TrySend(res)
		return
	}
	p, member, err := gh.PartyRegistry.Accept(req.PartyID, s.ID, party.Presence{
		UserID: req.Presence.UserID, Username: req.Presence.Username, SessionID: req.Presence.SessionID,
	})
	if err != nil {
		res, _ := json.Marshal(map[string]interface{}{"cid": cid, "error": err.Error()})
		s.TrySend(res)
		return
	}
	gh.trackParty(member.SessionID, req.PartyID)
	join := map[string]interface{}{
		"user_id":    member.UserID,
		"username":   member.Username,
		"session_id": member.SessionID,
	}
	gh.broadcastPartyPresence(req.PartyID, p.Members, []map[string]interface{}{join}, nil, "")
	if sess, ok := gh.registry.GetBySession(member.SessionID); ok && sess != nil {
		sess.TrySend(gh.marshalPartyReply("", p, sess))
	}
	ack, _ := json.Marshal(map[string]interface{}{"cid": cid, "party_accept": map[string]interface{}{"party_id": req.PartyID}})
	s.TrySend(ack)
}

func (gh *GatewayHandler) handlePartyRemove(s *Session, cid string, req *PartyRemovePayload) {
	if gh.PartyRegistry == nil {
		res, _ := json.Marshal(map[string]interface{}{"cid": cid, "error": "party registry not configured"})
		s.TrySend(res)
		return
	}
	out, err := gh.PartyRegistry.Remove(req.PartyID, s.ID, party.Presence{
		UserID: req.Presence.UserID, Username: req.Presence.Username, SessionID: req.Presence.SessionID,
	})
	if err != nil {
		res, _ := json.Marshal(map[string]interface{}{"cid": cid, "error": err.Error()})
		s.TrySend(res)
		return
	}
	if out.Kicked != nil {
		gh.untrackParty(out.Kicked.SessionID, req.PartyID)
		leave := map[string]interface{}{
			"user_id":    out.Kicked.UserID,
			"username":   out.Kicked.Username,
			"session_id": out.Kicked.SessionID,
		}
		if out.Party != nil {
			gh.broadcastPartyPresence(req.PartyID, out.Party.Members, nil, []map[string]interface{}{leave}, "")
		}
		closePayload, _ := json.Marshal(map[string]interface{}{
			"party_close": map[string]interface{}{"party_id": req.PartyID},
		})
		gh.registry.SendToSession(out.Kicked.SessionID, closePayload)
	}
	ack, _ := json.Marshal(map[string]interface{}{"cid": cid, "party_remove": map[string]interface{}{"party_id": req.PartyID}})
	s.TrySend(ack)
}

func (gh *GatewayHandler) handlePartyClose(s *Session, cid string, req *PartyClosePayload) {
	if gh.PartyRegistry == nil {
		res, _ := json.Marshal(map[string]interface{}{"cid": cid, "error": "party registry not configured"})
		s.TrySend(res)
		return
	}
	p, err := gh.PartyRegistry.Close(req.PartyID, s.ID)
	if err != nil {
		res, _ := json.Marshal(map[string]interface{}{"cid": cid, "error": err.Error()})
		s.TrySend(res)
		return
	}
	closePayload, _ := json.Marshal(map[string]interface{}{
		"party_close": map[string]interface{}{"party_id": req.PartyID},
	})
	for _, m := range p.Members {
		gh.untrackParty(m.SessionID, req.PartyID)
		gh.registry.SendToSession(m.SessionID, closePayload)
	}
	ack, _ := json.Marshal(map[string]interface{}{"cid": cid, "party_close": map[string]interface{}{"party_id": req.PartyID}})
	s.TrySend(ack)
}

func (gh *GatewayHandler) handlePartyUpdate(s *Session, cid string, req *PartyUpdatePayload) {
	if gh.PartyRegistry == nil {
		res, _ := json.Marshal(map[string]interface{}{"cid": cid, "error": "party registry not configured"})
		s.TrySend(res)
		return
	}
	label := req.Label
	if label == "" {
		label = "{}"
	}
	p, err := gh.PartyRegistry.Update(req.PartyID, s.ID, label, req.Open, req.Hidden)
	if err != nil {
		res, _ := json.Marshal(map[string]interface{}{"cid": cid, "error": err.Error()})
		s.TrySend(res)
		return
	}
	payload, _ := json.Marshal(map[string]interface{}{
		"party_update": map[string]interface{}{
			"party_id": p.PartyID,
			"open":     p.Open,
			"hidden":   p.Hidden,
			"label":    p.Label,
		},
	})
	for _, m := range p.Members {
		gh.registry.SendToSession(m.SessionID, payload)
	}
	ack, _ := json.Marshal(map[string]interface{}{"cid": cid, "party_update": map[string]interface{}{"party_id": req.PartyID}})
	s.TrySend(ack)
}

func (gh *GatewayHandler) handlePartyJoinRequestList(s *Session, cid string, req *PartyJoinRequestListPayload) {
	if gh.PartyRegistry == nil {
		res, _ := json.Marshal(map[string]interface{}{"cid": cid, "error": "party registry not configured"})
		s.TrySend(res)
		return
	}
	list, err := gh.PartyRegistry.JoinRequestList(req.PartyID, s.ID)
	if err != nil {
		res, _ := json.Marshal(map[string]interface{}{"cid": cid, "error": err.Error()})
		s.TrySend(res)
		return
	}
	presences := make([]map[string]interface{}, 0, len(list))
	for _, jr := range list {
		presences = append(presences, map[string]interface{}{
			"user_id":    jr.UserID,
			"username":   jr.Username,
			"session_id": jr.SessionID,
		})
	}
	res, _ := json.Marshal(map[string]interface{}{
		"cid": cid,
		"party_join_request": map[string]interface{}{
			"party_id":  req.PartyID,
			"presences": presences,
		},
	})
	s.TrySend(res)
}

func (gh *GatewayHandler) handlePartyDataSend(s *Session, cid string, req *PartyDataSendPayload) {
	if gh.PartyRegistry == nil {
		res, _ := json.Marshal(map[string]interface{}{"cid": cid, "error": "party registry not configured"})
		s.TrySend(res)
		return
	}
	p, err := gh.PartyRegistry.GetParty(req.PartyID)
	if err != nil {
		res, _ := json.Marshal(map[string]interface{}{"cid": cid, "error": err.Error()})
		s.TrySend(res)
		return
	}
	if _, ok := p.Members[s.UserID]; !ok {
		res, _ := json.Marshal(map[string]interface{}{"cid": cid, "error": "not a party member"})
		s.TrySend(res)
		return
	}
	payload, _ := json.Marshal(map[string]interface{}{
		"party_data": map[string]interface{}{
			"party_id": req.PartyID,
			"presence": map[string]interface{}{
				"user_id":    s.UserID,
				"username":   s.Username,
				"session_id": s.ID,
			},
			"op_code": req.OpCode,
			"data":    req.Data,
		},
	})
	for _, m := range p.Members {
		if m.SessionID == s.ID {
			continue
		}
		gh.registry.SendToSession(m.SessionID, payload)
	}
	ack, _ := json.Marshal(map[string]interface{}{
		"cid": cid,
		"party_data_send": map[string]interface{}{
			"party_id": req.PartyID,
		},
	})
	s.TrySend(ack)
}

func (gh *GatewayHandler) leaveAllParties(s *Session) {
	if gh.PartyRegistry == nil {
		return
	}
	partyID, _ := gh.PartyRegistry.PartyIDForSession(s.ID)
	out, err := gh.PartyRegistry.LeaveSession(s.ID)
	if gh.StreamTracker != nil {
		gh.StreamTracker.UntrackByMode(s.ID, presence.StreamModeParty)
	}
	if err != nil || out == nil || !out.WasMember {
		if partyID != "" {
			gh.untrackParty(s.ID, partyID)
		}
		return
	}
	leave := map[string]interface{}{
		"user_id":    s.UserID,
		"username":   s.Username,
		"session_id": s.ID,
	}
	if out.Party != nil {
		gh.broadcastPartyPresence(out.Party.PartyID, out.Party.Members, nil, []map[string]interface{}{leave}, s.ID)
		if out.PromotedLeader != nil {
			gh.broadcastPartyLeader(out.Party.PartyID, out.Party.Members, out.PromotedLeader)
		}
	}
}

func (gh *GatewayHandler) marshalPartyReply(cid string, p *party.PartySession, self *Session) []byte {
	presences := make([]map[string]interface{}, 0, len(p.Members))
	var leader map[string]interface{}
	for _, m := range p.Members {
		row := map[string]interface{}{
			"user_id":    m.UserID,
			"username":   m.Username,
			"session_id": m.SessionID,
		}
		presences = append(presences, row)
		if m.UserID == p.LeaderID {
			leader = row
		}
	}
	if leader == nil {
		leader = map[string]interface{}{"user_id": p.LeaderID}
	}
	res, _ := json.Marshal(map[string]interface{}{
		"cid": cid,
		"party": map[string]interface{}{
			"party_id": p.PartyID,
			"open":     p.Open,
			"hidden":   p.Hidden,
			"max_size": p.MaxSize,
			"label":    p.Label,
			"self": map[string]interface{}{
				"user_id":    self.UserID,
				"username":   self.Username,
				"session_id": self.ID,
			},
			"leader":    leader,
			"presences": presences,
		},
	})
	return res
}

func (gh *GatewayHandler) broadcastPartyPresence(
	partyID string,
	members map[string]*party.PartyMember,
	joins, leaves []map[string]interface{},
	excludeSessionID string,
) {
	if joins == nil {
		joins = []map[string]interface{}{}
	}
	if leaves == nil {
		leaves = []map[string]interface{}{}
	}
	payload, _ := json.Marshal(map[string]interface{}{
		"party_presence_event": map[string]interface{}{
			"party_id": partyID,
			"joins":    joins,
			"leaves":   leaves,
		},
	})
	for _, m := range members {
		if m.SessionID == excludeSessionID {
			continue
		}
		gh.registry.SendToSession(m.SessionID, payload)
	}
	if mesh := gh.clusterMesh(); mesh != nil {
		_ = mesh.PublishParty(context.Background(), cluster.PartyMessage{
			PartyID: partyID,
			Payload: payload,
		})
	}
}

func (gh *GatewayHandler) broadcastPartyLeader(partyID string, members map[string]*party.PartyMember, leader *party.PartyMember) {
	if leader == nil {
		return
	}
	payload, _ := json.Marshal(map[string]interface{}{
		"party_leader": map[string]interface{}{
			"party_id": partyID,
			"presence": map[string]interface{}{
				"user_id":    leader.UserID,
				"username":   leader.Username,
				"session_id": leader.SessionID,
			},
		},
	})
	for _, m := range members {
		gh.registry.SendToSession(m.SessionID, payload)
	}
	if mesh := gh.clusterMesh(); mesh != nil {
		_ = mesh.PublishParty(context.Background(), cluster.PartyMessage{
			PartyID: partyID,
			Payload: payload,
		})
	}
}

// DeliverClusterParty delivers a remote party envelope to local party members.
func (gh *GatewayHandler) DeliverClusterParty(partyID string, payload []byte) {
	if gh.PartyRegistry == nil {
		return
	}
	p, err := gh.PartyRegistry.GetParty(partyID)
	if err != nil || p == nil {
		return
	}
	for _, m := range p.Members {
		if m == nil {
			continue
		}
		gh.registry.SendToSession(m.SessionID, payload)
	}
}
