package socket

import (
	"encoding/json"

	"ultimate-game-server/internal/party"
)

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
	p, err := gh.PartyRegistry.CreateParty(s.UserID, s.Username, s.ID, req.Open, maxSize)
	if err != nil {
		res, _ := json.Marshal(map[string]interface{}{"cid": cid, "error": err.Error()})
		s.TrySend(res)
		return
	}
	s.TrySend(gh.marshalPartyReply(cid, p, s))
}

func (gh *GatewayHandler) handlePartyJoin(s *Session, cid string, req *PartyJoinPayload) {
	if gh.PartyRegistry == nil {
		res, _ := json.Marshal(map[string]interface{}{"cid": cid, "error": "party registry not configured"})
		s.TrySend(res)
		return
	}
	p, err := gh.PartyRegistry.JoinParty(req.PartyID, s.UserID, s.Username, s.ID)
	if err != nil {
		res, _ := json.Marshal(map[string]interface{}{"cid": cid, "error": err.Error()})
		s.TrySend(res)
		return
	}
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
	p, err := gh.PartyRegistry.LeaveParty(req.PartyID, s.UserID)
	if err != nil {
		res, _ := json.Marshal(map[string]interface{}{"cid": cid, "error": err.Error()})
		s.TrySend(res)
		return
	}
	leave := map[string]interface{}{
		"user_id":    s.UserID,
		"username":   s.Username,
		"session_id": s.ID,
	}
	if p != nil {
		gh.broadcastPartyPresence(req.PartyID, p.Members, nil, []map[string]interface{}{leave}, s.ID)
	}
	res, _ := json.Marshal(map[string]interface{}{
		"cid": cid,
		"party_leave": map[string]interface{}{
			"party_id": req.PartyID,
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

func (gh *GatewayHandler) marshalPartyReply(cid string, p *party.PartySession, self *Session) []byte {
	presences := make([]map[string]interface{}, 0, len(p.Members))
	var leader map[string]interface{}
	for _, m := range p.Members {
		presence := map[string]interface{}{
			"user_id":    m.UserID,
			"username":   m.Username,
			"session_id": m.SessionID,
		}
		presences = append(presences, presence)
		if m.UserID == p.LeaderID {
			leader = presence
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
			"max_size": p.MaxSize,
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
}
