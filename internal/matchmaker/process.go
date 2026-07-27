package matchmaker

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"time"

	"github.com/blevesearch/bleve/v2"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

func groupsFromInterface(in [][]interface{}) [][]*Ticket {
	if in == nil {
		return nil
	}
	out := make([][]*Ticket, 0, len(in))
	for _, row := range in {
		group := make([]*Ticket, 0, len(row))
		for _, item := range row {
			switch v := item.(type) {
			case *Ticket:
				group = append(group, v)
			case Ticket:
				cp := v
				group = append(group, &cp)
			}
		}
		if len(group) > 0 {
			out = append(out, group)
		}
	}
	return out
}

// Tick evaluates candidate tickets for all configured queues.
func (mm *Matchmaker) Tick(ctx context.Context) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	queuesToProcess := []string{"default"}
	if mm.config != nil && len(mm.config.Queues) > 0 {
		queuesToProcess = make([]string, 0, len(mm.config.Queues))
		for qName := range mm.config.Queues {
			queuesToProcess = append(queuesToProcess, qName)
		}
		sort.Strings(queuesToProcess)
	}

	for _, queueName := range queuesToProcess {
		mm.processQueue(ctx, queueName)
	}
}

func (mm *Matchmaker) processQueue(ctx context.Context, queueName string) {
	queueCfg := mm.queueConfig(queueName)

	if !mm.acquireQueueLock(ctx, queueName) {
		return
	}
	defer mm.releaseQueueLock(ctx, queueName)

	activeTickets := mm.loadActiveTickets(ctx, queueName)
	if len(activeTickets) == 0 {
		return
	}

	// Oldest first for stable pairing.
	sort.SliceStable(activeTickets, func(i, j int) bool {
		return activeTickets[i].CreatedAt.Before(activeTickets[j].CreatedAt)
	})

	// Persist interval increments for Redis-backed tickets.
	for _, t := range activeTickets {
		t.Intervals++
		mm.persistTicketLocked(ctx, t)
	}

	var groups [][]*Ticket
	if mm.processorHandler != nil {
		groups = mm.processorHandler(ctx, activeTickets)
	} else if mm.hookRegistry != nil {
		if handler := mm.hookRegistry.GetMatchmakerProcessor(); handler != nil {
			in := make([]interface{}, len(activeTickets))
			for i, t := range activeTickets {
				in[i] = t
			}
			out := handler(ctx, in)
			groups = groupsFromInterface(out)
		} else {
			groups = mm.matchDefault(ctx, activeTickets, &queueCfg)
		}
	} else {
		groups = mm.matchDefault(ctx, activeTickets, &queueCfg)
	}

	if mm.overrideHandler != nil {
		groups = mm.overrideHandler(ctx, groups)
	} else if mm.hookRegistry != nil {
		if handler := mm.hookRegistry.GetMatchmakerOverride(); handler != nil {
			in := make([][]interface{}, len(groups))
			for i, g := range groups {
				row := make([]interface{}, len(g))
				for j, t := range g {
					row[j] = t
				}
				in[i] = row
			}
			groups = groupsFromInterface(handler(ctx, in))
		}
	}

	matched := make(map[string]struct{})
	for _, group := range groups {
		if len(group) == 0 {
			continue
		}
		valid := true
		for _, t := range group {
			if _, used := matched[t.ID]; used {
				valid = false
				break
			}
		}
		if !valid {
			continue
		}
		for _, t := range group {
			matched[t.ID] = struct{}{}
		}
		mm.finalizeMatch(ctx, group, queueName)
	}
}

func (mm *Matchmaker) acquireQueueLock(ctx context.Context, queueName string) bool {
	if mm.rdb == nil {
		return true
	}
	ttl := 800 * time.Millisecond
	if mm.config.UseRedlock {
		// Best-effort multi-key SETNX pattern (works on a single Redis instance too).
		acquired := 0
		for i := 1; i <= 3; i++ {
			ok, err := mm.rdb.SetNX(ctx, formatRedlockKey(queueName, i), mm.nodeID, ttl).Result()
			if err == nil && ok {
				acquired++
			}
		}
		return acquired >= 2
	}
	lockKey := "lock:matchmaker:" + queueName
	ok, err := mm.rdb.SetNX(ctx, lockKey, mm.nodeID, ttl).Result()
	return err == nil && ok
}

func formatRedlockKey(queueName string, i int) string {
	return "lock:matchmaker:" + queueName + ":" + strconv.Itoa(i)
}

func (mm *Matchmaker) releaseQueueLock(ctx context.Context, queueName string) {
	if mm.rdb == nil {
		return
	}
	if mm.config.UseRedlock {
		for i := 1; i <= 3; i++ {
			key := formatRedlockKey(queueName, i)
			val, err := mm.rdb.Get(ctx, key).Result()
			if err == nil && val == mm.nodeID {
				_ = mm.rdb.Del(ctx, key).Err()
			}
		}
		return
	}
	lockKey := "lock:matchmaker:" + queueName
	val, err := mm.rdb.Get(ctx, lockKey).Result()
	if err == nil && val == mm.nodeID {
		_ = mm.rdb.Del(ctx, lockKey).Err()
	}
}

func (mm *Matchmaker) loadActiveTickets(ctx context.Context, queueName string) []*Ticket {
	now := time.Now()
	var active []*Ticket

	if mm.rdb != nil {
		ticketIDs, err := mm.rdb.ZRangeByScore(ctx, "matchmaker:queue:"+queueName, &redis.ZRangeBy{
			Min: "-inf",
			Max: "+inf",
		}).Result()
		if err != nil || len(ticketIDs) == 0 {
			// Fall through to local map if Redis empty (tests may mix)
			for _, t := range mm.tickets {
				if t.QueueName == queueName {
					active = append(active, t)
				}
			}
			return active
		}

		pipe := mm.rdb.Pipeline()
		for _, tid := range ticketIDs {
			pipe.HGet(ctx, "matchmaker:ticket:"+tid, "payload")
		}
		cmds, err := pipe.Exec(ctx)
		if err != nil && !errors.Is(err, redis.Nil) {
			mm.logger.Error("Failed to fetch tickets pipeline", zap.Error(err))
			return nil
		}

		for _, cmd := range cmds {
			payload, err := cmd.(*redis.StringCmd).Result()
			if err != nil {
				continue
			}
			var t Ticket
			if err := json.Unmarshal([]byte(payload), &t); err != nil {
				continue
			}
			if now.Sub(t.CreatedAt).Seconds() > float64(mm.config.TicketExpirySec) {
				mm.expireTicketLocked(ctx, &t, queueName)
				continue
			}
			// Prefer local pointer if present so Interval mutations stick for in-process callers.
			if local, ok := mm.tickets[t.ID]; ok {
				active = append(active, local)
			} else {
				cp := t
				mm.tickets[t.ID] = &cp
				active = append(active, &cp)
			}
		}
		return active
	}

	for id, t := range mm.tickets {
		if t.QueueName != queueName {
			continue
		}
		if now.Sub(t.CreatedAt).Seconds() > float64(mm.config.TicketExpirySec) {
			t.Status = "expired"
			expiredPayload, _ := json.Marshal(t)
			mm.ticketStatus[t.ID] = string(expiredPayload)
			delete(mm.tickets, id)
			if set, ok := mm.sessionTickets[t.SessionID]; ok {
				delete(set, t.ID)
			}
			continue
		}
		active = append(active, t)
	}
	return active
}

func (mm *Matchmaker) expireTicketLocked(ctx context.Context, t *Ticket, queueName string) {
	t.Status = "expired"
	expiredPayload, _ := json.Marshal(t)
	if mm.rdb != nil {
		_ = mm.rdb.Del(ctx, "matchmaker:user:"+t.UserID).Err()
		_ = mm.rdb.Del(ctx, "matchmaker:ticket:"+t.ID).Err()
		_ = mm.rdb.ZRem(ctx, "matchmaker:queue:"+queueName, t.ID).Err()
		_ = mm.rdb.SRem(ctx, "matchmaker:session:"+t.SessionID, t.ID).Err()
		_ = mm.rdb.Set(ctx, "matchmaker:status:"+t.ID, string(expiredPayload), 5*time.Minute).Err()
	}
	mm.ticketStatus[t.ID] = string(expiredPayload)
	delete(mm.tickets, t.ID)
	if set, ok := mm.sessionTickets[t.SessionID]; ok {
		delete(set, t.ID)
	}
}

func (mm *Matchmaker) persistTicketLocked(ctx context.Context, t *Ticket) {
	payload, _ := json.Marshal(t)
	mm.ticketStatus[t.ID] = string(payload)
	if mm.rdb != nil {
		_ = mm.rdb.HSet(ctx, "matchmaker:ticket:"+t.ID, "payload", string(payload)).Err()
		_ = mm.rdb.Set(ctx, "matchmaker:status:"+t.ID, string(payload), 5*time.Minute).Err()
	}
}

func (mm *Matchmaker) matchDefault(ctx context.Context, tickets []*Ticket, queueCfg *QueueConfig) [][]*Ticket {
	if len(tickets) < 2 && partyCount(tickets) < queueCfg.MinPlayers {
		// Still allow single party ticket if its Count already satisfies min.
		if len(tickets) == 1 && tickets[0].Count >= tickets[0].MinCount && tickets[0].Count <= tickets[0].MaxCount &&
			tickets[0].Count%tickets[0].CountMultiple == 0 &&
			(tickets[0].Intervals >= mm.config.MaxIntervals || tickets[0].MinCount == tickets[0].MaxCount) {
			return [][]*Ticket{tickets}
		}
		if len(tickets) < 2 {
			return nil
		}
	}

	index, err := mm.buildSharedIndex(tickets)
	if err != nil {
		mm.logger.Error("failed to build matchmaker index", zap.Error(err))
		return nil
	}
	defer func() { _ = index.Close() }()

	byID := make(map[string]*Ticket, len(tickets))
	for _, t := range tickets {
		byID[t.ID] = t
	}

	now := time.Now()
	maxIntervals := mm.config.MaxIntervals
	revEnabled := mm.config.RevPrecision || queueCfg.ReversePrecision.Enabled
	revThreshold := queueCfg.ReversePrecision.ReverseThresholdTicks
	if revThreshold <= 0 {
		revThreshold = 10
	}

	matched := make(map[string]struct{})
	var groups [][]*Ticket

	for _, seed := range tickets {
		if _, used := matched[seed.ID]; used {
			continue
		}

		lastInterval := seed.Intervals >= maxIntervals || seed.MinCount == seed.MaxCount
		hitIDs := mm.searchIndex(index, seed, byID, matched)
		candidates := make([]*Ticket, 0, len(hitIDs)+1)
		candidates = append(candidates, seed)
		partySeen := map[string]struct{}{}
		if seed.PartyID != "" {
			partySeen[seed.PartyID] = struct{}{}
		}

		for _, hid := range hitIDs {
			other := byID[hid]
			if other == nil {
				continue
			}
			if _, used := matched[other.ID]; used {
				continue
			}
			if other.PartyID != "" {
				if _, seen := partySeen[other.PartyID]; seen {
					continue
				}
			}
			if !mm.compatibleRange(seed, other) {
				continue
			}
			if !mm.evaluatePairing(seed, other, queueCfg, now, revEnabled, revThreshold, index, byID) {
				continue
			}

			// Softfill: while not last interval, prefer waiting for MaxCount.
			tentative := append([]*Ticket{}, candidates...)
			tentative = append(tentative, other)
			size := groupSize(tentative)
			if !lastInterval && size > seed.MaxCount {
				continue
			}
			if size > seed.MaxCount {
				continue
			}

			candidates = tentative
			if other.PartyID != "" {
				partySeen[other.PartyID] = struct{}{}
			}
			if groupSize(candidates) >= seed.MaxCount {
				break
			}
		}

		size := groupSize(candidates)
		if rem := size % seed.CountMultiple; rem != 0 {
			candidates = trimToMultiple(candidates, seed.CountMultiple, seed.MinCount)
			size = groupSize(candidates)
		}

		canMatch := size == seed.MaxCount || (lastInterval && size >= seed.MinCount && size <= seed.MaxCount)
		if !canMatch {
			continue
		}
		if !groupSatisfiesAll(candidates) {
			continue
		}

		for _, t := range candidates {
			matched[t.ID] = struct{}{}
		}
		groups = append(groups, candidates)
	}

	return groups
}

func partyCount(tickets []*Ticket) int {
	n := 0
	for _, t := range tickets {
		n += t.Count
		if t.Count <= 0 {
			n++
		}
	}
	return n
}

func groupSize(tickets []*Ticket) int {
	n := 0
	for _, t := range tickets {
		c := t.Count
		if c <= 0 {
			c = 1
		}
		n += c
	}
	return n
}

func groupSatisfiesAll(tickets []*Ticket) bool {
	size := groupSize(tickets)
	parties := map[string]int{}
	for _, t := range tickets {
		if t.PartyID != "" {
			parties[t.PartyID]++
			if parties[t.PartyID] > 1 {
				return false
			}
		}
		if size < t.MinCount || size > t.MaxCount {
			return false
		}
		if t.CountMultiple > 1 && size%t.CountMultiple != 0 {
			return false
		}
	}
	return true
}

func trimToMultiple(tickets []*Ticket, multiple, minCount int) []*Ticket {
	if multiple <= 1 {
		return tickets
	}
	size := groupSize(tickets)
	rem := size % multiple
	if rem == 0 {
		return tickets
	}
	// Drop newest/smallest tickets first (prefer keeping oldest seed at [0]).
	out := append([]*Ticket{}, tickets...)
	for rem > 0 && len(out) > 1 {
		dropIdx := len(out) - 1
		for i := 1; i < len(out); i++ {
			if out[i].Count <= rem {
				dropIdx = i
				break
			}
		}
		c := out[dropIdx].Count
		if c <= 0 {
			c = 1
		}
		out = append(out[:dropIdx], out[dropIdx+1:]...)
		rem -= c
		if rem < 0 {
			break
		}
	}
	if groupSize(out) < minCount {
		return tickets[:1] // invalid; caller will reject
	}
	return out
}

func (mm *Matchmaker) compatibleRange(a, b *Ticket) bool {
	return a.MinCount <= b.MaxCount && b.MinCount <= a.MaxCount
}

func (mm *Matchmaker) buildSharedIndex(tickets []*Ticket) (bleve.Index, error) {
	mapping := bleve.NewIndexMapping()
	index, err := bleve.NewMemOnly(mapping)
	if err != nil {
		return nil, err
	}
	for _, t := range tickets {
		doc := flattenTicketDoc(t)
		if err := index.Index(t.ID, doc); err != nil {
			_ = index.Close()
			return nil, err
		}
	}
	return index, nil
}

func flattenTicketDoc(t *Ticket) map[string]interface{} {
	doc := map[string]interface{}{
		"ticket_id":  t.ID,
		"party_id":   t.PartyID,
		"min_count":  t.MinCount,
		"max_count":  t.MaxCount,
		"skill":      t.SkillRating,
		"region":     t.Region,
		"created_at": t.CreatedAt.UnixNano(),
	}
	for k, v := range t.StringProperties {
		doc["properties."+k] = v
	}
	for k, v := range t.NumericProperties {
		doc["properties."+k] = v
	}
	return doc
}

func (mm *Matchmaker) searchIndex(index bleve.Index, seed *Ticket, byID map[string]*Ticket, matched map[string]struct{}) []string {
	var req *bleve.SearchRequest
	if seed.Query == "" || seed.Query == "*" {
		req = bleve.NewSearchRequestOptions(bleve.NewMatchAllQuery(), len(byID), 0, false)
	} else {
		req = bleve.NewSearchRequestOptions(bleve.NewQueryStringQuery(seed.Query), len(byID), 0, false)
	}
	req.SortBy([]string{"-_score", "created_at"})
	res, err := index.Search(req)
	if err != nil {
		return nil
	}
	ids := make([]string, 0, len(res.Hits))
	for _, hit := range res.Hits {
		if hit.ID == seed.ID {
			continue
		}
		if _, used := matched[hit.ID]; used {
			continue
		}
		ids = append(ids, hit.ID)
	}
	return ids
}

func (mm *Matchmaker) evaluatePairing(
	t1, t2 *Ticket,
	queueCfg *QueueConfig,
	now time.Time,
	revEnabled bool,
	revThreshold int,
	index bleve.Index,
	byID map[string]*Ticket,
) bool {
	wait1 := now.Sub(t1.CreatedAt).Seconds()
	wait2 := now.Sub(t2.CreatedAt).Seconds()

	if t1.Region != t2.Region && (t1.Region != "" && t2.Region != "") {
		if queueCfg.RegionMatch.Strict {
			return false
		}
		fallbackDelay := float64(queueCfg.RegionMatch.FallbackDelaySec)
		if wait1 < fallbackDelay || wait2 < fallbackDelay {
			return false
		}
	}

	if queueCfg.SkillMatch.Enabled {
		delta1 := getSkillDelta(wait1)
		delta2 := getSkillDelta(wait2)
		maxAllowed := delta1
		if delta2 > maxAllowed {
			maxAllowed = delta2
		}
		actual := t1.SkillRating - t2.SkillRating
		if actual < 0 {
			actual = -actual
		}
		if actual > maxAllowed {
			return false
		}
	}

	// Exclude same party already handled by caller; double-check.
	if t1.PartyID != "" && t1.PartyID == t2.PartyID {
		return false
	}

	if revEnabled || t1.ReversePrecision || t2.ReversePrecision {
		useRev := revEnabled || t1.ReversePrecision || t2.ReversePrecision
		withinThreshold := t1.Intervals < revThreshold && t2.Intervals < revThreshold
		if useRev && withinThreshold {
			if !mm.queryMatches(index, t2, t1.ID) {
				return false
			}
		}
	}

	_ = byID
	return true
}

func (mm *Matchmaker) queryMatches(index bleve.Index, querier *Ticket, targetID string) bool {
	if querier.Query == "" || querier.Query == "*" {
		return true
	}
	q := bleve.NewQueryStringQuery(querier.Query)
	req := bleve.NewSearchRequestOptions(q, 1000, 0, false)
	res, err := index.Search(req)
	if err != nil {
		return false
	}
	for _, hit := range res.Hits {
		if hit.ID == targetID {
			return true
		}
	}
	return false
}

// getSkillDelta returns the progressive MMR window used by skill matching tests.
func getSkillDelta(waitSeconds float64) int {
	if waitSeconds < 5 {
		return 50
	}
	if waitSeconds < 10 {
		return 75
	}
	if waitSeconds < 20 {
		return 125
	}
	if waitSeconds < 30 {
		return 200
	}
	if waitSeconds < 60 {
		return 350
	}
	return 500
}

func (mm *Matchmaker) finalizeMatch(ctx context.Context, tickets []*Ticket, queueName string) {
	ticketIDs := make(map[string]string, groupSize(tickets))
	users := make([]*Presence, 0, groupSize(tickets))
	playerIDs := make([]string, 0, groupSize(tickets))
	usernames := make([]string, 0, groupSize(tickets))

	for _, t := range tickets {
		pres := t.Presences
		if len(pres) == 0 {
			pres = []*Presence{{UserID: t.UserID, Username: t.Username, SessionID: t.SessionID}}
		}
		for _, p := range pres {
			users = append(users, p)
			playerIDs = append(playerIDs, p.UserID)
			usernames = append(usernames, p.Username)
			ticketIDs[p.UserID] = t.ID
		}
	}

	// Remove tickets from pool first.
	if mm.rdb != nil {
		tx := mm.rdb.TxPipeline()
		for _, t := range tickets {
			tx.Del(ctx, "matchmaker:user:"+t.UserID)
			tx.Del(ctx, "matchmaker:ticket:"+t.ID)
			tx.ZRem(ctx, "matchmaker:queue:"+queueName, t.ID)
			tx.SRem(ctx, "matchmaker:session:"+t.SessionID, t.ID)
		}
		if _, err := tx.Exec(ctx); err != nil {
			mm.logger.Error("Failed to delete matched tickets in Redis", zap.Error(err))
			return
		}
	}
	for _, t := range tickets {
		delete(mm.tickets, t.ID)
		if set, ok := mm.sessionTickets[t.SessionID]; ok {
			delete(set, t.ID)
			if len(set) == 0 {
				delete(mm.sessionTickets, t.SessionID)
			}
		}
	}

	matchID := ""
	authoritative := false
	if mm.hookRegistry != nil {
		handler := mm.hookRegistry.GetMatchmakerMatched()
		if handler != nil {
			entries := make([]interface{}, len(users))
			for idx, p := range users {
				tid := ticketIDs[p.UserID]
				var strProps map[string]string
				var numProps map[string]float64
				var partyID string
				for _, t := range tickets {
					if t.ID == tid {
						strProps = t.StringProperties
						numProps = t.NumericProperties
						partyID = t.PartyID
						break
					}
				}
				entries[idx] = map[string]interface{}{
					"ticket_id":          tid,
					"user_id":            p.UserID,
					"username":           p.Username,
					"session_id":         p.SessionID,
					"party_id":           partyID,
					"string_properties":  strProps,
					"numeric_properties": numProps,
				}
			}
			var err error
			matchID, err = handler(ctx, &runtimeLogger{zapLogger: mm.logger}, mm.db, mm.nk, entries)
			if err != nil {
				mm.logger.Warn("matchmaker_matched hook returned error", zap.Error(err))
			}
			if matchID != "" {
				authoritative = true
			}
		}
	}

	if matchID == "" {
		matchID = "match_" + uuid.New().String()
	}

	matchToken := ""
	if !authoritative {
		token, err := mm.mintMatchToken(matchID, users, ticketIDs)
		if err != nil {
			mm.logger.Error("failed to mint match token", zap.Error(err))
			return
		}
		matchToken = token
		_ = mm.storeMatchTokenRedis(ctx, token, mm.matchTokens[token])
	}

	if mm.rdb != nil {
		matchKey := "matchmaker:match:" + matchID
		_ = mm.rdb.HSet(ctx, matchKey, map[string]interface{}{
			"players":       playerIDs,
			"match_token":   matchToken,
			"authoritative": authoritative,
			"node_id":       mm.nodeID,
		}).Err()
	}

	for _, t := range tickets {
		t.Status = "matched"
		t.MatchID = matchID
		t.MatchToken = matchToken
		matchedPayload, _ := json.Marshal(t)
		if mm.rdb != nil {
			_ = mm.rdb.Set(ctx, "matchmaker:status:"+t.ID, string(matchedPayload), 5*time.Minute).Err()
		}
		mm.ticketStatus[t.ID] = string(matchedPayload)
	}

	module := ""
	matchedUsers := make([]MatchedUser, 0, len(users))
	for _, p := range users {
		tid := ticketIDs[p.UserID]
		mu := MatchedUser{
			UserID:    p.UserID,
			Username:  p.Username,
			SessionID: p.SessionID,
			TicketID:  tid,
		}
		for _, t := range tickets {
			if t.ID == tid {
				mu.PartyID = t.PartyID
				mu.StringProperties = t.StringProperties
				mu.NumericProperties = t.NumericProperties
				if t.StringProperties != nil {
					if m, ok := t.StringProperties["module"]; ok && m != "" && module == "" {
						module = m
					}
				}
				break
			}
		}
		matchedUsers = append(matchedUsers, mu)
	}
	if module == "" {
		for _, t := range tickets {
			if t.StringProperties != nil {
				if m, ok := t.StringProperties["module"]; ok && m != "" {
					module = m
					break
				}
			}
		}
	}

	result := MatchResult{
		MatchID:       matchID,
		PlayerIDs:     playerIDs,
		Usernames:     usernames,
		Users:         users,
		MatchedUsers:  matchedUsers,
		TicketIDs:     ticketIDs,
		MatchToken:    matchToken,
		QueueName:     queueName,
		Authoritative: authoritative,
		Module:        module,
	}

	mm.completions = append(mm.completions, CompletionRecord{
		MatchID:     matchID,
		CompletedAt: time.Now(),
		PlayerCount: len(playerIDs),
	})
	if len(mm.completions) > 10 {
		mm.completions = mm.completions[len(mm.completions)-10:]
	}

	onMatched := mm.onMatched
	onSpawn := mm.onSpawnMatch
	if onMatched != nil {
		go onMatched(result)
	}
	if authoritative && onSpawn != nil {
		go onSpawn(result)
	}

	mm.logger.Info("Match formed",
		zap.String("match_id", matchID),
		zap.Bool("authoritative", authoritative),
		zap.Int("players_count", len(playerIDs)),
	)
}
