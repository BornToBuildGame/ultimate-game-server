package matchmaker

import (
	"context"
	"sync"
	"time"
)

// Ticket represents a player's entry in the matchmaking queue.
type Ticket struct {
	ID          string
	UserID      string
	Username    string
	SkillRating int
	Region      string
	CreatedAt   time.Time
}

// MatchResult represents a successful pairing of two players.
type MatchResult struct {
	MatchID   string
	PlayerIDs []string
	Usernames []string
}

// Matchmaker processes queued tickets asynchronously using an interval loop.
type Matchmaker struct {
	mu         sync.Mutex
	tickets    map[string]*Ticket
	onMatched  func(result MatchResult)
	tickTicker *time.Ticker
	stopChan   chan struct{}
}

// NewMatchmaker creates a new Matchmaker instance.
func NewMatchmaker(onMatched func(result MatchResult)) *Matchmaker {
	return &Matchmaker{
		tickets:  make(map[string]*Ticket),
		onMatched: onMatched,
		stopChan: make(chan struct{}),
	}
}

// Submit adds a ticket to the matchmaking queue.
func (mm *Matchmaker) Submit(t *Ticket) {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	mm.tickets[t.ID] = t
}

// Cancel removes a ticket from the matchmaking queue.
func (mm *Matchmaker) Cancel(ticketID string) {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	delete(mm.tickets, ticketID)
}

// Start runs the matchmaking tick loop.
func (mm *Matchmaker) Start(ctx context.Context, interval time.Duration) {
	mm.tickTicker = time.NewTicker(interval)
	go func() {
		for {
			select {
			case <-mm.tickTicker.C:
				mm.Tick()
			case <-mm.stopChan:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
}

// Stop halts the matchmaking loop.
func (mm *Matchmaker) Stop() {
	if mm.tickTicker != nil {
		mm.tickTicker.Stop()
	}
	close(mm.stopChan)
}

// Tick evaluates candidate tickets, pairing them based on region and skill rating MMR.
func (mm *Matchmaker) Tick() {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	// Collect active tickets
	var activeTickets []*Ticket
	for _, t := range mm.tickets {
		activeTickets = append(activeTickets, t)
	}

	// Sort tickets by wait time (longest waiting first) to prevent starvation
	// In Go, we can write a simple custom sort or simple nested loops to evaluate matching.
	now := time.Now()

	// Keep track of paired tickets to remove them at the end of the tick
	matchedTicketIDs := make(map[string]bool)

	for i := 0; i < len(activeTickets); i++ {
		t1 := activeTickets[i]
		if matchedTicketIDs[t1.ID] {
			continue
		}

		// Find a match for t1
		for j := i + 1; j < len(activeTickets); j++ {
			t2 := activeTickets[j]
			if matchedTicketIDs[t2.ID] {
				continue
			}

			// 1. Must be in the same region
			if t1.Region != t2.Region {
				continue
			}

			// 2. Skill MMR delta check with wait time window expansion
			wait1 := now.Sub(t1.CreatedAt).Seconds()
			wait2 := now.Sub(t2.CreatedAt).Seconds()

			// Allowed delta expands: 50 MMR base + 10 MMR per second of wait time
			allowedDelta1 := 50 + int(wait1*10)
			allowedDelta2 := 50 + int(wait2*10)

			// Max allowed delta is the larger of the two allowed deltas
			maxAllowedDelta := allowedDelta1
			if allowedDelta2 > maxAllowedDelta {
				maxAllowedDelta = allowedDelta2
			}

			// Actual MMR delta
			actualDelta := t1.SkillRating - t2.SkillRating
			if actualDelta < 0 {
				actualDelta = -actualDelta
			}

			if actualDelta <= maxAllowedDelta {
				// Match found!
				matchedTicketIDs[t1.ID] = true
				matchedTicketIDs[t2.ID] = true

				matchID := "match_" + t1.ID + "_" + t2.ID
				result := MatchResult{
					MatchID:   matchID,
					PlayerIDs: []string{t1.UserID, t2.UserID},
					Usernames: []string{t1.Username, t2.Username},
				}

				// Trigger callback
				if mm.onMatched != nil {
					go mm.onMatched(result)
				}
				break
			}
		}
	}

	// Remove matched tickets from queue
	for id := range matchedTicketIDs {
		delete(mm.tickets, id)
	}
}
