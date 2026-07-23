package leaderboard

import (
	"time"

	"ultimate-game-server/internal/cronexpr"
)

// ParseResetSchedule parses a 5–7 field cron expression. Empty string returns nil.
func ParseResetSchedule(expr string) (*cronexpr.Expression, error) {
	if expr == "" {
		return nil, nil
	}
	return cronexpr.Parse(expr)
}

// MustParseResetSchedule parses a cron expression, returning nil on empty or error.
func MustParseResetSchedule(expr string) *cronexpr.Expression {
	s, err := ParseResetSchedule(expr)
	if err != nil {
		return nil
	}
	return s
}

// IsTournament returns true when duration > 0 (tournament-backed leaderboard).
func (lb *Leaderboard) IsTournament() bool {
	return lb.Duration > 0
}

// CalculateTournamentDeadlines mirrors the reference engine semantics.
// Returns startActiveUnix, endActiveUnix, expiryUnix.
func CalculateTournamentDeadlines(startTime, endTime, duration int64, resetSchedule *cronexpr.Expression, t time.Time) (int64, int64, int64) {
	tUnix := t.UTC().Unix()
	if resetSchedule != nil {
		var startActiveUnix int64

		if tUnix < startTime {
			startActiveUnix = resetSchedule.Next(time.Unix(startTime, 0).UTC()).UTC().Unix()
		} else {
			landsOnSched := resetSchedule.Next(t.Add(-1*time.Second)).Unix() == t.Unix()
			if landsOnSched {
				startActiveUnix = tUnix
			} else {
				startActiveUnix = resetSchedule.Last(t).UTC().Unix()
			}
		}

		endActiveUnix := startActiveUnix + duration
		expiryUnix := resetSchedule.Next(time.Unix(startActiveUnix, 0).UTC()).UTC().Unix()

		if endActiveUnix > expiryUnix {
			endActiveUnix = expiryUnix
		}

		if startTime > endActiveUnix {
			schedules := resetSchedule.NextN(time.Unix(startTime, 0).UTC(), 2)
			startActiveUnix = schedules[0].UTC().Unix()
			endActiveUnix = startActiveUnix + duration
			expiryUnix = schedules[1].UTC().Unix()
			if endActiveUnix > expiryUnix {
				endActiveUnix = expiryUnix
			}
		} else if startTime > startActiveUnix {
			startActiveUnix = startTime
		}

		if endTime > 0 && expiryUnix > endTime {
			expiryUnix = endTime
			if endActiveUnix > expiryUnix {
				endActiveUnix = expiryUnix
			}
		}

		return startActiveUnix, endActiveUnix, expiryUnix
	}

	endActiveUnix := startTime + duration
	expiryUnix := endTime
	if endTime > 0 && endActiveUnix > endTime {
		endActiveUnix = endTime
	}
	return startTime, endActiveUnix, expiryUnix
}

// CalculatePrevReset returns the previous reset unix time, or 0.
func CalculatePrevReset(currentTime time.Time, startTime int64, resetSchedule *cronexpr.Expression) int64 {
	if resetSchedule == nil {
		return 0
	}
	if time.Unix(startTime, 0).After(currentTime) {
		return 0
	}
	return resetSchedule.Last(currentTime).Unix()
}

// CalculateExpiry resolves the current expiry partition for a leaderboard.
// overrideExpiry of 0 means "use current occurrence".
// Returns (expiryUnix, recordsPossible).
func CalculateExpiry(lb *Leaderboard, overrideExpiry int64, now time.Time) (int64, bool) {
	if overrideExpiry != 0 {
		return overrideExpiry, true
	}

	resetSched := MustParseResetSchedule(lb.ResetSchedule)
	startUnix := lb.StartTime.Unix()
	endUnix := int64(0)
	if !lb.EndTime.IsZero() && lb.EndTime.Unix() > 0 {
		endUnix = lb.EndTime.Unix()
	}

	if lb.IsTournament() {
		_, _, expiryTime := CalculateTournamentDeadlines(startUnix, endUnix, int64(lb.Duration), resetSched, now)
		if expiryTime != 0 && expiryTime <= now.Unix() {
			return 0, false
		}
		return expiryTime, true
	}

	if resetSched != nil {
		return resetSched.Next(now).UTC().Unix(), true
	}

	// All-time leaderboard: epoch expiry partition.
	return 0, true
}

// ResolveExpiryTime converts expiry unix to timestamptz for DB queries.
func ResolveExpiryTime(expiryUnix int64) time.Time {
	return time.Unix(expiryUnix, 0).UTC()
}

// ActiveDeadlines returns start/end active and expiry for the given leaderboard at now.
func ActiveDeadlines(lb *Leaderboard, now time.Time) (startActive, endActive, expiry int64) {
	resetSched := MustParseResetSchedule(lb.ResetSchedule)
	startUnix := lb.StartTime.Unix()
	endUnix := int64(0)
	if !lb.EndTime.IsZero() && lb.EndTime.Unix() > 0 {
		endUnix = lb.EndTime.Unix()
	}
	if lb.IsTournament() {
		return CalculateTournamentDeadlines(startUnix, endUnix, int64(lb.Duration), resetSched, now)
	}
	if resetSched != nil {
		next := resetSched.Next(now).UTC().Unix()
		prev := CalculatePrevReset(now, startUnix, resetSched)
		return prev, next, next
	}
	return startUnix, 0, 0
}
