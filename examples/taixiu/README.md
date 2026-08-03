# Tài-Xỉu (Sic Bo / Over-Under) Sample Game Module

This directory contains a complete, production-ready sample module for an authoritative **Tài-Xỉu (Sic Bo / Over-Under)** betting game built on top of **Ultimate Game Engine**.

---

## Game Overview & Rules

Tài-Xỉu is a popular multiplayer casino/betting game where players guess the sum of 3 six-sided dice:

- **Tài (Over)**: Total dice sum is **11 to 17**. Win payout is **1:1**.
- **Xỉu (Under)**: Total dice sum is **4 to 10**. Win payout is **1:1**.
- **Triples (3 of a Kind)**: Sum is **3** (1-1-1) or **18** (6-6-6). Special outcome (House retains standard bets).

---

## Game Round Lifecycle (State Machine)

```
[BETTING] ──(Countdown Timer)──> [ROLLING] ──(Dice Roll)──> [RESULT] ──(Payouts)──> [INTERMISSION] ──> [BETTING (Next Round)]
```

1. **BETTING Phase** (Default: 10 seconds):
   - Players join the match room and place bets (`TAI` or `XIU`) using their wallet balance.
   - Bet amounts are deducted immediately from the player's wallet via `nk.WalletUpdate`.
   - Real-time countdowns and total pool stats are broadcast to all connected clients.
2. **ROLLING Phase** (2 seconds):
   - Betting closes.
   - The authoritative server rolls 3 random dice (`1..6`).
3. **RESULT Phase** (3 seconds):
   - Dice outcome and total sum are computed.
   - Winning bets receive a 1:1 payout (credited back to player wallet via `nk.WalletUpdate`).
   - Round summary event (`OpCodeRoundResult`) is broadcast with dice values, winning side, and individual win/loss amounts.
4. **INTERMISSION Phase** (2 seconds):
   - Active pools and bets reset.
   - Round number increments and the match transitions back to `BETTING`.

---

## WebSocket / Match Data Protocol

### OpCodes

| OpCode | Name | Direction | Description |
|---|---|---|---|
| `1` | `OpCodeBet` | Client -> Server | Place a bet on `TAI` or `XIU` |
| `2` | `OpCodeStateUpdate` | Server -> Client | Periodic round state and timer broadcast |
| `3` | `OpCodeBetAck` | Server -> Client | Confirmation response for bet placement |
| `4` | `OpCodeRoundResult` | Server -> Client | Final round result, dice roll, and payout summary |

### Request & Response Formats

#### Place Bet (`OpCodeBet` - OpCode 1)
```json
{
  "choice": "TAI",
  "amount": 100
}
```

#### Bet Response (`OpCodeBetAck` - OpCode 3)
```json
{
  "success": true,
  "choice": "TAI",
  "amount": 100,
  "balance": 900
}
```

#### Round Result Broadcast (`OpCodeRoundResult` - OpCode 4)
```json
{
  "round_number": 1,
  "dice": [4, 5, 6],
  "sum": 15,
  "winning_side": "TAI",
  "payouts": [
    {
      "user_id": "user-1",
      "choice": "TAI",
      "bet_amount": 100,
      "payout": 200,
      "net_profit": 100,
      "new_balance": 1100
    }
  ]
}
```

---

## Registration & Server Integration

To register the `TaiXiuMatch` handler in your Ultimate Game Engine Go runtime module:

```go
package main

import (
    "context"
    "database/sql"
    "github.com/BornToBuildGame/ultimate-game-server/examples/taixiu"
    "github.com/BornToBuildGame/ultimate-game-server/internal/runtime"
)

func InitModule(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule, initializer runtime.Initializer) error {
    // Register authoritative match
    err := initializer.RegisterMatch("tai_xiu", func(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule) (runtime.Match, error) {
        return taixiu.NewTaiXiuMatch(), nil
    })
    if err != nil {
        return err
    }
    logger.Info("Successfully registered TaiXiuMatch module!")
    return nil
}
```

---

## Running Automated Tests

Run the sample unit tests using Go:

```bash
go test -v ./examples/taixiu/...
```
