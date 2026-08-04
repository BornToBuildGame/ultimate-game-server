# Ultimate Game Engine — Game Server Framework

Ultimate Game Engine (UGE) is a distributed, high-performance, authoritative multiplayer game server framework built in Go (`go 1.25`). It is designed to be fully compatible with reference-shaped multiplayer API contracts and runtime interfaces, extending them with configurable **Single-Instance** (in-memory local state) or **Multi-Instance Cluster** modes (shared PostgreSQL and Redis Pub/Sub) for horizontal scale-out.

This repository containing the UGE server engine can be used as an open-source framework by other game projects to run game logic, manage player sessions, validate in-app purchases, and execute custom server-side runtimes.

---

## Key Features

- **Modular Server Architecture**: Plug-and-play modules (`pkg/modules/*`) including Authentication, Storage Engine, Matchmaking, Economy/IAP, Social Systems, and Leaderboards.
- **Multi-Runtime Extensibility**: Write game logic in native Go (embedded or `.so` plugins), sandboxed Lua (`gopher-lua` VM), or sandboxed JavaScript (`goja` ES6 VM).
- **Authoritative Multiplayer**: Tick-rate based game loops executing inside authoritative match processes, supporting asynchronous join attempts, custom state transitions, opcode dispatching, and deferred broadcast flushes.
- **Real-Time & RPC Communications**: Dual HTTP/WebSocket (`0.0.0.0:7350`) and gRPC (`0.0.0.0:7349`) connection pipelines for low-latency client-server messaging, presences, and RPC dispatching.
- **Built-in Game Services**:
  - **Storage Engine**: Full object storage with ACL permissions, optimistic concurrency control (OCC), and Bluge indexing & querying.
  - **Authentication**: Device, Custom, Email, Google, Apple, Facebook Instant, Huawei, and Samsung login; JWT session token issuance and verification; session registry and revocation.
  - **Economy & IAP**: Multi-currency wallet operations (`nk.WalletUpdate`) and receipt validation for Apple App Store, Google Play Store, Huawei AppGallery, Facebook Instant Games, and Samsung Galaxy Store.
  - **Social Systems**: Friends, Chat channels, Parties, Groups, and User Presences.
  - **Leaderboards & Tournaments**: Ranking tables, score submissions, reset schedules, and rewards.
  - **Fleet & Satori Integration**: Local stub FleetManager and optional Satori telemetry client integration.
  - **Cron Scheduler**: Distributed background job runner with optional Redis cluster locking.
- **Flexible Deployment Modes**:
  - **Single-Instance Mode (Default)**: Zero Redis dependency, zero pub/sub overhead, pure in-memory local state execution.
  - **Multi-Instance Cluster Mode**: Redis Pub/Sub coordinates stream messages, match routing, and distributed locks across nodes.

---

## Architecture Overview

```
+-----------------------------------------------------------------------------------+
|                            Ultimate Game Server Engine                            |
|                                (pkg/engine.Server)                                |
+-----------------------------------------------------------------------------------+
|  [HTTP/WS Listener :7350]   [gRPC Listener :7349]    [Admin Console :7351]       |
+-----------------------------------------------------------------------------------+
|  Built-in Modules:                                                                |
|  - Auth Module           - Storage Module           - Matchmaker Module           |
|  - Economy Module        - Social Module            - Leaderboard Module          |
+-----------------------------------------------------------------------------------+
|  Runtime Scripting & Handlers:                                                    |
|  - Native Go Handlers    - Lua VM (Gopher-Lua)      - JavaScript VM (Goja ES6)   |
|  - Dynamic .so Plugins   - Cron Scheduler           - Bluge Storage Indexing      |
+-----------------------------------------------------------------------------------+
|  Persistence & Cluster Layer:                                                     |
|  - PostgreSQL / CockroachDB (SQL Storage & Migrations)                             |
|  - Optional Redis Pub/Sub (Multi-Instance Cluster Scaling)                        |
+-----------------------------------------------------------------------------------+
```

---

## Standalone Project Initialization Guide

To build a custom game server using UGE, create a completely separate, standalone Go project.

### 1. Initialize Your Go Module
Create a new directory for your game server and run `go mod init`:
```bash
mkdir my-game-server
cd my-game-server
go mod init my-game-server
```

### 2. Add UGE Dependency
Add UGE directly to your project using `go get`:
```bash
go get github.com/BornToBuildGame/ultimate-game-server@v1.1.0
```
*(Replace `v1.1.0` with the specific release tag or commit hash target.)*

#### Local Development Setup
If developing locally alongside the UGE source code directory, add a `replace` directive to your `go.mod`:
```go
module my-game-server

go 1.25

require github.com/BornToBuildGame/ultimate-game-server v0.0.0

replace github.com/BornToBuildGame/ultimate-game-server => ../ultimate-game-engine/ultimate-game-server
```

After updating `go.mod`, run `go mod tidy` to download dependencies:
```bash
go mod tidy
```

### 3. Create Your Main Server Entrypoint
Create a `main.go` file inside your project root using UGE as an embeddable engine:

```go
package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/BornToBuildGame/ultimate-game-server/internal/config"
	"github.com/BornToBuildGame/ultimate-game-server/pkg/engine"
	"github.com/BornToBuildGame/ultimate-game-server/pkg/modules/auth"
	"github.com/BornToBuildGame/ultimate-game-server/pkg/modules/economy"
	"github.com/BornToBuildGame/ultimate-game-server/pkg/modules/leaderboard"
	"github.com/BornToBuildGame/ultimate-game-server/pkg/modules/matchmaker"
	"github.com/BornToBuildGame/ultimate-game-server/pkg/modules/social"
	"github.com/BornToBuildGame/ultimate-game-server/pkg/modules/storage"
	"github.com/BornToBuildGame/ultimate-game-server/pkg/runtime"
)

func main() {
	// Parse CLI flags, environment variables, or config YAML file
	cfg, err := config.Parse(os.Args)
	if err != nil {
		log.Fatalf("failed to parse config: %v", err)
	}

	// 1. Initialize UGE Server Engine
	srv, err := engine.NewServer(cfg)
	if err != nil {
		log.Fatalf("failed to create server instance: %v", err)
	}

	// 2. Register modular features
	srv.Use(auth.NewModule())
	srv.Use(storage.NewModule())
	srv.Use(matchmaker.NewModule())
	srv.Use(economy.NewModule())
	srv.Use(social.NewModule())
	srv.Use(leaderboard.NewModule())

	// 3. Register native Go custom game logic
	srv.RegisterInit(func(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule, initializer runtime.Initializer) error {
		logger.Info("Standalone MyGameServer initialized successfully!")
		
		// Register custom RPC handler
		err := initializer.RegisterRpc("ping", func(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule, payload string) (string, error) {
			return `{"status":"ok"}`, nil
		})
		return err
	})

	// 4. Launch server in a background goroutine
	ctx := context.Background()
	go func() {
		if err := srv.Start(ctx); err != nil {
			log.Fatalf("server crash: %v", err)
		}
	}()

	// 5. Handle graceful shutdown
	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-shutdownChan

	teardownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Stop(teardownCtx); err != nil {
		log.Printf("error during server shutdown: %v", err)
	}
}
```

### 4. Build and Run Your Game Server
Compile and launch your custom binary:
```bash
go build -o my_game_server main.go
./my_game_server --config config.yaml
```

---

## Custom Module Development

UGE supports both embedded Go code registration and dynamically loaded `.so` shared object plugins.

### 1. InitModule Entrypoint
Custom runtime modules export an `InitModule` entrypoint signature:

```go
package main

import (
	"context"
	"database/sql"

	"github.com/BornToBuildGame/ultimate-game-server/pkg/runtime"
)

// InitModule is invoked by Ultimate Game Engine when initializing the module.
func InitModule(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule, initializer runtime.Initializer) error {
	logger.Info("Initializing custom game logic...")

	// Register RPCs, Intercept Hooks, and Authoritative Matches
	return nil
}
```

### 2. Component Registration Examples

#### Custom RPC Handlers
```go
initializer.RegisterRpc("claim_daily_reward", func(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule, payload string) (string, error) {
	userID, _ := ctx.Value(runtime.RUNTIME_CTX_USER_ID).(string)
	logger.Info("Processing reward claim for user %s", userID)
	return `{"success": true, "reward": 500}`, nil
})
```

#### Before & After Interceptor Hooks
```go
// Intercept Email Authentication request before execution
initializer.RegisterBeforeAuthenticateEmail(func(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule, in *runtime.AuthenticateEmailRequest) (*runtime.AuthenticateEmailRequest, error) {
	if in.Email == "" {
		return nil, runtime.ErrBadRequest("email cannot be empty")
	}
	return in, nil
})

// React asynchronously after storage objects are written
initializer.RegisterAfterWriteStorageObjects(func(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule, out *runtime.StorageObjectAcks, in *runtime.WriteStorageObjectsRequest) error {
	logger.Info("Successfully wrote %d storage objects", len(out.Acks))
	return nil
})
```

### 3. Dynamic `.so` Plugin Compilation
To compile plugins for runtime loading via `runtime.path`:
```bash
go build --buildmode=plugin -o data/modules/my_plugin.so main.go
```

> **Note**: Because Go's standard `plugin` package enforces binary safety, the compiled `.so` plugin and target UGE server binary must be built with the exact same Go toolchain version, OS/architecture targets (`GOOS`, `GOARCH`), and matching dependency module versions.

---

## Lua & JavaScript VM Runtime Support

In addition to Go native modules, UGE automatically loads Lua (`*.lua`) and JavaScript (`*.js`) script modules placed in the directory configured by `runtime.path` (default `data/modules`).

### Lua Runtime Scripting
Lua scripts run inside a sandboxed `gopher-lua` VM state. They have access to the global `nk` object for engine services:

```lua
-- data/modules/test_rpc.lua
local nk = require("nk")

local function echo_rpc(context, payload)
    nk.logger_info("Lua RPC invoked with payload: " .. tostring(payload))
    return nk.json_encode({ status = "success", received = payload })
end

nk.register_rpc(echo_rpc, "lua_echo")
```

### JavaScript Runtime Scripting
JavaScript scripts execute inside a sandboxed ES6 `goja` JS runtime context:

```js
// data/modules/test_rpc.js
function handleRpc(ctx, logger, nk, payload) {
    logger.info("JS RPC executed!");
    return JSON.stringify({ message: "Hello from JavaScript VM!" });
}

nk.registerRpc("js_echo", handleRpc);
```

---

## Sample Game Module (Tài-Xỉu / Sic Bo)

UGE includes a complete, production-ready sample authoritative game module under [`examples/taixiu`](file:///Users/lap11252/ultimate-game-engine/ultimate-game-server/examples/taixiu).

### Features Demonstrated
- **Authoritative Match Loop**: Tick-rate game loop (`MatchLoop`) managing a 4-phase state machine (`BETTING` -> `ROLLING` -> `RESULT` -> `INTERMISSION`).
- **Economy Integration**: Real-time bet validation and payout credits using `nk.WalletUpdate`.
- **WebSocket Messaging**: Dual-way binary/JSON opcode messages (`OpCodeBet`, `OpCodeStateUpdate`, `OpCodeBetAck`, `OpCodeRoundResult`).
- **Automated Testing**: Comprehensive unit and integration test suite (`taixiu_test.go` and `taixiu_integration_test.go`).

Run the sample tests with:
```bash
go test -v ./examples/taixiu/...
```

---

## Configuration Reference

UGE options can be provided via **YAML configuration file**, **Environment Variables**, or **CLI Flags** (supporting flat names and hierarchical names).

### Primary Configuration Options

| Option / Setting | YAML Key | Environment Variable | CLI Flag | Default Value |
|------------------|----------|----------------------|----------|---------------|
| Config File Path | — | `CONFIG` | `--config` | `""` |
| HTTP Socket Address | `socket.http_addr` | `HTTP_ADDR` | `--http_addr` or `--socket.http_addr` | `0.0.0.0:7350` |
| gRPC Socket Address | `socket.grpc_addr` | `GRPC_ADDR` | `--grpc_addr` or `--socket.grpc_addr` | `0.0.0.0:7349` |
| Console Admin Address | `console.address` | `CONSOLE_ADDR` | `--console_addr` or `--console.address` | `0.0.0.0:7351` |
| Database DSN | `database.dsn` | `DATABASE_URL` | `--dsn` or `--database.dsn` | `postgres://game_admin:game_password@localhost:5432/ultimate_game_db?sslmode=disable` |
| Database Read DSN | `database.read_dsn` | `DATABASE_READ_URL` | `--read_dsn` or `--database.read_dsn` | `""` |
| Run Auto Migrations | `database.migration` | `DATABASE_MIGRATION` | `--database_migration` | `true` |
| Database Max Open Conns | `database.max_open_conns` | `DATABASE_MAX_OPEN_CONNS` | `--database_max_open_conns` | `20` |
| Database Max Idle Conns | `database.max_idle_conns` | `DATABASE_MAX_IDLE_CONNS` | `--database_max_idle_conns` | `5` |
| Session Signing Key | `session.encryption_key` | `JWT_SECRET` | `--jwt_secret` or `--session.encryption_key` | `super_secret_signing_key_at_least_32_bytes_long_1234567` |
| Modules Runtime Path | `runtime.path` | `UGE_RUNTIME_PATH` | `--runtime_path` or `--runtime.path` | `data/modules` |
| RPC HTTP Auth Key | `runtime.http_key` | `UGE_RPC_HTTP_KEY` | `--rpc_http_key` or `--runtime.http_key` | `""` |
| Enable Multi-Instance Mode | `multi_instance.enabled` | `MULTI_INSTANCE` | `--multi_instance` or `--multi_instance.enabled` | `false` |
| Redis Server Address | `multi_instance.redis_addr` | `REDIS_ADDR` or `REDIS_URL` | `--redis_addr` or `--multi_instance.redis_addr` | `localhost:6379` |

### Sample YAML Configuration (`config.yaml`)

```yaml
name: ultimate-game-server

database:
  dsn: "postgres://game_admin:game_password@localhost:5432/ultimate_game_db?sslmode=disable"
  migration: true
  max_open_conns: 25
  max_idle_conns: 10

socket:
  http_addr: "0.0.0.0:7350"
  grpc_addr: "0.0.0.0:7349"

console:
  address: "0.0.0.0:7351"

session:
  encryption_key: "my_custom_secure_jwt_secret_key_change_me_in_prod"

runtime:
  path: "data/modules"

multi_instance:
  enabled: false
  redis_addr: "localhost:6379"
```

---

## Docker & Local Infrastructure

Use Docker Compose to quickly spin up PostgreSQL 16 and Redis 7 dependencies for local development.

### `docker-compose.yml`
```yaml
version: '3.8'

services:
  postgres:
    image: postgres:16-alpine
    container_name: ultimate-game-postgres
    ports:
      - "5432:5432"
    environment:
      POSTGRES_DB: ultimate_game_db
      POSTGRES_USER: game_admin
      POSTGRES_PASSWORD: game_password
    volumes:
      - pgdata:/var/lib/postgresql/data

  redis:
    image: redis:7-alpine
    container_name: ultimate-game-redis
    ports:
      - "6379:6379"
    volumes:
      - redisdata:/data

volumes:
  pgdata:
  redisdata:
```

Launch infrastructure services:
```bash
docker-compose up -d
```

---

## Building and Testing

### 1. Run Unit Tests
Run all test packages in the repository:
```bash
go test ./pkg/... ./internal/... ./examples/taixiu/...
```

### 2. Build the Server Binary
Build the primary server entrypoint executable:
```bash
go build -o ultimate-game-server ./cmd/server
```

### 3. Run the Server
Launch the server binary with automatic database migration enabled:
```bash
./ultimate-game-server --config config.yaml --database_migration true
```
