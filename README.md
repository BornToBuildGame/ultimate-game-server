# Ultimate Game Engine — Game Server Framework

Ultimate Game Engine (UGE) is a distributed, high-performance, authoritative multiplayer game server framework built in Go. It is designed to be fully compatible with reference-shaped multiplayer API contracts and runtime interfaces, extending them with built-in multi-node clustering (using shared PostgreSQL and Redis Pub/Sub) out-of-the-box for horizontal scale-out.

This repository containing the UGE server engine can be used as an open-source framework by other game projects to run game logic, manage player sessions, validate in-app purchases, and execute custom server-side runtimes.

---

## Key Features

- **Multi-Runtime Extensibility**: Write game logic in Go (native `.so` plugins), Lua (sandboxed VM), or JavaScript (ES6 sandboxed VM).
- **Authoritative Multiplayer**: Tick-rate based game loops executing inside authoritative match processes, supporting asynchronous join attempts and deferred broadcast flushes.
- **Real-Time Communications**: Direct WebSocket connection pipelines for low-latency messaging, stream tracking, and RPC dispatching.
- **Built-in Game Services**:
  - Storage Engine with full query indexing (via Bluge storage indexing).
  - User Authentication (Device, Custom, Email, JWT session token generation).
  - Economy and In-App Purchase (IAP) validation.
  - Social systems (Friends, Chat, Parties, Groups, and Guilds).
  - Leaderboards, Tournaments, and background Cron Scheduler.
- **Clustered Architecture**: Scalable, multi-node configuration where Redis Pub/Sub coordinates stream messages and PostgreSQL acts as the persistent datastore.

---

## Standalone Project Initialization Guide

To build a custom game server using UGE, you should create a completely separate, standalone Go project (outside UGE's source repository). Follow these steps to initialize your project:

### 1. Initialize Your Go Module
Create a new directory for your game server and run `go mod init`:
```bash
mkdir my-game-server
cd my-game-server
go mod init my-game-server
```

### 2. Add UGE Dependency
Since UGE is published as a standard Go module on GitHub, you can add it directly to your project using `go get`:
```bash
go get github.com/BornToBuildGame/ultimate-game-server@v1.1.0
```
*(Replace `v1.1.0` with the specific release version or commit hash you want to target.)*

#### For Local Development
If you are developing locally alongside the UGE source code directory, you can redirect the dependency to your local folder by adding a `replace` directive to your `go.mod`:
```go
module my-game-server

go 1.25

require github.com/BornToBuildGame/ultimate-game-server v0.0.0

replace github.com/BornToBuildGame/ultimate-game-server => ../ultimate-game-engine/ultimate-game-server
```

After configuring your dependency, run `go mod tidy` to download and link all required packages:
```bash
go mod tidy
```

### 3. Create Your Main Game Server File
Create a `main.go` file inside your project directory using UGE as an embeddable Go library:

```go
package main

import (
	"context"
	"database/sql"
	"log"

	"github.com/BornToBuildGame/ultimate-game-server/internal/config"
	"github.com/BornToBuildGame/ultimate-game-server/pkg/engine"
	"github.com/BornToBuildGame/ultimate-game-server/pkg/modules/economy"
	"github.com/BornToBuildGame/ultimate-game-server/pkg/modules/matchmaker"
	"github.com/BornToBuildGame/ultimate-game-server/pkg/runtime"
)

func main() {
	cfg, err := config.Parse(nil)
	if err != nil {
		log.Fatalf("failed to parse config: %v", err)
	}

	// 1. Initialize UGE Server Engine
	srv, err := engine.NewServer(cfg)
	if err != nil {
		log.Fatalf("failed to create server: %v", err)
	}

	// 2. Enable desired modular game features
	srv.Use(matchmaker.NewModule())
	srv.Use(economy.NewModule())

	// 3. Register native Go game logic (No .so dynamic plugin required!)
	srv.RegisterInit(func(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule, initializer runtime.Initializer) error {
		logger.Info("Standalone MyGameServer module loaded successfully!")
		// Register custom RPCs, hooks, and match handlers here
		return nil
	})

	// 4. Start static server
	if err := srv.Start(context.Background()); err != nil {
		log.Fatalf("server crash: %v", err)
	}
}
```

### 4. Build and Run Your Server
Compile and launch your game server binary as a standard Go executable (works on Linux, macOS, and Windows):
```bash
go build -o my_game_server main.go
./my_game_server
```

---

## Go Native Plugin Architecture (`.so` Loading)

UGE supports loading custom Go modules compiled as shared object (`.so`) files. The server dynamically scans the configured modules directory during startup, loads alphabetical plugins, and invokes their entrypoints.

### The InitModule Entrypoint

Every Go runtime plugin must export a single public function with this exact signature:

```go
package main

import (
	"context"
	"database/sql"
	
	"github.com/BornToBuildGame/ultimate-game-server/pkg/runtime"
)

// InitModule is the entrypoint invoked by Ultimate Game Engine when loading the plugin.
func InitModule(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule, initializer runtime.Initializer) error {
	// Register custom RPCs, hooks, and authoritative matches here
	return nil
}
```

### Module Component Registration

Inside `InitModule`, developers use the `initializer` parameter to wire their custom logic:

- **Register Custom RPCs**:
  ```go
  initializer.RegisterRpc("my_custom_rpc", func(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule, payload string) (string, error) {
      logger.Info("RPC triggered with payload: %s", payload)
      return `{"success": true}`, nil
  })
  ```
- **Register Authoritative Matches**:
  ```go
  initializer.RegisterMatch("my_game_mode", func(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule) (runtime.Match, error) {
      return myGame.NewMatch(), nil
  })
  ```
- **Register Interceptor Hooks**: Hook intercepts client requests before or after they are processed by the internal handlers:
  ```go
  // Before hook
  initializer.RegisterBeforeAuthenticateEmail(func(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule, in *runtime.AuthenticateEmailRequest) (*runtime.AuthenticateEmailRequest, error) {
      // Validate or modify authentication input
      return in, nil
  })

  // After hook
  initializer.RegisterAfterWriteStorageObjects(func(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule, out *runtime.StorageObjectAcks, in *runtime.WriteStorageObjectsRequest) error {
      // React asynchronously to storage writes
      return nil
  })
  ```

---

## How to Build and Run Plugins

### 1. Compile the Shared Object

To compile your custom module for UGE, run the Go compiler specifying the `--buildmode=plugin` flag:

```bash
go build --buildmode=plugin -o modules/my_game_plugin.so path/to/plugin/main.go
```

### 2. Strict Compilation Requirements

Because Go's standard `plugin` package enforces strict safety checks, the compiled `.so` plugin and the running UGE server binary must be compiled with:
1. The exact same Go compiler version (e.g. `go 1.25.5`).
2. The exact same compiler flags and environment targets (`GOOS`, `GOARCH`).
3. The exact same dependency graph (version matches for common packages like `google.golang.org/protobuf`, `go.uber.org/zap`, etc.).

#### Recommended Workflows:
- **Local Development**: Compile both the UGE server and the plugin in the same workspace or toolchain environment.
- **Production Build (Docker)**: Use a multi-stage Dockerfile that builds the plugin inside the same Go compiler image as the target UGE server.

---

## Configuration Reference

UGE runtime and system parameters can be configured using a YAML configuration file, environment variables, or CLI flags.

### Module Path Configuration

Instruct UGE where to look for your compiled Go plugins and Lua/JS scripts:

| Configuration Method | Option Value |
|----------------------|--------------|
| **YAML Config File** | `runtime.path: "data/modules"` |
| **CLI Flag**         | `--runtime_path "data/modules"` or `--runtime.path "data/modules"` |
| **Environment Var**  | `UGE_RUNTIME_PATH="data/modules"` |

### Running the Server

Start the UGE server binary directly, pointing it to your config file:

```bash
./ultimate-game-server --config config.yaml
```

If database migrations should automatically run on startup:

```bash
./ultimate-game-server --config config.yaml --database_migration true
```

---

## Docker Compose Quickstart

The easiest way to orchestrate the UGE server cluster locally along with PostgreSQL and Redis is using Docker Compose.

Create a `docker-compose.yml` in your project root:

```yaml
version: '3.8'

services:
  postgres:
    image: postgres:15-alpine
    environment:
      POSTGRES_DB: ultimate_game_db
      POSTGRES_USER: game_admin
      POSTGRES_PASSWORD: game_password
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"

  uge:
    image: ultimate-game-server:latest # Replace with your custom built server image
    depends_on:
      - postgres
      - redis
    ports:
      - "7350:7350" # HTTP client port
      - "7349:7349" # gRPC client port
      - "7351:7351" # Admin console port
    environment:
      - DATABASE_URL=postgres://game_admin:game_password@postgres:5432/ultimate_game_db?sslmode=disable
      - UGE_RUNTIME_PATH=/opt/uge/modules
      - REDIS_URL=redis://redis:6379
      - DATABASE_MIGRATION=true
    volumes:
      - ./modules:/opt/uge/modules # Mount directory containing .so plugins

volumes:
  pgdata:
```

---

## Detailed Custom Module Boilerplate

Below is a complete, boilerplate example of a custom module registering a before-hook, a custom RPC handler, and an authoritative multiplayer match handler.

```go
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/BornToBuildGame/ultimate-game-server/pkg/runtime"
)

// InitModule is the standard entrypoint that UGE looks for.
func InitModule(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule, initializer runtime.Initializer) error {
	logger.Info("Initializing MyCustomGame module...")

	// 1. Register a custom RPC handler
	if err := initializer.RegisterRpc("ping_server", PingHandler); err != nil {
		return fmt.Errorf("failed to register RPC: %w", err)
	}

	// 2. Register a before-hook interceptor for Email Authenticate
	if err := initializer.RegisterBeforeAuthenticateEmail(BeforeAuthEmail); err != nil {
		return fmt.Errorf("failed to register before-hook: %w", err)
	}

	// 3. Register an authoritative multiplayer match handler
	if err := initializer.RegisterMatch("lobby_match", LobbyMatchFactory); err != nil {
		return fmt.Errorf("failed to register match handler: %w", err)
	}

	logger.Info("MyCustomGame module successfully registered!")
	return nil
}

// -----------------------------------------------------------------------------
// 1. RPC Handler
// -----------------------------------------------------------------------------
func PingHandler(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule, payload string) (string, error) {
	logger.Debug("Ping RPC called with payload size: %d", len(payload))
	return `{"message": "pong"}`, nil
}

// -----------------------------------------------------------------------------
// 2. Interceptor Hook
// -----------------------------------------------------------------------------
func BeforeAuthEmail(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule, in *runtime.AuthenticateEmailRequest) (*runtime.AuthenticateEmailRequest, error) {
	// Block signups with invalid domain extensions
	if in.Email == "" {
		return nil, errors.New("email is required")
	}
	logger.Info("Authenticating user via email: %s", in.Email)
	return in, nil
}

// -----------------------------------------------------------------------------
// 3. Authoritative Match Handler
// -----------------------------------------------------------------------------

// MatchDispatcher defines the methods available on the dispatcher object passed to match handlers.
// The dispatcher parameter is passed as interface{} to remain decoupled, and should be type-asserted.
type MatchDispatcher interface {
	BroadcastMessage(opCode int64, data []byte, presences []runtime.Presence, sender runtime.Presence, reliable bool) error
	BroadcastMessageDeferred(opCode int64, data []byte, presences []runtime.Presence, sender runtime.Presence, reliable bool) error
	MatchKick(presences []runtime.Presence) error
	MatchLabelUpdate(label string) error
}

type LobbyMatch struct{}

func LobbyMatchFactory(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule) (runtime.Match, error) {
	return &LobbyMatch{}, nil
}

func (m *LobbyMatch) MatchInit(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule, params map[string]interface{}) (interface{}, int, string) {
	state := make(map[string]interface{})
	tickRate := 10 // 10 ticks per second
	label := "LobbyRoom"
	return state, tickRate, label
}

func (m *LobbyMatch) MatchJoinAttempt(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule, dispatcher interface{}, tick int64, state interface{}, presence runtime.Presence, metadata map[string]string) (interface{}, bool, string) {
	acceptJoin := true
	return state, acceptJoin, ""
}

func (m *LobbyMatch) MatchJoin(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule, dispatcher interface{}, tick int64, state interface{}, presences []runtime.Presence) interface{} {
	return state
}

func (m *LobbyMatch) MatchLeave(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule, dispatcher interface{}, tick int64, state interface{}, presences []runtime.Presence) interface{} {
	return state
}

func (m *LobbyMatch) MatchLoop(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule, dispatcher interface{}, tick int64, state interface{}, messages []runtime.MatchData) interface{} {
	// Periodic logic executed on each tick
	return state
}

func (m *LobbyMatch) MatchSignal(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule, dispatcher interface{}, tick int64, state interface{}, data string) (interface{}, string) {
	return state, "signal_received"
}

func (m *LobbyMatch) MatchTerminate(ctx context.Context, logger runtime.Logger, db *sql.DB, nk runtime.RuntimeModule, dispatcher interface{}, tick int64, state interface{}, graceSeconds int) interface{} {
	return state
}
```
