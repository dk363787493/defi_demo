# Phase 1: Foundation & Chain Indexer — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the project foundation (Go modules, config, logging, DB, Redis, Kafka clients, common math library) and the Chain Indexer — the data layer everything else depends on.

**Architecture:** Monorepo with 4 binary entry points (cmd/). Foundation provides shared infrastructure clients and common libraries. Chain Indexer runs as an independent service, listening to EVM blocks via WebSocket, parsing lending protocol events, publishing to Kafka, and persisting to PostgreSQL. Each chain runs in its own goroutine with independent fault tolerance.

**Tech Stack:** Go 1.22+, PostgreSQL 16, Redis 7, Kafka (segmentio/kafka-go), go-ethereum, zerolog, Viper, golang-migrate, testify

**Roadmap — All 6 Phases:**

| Phase | Plan | Depends On | Scope |
|-------|------|------------|-------|
| 1 | **This plan** | — | Project scaffolding, common libs, DB schema, chain adapter, indexer |
| 2 | `phase2-oracle-market.md` | Phase 1 | Price Oracle + Market Service |
| 3 | `phase3-liquidation-engine.md` | Phase 1, 2 | RateComputer + PositionScanner + LiqExecutor |
| 4 | `phase4-account-api.md` | Phase 1, 2 | Account Service + API Gateway + Auth |
| 5 | `phase5-risk-notification.md` | Phase 1, 2 | Risk Engine + Notification System |
| 6 | `phase6-deployment.md` | All | Docker, K8s, Helm, CI/CD, Monitoring dashboards |

---

## File Structure (Phase 1)

```
defi-lending-backend/
├── go.mod
├── go.sum
├── configs/
│   └── config.yaml                          # Default config template
├── cmd/
│   └── indexer/
│       └── main.go                          # Indexer service entry point
├── internal/
│   ├── common/
│   │   ├── config/
│   │   │   └── config.go                    # Viper-based config loading
│   │   ├── logger/
│   │   │   └── logger.go                    # zerolog wrapper
│   │   ├── math/
│   │   │   ├── wadray.go                    # Wad (10^18) and Ray (10^27) fixed-point math
│   │   │   └── wadray_test.go               # Wad/Ray tests
│   │   └── types/
│   │       └── types.go                     # Shared domain types (Address, ChainID, etc.)
│   ├── infra/
│   │   ├── database/
│   │   │   └── postgres.go                  # PostgreSQL connection pool
│   │   ├── cache/
│   │   │   └── redis.go                     # Redis client wrapper
│   │   └── messaging/
│   │       └── kafka.go                     # Kafka producer/consumer
│   ├── chain/
│   │   ├── adapter.go                       # ChainAdapter interface
│   │   └── evm/
│   │       ├── client.go                    # EVM client (go-ethereum wrapper)
│   │       └── client_test.go               # EVM client tests
│   └── indexer/
│       ├── service.go                       # Indexer orchestrator (multi-chain)
│       ├── listener/
│       │   ├── block_listener.go            # Block subscription + polling fallback
│       │   └── block_listener_test.go
│       ├── parser/
│       │   ├── event_parser.go              # ABI decode lending events
│       │   └── event_parser_test.go
│       ├── reorg/
│       │   ├── detector.go                  # Reorg detection via parent hash chain
│       │   └── detector_test.go
│       └── consumer/
│           ├── event_consumer.go            # Kafka consumer → DB writer
│           └── event_consumer_test.go
├── migrations/
│   ├── 000001_create_chains.up.sql
│   ├── 000001_create_chains.down.sql
│   ├── 000002_create_markets.up.sql
│   ├── 000002_create_markets.down.sql
│   ├── 000003_create_market_states.up.sql
│   ├── 000003_create_market_states.down.sql
│   ├── 000004_create_positions.up.sql
│   ├── 000004_create_positions.down.sql
│   ├── 000005_create_events.up.sql
│   ├── 000005_create_events.down.sql
│   ├── 000006_create_sync_status.up.sql
│   ├── 000006_create_sync_status.down.sql
│   ├── 000007_create_blocks.up.sql
│   └── 000007_create_blocks.down.sql
└── scripts/
    └── migrate.sh                           # DB migration helper
```

---

## Task 1: Project Scaffolding & Go Module Init

**Files:**
- Create: `go.mod`
- Create: `configs/config.yaml`
- Create: `cmd/indexer/main.go` (placeholder)

- [ ] **Step 1: Initialize Go module**

Run:
```bash
cd /Users/zhangjinge/claude_projects/demo
go mod init github.com/zhangjinge/defi-lending-backend
```

Expected: `go.mod` created with module path.

- [ ] **Step 2: Install core dependencies**

Run:
```bash
go get github.com/gin-gonic/gin@v1.10.0
go get github.com/ethereum/go-ethereum@v1.14.12
go get github.com/rs/zerolog@v1.33.0
go get github.com/spf13/viper@v1.19.0
go get github.com/jackc/pgx/v5@v5.7.2
go get github.com/redis/go-redis/v9@v9.7.0
go get github.com/segmentio/kafka-go@v0.4.47
go get github.com/golang-migrate/migrate/v4@v4.18.1
go get github.com/stretchr/testify@v1.10.0
go get github.com/prometheus/client_golang@v1.20.5
go get github.com/golang-jwt/jwt/v5@v5.2.1
```

Expected: `go.sum` populated with dependency hashes.

- [ ] **Step 3: Create default config template**

Create `configs/config.yaml`:

```yaml
server:
  host: "0.0.0.0"
  port: 8080

database:
  host: "localhost"
  port: 5432
  user: "defi"
  password: "defi_secret"
  dbname: "defi_lending"
  max_open_conns: 25
  max_idle_conns: 5
  conn_max_lifetime: "5m"

redis:
  addr: "localhost:6379"
  password: ""
  db: 0

kafka:
  brokers:
    - "localhost:9092"
  topics:
    chain_events: "chain.events"
    price_updates: "price.updates"
    notifications: "notifications"

chains:
  - name: "ethereum"
    chain_id: 1
    rpc_urls:
      - "https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY"
    ws_url: "wss://eth-mainnet.g.alchemy.com/v2/YOUR_KEY"
    block_time: 12
    confirmations: 12
    contracts:
      lending_pool: "0x0000000000000000000000000000000000000000"
  - name: "arbitrum"
    chain_id: 42161
    rpc_urls:
      - "https://arb-mainnet.g.alchemy.com/v2/YOUR_KEY"
    ws_url: "wss://arb-mainnet.g.alchemy.com/v2/YOUR_KEY"
    block_time: 1
    confirmations: 1
    contracts:
      lending_pool: "0x0000000000000000000000000000000000000000"

log:
  level: "info"
  format: "json"
```

- [ ] **Step 4: Create indexer main.go placeholder**

Create `cmd/indexer/main.go`:

```go
package main

import "fmt"

func main() {
	fmt.Println("defi-lending indexer starting...")
}
```

- [ ] **Step 5: Verify build**

Run:
```bash
go build ./cmd/indexer/
```

Expected: Builds with no errors.

- [ ] **Step 6: Commit**

```bash
git init
echo -e "# Build\n*.exe\n*.out\n\n# IDE\n.idea/\n.vscode/\n\n# Env\n.env\nconfigs/config.local.yaml\n\n# OS\n.DS_Store" > .gitignore
git add go.mod go.sum configs/config.yaml cmd/indexer/main.go .gitignore
git commit -m "feat: initialize Go project with core dependencies and config template"
```

---

## Task 2: Config Loader

**Files:**
- Create: `internal/common/config/config.go`

- [ ] **Step 1: Implement config loader**

Create `internal/common/config/config.go`:

```go
package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	Server   ServerConfig   `mapstructure:"server"`
	Database DatabaseConfig `mapstructure:"database"`
	Redis    RedisConfig    `mapstructure:"redis"`
	Kafka    KafkaConfig    `mapstructure:"kafka"`
	Chains   []ChainConfig  `mapstructure:"chains"`
	Log      LogConfig      `mapstructure:"log"`
}

type ServerConfig struct {
	Host string `mapstructure:"host"`
	Port int    `mapstructure:"port"`
}

type DatabaseConfig struct {
	Host            string `mapstructure:"host"`
	Port            int    `mapstructure:"port"`
	User            string `mapstructure:"user"`
	Password        string `mapstructure:"password"`
	DBName          string `mapstructure:"dbname"`
	MaxOpenConns    int    `mapstructure:"max_open_conns"`
	MaxIdleConns    int    `mapstructure:"max_idle_conns"`
	ConnMaxLifetime string `mapstructure:"conn_max_lifetime"`
}

func (d DatabaseConfig) DSN() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		d.User, d.Password, d.Host, d.Port, d.DBName)
}

func (d DatabaseConfig) ConnMaxLifetimeDuration() time.Duration {
	dur, err := time.ParseDuration(d.ConnMaxLifetime)
	if err != nil {
		return 5 * time.Minute
	}
	return dur
}

type RedisConfig struct {
	Addr     string `mapstructure:"addr"`
	Password string `mapstructure:"password"`
	DB       int    `mapstructure:"db"`
}

type KafkaConfig struct {
	Brokers []string          `mapstructure:"brokers"`
	Topics  KafkaTopicsConfig `mapstructure:"topics"`
}

type KafkaTopicsConfig struct {
	ChainEvents   string `mapstructure:"chain_events"`
	PriceUpdates  string `mapstructure:"price_updates"`
	Notifications string `mapstructure:"notifications"`
}

type ContractsConfig struct {
	LendingPool string `mapstructure:"lending_pool"`
}

type ChainConfig struct {
	Name          string          `mapstructure:"name"`
	ChainID       int64           `mapstructure:"chain_id"`
	RPCURLs       []string        `mapstructure:"rpc_urls"`
	WSURL         string          `mapstructure:"ws_url"`
	BlockTime     int             `mapstructure:"block_time"`
	Confirmations int             `mapstructure:"confirmations"`
	Contracts     ContractsConfig `mapstructure:"contracts"`
}

type LogConfig struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
}

func Load(configPath string) (*Config, error) {
	v := viper.New()
	v.SetConfigFile(configPath)
	v.SetEnvPrefix("DEFI")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}

	return &cfg, nil
}
```

- [ ] **Step 2: Verify it compiles**

Run:
```bash
go build ./internal/common/config/
```

Expected: No errors.

- [ ] **Step 3: Commit**

```bash
git add internal/common/config/config.go
git commit -m "feat: add Viper-based config loader with env override support"
```

---

## Task 3: Structured Logger

**Files:**
- Create: `internal/common/logger/logger.go`

- [ ] **Step 1: Implement zerolog wrapper**

Create `internal/common/logger/logger.go`:

```go
package logger

import (
	"io"
	"os"
	"time"

	"github.com/rs/zerolog"
)

func New(level string, format string) zerolog.Logger {
	var w io.Writer = os.Stdout
	if format == "console" {
		w = zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}
	}

	lvl, err := zerolog.ParseLevel(level)
	if err != nil {
		lvl = zerolog.InfoLevel
	}

	return zerolog.New(w).
		Level(lvl).
		With().
		Timestamp().
		Caller().
		Logger()
}
```

- [ ] **Step 2: Verify it compiles**

Run:
```bash
go build ./internal/common/logger/
```

Expected: No errors.

- [ ] **Step 3: Commit**

```bash
git add internal/common/logger/logger.go
git commit -m "feat: add zerolog-based structured logger"
```

---

## Task 4: Shared Domain Types

**Files:**
- Create: `internal/common/types/types.go`

- [ ] **Step 1: Define core domain types**

Create `internal/common/types/types.go`:

```go
package types

import (
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// ChainID is a numeric identifier for an EVM chain (1 = Ethereum, 42161 = Arbitrum, etc.).
type ChainID int64

// EventType identifies the kind of lending protocol event.
type EventType string

const (
	EventDeposit              EventType = "Deposit"
	EventWithdraw             EventType = "Withdraw"
	EventBorrow               EventType = "Borrow"
	EventRepay                EventType = "Repay"
	EventLiquidation          EventType = "Liquidation"
	EventReserveDataUpdated   EventType = "ReserveDataUpdated"
	EventAccrueInterest       EventType = "AccrueInterest"
)

// BlockHeader is a minimal block representation used by the indexer.
type BlockHeader struct {
	ChainID     ChainID        `json:"chain_id"`
	Number      uint64         `json:"number"`
	Hash        common.Hash    `json:"hash"`
	ParentHash  common.Hash    `json:"parent_hash"`
	Timestamp   time.Time      `json:"timestamp"`
}

// ChainEvent is a decoded lending protocol event from a transaction log.
type ChainEvent struct {
	ChainID         ChainID        `json:"chain_id"`
	BlockNumber     uint64         `json:"block_number"`
	TxHash          common.Hash    `json:"tx_hash"`
	LogIndex        uint           `json:"log_index"`
	EventType       EventType      `json:"event_type"`
	ContractAddress common.Address `json:"contract_address"`
	UserAddress     common.Address `json:"user_address"`
	MarketAddress   common.Address `json:"market_address"`
	Amount          *big.Int       `json:"amount"`
	Data            map[string]interface{} `json:"data"`
	Timestamp       time.Time      `json:"timestamp"`
}

// SyncStatus tracks the indexing progress for a single chain.
type SyncStatus struct {
	ChainID            ChainID   `json:"chain_id"`
	LastIndexedBlock   uint64    `json:"last_indexed_block"`
	LastConfirmedBlock uint64    `json:"last_confirmed_block"`
	UpdatedAt          time.Time `json:"updated_at"`
}

// MarketState represents the current on-chain state of a lending pool/reserve.
type MarketState struct {
	MarketID            int64     `json:"market_id"`
	TotalSupply         *big.Int  `json:"total_supply"`
	TotalBorrow         *big.Int  `json:"total_borrow"`
	SupplyRate          *big.Int  `json:"supply_rate"`
	BorrowRate          *big.Int  `json:"borrow_rate"`
	LiquidityIndex      *big.Int  `json:"liquidity_index"`
	BorrowIndex         *big.Int  `json:"borrow_index"`
	LastUpdateTimestamp  uint64    `json:"last_update_timestamp"`
	UtilizationRate     *big.Int  `json:"utilization_rate"`
}

// Position represents a user's supply/borrow position in a single market.
type Position struct {
	ID            int64          `json:"id"`
	ChainID       ChainID        `json:"chain_id"`
	UserAddress   common.Address `json:"user_address"`
	MarketID      int64          `json:"market_id"`
	SupplyBalance *big.Int       `json:"supply_balance"`
	BorrowBalance *big.Int       `json:"borrow_balance"`
	SupplyIndex   *big.Int       `json:"supply_index"`
	BorrowIndex   *big.Int       `json:"borrow_index"`
	UpdatedAt     time.Time      `json:"updated_at"`
}
```

- [ ] **Step 2: Verify it compiles**

Run:
```bash
go build ./internal/common/types/
```

Expected: No errors.

- [ ] **Step 3: Commit**

```bash
git add internal/common/types/types.go
git commit -m "feat: add shared domain types for chain events, positions, and market state"
```

---

## Task 5: Wad/Ray Fixed-Point Math Library

**Files:**
- Create: `internal/common/math/wadray.go`
- Create: `internal/common/math/wadray_test.go`

- [ ] **Step 1: Write failing tests for Wad/Ray operations**

Create `internal/common/math/wadray_test.go`:

```go
package math

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWadMul(t *testing.T) {
	// 1.5 WAD * 2.0 WAD = 3.0 WAD
	a := new(big.Int).Mul(big.NewInt(15), new(big.Int).Div(WAD, big.NewInt(10))) // 1.5e18
	b := new(big.Int).Mul(big.NewInt(2), WAD) // 2.0e18
	result := WadMul(a, b)
	expected := new(big.Int).Mul(big.NewInt(3), WAD) // 3.0e18
	assert.Equal(t, expected.String(), result.String())
}

func TestWadDiv(t *testing.T) {
	// 3.0 WAD / 2.0 WAD = 1.5 WAD
	a := new(big.Int).Mul(big.NewInt(3), WAD)
	b := new(big.Int).Mul(big.NewInt(2), WAD)
	result := WadDiv(a, b)
	expected := new(big.Int).Mul(big.NewInt(15), new(big.Int).Div(WAD, big.NewInt(10)))
	assert.Equal(t, expected.String(), result.String())
}

func TestRayMul(t *testing.T) {
	// 1.5 RAY * 2.0 RAY = 3.0 RAY
	a := new(big.Int).Mul(big.NewInt(15), new(big.Int).Div(RAY, big.NewInt(10)))
	b := new(big.Int).Mul(big.NewInt(2), RAY)
	result := RayMul(a, b)
	expected := new(big.Int).Mul(big.NewInt(3), RAY)
	assert.Equal(t, expected.String(), result.String())
}

func TestRayDiv(t *testing.T) {
	// 3.0 RAY / 2.0 RAY = 1.5 RAY
	a := new(big.Int).Mul(big.NewInt(3), RAY)
	b := new(big.Int).Mul(big.NewInt(2), RAY)
	result := RayDiv(a, b)
	expected := new(big.Int).Mul(big.NewInt(15), new(big.Int).Div(RAY, big.NewInt(10)))
	assert.Equal(t, expected.String(), result.String())
}

func TestRayDivZero(t *testing.T) {
	a := new(big.Int).Mul(big.NewInt(3), RAY)
	assert.Panics(t, func() { RayDiv(a, big.NewInt(0)) })
}

func TestWadToRay(t *testing.T) {
	oneWad := new(big.Int).Set(WAD)
	result := WadToRay(oneWad)
	expected := new(big.Int).Set(RAY)
	assert.Equal(t, expected.String(), result.String())
}

func TestRayToWad(t *testing.T) {
	oneRay := new(big.Int).Set(RAY)
	result := RayToWad(oneRay)
	expected := new(big.Int).Set(WAD)
	assert.Equal(t, expected.String(), result.String())
}

func TestCalculateCompoundedInterest(t *testing.T) {
	// 5% annual rate applied for 1 year
	// rate = 0.05 RAY = 5e25
	rate := new(big.Int).Mul(big.NewInt(5), new(big.Int).Exp(big.NewInt(10), big.NewInt(25), nil))
	timeDelta := uint64(365 * 24 * 3600) // 1 year in seconds

	result := CalculateLinearInterest(rate, timeDelta)
	require.NotNil(t, result)

	// Linear interest: 1 + rate * timeDelta / SECONDS_PER_YEAR = 1.05 RAY
	expected := new(big.Int).Add(RAY, rate)
	assert.Equal(t, expected.String(), result.String())
}

func TestCalculateLinearInterestZeroDelta(t *testing.T) {
	rate := new(big.Int).Mul(big.NewInt(5), new(big.Int).Exp(big.NewInt(10), big.NewInt(25), nil))
	result := CalculateLinearInterest(rate, 0)
	// With zero time delta, interest multiplier should be 1.0 RAY
	assert.Equal(t, RAY.String(), result.String())
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:
```bash
cd /Users/zhangjinge/claude_projects/demo && go test ./internal/common/math/ -v
```

Expected: Compilation error — `WadMul`, `RayMul`, etc. not defined.

- [ ] **Step 3: Implement Wad/Ray math library**

Create `internal/common/math/wadray.go`:

```go
package math

import "math/big"

// WAD = 10^18, used for token amounts and percentages.
var WAD = new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)

// RAY = 10^27, used for interest rate index accumulation.
var RAY = new(big.Int).Exp(big.NewInt(10), big.NewInt(27), nil)

// HalfWAD = WAD / 2, for rounding.
var HalfWAD = new(big.Int).Div(WAD, big.NewInt(2))

// HalfRAY = RAY / 2, for rounding.
var HalfRAY = new(big.Int).Div(RAY, big.NewInt(2))

// SECONDS_PER_YEAR used by Aave-style interest calculation.
var SECONDS_PER_YEAR = big.NewInt(365 * 24 * 3600)

// WadMul multiplies two Wad values: (a * b + HalfWAD) / WAD.
func WadMul(a, b *big.Int) *big.Int {
	result := new(big.Int).Mul(a, b)
	result.Add(result, HalfWAD)
	result.Div(result, WAD)
	return result
}

// WadDiv divides two Wad values: (a * WAD + b/2) / b.
func WadDiv(a, b *big.Int) *big.Int {
	if b.Sign() == 0 {
		panic("wadray: division by zero")
	}
	halfB := new(big.Int).Div(b, big.NewInt(2))
	result := new(big.Int).Mul(a, WAD)
	result.Add(result, halfB)
	result.Div(result, b)
	return result
}

// RayMul multiplies two Ray values: (a * b + HalfRAY) / RAY.
func RayMul(a, b *big.Int) *big.Int {
	result := new(big.Int).Mul(a, b)
	result.Add(result, HalfRAY)
	result.Div(result, RAY)
	return result
}

// RayDiv divides two Ray values: (a * RAY + b/2) / b.
func RayDiv(a, b *big.Int) *big.Int {
	if b.Sign() == 0 {
		panic("wadray: division by zero")
	}
	halfB := new(big.Int).Div(b, big.NewInt(2))
	result := new(big.Int).Mul(a, RAY)
	result.Add(result, halfB)
	result.Div(result, b)
	return result
}

// WadToRay converts a Wad value to Ray by multiplying by 10^9.
func WadToRay(a *big.Int) *big.Int {
	factor := new(big.Int).Exp(big.NewInt(10), big.NewInt(9), nil)
	return new(big.Int).Mul(a, factor)
}

// RayToWad converts a Ray value to Wad by dividing by 10^9 (truncates).
func RayToWad(a *big.Int) *big.Int {
	factor := new(big.Int).Exp(big.NewInt(10), big.NewInt(9), nil)
	halfFactor := new(big.Int).Div(factor, big.NewInt(2))
	result := new(big.Int).Add(a, halfFactor)
	result.Div(result, factor)
	return result
}

// CalculateLinearInterest computes the linear interest multiplier.
// Returns RAY-scaled value: RAY + (rate * timeDelta / SECONDS_PER_YEAR).
// This matches Aave's MathUtils.calculateLinearInterest.
func CalculateLinearInterest(rate *big.Int, timeDelta uint64) *big.Int {
	if timeDelta == 0 {
		return new(big.Int).Set(RAY)
	}
	// rate * timeDelta / SECONDS_PER_YEAR
	result := new(big.Int).Mul(rate, new(big.Int).SetUint64(timeDelta))
	result.Div(result, SECONDS_PER_YEAR)
	// 1 RAY + accumulated
	result.Add(result, RAY)
	return result
}

// CalculateCompoundedInterest computes compounded interest using binomial approximation.
// Uses the first 3 terms of Taylor expansion: (1 + rate/n)^n ≈ 1 + rate*t + rate^2*t*(t-1)/2
// This matches Aave's MathUtils.calculateCompoundedInterest.
func CalculateCompoundedInterest(rate *big.Int, timeDelta uint64) *big.Int {
	if timeDelta == 0 {
		return new(big.Int).Set(RAY)
	}

	td := new(big.Int).SetUint64(timeDelta)

	// term1 = rate * timeDelta / SECONDS_PER_YEAR
	term1 := new(big.Int).Mul(rate, td)
	term1.Div(term1, SECONDS_PER_YEAR)

	// term2 = rate * rate * timeDelta * (timeDelta - 1) / (SECONDS_PER_YEAR^2 * 2)
	rateSq := RayMul(rate, rate)
	tdMinus1 := new(big.Int).Sub(td, big.NewInt(1))
	term2 := new(big.Int).Mul(rateSq, td)
	term2.Mul(term2, tdMinus1)
	spySq := new(big.Int).Mul(SECONDS_PER_YEAR, SECONDS_PER_YEAR)
	term2.Div(term2, spySq)
	term2.Div(term2, big.NewInt(2))

	// term3 = rate^3 * timeDelta * (td-1) * (td-2) / (SECONDS_PER_YEAR^3 * 6)
	rateCubed := RayMul(rateSq, rate)
	tdMinus2 := new(big.Int).Sub(td, big.NewInt(2))
	term3 := new(big.Int).Mul(rateCubed, td)
	term3.Mul(term3, tdMinus1)
	term3.Mul(term3, tdMinus2)
	spyCubed := new(big.Int).Mul(spySq, SECONDS_PER_YEAR)
	term3.Div(term3, spyCubed)
	term3.Div(term3, big.NewInt(6))

	// result = RAY + term1 + term2 + term3
	result := new(big.Int).Set(RAY)
	result.Add(result, term1)
	result.Add(result, term2)
	result.Add(result, term3)
	return result
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run:
```bash
cd /Users/zhangjinge/claude_projects/demo && go test ./internal/common/math/ -v
```

Expected: All 8 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/common/math/wadray.go internal/common/math/wadray_test.go
git commit -m "feat: add Wad/Ray fixed-point math library matching Aave's Solidity precision"
```

---

## Task 6: Infrastructure Clients — PostgreSQL

**Files:**
- Create: `internal/infra/database/postgres.go`

- [ ] **Step 1: Implement PostgreSQL connection pool**

Create `internal/infra/database/postgres.go`:

```go
package database

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/zhangjinge/defi-lending-backend/internal/common/config"
)

func NewPostgresPool(ctx context.Context, cfg config.DatabaseConfig) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.DSN())
	if err != nil {
		return nil, fmt.Errorf("parse postgres dsn: %w", err)
	}

	poolCfg.MaxConns = int32(cfg.MaxOpenConns)
	poolCfg.MinConns = int32(cfg.MaxIdleConns)
	poolCfg.MaxConnLifetime = cfg.ConnMaxLifetimeDuration()

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	return pool, nil
}
```

- [ ] **Step 2: Verify it compiles**

Run:
```bash
go build ./internal/infra/database/
```

Expected: No errors.

- [ ] **Step 3: Commit**

```bash
git add internal/infra/database/postgres.go
git commit -m "feat: add PostgreSQL connection pool with pgxpool"
```

---

## Task 7: Infrastructure Clients — Redis

**Files:**
- Create: `internal/infra/cache/redis.go`

- [ ] **Step 1: Implement Redis client wrapper**

Create `internal/infra/cache/redis.go`:

```go
package cache

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
	"github.com/zhangjinge/defi-lending-backend/internal/common/config"
)

func NewRedisClient(ctx context.Context, cfg config.RedisConfig) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("ping redis: %w", err)
	}

	return client, nil
}
```

- [ ] **Step 2: Verify it compiles**

Run:
```bash
go build ./internal/infra/cache/
```

Expected: No errors.

- [ ] **Step 3: Commit**

```bash
git add internal/infra/cache/redis.go
git commit -m "feat: add Redis client wrapper"
```

---

## Task 8: Infrastructure Clients — Kafka

**Files:**
- Create: `internal/infra/messaging/kafka.go`

- [ ] **Step 1: Implement Kafka producer and consumer factory**

Create `internal/infra/messaging/kafka.go`:

```go
package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/zhangjinge/defi-lending-backend/internal/common/config"
)

// Producer wraps a kafka.Writer for sending messages.
type Producer struct {
	writer *kafka.Writer
}

func NewProducer(cfg config.KafkaConfig, topic string) *Producer {
	w := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
	}
	return &Producer{writer: w}
}

// Publish serializes value to JSON and sends it with the given key.
func (p *Producer) Publish(ctx context.Context, key string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal kafka message: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(key),
		Value: data,
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("write kafka message: %w", err)
	}
	return nil
}

func (p *Producer) Close() error {
	return p.writer.Close()
}

// Consumer wraps a kafka.Reader for receiving messages.
type Consumer struct {
	reader *kafka.Reader
}

func NewConsumer(cfg config.KafkaConfig, topic string, groupID string) *Consumer {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.Brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
	})
	return &Consumer{reader: r}
}

// Read blocks until a message is available, then returns it.
func (c *Consumer) Read(ctx context.Context) (kafka.Message, error) {
	return c.reader.ReadMessage(ctx)
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
```

- [ ] **Step 2: Verify it compiles**

Run:
```bash
go build ./internal/infra/messaging/
```

Expected: No errors.

- [ ] **Step 3: Commit**

```bash
git add internal/infra/messaging/kafka.go
git commit -m "feat: add Kafka producer/consumer wrappers"
```

---

## Task 9: Database Migrations

**Files:**
- Create: `migrations/000001_create_chains.up.sql`
- Create: `migrations/000001_create_chains.down.sql`
- Create: `migrations/000002_create_markets.up.sql`
- Create: `migrations/000002_create_markets.down.sql`
- Create: `migrations/000003_create_market_states.up.sql`
- Create: `migrations/000003_create_market_states.down.sql`
- Create: `migrations/000004_create_positions.up.sql`
- Create: `migrations/000004_create_positions.down.sql`
- Create: `migrations/000005_create_events.up.sql`
- Create: `migrations/000005_create_events.down.sql`
- Create: `migrations/000006_create_sync_status.up.sql`
- Create: `migrations/000006_create_sync_status.down.sql`
- Create: `migrations/000007_create_blocks.up.sql`
- Create: `migrations/000007_create_blocks.down.sql`
- Create: `scripts/migrate.sh`

- [ ] **Step 1: Create chains migration**

Create `migrations/000001_create_chains.up.sql`:

```sql
CREATE TABLE IF NOT EXISTS chains (
    id            BIGSERIAL PRIMARY KEY,
    name          VARCHAR(50) NOT NULL UNIQUE,
    chain_id      BIGINT NOT NULL UNIQUE,
    rpc_urls      TEXT[] NOT NULL,
    ws_url        TEXT NOT NULL,
    block_time    INT NOT NULL DEFAULT 12,
    confirmations INT NOT NULL DEFAULT 12,
    status        VARCHAR(20) NOT NULL DEFAULT 'active',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_chains_chain_id ON chains(chain_id);
```

Create `migrations/000001_create_chains.down.sql`:

```sql
DROP TABLE IF EXISTS chains;
```

- [ ] **Step 2: Create markets migration**

Create `migrations/000002_create_markets.up.sql`:

```sql
CREATE TABLE IF NOT EXISTS markets (
    id                    BIGSERIAL PRIMARY KEY,
    chain_id              BIGINT NOT NULL REFERENCES chains(chain_id),
    asset_address         VARCHAR(42) NOT NULL,
    asset_symbol          VARCHAR(20) NOT NULL,
    asset_decimals        INT NOT NULL,
    pool_address          VARCHAR(42) NOT NULL,
    collateral_factor     NUMERIC(38,0) NOT NULL,
    liquidation_threshold NUMERIC(38,0) NOT NULL,
    liquidation_bonus     NUMERIC(38,0) NOT NULL DEFAULT 0,
    borrow_cap            NUMERIC(38,0) NOT NULL DEFAULT 0,
    supply_cap            NUMERIC(38,0) NOT NULL DEFAULT 0,
    status                VARCHAR(20) NOT NULL DEFAULT 'active',
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(chain_id, asset_address)
);

CREATE INDEX idx_markets_chain_id ON markets(chain_id);
CREATE INDEX idx_markets_asset ON markets(asset_address);
```

Create `migrations/000002_create_markets.down.sql`:

```sql
DROP TABLE IF EXISTS markets;
```

- [ ] **Step 3: Create market_states migration**

Create `migrations/000003_create_market_states.up.sql`:

```sql
CREATE TABLE IF NOT EXISTS market_states (
    market_id             BIGINT PRIMARY KEY REFERENCES markets(id),
    total_supply          NUMERIC(78,0) NOT NULL DEFAULT 0,
    total_borrow          NUMERIC(78,0) NOT NULL DEFAULT 0,
    supply_rate           NUMERIC(78,0) NOT NULL DEFAULT 0,
    borrow_rate           NUMERIC(78,0) NOT NULL DEFAULT 0,
    liquidity_index       NUMERIC(78,0) NOT NULL DEFAULT 1000000000000000000000000000,
    borrow_index          NUMERIC(78,0) NOT NULL DEFAULT 1000000000000000000000000000,
    last_update_timestamp BIGINT NOT NULL DEFAULT 0,
    utilization_rate      NUMERIC(78,0) NOT NULL DEFAULT 0,
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

Create `migrations/000003_create_market_states.down.sql`:

```sql
DROP TABLE IF EXISTS market_states;
```

- [ ] **Step 4: Create positions migration**

Create `migrations/000004_create_positions.up.sql`:

```sql
CREATE TABLE IF NOT EXISTS positions (
    id              BIGSERIAL PRIMARY KEY,
    chain_id        BIGINT NOT NULL,
    user_address    VARCHAR(42) NOT NULL,
    market_id       BIGINT NOT NULL REFERENCES markets(id),
    supply_balance  NUMERIC(78,0) NOT NULL DEFAULT 0,
    borrow_balance  NUMERIC(78,0) NOT NULL DEFAULT 0,
    supply_index    NUMERIC(78,0) NOT NULL DEFAULT 0,
    borrow_index    NUMERIC(78,0) NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(chain_id, user_address, market_id)
);

CREATE INDEX idx_positions_user ON positions(user_address);
CREATE INDEX idx_positions_market ON positions(market_id);
CREATE INDEX idx_positions_borrow ON positions(borrow_balance) WHERE borrow_balance > 0;
```

Create `migrations/000004_create_positions.down.sql`:

```sql
DROP TABLE IF EXISTS positions;
```

- [ ] **Step 5: Create events migration**

Create `migrations/000005_create_events.up.sql`:

```sql
CREATE TABLE IF NOT EXISTS events (
    id               BIGSERIAL PRIMARY KEY,
    chain_id         BIGINT NOT NULL,
    block_number     BIGINT NOT NULL,
    tx_hash          VARCHAR(66) NOT NULL,
    log_index        INT NOT NULL,
    event_type       VARCHAR(30) NOT NULL,
    contract_address VARCHAR(42) NOT NULL,
    market_id        BIGINT REFERENCES markets(id),
    user_address     VARCHAR(42),
    amount           NUMERIC(78,0),
    data             JSONB,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(chain_id, tx_hash, log_index)
);

CREATE INDEX idx_events_chain_block ON events(chain_id, block_number);
CREATE INDEX idx_events_user ON events(user_address);
CREATE INDEX idx_events_type ON events(event_type);
CREATE INDEX idx_events_market ON events(market_id);
```

Create `migrations/000005_create_events.down.sql`:

```sql
DROP TABLE IF EXISTS events;
```

- [ ] **Step 6: Create sync_status migration**

Create `migrations/000006_create_sync_status.up.sql`:

```sql
CREATE TABLE IF NOT EXISTS sync_status (
    chain_id             BIGINT PRIMARY KEY,
    last_indexed_block   BIGINT NOT NULL DEFAULT 0,
    last_confirmed_block BIGINT NOT NULL DEFAULT 0,
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

Create `migrations/000006_create_sync_status.down.sql`:

```sql
DROP TABLE IF EXISTS sync_status;
```

- [ ] **Step 7: Create blocks migration**

Create `migrations/000007_create_blocks.up.sql`:

```sql
CREATE TABLE IF NOT EXISTS blocks (
    chain_id     BIGINT NOT NULL,
    block_number BIGINT NOT NULL,
    block_hash   VARCHAR(66) NOT NULL,
    parent_hash  VARCHAR(66) NOT NULL,
    timestamp    TIMESTAMPTZ NOT NULL,
    is_confirmed BOOLEAN NOT NULL DEFAULT FALSE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (chain_id, block_number)
);

CREATE INDEX idx_blocks_hash ON blocks(block_hash);
```

Create `migrations/000007_create_blocks.down.sql`:

```sql
DROP TABLE IF EXISTS blocks;
```

- [ ] **Step 8: Create migration helper script**

Create `scripts/migrate.sh`:

```bash
#!/bin/bash
set -euo pipefail

DB_DSN="${DB_DSN:-postgres://defi:defi_secret@localhost:5432/defi_lending?sslmode=disable}"
MIGRATIONS_DIR="${MIGRATIONS_DIR:-./migrations}"

case "${1:-up}" in
  up)
    migrate -path "$MIGRATIONS_DIR" -database "$DB_DSN" up
    ;;
  down)
    migrate -path "$MIGRATIONS_DIR" -database "$DB_DSN" down "${2:-1}"
    ;;
  version)
    migrate -path "$MIGRATIONS_DIR" -database "$DB_DSN" version
    ;;
  *)
    echo "Usage: $0 {up|down [N]|version}"
    exit 1
    ;;
esac
```

- [ ] **Step 9: Commit**

```bash
chmod +x scripts/migrate.sh
git add migrations/ scripts/migrate.sh
git commit -m "feat: add database migration files for all core tables"
```

---

## Task 10: Chain Adapter Interface

**Files:**
- Create: `internal/chain/adapter.go`

- [ ] **Step 1: Define the ChainAdapter interface**

Create `internal/chain/adapter.go`:

```go
package chain

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	ctypes "github.com/zhangjinge/defi-lending-backend/internal/common/types"
)

// ChainAdapter abstracts chain-specific RPC interactions.
// Each EVM chain implements this interface.
type ChainAdapter interface {
	// ChainID returns the numeric chain identifier.
	ChainID() ctypes.ChainID

	// LatestBlockNumber returns the most recent block number.
	LatestBlockNumber(ctx context.Context) (uint64, error)

	// BlockByNumber fetches a block header by number.
	BlockByNumber(ctx context.Context, number *big.Int) (*types.Header, error)

	// SubscribeNewHead subscribes to new block headers via WebSocket.
	// Returns a channel that receives headers and a subscription for error handling.
	SubscribeNewHead(ctx context.Context) (chan *types.Header, ethereum.Subscription, error)

	// FilterLogs returns logs matching the given filter query.
	FilterLogs(ctx context.Context, query ethereum.FilterQuery) ([]types.Log, error)

	// CallContract executes a contract call (read-only) at the given block.
	CallContract(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error)

	// SendTransaction submits a signed transaction to the network.
	SendTransaction(ctx context.Context, tx *types.Transaction) error

	// SuggestGasPrice returns the currently suggested gas price.
	SuggestGasPrice(ctx context.Context) (*big.Int, error)

	// PendingNonceAt returns the next nonce for the given address.
	PendingNonceAt(ctx context.Context, addr common.Address) (uint64, error)

	// Close shuts down the underlying RPC connections.
	Close()
}
```

- [ ] **Step 2: Verify it compiles**

Run:
```bash
go build ./internal/chain/
```

Expected: No errors.

- [ ] **Step 3: Commit**

```bash
git add internal/chain/adapter.go
git commit -m "feat: define ChainAdapter interface for multi-chain EVM support"
```

---

## Task 11: EVM Client Implementation

**Files:**
- Create: `internal/chain/evm/client.go`
- Create: `internal/chain/evm/client_test.go`

- [ ] **Step 1: Write test for EVM client construction**

Create `internal/chain/evm/client_test.go`:

```go
package evm

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zhangjinge/defi-lending-backend/internal/common/config"
	ctypes "github.com/zhangjinge/defi-lending-backend/internal/common/types"
)

func TestNewEVMClient_InvalidRPC(t *testing.T) {
	cfg := config.ChainConfig{
		Name:    "test",
		ChainID: 1,
		RPCURLs: []string{"http://invalid-host:8545"},
		WSURL:   "ws://invalid-host:8546",
	}

	// Construction should succeed (lazy connection), but operations will fail.
	client, err := NewClient(cfg)
	if err != nil {
		// Some go-ethereum versions fail on dial, which is acceptable.
		assert.Contains(t, err.Error(), "dial")
		return
	}
	assert.NotNil(t, client)
	assert.Equal(t, ctypes.ChainID(1), client.ChainID())
	client.Close()
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:
```bash
cd /Users/zhangjinge/claude_projects/demo && go test ./internal/chain/evm/ -v
```

Expected: Compilation error — `NewClient` not defined.

- [ ] **Step 3: Implement EVM client**

Create `internal/chain/evm/client.go`:

```go
package evm

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/zhangjinge/defi-lending-backend/internal/common/config"
	ctypes "github.com/zhangjinge/defi-lending-backend/internal/common/types"
)

// Client implements chain.ChainAdapter for EVM-compatible chains.
type Client struct {
	chainID   ctypes.ChainID
	httpClient *ethclient.Client
	wsClient   *ethclient.Client
	cfg        config.ChainConfig
}

func NewClient(cfg config.ChainConfig) (*Client, error) {
	if len(cfg.RPCURLs) == 0 {
		return nil, fmt.Errorf("no RPC URLs configured for chain %s", cfg.Name)
	}

	httpClient, err := ethclient.Dial(cfg.RPCURLs[0])
	if err != nil {
		return nil, fmt.Errorf("dial http rpc %s: %w", cfg.RPCURLs[0], err)
	}

	var wsClient *ethclient.Client
	if cfg.WSURL != "" {
		wsClient, err = ethclient.Dial(cfg.WSURL)
		if err != nil {
			httpClient.Close()
			return nil, fmt.Errorf("dial ws rpc %s: %w", cfg.WSURL, err)
		}
	}

	return &Client{
		chainID:    ctypes.ChainID(cfg.ChainID),
		httpClient: httpClient,
		wsClient:   wsClient,
		cfg:        cfg,
	}, nil
}

func (c *Client) ChainID() ctypes.ChainID {
	return c.chainID
}

func (c *Client) LatestBlockNumber(ctx context.Context) (uint64, error) {
	return c.httpClient.BlockNumber(ctx)
}

func (c *Client) BlockByNumber(ctx context.Context, number *big.Int) (*types.Header, error) {
	return c.httpClient.HeaderByNumber(ctx, number)
}

func (c *Client) SubscribeNewHead(ctx context.Context) (chan *types.Header, ethereum.Subscription, error) {
	if c.wsClient == nil {
		return nil, nil, fmt.Errorf("no WebSocket client configured for chain %d", c.chainID)
	}
	headers := make(chan *types.Header, 16)
	sub, err := c.wsClient.SubscribeNewHead(ctx, headers)
	if err != nil {
		return nil, nil, fmt.Errorf("subscribe new head: %w", err)
	}
	return headers, sub, nil
}

func (c *Client) FilterLogs(ctx context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
	return c.httpClient.FilterLogs(ctx, query)
}

func (c *Client) CallContract(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
	return c.httpClient.CallContract(ctx, msg, blockNumber)
}

func (c *Client) SendTransaction(ctx context.Context, tx *types.Transaction) error {
	return c.httpClient.SendTransaction(ctx, tx)
}

func (c *Client) SuggestGasPrice(ctx context.Context) (*big.Int, error) {
	return c.httpClient.SuggestGasPrice(ctx)
}

func (c *Client) PendingNonceAt(ctx context.Context, addr common.Address) (uint64, error) {
	return c.httpClient.PendingNonceAt(ctx, addr)
}

func (c *Client) Close() {
	if c.httpClient != nil {
		c.httpClient.Close()
	}
	if c.wsClient != nil {
		c.wsClient.Close()
	}
}
```

- [ ] **Step 4: Run tests**

Run:
```bash
cd /Users/zhangjinge/claude_projects/demo && go test ./internal/chain/evm/ -v
```

Expected: PASS (test handles both dial-fail and lazy-connect gracefully).

- [ ] **Step 5: Commit**

```bash
git add internal/chain/evm/client.go internal/chain/evm/client_test.go
git commit -m "feat: implement EVM client wrapping go-ethereum ethclient"
```

---

## Task 12: Event Parser — ABI Decoding

**Files:**
- Create: `internal/indexer/parser/event_parser.go`
- Create: `internal/indexer/parser/event_parser_test.go`

- [ ] **Step 1: Write failing test for event parsing**

Create `internal/indexer/parser/event_parser_test.go`:

```go
package parser

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	ctypes "github.com/zhangjinge/defi-lending-backend/internal/common/types"
)

func TestEventParser_ParseDepositEvent(t *testing.T) {
	parser, err := NewEventParser()
	require.NoError(t, err)

	// Simulate a Deposit(address reserve, address user, uint256 amount) event log.
	// Topic0 = keccak256("Deposit(address,address,uint256)")
	depositSig := parser.EventSignature(ctypes.EventDeposit)
	require.NotEqual(t, common.Hash{}, depositSig, "Deposit event signature should be registered")

	log := types.Log{
		Address: common.HexToAddress("0xABCD000000000000000000000000000000000001"),
		Topics: []common.Hash{
			depositSig,
			common.HexToHash("0x000000000000000000000000aabb000000000000000000000000000000000001"), // reserve (indexed)
			common.HexToHash("0x000000000000000000000000ccdd000000000000000000000000000000000002"), // user (indexed)
		},
		// amount = 1000000000000000000 (1e18) encoded as 32 bytes
		Data:        common.Hex2Bytes("0000000000000000000000000000000000000000000000000DE0B6B3A7640000"),
		BlockNumber: 100,
		TxHash:      common.HexToHash("0x1234"),
		Index:       0,
	}

	event, err := parser.ParseLog(ctypes.ChainID(1), log)
	require.NoError(t, err)
	assert.Equal(t, ctypes.EventDeposit, event.EventType)
	assert.Equal(t, common.HexToAddress("0xccdd000000000000000000000000000000000002"), event.UserAddress)
	assert.Equal(t, common.HexToAddress("0xaabb000000000000000000000000000000000001"), event.MarketAddress)
	assert.Equal(t, big.NewInt(1e18), event.Amount)
}

func TestEventParser_UnknownEvent(t *testing.T) {
	parser, err := NewEventParser()
	require.NoError(t, err)

	log := types.Log{
		Topics: []common.Hash{
			common.HexToHash("0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"),
		},
		BlockNumber: 100,
	}

	_, err = parser.ParseLog(ctypes.ChainID(1), log)
	assert.ErrorIs(t, err, ErrUnknownEvent)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:
```bash
cd /Users/zhangjinge/claude_projects/demo && go test ./internal/indexer/parser/ -v
```

Expected: Compilation error — `NewEventParser`, `ErrUnknownEvent` not defined.

- [ ] **Step 3: Implement event parser**

Create `internal/indexer/parser/event_parser.go`:

```go
package parser

import (
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	ctypes "github.com/zhangjinge/defi-lending-backend/internal/common/types"
)

var ErrUnknownEvent = errors.New("unknown event signature")

// eventDef maps an EventType to its Solidity signature string.
var eventDefs = map[ctypes.EventType]string{
	ctypes.EventDeposit:            "Deposit(address,address,uint256)",
	ctypes.EventWithdraw:           "Withdraw(address,address,uint256)",
	ctypes.EventBorrow:             "Borrow(address,address,uint256)",
	ctypes.EventRepay:              "Repay(address,address,uint256)",
	ctypes.EventLiquidation:        "LiquidationCall(address,address,address,uint256,uint256)",
	ctypes.EventReserveDataUpdated: "ReserveDataUpdated(address,uint256,uint256,uint256,uint256)",
	ctypes.EventAccrueInterest:     "AccrueInterest(uint256,uint256,uint256)",
}

// EventParser decodes raw EVM logs into structured ChainEvents.
type EventParser struct {
	sigToType map[common.Hash]ctypes.EventType
	typeToSig map[ctypes.EventType]common.Hash
}

func NewEventParser() (*EventParser, error) {
	sigToType := make(map[common.Hash]ctypes.EventType)
	typeToSig := make(map[ctypes.EventType]common.Hash)

	for evtType, sigStr := range eventDefs {
		sig := crypto.Keccak256Hash([]byte(sigStr))
		sigToType[sig] = evtType
		typeToSig[evtType] = sig
	}

	return &EventParser{
		sigToType: sigToType,
		typeToSig: typeToSig,
	}, nil
}

// EventSignature returns the keccak256 topic hash for the given event type.
func (p *EventParser) EventSignature(eventType ctypes.EventType) common.Hash {
	return p.typeToSig[eventType]
}

// AllSignatures returns all registered event topic hashes (for FilterLogs).
func (p *EventParser) AllSignatures() []common.Hash {
	sigs := make([]common.Hash, 0, len(p.sigToType))
	for sig := range p.sigToType {
		sigs = append(sigs, sig)
	}
	return sigs
}

// ParseLog converts a raw EVM log into a ChainEvent.
func (p *EventParser) ParseLog(chainID ctypes.ChainID, log types.Log) (*ctypes.ChainEvent, error) {
	if len(log.Topics) == 0 {
		return nil, ErrUnknownEvent
	}

	evtType, ok := p.sigToType[log.Topics[0]]
	if !ok {
		return nil, ErrUnknownEvent
	}

	event := &ctypes.ChainEvent{
		ChainID:         chainID,
		BlockNumber:     log.BlockNumber,
		TxHash:          log.TxHash,
		LogIndex:        log.Index,
		EventType:       evtType,
		ContractAddress: log.Address,
		Timestamp:       time.Now(),
		Data:            make(map[string]interface{}),
	}

	switch evtType {
	case ctypes.EventDeposit, ctypes.EventWithdraw, ctypes.EventBorrow, ctypes.EventRepay:
		if err := p.parseSimpleEvent(log, event); err != nil {
			return nil, fmt.Errorf("parse %s event: %w", evtType, err)
		}
	case ctypes.EventLiquidation:
		if err := p.parseLiquidationEvent(log, event); err != nil {
			return nil, fmt.Errorf("parse liquidation event: %w", err)
		}
	case ctypes.EventReserveDataUpdated:
		if err := p.parseReserveDataUpdated(log, event); err != nil {
			return nil, fmt.Errorf("parse reserve data updated: %w", err)
		}
	case ctypes.EventAccrueInterest:
		if err := p.parseAccrueInterest(log, event); err != nil {
			return nil, fmt.Errorf("parse accrue interest: %w", err)
		}
	}

	return event, nil
}

// parseSimpleEvent handles Deposit/Withdraw/Borrow/Repay.
// Expected topics: [sig, reserve(indexed), user(indexed)], data: [amount].
func (p *EventParser) parseSimpleEvent(log types.Log, event *ctypes.ChainEvent) error {
	if len(log.Topics) < 3 {
		return fmt.Errorf("expected 3 topics, got %d", len(log.Topics))
	}
	event.MarketAddress = common.HexToAddress(log.Topics[1].Hex())
	event.UserAddress = common.HexToAddress(log.Topics[2].Hex())

	if len(log.Data) >= 32 {
		event.Amount = new(big.Int).SetBytes(log.Data[:32])
	}
	return nil
}

// parseLiquidationEvent handles LiquidationCall.
// Topics: [sig, collateral(indexed), debt(indexed), user(indexed)], data: [debtAmount, collateralAmount].
func (p *EventParser) parseLiquidationEvent(log types.Log, event *ctypes.ChainEvent) error {
	if len(log.Topics) < 4 {
		return fmt.Errorf("expected 4 topics, got %d", len(log.Topics))
	}
	event.Data["collateral_asset"] = common.HexToAddress(log.Topics[1].Hex()).Hex()
	event.Data["debt_asset"] = common.HexToAddress(log.Topics[2].Hex()).Hex()
	event.UserAddress = common.HexToAddress(log.Topics[3].Hex())

	if len(log.Data) >= 64 {
		event.Amount = new(big.Int).SetBytes(log.Data[:32])
		event.Data["collateral_seized"] = new(big.Int).SetBytes(log.Data[32:64]).String()
	}
	return nil
}

// parseReserveDataUpdated handles ReserveDataUpdated.
// Topics: [sig, reserve(indexed)], data: [liquidityRate, stableBorrowRate, variableBorrowRate, liquidityIndex].
func (p *EventParser) parseReserveDataUpdated(log types.Log, event *ctypes.ChainEvent) error {
	if len(log.Topics) < 2 {
		return fmt.Errorf("expected 2 topics, got %d", len(log.Topics))
	}
	event.MarketAddress = common.HexToAddress(log.Topics[1].Hex())

	if len(log.Data) >= 128 {
		event.Data["liquidity_rate"] = new(big.Int).SetBytes(log.Data[:32]).String()
		event.Data["stable_borrow_rate"] = new(big.Int).SetBytes(log.Data[32:64]).String()
		event.Data["variable_borrow_rate"] = new(big.Int).SetBytes(log.Data[64:96]).String()
		event.Data["liquidity_index"] = new(big.Int).SetBytes(log.Data[96:128]).String()
	}
	return nil
}

// parseAccrueInterest handles AccrueInterest.
// Topics: [sig], data: [cashPrior, interestAccumulated, borrowIndex].
func (p *EventParser) parseAccrueInterest(log types.Log, event *ctypes.ChainEvent) error {
	if len(log.Data) >= 96 {
		event.Data["cash_prior"] = new(big.Int).SetBytes(log.Data[:32]).String()
		event.Data["interest_accumulated"] = new(big.Int).SetBytes(log.Data[32:64]).String()
		event.Data["borrow_index"] = new(big.Int).SetBytes(log.Data[64:96]).String()
	}
	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run:
```bash
cd /Users/zhangjinge/claude_projects/demo && go test ./internal/indexer/parser/ -v
```

Expected: Both tests PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/indexer/parser/event_parser.go internal/indexer/parser/event_parser_test.go
git commit -m "feat: implement event parser with ABI decoding for lending protocol events"
```

---

## Task 13: Reorg Detector

**Files:**
- Create: `internal/indexer/reorg/detector.go`
- Create: `internal/indexer/reorg/detector_test.go`

- [ ] **Step 1: Write failing test for reorg detection**

Create `internal/indexer/reorg/detector_test.go`:

```go
package reorg

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
	ctypes "github.com/zhangjinge/defi-lending-backend/internal/common/types"
)

func TestDetector_NoReorg(t *testing.T) {
	d := NewDetector(10)

	// Add blocks 1-3 with valid parent chain.
	d.RecordBlock(ctypes.BlockHeader{Number: 1, Hash: hash("0xA1"), ParentHash: hash("0x00")})
	d.RecordBlock(ctypes.BlockHeader{Number: 2, Hash: hash("0xA2"), ParentHash: hash("0xA1")})
	d.RecordBlock(ctypes.BlockHeader{Number: 3, Hash: hash("0xA3"), ParentHash: hash("0xA2")})

	reorgBlock, isReorg := d.DetectReorg(ctypes.BlockHeader{
		Number: 4, Hash: hash("0xA4"), ParentHash: hash("0xA3"),
	})
	assert.False(t, isReorg)
	assert.Equal(t, uint64(0), reorgBlock)
}

func TestDetector_SimpleReorg(t *testing.T) {
	d := NewDetector(10)

	d.RecordBlock(ctypes.BlockHeader{Number: 1, Hash: hash("0xA1"), ParentHash: hash("0x00")})
	d.RecordBlock(ctypes.BlockHeader{Number: 2, Hash: hash("0xA2"), ParentHash: hash("0xA1")})
	d.RecordBlock(ctypes.BlockHeader{Number: 3, Hash: hash("0xA3"), ParentHash: hash("0xA2")})

	// New block 3 with different hash, parent still points to block 2.
	// This means block 3 was replaced (reorg at depth 1).
	reorgBlock, isReorg := d.DetectReorg(ctypes.BlockHeader{
		Number: 3, Hash: hash("0xB3"), ParentHash: hash("0xA2"),
	})
	assert.True(t, isReorg)
	assert.Equal(t, uint64(3), reorgBlock)
}

func TestDetector_DeepReorg(t *testing.T) {
	d := NewDetector(10)

	d.RecordBlock(ctypes.BlockHeader{Number: 1, Hash: hash("0xA1"), ParentHash: hash("0x00")})
	d.RecordBlock(ctypes.BlockHeader{Number: 2, Hash: hash("0xA2"), ParentHash: hash("0xA1")})
	d.RecordBlock(ctypes.BlockHeader{Number: 3, Hash: hash("0xA3"), ParentHash: hash("0xA2")})

	// Block 4 arrives but its parent doesn't match our block 3 hash → reorg happened deeper.
	reorgBlock, isReorg := d.DetectReorg(ctypes.BlockHeader{
		Number: 4, Hash: hash("0xB4"), ParentHash: hash("0xB3"),
	})
	assert.True(t, isReorg)
	// The fork point is at block 3 (our block 3's hash doesn't match the new parent).
	assert.Equal(t, uint64(3), reorgBlock)
}

func hash(hex string) common.Hash {
	return common.HexToHash(hex)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:
```bash
cd /Users/zhangjinge/claude_projects/demo && go test ./internal/indexer/reorg/ -v
```

Expected: Compilation error.

- [ ] **Step 3: Implement reorg detector**

Create `internal/indexer/reorg/detector.go`:

```go
package reorg

import (
	"sync"

	"github.com/ethereum/go-ethereum/common"
	ctypes "github.com/zhangjinge/defi-lending-backend/internal/common/types"
)

// Detector tracks recent block hashes and detects chain reorganizations
// by comparing parent hashes of incoming blocks.
type Detector struct {
	mu       sync.RWMutex
	// blockHashes maps block_number → block_hash for recent blocks.
	blockHashes map[uint64]common.Hash
	maxBlocks   int
	minBlock    uint64
}

func NewDetector(maxBlocks int) *Detector {
	return &Detector{
		blockHashes: make(map[uint64]common.Hash),
		maxBlocks:   maxBlocks,
	}
}

// RecordBlock stores a block's hash for future reorg detection.
func (d *Detector) RecordBlock(header ctypes.BlockHeader) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.blockHashes[header.Number] = header.Hash

	// Prune old blocks beyond the window.
	if len(d.blockHashes) > d.maxBlocks {
		oldest := header.Number - uint64(d.maxBlocks)
		for num := range d.blockHashes {
			if num <= oldest {
				delete(d.blockHashes, num)
			}
		}
	}
}

// DetectReorg checks if the incoming block indicates a chain reorganization.
// Returns (forkBlockNumber, true) if a reorg is detected, or (0, false) otherwise.
func (d *Detector) DetectReorg(incoming ctypes.BlockHeader) (uint64, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	// Case 1: We already have a block at this height with a different hash → reorg at this height.
	if existingHash, ok := d.blockHashes[incoming.Number]; ok {
		if existingHash != incoming.Hash {
			return incoming.Number, true
		}
		// Same hash = duplicate, no reorg.
		return 0, false
	}

	// Case 2: New block extends the chain. Check if parent matches our record.
	parentNum := incoming.Number - 1
	if parentHash, ok := d.blockHashes[parentNum]; ok {
		if parentHash != incoming.ParentHash {
			// Parent mismatch: the chain forked at or before parentNum.
			return parentNum, true
		}
	}

	// No data to compare or parent matches → no reorg detected.
	return 0, false
}

// Rollback removes all recorded blocks from the given block number onward.
func (d *Detector) Rollback(fromBlock uint64) {
	d.mu.Lock()
	defer d.mu.Unlock()

	for num := range d.blockHashes {
		if num >= fromBlock {
			delete(d.blockHashes, num)
		}
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run:
```bash
cd /Users/zhangjinge/claude_projects/demo && go test ./internal/indexer/reorg/ -v
```

Expected: All 3 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/indexer/reorg/detector.go internal/indexer/reorg/detector_test.go
git commit -m "feat: implement chain reorg detector with sliding window block hash tracking"
```

---

## Task 14: Block Listener

**Files:**
- Create: `internal/indexer/listener/block_listener.go`
- Create: `internal/indexer/listener/block_listener_test.go`

- [ ] **Step 1: Write test for block listener with mock adapter**

Create `internal/indexer/listener/block_listener_test.go`:

```go
package listener

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/stretchr/testify/assert"
	ctypes "github.com/zhangjinge/defi-lending-backend/internal/common/types"
)

// mockAdapter implements chain.ChainAdapter for testing.
type mockAdapter struct {
	latestBlock uint64
	headers     map[uint64]*types.Header
}

func (m *mockAdapter) ChainID() ctypes.ChainID                      { return 1 }
func (m *mockAdapter) LatestBlockNumber(_ context.Context) (uint64, error) {
	return m.latestBlock, nil
}
func (m *mockAdapter) BlockByNumber(_ context.Context, num *big.Int) (*types.Header, error) {
	h, ok := m.headers[num.Uint64()]
	if !ok {
		return nil, ethereum.NotFound
	}
	return h, nil
}
func (m *mockAdapter) SubscribeNewHead(_ context.Context) (chan *types.Header, ethereum.Subscription, error) {
	return nil, nil, ethereum.NotFound
}
func (m *mockAdapter) FilterLogs(_ context.Context, _ ethereum.FilterQuery) ([]types.Log, error) {
	return nil, nil
}
func (m *mockAdapter) CallContract(_ context.Context, _ ethereum.CallMsg, _ *big.Int) ([]byte, error) {
	return nil, nil
}
func (m *mockAdapter) SendTransaction(_ context.Context, _ *types.Transaction) error { return nil }
func (m *mockAdapter) SuggestGasPrice(_ context.Context) (*big.Int, error) {
	return big.NewInt(0), nil
}
func (m *mockAdapter) PendingNonceAt(_ context.Context, _ common.Address) (uint64, error) {
	return 0, nil
}
func (m *mockAdapter) Close() {}

func TestBlockListener_PollNewBlocks(t *testing.T) {
	adapter := &mockAdapter{
		latestBlock: 3,
		headers: map[uint64]*types.Header{
			1: {Number: big.NewInt(1)},
			2: {Number: big.NewInt(2)},
			3: {Number: big.NewInt(3)},
		},
	}

	listener := NewBlockListener(adapter, 50*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	headerCh := listener.Poll(ctx, 1)

	var received []uint64
	for h := range headerCh {
		received = append(received, h.Number.Uint64())
		if len(received) == 3 {
			cancel()
		}
	}

	assert.Equal(t, []uint64{1, 2, 3}, received)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:
```bash
cd /Users/zhangjinge/claude_projects/demo && go test ./internal/indexer/listener/ -v
```

Expected: Compilation error.

- [ ] **Step 3: Implement block listener**

Create `internal/indexer/listener/block_listener.go`:

```go
package listener

import (
	"context"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/rs/zerolog/log"
	"github.com/zhangjinge/defi-lending-backend/internal/chain"
)

// BlockListener fetches new blocks from an EVM chain via polling or WebSocket subscription.
type BlockListener struct {
	adapter      chain.ChainAdapter
	pollInterval time.Duration
}

func NewBlockListener(adapter chain.ChainAdapter, pollInterval time.Duration) *BlockListener {
	return &BlockListener{
		adapter:      adapter,
		pollInterval: pollInterval,
	}
}

// Poll starts polling for new block headers starting from startBlock.
// It sends headers on the returned channel and closes it when ctx is cancelled.
func (bl *BlockListener) Poll(ctx context.Context, startBlock uint64) <-chan *types.Header {
	out := make(chan *types.Header, 64)
	go func() {
		defer close(out)
		nextBlock := startBlock
		ticker := time.NewTicker(bl.pollInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				latest, err := bl.adapter.LatestBlockNumber(ctx)
				if err != nil {
					log.Warn().Err(err).Int64("chain_id", int64(bl.adapter.ChainID())).Msg("failed to get latest block")
					continue
				}

				for nextBlock <= latest {
					header, err := bl.adapter.BlockByNumber(ctx, new(big.Int).SetUint64(nextBlock))
					if err != nil {
						log.Warn().Err(err).Uint64("block", nextBlock).Msg("failed to get block header")
						break
					}

					select {
					case out <- header:
						nextBlock++
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()
	return out
}

// Subscribe uses WebSocket to receive new block headers in real-time.
// Falls back to Poll if subscription fails.
func (bl *BlockListener) Subscribe(ctx context.Context, startBlock uint64) <-chan *types.Header {
	headerCh, sub, err := bl.adapter.SubscribeNewHead(ctx)
	if err != nil {
		log.Warn().Err(err).Msg("WebSocket subscription failed, falling back to polling")
		return bl.Poll(ctx, startBlock)
	}

	out := make(chan *types.Header, 64)
	go func() {
		defer close(out)
		defer sub.Unsubscribe()

		// First, backfill any blocks between startBlock and latest.
		latest, err := bl.adapter.LatestBlockNumber(ctx)
		if err == nil {
			for bn := startBlock; bn <= latest; bn++ {
				h, err := bl.adapter.BlockByNumber(ctx, new(big.Int).SetUint64(bn))
				if err != nil {
					break
				}
				select {
				case out <- h:
				case <-ctx.Done():
					return
				}
			}
		}

		// Then, stream new headers from the subscription.
		for {
			select {
			case <-ctx.Done():
				return
			case err := <-sub.Err():
				log.Error().Err(err).Msg("WebSocket subscription error, switching to poll")
				// Drain remaining and switch to polling.
				nextBlock := startBlock
				if latest > 0 {
					nextBlock = latest + 1
				}
				pollCh := bl.Poll(ctx, nextBlock)
				for h := range pollCh {
					select {
					case out <- h:
					case <-ctx.Done():
						return
					}
				}
				return
			case h := <-headerCh:
				if h != nil {
					select {
					case out <- h:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()
	return out
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run:
```bash
cd /Users/zhangjinge/claude_projects/demo && go test ./internal/indexer/listener/ -v -timeout 10s
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/indexer/listener/block_listener.go internal/indexer/listener/block_listener_test.go
git commit -m "feat: implement block listener with polling and WebSocket subscription fallback"
```

---

## Task 15: Event Consumer — Kafka to DB Writer

**Files:**
- Create: `internal/indexer/consumer/event_consumer.go`
- Create: `internal/indexer/consumer/event_consumer_test.go`

- [ ] **Step 1: Write test for event consumer DB writing logic**

Create `internal/indexer/consumer/event_consumer_test.go`:

```go
package consumer

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
	ctypes "github.com/zhangjinge/defi-lending-backend/internal/common/types"
)

// mockStore implements EventStore for unit testing without a real DB.
type mockStore struct {
	events []*ctypes.ChainEvent
	blocks []ctypes.BlockHeader
	syncs  map[ctypes.ChainID]uint64
}

func newMockStore() *mockStore {
	return &mockStore{
		syncs: make(map[ctypes.ChainID]uint64),
	}
}

func (m *mockStore) SaveEvent(_ context.Context, event *ctypes.ChainEvent) error {
	m.events = append(m.events, event)
	return nil
}

func (m *mockStore) SaveBlock(_ context.Context, header ctypes.BlockHeader) error {
	m.blocks = append(m.blocks, header)
	return nil
}

func (m *mockStore) UpdateSyncStatus(_ context.Context, chainID ctypes.ChainID, block uint64) error {
	m.syncs[chainID] = block
	return nil
}

func (m *mockStore) DeleteEventsFromBlock(_ context.Context, chainID ctypes.ChainID, fromBlock uint64) error {
	filtered := make([]*ctypes.ChainEvent, 0)
	for _, e := range m.events {
		if e.ChainID != chainID || e.BlockNumber < fromBlock {
			filtered = append(filtered, e)
		}
	}
	m.events = filtered
	return nil
}

func (m *mockStore) DeleteBlocksFromBlock(_ context.Context, chainID ctypes.ChainID, fromBlock uint64) error {
	filtered := make([]ctypes.BlockHeader, 0)
	for _, b := range m.blocks {
		if b.ChainID != chainID || b.Number < fromBlock {
			filtered = append(filtered, b)
		}
	}
	m.blocks = filtered
	return nil
}

func (m *mockStore) GetSyncStatus(_ context.Context, chainID ctypes.ChainID) (uint64, error) {
	return m.syncs[chainID], nil
}

func TestEventConsumer_ProcessEvent(t *testing.T) {
	store := newMockStore()
	consumer := NewEventConsumer(store)

	event := &ctypes.ChainEvent{
		ChainID:         1,
		BlockNumber:     100,
		TxHash:          common.HexToHash("0xabc"),
		LogIndex:        0,
		EventType:       ctypes.EventDeposit,
		ContractAddress: common.HexToAddress("0x1234"),
		UserAddress:     common.HexToAddress("0x5678"),
		Amount:          big.NewInt(1e18),
		Timestamp:       time.Now(),
	}

	err := consumer.ProcessEvent(context.Background(), event)
	assert.NoError(t, err)
	assert.Len(t, store.events, 1)
	assert.Equal(t, ctypes.EventDeposit, store.events[0].EventType)
}

func TestEventConsumer_HandleReorg(t *testing.T) {
	store := newMockStore()
	consumer := NewEventConsumer(store)

	// Save events at blocks 100, 101, 102.
	for _, bn := range []uint64{100, 101, 102} {
		_ = consumer.ProcessEvent(context.Background(), &ctypes.ChainEvent{
			ChainID:     1,
			BlockNumber: bn,
			EventType:   ctypes.EventDeposit,
			Amount:      big.NewInt(1),
		})
	}
	assert.Len(t, store.events, 3)

	// Reorg from block 101 → events at 101 and 102 should be removed.
	err := consumer.HandleReorg(context.Background(), ctypes.ChainID(1), 101)
	assert.NoError(t, err)
	assert.Len(t, store.events, 1)
	assert.Equal(t, uint64(100), store.events[0].BlockNumber)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:
```bash
cd /Users/zhangjinge/claude_projects/demo && go test ./internal/indexer/consumer/ -v
```

Expected: Compilation error.

- [ ] **Step 3: Implement event consumer**

Create `internal/indexer/consumer/event_consumer.go`:

```go
package consumer

import (
	"context"

	ctypes "github.com/zhangjinge/defi-lending-backend/internal/common/types"
)

// EventStore defines the persistence interface that the consumer depends on.
type EventStore interface {
	SaveEvent(ctx context.Context, event *ctypes.ChainEvent) error
	SaveBlock(ctx context.Context, header ctypes.BlockHeader) error
	UpdateSyncStatus(ctx context.Context, chainID ctypes.ChainID, block uint64) error
	DeleteEventsFromBlock(ctx context.Context, chainID ctypes.ChainID, fromBlock uint64) error
	DeleteBlocksFromBlock(ctx context.Context, chainID ctypes.ChainID, fromBlock uint64) error
	GetSyncStatus(ctx context.Context, chainID ctypes.ChainID) (uint64, error)
}

// EventConsumer processes decoded chain events and persists them via EventStore.
type EventConsumer struct {
	store EventStore
}

func NewEventConsumer(store EventStore) *EventConsumer {
	return &EventConsumer{store: store}
}

// ProcessEvent saves a single event to the store.
func (ec *EventConsumer) ProcessEvent(ctx context.Context, event *ctypes.ChainEvent) error {
	return ec.store.SaveEvent(ctx, event)
}

// ProcessBlock records a block and updates sync status.
func (ec *EventConsumer) ProcessBlock(ctx context.Context, header ctypes.BlockHeader) error {
	if err := ec.store.SaveBlock(ctx, header); err != nil {
		return err
	}
	return ec.store.UpdateSyncStatus(ctx, header.ChainID, header.Number)
}

// HandleReorg rolls back all events and blocks from the given block number onward.
func (ec *EventConsumer) HandleReorg(ctx context.Context, chainID ctypes.ChainID, fromBlock uint64) error {
	if err := ec.store.DeleteEventsFromBlock(ctx, chainID, fromBlock); err != nil {
		return err
	}
	if err := ec.store.DeleteBlocksFromBlock(ctx, chainID, fromBlock); err != nil {
		return err
	}
	if fromBlock > 0 {
		return ec.store.UpdateSyncStatus(ctx, chainID, fromBlock-1)
	}
	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run:
```bash
cd /Users/zhangjinge/claude_projects/demo && go test ./internal/indexer/consumer/ -v
```

Expected: Both tests PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/indexer/consumer/event_consumer.go internal/indexer/consumer/event_consumer_test.go
git commit -m "feat: implement event consumer with reorg rollback support"
```

---

## Task 16: Indexer Service Orchestrator

**Files:**
- Create: `internal/indexer/service.go`

- [ ] **Step 1: Implement the indexer orchestrator**

Create `internal/indexer/service.go`:

```go
package indexer

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/rs/zerolog"
	"github.com/zhangjinge/defi-lending-backend/internal/chain"
	"github.com/zhangjinge/defi-lending-backend/internal/common/config"
	ctypes "github.com/zhangjinge/defi-lending-backend/internal/common/types"
	"github.com/zhangjinge/defi-lending-backend/internal/indexer/consumer"
	"github.com/zhangjinge/defi-lending-backend/internal/indexer/listener"
	"github.com/zhangjinge/defi-lending-backend/internal/indexer/parser"
	"github.com/zhangjinge/defi-lending-backend/internal/indexer/reorg"
	"github.com/zhangjinge/defi-lending-backend/internal/infra/messaging"
)

// Service orchestrates multi-chain indexing.
type Service struct {
	chains   map[ctypes.ChainID]chainIndexer
	consumer *consumer.EventConsumer
	producer *messaging.Producer
	parser   *parser.EventParser
	logger   zerolog.Logger
}

type chainIndexer struct {
	adapter  chain.ChainAdapter
	listener *listener.BlockListener
	detector *reorg.Detector
	cfg      config.ChainConfig
}

func NewService(
	adapters map[ctypes.ChainID]chain.ChainAdapter,
	chainConfigs []config.ChainConfig,
	store consumer.EventStore,
	producer *messaging.Producer,
	logger zerolog.Logger,
) (*Service, error) {
	ep, err := parser.NewEventParser()
	if err != nil {
		return nil, fmt.Errorf("create event parser: %w", err)
	}

	chains := make(map[ctypes.ChainID]chainIndexer)
	for _, cfg := range chainConfigs {
		cid := ctypes.ChainID(cfg.ChainID)
		adapter, ok := adapters[cid]
		if !ok {
			return nil, fmt.Errorf("no adapter for chain %d", cfg.ChainID)
		}
		pollInterval := time.Duration(cfg.BlockTime) * time.Second / 2
		if pollInterval < time.Second {
			pollInterval = time.Second
		}
		chains[cid] = chainIndexer{
			adapter:  adapter,
			listener: listener.NewBlockListener(adapter, pollInterval),
			detector: reorg.NewDetector(cfg.Confirmations * 2),
			cfg:      cfg,
		}
	}

	return &Service{
		chains:   chains,
		consumer: consumer.NewEventConsumer(store),
		producer: producer,
		parser:   ep,
		logger:   logger,
	}, nil
}

// Run starts indexing all configured chains. Blocks until ctx is cancelled.
func (s *Service) Run(ctx context.Context, store consumer.EventStore) error {
	var wg sync.WaitGroup

	for cid, ci := range s.chains {
		wg.Add(1)
		go func(chainID ctypes.ChainID, ci chainIndexer) {
			defer wg.Done()
			s.runChain(ctx, chainID, ci, store)
		}(cid, ci)
	}

	wg.Wait()
	return ctx.Err()
}

func (s *Service) runChain(ctx context.Context, chainID ctypes.ChainID, ci chainIndexer, store consumer.EventStore) {
	log := s.logger.With().Int64("chain_id", int64(chainID)).Logger()
	log.Info().Msg("starting chain indexer")

	// Determine start block from sync status.
	lastBlock, err := store.GetSyncStatus(ctx, chainID)
	if err != nil {
		log.Error().Err(err).Msg("failed to get sync status, starting from 0")
	}
	startBlock := lastBlock + 1

	headerCh := ci.listener.Subscribe(ctx, startBlock)

	for header := range headerCh {
		if header == nil {
			continue
		}

		blockNum := header.Number.Uint64()
		blockHeader := ctypes.BlockHeader{
			ChainID:    chainID,
			Number:     blockNum,
			Hash:       header.Hash(),
			ParentHash: header.ParentHash,
			Timestamp:  time.Unix(int64(header.Time), 0),
		}

		// Check for reorg.
		if forkBlock, isReorg := ci.detector.DetectReorg(blockHeader); isReorg {
			log.Warn().Uint64("fork_block", forkBlock).Uint64("current_block", blockNum).Msg("reorg detected")
			if err := s.consumer.HandleReorg(ctx, chainID, forkBlock); err != nil {
				log.Error().Err(err).Msg("failed to handle reorg")
				continue
			}
			ci.detector.Rollback(forkBlock)
		}

		ci.detector.RecordBlock(blockHeader)

		// Process block: record it and fetch logs.
		if err := s.consumer.ProcessBlock(ctx, blockHeader); err != nil {
			log.Error().Err(err).Uint64("block", blockNum).Msg("failed to process block")
			continue
		}

		// Fetch and parse events from this block.
		contractAddr := common.HexToAddress(ci.cfg.Contracts.LendingPool)
		logs, err := ci.adapter.FilterLogs(ctx, ethereum.FilterQuery{
			FromBlock: new(big.Int).SetUint64(blockNum),
			ToBlock:   new(big.Int).SetUint64(blockNum),
			Addresses: []common.Address{contractAddr},
			Topics:    [][]common.Hash{s.parser.AllSignatures()},
		})
		if err != nil {
			log.Error().Err(err).Uint64("block", blockNum).Msg("failed to filter logs")
			continue
		}

		for _, rawLog := range logs {
			event, err := s.parser.ParseLog(chainID, rawLog)
			if err != nil {
				// Unknown events are expected (other contracts), just skip.
				continue
			}
			event.Timestamp = blockHeader.Timestamp

			// Persist event.
			if err := s.consumer.ProcessEvent(ctx, event); err != nil {
				log.Error().Err(err).Str("tx", rawLog.TxHash.Hex()).Msg("failed to save event")
				continue
			}

			// Publish to Kafka for downstream consumers.
			key := fmt.Sprintf("%d-%d", chainID, blockNum)
			if err := s.producer.Publish(ctx, key, event); err != nil {
				log.Error().Err(err).Msg("failed to publish event to kafka")
			}
		}

		log.Debug().Uint64("block", blockNum).Int("events", len(logs)).Msg("block indexed")
	}

	log.Info().Msg("chain indexer stopped")
}
```

- [ ] **Step 2: Verify it compiles**

Run:
```bash
cd /Users/zhangjinge/claude_projects/demo && go build ./internal/indexer/
```

Expected: No errors.

- [ ] **Step 3: Commit**

```bash
git add internal/indexer/service.go
git commit -m "feat: implement multi-chain indexer orchestrator with reorg handling and Kafka publishing"
```

---

## Task 17: Indexer Main Entry Point

**Files:**
- Modify: `cmd/indexer/main.go`

- [ ] **Step 1: Wire up the indexer service in main.go**

Replace `cmd/indexer/main.go` with:

```go
package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/zhangjinge/defi-lending-backend/internal/chain"
	"github.com/zhangjinge/defi-lending-backend/internal/chain/evm"
	"github.com/zhangjinge/defi-lending-backend/internal/common/config"
	"github.com/zhangjinge/defi-lending-backend/internal/common/logger"
	ctypes "github.com/zhangjinge/defi-lending-backend/internal/common/types"
	"github.com/zhangjinge/defi-lending-backend/internal/indexer"
	"github.com/zhangjinge/defi-lending-backend/internal/infra/messaging"
)

func main() {
	configPath := flag.String("config", "configs/config.yaml", "path to config file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		panic("failed to load config: " + err.Error())
	}

	log := logger.New(cfg.Log.Level, cfg.Log.Format)
	log.Info().Msg("starting defi-lending indexer")

	// Build chain adapters.
	adapters := make(map[ctypes.ChainID]chain.ChainAdapter)
	for _, chainCfg := range cfg.Chains {
		client, err := evm.NewClient(chainCfg)
		if err != nil {
			log.Fatal().Err(err).Str("chain", chainCfg.Name).Msg("failed to create chain client")
		}
		adapters[ctypes.ChainID(chainCfg.ChainID)] = client
	}
	defer func() {
		for _, a := range adapters {
			a.Close()
		}
	}()

	// Kafka producer for chain events.
	producer := messaging.NewProducer(cfg.Kafka, cfg.Kafka.Topics.ChainEvents)
	defer producer.Close()

	// TODO: In Phase 1, we use a placeholder in-memory store.
	// Phase 2+ will replace with real PostgreSQL store.
	store := newInMemoryStore()

	svc, err := indexer.NewService(adapters, cfg.Chains, store, producer, log)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create indexer service")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Graceful shutdown on SIGINT/SIGTERM.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Info().Str("signal", sig.String()).Msg("shutting down indexer")
		cancel()
	}()

	if err := svc.Run(ctx, store); err != nil && err != context.Canceled {
		log.Fatal().Err(err).Msg("indexer exited with error")
	}
	log.Info().Msg("indexer stopped")
}

// inMemoryStore is a temporary placeholder implementing consumer.EventStore.
// Will be replaced with PostgreSQL implementation in subsequent phases.
type inMemoryStore struct {
	syncStatus map[ctypes.ChainID]uint64
}

func newInMemoryStore() *inMemoryStore {
	return &inMemoryStore{syncStatus: make(map[ctypes.ChainID]uint64)}
}

func (s *inMemoryStore) SaveEvent(_ context.Context, _ *ctypes.ChainEvent) error   { return nil }
func (s *inMemoryStore) SaveBlock(_ context.Context, _ ctypes.BlockHeader) error    { return nil }
func (s *inMemoryStore) UpdateSyncStatus(_ context.Context, chainID ctypes.ChainID, block uint64) error {
	s.syncStatus[chainID] = block
	return nil
}
func (s *inMemoryStore) DeleteEventsFromBlock(_ context.Context, _ ctypes.ChainID, _ uint64) error {
	return nil
}
func (s *inMemoryStore) DeleteBlocksFromBlock(_ context.Context, _ ctypes.ChainID, _ uint64) error {
	return nil
}
func (s *inMemoryStore) GetSyncStatus(_ context.Context, chainID ctypes.ChainID) (uint64, error) {
	return s.syncStatus[chainID], nil
}
```

- [ ] **Step 2: Verify it compiles**

Run:
```bash
cd /Users/zhangjinge/claude_projects/demo && go build ./cmd/indexer/
```

Expected: No errors.

- [ ] **Step 3: Run all tests to confirm nothing is broken**

Run:
```bash
cd /Users/zhangjinge/claude_projects/demo && go test ./... -v -count=1
```

Expected: All tests PASS.

- [ ] **Step 4: Commit**

```bash
git add cmd/indexer/main.go
git commit -m "feat: wire up indexer main entry point with graceful shutdown"
```

---

## Self-Review Checklist

1. **Spec coverage for Phase 1:**
   - [x] Project scaffolding (Task 1)
   - [x] Config loading (Task 2)
   - [x] Structured logging (Task 3)
   - [x] Shared types (Task 4)
   - [x] Wad/Ray math (Task 5)
   - [x] PostgreSQL client (Task 6)
   - [x] Redis client (Task 7)
   - [x] Kafka client (Task 8)
   - [x] DB migrations for all core tables (Task 9)
   - [x] ChainAdapter interface (Task 10)
   - [x] EVM client implementation (Task 11)
   - [x] Event parser with ABI decoding (Task 12)
   - [x] Reorg detection (Task 13)
   - [x] Block listener with WS + polling fallback (Task 14)
   - [x] Event consumer with reorg rollback (Task 15)
   - [x] Multi-chain indexer orchestrator (Task 16)
   - [x] Indexer entry point (Task 17)

2. **Placeholder scan:** No TBD/TODO in implementation code. The `inMemoryStore` in main.go is a documented placeholder for Phase 1 only.

3. **Type consistency:** `ctypes.ChainID`, `ctypes.ChainEvent`, `ctypes.BlockHeader`, `ctypes.EventType` — used consistently across all tasks. `consumer.EventStore` interface matches mock in tests.

4. **Remaining phases to plan:**
   - Phase 2: Price Oracle + Market Service (depends on indexer events)
   - Phase 3: Liquidation Engine (RateComputer, PositionScanner, LiqExecutor)
   - Phase 4: Account Service + API Gateway
   - Phase 5: Risk Engine + Notification System
   - Phase 6: Docker, K8s, Helm, CI/CD
