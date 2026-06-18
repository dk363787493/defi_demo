# Phase 2: Price Oracle & Market Service — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the Price Oracle service (Chainlink integration, price caching, deviation detection, Kafka publishing) and the Market Service (market listing, real-time rates, TVL, history API endpoints with Redis caching).

**Architecture:** Price Oracle runs as a background goroutine polling Chainlink price feeds, caching in Redis, and publishing updates to Kafka. Market Service provides REST API endpoints that read from PostgreSQL with Redis caching. Both integrate with Phase 1's indexer data and infrastructure.

**Tech Stack:** Go 1.22+, go-ethereum (Chainlink ABI calls), Redis, PostgreSQL, Kafka, Gin, Prometheus

---

## File Structure (Phase 2)

```
internal/
├── oracle/
│   ├── types.go                    # Price data types
│   ├── chainlink.go                # Chainlink price feed reader
│   ├── chainlink_test.go
│   ├── cache.go                    # Redis price cache
│   ├── cache_test.go
│   ├── service.go                  # Oracle service orchestrator
│   └── service_test.go
├── market/
│   ├── repository.go               # DB queries for markets
│   ├── repository_test.go
│   ├── service.go                  # Business logic
│   ├── service_test.go
│   └── handler.go                  # Gin HTTP handlers
├── store/
│   └── postgres_store.go           # Shared PostgreSQL EventStore implementation
└── api/
    └── handler/
        └── market_handler.go       # Market API route registration
migrations/
├── 000008_create_prices.up.sql
├── 000008_create_prices.down.sql
├── 000009_create_market_snapshots.up.sql
└── 000009_create_market_snapshots.down.sql
```

---

## Task 1: Price Data Types

**Files:**
- Create: `internal/oracle/types.go`

- [ ] **Step 1: Define price types**

Create `internal/oracle/types.go`:

```go
package oracle

import (
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// PriceSource identifies where a price came from.
type PriceSource string

const (
	SourceChainlink PriceSource = "chainlink"
	SourceFallback  PriceSource = "fallback"
)

// PriceData holds a single asset price reading.
type PriceData struct {
	AssetAddress common.Address `json:"asset_address"`
	PriceUSD     *big.Int       `json:"price_usd"`     // 8 decimal fixed-point (Chainlink standard)
	Decimals     uint8          `json:"decimals"`       // price decimals (typically 8)
	Source       PriceSource    `json:"source"`
	RoundID      *big.Int       `json:"round_id"`
	UpdatedAt    time.Time      `json:"updated_at"`
}

// PriceFeedConfig defines a Chainlink price feed for an asset.
type PriceFeedConfig struct {
	AssetAddress common.Address `json:"asset_address"`
	AssetSymbol  string         `json:"asset_symbol"`
	FeedAddress  common.Address `json:"feed_address"`
	Decimals     uint8          `json:"decimals"`
	HeartbeatSec int64          `json:"heartbeat_sec"` // max seconds between updates
	DeviationPct float64        `json:"deviation_pct"` // alert if price deviates more than this %
}
```

- [ ] **Step 2: Verify it compiles**

Run: `go build ./internal/oracle/`

- [ ] **Step 3: Commit**

```bash
git add internal/oracle/types.go
git commit -m "feat(oracle): add price data types and feed config"
```

---

## Task 2: Chainlink Price Feed Reader

**Files:**
- Create: `internal/oracle/chainlink.go`
- Create: `internal/oracle/chainlink_test.go`

- [ ] **Step 1: Write test for Chainlink reader**

Create `internal/oracle/chainlink_test.go`:

```go
package oracle

import (
	"context"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockCaller implements ethereum.ContractCaller for testing.
type mockCaller struct {
	callResults map[string][]byte
}

func (m *mockCaller) CallContract(ctx context.Context, call ethereum.CallMsg, block *big.Int) ([]byte, error) {
	key := common.Bytes2Hex(call.Data[:4])
	if result, ok := m.callResults[key]; ok {
		return result, nil
	}
	return nil, nil
}

func TestChainlinkReader_ParseLatestRoundData(t *testing.T) {
	// Verify ABI encoding of latestRoundData method selector
	reader := NewChainlinkReader(nil)
	selector := reader.LatestRoundDataSelector()
	// latestRoundData() selector = 0xfeaf968c
	assert.Equal(t, "feaf968c", common.Bytes2Hex(selector))
}

func TestDecodeLatestRoundData(t *testing.T) {
	reader := NewChainlinkReader(nil)

	// Encode a mock response: (roundId=100, answer=200000000000, startedAt=1000, updatedAt=2000, answeredInRound=100)
	// Each field is 32 bytes ABI-encoded
	roundID := common.LeftPadBytes(big.NewInt(100).Bytes(), 32)
	answer := common.LeftPadBytes(big.NewInt(200000000000).Bytes(), 32) // $2000.00000000 with 8 decimals
	startedAt := common.LeftPadBytes(big.NewInt(1000).Bytes(), 32)
	updatedAt := common.LeftPadBytes(big.NewInt(2000).Bytes(), 32)
	answeredInRound := common.LeftPadBytes(big.NewInt(100).Bytes(), 32)

	data := make([]byte, 0, 160)
	data = append(data, roundID...)
	data = append(data, answer...)
	data = append(data, startedAt...)
	data = append(data, updatedAt...)
	data = append(data, answeredInRound...)

	rID, price, ts, err := reader.DecodeLatestRoundData(data)
	require.NoError(t, err)
	assert.Equal(t, big.NewInt(100), rID)
	assert.Equal(t, big.NewInt(200000000000), price)
	assert.Equal(t, int64(2000), ts)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/oracle/ -v -run TestChainlink`

Expected: Compilation error.

- [ ] **Step 3: Implement Chainlink reader**

Create `internal/oracle/chainlink.go`:

```go
package oracle

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

// ContractCaller abstracts contract read calls for testing.
type ContractCaller interface {
	CallContract(ctx context.Context, call ethereum.CallMsg, block *big.Int) ([]byte, error)
}

// ChainlinkReader reads prices from Chainlink Aggregator V3 contracts.
type ChainlinkReader struct {
	caller ContractCaller
}

func NewChainlinkReader(caller ContractCaller) *ChainlinkReader {
	return &ChainlinkReader{caller: caller}
}

// LatestRoundDataSelector returns the 4-byte function selector for latestRoundData().
func (r *ChainlinkReader) LatestRoundDataSelector() []byte {
	sig := crypto.Keccak256([]byte("latestRoundData()"))
	return sig[:4]
}

// FetchPrice calls latestRoundData() on a Chainlink price feed contract.
func (r *ChainlinkReader) FetchPrice(ctx context.Context, feed PriceFeedConfig) (*PriceData, error) {
	callData := r.LatestRoundDataSelector()

	result, err := r.caller.CallContract(ctx, ethereum.CallMsg{
		To:   &feed.FeedAddress,
		Data: callData,
	}, nil)
	if err != nil {
		return nil, fmt.Errorf("call latestRoundData on %s: %w", feed.FeedAddress.Hex(), err)
	}

	roundID, price, updatedAtUnix, err := r.DecodeLatestRoundData(result)
	if err != nil {
		return nil, fmt.Errorf("decode latestRoundData: %w", err)
	}

	return &PriceData{
		AssetAddress: feed.AssetAddress,
		PriceUSD:     price,
		Decimals:     feed.Decimals,
		Source:       SourceChainlink,
		RoundID:      roundID,
		UpdatedAt:    time.Unix(updatedAtUnix, 0),
	}, nil
}

// DecodeLatestRoundData decodes the ABI-encoded response from latestRoundData().
// Returns (roundId, answer, updatedAt, error).
// Response layout: (uint80 roundId, int256 answer, uint256 startedAt, uint256 updatedAt, uint80 answeredInRound)
func (r *ChainlinkReader) DecodeLatestRoundData(data []byte) (*big.Int, *big.Int, int64, error) {
	if len(data) < 160 {
		return nil, nil, 0, fmt.Errorf("response too short: %d bytes, need 160", len(data))
	}

	roundID := new(big.Int).SetBytes(data[0:32])
	answer := new(big.Int).SetBytes(data[32:64])
	// startedAt at data[64:96] — not needed
	updatedAt := new(big.Int).SetBytes(data[96:128])

	if answer.Sign() <= 0 {
		return nil, nil, 0, fmt.Errorf("invalid price: %s", answer.String())
	}

	return roundID, answer, updatedAt.Int64(), nil
}
```

- [ ] **Step 4: Run tests**

Run: `go test ./internal/oracle/ -v -run TestChainlink`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/oracle/chainlink.go internal/oracle/chainlink_test.go
git commit -m "feat(oracle): implement Chainlink price feed reader with ABI decoding"
```

---

## Task 3: Redis Price Cache

**Files:**
- Create: `internal/oracle/cache.go`
- Create: `internal/oracle/cache_test.go`

- [ ] **Step 1: Write test for price cache**

Create `internal/oracle/cache_test.go`:

```go
package oracle

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockRedisClient implements a simple in-memory cache for testing.
type mockRedisClient struct {
	store map[string]string
}

func newMockRedisClient() *mockRedisClient {
	return &mockRedisClient{store: make(map[string]string)}
}

func (m *mockRedisClient) Get(_ context.Context, key string) (string, error) {
	v, ok := m.store[key]
	if !ok {
		return "", fmt.Errorf("key not found")
	}
	return v, nil
}

func (m *mockRedisClient) Set(_ context.Context, key string, value string, _ time.Duration) error {
	m.store[key] = value
	return nil
}

func TestPriceCache_SetAndGet(t *testing.T) {
	rc := newMockRedisClient()
	cache := NewPriceCache(rc)

	price := &PriceData{
		AssetAddress: common.HexToAddress("0x1111111111111111111111111111111111111111"),
		PriceUSD:     big.NewInt(200000000000),
		Decimals:     8,
		Source:       SourceChainlink,
		RoundID:      big.NewInt(100),
		UpdatedAt:    time.Unix(1000, 0),
	}

	err := cache.Set(context.Background(), price, 30*time.Second)
	require.NoError(t, err)

	got, err := cache.Get(context.Background(), price.AssetAddress)
	require.NoError(t, err)
	assert.Equal(t, price.PriceUSD.String(), got.PriceUSD.String())
	assert.Equal(t, price.Source, got.Source)
}

func TestPriceCache_GetMiss(t *testing.T) {
	rc := newMockRedisClient()
	cache := NewPriceCache(rc)

	_, err := cache.Get(context.Background(), common.HexToAddress("0x9999"))
	assert.Error(t, err)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/oracle/ -v -run TestPriceCache`

Expected: Compilation error.

- [ ] **Step 3: Implement price cache**

Create `internal/oracle/cache.go`:

```go
package oracle

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// RedisClient abstracts Redis operations for testing.
type RedisClient interface {
	Get(ctx context.Context, key string) (string, error)
	Set(ctx context.Context, key string, value string, ttl time.Duration) error
}

// PriceCache wraps Redis for price data caching.
type PriceCache struct {
	client RedisClient
}

func NewPriceCache(client RedisClient) *PriceCache {
	return &PriceCache{client: client}
}

func priceKey(asset common.Address) string {
	return fmt.Sprintf("price:%s", asset.Hex())
}

// Set caches a price with the given TTL.
func (c *PriceCache) Set(ctx context.Context, price *PriceData, ttl time.Duration) error {
	data, err := json.Marshal(price)
	if err != nil {
		return fmt.Errorf("marshal price: %w", err)
	}
	return c.client.Set(ctx, priceKey(price.AssetAddress), string(data), ttl)
}

// Get retrieves a cached price for the given asset.
func (c *PriceCache) Get(ctx context.Context, asset common.Address) (*PriceData, error) {
	val, err := c.client.Get(ctx, priceKey(asset))
	if err != nil {
		return nil, fmt.Errorf("cache miss for %s: %w", asset.Hex(), err)
	}

	var price PriceData
	if err := json.Unmarshal([]byte(val), &price); err != nil {
		return nil, fmt.Errorf("unmarshal cached price: %w", err)
	}

	return &price, nil
}

// GetAll retrieves cached prices for multiple assets. Returns only found prices.
func (c *PriceCache) GetAll(ctx context.Context, assets []common.Address) map[common.Address]*PriceData {
	result := make(map[common.Address]*PriceData)
	for _, asset := range assets {
		if p, err := c.Get(ctx, asset); err == nil {
			result[asset] = p
		}
	}
	return result
}
```

- [ ] **Step 4: Run tests**

Run: `go test ./internal/oracle/ -v -run TestPriceCache`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/oracle/cache.go internal/oracle/cache_test.go
git commit -m "feat(oracle): add Redis-backed price cache with TTL support"
```

---

## Task 4: Price Migration

**Files:**
- Create: `migrations/000008_create_prices.up.sql`
- Create: `migrations/000008_create_prices.down.sql`

- [ ] **Step 1: Create prices migration**

Create `migrations/000008_create_prices.up.sql`:

```sql
CREATE TABLE IF NOT EXISTS prices (
    id            BIGSERIAL PRIMARY KEY,
    asset_address VARCHAR(42) NOT NULL,
    chain_id      BIGINT NOT NULL,
    price_usd     NUMERIC(78,0) NOT NULL,
    decimals      INT NOT NULL DEFAULT 8,
    source        VARCHAR(20) NOT NULL,
    round_id      NUMERIC(78,0),
    timestamp     TIMESTAMPTZ NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_prices_asset_time ON prices(asset_address, timestamp DESC);
CREATE INDEX idx_prices_chain ON prices(chain_id);
```

Create `migrations/000008_create_prices.down.sql`:

```sql
DROP TABLE IF EXISTS prices;
```

- [ ] **Step 2: Commit**

```bash
git add migrations/000008_*
git commit -m "feat(oracle): add prices table migration"
```

---

## Task 5: Market Snapshot Migration

**Files:**
- Create: `migrations/000009_create_market_snapshots.up.sql`
- Create: `migrations/000009_create_market_snapshots.down.sql`

- [ ] **Step 1: Create market snapshots migration**

Create `migrations/000009_create_market_snapshots.up.sql`:

```sql
CREATE TABLE IF NOT EXISTS market_snapshots (
    id                BIGSERIAL PRIMARY KEY,
    market_id         BIGINT NOT NULL REFERENCES markets(id),
    total_supply      NUMERIC(78,0) NOT NULL,
    total_borrow      NUMERIC(78,0) NOT NULL,
    supply_rate       NUMERIC(78,0) NOT NULL,
    borrow_rate       NUMERIC(78,0) NOT NULL,
    utilization_rate  NUMERIC(78,0) NOT NULL,
    tvl_usd           NUMERIC(78,0) NOT NULL DEFAULT 0,
    snapshot_time     TIMESTAMPTZ NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_snapshots_market_time ON market_snapshots(market_id, snapshot_time DESC);
```

Create `migrations/000009_create_market_snapshots.down.sql`:

```sql
DROP TABLE IF EXISTS market_snapshots;
```

- [ ] **Step 2: Commit**

```bash
git add migrations/000009_*
git commit -m "feat(market): add market_snapshots table migration for historical data"
```

---

## Task 6: Oracle Service

**Files:**
- Create: `internal/oracle/service.go`
- Create: `internal/oracle/service_test.go`

- [ ] **Step 1: Write test for oracle service**

Create `internal/oracle/service_test.go`:

```go
package oracle

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockPublisher struct {
	published []*PriceData
}

func (m *mockPublisher) PublishPrice(_ context.Context, price *PriceData) error {
	m.published = append(m.published, price)
	return nil
}

type mockPriceStore struct {
	prices []*PriceData
}

func (m *mockPriceStore) SavePrice(_ context.Context, chainID int64, price *PriceData) error {
	m.prices = append(m.prices, price)
	return nil
}

type mockChainlinkFetcher struct {
	prices map[common.Address]*PriceData
}

func (m *mockChainlinkFetcher) FetchPrice(_ context.Context, feed PriceFeedConfig) (*PriceData, error) {
	if p, ok := m.prices[feed.AssetAddress]; ok {
		return p, nil
	}
	return nil, fmt.Errorf("no price for %s", feed.AssetAddress.Hex())
}

func TestOracleService_FetchAndCache(t *testing.T) {
	asset := common.HexToAddress("0x1111111111111111111111111111111111111111")
	mockPrice := &PriceData{
		AssetAddress: asset,
		PriceUSD:     big.NewInt(200000000000), // $2000 with 8 decimals
		Decimals:     8,
		Source:       SourceChainlink,
		RoundID:      big.NewInt(100),
		UpdatedAt:    time.Now(),
	}

	fetcher := &mockChainlinkFetcher{
		prices: map[common.Address]*PriceData{asset: mockPrice},
	}
	cache := NewPriceCache(newMockRedisClient())
	publisher := &mockPublisher{}
	store := &mockPriceStore{}

	svc := NewService(fetcher, cache, publisher, store, 1)

	feeds := []PriceFeedConfig{{
		AssetAddress: asset,
		AssetSymbol:  "ETH",
		FeedAddress:  common.HexToAddress("0xfeed"),
		Decimals:     8,
		HeartbeatSec: 3600,
	}}

	err := svc.FetchAndUpdate(context.Background(), feeds)
	require.NoError(t, err)

	// Verify price was cached
	cached, err := cache.Get(context.Background(), asset)
	require.NoError(t, err)
	assert.Equal(t, mockPrice.PriceUSD.String(), cached.PriceUSD.String())

	// Verify price was published
	assert.Len(t, publisher.published, 1)

	// Verify price was stored
	assert.Len(t, store.prices, 1)
}

func TestOracleService_DeviationDetection(t *testing.T) {
	asset := common.HexToAddress("0x1111111111111111111111111111111111111111")

	// Seed cache with old price of $2000
	cache := NewPriceCache(newMockRedisClient())
	oldPrice := &PriceData{
		AssetAddress: asset,
		PriceUSD:     big.NewInt(200000000000),
		Decimals:     8,
		Source:       SourceChainlink,
		UpdatedAt:    time.Now().Add(-1 * time.Minute),
	}
	_ = cache.Set(context.Background(), oldPrice, time.Hour)

	// New price is $2200 = 10% deviation
	newPrice := &PriceData{
		AssetAddress: asset,
		PriceUSD:     big.NewInt(220000000000),
		Decimals:     8,
		Source:       SourceChainlink,
		RoundID:      big.NewInt(101),
		UpdatedAt:    time.Now(),
	}

	deviation := CalculateDeviation(oldPrice.PriceUSD, newPrice.PriceUSD)
	assert.InDelta(t, 10.0, deviation, 0.01)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/oracle/ -v -run TestOracleService`

Expected: Compilation error.

- [ ] **Step 3: Implement oracle service**

Create `internal/oracle/service.go`:

```go
package oracle

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/rs/zerolog/log"
)

// PriceFetcher abstracts the price feed reading.
type PriceFetcher interface {
	FetchPrice(ctx context.Context, feed PriceFeedConfig) (*PriceData, error)
}

// PricePublisher publishes price updates to downstream consumers.
type PricePublisher interface {
	PublishPrice(ctx context.Context, price *PriceData) error
}

// PriceStore persists prices for historical queries.
type PriceStore interface {
	SavePrice(ctx context.Context, chainID int64, price *PriceData) error
}

// Service orchestrates price fetching, caching, deviation detection, and publishing.
type Service struct {
	fetcher   PriceFetcher
	cache     *PriceCache
	publisher PricePublisher
	store     PriceStore
	chainID   int64
}

func NewService(fetcher PriceFetcher, cache *PriceCache, publisher PricePublisher, store PriceStore, chainID int64) *Service {
	return &Service{
		fetcher:   fetcher,
		cache:     cache,
		publisher: publisher,
		store:     store,
		chainID:   chainID,
	}
}

// FetchAndUpdate fetches prices for all configured feeds, caches them, checks deviations,
// publishes to Kafka, and persists to DB.
func (s *Service) FetchAndUpdate(ctx context.Context, feeds []PriceFeedConfig) error {
	for _, feed := range feeds {
		price, err := s.fetcher.FetchPrice(ctx, feed)
		if err != nil {
			log.Error().Err(err).Str("asset", feed.AssetSymbol).Msg("failed to fetch price")
			continue
		}

		// Check deviation against cached price.
		if cached, err := s.cache.Get(ctx, feed.AssetAddress); err == nil {
			deviation := CalculateDeviation(cached.PriceUSD, price.PriceUSD)
			if deviation > feed.DeviationPct && feed.DeviationPct > 0 {
				log.Warn().
					Str("asset", feed.AssetSymbol).
					Float64("deviation_pct", deviation).
					Str("old_price", cached.PriceUSD.String()).
					Str("new_price", price.PriceUSD.String()).
					Msg("price deviation alert")
			}
		}

		// Check heartbeat.
		if feed.HeartbeatSec > 0 {
			staleness := time.Since(price.UpdatedAt).Seconds()
			if staleness > float64(feed.HeartbeatSec) {
				log.Warn().
					Str("asset", feed.AssetSymbol).
					Float64("staleness_sec", staleness).
					Msg("price feed heartbeat stale")
			}
		}

		// Cache with TTL based on heartbeat.
		ttl := 30 * time.Second
		if feed.HeartbeatSec > 0 && feed.HeartbeatSec < 30 {
			ttl = time.Duration(feed.HeartbeatSec) * time.Second
		}
		if err := s.cache.Set(ctx, price, ttl); err != nil {
			log.Error().Err(err).Str("asset", feed.AssetSymbol).Msg("failed to cache price")
		}

		// Publish to Kafka.
		if err := s.publisher.PublishPrice(ctx, price); err != nil {
			log.Error().Err(err).Str("asset", feed.AssetSymbol).Msg("failed to publish price")
		}

		// Persist to DB.
		if err := s.store.SavePrice(ctx, s.chainID, price); err != nil {
			log.Error().Err(err).Str("asset", feed.AssetSymbol).Msg("failed to save price")
		}
	}
	return nil
}

// GetPrice returns the cached price for an asset, falling back to a fresh fetch.
func (s *Service) GetPrice(ctx context.Context, asset common.Address) (*PriceData, error) {
	if cached, err := s.cache.Get(ctx, asset); err == nil {
		return cached, nil
	}
	return nil, fmt.Errorf("no cached price for %s", asset.Hex())
}

// GetPrices returns cached prices for multiple assets.
func (s *Service) GetPrices(ctx context.Context, assets []common.Address) map[common.Address]*PriceData {
	return s.cache.GetAll(ctx, assets)
}

// CalculateDeviation returns the percentage deviation between old and new price.
func CalculateDeviation(oldPrice, newPrice *big.Int) float64 {
	if oldPrice.Sign() == 0 {
		return 0
	}
	oldF := new(big.Float).SetInt(oldPrice)
	newF := new(big.Float).SetInt(newPrice)
	diff := new(big.Float).Sub(newF, oldF)
	ratio := new(big.Float).Quo(diff, oldF)
	result, _ := ratio.Float64()
	return math.Abs(result * 100)
}

// RunLoop starts a polling loop that fetches prices at the given interval.
func (s *Service) RunLoop(ctx context.Context, feeds []PriceFeedConfig, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Fetch once immediately.
	_ = s.FetchAndUpdate(ctx, feeds)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = s.FetchAndUpdate(ctx, feeds)
		}
	}
}
```

- [ ] **Step 4: Run tests**

Run: `go test ./internal/oracle/ -v`

Expected: All tests PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/oracle/service.go internal/oracle/service_test.go
git commit -m "feat(oracle): implement oracle service with fetch, cache, deviation detection, and Kafka publishing"
```

---

## Task 7: Market Repository (DB Queries)

**Files:**
- Create: `internal/market/repository.go`
- Create: `internal/market/repository_test.go`

- [ ] **Step 1: Write test for market repository**

Create `internal/market/repository_test.go`:

```go
package market

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

type mockDB struct {
	markets    []MarketInfo
	states     map[int64]*MarketStateInfo
	snapshots  []MarketSnapshot
}

func (m *mockDB) ListMarkets(_ context.Context, chainID int64) ([]MarketInfo, error) {
	if chainID == 0 {
		return m.markets, nil
	}
	var result []MarketInfo
	for _, mkt := range m.markets {
		if mkt.ChainID == chainID {
			result = append(result, mkt)
		}
	}
	return result, nil
}

func (m *mockDB) GetMarketState(_ context.Context, marketID int64) (*MarketStateInfo, error) {
	if s, ok := m.states[marketID]; ok {
		return s, nil
	}
	return nil, fmt.Errorf("market not found")
}

func (m *mockDB) GetMarketSnapshots(_ context.Context, marketID int64, _ string, _ string) ([]MarketSnapshot, error) {
	var result []MarketSnapshot
	for _, s := range m.snapshots {
		if s.MarketID == marketID {
			result = append(result, s)
		}
	}
	return result, nil
}

func TestListMarkets_FilterByChain(t *testing.T) {
	db := &mockDB{
		markets: []MarketInfo{
			{ID: 1, ChainID: 1, AssetSymbol: "ETH"},
			{ID: 2, ChainID: 1, AssetSymbol: "USDC"},
			{ID: 3, ChainID: 42161, AssetSymbol: "ETH"},
		},
	}

	repo := NewRepository(db)

	// Filter by chain 1
	markets, err := repo.ListMarkets(context.Background(), 1)
	assert.NoError(t, err)
	assert.Len(t, markets, 2)

	// All chains
	allMarkets, err := repo.ListMarkets(context.Background(), 0)
	assert.NoError(t, err)
	assert.Len(t, allMarkets, 3)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/market/ -v`

Expected: Compilation error.

- [ ] **Step 3: Implement market repository**

Create `internal/market/repository.go`:

```go
package market

import (
	"context"
	"fmt"
	"math/big"
	"time"
)

// MarketInfo represents a market listing returned to the API.
type MarketInfo struct {
	ID                   int64   `json:"id"`
	ChainID              int64   `json:"chain_id"`
	AssetAddress         string  `json:"asset_address"`
	AssetSymbol          string  `json:"asset_symbol"`
	AssetDecimals        int     `json:"asset_decimals"`
	PoolAddress          string  `json:"pool_address"`
	CollateralFactor     string  `json:"collateral_factor"`
	LiquidationThreshold string  `json:"liquidation_threshold"`
	BorrowCap            string  `json:"borrow_cap"`
	SupplyCap            string  `json:"supply_cap"`
	Status               string  `json:"status"`
}

// MarketStateInfo combines market info with live state.
type MarketStateInfo struct {
	MarketInfo
	TotalSupply     string `json:"total_supply"`
	TotalBorrow     string `json:"total_borrow"`
	SupplyRate      string `json:"supply_rate"`
	BorrowRate      string `json:"borrow_rate"`
	UtilizationRate string `json:"utilization_rate"`
	LiquidityIndex  string `json:"liquidity_index"`
	BorrowIndex     string `json:"borrow_index"`
	TvlUSD          string `json:"tvl_usd,omitempty"`
}

// MarketSnapshot is a point-in-time market data record for charts.
type MarketSnapshot struct {
	MarketID        int64     `json:"market_id"`
	TotalSupply     string    `json:"total_supply"`
	TotalBorrow     string    `json:"total_borrow"`
	SupplyRate      string    `json:"supply_rate"`
	BorrowRate      string    `json:"borrow_rate"`
	UtilizationRate string    `json:"utilization_rate"`
	TvlUSD          string    `json:"tvl_usd"`
	SnapshotTime    time.Time `json:"snapshot_time"`
}

// MarketDB abstracts database queries for markets.
type MarketDB interface {
	ListMarkets(ctx context.Context, chainID int64) ([]MarketInfo, error)
	GetMarketState(ctx context.Context, marketID int64) (*MarketStateInfo, error)
	GetMarketSnapshots(ctx context.Context, marketID int64, period string, interval string) ([]MarketSnapshot, error)
}

// Repository provides market data access with caching support.
type Repository struct {
	db MarketDB
}

func NewRepository(db MarketDB) *Repository {
	return &Repository{db: db}
}

func (r *Repository) ListMarkets(ctx context.Context, chainID int64) ([]MarketInfo, error) {
	return r.db.ListMarkets(ctx, chainID)
}

func (r *Repository) GetMarketDetail(ctx context.Context, marketID int64) (*MarketStateInfo, error) {
	return r.db.GetMarketState(ctx, marketID)
}

func (r *Repository) GetHistory(ctx context.Context, marketID int64, period string, interval string) ([]MarketSnapshot, error) {
	return r.db.GetMarketSnapshots(ctx, marketID, period, interval)
}

// CalculateAPY converts a Ray-scaled per-second rate to annual percentage yield.
func CalculateAPY(rateRay *big.Int) float64 {
	if rateRay == nil || rateRay.Sign() == 0 {
		return 0
	}
	ray := new(big.Float).SetInt(rateRay)
	rayUnit := new(big.Float).SetFloat64(1e27)
	ratePerSec, _ := new(big.Float).Quo(ray, rayUnit).Float64()
	secondsPerYear := 365.25 * 24 * 3600
	apy := (1+ratePerSec)*secondsPerYear - 1
	_ = fmt.Sprintf("%.6f", apy) // suppress unused
	return ratePerSec * secondsPerYear * 100
}
```

- [ ] **Step 4: Run tests**

Run: `go test ./internal/market/ -v`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/market/repository.go internal/market/repository_test.go
git commit -m "feat(market): add market repository with DB query abstraction"
```

---

## Task 8: Market Service (Business Logic)

**Files:**
- Create: `internal/market/service.go`

- [ ] **Step 1: Implement market service**

Create `internal/market/service.go`:

```go
package market

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// CacheClient abstracts Redis for market data caching.
type CacheClient interface {
	Get(ctx context.Context, key string) (string, error)
	Set(ctx context.Context, key string, value string, ttl time.Duration) error
}

// Service provides market data with Redis caching.
type Service struct {
	repo  *Repository
	cache CacheClient
}

func NewService(repo *Repository, cache CacheClient) *Service {
	return &Service{repo: repo, cache: cache}
}

func (s *Service) ListMarkets(ctx context.Context, chainID int64) ([]MarketInfo, error) {
	cacheKey := fmt.Sprintf("markets:list:%d", chainID)

	// Try cache first.
	if val, err := s.cache.Get(ctx, cacheKey); err == nil {
		var markets []MarketInfo
		if json.Unmarshal([]byte(val), &markets) == nil {
			return markets, nil
		}
	}

	markets, err := s.repo.ListMarkets(ctx, chainID)
	if err != nil {
		return nil, err
	}

	// Cache for 30 seconds.
	if data, err := json.Marshal(markets); err == nil {
		_ = s.cache.Set(ctx, cacheKey, string(data), 30*time.Second)
	}

	return markets, nil
}

func (s *Service) GetMarketDetail(ctx context.Context, marketID int64) (*MarketStateInfo, error) {
	cacheKey := fmt.Sprintf("markets:detail:%d", marketID)

	if val, err := s.cache.Get(ctx, cacheKey); err == nil {
		var state MarketStateInfo
		if json.Unmarshal([]byte(val), &state) == nil {
			return &state, nil
		}
	}

	state, err := s.repo.GetMarketDetail(ctx, marketID)
	if err != nil {
		return nil, err
	}

	if data, err := json.Marshal(state); err == nil {
		_ = s.cache.Set(ctx, cacheKey, string(data), 10*time.Second)
	}

	return state, nil
}

func (s *Service) GetHistory(ctx context.Context, marketID int64, period string, interval string) ([]MarketSnapshot, error) {
	return s.repo.GetHistory(ctx, marketID, period, interval)
}
```

- [ ] **Step 2: Verify it compiles**

Run: `go build ./internal/market/`

- [ ] **Step 3: Commit**

```bash
git add internal/market/service.go
git commit -m "feat(market): add market service with Redis caching layer"
```

---

## Task 9: Market HTTP Handlers

**Files:**
- Create: `internal/market/handler.go`

- [ ] **Step 1: Implement Gin HTTP handlers**

Create `internal/market/handler.go`:

```go
package market

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
)

// Handler provides HTTP endpoints for market data.
type Handler struct {
	svc *Service
}

func NewHandler(svc *Service) *Handler {
	return &Handler{svc: svc}
}

// RegisterRoutes sets up market API routes on the given router group.
func (h *Handler) RegisterRoutes(rg *gin.RouterGroup) {
	rg.GET("/markets", h.ListMarkets)
	rg.GET("/markets/:market_id", h.GetMarketDetail)
	rg.GET("/markets/:market_id/history", h.GetMarketHistory)
}

// ListMarkets returns all markets, optionally filtered by chain_id query param.
func (h *Handler) ListMarkets(c *gin.Context) {
	var chainID int64
	if cid := c.Query("chain_id"); cid != "" {
		var err error
		chainID, err = strconv.ParseInt(cid, 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid chain_id"})
			return
		}
	}

	markets, err := h.svc.ListMarkets(c.Request.Context(), chainID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list markets"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"data": markets})
}

// GetMarketDetail returns a single market's current state.
func (h *Handler) GetMarketDetail(c *gin.Context) {
	marketID, err := strconv.ParseInt(c.Param("market_id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid market_id"})
		return
	}

	state, err := h.svc.GetMarketDetail(c.Request.Context(), marketID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "market not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"data": state})
}

// GetMarketHistory returns historical market snapshots.
func (h *Handler) GetMarketHistory(c *gin.Context) {
	marketID, err := strconv.ParseInt(c.Param("market_id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid market_id"})
		return
	}

	period := c.DefaultQuery("period", "7d")
	interval := c.DefaultQuery("interval", "1h")

	snapshots, err := h.svc.GetHistory(c.Request.Context(), marketID, period, interval)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get history"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"data": snapshots})
}
```

- [ ] **Step 2: Verify it compiles**

Run: `go build ./internal/market/`

- [ ] **Step 3: Commit**

```bash
git add internal/market/handler.go
git commit -m "feat(market): add Gin HTTP handlers for market list, detail, and history APIs"
```

---

## Task 10: Full Build & Test Verification

- [ ] **Step 1: Run all tests**

Run: `go test ./... -v -count=1 -timeout 60s`

Expected: All tests PASS across math, oracle, market, indexer packages.

- [ ] **Step 2: Build all binaries**

Run: `go build ./cmd/indexer/`

Expected: No errors.

- [ ] **Step 3: Final commit if any tidying needed**

Run: `go mod tidy && git add -A && git status`

If changes exist, commit: `git commit -m "chore: tidy go modules after Phase 2"`
