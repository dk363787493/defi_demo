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
	PriceUSD     *big.Int       `json:"price_usd"`
	Decimals     uint8          `json:"decimals"`
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
	HeartbeatSec int64          `json:"heartbeat_sec"`
	DeviationPct float64        `json:"deviation_pct"`
}
