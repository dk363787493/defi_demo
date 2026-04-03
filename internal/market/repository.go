package market

import (
	"context"
	"math/big"
	"time"
)

// MarketInfo represents a market listing returned to the API.
type MarketInfo struct {
	ID                   int64  `json:"id"`
	ChainID              int64  `json:"chain_id"`
	AssetAddress         string `json:"asset_address"`
	AssetSymbol          string `json:"asset_symbol"`
	AssetDecimals        int    `json:"asset_decimals"`
	PoolAddress          string `json:"pool_address"`
	CollateralFactor     string `json:"collateral_factor"`
	LiquidationThreshold string `json:"liquidation_threshold"`
	BorrowCap            string `json:"borrow_cap"`
	SupplyCap            string `json:"supply_cap"`
	Status               string `json:"status"`
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

// Repository provides market data access.
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
	return ratePerSec * secondsPerYear * 100
}
