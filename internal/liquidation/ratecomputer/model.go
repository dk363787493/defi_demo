package ratecomputer

import (
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	dmath "github.com/zhangjinge/defi-lending-backend/internal/common/math"
)

// InterestRateModelParams holds the parameters for a jump-rate interest model.
type InterestRateModelParams struct {
	BaseRate           *big.Int // Ray-scaled base borrow rate
	Slope1             *big.Int // Ray-scaled slope below optimal utilization
	Slope2             *big.Int // Ray-scaled slope above optimal utilization
	OptimalUtilization *big.Int // Ray-scaled optimal utilization rate (e.g., 0.8 = 8e26)
}

// PoolState holds the current state of a lending pool for local rate computation.
type PoolState struct {
	MarketID           int64
	AssetAddress       common.Address
	TotalSupply        *big.Int // Wad-scaled
	TotalBorrow        *big.Int // Wad-scaled
	LiquidityIndex     *big.Int // Ray-scaled
	BorrowIndex        *big.Int // Ray-scaled
	CurrentBorrowRate  *big.Int // Ray-scaled per-second rate
	CurrentSupplyRate  *big.Int // Ray-scaled per-second rate
	LastUpdateTimestamp int64    // Unix seconds
	Model              InterestRateModelParams
}

// RateComputer locally replicates on-chain interest rate calculations.
type RateComputer struct {
	mu    sync.RWMutex
	pools map[int64]*PoolState // keyed by MarketID
}

func New() *RateComputer {
	return &RateComputer{
		pools: make(map[int64]*PoolState),
	}
}

// SetPool initializes or updates a pool's state from on-chain data.
func (rc *RateComputer) SetPool(state *PoolState) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.pools[state.MarketID] = state
}

// GetPool returns a copy of the pool state.
func (rc *RateComputer) GetPool(marketID int64) (*PoolState, bool) {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	p, ok := rc.pools[marketID]
	if !ok {
		return nil, false
	}
	// Return a shallow copy to avoid races.
	cp := *p
	return &cp, true
}

// CalculateUtilizationRate computes: totalBorrow / (totalSupply + totalBorrow).
// Returns a Ray-scaled value.
func CalculateUtilizationRate(totalSupply, totalBorrow *big.Int) *big.Int {
	total := new(big.Int).Add(totalSupply, totalBorrow)
	if total.Sign() == 0 {
		return new(big.Int)
	}
	// (totalBorrow * RAY) / total
	return dmath.RayDiv(
		dmath.WadToRay(totalBorrow),
		dmath.WadToRay(total),
	)
}

// CalculateBorrowRate computes the borrow rate from the jump-rate model.
// If utilization <= optimal: baseRate + (utilization / optimal) * slope1
// If utilization > optimal:  baseRate + slope1 + ((utilization - optimal) / (1 - optimal)) * slope2
func CalculateBorrowRate(utilization *big.Int, params InterestRateModelParams) *big.Int {
	if utilization.Cmp(params.OptimalUtilization) <= 0 {
		// Normal range
		if params.OptimalUtilization.Sign() == 0 {
			return new(big.Int).Set(params.BaseRate)
		}
		ratio := dmath.RayDiv(utilization, params.OptimalUtilization)
		variableRate := dmath.RayMul(ratio, params.Slope1)
		return new(big.Int).Add(params.BaseRate, variableRate)
	}

	// Excess range
	excessUtil := new(big.Int).Sub(utilization, params.OptimalUtilization)
	remaining := new(big.Int).Sub(dmath.RAY, params.OptimalUtilization)
	if remaining.Sign() == 0 {
		remaining = new(big.Int).Set(dmath.RAY)
	}
	excessRatio := dmath.RayDiv(excessUtil, remaining)
	excessRate := dmath.RayMul(excessRatio, params.Slope2)

	result := new(big.Int).Add(params.BaseRate, params.Slope1)
	result.Add(result, excessRate)
	return result
}

// CalculateSupplyRate computes supply rate: borrowRate * utilization * (1 - reserveFactor) / RAY.
// For simplicity, reserveFactor = 0 in this implementation.
func CalculateSupplyRate(borrowRate, utilization *big.Int) *big.Int {
	return dmath.RayMul(borrowRate, utilization)
}

// ExtrapolateIndex extrapolates a Ray-scaled index forward from lastTimestamp to now.
// newIndex = lastIndex * (1 + rate * timeDelta / SECONDS_PER_YEAR)
func ExtrapolateIndex(lastIndex *big.Int, ratePerSecond *big.Int, lastTimestamp int64, now time.Time) *big.Int {
	timeDelta := now.Unix() - lastTimestamp
	if timeDelta <= 0 {
		return new(big.Int).Set(lastIndex)
	}

	interestMultiplier := dmath.CalculateLinearInterest(ratePerSecond, uint64(timeDelta))
	return dmath.RayMul(lastIndex, interestMultiplier)
}

// CurrentBorrowIndex returns the extrapolated borrow index for a market at the given time.
func (rc *RateComputer) CurrentBorrowIndex(marketID int64, now time.Time) (*big.Int, bool) {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	pool, ok := rc.pools[marketID]
	if !ok {
		return nil, false
	}

	return ExtrapolateIndex(pool.BorrowIndex, pool.CurrentBorrowRate, pool.LastUpdateTimestamp, now), true
}

// CurrentLiquidityIndex returns the extrapolated liquidity index for a market at the given time.
func (rc *RateComputer) CurrentLiquidityIndex(marketID int64, now time.Time) (*big.Int, bool) {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	pool, ok := rc.pools[marketID]
	if !ok {
		return nil, false
	}

	return ExtrapolateIndex(pool.LiquidityIndex, pool.CurrentSupplyRate, pool.LastUpdateTimestamp, now), true
}

// CalibratePool updates a pool's state from on-chain data and returns the deviation.
// Deviation = |localIndex - chainIndex| / chainIndex * 100 (percentage).
func (rc *RateComputer) CalibratePool(marketID int64, chainBorrowIndex *big.Int, chainLiquidityIndex *big.Int, chainTimestamp int64) float64 {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	pool, ok := rc.pools[marketID]
	if !ok {
		return 0
	}

	// Calculate deviation before updating.
	localBorrowIndex := ExtrapolateIndex(pool.BorrowIndex, pool.CurrentBorrowRate, pool.LastUpdateTimestamp, time.Unix(chainTimestamp, 0))

	var deviation float64
	if chainBorrowIndex.Sign() > 0 {
		diff := new(big.Int).Sub(localBorrowIndex, chainBorrowIndex)
		diff.Abs(diff)
		diffF := new(big.Float).SetInt(diff)
		chainF := new(big.Float).SetInt(chainBorrowIndex)
		ratio := new(big.Float).Quo(diffF, chainF)
		deviation, _ = ratio.Float64()
		deviation *= 100
	}

	// Apply correction.
	pool.BorrowIndex = new(big.Int).Set(chainBorrowIndex)
	pool.LiquidityIndex = new(big.Int).Set(chainLiquidityIndex)
	pool.LastUpdateTimestamp = chainTimestamp

	// Recalculate rates.
	utilization := CalculateUtilizationRate(pool.TotalSupply, pool.TotalBorrow)
	pool.CurrentBorrowRate = CalculateBorrowRate(utilization, pool.Model)
	pool.CurrentSupplyRate = CalculateSupplyRate(pool.CurrentBorrowRate, utilization)

	return deviation
}

// HandleEvent updates pool state when a deposit/withdraw/borrow/repay occurs.
func (rc *RateComputer) HandleEvent(marketID int64, supplyDelta, borrowDelta *big.Int, chainTimestamp int64) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	pool, ok := rc.pools[marketID]
	if !ok {
		return
	}

	// First, accrue interest up to the event timestamp.
	now := time.Unix(chainTimestamp, 0)
	pool.BorrowIndex = ExtrapolateIndex(pool.BorrowIndex, pool.CurrentBorrowRate, pool.LastUpdateTimestamp, now)
	pool.LiquidityIndex = ExtrapolateIndex(pool.LiquidityIndex, pool.CurrentSupplyRate, pool.LastUpdateTimestamp, now)
	pool.LastUpdateTimestamp = chainTimestamp

	// Apply deltas.
	if supplyDelta != nil {
		pool.TotalSupply = new(big.Int).Add(pool.TotalSupply, supplyDelta)
		if pool.TotalSupply.Sign() < 0 {
			pool.TotalSupply = new(big.Int)
		}
	}
	if borrowDelta != nil {
		pool.TotalBorrow = new(big.Int).Add(pool.TotalBorrow, borrowDelta)
		if pool.TotalBorrow.Sign() < 0 {
			pool.TotalBorrow = new(big.Int)
		}
	}

	// Recalculate rates.
	utilization := CalculateUtilizationRate(pool.TotalSupply, pool.TotalBorrow)
	pool.CurrentBorrowRate = CalculateBorrowRate(utilization, pool.Model)
	pool.CurrentSupplyRate = CalculateSupplyRate(pool.CurrentBorrowRate, utilization)
}
