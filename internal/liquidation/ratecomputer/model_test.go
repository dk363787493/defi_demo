package ratecomputer

import (
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	dmath "github.com/zhangjinge/defi-lending-backend/internal/common/math"
)

func rayPercent(pct int64) *big.Int {
	// e.g., 80% = 0.8 RAY = 8 * 10^26
	return new(big.Int).Mul(big.NewInt(pct), new(big.Int).Exp(big.NewInt(10), big.NewInt(25), nil))
}

func newTestModel() InterestRateModelParams {
	return InterestRateModelParams{
		BaseRate:           rayPercent(2),  // 2%
		Slope1:             rayPercent(4),  // 4%
		Slope2:             rayPercent(75), // 75%
		OptimalUtilization: rayPercent(80), // 80%
	}
}

func TestCalculateUtilizationRate(t *testing.T) {
	supply := new(big.Int).Mul(big.NewInt(800), dmath.WAD)  // 800 tokens
	borrow := new(big.Int).Mul(big.NewInt(200), dmath.WAD)  // 200 tokens
	util := CalculateUtilizationRate(supply, borrow)

	// Expected: 200 / (800+200) = 0.2 = 20% = 2*10^26 RAY
	expected := rayPercent(20)
	assert.Equal(t, expected.String(), util.String())
}

func TestCalculateUtilizationRate_Zero(t *testing.T) {
	util := CalculateUtilizationRate(big.NewInt(0), big.NewInt(0))
	assert.Equal(t, "0", util.String())
}

func TestCalculateBorrowRate_BelowOptimal(t *testing.T) {
	model := newTestModel()
	// 40% utilization (half of 80% optimal)
	util := rayPercent(40)
	rate := CalculateBorrowRate(util, model)

	// Expected: baseRate + (40/80) * slope1 = 2% + 0.5 * 4% = 4%
	expected := rayPercent(4)
	assert.Equal(t, expected.String(), rate.String())
}

func TestCalculateBorrowRate_AtOptimal(t *testing.T) {
	model := newTestModel()
	util := rayPercent(80)
	rate := CalculateBorrowRate(util, model)

	// Expected: baseRate + slope1 = 2% + 4% = 6%
	expected := rayPercent(6)
	assert.Equal(t, expected.String(), rate.String())
}

func TestCalculateBorrowRate_AboveOptimal(t *testing.T) {
	model := newTestModel()
	// 90% utilization
	util := rayPercent(90)
	rate := CalculateBorrowRate(util, model)

	// Expected: baseRate + slope1 + ((90-80)/(100-80)) * slope2
	//         = 2% + 4% + (10/20) * 75% = 2% + 4% + 37.5% = 43.5%
	expected := rayPercent(6) // base + slope1
	excessRatio := dmath.RayDiv(rayPercent(10), rayPercent(20))
	excessRate := dmath.RayMul(excessRatio, rayPercent(75))
	expected.Add(expected, excessRate)
	assert.Equal(t, expected.String(), rate.String())
}

func TestExtrapolateIndex(t *testing.T) {
	// Start with 1.0 RAY index, 5% annual rate, 1 year later
	lastIndex := new(big.Int).Set(dmath.RAY)
	rate := rayPercent(5)
	lastTs := int64(1000)
	now := time.Unix(lastTs+365*24*3600, 0) // 1 year later

	newIndex := ExtrapolateIndex(lastIndex, rate, lastTs, now)

	// Linear: 1.0 * (1 + 0.05) = 1.05 RAY
	expected := new(big.Int).Add(dmath.RAY, rayPercent(5))
	assert.Equal(t, expected.String(), newIndex.String())
}

func TestExtrapolateIndex_ZeroDelta(t *testing.T) {
	lastIndex := new(big.Int).Set(dmath.RAY)
	rate := rayPercent(5)
	lastTs := int64(1000)
	now := time.Unix(lastTs, 0)

	newIndex := ExtrapolateIndex(lastIndex, rate, lastTs, now)
	assert.Equal(t, lastIndex.String(), newIndex.String())
}

func TestRateComputer_CalibratePool(t *testing.T) {
	rc := New()
	model := newTestModel()

	pool := &PoolState{
		MarketID:           1,
		TotalSupply:        new(big.Int).Mul(big.NewInt(1000), dmath.WAD),
		TotalBorrow:        new(big.Int).Mul(big.NewInt(500), dmath.WAD),
		LiquidityIndex:     new(big.Int).Set(dmath.RAY),
		BorrowIndex:        new(big.Int).Set(dmath.RAY),
		CurrentBorrowRate:  rayPercent(5),
		CurrentSupplyRate:  rayPercent(2),
		LastUpdateTimestamp: 1000,
		Model:              model,
	}
	rc.SetPool(pool)

	// Calibrate with slightly different chain values
	chainBorrowIndex := new(big.Int).Set(dmath.RAY)
	chainBorrowIndex.Add(chainBorrowIndex, big.NewInt(1e18)) // tiny difference
	chainLiqIndex := new(big.Int).Set(dmath.RAY)

	deviation := rc.CalibratePool(1, chainBorrowIndex, chainLiqIndex, 2000)
	require.True(t, deviation >= 0)

	// Verify pool was updated
	p, ok := rc.GetPool(1)
	require.True(t, ok)
	assert.Equal(t, chainBorrowIndex.String(), p.BorrowIndex.String())
}

func TestRateComputer_HandleEvent(t *testing.T) {
	rc := New()
	model := newTestModel()

	pool := &PoolState{
		MarketID:           1,
		TotalSupply:        new(big.Int).Mul(big.NewInt(1000), dmath.WAD),
		TotalBorrow:        new(big.Int).Mul(big.NewInt(500), dmath.WAD),
		LiquidityIndex:     new(big.Int).Set(dmath.RAY),
		BorrowIndex:        new(big.Int).Set(dmath.RAY),
		CurrentBorrowRate:  rayPercent(5),
		CurrentSupplyRate:  rayPercent(2),
		LastUpdateTimestamp: 1000,
		Model:              model,
	}
	rc.SetPool(pool)

	// Deposit 200 tokens
	depositDelta := new(big.Int).Mul(big.NewInt(200), dmath.WAD)
	rc.HandleEvent(1, depositDelta, nil, 2000)

	p, ok := rc.GetPool(1)
	require.True(t, ok)
	// Supply should now be 1200
	expectedSupply := new(big.Int).Mul(big.NewInt(1200), dmath.WAD)
	assert.Equal(t, expectedSupply.String(), p.TotalSupply.String())
	// Rates should have been recalculated (lower utilization = lower rate)
	assert.True(t, p.CurrentBorrowRate.Sign() > 0)
}
