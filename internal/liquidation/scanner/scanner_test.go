package scanner

import (
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	dmath "github.com/zhangjinge/defi-lending-backend/internal/common/math"
)

// mockIndexProvider returns RAY (1.0) for all indices — no interest accrual.
type mockIndexProvider struct{}

func (m *mockIndexProvider) CurrentBorrowIndex(_ int64, _ time.Time) (*big.Int, bool) {
	return new(big.Int).Set(dmath.RAY), true
}

func (m *mockIndexProvider) CurrentLiquidityIndex(_ int64, _ time.Time) (*big.Int, bool) {
	return new(big.Int).Set(dmath.RAY), true
}

func makePosition(user string, collateralUSD, debtUSD int64, liqThreshold int64) *UserPosition {
	// collateralUSD and debtUSD in whole dollars, price = 1 USD (1e8 in 8-decimal)
	price := big.NewInt(1e8)
	return &UserPosition{
		UserAddress: common.HexToAddress(user),
		Collaterals: []CollateralEntry{{
			MarketID:             1,
			SupplyBalance:        new(big.Int).Mul(big.NewInt(collateralUSD), dmath.WAD),
			SupplyIndex:          new(big.Int).Set(dmath.RAY),
			LiquidationThreshold: new(big.Int).Mul(big.NewInt(liqThreshold), new(big.Int).Exp(big.NewInt(10), big.NewInt(25), nil)), // percent as Ray
			PriceUSD:             price,
			Decimals:             18,
		}},
		Debts: []DebtEntry{{
			MarketID:      2,
			BorrowBalance: new(big.Int).Mul(big.NewInt(debtUSD), dmath.WAD),
			BorrowIndex:   new(big.Int).Set(dmath.RAY),
			PriceUSD:      price,
			Decimals:      18,
		}},
	}
}

func TestCalculateHealthFactor_Healthy(t *testing.T) {
	// $1000 collateral, $500 debt, 80% liq threshold
	// HF = (1000 * 0.80) / 500 = 1.6
	pos := makePosition("0x01", 1000, 500, 80)
	indexProvider := &mockIndexProvider{}

	hf := CalculateHealthFactor(pos, indexProvider, time.Now())
	require.NotNil(t, hf)

	// 1.6 RAY
	expectedHF := new(big.Int).Mul(big.NewInt(16), new(big.Int).Exp(big.NewInt(10), big.NewInt(26), nil))
	assert.Equal(t, expectedHF.String(), hf.String())
}

func TestCalculateHealthFactor_Liquidatable(t *testing.T) {
	// $1000 collateral, $900 debt, 80% liq threshold
	// HF = (1000 * 0.80) / 900 = 0.888...
	pos := makePosition("0x02", 1000, 900, 80)
	indexProvider := &mockIndexProvider{}

	hf := CalculateHealthFactor(pos, indexProvider, time.Now())
	require.NotNil(t, hf)

	// HF should be < 1.0 RAY
	assert.True(t, hf.Cmp(dmath.RAY) < 0, "HF should be < 1.0, got %s", hf.String())
}

func TestCalculateHealthFactor_NoDebt(t *testing.T) {
	pos := &UserPosition{
		UserAddress: common.HexToAddress("0x03"),
		Collaterals: []CollateralEntry{{
			MarketID:             1,
			SupplyBalance:        new(big.Int).Mul(big.NewInt(1000), dmath.WAD),
			SupplyIndex:          new(big.Int).Set(dmath.RAY),
			LiquidationThreshold: new(big.Int).Set(dmath.RAY),
			PriceUSD:             big.NewInt(1e8),
			Decimals:             18,
		}},
		Debts: nil,
	}
	indexProvider := &mockIndexProvider{}

	hf := CalculateHealthFactor(pos, indexProvider, time.Now())
	// No debt = max HF (1000 RAY)
	maxHF := new(big.Int).Mul(big.NewInt(1000), dmath.RAY)
	assert.Equal(t, maxHF.String(), hf.String())
}

func TestScanner_FindLiquidatable(t *testing.T) {
	s := New()
	indexProvider := &mockIndexProvider{}

	// Healthy position
	healthy := makePosition("0x01", 1000, 500, 80)
	healthy.HealthFactor = CalculateHealthFactor(healthy, indexProvider, time.Now())
	s.SetPosition(healthy)

	// Liquidatable position
	risky := makePosition("0x02", 1000, 900, 80)
	risky.HealthFactor = CalculateHealthFactor(risky, indexProvider, time.Now())
	s.SetPosition(risky)

	liquidatable := s.FindLiquidatable()
	assert.Len(t, liquidatable, 1)
	assert.Equal(t, common.HexToAddress("0x02"), liquidatable[0].UserAddress)
}

func TestScanner_MostDangerous(t *testing.T) {
	s := New()
	indexProvider := &mockIndexProvider{}

	pos1 := makePosition("0x01", 1000, 500, 80) // HF = 1.6
	pos1.HealthFactor = CalculateHealthFactor(pos1, indexProvider, time.Now())
	s.SetPosition(pos1)

	pos2 := makePosition("0x02", 1000, 900, 80) // HF ≈ 0.89
	pos2.HealthFactor = CalculateHealthFactor(pos2, indexProvider, time.Now())
	s.SetPosition(pos2)

	pos3 := makePosition("0x03", 1000, 700, 80) // HF ≈ 1.14
	pos3.HealthFactor = CalculateHealthFactor(pos3, indexProvider, time.Now())
	s.SetPosition(pos3)

	most := s.MostDangerous()
	require.NotNil(t, most)
	assert.Equal(t, common.HexToAddress("0x02"), most.UserAddress)
	assert.Equal(t, 3, s.PositionCount())
}

func TestScanner_RemovePosition(t *testing.T) {
	s := New()
	pos := makePosition("0x01", 1000, 500, 80)
	pos.HealthFactor = new(big.Int).Set(dmath.RAY)
	s.SetPosition(pos)

	assert.Equal(t, 1, s.PositionCount())
	s.RemovePosition(common.HexToAddress("0x01"))
	assert.Equal(t, 0, s.PositionCount())
}
