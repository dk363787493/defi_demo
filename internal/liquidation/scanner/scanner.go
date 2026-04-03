package scanner

import (
	"container/heap"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	dmath "github.com/zhangjinge/defi-lending-backend/internal/common/math"
)

// UserPosition holds a user's aggregated position across markets.
type UserPosition struct {
	UserAddress       common.Address
	ChainID           int64
	Collaterals       []CollateralEntry // all supply positions used as collateral
	Debts             []DebtEntry       // all borrow positions
	HealthFactor      *big.Int          // Ray-scaled, HF < RAY means liquidatable
	TotalCollateralUSD *big.Int         // 8-decimal USD value
	TotalDebtUSD      *big.Int          // 8-decimal USD value
	heapIndex         int               // index in the priority queue
}

// CollateralEntry is a single collateral position.
type CollateralEntry struct {
	MarketID             int64
	AssetAddress         common.Address
	SupplyBalance        *big.Int // scaled token amount (principal)
	SupplyIndex          *big.Int // user's index at deposit time
	LiquidationThreshold *big.Int // Ray-scaled (e.g., 0.85 = 85%)
	LiquidationBonus     *big.Int // Wad-scaled (e.g., 1.05e18 = 5% bonus)
	PriceUSD             *big.Int // 8-decimal
	Decimals             int
}

// DebtEntry is a single borrow position.
type DebtEntry struct {
	MarketID      int64
	AssetAddress  common.Address
	BorrowBalance *big.Int // principal
	BorrowIndex   *big.Int // user's index at borrow time
	PriceUSD      *big.Int // 8-decimal
	Decimals      int
}

// LiquidationCandidate is a position ready for liquidation.
type LiquidationCandidate struct {
	User              common.Address
	HealthFactor      *big.Int
	DebtMarketID      int64
	CollateralMarketID int64
	DebtAmount        *big.Int
	CollateralAmount  *big.Int
	ExpectedProfit    *big.Int
}

// PriceProvider returns the current USD price for an asset.
type PriceProvider interface {
	GetPriceUSD(asset common.Address) (*big.Int, uint8, bool)
}

// IndexProvider returns the current extrapolated borrow/liquidity index.
type IndexProvider interface {
	CurrentBorrowIndex(marketID int64, now time.Time) (*big.Int, bool)
	CurrentLiquidityIndex(marketID int64, now time.Time) (*big.Int, bool)
}

// Scanner maintains all active positions and finds liquidatable ones.
type Scanner struct {
	mu        sync.RWMutex
	positions map[common.Address]*UserPosition
	queue     positionHeap
}

func New() *Scanner {
	return &Scanner{
		positions: make(map[common.Address]*UserPosition),
	}
}

// SetPosition adds or replaces a user's position.
func (s *Scanner) SetPosition(pos *UserPosition) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.positions[pos.UserAddress]; ok {
		// Update in-place and fix heap.
		existing.Collaterals = pos.Collaterals
		existing.Debts = pos.Debts
		existing.HealthFactor = pos.HealthFactor
		existing.TotalCollateralUSD = pos.TotalCollateralUSD
		existing.TotalDebtUSD = pos.TotalDebtUSD
		if existing.heapIndex >= 0 {
			heap.Fix(&s.queue, existing.heapIndex)
		}
	} else {
		pos.heapIndex = -1
		s.positions[pos.UserAddress] = pos
		heap.Push(&s.queue, pos)
	}
}

// RemovePosition removes a user's position (e.g., fully repaid).
func (s *Scanner) RemovePosition(user common.Address) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if pos, ok := s.positions[user]; ok {
		if pos.heapIndex >= 0 {
			heap.Remove(&s.queue, pos.heapIndex)
		}
		delete(s.positions, user)
	}
}

// CalculateHealthFactor computes HF for a position given current prices and indices.
// HF = (sum of collateral_value * liquidation_threshold) / total_debt_value
// All in Ray scale. Returns RAY-scaled result.
func CalculateHealthFactor(pos *UserPosition, indexProvider IndexProvider, now time.Time) *big.Int {
	totalWeightedCollateral := new(big.Int)
	totalDebt := new(big.Int)

	for _, c := range pos.Collaterals {
		// Get current liquidity index to compute actual balance.
		currentLiqIdx, ok := indexProvider.CurrentLiquidityIndex(c.MarketID, now)
		if !ok {
			currentLiqIdx = dmath.RAY
		}
		// actualBalance = principal * currentIndex / userIndex
		actualBalance := c.SupplyBalance
		if c.SupplyIndex != nil && c.SupplyIndex.Sign() > 0 {
			actualBalance = dmath.RayMul(
				dmath.WadToRay(c.SupplyBalance),
				dmath.RayDiv(currentLiqIdx, c.SupplyIndex),
			)
			actualBalance = dmath.RayToWad(actualBalance)
		}

		// value = balance * price / 10^decimals (result in 8-decimal USD)
		value := new(big.Int).Mul(actualBalance, c.PriceUSD)
		decimalsFactor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(c.Decimals)), nil)
		value.Div(value, decimalsFactor)

		// weighted = value * liquidationThreshold / RAY
		weighted := dmath.RayMul(dmath.WadToRay(value), c.LiquidationThreshold)
		totalWeightedCollateral.Add(totalWeightedCollateral, weighted)
	}

	for _, d := range pos.Debts {
		currentBorrowIdx, ok := indexProvider.CurrentBorrowIndex(d.MarketID, now)
		if !ok {
			currentBorrowIdx = dmath.RAY
		}
		actualDebt := d.BorrowBalance
		if d.BorrowIndex != nil && d.BorrowIndex.Sign() > 0 {
			actualDebt = dmath.RayMul(
				dmath.WadToRay(d.BorrowBalance),
				dmath.RayDiv(currentBorrowIdx, d.BorrowIndex),
			)
			actualDebt = dmath.RayToWad(actualDebt)
		}

		value := new(big.Int).Mul(actualDebt, d.PriceUSD)
		decimalsFactor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(d.Decimals)), nil)
		value.Div(value, decimalsFactor)

		totalDebt.Add(totalDebt, dmath.WadToRay(value))
	}

	if totalDebt.Sign() == 0 {
		// No debt = infinite health factor, represent as max.
		return new(big.Int).Mul(big.NewInt(1000), dmath.RAY)
	}

	return dmath.RayDiv(totalWeightedCollateral, totalDebt)
}

// RefreshAll recalculates health factors for all positions.
func (s *Scanner) RefreshAll(indexProvider IndexProvider, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, pos := range s.positions {
		pos.HealthFactor = CalculateHealthFactor(pos, indexProvider, now)
		if pos.heapIndex >= 0 {
			heap.Fix(&s.queue, pos.heapIndex)
		}
	}
}

// FindLiquidatable returns all positions with HF < 1.0 RAY, sorted by most dangerous first.
func (s *Scanner) FindLiquidatable() []*UserPosition {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []*UserPosition
	for _, pos := range s.positions {
		if pos.HealthFactor != nil && pos.HealthFactor.Cmp(dmath.RAY) < 0 {
			result = append(result, pos)
		}
	}
	return result
}

// MostDangerous returns the position with the lowest health factor, or nil.
func (s *Scanner) MostDangerous() *UserPosition {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.queue.Len() == 0 {
		return nil
	}
	return s.queue[0]
}

// PositionCount returns the number of tracked positions.
func (s *Scanner) PositionCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.positions)
}

// --- Min-Heap by Health Factor ---

type positionHeap []*UserPosition

func (h positionHeap) Len() int { return len(h) }

func (h positionHeap) Less(i, j int) bool {
	if h[i].HealthFactor == nil {
		return false
	}
	if h[j].HealthFactor == nil {
		return true
	}
	return h[i].HealthFactor.Cmp(h[j].HealthFactor) < 0
}

func (h positionHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIndex = i
	h[j].heapIndex = j
}

func (h *positionHeap) Push(x interface{}) {
	pos := x.(*UserPosition)
	pos.heapIndex = len(*h)
	*h = append(*h, pos)
}

func (h *positionHeap) Pop() interface{} {
	old := *h
	n := len(old)
	pos := old[n-1]
	old[n-1] = nil
	pos.heapIndex = -1
	*h = old[:n-1]
	return pos
}
