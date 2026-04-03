package market

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type mockDB struct {
	markets   []MarketInfo
	states    map[int64]*MarketStateInfo
	snapshots []MarketSnapshot
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

	markets, err := repo.ListMarkets(context.Background(), 1)
	assert.NoError(t, err)
	assert.Len(t, markets, 2)

	allMarkets, err := repo.ListMarkets(context.Background(), 0)
	assert.NoError(t, err)
	assert.Len(t, allMarkets, 3)
}

func TestGetMarketDetail_NotFound(t *testing.T) {
	db := &mockDB{states: make(map[int64]*MarketStateInfo)}
	repo := NewRepository(db)

	_, err := repo.GetMarketDetail(context.Background(), 999)
	assert.Error(t, err)
}
