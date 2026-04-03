package account

import (
	"context"
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockAccountDB struct {
	positions []PositionSummary
	history   []TxHistoryEntry
}

func (m *mockAccountDB) GetPositions(_ context.Context, _ string, _ int64) ([]PositionSummary, error) {
	return m.positions, nil
}

func (m *mockAccountDB) GetTxHistory(_ context.Context, _ string, _ int64, limit int, offset int) ([]TxHistoryEntry, error) {
	end := offset + limit
	if end > len(m.history) {
		end = len(m.history)
	}
	if offset >= len(m.history) {
		return nil, nil
	}
	return m.history[offset:end], nil
}

func TestService_GetOverview(t *testing.T) {
	db := &mockAccountDB{
		positions: []PositionSummary{
			{MarketID: 1, AssetSymbol: "ETH", SupplyValueUSD: "200000000000", BorrowValueUSD: "0"},
			{MarketID: 2, AssetSymbol: "USDC", SupplyValueUSD: "0", BorrowValueUSD: "100000000000"},
		},
	}

	svc := NewService(db)
	overview, err := svc.GetOverview(context.Background(), "0xuser", 1)
	require.NoError(t, err)

	assert.Equal(t, "0xuser", overview.Address)
	assert.Equal(t, "200000000000", overview.TotalSupplyUSD)
	assert.Equal(t, "100000000000", overview.TotalBorrowUSD)
	assert.Equal(t, "100000000000", overview.NetWorthUSD)
	assert.NotEqual(t, "∞", overview.HealthFactor)
	assert.Len(t, overview.Positions, 2)
}

func TestService_GetOverview_NoBorrow(t *testing.T) {
	db := &mockAccountDB{
		positions: []PositionSummary{
			{MarketID: 1, SupplyValueUSD: "1000", BorrowValueUSD: "0"},
		},
	}

	svc := NewService(db)
	overview, err := svc.GetOverview(context.Background(), "0xuser", 0)
	require.NoError(t, err)
	assert.Equal(t, "∞", overview.HealthFactor)
}

func TestService_GetHistory(t *testing.T) {
	db := &mockAccountDB{
		history: make([]TxHistoryEntry, 50),
	}
	for i := range db.history {
		db.history[i] = TxHistoryEntry{EventType: fmt.Sprintf("event-%d", i)}
	}

	svc := NewService(db)

	// Default limit
	entries, err := svc.GetHistory(context.Background(), "0xuser", 1, 0, 0)
	require.NoError(t, err)
	assert.Len(t, entries, 20) // default limit is 20

	// Explicit limit
	entries, err = svc.GetHistory(context.Background(), "0xuser", 1, 5, 0)
	require.NoError(t, err)
	assert.Len(t, entries, 5)
}

func TestService_BuildTransaction(t *testing.T) {
	svc := NewService(nil)
	pool := common.HexToAddress("0xPool")

	resp, err := svc.BuildTransaction(TxBuildRequest{
		ChainID: 1,
		Action:  "deposit",
		Asset:   "0x1111111111111111111111111111111111111111",
		Amount:  "1000000000000000000",
	}, pool)
	require.NoError(t, err)
	assert.Equal(t, pool.Hex(), resp.To)
	assert.NotEmpty(t, resp.Data)
	assert.Equal(t, "0", resp.Value)
}

func TestService_BuildTransaction_InvalidAction(t *testing.T) {
	svc := NewService(nil)
	_, err := svc.BuildTransaction(TxBuildRequest{
		ChainID: 1,
		Action:  "invalid",
		Asset:   "0x1111111111111111111111111111111111111111",
		Amount:  "1000",
	}, common.HexToAddress("0xPool"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown action")
}
