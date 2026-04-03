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
