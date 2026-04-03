package consumer

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
	ctypes "github.com/zhangjinge/defi-lending-backend/internal/common/types"
)

type mockStore struct {
	events []*ctypes.ChainEvent
	blocks []ctypes.BlockHeader
	syncs  map[ctypes.ChainID]uint64
}

func newMockStore() *mockStore {
	return &mockStore{
		syncs: make(map[ctypes.ChainID]uint64),
	}
}

func (m *mockStore) SaveEvent(_ context.Context, event *ctypes.ChainEvent) error {
	m.events = append(m.events, event)
	return nil
}

func (m *mockStore) SaveBlock(_ context.Context, header ctypes.BlockHeader) error {
	m.blocks = append(m.blocks, header)
	return nil
}

func (m *mockStore) UpdateSyncStatus(_ context.Context, chainID ctypes.ChainID, block uint64) error {
	m.syncs[chainID] = block
	return nil
}

func (m *mockStore) DeleteEventsFromBlock(_ context.Context, chainID ctypes.ChainID, fromBlock uint64) error {
	filtered := make([]*ctypes.ChainEvent, 0)
	for _, e := range m.events {
		if e.ChainID != chainID || e.BlockNumber < fromBlock {
			filtered = append(filtered, e)
		}
	}
	m.events = filtered
	return nil
}

func (m *mockStore) DeleteBlocksFromBlock(_ context.Context, chainID ctypes.ChainID, fromBlock uint64) error {
	filtered := make([]ctypes.BlockHeader, 0)
	for _, b := range m.blocks {
		if b.ChainID != chainID || b.Number < fromBlock {
			filtered = append(filtered, b)
		}
	}
	m.blocks = filtered
	return nil
}

func (m *mockStore) GetSyncStatus(_ context.Context, chainID ctypes.ChainID) (uint64, error) {
	return m.syncs[chainID], nil
}

func TestEventConsumer_ProcessEvent(t *testing.T) {
	store := newMockStore()
	consumer := NewEventConsumer(store)

	event := &ctypes.ChainEvent{
		ChainID:         1,
		BlockNumber:     100,
		TxHash:          common.HexToHash("0xabc"),
		LogIndex:        0,
		EventType:       ctypes.EventDeposit,
		ContractAddress: common.HexToAddress("0x1234"),
		UserAddress:     common.HexToAddress("0x5678"),
		Amount:          big.NewInt(1e18),
		Timestamp:       time.Now(),
	}

	err := consumer.ProcessEvent(context.Background(), event)
	assert.NoError(t, err)
	assert.Len(t, store.events, 1)
	assert.Equal(t, ctypes.EventDeposit, store.events[0].EventType)
}

func TestEventConsumer_HandleReorg(t *testing.T) {
	store := newMockStore()
	consumer := NewEventConsumer(store)

	for _, bn := range []uint64{100, 101, 102} {
		_ = consumer.ProcessEvent(context.Background(), &ctypes.ChainEvent{
			ChainID:     1,
			BlockNumber: bn,
			EventType:   ctypes.EventDeposit,
			Amount:      big.NewInt(1),
		})
	}
	assert.Len(t, store.events, 3)

	err := consumer.HandleReorg(context.Background(), ctypes.ChainID(1), 101)
	assert.NoError(t, err)
	assert.Len(t, store.events, 1)
	assert.Equal(t, uint64(100), store.events[0].BlockNumber)
}
