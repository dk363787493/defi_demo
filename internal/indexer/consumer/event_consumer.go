package consumer

import (
	"context"

	ctypes "github.com/zhangjinge/defi-lending-backend/internal/common/types"
)

// EventStore defines the persistence interface that the consumer depends on.
type EventStore interface {
	SaveEvent(ctx context.Context, event *ctypes.ChainEvent) error
	SaveBlock(ctx context.Context, header ctypes.BlockHeader) error
	UpdateSyncStatus(ctx context.Context, chainID ctypes.ChainID, block uint64) error
	DeleteEventsFromBlock(ctx context.Context, chainID ctypes.ChainID, fromBlock uint64) error
	DeleteBlocksFromBlock(ctx context.Context, chainID ctypes.ChainID, fromBlock uint64) error
	GetSyncStatus(ctx context.Context, chainID ctypes.ChainID) (uint64, error)
}

// EventConsumer processes decoded chain events and persists them via EventStore.
type EventConsumer struct {
	store EventStore
}

func NewEventConsumer(store EventStore) *EventConsumer {
	return &EventConsumer{store: store}
}

// ProcessEvent saves a single event to the store.
func (ec *EventConsumer) ProcessEvent(ctx context.Context, event *ctypes.ChainEvent) error {
	return ec.store.SaveEvent(ctx, event)
}

// ProcessBlock records a block and updates sync status.
func (ec *EventConsumer) ProcessBlock(ctx context.Context, header ctypes.BlockHeader) error {
	if err := ec.store.SaveBlock(ctx, header); err != nil {
		return err
	}
	return ec.store.UpdateSyncStatus(ctx, header.ChainID, header.Number)
}

// HandleReorg rolls back all events and blocks from the given block number onward.
func (ec *EventConsumer) HandleReorg(ctx context.Context, chainID ctypes.ChainID, fromBlock uint64) error {
	if err := ec.store.DeleteEventsFromBlock(ctx, chainID, fromBlock); err != nil {
		return err
	}
	if err := ec.store.DeleteBlocksFromBlock(ctx, chainID, fromBlock); err != nil {
		return err
	}
	if fromBlock > 0 {
		return ec.store.UpdateSyncStatus(ctx, chainID, fromBlock-1)
	}
	return nil
}
