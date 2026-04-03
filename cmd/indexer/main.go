package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/zhangjinge/defi-lending-backend/internal/chain"
	"github.com/zhangjinge/defi-lending-backend/internal/chain/evm"
	"github.com/zhangjinge/defi-lending-backend/internal/common/config"
	"github.com/zhangjinge/defi-lending-backend/internal/common/logger"
	ctypes "github.com/zhangjinge/defi-lending-backend/internal/common/types"
	"github.com/zhangjinge/defi-lending-backend/internal/indexer"
	"github.com/zhangjinge/defi-lending-backend/internal/infra/messaging"
)

func main() {
	configPath := flag.String("config", "configs/config.yaml", "path to config file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		panic("failed to load config: " + err.Error())
	}

	log := logger.New(cfg.Log.Level, cfg.Log.Format)
	log.Info().Msg("starting defi-lending indexer")

	// Build chain adapters.
	adapters := make(map[ctypes.ChainID]chain.ChainAdapter)
	for _, chainCfg := range cfg.Chains {
		client, err := evm.NewClient(chainCfg)
		if err != nil {
			log.Fatal().Err(err).Str("chain", chainCfg.Name).Msg("failed to create chain client")
		}
		adapters[ctypes.ChainID(chainCfg.ChainID)] = client
	}
	defer func() {
		for _, a := range adapters {
			a.Close()
		}
	}()

	// Kafka producer for chain events.
	producer := messaging.NewProducer(cfg.Kafka, cfg.Kafka.Topics.ChainEvents)
	defer producer.Close()

	// Placeholder in-memory store for Phase 1.
	// Phase 2+ will replace with real PostgreSQL store.
	store := newInMemoryStore()

	svc, err := indexer.NewService(adapters, cfg.Chains, store, producer, log)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create indexer service")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Graceful shutdown on SIGINT/SIGTERM.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Info().Str("signal", sig.String()).Msg("shutting down indexer")
		cancel()
	}()

	if err := svc.Run(ctx, store); err != nil && err != context.Canceled {
		log.Fatal().Err(err).Msg("indexer exited with error")
	}
	log.Info().Msg("indexer stopped")
}

// inMemoryStore is a temporary placeholder implementing consumer.EventStore.
type inMemoryStore struct {
	syncStatus map[ctypes.ChainID]uint64
}

func newInMemoryStore() *inMemoryStore {
	return &inMemoryStore{syncStatus: make(map[ctypes.ChainID]uint64)}
}

func (s *inMemoryStore) SaveEvent(_ context.Context, _ *ctypes.ChainEvent) error   { return nil }
func (s *inMemoryStore) SaveBlock(_ context.Context, _ ctypes.BlockHeader) error    { return nil }
func (s *inMemoryStore) UpdateSyncStatus(_ context.Context, chainID ctypes.ChainID, block uint64) error {
	s.syncStatus[chainID] = block
	return nil
}
func (s *inMemoryStore) DeleteEventsFromBlock(_ context.Context, _ ctypes.ChainID, _ uint64) error {
	return nil
}
func (s *inMemoryStore) DeleteBlocksFromBlock(_ context.Context, _ ctypes.ChainID, _ uint64) error {
	return nil
}
func (s *inMemoryStore) GetSyncStatus(_ context.Context, chainID ctypes.ChainID) (uint64, error) {
	return s.syncStatus[chainID], nil
}
