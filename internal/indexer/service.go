package indexer

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/rs/zerolog"
	"github.com/zhangjinge/defi-lending-backend/internal/chain"
	"github.com/zhangjinge/defi-lending-backend/internal/common/config"
	ctypes "github.com/zhangjinge/defi-lending-backend/internal/common/types"
	"github.com/zhangjinge/defi-lending-backend/internal/indexer/consumer"
	"github.com/zhangjinge/defi-lending-backend/internal/indexer/listener"
	"github.com/zhangjinge/defi-lending-backend/internal/indexer/parser"
	"github.com/zhangjinge/defi-lending-backend/internal/indexer/reorg"
	"github.com/zhangjinge/defi-lending-backend/internal/infra/messaging"
)

// Service orchestrates multi-chain indexing.
type Service struct {
	chains   map[ctypes.ChainID]chainIndexer
	consumer *consumer.EventConsumer
	producer *messaging.Producer
	parser   *parser.EventParser
	logger   zerolog.Logger
}

type chainIndexer struct {
	adapter  chain.ChainAdapter
	listener *listener.BlockListener
	detector *reorg.Detector
	cfg      config.ChainConfig
}

func NewService(
	adapters map[ctypes.ChainID]chain.ChainAdapter,
	chainConfigs []config.ChainConfig,
	store consumer.EventStore,
	producer *messaging.Producer,
	logger zerolog.Logger,
) (*Service, error) {
	ep, err := parser.NewEventParser()
	if err != nil {
		return nil, fmt.Errorf("create event parser: %w", err)
	}

	chains := make(map[ctypes.ChainID]chainIndexer)
	for _, cfg := range chainConfigs {
		cid := ctypes.ChainID(cfg.ChainID)
		adapter, ok := adapters[cid]
		if !ok {
			return nil, fmt.Errorf("no adapter for chain %d", cfg.ChainID)
		}
		pollInterval := time.Duration(cfg.BlockTime) * time.Second / 2
		if pollInterval < time.Second {
			pollInterval = time.Second
		}
		chains[cid] = chainIndexer{
			adapter:  adapter,
			listener: listener.NewBlockListener(adapter, pollInterval),
			detector: reorg.NewDetector(cfg.Confirmations * 2),
			cfg:      cfg,
		}
	}

	return &Service{
		chains:   chains,
		consumer: consumer.NewEventConsumer(store),
		producer: producer,
		parser:   ep,
		logger:   logger,
	}, nil
}

// Run starts indexing all configured chains. Blocks until ctx is cancelled.
func (s *Service) Run(ctx context.Context, store consumer.EventStore) error {
	var wg sync.WaitGroup

	for cid, ci := range s.chains {
		wg.Add(1)
		go func(chainID ctypes.ChainID, ci chainIndexer) {
			defer wg.Done()
			s.runChain(ctx, chainID, ci, store)
		}(cid, ci)
	}

	wg.Wait()
	return ctx.Err()
}

func (s *Service) runChain(ctx context.Context, chainID ctypes.ChainID, ci chainIndexer, store consumer.EventStore) {
	log := s.logger.With().Int64("chain_id", int64(chainID)).Logger()
	log.Info().Msg("starting chain indexer")

	lastBlock, err := store.GetSyncStatus(ctx, chainID)
	if err != nil {
		log.Error().Err(err).Msg("failed to get sync status, starting from 0")
	}
	startBlock := lastBlock + 1

	headerCh := ci.listener.Subscribe(ctx, startBlock)

	for header := range headerCh {
		if header == nil {
			continue
		}

		blockNum := header.Number.Uint64()
		blockHeader := ctypes.BlockHeader{
			ChainID:    chainID,
			Number:     blockNum,
			Hash:       header.Hash(),
			ParentHash: header.ParentHash,
			Timestamp:  time.Unix(int64(header.Time), 0),
		}

		// Check for reorg.
		if forkBlock, isReorg := ci.detector.DetectReorg(blockHeader); isReorg {
			log.Warn().Uint64("fork_block", forkBlock).Uint64("current_block", blockNum).Msg("reorg detected")
			if err := s.consumer.HandleReorg(ctx, chainID, forkBlock); err != nil {
				log.Error().Err(err).Msg("failed to handle reorg")
				continue
			}
			ci.detector.Rollback(forkBlock)
		}

		ci.detector.RecordBlock(blockHeader)

		if err := s.consumer.ProcessBlock(ctx, blockHeader); err != nil {
			log.Error().Err(err).Uint64("block", blockNum).Msg("failed to process block")
			continue
		}

		// Fetch and parse events from this block.
		contractAddr := common.HexToAddress(ci.cfg.Contracts.LendingPool)
		logs, err := ci.adapter.FilterLogs(ctx, ethereum.FilterQuery{
			FromBlock: new(big.Int).SetUint64(blockNum),
			ToBlock:   new(big.Int).SetUint64(blockNum),
			Addresses: []common.Address{contractAddr},
			Topics:    [][]common.Hash{s.parser.AllSignatures()},
		})
		if err != nil {
			log.Error().Err(err).Uint64("block", blockNum).Msg("failed to filter logs")
			continue
		}

		for _, rawLog := range logs {
			event, err := s.parser.ParseLog(chainID, rawLog)
			if err != nil {
				continue
			}
			event.Timestamp = blockHeader.Timestamp

			if err := s.consumer.ProcessEvent(ctx, event); err != nil {
				log.Error().Err(err).Str("tx", rawLog.TxHash.Hex()).Msg("failed to save event")
				continue
			}

			key := fmt.Sprintf("%d-%d", chainID, blockNum)
			if err := s.producer.Publish(ctx, key, event); err != nil {
				log.Error().Err(err).Msg("failed to publish event to kafka")
			}
		}

		log.Debug().Uint64("block", blockNum).Int("events", len(logs)).Msg("block indexed")
	}

	log.Info().Msg("chain indexer stopped")
}
