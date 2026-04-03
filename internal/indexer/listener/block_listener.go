package listener

import (
	"context"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/rs/zerolog/log"
	"github.com/zhangjinge/defi-lending-backend/internal/chain"
)

// BlockListener fetches new blocks from an EVM chain via polling or WebSocket subscription.
type BlockListener struct {
	adapter      chain.ChainAdapter
	pollInterval time.Duration
}

func NewBlockListener(adapter chain.ChainAdapter, pollInterval time.Duration) *BlockListener {
	return &BlockListener{
		adapter:      adapter,
		pollInterval: pollInterval,
	}
}

// Poll starts polling for new block headers starting from startBlock.
func (bl *BlockListener) Poll(ctx context.Context, startBlock uint64) <-chan *types.Header {
	out := make(chan *types.Header, 64)
	go func() {
		defer close(out)
		nextBlock := startBlock
		ticker := time.NewTicker(bl.pollInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				latest, err := bl.adapter.LatestBlockNumber(ctx)
				if err != nil {
					log.Warn().Err(err).Int64("chain_id", int64(bl.adapter.ChainID())).Msg("failed to get latest block")
					continue
				}

				for nextBlock <= latest {
					header, err := bl.adapter.BlockByNumber(ctx, new(big.Int).SetUint64(nextBlock))
					if err != nil {
						log.Warn().Err(err).Uint64("block", nextBlock).Msg("failed to get block header")
						break
					}

					select {
					case out <- header:
						nextBlock++
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()
	return out
}

// Subscribe uses WebSocket to receive new block headers in real-time.
// Falls back to Poll if subscription fails.
func (bl *BlockListener) Subscribe(ctx context.Context, startBlock uint64) <-chan *types.Header {
	headerCh, sub, err := bl.adapter.SubscribeNewHead(ctx)
	if err != nil {
		log.Warn().Err(err).Msg("WebSocket subscription failed, falling back to polling")
		return bl.Poll(ctx, startBlock)
	}

	out := make(chan *types.Header, 64)
	go func() {
		defer close(out)
		defer sub.Unsubscribe()

		// Backfill blocks between startBlock and latest.
		latest, err := bl.adapter.LatestBlockNumber(ctx)
		if err == nil {
			for bn := startBlock; bn <= latest; bn++ {
				h, err := bl.adapter.BlockByNumber(ctx, new(big.Int).SetUint64(bn))
				if err != nil {
					break
				}
				select {
				case out <- h:
				case <-ctx.Done():
					return
				}
			}
		}

		for {
			select {
			case <-ctx.Done():
				return
			case err := <-sub.Err():
				log.Error().Err(err).Msg("WebSocket subscription error, switching to poll")
				nextBlock := startBlock
				if latest > 0 {
					nextBlock = latest + 1
				}
				pollCh := bl.Poll(ctx, nextBlock)
				for h := range pollCh {
					select {
					case out <- h:
					case <-ctx.Done():
						return
					}
				}
				return
			case h := <-headerCh:
				if h != nil {
					select {
					case out <- h:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()
	return out
}
