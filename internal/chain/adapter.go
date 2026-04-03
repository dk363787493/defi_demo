package chain

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	ctypes "github.com/zhangjinge/defi-lending-backend/internal/common/types"
)

// ChainAdapter abstracts chain-specific RPC interactions.
// Each EVM chain implements this interface.
type ChainAdapter interface {
	// ChainID returns the numeric chain identifier.
	ChainID() ctypes.ChainID

	// LatestBlockNumber returns the most recent block number.
	LatestBlockNumber(ctx context.Context) (uint64, error)

	// BlockByNumber fetches a block header by number.
	BlockByNumber(ctx context.Context, number *big.Int) (*types.Header, error)

	// SubscribeNewHead subscribes to new block headers via WebSocket.
	SubscribeNewHead(ctx context.Context) (chan *types.Header, ethereum.Subscription, error)

	// FilterLogs returns logs matching the given filter query.
	FilterLogs(ctx context.Context, query ethereum.FilterQuery) ([]types.Log, error)

	// CallContract executes a contract call (read-only) at the given block.
	CallContract(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error)

	// SendTransaction submits a signed transaction to the network.
	SendTransaction(ctx context.Context, tx *types.Transaction) error

	// SuggestGasPrice returns the currently suggested gas price.
	SuggestGasPrice(ctx context.Context) (*big.Int, error)

	// PendingNonceAt returns the next nonce for the given address.
	PendingNonceAt(ctx context.Context, addr common.Address) (uint64, error)

	// Close shuts down the underlying RPC connections.
	Close()
}
