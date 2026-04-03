package types

import (
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// ChainID is a numeric identifier for an EVM chain (1 = Ethereum, 42161 = Arbitrum, etc.).
type ChainID int64

// EventType identifies the kind of lending protocol event.
type EventType string

const (
	EventDeposit            EventType = "Deposit"
	EventWithdraw           EventType = "Withdraw"
	EventBorrow             EventType = "Borrow"
	EventRepay              EventType = "Repay"
	EventLiquidation        EventType = "Liquidation"
	EventReserveDataUpdated EventType = "ReserveDataUpdated"
	EventAccrueInterest     EventType = "AccrueInterest"
)

// BlockHeader is a minimal block representation used by the indexer.
type BlockHeader struct {
	ChainID    ChainID     `json:"chain_id"`
	Number     uint64      `json:"number"`
	Hash       common.Hash `json:"hash"`
	ParentHash common.Hash `json:"parent_hash"`
	Timestamp  time.Time   `json:"timestamp"`
}

// ChainEvent is a decoded lending protocol event from a transaction log.
type ChainEvent struct {
	ChainID         ChainID                `json:"chain_id"`
	BlockNumber     uint64                 `json:"block_number"`
	TxHash          common.Hash            `json:"tx_hash"`
	LogIndex        uint                   `json:"log_index"`
	EventType       EventType              `json:"event_type"`
	ContractAddress common.Address         `json:"contract_address"`
	UserAddress     common.Address         `json:"user_address"`
	MarketAddress   common.Address         `json:"market_address"`
	Amount          *big.Int               `json:"amount"`
	Data            map[string]interface{} `json:"data"`
	Timestamp       time.Time              `json:"timestamp"`
}

// SyncStatus tracks the indexing progress for a single chain.
type SyncStatus struct {
	ChainID            ChainID   `json:"chain_id"`
	LastIndexedBlock   uint64    `json:"last_indexed_block"`
	LastConfirmedBlock uint64    `json:"last_confirmed_block"`
	UpdatedAt          time.Time `json:"updated_at"`
}

// MarketState represents the current on-chain state of a lending pool/reserve.
type MarketState struct {
	MarketID             int64    `json:"market_id"`
	TotalSupply          *big.Int `json:"total_supply"`
	TotalBorrow          *big.Int `json:"total_borrow"`
	SupplyRate           *big.Int `json:"supply_rate"`
	BorrowRate           *big.Int `json:"borrow_rate"`
	LiquidityIndex       *big.Int `json:"liquidity_index"`
	BorrowIndex          *big.Int `json:"borrow_index"`
	LastUpdateTimestamp  uint64   `json:"last_update_timestamp"`
	UtilizationRate      *big.Int `json:"utilization_rate"`
}

// Position represents a user's supply/borrow position in a single market.
type Position struct {
	ID            int64          `json:"id"`
	ChainID       ChainID        `json:"chain_id"`
	UserAddress   common.Address `json:"user_address"`
	MarketID      int64          `json:"market_id"`
	SupplyBalance *big.Int       `json:"supply_balance"`
	BorrowBalance *big.Int       `json:"borrow_balance"`
	SupplyIndex   *big.Int       `json:"supply_index"`
	BorrowIndex   *big.Int       `json:"borrow_index"`
	UpdatedAt     time.Time      `json:"updated_at"`
}
