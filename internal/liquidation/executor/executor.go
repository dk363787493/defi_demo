package executor

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/rs/zerolog"
)

// TxStatus tracks the lifecycle of a submitted liquidation transaction.
type TxStatus string

const (
	TxPending   TxStatus = "pending"
	TxConfirmed TxStatus = "confirmed"
	TxFailed    TxStatus = "failed"
	TxDropped   TxStatus = "dropped"
)

// LiquidationTx represents a liquidation transaction in flight.
type LiquidationTx struct {
	TxHash          common.Hash
	Borrower        common.Address
	DebtAsset       common.Address
	CollateralAsset common.Address
	DebtAmount      *big.Int
	Status          TxStatus
	GasUsed         uint64
	GasPrice        *big.Int
	Profit          *big.Int
	SubmittedAt     time.Time
	ConfirmedAt     time.Time
	RetryCount      int
}

// ChainSender abstracts on-chain transaction operations.
type ChainSender interface {
	SuggestGasPrice(ctx context.Context) (*big.Int, error)
	PendingNonceAt(ctx context.Context, addr common.Address) (uint64, error)
	SendTransaction(ctx context.Context, tx *types.Transaction) error
}

// TxSigner signs transactions without exposing private keys.
type TxSigner interface {
	SignTx(tx *types.Transaction, chainID *big.Int) (*types.Transaction, error)
	Address() common.Address
}

// Config holds executor configuration.
type Config struct {
	ChainID        *big.Int
	LendingPool    common.Address
	MinProfitWei   *big.Int // minimum profit to execute liquidation
	MaxGasPrice    *big.Int // gas price cap
	GasLimit       uint64   // gas limit for liquidation tx
	MaxRetries     int
	RetryBaseDelay time.Duration
}

// Executor builds, signs, submits, and tracks liquidation transactions.
type Executor struct {
	cfg    Config
	sender ChainSender
	signer TxSigner
	logger zerolog.Logger

	mu       sync.Mutex
	nonce    uint64
	nonceSet bool
	pending  map[common.Hash]*LiquidationTx
}

func New(cfg Config, sender ChainSender, signer TxSigner, logger zerolog.Logger) *Executor {
	return &Executor{
		cfg:     cfg,
		sender:  sender,
		signer:  signer,
		logger:  logger,
		pending: make(map[common.Hash]*LiquidationTx),
	}
}

// BuildLiquidationCalldata constructs the calldata for liquidationCall(collateral, debt, user, debtAmount, receiveAToken).
func BuildLiquidationCalldata(collateralAsset, debtAsset, borrower common.Address, debtAmount *big.Int, receiveAToken bool) []byte {
	// Function selector: liquidationCall(address,address,address,uint256,bool)
	sig := crypto.Keccak256([]byte("liquidationCall(address,address,address,uint256,bool)"))[:4]

	data := make([]byte, 4+32*5)
	copy(data[0:4], sig)
	copy(data[4+12:4+32], collateralAsset.Bytes())
	copy(data[4+32+12:4+64], debtAsset.Bytes())
	copy(data[4+64+12:4+96], borrower.Bytes())

	debtBytes := debtAmount.Bytes()
	copy(data[4+96+(32-len(debtBytes)):4+128], debtBytes)

	if receiveAToken {
		data[4+128+31] = 1
	}

	return data
}

// EstimateProfit estimates the profit from a liquidation.
// profit = liquidation_bonus_value - gas_cost
func (e *Executor) EstimateProfit(ctx context.Context, debtAmountUSD, liquidationBonus *big.Int) (*big.Int, *big.Int, error) {
	gasPrice, err := e.sender.SuggestGasPrice(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("suggest gas price: %w", err)
	}

	// Cap gas price.
	if e.cfg.MaxGasPrice != nil && gasPrice.Cmp(e.cfg.MaxGasPrice) > 0 {
		gasPrice = new(big.Int).Set(e.cfg.MaxGasPrice)
	}

	gasCost := new(big.Int).Mul(gasPrice, new(big.Int).SetUint64(e.cfg.GasLimit))

	// Bonus value: debtAmount * (bonus - 1.0), e.g., 5% bonus on $1000 = $50
	// liquidationBonus is Wad-scaled (1.05e18 for 5% bonus)
	bonusWad := new(big.Int).Sub(liquidationBonus, new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))
	bonusValue := new(big.Int).Mul(debtAmountUSD, bonusWad)
	bonusValue.Div(bonusValue, new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))

	profit := new(big.Int).Sub(bonusValue, gasCost)
	return profit, gasPrice, nil
}

// Execute submits a liquidation transaction.
func (e *Executor) Execute(ctx context.Context, collateralAsset, debtAsset, borrower common.Address, debtAmount *big.Int) (*LiquidationTx, error) {
	calldata := BuildLiquidationCalldata(collateralAsset, debtAsset, borrower, debtAmount, false)

	gasPrice, err := e.sender.SuggestGasPrice(ctx)
	if err != nil {
		return nil, fmt.Errorf("suggest gas price: %w", err)
	}
	if e.cfg.MaxGasPrice != nil && gasPrice.Cmp(e.cfg.MaxGasPrice) > 0 {
		gasPrice = new(big.Int).Set(e.cfg.MaxGasPrice)
	}

	nonce, err := e.nextNonce(ctx)
	if err != nil {
		return nil, fmt.Errorf("get nonce: %w", err)
	}

	tx := types.NewTransaction(nonce, e.cfg.LendingPool, big.NewInt(0), e.cfg.GasLimit, gasPrice, calldata)

	signedTx, err := e.signer.SignTx(tx, e.cfg.ChainID)
	if err != nil {
		return nil, fmt.Errorf("sign tx: %w", err)
	}

	if err := e.sender.SendTransaction(ctx, signedTx); err != nil {
		return nil, fmt.Errorf("send tx: %w", err)
	}

	liqTx := &LiquidationTx{
		TxHash:          signedTx.Hash(),
		Borrower:        borrower,
		DebtAsset:       debtAsset,
		CollateralAsset: collateralAsset,
		DebtAmount:      debtAmount,
		Status:          TxPending,
		GasPrice:        gasPrice,
		SubmittedAt:     time.Now(),
	}

	e.mu.Lock()
	e.pending[signedTx.Hash()] = liqTx
	e.mu.Unlock()

	e.logger.Info().
		Str("tx_hash", signedTx.Hash().Hex()).
		Str("borrower", borrower.Hex()).
		Str("debt_amount", debtAmount.String()).
		Msg("liquidation tx submitted")

	return liqTx, nil
}

// MarkConfirmed updates a pending tx as confirmed.
func (e *Executor) MarkConfirmed(txHash common.Hash, gasUsed uint64, profit *big.Int) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if tx, ok := e.pending[txHash]; ok {
		tx.Status = TxConfirmed
		tx.GasUsed = gasUsed
		tx.Profit = profit
		tx.ConfirmedAt = time.Now()
	}
}

// MarkFailed updates a pending tx as failed.
func (e *Executor) MarkFailed(txHash common.Hash) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if tx, ok := e.pending[txHash]; ok {
		tx.Status = TxFailed
	}
}

// PendingCount returns the number of in-flight transactions.
func (e *Executor) PendingCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()

	count := 0
	for _, tx := range e.pending {
		if tx.Status == TxPending {
			count++
		}
	}
	return count
}

func (e *Executor) nextNonce(ctx context.Context) (uint64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.nonceSet {
		n := e.nonce
		e.nonce++
		return n, nil
	}

	nonce, err := e.sender.PendingNonceAt(ctx, e.signer.Address())
	if err != nil {
		return 0, err
	}
	e.nonce = nonce + 1
	e.nonceSet = true
	return nonce, nil
}
