package executor

import (
	"context"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockSender struct {
	gasPrice *big.Int
	nonce    uint64
	sent     []*types.Transaction
}

func (m *mockSender) SuggestGasPrice(_ context.Context) (*big.Int, error) {
	return m.gasPrice, nil
}

func (m *mockSender) PendingNonceAt(_ context.Context, _ common.Address) (uint64, error) {
	return m.nonce, nil
}

func (m *mockSender) SendTransaction(_ context.Context, tx *types.Transaction) error {
	m.sent = append(m.sent, tx)
	return nil
}

type mockTxSigner struct {
	addr common.Address
}

func (m *mockTxSigner) SignTx(tx *types.Transaction, _ *big.Int) (*types.Transaction, error) {
	// Return the same tx (in real code, this would be signed).
	return tx, nil
}

func (m *mockTxSigner) Address() common.Address {
	return m.addr
}

func TestBuildLiquidationCalldata(t *testing.T) {
	collateral := common.HexToAddress("0x1111111111111111111111111111111111111111")
	debt := common.HexToAddress("0x2222222222222222222222222222222222222222")
	borrower := common.HexToAddress("0x3333333333333333333333333333333333333333")
	amount := big.NewInt(1e18)

	data := BuildLiquidationCalldata(collateral, debt, borrower, amount, false)

	// Should be 4 + 5*32 = 164 bytes
	assert.Len(t, data, 164)

	// First 4 bytes = function selector
	assert.Equal(t, 4, len(data[:4]))
}

func TestExecutor_Execute(t *testing.T) {
	sender := &mockSender{
		gasPrice: big.NewInt(20e9), // 20 gwei
		nonce:    42,
	}
	signer := &mockTxSigner{
		addr: common.HexToAddress("0xLiquidator"),
	}

	cfg := Config{
		ChainID:     big.NewInt(1),
		LendingPool: common.HexToAddress("0xPool"),
		GasLimit:    500000,
		MaxRetries:  3,
	}

	exec := New(cfg, sender, signer, zerolog.Nop())

	liqTx, err := exec.Execute(
		context.Background(),
		common.HexToAddress("0xCollateral"),
		common.HexToAddress("0xDebt"),
		common.HexToAddress("0xBorrower"),
		big.NewInt(1e18),
	)

	require.NoError(t, err)
	require.NotNil(t, liqTx)
	assert.Equal(t, TxPending, liqTx.Status)
	assert.Equal(t, common.HexToAddress("0xBorrower"), liqTx.Borrower)
	assert.Len(t, sender.sent, 1)
	assert.Equal(t, 1, exec.PendingCount())
}

func TestExecutor_MarkConfirmed(t *testing.T) {
	sender := &mockSender{gasPrice: big.NewInt(20e9), nonce: 0}
	signer := &mockTxSigner{addr: common.HexToAddress("0xLiq")}
	cfg := Config{ChainID: big.NewInt(1), LendingPool: common.HexToAddress("0xPool"), GasLimit: 500000}
	exec := New(cfg, sender, signer, zerolog.Nop())

	liqTx, err := exec.Execute(context.Background(),
		common.HexToAddress("0xC"), common.HexToAddress("0xD"),
		common.HexToAddress("0xB"), big.NewInt(1e18))
	require.NoError(t, err)

	exec.MarkConfirmed(liqTx.TxHash, 300000, big.NewInt(1e16))
	assert.Equal(t, 0, exec.PendingCount())
}

func TestEstimateProfit(t *testing.T) {
	sender := &mockSender{gasPrice: big.NewInt(20e9)} // 20 gwei
	signer := &mockTxSigner{}
	cfg := Config{
		ChainID:      big.NewInt(1),
		GasLimit:     500000,
		MaxGasPrice:  big.NewInt(100e9),
	}
	exec := New(cfg, sender, signer, zerolog.Nop())

	debtAmountUSD := big.NewInt(1000e8) // $1000 in 8-decimal
	// 5% bonus = 1.05e18
	bonus := new(big.Int).Mul(big.NewInt(105), new(big.Int).Exp(big.NewInt(10), big.NewInt(16), nil))

	profit, gasPrice, err := exec.EstimateProfit(context.Background(), debtAmountUSD, bonus)
	require.NoError(t, err)
	assert.NotNil(t, profit)
	assert.Equal(t, big.NewInt(20e9), gasPrice)
}
