package account

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

// AccountDB abstracts database queries for account data.
type AccountDB interface {
	GetPositions(ctx context.Context, userAddress string, chainID int64) ([]PositionSummary, error)
	GetTxHistory(ctx context.Context, userAddress string, chainID int64, limit int, offset int) ([]TxHistoryEntry, error)
}

// PriceProvider returns USD price for an asset.
type PriceProvider interface {
	GetPriceUSD(asset common.Address) (*big.Int, uint8, bool)
}

// Service provides account-related business logic.
type Service struct {
	db AccountDB
}

func NewService(db AccountDB) *Service {
	return &Service{db: db}
}

// GetOverview returns the account overview for a user.
func (s *Service) GetOverview(ctx context.Context, userAddress string, chainID int64) (*AccountOverview, error) {
	positions, err := s.db.GetPositions(ctx, userAddress, chainID)
	if err != nil {
		return nil, fmt.Errorf("get positions: %w", err)
	}

	totalSupply := new(big.Int)
	totalBorrow := new(big.Int)

	for _, p := range positions {
		supplyUSD, _ := new(big.Int).SetString(p.SupplyValueUSD, 10)
		borrowUSD, _ := new(big.Int).SetString(p.BorrowValueUSD, 10)
		if supplyUSD != nil {
			totalSupply.Add(totalSupply, supplyUSD)
		}
		if borrowUSD != nil {
			totalBorrow.Add(totalBorrow, borrowUSD)
		}
	}

	netWorth := new(big.Int).Sub(totalSupply, totalBorrow)

	hf := "∞"
	if totalBorrow.Sign() > 0 {
		// Simplified HF = totalSupply / totalBorrow (full HF uses liq threshold per market)
		hfBig := new(big.Float).Quo(
			new(big.Float).SetInt(totalSupply),
			new(big.Float).SetInt(totalBorrow),
		)
		hf = fmt.Sprintf("%.4f", hfBig)
	}

	return &AccountOverview{
		Address:        userAddress,
		TotalSupplyUSD: totalSupply.String(),
		TotalBorrowUSD: totalBorrow.String(),
		NetWorthUSD:    netWorth.String(),
		HealthFactor:   hf,
		Positions:      positions,
	}, nil
}

// GetHistory returns paginated transaction history.
func (s *Service) GetHistory(ctx context.Context, userAddress string, chainID int64, limit int, offset int) ([]TxHistoryEntry, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	if offset < 0 {
		offset = 0
	}
	return s.db.GetTxHistory(ctx, userAddress, chainID, limit, offset)
}

// BuildTransaction constructs calldata for a lending action.
func (s *Service) BuildTransaction(req TxBuildRequest, poolAddress common.Address) (*TxBuildResponse, error) {
	asset := common.HexToAddress(req.Asset)
	amount, ok := new(big.Int).SetString(req.Amount, 10)
	if !ok {
		return nil, fmt.Errorf("invalid amount: %s", req.Amount)
	}

	var funcSig string
	switch req.Action {
	case "deposit":
		funcSig = "deposit(address,uint256,address,uint16)"
	case "withdraw":
		funcSig = "withdraw(address,uint256,address)"
	case "borrow":
		funcSig = "borrow(address,uint256,uint256,uint16,address)"
	case "repay":
		funcSig = "repay(address,uint256,uint256,address)"
	default:
		return nil, fmt.Errorf("unknown action: %s", req.Action)
	}

	selector := crypto.Keccak256([]byte(funcSig))[:4]

	// Build simplified calldata: selector + asset + amount (real impl would ABI-encode all params)
	data := make([]byte, 4+64)
	copy(data[0:4], selector)
	copy(data[4+12:4+32], asset.Bytes())
	amountBytes := amount.Bytes()
	copy(data[36+(32-len(amountBytes)):68], amountBytes)

	return &TxBuildResponse{
		To:       poolAddress.Hex(),
		Data:     fmt.Sprintf("0x%x", data),
		Value:    "0",
		GasLimit: "300000",
	}, nil
}
