package oracle

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/crypto"
)

// ContractCaller abstracts contract read calls for testing.
type ContractCaller interface {
	CallContract(ctx context.Context, call ethereum.CallMsg, block *big.Int) ([]byte, error)
}

// ChainlinkReader reads prices from Chainlink Aggregator V3 contracts.
type ChainlinkReader struct {
	caller ContractCaller
}

func NewChainlinkReader(caller ContractCaller) *ChainlinkReader {
	return &ChainlinkReader{caller: caller}
}

// LatestRoundDataSelector returns the 4-byte function selector for latestRoundData().
func (r *ChainlinkReader) LatestRoundDataSelector() []byte {
	sig := crypto.Keccak256([]byte("latestRoundData()"))
	return sig[:4]
}

// FetchPrice calls latestRoundData() on a Chainlink price feed contract.
func (r *ChainlinkReader) FetchPrice(ctx context.Context, feed PriceFeedConfig) (*PriceData, error) {
	callData := r.LatestRoundDataSelector()

	result, err := r.caller.CallContract(ctx, ethereum.CallMsg{
		To:   &feed.FeedAddress,
		Data: callData,
	}, nil)
	if err != nil {
		return nil, fmt.Errorf("call latestRoundData on %s: %w", feed.FeedAddress.Hex(), err)
	}

	roundID, price, updatedAtUnix, err := r.DecodeLatestRoundData(result)
	if err != nil {
		return nil, fmt.Errorf("decode latestRoundData: %w", err)
	}

	return &PriceData{
		AssetAddress: feed.AssetAddress,
		PriceUSD:     price,
		Decimals:     feed.Decimals,
		Source:       SourceChainlink,
		RoundID:      roundID,
		UpdatedAt:    time.Unix(updatedAtUnix, 0),
	}, nil
}

// DecodeLatestRoundData decodes the ABI-encoded response from latestRoundData().
// Response: (uint80 roundId, int256 answer, uint256 startedAt, uint256 updatedAt, uint80 answeredInRound)
func (r *ChainlinkReader) DecodeLatestRoundData(data []byte) (*big.Int, *big.Int, int64, error) {
	if len(data) < 160 {
		return nil, nil, 0, fmt.Errorf("response too short: %d bytes, need 160", len(data))
	}

	roundID := new(big.Int).SetBytes(data[0:32])
	answer := new(big.Int).SetBytes(data[32:64])
	updatedAt := new(big.Int).SetBytes(data[96:128])

	if answer.Sign() <= 0 {
		return nil, nil, 0, fmt.Errorf("invalid price: %s", answer.String())
	}

	return roundID, answer, updatedAt.Int64(), nil
}
