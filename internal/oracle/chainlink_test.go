package oracle

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChainlinkReader_LatestRoundDataSelector(t *testing.T) {
	reader := NewChainlinkReader(nil)
	selector := reader.LatestRoundDataSelector()
	// latestRoundData() selector = 0xfeaf968c
	assert.Equal(t, "feaf968c", common.Bytes2Hex(selector))
}

func TestDecodeLatestRoundData(t *testing.T) {
	reader := NewChainlinkReader(nil)

	roundID := common.LeftPadBytes(big.NewInt(100).Bytes(), 32)
	answer := common.LeftPadBytes(big.NewInt(200000000000).Bytes(), 32)
	startedAt := common.LeftPadBytes(big.NewInt(1000).Bytes(), 32)
	updatedAt := common.LeftPadBytes(big.NewInt(2000).Bytes(), 32)
	answeredInRound := common.LeftPadBytes(big.NewInt(100).Bytes(), 32)

	data := make([]byte, 0, 160)
	data = append(data, roundID...)
	data = append(data, answer...)
	data = append(data, startedAt...)
	data = append(data, updatedAt...)
	data = append(data, answeredInRound...)

	rID, price, ts, err := reader.DecodeLatestRoundData(data)
	require.NoError(t, err)
	assert.Equal(t, big.NewInt(100), rID)
	assert.Equal(t, big.NewInt(200000000000), price)
	assert.Equal(t, int64(2000), ts)
}

func TestDecodeLatestRoundData_TooShort(t *testing.T) {
	reader := NewChainlinkReader(nil)
	_, _, _, err := reader.DecodeLatestRoundData(make([]byte, 100))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "too short")
}
