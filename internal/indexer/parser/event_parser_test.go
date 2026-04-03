package parser

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	ctypes "github.com/zhangjinge/defi-lending-backend/internal/common/types"
)

func TestEventParser_ParseDepositEvent(t *testing.T) {
	parser, err := NewEventParser()
	require.NoError(t, err)

	depositSig := parser.EventSignature(ctypes.EventDeposit)
	require.NotEqual(t, common.Hash{}, depositSig)

	log := types.Log{
		Address: common.HexToAddress("0xABCD000000000000000000000000000000000001"),
		Topics: []common.Hash{
			depositSig,
			common.HexToHash("0x000000000000000000000000aabb000000000000000000000000000000000001"),
			common.HexToHash("0x000000000000000000000000ccdd000000000000000000000000000000000002"),
		},
		Data:        common.Hex2Bytes("0000000000000000000000000000000000000000000000000DE0B6B3A7640000"),
		BlockNumber: 100,
		TxHash:      common.HexToHash("0x1234"),
		Index:       0,
	}

	event, err := parser.ParseLog(ctypes.ChainID(1), log)
	require.NoError(t, err)
	assert.Equal(t, ctypes.EventDeposit, event.EventType)
	assert.Equal(t, common.HexToAddress("0xccdd000000000000000000000000000000000002"), event.UserAddress)
	assert.Equal(t, common.HexToAddress("0xaabb000000000000000000000000000000000001"), event.MarketAddress)
	assert.Equal(t, big.NewInt(1e18), event.Amount)
}

func TestEventParser_UnknownEvent(t *testing.T) {
	parser, err := NewEventParser()
	require.NoError(t, err)

	log := types.Log{
		Topics: []common.Hash{
			common.HexToHash("0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"),
		},
		BlockNumber: 100,
	}

	_, err = parser.ParseLog(ctypes.ChainID(1), log)
	assert.ErrorIs(t, err, ErrUnknownEvent)
}
