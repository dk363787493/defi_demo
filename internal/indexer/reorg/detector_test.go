package reorg

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
	ctypes "github.com/zhangjinge/defi-lending-backend/internal/common/types"
)

func TestDetector_NoReorg(t *testing.T) {
	d := NewDetector(10)

	d.RecordBlock(ctypes.BlockHeader{Number: 1, Hash: hash("0xA1"), ParentHash: hash("0x00")})
	d.RecordBlock(ctypes.BlockHeader{Number: 2, Hash: hash("0xA2"), ParentHash: hash("0xA1")})
	d.RecordBlock(ctypes.BlockHeader{Number: 3, Hash: hash("0xA3"), ParentHash: hash("0xA2")})

	reorgBlock, isReorg := d.DetectReorg(ctypes.BlockHeader{
		Number: 4, Hash: hash("0xA4"), ParentHash: hash("0xA3"),
	})
	assert.False(t, isReorg)
	assert.Equal(t, uint64(0), reorgBlock)
}

func TestDetector_SimpleReorg(t *testing.T) {
	d := NewDetector(10)

	d.RecordBlock(ctypes.BlockHeader{Number: 1, Hash: hash("0xA1"), ParentHash: hash("0x00")})
	d.RecordBlock(ctypes.BlockHeader{Number: 2, Hash: hash("0xA2"), ParentHash: hash("0xA1")})
	d.RecordBlock(ctypes.BlockHeader{Number: 3, Hash: hash("0xA3"), ParentHash: hash("0xA2")})

	reorgBlock, isReorg := d.DetectReorg(ctypes.BlockHeader{
		Number: 3, Hash: hash("0xB3"), ParentHash: hash("0xA2"),
	})
	assert.True(t, isReorg)
	assert.Equal(t, uint64(3), reorgBlock)
}

func TestDetector_DeepReorg(t *testing.T) {
	d := NewDetector(10)

	d.RecordBlock(ctypes.BlockHeader{Number: 1, Hash: hash("0xA1"), ParentHash: hash("0x00")})
	d.RecordBlock(ctypes.BlockHeader{Number: 2, Hash: hash("0xA2"), ParentHash: hash("0xA1")})
	d.RecordBlock(ctypes.BlockHeader{Number: 3, Hash: hash("0xA3"), ParentHash: hash("0xA2")})

	reorgBlock, isReorg := d.DetectReorg(ctypes.BlockHeader{
		Number: 4, Hash: hash("0xB4"), ParentHash: hash("0xB3"),
	})
	assert.True(t, isReorg)
	assert.Equal(t, uint64(3), reorgBlock)
}

func hash(hex string) common.Hash {
	return common.HexToHash(hex)
}
