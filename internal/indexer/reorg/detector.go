package reorg

import (
	"sync"

	"github.com/ethereum/go-ethereum/common"
	ctypes "github.com/zhangjinge/defi-lending-backend/internal/common/types"
)

// Detector tracks recent block hashes and detects chain reorganizations
// by comparing parent hashes of incoming blocks.
type Detector struct {
	mu          sync.RWMutex
	blockHashes map[uint64]common.Hash
	maxBlocks   int
}

func NewDetector(maxBlocks int) *Detector {
	return &Detector{
		blockHashes: make(map[uint64]common.Hash),
		maxBlocks:   maxBlocks,
	}
}

// RecordBlock stores a block's hash for future reorg detection.
func (d *Detector) RecordBlock(header ctypes.BlockHeader) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.blockHashes[header.Number] = header.Hash

	if len(d.blockHashes) > d.maxBlocks {
		oldest := header.Number - uint64(d.maxBlocks)
		for num := range d.blockHashes {
			if num <= oldest {
				delete(d.blockHashes, num)
			}
		}
	}
}

// DetectReorg checks if the incoming block indicates a chain reorganization.
// Returns (forkBlockNumber, true) if a reorg is detected, or (0, false) otherwise.
func (d *Detector) DetectReorg(incoming ctypes.BlockHeader) (uint64, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	// Case 1: We already have a block at this height with a different hash.
	if existingHash, ok := d.blockHashes[incoming.Number]; ok {
		if existingHash != incoming.Hash {
			return incoming.Number, true
		}
		return 0, false
	}

	// Case 2: New block extends the chain. Check if parent matches our record.
	parentNum := incoming.Number - 1
	if parentHash, ok := d.blockHashes[parentNum]; ok {
		if parentHash != incoming.ParentHash {
			return parentNum, true
		}
	}

	return 0, false
}

// Rollback removes all recorded blocks from the given block number onward.
func (d *Detector) Rollback(fromBlock uint64) {
	d.mu.Lock()
	defer d.mu.Unlock()

	for num := range d.blockHashes {
		if num >= fromBlock {
			delete(d.blockHashes, num)
		}
	}
}
