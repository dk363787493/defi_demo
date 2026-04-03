package parser

import (
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	ctypes "github.com/zhangjinge/defi-lending-backend/internal/common/types"
)

var ErrUnknownEvent = errors.New("unknown event signature")

// eventDefs maps an EventType to its Solidity signature string.
var eventDefs = map[ctypes.EventType]string{
	ctypes.EventDeposit:            "Deposit(address,address,uint256)",
	ctypes.EventWithdraw:           "Withdraw(address,address,uint256)",
	ctypes.EventBorrow:             "Borrow(address,address,uint256)",
	ctypes.EventRepay:              "Repay(address,address,uint256)",
	ctypes.EventLiquidation:        "LiquidationCall(address,address,address,uint256,uint256)",
	ctypes.EventReserveDataUpdated: "ReserveDataUpdated(address,uint256,uint256,uint256,uint256)",
	ctypes.EventAccrueInterest:     "AccrueInterest(uint256,uint256,uint256)",
}

// EventParser decodes raw EVM logs into structured ChainEvents.
type EventParser struct {
	sigToType map[common.Hash]ctypes.EventType
	typeToSig map[ctypes.EventType]common.Hash
}

func NewEventParser() (*EventParser, error) {
	sigToType := make(map[common.Hash]ctypes.EventType)
	typeToSig := make(map[ctypes.EventType]common.Hash)

	for evtType, sigStr := range eventDefs {
		sig := crypto.Keccak256Hash([]byte(sigStr))
		sigToType[sig] = evtType
		typeToSig[evtType] = sig
	}

	return &EventParser{
		sigToType: sigToType,
		typeToSig: typeToSig,
	}, nil
}

// EventSignature returns the keccak256 topic hash for the given event type.
func (p *EventParser) EventSignature(eventType ctypes.EventType) common.Hash {
	return p.typeToSig[eventType]
}

// AllSignatures returns all registered event topic hashes (for FilterLogs).
func (p *EventParser) AllSignatures() []common.Hash {
	sigs := make([]common.Hash, 0, len(p.sigToType))
	for sig := range p.sigToType {
		sigs = append(sigs, sig)
	}
	return sigs
}

// ParseLog converts a raw EVM log into a ChainEvent.
func (p *EventParser) ParseLog(chainID ctypes.ChainID, log types.Log) (*ctypes.ChainEvent, error) {
	if len(log.Topics) == 0 {
		return nil, ErrUnknownEvent
	}

	evtType, ok := p.sigToType[log.Topics[0]]
	if !ok {
		return nil, ErrUnknownEvent
	}

	event := &ctypes.ChainEvent{
		ChainID:         chainID,
		BlockNumber:     log.BlockNumber,
		TxHash:          log.TxHash,
		LogIndex:        log.Index,
		EventType:       evtType,
		ContractAddress: log.Address,
		Timestamp:       time.Now(),
		Data:            make(map[string]interface{}),
	}

	switch evtType {
	case ctypes.EventDeposit, ctypes.EventWithdraw, ctypes.EventBorrow, ctypes.EventRepay:
		if err := p.parseSimpleEvent(log, event); err != nil {
			return nil, fmt.Errorf("parse %s event: %w", evtType, err)
		}
	case ctypes.EventLiquidation:
		if err := p.parseLiquidationEvent(log, event); err != nil {
			return nil, fmt.Errorf("parse liquidation event: %w", err)
		}
	case ctypes.EventReserveDataUpdated:
		if err := p.parseReserveDataUpdated(log, event); err != nil {
			return nil, fmt.Errorf("parse reserve data updated: %w", err)
		}
	case ctypes.EventAccrueInterest:
		if err := p.parseAccrueInterest(log, event); err != nil {
			return nil, fmt.Errorf("parse accrue interest: %w", err)
		}
	}

	return event, nil
}

// parseSimpleEvent handles Deposit/Withdraw/Borrow/Repay.
func (p *EventParser) parseSimpleEvent(log types.Log, event *ctypes.ChainEvent) error {
	if len(log.Topics) < 3 {
		return fmt.Errorf("expected 3 topics, got %d", len(log.Topics))
	}
	event.MarketAddress = common.HexToAddress(log.Topics[1].Hex())
	event.UserAddress = common.HexToAddress(log.Topics[2].Hex())

	if len(log.Data) >= 32 {
		event.Amount = new(big.Int).SetBytes(log.Data[:32])
	}
	return nil
}

// parseLiquidationEvent handles LiquidationCall.
func (p *EventParser) parseLiquidationEvent(log types.Log, event *ctypes.ChainEvent) error {
	if len(log.Topics) < 4 {
		return fmt.Errorf("expected 4 topics, got %d", len(log.Topics))
	}
	event.Data["collateral_asset"] = common.HexToAddress(log.Topics[1].Hex()).Hex()
	event.Data["debt_asset"] = common.HexToAddress(log.Topics[2].Hex()).Hex()
	event.UserAddress = common.HexToAddress(log.Topics[3].Hex())

	if len(log.Data) >= 64 {
		event.Amount = new(big.Int).SetBytes(log.Data[:32])
		event.Data["collateral_seized"] = new(big.Int).SetBytes(log.Data[32:64]).String()
	}
	return nil
}

// parseReserveDataUpdated handles ReserveDataUpdated.
func (p *EventParser) parseReserveDataUpdated(log types.Log, event *ctypes.ChainEvent) error {
	if len(log.Topics) < 2 {
		return fmt.Errorf("expected 2 topics, got %d", len(log.Topics))
	}
	event.MarketAddress = common.HexToAddress(log.Topics[1].Hex())

	if len(log.Data) >= 128 {
		event.Data["liquidity_rate"] = new(big.Int).SetBytes(log.Data[:32]).String()
		event.Data["stable_borrow_rate"] = new(big.Int).SetBytes(log.Data[32:64]).String()
		event.Data["variable_borrow_rate"] = new(big.Int).SetBytes(log.Data[64:96]).String()
		event.Data["liquidity_index"] = new(big.Int).SetBytes(log.Data[96:128]).String()
	}
	return nil
}

// parseAccrueInterest handles AccrueInterest.
func (p *EventParser) parseAccrueInterest(log types.Log, event *ctypes.ChainEvent) error {
	if len(log.Data) >= 96 {
		event.Data["cash_prior"] = new(big.Int).SetBytes(log.Data[:32]).String()
		event.Data["interest_accumulated"] = new(big.Int).SetBytes(log.Data[32:64]).String()
		event.Data["borrow_index"] = new(big.Int).SetBytes(log.Data[64:96]).String()
	}
	return nil
}
