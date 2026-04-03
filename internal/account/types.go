package account

import "time"

// PositionSummary is the API response for a user's position in a single market.
type PositionSummary struct {
	MarketID       int64  `json:"market_id"`
	ChainID        int64  `json:"chain_id"`
	AssetSymbol    string `json:"asset_symbol"`
	AssetAddress   string `json:"asset_address"`
	SupplyBalance  string `json:"supply_balance"`
	BorrowBalance  string `json:"borrow_balance"`
	SupplyValueUSD string `json:"supply_value_usd"`
	BorrowValueUSD string `json:"borrow_value_usd"`
}

// AccountOverview is the aggregate account view returned to the API.
type AccountOverview struct {
	Address            string            `json:"address"`
	TotalSupplyUSD     string            `json:"total_supply_usd"`
	TotalBorrowUSD     string            `json:"total_borrow_usd"`
	NetWorthUSD        string            `json:"net_worth_usd"`
	HealthFactor       string            `json:"health_factor"`
	Positions          []PositionSummary `json:"positions"`
}

// TxHistoryEntry is a single transaction in the user's history.
type TxHistoryEntry struct {
	ChainID     int64     `json:"chain_id"`
	TxHash      string    `json:"tx_hash"`
	EventType   string    `json:"event_type"`
	AssetSymbol string    `json:"asset_symbol"`
	Amount      string    `json:"amount"`
	Timestamp   time.Time `json:"timestamp"`
}

// TxBuildRequest is the body for POST /account/tx/build.
type TxBuildRequest struct {
	ChainID  int64  `json:"chain_id" binding:"required"`
	Action   string `json:"action" binding:"required"` // deposit, withdraw, borrow, repay
	Asset    string `json:"asset" binding:"required"`
	Amount   string `json:"amount" binding:"required"`
}

// TxBuildResponse contains the constructed calldata for the user to sign.
type TxBuildResponse struct {
	To       string `json:"to"`
	Data     string `json:"data"`
	Value    string `json:"value"`
	GasLimit string `json:"gas_limit"`
}
