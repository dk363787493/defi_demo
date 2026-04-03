package notification

import "time"

// Channel identifies a notification delivery channel.
type Channel string

const (
	ChannelWebSocket Channel = "websocket"
	ChannelTelegram  Channel = "telegram"
	ChannelEmail     Channel = "email"
	ChannelWebhook   Channel = "webhook"
)

// NotificationType categorizes notifications.
type NotificationType string

const (
	TypeHealthFactorAlert NotificationType = "health_factor_alert"
	TypeLiquidation       NotificationType = "liquidation"
	TypeRateChange        NotificationType = "rate_change"
	TypeRiskAlert         NotificationType = "risk_alert"
	TypeSystemAlert       NotificationType = "system_alert"
)

// Status tracks delivery status.
type Status string

const (
	StatusPending   Status = "pending"
	StatusSent      Status = "sent"
	StatusFailed    Status = "failed"
	StatusThrottled Status = "throttled"
)

// Notification is a single notification to send.
type Notification struct {
	ID          int64            `json:"id"`
	UserAddress string           `json:"user_address,omitempty"` // empty for admin alerts
	Type        NotificationType `json:"type"`
	Channel     Channel          `json:"channel"`
	Title       string           `json:"title"`
	Content     string           `json:"content"`
	Data        map[string]string `json:"data,omitempty"`
	Status      Status           `json:"status"`
	CreatedAt   time.Time        `json:"created_at"`
	SentAt      *time.Time       `json:"sent_at,omitempty"`
}

// UserPreference defines a user's notification settings.
type UserPreference struct {
	UserAddress        string   `json:"user_address"`
	EnabledChannels    []Channel `json:"enabled_channels"`
	HealthFactorThreshold float64 `json:"health_factor_threshold"` // alert when HF below this
}
