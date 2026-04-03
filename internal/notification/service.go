package notification

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

// NotificationStore persists notifications.
type NotificationStore interface {
	Save(ctx context.Context, n *Notification) error
	UpdateStatus(ctx context.Context, id int64, status Status) error
}

// Sender delivers a notification through a specific channel.
type Sender interface {
	Channel() Channel
	Send(ctx context.Context, n *Notification) error
}

// Service dispatches notifications with dedup, throttling, and multi-channel support.
type Service struct {
	store   NotificationStore
	senders map[Channel]Sender
	logger  zerolog.Logger

	// Dedup/throttle: key = "type:user" → last sent time
	mu        sync.Mutex
	lastSent  map[string]time.Time
	cooldown  time.Duration
}

func NewService(store NotificationStore, senders []Sender, logger zerolog.Logger, cooldown time.Duration) *Service {
	sm := make(map[Channel]Sender)
	for _, s := range senders {
		sm[s.Channel()] = s
	}
	return &Service{
		store:    store,
		senders:  sm,
		logger:   logger,
		lastSent: make(map[string]time.Time),
		cooldown: cooldown,
	}
}

// Notify creates and dispatches a notification.
func (s *Service) Notify(ctx context.Context, n *Notification) error {
	// Check throttle.
	if s.isThrottled(n) {
		n.Status = StatusThrottled
		s.logger.Debug().
			Str("type", string(n.Type)).
			Str("user", n.UserAddress).
			Msg("notification throttled")
		return nil
	}

	n.Status = StatusPending
	n.CreatedAt = time.Now()

	// Persist first.
	if err := s.store.Save(ctx, n); err != nil {
		return fmt.Errorf("save notification: %w", err)
	}

	// Dispatch to channel.
	sender, ok := s.senders[n.Channel]
	if !ok {
		s.logger.Warn().Str("channel", string(n.Channel)).Msg("no sender for channel")
		_ = s.store.UpdateStatus(ctx, n.ID, StatusFailed)
		return fmt.Errorf("no sender for channel %s", n.Channel)
	}

	if err := sender.Send(ctx, n); err != nil {
		s.logger.Error().Err(err).Str("channel", string(n.Channel)).Msg("send failed")
		_ = s.store.UpdateStatus(ctx, n.ID, StatusFailed)
		return fmt.Errorf("send notification: %w", err)
	}

	now := time.Now()
	n.Status = StatusSent
	n.SentAt = &now
	_ = s.store.UpdateStatus(ctx, n.ID, StatusSent)

	s.recordSent(n)

	s.logger.Info().
		Str("type", string(n.Type)).
		Str("channel", string(n.Channel)).
		Str("user", n.UserAddress).
		Msg("notification sent")

	return nil
}

// NotifyMultiChannel sends a notification to multiple channels.
func (s *Service) NotifyMultiChannel(ctx context.Context, channels []Channel, baseNotification *Notification) {
	for _, ch := range channels {
		n := *baseNotification // copy
		n.Channel = ch
		if err := s.Notify(ctx, &n); err != nil {
			s.logger.Error().Err(err).Str("channel", string(ch)).Msg("multi-channel send failed")
		}
	}
}

func (s *Service) isThrottled(n *Notification) bool {
	key := fmt.Sprintf("%s:%s", n.Type, n.UserAddress)
	s.mu.Lock()
	defer s.mu.Unlock()

	if lastTime, ok := s.lastSent[key]; ok {
		if time.Since(lastTime) < s.cooldown {
			return true
		}
	}
	return false
}

func (s *Service) recordSent(n *Notification) {
	key := fmt.Sprintf("%s:%s", n.Type, n.UserAddress)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastSent[key] = time.Now()
}
