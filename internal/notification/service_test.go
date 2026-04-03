package notification

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockNotifStore struct {
	saved   []*Notification
	updates map[int64]Status
	nextID  int64
}

func newMockNotifStore() *mockNotifStore {
	return &mockNotifStore{updates: make(map[int64]Status), nextID: 1}
}

func (m *mockNotifStore) Save(_ context.Context, n *Notification) error {
	n.ID = m.nextID
	m.nextID++
	m.saved = append(m.saved, n)
	return nil
}

func (m *mockNotifStore) UpdateStatus(_ context.Context, id int64, status Status) error {
	m.updates[id] = status
	return nil
}

type mockSender struct {
	ch   Channel
	sent []*Notification
}

func (m *mockSender) Channel() Channel { return m.ch }
func (m *mockSender) Send(_ context.Context, n *Notification) error {
	m.sent = append(m.sent, n)
	return nil
}

func TestService_Notify(t *testing.T) {
	store := newMockNotifStore()
	sender := &mockSender{ch: ChannelTelegram}
	svc := NewService(store, []Sender{sender}, zerolog.Nop(), 5*time.Minute)

	n := &Notification{
		UserAddress: "0xuser1",
		Type:        TypeHealthFactorAlert,
		Channel:     ChannelTelegram,
		Title:       "Health Factor Low",
		Content:     "Your health factor is 1.05",
	}

	err := svc.Notify(context.Background(), n)
	require.NoError(t, err)

	assert.Len(t, store.saved, 1)
	assert.Len(t, sender.sent, 1)
	assert.Equal(t, StatusSent, store.updates[1])
}

func TestService_Throttling(t *testing.T) {
	store := newMockNotifStore()
	sender := &mockSender{ch: ChannelTelegram}
	svc := NewService(store, []Sender{sender}, zerolog.Nop(), 1*time.Hour) // 1 hour cooldown

	n1 := &Notification{
		UserAddress: "0xuser1",
		Type:        TypeHealthFactorAlert,
		Channel:     ChannelTelegram,
		Title:       "Alert 1",
	}
	n2 := &Notification{
		UserAddress: "0xuser1",
		Type:        TypeHealthFactorAlert,
		Channel:     ChannelTelegram,
		Title:       "Alert 2 (should be throttled)",
	}

	// First notification should succeed.
	err := svc.Notify(context.Background(), n1)
	require.NoError(t, err)
	assert.Len(t, sender.sent, 1)

	// Second notification (same type+user) within cooldown should be throttled.
	err = svc.Notify(context.Background(), n2)
	require.NoError(t, err)
	assert.Len(t, sender.sent, 1) // still 1, second was throttled
	assert.Equal(t, StatusThrottled, n2.Status)
}

func TestService_DifferentUsersNotThrottled(t *testing.T) {
	store := newMockNotifStore()
	sender := &mockSender{ch: ChannelWebhook}
	svc := NewService(store, []Sender{sender}, zerolog.Nop(), 1*time.Hour)

	n1 := &Notification{UserAddress: "0xuser1", Type: TypeLiquidation, Channel: ChannelWebhook}
	n2 := &Notification{UserAddress: "0xuser2", Type: TypeLiquidation, Channel: ChannelWebhook}

	_ = svc.Notify(context.Background(), n1)
	_ = svc.Notify(context.Background(), n2)

	assert.Len(t, sender.sent, 2) // different users, both sent
}

func TestService_MultiChannel(t *testing.T) {
	store := newMockNotifStore()
	tgSender := &mockSender{ch: ChannelTelegram}
	whSender := &mockSender{ch: ChannelWebhook}
	svc := NewService(store, []Sender{tgSender, whSender}, zerolog.Nop(), 0)

	n := &Notification{
		UserAddress: "0xadmin",
		Type:        TypeSystemAlert,
		Title:       "System Alert",
	}

	svc.NotifyMultiChannel(context.Background(), []Channel{ChannelTelegram, ChannelWebhook}, n)

	assert.Len(t, tgSender.sent, 1)
	assert.Len(t, whSender.sent, 1)
}

func TestService_NoSenderForChannel(t *testing.T) {
	store := newMockNotifStore()
	svc := NewService(store, nil, zerolog.Nop(), 0)

	n := &Notification{
		UserAddress: "0xuser",
		Type:        TypeRiskAlert,
		Channel:     ChannelEmail, // no email sender registered
	}

	err := svc.Notify(context.Background(), n)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no sender")
}
