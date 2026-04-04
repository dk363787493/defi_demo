package websocket

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestHub() *Hub {
	h := NewHub(zerolog.Nop())
	go h.Run()
	return h
}

func newTestClient(hub *Hub, user string) *Client {
	c := &Client{
		hub:         hub,
		send:        make(chan []byte, 256),
		topics:      make(map[Topic]bool),
		userAddress: user,
	}
	hub.register <- c
	time.Sleep(10 * time.Millisecond) // allow registration to process
	return c
}

func TestHub_RegisterUnregister(t *testing.T) {
	hub := newTestHub()

	c1 := newTestClient(hub, "0xuser1")
	c2 := newTestClient(hub, "0xuser2")

	stats := hub.Stats()
	assert.Equal(t, 2, stats.TotalClients)
	assert.Equal(t, 2, stats.AuthenticatedAt)

	hub.unregister <- c1
	time.Sleep(10 * time.Millisecond)

	stats = hub.Stats()
	assert.Equal(t, 1, stats.TotalClients)

	hub.unregister <- c2
	time.Sleep(10 * time.Millisecond)

	stats = hub.Stats()
	assert.Equal(t, 0, stats.TotalClients)
}

func TestHub_SubscribeUnsubscribe(t *testing.T) {
	hub := newTestHub()
	c := newTestClient(hub, "0xsub")

	hub.Subscribe(c, TopicPrices)
	hub.Subscribe(c, TopicMarkets)

	stats := hub.Stats()
	assert.Equal(t, 1, stats.TopicSubCounts[TopicPrices])
	assert.Equal(t, 1, stats.TopicSubCounts[TopicMarkets])

	hub.Unsubscribe(c, TopicPrices)
	stats = hub.Stats()
	assert.Equal(t, 0, stats.TopicSubCounts[TopicPrices])
	assert.Equal(t, 1, stats.TopicSubCounts[TopicMarkets])
}

func TestHub_BroadcastToTopic(t *testing.T) {
	hub := newTestHub()

	c1 := newTestClient(hub, "0xlistener1")
	c2 := newTestClient(hub, "0xlistener2")
	c3 := newTestClient(hub, "0xnonsub")

	hub.Subscribe(c1, TopicPrices)
	hub.Subscribe(c2, TopicPrices)
	// c3 not subscribed to prices

	priceData := map[string]string{"ETH": "3500.00"}
	hub.BroadcastToTopic(TopicPrices, "price_update", priceData)

	// c1 and c2 should receive
	select {
	case msg := <-c1.send:
		var m Message
		require.NoError(t, json.Unmarshal(msg, &m))
		assert.Equal(t, TopicPrices, m.Topic)
		assert.Equal(t, "price_update", m.Type)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("c1 did not receive broadcast")
	}

	select {
	case msg := <-c2.send:
		var m Message
		require.NoError(t, json.Unmarshal(msg, &m))
		assert.Equal(t, TopicPrices, m.Topic)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("c2 did not receive broadcast")
	}

	// c3 should NOT receive
	select {
	case <-c3.send:
		t.Fatal("c3 should not have received broadcast")
	case <-time.After(50 * time.Millisecond):
		// expected
	}
}

func TestHub_SendToUser(t *testing.T) {
	hub := newTestHub()

	c1 := newTestClient(hub, "0xtarget")
	c2 := newTestClient(hub, "0xother")

	hub.SendToUser("0xtarget", TopicNotifications, "health_alert", map[string]float64{"hf": 1.05})

	select {
	case msg := <-c1.send:
		var m Message
		require.NoError(t, json.Unmarshal(msg, &m))
		assert.Equal(t, TopicNotifications, m.Topic)
		assert.Equal(t, "health_alert", m.Type)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("target user did not receive message")
	}

	select {
	case <-c2.send:
		t.Fatal("other user should not have received message")
	case <-time.After(50 * time.Millisecond):
		// expected
	}
}

func TestHub_SendToUser_NonExistent(t *testing.T) {
	hub := newTestHub()

	// Should not panic for non-existent user.
	hub.SendToUser("0xghost", TopicNotifications, "test", nil)
}

func TestHub_DoubleUnregister(t *testing.T) {
	hub := newTestHub()
	c := newTestClient(hub, "0xdouble")

	hub.unregister <- c
	time.Sleep(10 * time.Millisecond)

	// Second unregister should be a no-op (not panic).
	hub.unregister <- c
	time.Sleep(10 * time.Millisecond)

	stats := hub.Stats()
	assert.Equal(t, 0, stats.TotalClients)
}

func TestIsValidTopic(t *testing.T) {
	assert.True(t, isValidTopic(TopicPrices))
	assert.True(t, isValidTopic(TopicPositions))
	assert.True(t, isValidTopic(TopicNotifications))
	assert.True(t, isValidTopic(TopicMarkets))
	assert.False(t, isValidTopic(Topic("invalid")))
	assert.False(t, isValidTopic(Topic("")))
}
