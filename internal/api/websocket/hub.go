package websocket

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
)

// Topic identifies a subscription channel.
type Topic string

const (
	TopicPrices        Topic = "prices"
	TopicPositions     Topic = "positions"
	TopicNotifications Topic = "notifications"
	TopicMarkets       Topic = "markets"
)

// Message is a WebSocket message sent to clients.
type Message struct {
	Topic     Topic           `json:"topic"`
	Type      string          `json:"type"`
	Data      json.RawMessage `json:"data"`
	Timestamp int64           `json:"timestamp"`
}

// Client represents a connected WebSocket client.
type Client struct {
	hub         *Hub
	conn        *websocket.Conn
	send        chan []byte
	topics      map[Topic]bool
	userAddress string // empty for unauthenticated clients
	mu          sync.Mutex
}

// Hub manages WebSocket clients and message broadcasting.
type Hub struct {
	mu         sync.RWMutex
	clients    map[*Client]bool
	topicSubs  map[Topic]map[*Client]bool // topic → subscribed clients
	userIndex  map[string]*Client         // userAddress → client (for targeted messages)
	register   chan *Client
	unregister chan *Client
	logger     zerolog.Logger
}

// NewHub creates a new WebSocket hub.
func NewHub(logger zerolog.Logger) *Hub {
	return &Hub{
		clients:    make(map[*Client]bool),
		topicSubs:  make(map[Topic]map[*Client]bool),
		userIndex:  make(map[string]*Client),
		register:   make(chan *Client, 64),
		unregister: make(chan *Client, 64),
		logger:     logger,
	}
}

// Run starts the hub event loop. Should be called in a goroutine.
func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			h.addClient(client)
		case client := <-h.unregister:
			h.removeClient(client)
		}
	}
}

func (h *Hub) addClient(c *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.clients[c] = true
	if c.userAddress != "" {
		h.userIndex[c.userAddress] = c
	}
	h.logger.Debug().Str("user", c.userAddress).Int("total", len(h.clients)).Msg("ws client connected")
}

func (h *Hub) removeClient(c *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.clients[c]; !ok {
		return
	}
	delete(h.clients, c)
	if c.userAddress != "" {
		delete(h.userIndex, c.userAddress)
	}
	// Remove from all topic subscriptions.
	for topic, subs := range h.topicSubs {
		delete(subs, c)
		if len(subs) == 0 {
			delete(h.topicSubs, topic)
		}
	}
	close(c.send)
	h.logger.Debug().Str("user", c.userAddress).Int("total", len(h.clients)).Msg("ws client disconnected")
}

// Subscribe adds a client to a topic.
func (h *Hub) Subscribe(c *Client, topic Topic) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.topicSubs[topic] == nil {
		h.topicSubs[topic] = make(map[*Client]bool)
	}
	h.topicSubs[topic][c] = true
	c.mu.Lock()
	c.topics[topic] = true
	c.mu.Unlock()
}

// Unsubscribe removes a client from a topic.
func (h *Hub) Unsubscribe(c *Client, topic Topic) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if subs, ok := h.topicSubs[topic]; ok {
		delete(subs, c)
	}
	c.mu.Lock()
	delete(c.topics, topic)
	c.mu.Unlock()
}

// BroadcastToTopic sends a message to all clients subscribed to a topic.
func (h *Hub) BroadcastToTopic(topic Topic, msgType string, data interface{}) {
	raw, err := json.Marshal(data)
	if err != nil {
		h.logger.Error().Err(err).Str("topic", string(topic)).Msg("failed to marshal broadcast data")
		return
	}

	msg := Message{
		Topic:     topic,
		Type:      msgType,
		Data:      raw,
		Timestamp: time.Now().UnixMilli(),
	}
	payload, err := json.Marshal(msg)
	if err != nil {
		return
	}

	h.mu.RLock()
	subs := h.topicSubs[topic]
	h.mu.RUnlock()

	for client := range subs {
		select {
		case client.send <- payload:
		default:
			// Client buffer full; disconnect.
			h.unregister <- client
		}
	}
}

// SendToUser sends a message to a specific user's WebSocket connection.
func (h *Hub) SendToUser(userAddress string, topic Topic, msgType string, data interface{}) {
	h.mu.RLock()
	client, ok := h.userIndex[userAddress]
	h.mu.RUnlock()
	if !ok {
		return
	}

	raw, err := json.Marshal(data)
	if err != nil {
		return
	}
	msg := Message{
		Topic:     topic,
		Type:      msgType,
		Data:      raw,
		Timestamp: time.Now().UnixMilli(),
	}
	payload, err := json.Marshal(msg)
	if err != nil {
		return
	}

	select {
	case client.send <- payload:
	default:
		h.unregister <- client
	}
}

// Stats returns current hub statistics.
func (h *Hub) Stats() HubStats {
	h.mu.RLock()
	defer h.mu.RUnlock()

	topicCounts := make(map[Topic]int)
	for topic, subs := range h.topicSubs {
		topicCounts[topic] = len(subs)
	}
	return HubStats{
		TotalClients:    len(h.clients),
		TopicSubCounts:  topicCounts,
		AuthenticatedAt: len(h.userIndex),
	}
}

// HubStats provides runtime stats about the hub.
type HubStats struct {
	TotalClients    int            `json:"total_clients"`
	TopicSubCounts  map[Topic]int  `json:"topic_sub_counts"`
	AuthenticatedAt int            `json:"authenticated_clients"`
}
