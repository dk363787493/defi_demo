package websocket

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 4096
	sendBufferSize = 256
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		// In production, validate against allowed origins.
		return true
	},
}

// ClientCommand is a message received from the client.
type ClientCommand struct {
	Action string `json:"action"` // "subscribe" | "unsubscribe"
	Topic  Topic  `json:"topic"`
}

// Handler handles WebSocket upgrade and message routing.
type Handler struct {
	hub    *Hub
	logger zerolog.Logger
}

// NewHandler creates a new WebSocket handler.
func NewHandler(hub *Hub, logger zerolog.Logger) *Handler {
	return &Handler{hub: hub, logger: logger}
}

// RegisterRoutes registers the WebSocket endpoint on the Gin router group.
func (h *Handler) RegisterRoutes(rg *gin.RouterGroup) {
	rg.GET("/ws", h.HandleUpgrade)
}

// HandleUpgrade upgrades an HTTP connection to WebSocket.
func (h *Handler) HandleUpgrade(c *gin.Context) {
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		h.logger.Error().Err(err).Msg("ws upgrade failed")
		return
	}

	// Extract user address from JWT if present (optional auth).
	userAddress, _ := c.Get("user_address")
	addr, _ := userAddress.(string)

	client := &Client{
		hub:         h.hub,
		conn:        conn,
		send:        make(chan []byte, sendBufferSize),
		topics:      make(map[Topic]bool),
		userAddress: addr,
	}

	h.hub.register <- client

	go h.writePump(client)
	go h.readPump(client)
}

// readPump reads messages from the WebSocket connection.
func (h *Handler) readPump(c *Client) {
	defer func() {
		h.hub.unregister <- c
		c.conn.Close()
	}()

	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				h.logger.Warn().Err(err).Str("user", c.userAddress).Msg("ws unexpected close")
			}
			return
		}

		var cmd ClientCommand
		if err := json.Unmarshal(message, &cmd); err != nil {
			h.logger.Debug().Err(err).Msg("ws invalid command")
			continue
		}

		switch cmd.Action {
		case "subscribe":
			if isValidTopic(cmd.Topic) {
				h.hub.Subscribe(c, cmd.Topic)
				h.sendAck(c, "subscribed", cmd.Topic)
			}
		case "unsubscribe":
			h.hub.Unsubscribe(c, cmd.Topic)
			h.sendAck(c, "unsubscribed", cmd.Topic)
		default:
			h.logger.Debug().Str("action", cmd.Action).Msg("ws unknown action")
		}
	}
}

// writePump writes messages to the WebSocket connection.
func (h *Handler) writePump(c *Client) {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := c.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			w.Write(message)

			// Batch queued messages into the same frame.
			n := len(c.send)
			for i := 0; i < n; i++ {
				w.Write([]byte("\n"))
				w.Write(<-c.send)
			}

			if err := w.Close(); err != nil {
				return
			}

		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (h *Handler) sendAck(c *Client, action string, topic Topic) {
	ack := Message{
		Topic:     topic,
		Type:      action,
		Data:      json.RawMessage(`{}`),
		Timestamp: time.Now().UnixMilli(),
	}
	payload, _ := json.Marshal(ack)
	select {
	case c.send <- payload:
	default:
	}
}

func isValidTopic(t Topic) bool {
	switch t {
	case TopicPrices, TopicPositions, TopicNotifications, TopicMarkets:
		return true
	}
	return false
}
