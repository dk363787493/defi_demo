package market

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
)

// Handler provides HTTP endpoints for market data.
type Handler struct {
	svc *Service
}

func NewHandler(svc *Service) *Handler {
	return &Handler{svc: svc}
}

// RegisterRoutes sets up market API routes on the given router group.
func (h *Handler) RegisterRoutes(rg *gin.RouterGroup) {
	rg.GET("/markets", h.ListMarkets)
	rg.GET("/markets/:market_id", h.GetMarketDetail)
	rg.GET("/markets/:market_id/history", h.GetMarketHistory)
}

// ListMarkets returns all markets, optionally filtered by chain_id query param.
func (h *Handler) ListMarkets(c *gin.Context) {
	var chainID int64
	if cid := c.Query("chain_id"); cid != "" {
		var err error
		chainID, err = strconv.ParseInt(cid, 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid chain_id"})
			return
		}
	}

	markets, err := h.svc.ListMarkets(c.Request.Context(), chainID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list markets"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"data": markets})
}

// GetMarketDetail returns a single market's current state.
func (h *Handler) GetMarketDetail(c *gin.Context) {
	marketID, err := strconv.ParseInt(c.Param("market_id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid market_id"})
		return
	}

	state, err := h.svc.GetMarketDetail(c.Request.Context(), marketID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "market not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"data": state})
}

// GetMarketHistory returns historical market snapshots.
func (h *Handler) GetMarketHistory(c *gin.Context) {
	marketID, err := strconv.ParseInt(c.Param("market_id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid market_id"})
		return
	}

	period := c.DefaultQuery("period", "7d")
	interval := c.DefaultQuery("interval", "1h")

	snapshots, err := h.svc.GetHistory(c.Request.Context(), marketID, period, interval)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get history"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"data": snapshots})
}
