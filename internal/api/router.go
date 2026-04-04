package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"github.com/zhangjinge/defi-lending-backend/internal/account"
	"github.com/zhangjinge/defi-lending-backend/internal/api/middleware"
	ws "github.com/zhangjinge/defi-lending-backend/internal/api/websocket"
	"github.com/zhangjinge/defi-lending-backend/internal/market"
)

// RouterConfig holds dependencies for building the API router.
type RouterConfig struct {
	MarketHandler  *market.Handler
	AccountHandler *account.Handler
	WSHandler      *ws.Handler
	Logger         zerolog.Logger
	RateLimitCfg   middleware.RateLimitConfig
}

// NewRouter creates and configures the Gin router with all middleware and routes.
func NewRouter(cfg RouterConfig) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()

	// Global middleware.
	r.Use(gin.Recovery())
	r.Use(middleware.CORSMiddleware())
	r.Use(middleware.RequestLoggingMiddleware(cfg.Logger))
	r.Use(middleware.RateLimitMiddleware(cfg.RateLimitCfg))

	// Health check endpoints for K8s.
	r.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})
	r.GET("/readyz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ready"})
	})

	// Prometheus metrics.
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))

	// API v1 routes.
	v1 := r.Group("/api/v1")

	if cfg.MarketHandler != nil {
		cfg.MarketHandler.RegisterRoutes(v1)
	}
	if cfg.AccountHandler != nil {
		cfg.AccountHandler.RegisterRoutes(v1)
	}
	if cfg.WSHandler != nil {
		cfg.WSHandler.RegisterRoutes(v1)
	}

	return r
}
