package middleware

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// RateLimitStore abstracts Redis for rate limiting.
type RateLimitStore interface {
	Increment(ctx context.Context, key string, window time.Duration) (int64, error)
}

// RateLimitConfig holds rate limiter settings.
type RateLimitConfig struct {
	RequestsPerMinute int
	Store             RateLimitStore
}

// RateLimitMiddleware limits requests per IP per minute using a sliding window.
func RateLimitMiddleware(cfg RateLimitConfig) gin.HandlerFunc {
	return func(c *gin.Context) {
		if cfg.Store == nil {
			c.Next()
			return
		}

		ip := c.ClientIP()
		key := fmt.Sprintf("ratelimit:%s:%d", ip, time.Now().Unix()/60)

		count, err := cfg.Store.Increment(c.Request.Context(), key, time.Minute)
		if err != nil {
			// On Redis failure, allow the request (fail open).
			c.Next()
			return
		}

		if count > int64(cfg.RequestsPerMinute) {
			c.JSON(http.StatusTooManyRequests, gin.H{"error": "rate limit exceeded"})
			c.Abort()
			return
		}

		c.Next()
	}
}
