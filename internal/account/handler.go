package account

import (
	"net/http"
	"strconv"

	"github.com/ethereum/go-ethereum/common"
	"github.com/gin-gonic/gin"
	"github.com/zhangjinge/defi-lending-backend/internal/api/middleware"
)

// Handler provides HTTP endpoints for account operations.
type Handler struct {
	svc         *Service
	authCfg     middleware.AuthConfig
	poolAddress common.Address
}

func NewHandler(svc *Service, authCfg middleware.AuthConfig, poolAddress common.Address) *Handler {
	return &Handler{svc: svc, authCfg: authCfg, poolAddress: poolAddress}
}

// RegisterRoutes sets up account API routes.
func (h *Handler) RegisterRoutes(rg *gin.RouterGroup) {
	rg.POST("/auth/login", h.Login)

	authed := rg.Group("", middleware.JWTAuthMiddleware(h.authCfg))
	authed.GET("/account/positions", h.GetPositions)
	authed.GET("/account/history", h.GetHistory)
	authed.POST("/account/tx/build", h.BuildTx)
}

// Login handles EIP-4361 sign-in.
func (h *Handler) Login(c *gin.Context) {
	var req middleware.LoginRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		return
	}

	// Verify signature.
	recovered, err := middleware.VerifyEIP4361Signature(req.Message, req.Signature)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid signature"})
		return
	}

	// Verify recovered address matches the one in the SIWE message.
	claimedAddr, err := middleware.ExtractAddressFromSIWEMessage(req.Message)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid SIWE message"})
		return
	}

	if !common.IsHexAddress(claimedAddr) || common.HexToAddress(claimedAddr) != recovered {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "address mismatch"})
		return
	}

	// Generate JWT.
	token, err := middleware.GenerateJWT(h.authCfg, recovered.Hex())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to generate token"})
		return
	}

	c.JSON(http.StatusOK, middleware.LoginResponse{
		Token:   token,
		Address: recovered.Hex(),
	})
}

// GetPositions returns the authenticated user's positions.
func (h *Handler) GetPositions(c *gin.Context) {
	address, _ := c.Get("wallet_address")
	chainID, _ := strconv.ParseInt(c.DefaultQuery("chain_id", "0"), 10, 64)

	overview, err := h.svc.GetOverview(c.Request.Context(), address.(string), chainID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get positions"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"data": overview})
}

// GetHistory returns the authenticated user's transaction history.
func (h *Handler) GetHistory(c *gin.Context) {
	address, _ := c.Get("wallet_address")
	chainID, _ := strconv.ParseInt(c.DefaultQuery("chain_id", "0"), 10, 64)
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "20"))
	offset, _ := strconv.Atoi(c.DefaultQuery("offset", "0"))

	entries, err := h.svc.GetHistory(c.Request.Context(), address.(string), chainID, limit, offset)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get history"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"data": entries})
}

// BuildTx constructs a transaction for the user to sign.
func (h *Handler) BuildTx(c *gin.Context) {
	var req TxBuildRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		return
	}

	resp, err := h.svc.BuildTransaction(req, h.poolAddress)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"data": resp})
}
