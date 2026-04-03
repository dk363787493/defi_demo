package middleware

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
)

// AuthConfig holds JWT configuration.
type AuthConfig struct {
	Secret     string
	Issuer     string
	Expiration time.Duration
}

// Claims is the JWT payload containing the wallet address.
type Claims struct {
	Address string `json:"address"`
	jwt.RegisteredClaims
}

// LoginRequest is the body for POST /auth/login.
type LoginRequest struct {
	Message   string `json:"message" binding:"required"`   // EIP-4361 message
	Signature string `json:"signature" binding:"required"` // hex-encoded signature
}

// LoginResponse is returned on successful login.
type LoginResponse struct {
	Token   string `json:"token"`
	Address string `json:"address"`
}

// GenerateJWT creates a signed JWT for the given wallet address.
func GenerateJWT(cfg AuthConfig, address string) (string, error) {
	now := time.Now()
	claims := Claims{
		Address: strings.ToLower(address),
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    cfg.Issuer,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(cfg.Expiration)),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString([]byte(cfg.Secret))
}

// ValidateJWT parses and validates a JWT, returning the claims.
func ValidateJWT(cfg AuthConfig, tokenStr string) (*Claims, error) {
	token, err := jwt.ParseWithClaims(tokenStr, &Claims{}, func(t *jwt.Token) (interface{}, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		return []byte(cfg.Secret), nil
	})
	if err != nil {
		return nil, fmt.Errorf("parse jwt: %w", err)
	}

	claims, ok := token.Claims.(*Claims)
	if !ok || !token.Valid {
		return nil, fmt.Errorf("invalid token")
	}

	return claims, nil
}

// VerifyEIP4361Signature verifies an EIP-4361 (Sign-In with Ethereum) signature.
// Returns the recovered signer address.
func VerifyEIP4361Signature(message string, signatureHex string) (common.Address, error) {
	sigBytes, err := hexutil.Decode(signatureHex)
	if err != nil {
		return common.Address{}, fmt.Errorf("decode signature: %w", err)
	}

	if len(sigBytes) != 65 {
		return common.Address{}, fmt.Errorf("invalid signature length: %d", len(sigBytes))
	}

	// Adjust V value: Ethereum signatures use V=27/28, but crypto.Ecrecover expects 0/1.
	if sigBytes[64] >= 27 {
		sigBytes[64] -= 27
	}

	// Hash the message with Ethereum prefix.
	prefixed := fmt.Sprintf("\x19Ethereum Signed Message:\n%d%s", len(message), message)
	hash := crypto.Keccak256Hash([]byte(prefixed))

	pubKey, err := crypto.Ecrecover(hash.Bytes(), sigBytes)
	if err != nil {
		return common.Address{}, fmt.Errorf("ecrecover: %w", err)
	}

	pubKeyECDSA, err := crypto.UnmarshalPubkey(pubKey)
	if err != nil {
		return common.Address{}, fmt.Errorf("unmarshal pubkey: %w", err)
	}

	return crypto.PubkeyToAddress(*pubKeyECDSA), nil
}

// ExtractAddressFromSIWEMessage extracts the Ethereum address from an EIP-4361 message.
// The message format includes a line like: "0x1234...abcd wants you to sign in..."
func ExtractAddressFromSIWEMessage(message string) (string, error) {
	// EIP-4361 format: "<domain> wants you to sign in with your Ethereum account:\n<address>\n..."
	lines := strings.Split(message, "\n")
	if len(lines) < 2 {
		return "", fmt.Errorf("invalid SIWE message format")
	}

	address := strings.TrimSpace(lines[1])
	if !common.IsHexAddress(address) {
		return "", fmt.Errorf("invalid address in SIWE message: %s", address)
	}

	return address, nil
}

// JWTAuthMiddleware returns a Gin middleware that validates JWT tokens.
func JWTAuthMiddleware(cfg AuthConfig) gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "missing authorization header"})
			c.Abort()
			return
		}

		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 || strings.ToLower(parts[0]) != "bearer" {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid authorization format"})
			c.Abort()
			return
		}

		claims, err := ValidateJWT(cfg, parts[1])
		if err != nil {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid token"})
			c.Abort()
			return
		}

		c.Set("wallet_address", claims.Address)
		c.Next()
	}
}
