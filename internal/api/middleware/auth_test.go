package middleware

import (
	"crypto/ecdsa"
	"fmt"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testAuthConfig() AuthConfig {
	return AuthConfig{
		Secret:     "test-secret-key-for-jwt-signing",
		Issuer:     "defi-lending-test",
		Expiration: time.Hour,
	}
}

func TestGenerateAndValidateJWT(t *testing.T) {
	cfg := testAuthConfig()
	address := "0x1234567890abcdef1234567890abcdef12345678"

	token, err := GenerateJWT(cfg, address)
	require.NoError(t, err)
	require.NotEmpty(t, token)

	claims, err := ValidateJWT(cfg, token)
	require.NoError(t, err)
	assert.Equal(t, address, claims.Address)
	assert.Equal(t, cfg.Issuer, claims.Issuer)
}

func TestValidateJWT_InvalidSecret(t *testing.T) {
	cfg := testAuthConfig()
	token, err := GenerateJWT(cfg, "0xabc")
	require.NoError(t, err)

	wrongCfg := AuthConfig{Secret: "wrong-secret", Issuer: cfg.Issuer, Expiration: cfg.Expiration}
	_, err = ValidateJWT(wrongCfg, token)
	assert.Error(t, err)
}

func TestValidateJWT_Expired(t *testing.T) {
	cfg := AuthConfig{
		Secret:     "test-secret",
		Issuer:     "test",
		Expiration: -1 * time.Hour, // already expired
	}

	token, err := GenerateJWT(cfg, "0xabc")
	require.NoError(t, err)

	_, err = ValidateJWT(cfg, token)
	assert.Error(t, err)
}

func TestVerifyEIP4361Signature(t *testing.T) {
	// Generate a test key pair.
	privateKey, err := crypto.GenerateKey()
	require.NoError(t, err)

	address := crypto.PubkeyToAddress(privateKey.PublicKey)
	message := fmt.Sprintf("localhost wants you to sign in with your Ethereum account:\n%s\n\nSign in to DeFi Lending\n\nURI: http://localhost:8080\nVersion: 1\nChain ID: 1\nNonce: abc123\nIssued At: 2024-01-01T00:00:00Z", address.Hex())

	// Sign the message.
	prefixed := fmt.Sprintf("\x19Ethereum Signed Message:\n%d%s", len(message), message)
	hash := crypto.Keccak256Hash([]byte(prefixed))
	sig, err := crypto.Sign(hash.Bytes(), privateKey)
	require.NoError(t, err)

	// Adjust V to Ethereum convention.
	sig[64] += 27

	sigHex := "0x" + fmt.Sprintf("%x", sig)

	recovered, err := VerifyEIP4361Signature(message, sigHex)
	require.NoError(t, err)
	assert.Equal(t, address, recovered)
}

func TestVerifyEIP4361Signature_InvalidSig(t *testing.T) {
	_, err := VerifyEIP4361Signature("some message", "0xinvalid")
	assert.Error(t, err)
}

func TestExtractAddressFromSIWEMessage(t *testing.T) {
	message := "localhost wants you to sign in with your Ethereum account:\n0x1234567890AbcdEF1234567890aBcdef12345678\n\nSign in"

	addr, err := ExtractAddressFromSIWEMessage(message)
	require.NoError(t, err)
	assert.Equal(t, "0x1234567890AbcdEF1234567890aBcdef12345678", addr)
}

func TestExtractAddressFromSIWEMessage_Invalid(t *testing.T) {
	_, err := ExtractAddressFromSIWEMessage("bad format")
	assert.Error(t, err)
}

// Suppress unused import warning for ecdsa.
var _ *ecdsa.PrivateKey
