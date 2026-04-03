package oracle

import (
	"context"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockRedisClient implements a simple in-memory cache for testing.
type mockRedisClient struct {
	store map[string]string
}

func newMockRedisClient() *mockRedisClient {
	return &mockRedisClient{store: make(map[string]string)}
}

func (m *mockRedisClient) Get(_ context.Context, key string) (string, error) {
	v, ok := m.store[key]
	if !ok {
		return "", fmt.Errorf("key not found")
	}
	return v, nil
}

func (m *mockRedisClient) Set(_ context.Context, key string, value string, _ time.Duration) error {
	m.store[key] = value
	return nil
}

func TestPriceCache_SetAndGet(t *testing.T) {
	rc := newMockRedisClient()
	cache := NewPriceCache(rc)

	price := &PriceData{
		AssetAddress: common.HexToAddress("0x1111111111111111111111111111111111111111"),
		PriceUSD:     big.NewInt(200000000000),
		Decimals:     8,
		Source:       SourceChainlink,
		RoundID:      big.NewInt(100),
		UpdatedAt:    time.Unix(1000, 0),
	}

	err := cache.Set(context.Background(), price, 30*time.Second)
	require.NoError(t, err)

	got, err := cache.Get(context.Background(), price.AssetAddress)
	require.NoError(t, err)
	assert.Equal(t, price.PriceUSD.String(), got.PriceUSD.String())
	assert.Equal(t, price.Source, got.Source)
}

func TestPriceCache_GetMiss(t *testing.T) {
	rc := newMockRedisClient()
	cache := NewPriceCache(rc)

	_, err := cache.Get(context.Background(), common.HexToAddress("0x9999"))
	assert.Error(t, err)
}
