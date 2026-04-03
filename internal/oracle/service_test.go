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

type mockPublisher struct {
	published []*PriceData
}

func (m *mockPublisher) PublishPrice(_ context.Context, price *PriceData) error {
	m.published = append(m.published, price)
	return nil
}

type mockPriceStore struct {
	prices []*PriceData
}

func (m *mockPriceStore) SavePrice(_ context.Context, _ int64, price *PriceData) error {
	m.prices = append(m.prices, price)
	return nil
}

type mockChainlinkFetcher struct {
	prices map[common.Address]*PriceData
}

func (m *mockChainlinkFetcher) FetchPrice(_ context.Context, feed PriceFeedConfig) (*PriceData, error) {
	if p, ok := m.prices[feed.AssetAddress]; ok {
		return p, nil
	}
	return nil, fmt.Errorf("no price for %s", feed.AssetAddress.Hex())
}

func TestOracleService_FetchAndCache(t *testing.T) {
	asset := common.HexToAddress("0x1111111111111111111111111111111111111111")
	mockPrice := &PriceData{
		AssetAddress: asset,
		PriceUSD:     big.NewInt(200000000000),
		Decimals:     8,
		Source:       SourceChainlink,
		RoundID:      big.NewInt(100),
		UpdatedAt:    time.Now(),
	}

	fetcher := &mockChainlinkFetcher{
		prices: map[common.Address]*PriceData{asset: mockPrice},
	}
	cache := NewPriceCache(newMockRedisClient())
	publisher := &mockPublisher{}
	store := &mockPriceStore{}

	svc := NewService(fetcher, cache, publisher, store, 1)

	feeds := []PriceFeedConfig{{
		AssetAddress: asset,
		AssetSymbol:  "ETH",
		FeedAddress:  common.HexToAddress("0xfeed"),
		Decimals:     8,
		HeartbeatSec: 3600,
	}}

	err := svc.FetchAndUpdate(context.Background(), feeds)
	require.NoError(t, err)

	cached, err := cache.Get(context.Background(), asset)
	require.NoError(t, err)
	assert.Equal(t, mockPrice.PriceUSD.String(), cached.PriceUSD.String())
	assert.Len(t, publisher.published, 1)
	assert.Len(t, store.prices, 1)
}

func TestCalculateDeviation(t *testing.T) {
	oldPrice := big.NewInt(200000000000)
	newPrice := big.NewInt(220000000000)
	deviation := CalculateDeviation(oldPrice, newPrice)
	assert.InDelta(t, 10.0, deviation, 0.01)
}

func TestCalculateDeviation_Zero(t *testing.T) {
	deviation := CalculateDeviation(big.NewInt(0), big.NewInt(100))
	assert.Equal(t, 0.0, deviation)
}
