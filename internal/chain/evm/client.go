package evm

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/zhangjinge/defi-lending-backend/internal/common/config"
	ctypes "github.com/zhangjinge/defi-lending-backend/internal/common/types"
)

// Client implements chain.ChainAdapter for EVM-compatible chains.
type Client struct {
	chainID    ctypes.ChainID
	httpClient *ethclient.Client
	wsClient   *ethclient.Client
	cfg        config.ChainConfig
}

func NewClient(cfg config.ChainConfig) (*Client, error) {
	if len(cfg.RPCURLs) == 0 {
		return nil, fmt.Errorf("no RPC URLs configured for chain %s", cfg.Name)
	}

	httpClient, err := ethclient.Dial(cfg.RPCURLs[0])
	if err != nil {
		return nil, fmt.Errorf("dial http rpc %s: %w", cfg.RPCURLs[0], err)
	}

	var wsClient *ethclient.Client
	if cfg.WSURL != "" {
		wsClient, err = ethclient.Dial(cfg.WSURL)
		if err != nil {
			httpClient.Close()
			return nil, fmt.Errorf("dial ws rpc %s: %w", cfg.WSURL, err)
		}
	}

	return &Client{
		chainID:    ctypes.ChainID(cfg.ChainID),
		httpClient: httpClient,
		wsClient:   wsClient,
		cfg:        cfg,
	}, nil
}

func (c *Client) ChainID() ctypes.ChainID {
	return c.chainID
}

func (c *Client) LatestBlockNumber(ctx context.Context) (uint64, error) {
	return c.httpClient.BlockNumber(ctx)
}

func (c *Client) BlockByNumber(ctx context.Context, number *big.Int) (*types.Header, error) {
	return c.httpClient.HeaderByNumber(ctx, number)
}

func (c *Client) SubscribeNewHead(ctx context.Context) (chan *types.Header, ethereum.Subscription, error) {
	if c.wsClient == nil {
		return nil, nil, fmt.Errorf("no WebSocket client configured for chain %d", c.chainID)
	}
	headers := make(chan *types.Header, 16)
	sub, err := c.wsClient.SubscribeNewHead(ctx, headers)
	if err != nil {
		return nil, nil, fmt.Errorf("subscribe new head: %w", err)
	}
	return headers, sub, nil
}

func (c *Client) FilterLogs(ctx context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
	return c.httpClient.FilterLogs(ctx, query)
}

func (c *Client) CallContract(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
	return c.httpClient.CallContract(ctx, msg, blockNumber)
}

func (c *Client) SendTransaction(ctx context.Context, tx *types.Transaction) error {
	return c.httpClient.SendTransaction(ctx, tx)
}

func (c *Client) SuggestGasPrice(ctx context.Context) (*big.Int, error) {
	return c.httpClient.SuggestGasPrice(ctx)
}

func (c *Client) PendingNonceAt(ctx context.Context, addr common.Address) (uint64, error) {
	return c.httpClient.PendingNonceAt(ctx, addr)
}

func (c *Client) Close() {
	if c.httpClient != nil {
		c.httpClient.Close()
	}
	if c.wsClient != nil {
		c.wsClient.Close()
	}
}
