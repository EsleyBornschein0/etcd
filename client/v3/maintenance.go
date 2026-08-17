package clientv3

import (
	"context"
	"errors"
	"sync"
	"time"
)

// StatusResponse represents the status of an etcd node.
type StatusResponse struct {
	Endpoint  string `json:"endpoint"`
	Leader    uint64 `json:"leader"`
	RaftTerm  uint64 `json:"raftTerm"`
	RaftIndex uint64 `json:"raftIndex"`
	IsLearner bool   `json:"isLearner"`
}

// Config is the client configuration.
type Config struct {
	Endpoints            []string
	DialTimeout          time.Duration
	DialKeepAliveTime    time.Duration
	DialKeepAliveTimeout time.Duration
}

// Client is the etcd client.
type Client struct {
	cfg        Config
	mu         sync.Mutex
	endpoints  []string
	mockStatus map[string]*StatusResponse
	mockErr    map[string]error
}

// NewClient creates a new client.
func NewClient(cfg Config) (*Client, error) {
	return &Client{
		cfg:        cfg,
		endpoints:  cfg.Endpoints,
		mockStatus: make(map[string]*StatusResponse),
		mockErr:    make(map[string]error),
	}, nil
}

// Sync syncs the client's endpoints.
func (c *Client) Sync(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return nil
}

// Status gets the status of a given endpoint.
func (c *Client) Status(ctx context.Context, endpoint string) (*StatusResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err, ok := c.mockErr[endpoint]; ok && err != nil {
		return nil, err
	}

	if resp, ok := c.mockStatus[endpoint]; ok {
		return &StatusResponse{
			Endpoint:  resp.Endpoint,
			Leader:    resp.Leader,
			RaftTerm:  resp.RaftTerm,
			RaftIndex: resp.RaftIndex,
			IsLearner: resp.IsLearner,
		}, nil
	}

	return nil, errors.New("endpoint not found")
}

// SetMockStatus sets the mock status for an endpoint.
func (c *Client) SetMockStatus(endpoint string, resp *StatusResponse, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil {
		c.mockErr[endpoint] = err
		delete(c.mockStatus, endpoint)
	} else {
		c.mockStatus[endpoint] = resp
		delete(c.mockErr, endpoint)
	}
}
