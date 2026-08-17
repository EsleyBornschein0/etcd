package command

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/EsleyBornschein0/etcd/client/v3"
)

// EndpointStatusConfig holds configuration for the endpoint status command.
type EndpointStatusConfig struct {
	Timeout time.Duration
}

// EndpointStatusCommand executes the "endpoint status" command.
// It queries the status of each endpoint, retrying with exponential backoff if a transient state is detected.
func EndpointStatusCommand(ctx context.Context, client *clientv3.Client, endpoints []string, out io.Writer, cfg EndpointStatusConfig) error {
	if cfg.Timeout == 0 {
		cfg.Timeout = 5 * time.Second
	}

	responses, err := getEndpointStatusWithRetry(ctx, client, endpoints, cfg.Timeout)
	if err != nil {
		return fmt.Errorf("failed to get endpoint status: %w", err)
	}

	return printStatus(responses, out)
}

func getEndpointStatusWithRetry(ctx context.Context, client *clientv3.Client, endpoints []string, timeout time.Duration) ([]*clientv3.StatusResponse, error) {
	var responses []*clientv3.StatusResponse
	var lastErr error

	start := time.Now()
	backoff := 100 * time.Millisecond
	maxBackoff := 1 * time.Second

	for {
		responses = nil
		lastErr = nil
		termMismatch := false
		hasZeroLeader := false
		var commonTerm uint64
		termSet := false

		// Force sync before querying status to refresh client's internal endpoint list
		if syncErr := client.Sync(ctx); syncErr != nil {
			// Log or handle sync error, but continue to try getting status
		}

		for _, ep := range endpoints {
			resp, err := client.Status(ctx, ep)
			if err != nil {
				lastErr = err
				break
			}
			if resp.Leader == 0 {
				hasZeroLeader = true
			}
			if !termSet {
				commonTerm = resp.RaftTerm
				termSet = true
			} else if resp.RaftTerm != commonTerm {
				termMismatch = true
			}
			responses = append(responses, resp)
		}

		// Check if we have a valid, converged state (no errors, no zero leader, and terms match)
		if lastErr == nil && !hasZeroLeader && !termMismatch && len(responses) == len(endpoints) {
			return responses, nil
		}

		// If we exceeded the timeout, return what we have or the last error
		if time.Since(start) >= timeout {
			if lastErr != nil {
				return nil, lastErr
			}
			return responses, nil
		}

		// Backoff with exponential delay
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

func printStatus(responses []*clientv3.StatusResponse, out io.Writer) error {
	type DisplayStatus struct {
		Endpoint  string `json:"endpoint"`
		Leader    string `json:"leader"` // Use string to handle "None" or "0" gracefully
		RaftTerm  uint64 `json:"raftTerm"`
		RaftIndex uint64 `json:"raftIndex"`
		IsLearner bool   `json:"isLearner"`
	}

	displayList := make([]DisplayStatus, len(responses))
	for i, resp := range responses {
		leaderStr := fmt.Sprintf("%d", resp.Leader)
		if resp.Leader == 0 {
			leaderStr = "None (Candidate/PreCandidate)"
		}
		displayList[i] = DisplayStatus{
			Endpoint:  resp.Endpoint,
			Leader:    leaderStr,
			RaftTerm:  resp.RaftTerm,
			RaftIndex: resp.RaftIndex,
			IsLearner: resp.IsLearner,
		}
	}

	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	return encoder.Encode(displayList)
}
