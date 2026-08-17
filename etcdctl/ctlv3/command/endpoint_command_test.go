package command

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/EsleyBornschein0/etcd/client/v3"
)

func TestEndpointStatusCommand_Converged(t *testing.T) {
	endpoints := []string{"127.0.0.1:2379", "127.0.0.1:22379", "127.0.0.1:32379"}
	client, err := clientv3.NewClient(clientv3.Config{
		Endpoints:            endpoints,
		DialKeepAliveTime:    1 * time.Second,
		DialKeepAliveTimeout: 1 * time.Second,
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	for _, ep := range endpoints {
		client.SetMockStatus(ep, &clientv3.StatusResponse{
			Endpoint:  ep,
			Leader:    12345,
			RaftTerm:  2,
			RaftIndex: 100,
		}, nil)
	}

	var buf bytes.Buffer
	ctx := context.Background()
	err = EndpointStatusCommand(ctx, client, endpoints, &buf, EndpointStatusConfig{Timeout: 1 * time.Second})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	var status []map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &status); err != nil {
		t.Fatalf("failed to unmarshal output: %v", err)
	}

	if len(status) != 3 {
		t.Errorf("expected 3 status responses, got %d", len(status))
	}

	for _, s := range status {
		if s["leader"] != "12345" {
			t.Errorf("expected leader 12345, got %v", s["leader"])
		}
		if s["raftTerm"] != float64(2) {
			t.Errorf("expected raftTerm 2, got %v", s["raftTerm"])
		}
	}
}

func TestEndpointStatusCommand_TransientLeaderZero(t *testing.T) {
	endpoints := []string{"127.0.0.1:2379"}
	client, err := clientv3.NewClient(clientv3.Config{
		Endpoints: endpoints,
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	client.SetMockStatus(endpoints[0], &clientv3.StatusResponse{
		Endpoint:  endpoints[0],
		Leader:    0,
		RaftTerm:  2,
		RaftIndex: 100,
	}, nil)

	go func() {
		time.Sleep(200 * time.Millisecond)
		client.SetMockStatus(endpoints[0], &clientv3.StatusResponse{
			Endpoint:  endpoints[0],
			Leader:    12345,
			RaftTerm:  2,
			RaftIndex: 101,
		}, nil)
	}()

	var buf bytes.Buffer
	ctx := context.Background()
	err = EndpointStatusCommand(ctx, client, endpoints, &buf, EndpointStatusConfig{Timeout: 1 * time.Second})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	var status []map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &status); err != nil {
		t.Fatalf("failed to unmarshal output: %v", err)
	}

	if status[0]["leader"] != "12345" {
		t.Errorf("expected leader to converge to 12345, got %v", status[0]["leader"])
	}
}

func TestEndpointStatusCommand_TransientTermMismatch(t *testing.T) {
	endpoints := []string{"127.0.0.1:2379", "127.0.0.1:22379"}
	client, err := clientv3.NewClient(clientv3.Config{
		Endpoints: endpoints,
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	client.SetMockStatus(endpoints[0], &clientv3.StatusResponse{Endpoint: endpoints[0], Leader: 1, RaftTerm: 2}, nil)
	client.SetMockStatus(endpoints[1], &clientv3.StatusResponse{Endpoint: endpoints[1], Leader: 1, RaftTerm: 1}, nil)

	go func() {
		time.Sleep(200 * time.Millisecond)
		client.SetMockStatus(endpoints[1], &clientv3.StatusResponse{Endpoint: endpoints[1], Leader: 1, RaftTerm: 2}, nil)
	}()

	var buf bytes.Buffer
	ctx := context.Background()
	err = EndpointStatusCommand(ctx, client, endpoints, &buf, EndpointStatusConfig{Timeout: 1 * time.Second})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	var status []map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &status); err != nil {
		t.Fatalf("failed to unmarshal output: %v", err)
	}

	if status[0]["raftTerm"] != float64(2) || status[1]["raftTerm"] != float64(2) {
		t.Errorf("expected terms to converge to 2, got %v and %v", status[0]["raftTerm"], status[1]["raftTerm"])
	}
}

func TestEndpointStatusCommand_CandidateStateGraceful(t *testing.T) {
	endpoints := []string{"127.0.0.1:2379"}
	client, err := clientv3.NewClient(clientv3.Config{
		Endpoints: endpoints,
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	client.SetMockStatus(endpoints[0], &clientv3.StatusResponse{
		Endpoint:  endpoints[0],
		Leader:    0,
		RaftTerm:  3,
		RaftIndex: 150,
	}, nil)

	var buf bytes.Buffer
	ctx := context.Background()
	err = EndpointStatusCommand(ctx, client, endpoints, &buf, EndpointStatusConfig{Timeout: 200 * time.Millisecond})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	var status []map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &status); err != nil {
		t.Fatalf("failed to unmarshal output: %v", err)
	}

	if !strings.Contains(status[0]["leader"].(string), "None") {
		t.Errorf("expected leader to be handled gracefully as None, got %v", status[0]["leader"])
	}
}

func TestEndpointStatusCommand_TransientError(t *testing.T) {
	endpoints := []string{"127.0.0.1:2379"}
	client, err := clientv3.NewClient(clientv3.Config{
		Endpoints: endpoints,
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	client.SetMockStatus(endpoints[0], nil, errors.New("connection refused"))

	go func() {
		time.Sleep(200 * time.Millisecond)
		client.SetMockStatus(endpoints[0], &clientv3.StatusResponse{
			Endpoint:  endpoints[0],
			Leader:    1,
			RaftTerm:  2,
			RaftIndex: 100,
		}, nil)
	}()

	var buf bytes.Buffer
	ctx := context.Background()
	err = EndpointStatusCommand(ctx, client, endpoints, &buf, EndpointStatusConfig{Timeout: 1 * time.Second})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	var status []map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &status); err != nil {
		t.Fatalf("failed to unmarshal output: %v", err)
	}

	if status[0]["leader"] != "1" {
		t.Errorf("expected leader to be 1, got %v", status[0]["leader"])
	}
}
