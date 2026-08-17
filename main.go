package main

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/EsleyBornschein0/etcd/client/v3"
	"github.com/EsleyBornschein0/etcd/etcdctl/ctlv3/command"
)

func main() {
	fmt.Println("Starting etcdctl endpoint status simulation...")

	endpoints := []string{"127.0.0.1:2379", "127.0.0.1:22379", "127.0.0.1:32379"}
	client, err := clientv3.NewClient(clientv3.Config{
		Endpoints:            endpoints,
		DialKeepAliveTime:    1 * time.Second,
		DialKeepAliveTimeout: 1 * time.Second,
	})
	if err != nil {
		fmt.Printf("Failed to create client: %v\n", err)
		return
	}

	fmt.Println("\n[Simulation] Simulating transient leader election...")
	client.SetMockStatus(endpoints[0], &clientv3.StatusResponse{Endpoint: endpoints[0], Leader: 0, RaftTerm: 1, RaftIndex: 100}, nil)
	client.SetMockStatus(endpoints[1], &clientv3.StatusResponse{Endpoint: endpoints[1], Leader: 0, RaftTerm: 2, RaftIndex: 105}, nil)
	client.SetMockStatus(endpoints[2], &clientv3.StatusResponse{Endpoint: endpoints[2], Leader: 0, RaftTerm: 2, RaftIndex: 105}, nil)

	go func() {
		time.Sleep(1500 * time.Millisecond)
		fmt.Println("\n[Simulation] Cluster converged! New leader elected: 999, Raft Term: 3")
		for _, ep := range endpoints {
			client.SetMockStatus(ep, &clientv3.StatusResponse{
				Endpoint:  ep,
				Leader:    999,
				RaftTerm:  3,
				RaftIndex: 110,
			}, nil)
		}
	}()

	var buf bytes.Buffer
	ctx := context.Background()
	
	fmt.Println("Running: etcdctl endpoint status...")
	start := time.Now()
	err = command.EndpointStatusCommand(ctx, client, endpoints, &buf, command.EndpointStatusConfig{Timeout: 5 * time.Second})
	duration := time.Since(start)

	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Command finished in %v\n", duration)
	fmt.Println("Output:")
	fmt.Println(buf.String())
}
