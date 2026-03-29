package server

import (
	"context"
	"fmt"

	paxosv1 "github.com/filmil/synod/proto/paxos/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// PaxosClient wraps a gRPC connection to a remote Paxos agent.
type PaxosClient struct {
	agentID string
	client  paxosv1.PaxosServiceClient
	conn    *grpc.ClientConn
}

// NewPaxosClient establishes a connection to the specified address.
func NewPaxosClient(agentID string, addr string) (*PaxosClient, error) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to dial: %w", err)
	}
	return &PaxosClient{
		agentID: agentID,
		client:  paxosv1.NewPaxosServiceClient(conn),
		conn:    conn,
	}, nil
}

// Prepare forwards a Prepare request.
func (c *PaxosClient) Prepare(ctx context.Context, req *paxosv1.PrepareRequest) (*paxosv1.PromiseResponse, error) {
	return c.client.Prepare(ctx, req)
}

// Accept forwards an Accept request.
func (c *PaxosClient) Accept(ctx context.Context, req *paxosv1.AcceptRequest) (*paxosv1.AcceptedResponse, error) {
	return c.client.Accept(ctx, req)
}

// JoinCluster forwards a JoinCluster request.
func (c *PaxosClient) JoinCluster(ctx context.Context, req *paxosv1.JoinClusterRequest) (*paxosv1.JoinClusterResponse, error) {
	return c.client.JoinCluster(ctx, req)
}

// Sync forwards a Sync request.
func (c *PaxosClient) Sync(ctx context.Context, req *paxosv1.SyncRequest) (*paxosv1.SyncResponse, error) {
	return c.client.Sync(ctx, req)
}

// GetKVEntry forwards a GetKVEntry request.
func (c *PaxosClient) GetKVEntry(ctx context.Context, req *paxosv1.GetKVEntryRequest) (*paxosv1.GetKVEntryResponse, error) {
	return c.client.GetKVEntry(ctx, req)
}

// Ping forwards a Ping request.
func (c *PaxosClient) Ping(ctx context.Context, req *paxosv1.PingRequest) (*paxosv1.PingResponse, error) {
	return c.client.Ping(ctx, req)
}

// GetPeerEndpoints forwards a GetPeerEndpoints request.
func (c *PaxosClient) GetPeerEndpoints(ctx context.Context, req *paxosv1.GetPeerEndpointsRequest) (*paxosv1.GetPeerEndpointsResponse, error) {
	return c.client.GetPeerEndpoints(ctx, req)
}

// AgentID returns the remote agent's ID.
func (c *PaxosClient) AgentID() string {
	return c.agentID
}

// Close closes the underlying gRPC connection.
func (c *PaxosClient) Close() error {
	return c.conn.Close()
}
