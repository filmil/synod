package server

import (
	"context"
	"fmt"

	paxosv1 "github.com/filmil/synod/proto/paxos/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type PaxosClient struct {
	agentID string
	client  paxosv1.PaxosServiceClient
	conn    *grpc.ClientConn
}

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

func (c *PaxosClient) Prepare(ctx context.Context, req *paxosv1.PrepareRequest) (*paxosv1.PromiseResponse, error) {
	return c.client.Prepare(ctx, req)
}

func (c *PaxosClient) Accept(ctx context.Context, req *paxosv1.AcceptRequest) (*paxosv1.AcceptedResponse, error) {
	return c.client.Accept(ctx, req)
}

func (c *PaxosClient) JoinCluster(ctx context.Context, req *paxosv1.JoinClusterRequest) (*paxosv1.JoinClusterResponse, error) {
	return c.client.JoinCluster(ctx, req)
}

func (c *PaxosClient) Sync(ctx context.Context, req *paxosv1.SyncRequest) (*paxosv1.SyncResponse, error) {
	return c.client.Sync(ctx, req)
}

func (c *PaxosClient) GetKVEntry(ctx context.Context, req *paxosv1.GetKVEntryRequest) (*paxosv1.GetKVEntryResponse, error) {
	return c.client.GetKVEntry(ctx, req)
}

func (c *PaxosClient) AgentID() string {
	return c.agentID
}

func (c *PaxosClient) Close() error {
	return c.conn.Close()
}
