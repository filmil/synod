package server

import (
	"context"
	"fmt"
	"net"

	"github.com/filmil/synod/internal/paxos"
	"github.com/filmil/synod/internal/state"
	paxosv1 "github.com/filmil/synod/proto/paxos/v1"
	"google.golang.org/grpc"
	"github.com/golang/glog"
)

type PaxosServer struct {
	paxosv1.UnimplementedPaxosServiceServer
	acceptor *paxos.Acceptor
	store    *state.Store
}

func NewPaxosServer(store *state.Store, acceptor *paxos.Acceptor) *PaxosServer {
	return &PaxosServer{
		acceptor: acceptor,
		store:    store,
	}
}

func (s *PaxosServer) Prepare(ctx context.Context, req *paxosv1.PrepareRequest) (*paxosv1.PromiseResponse, error) {
	return s.acceptor.Prepare(ctx, req)
}

func (s *PaxosServer) Accept(ctx context.Context, req *paxosv1.AcceptRequest) (*paxosv1.AcceptedResponse, error) {
	return s.acceptor.Accept(ctx, req)
}

func (s *PaxosServer) JoinCluster(ctx context.Context, req *paxosv1.JoinClusterRequest) (*paxosv1.JoinClusterResponse, error) {
	// Implementation for dynamic membership will go here.
	return &paxosv1.JoinClusterResponse{Success: true, Message: "Join accepted"}, nil
}

func (s *PaxosServer) Sync(ctx context.Context, req *paxosv1.SyncRequest) (*paxosv1.SyncResponse, error) {
	// Implementation for sync will go here.
	return &paxosv1.SyncResponse{}, nil
}

func RunGRPCServer(addr string, srv *PaxosServer) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	s := grpc.NewServer()
	paxosv1.RegisterPaxosServiceServer(s, srv)
	glog.Infof("gRPC server listening at %v", lis.Addr())
	return s.Serve(lis)
}
