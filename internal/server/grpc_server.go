package server

import (
	"context"
	"net"

	"github.com/filmil/synod/internal/paxos"
	"github.com/filmil/synod/internal/state"
	paxosv1 "github.com/filmil/synod/proto/paxos/v1"
	"google.golang.org/grpc"
	"github.com/golang/glog"
)

type PaxosServer struct {
	paxosv1.UnimplementedPaxosServiceServer
	agentID  string
	acceptor *paxos.Acceptor
	store    *state.Store
	cell     *paxos.Cell
}

func NewPaxosServer(agentID string, store *state.Store, acceptor *paxos.Acceptor, cell *paxos.Cell) *PaxosServer {
	return &PaxosServer{
		agentID:  agentID,
		acceptor: acceptor,
		store:    store,
		cell:     cell,
	}
}

func (s *PaxosServer) Prepare(ctx context.Context, req *paxosv1.PrepareRequest) (*paxosv1.PromiseResponse, error) {
	return s.acceptor.Prepare(ctx, req)
}

func (s *PaxosServer) Accept(ctx context.Context, req *paxosv1.AcceptRequest) (*paxosv1.AcceptedResponse, error) {
	return s.acceptor.Accept(ctx, req)
}

func (s *PaxosServer) JoinCluster(ctx context.Context, req *paxosv1.JoinClusterRequest) (*paxosv1.JoinClusterResponse, error) {
	glog.Infof("gRPC: Received JoinCluster from %s at %s", req.AgentId, req.HostPort)
	info := state.PeerInfo{
		GRPCAddr:  req.HostPort,
		HTTPURL:   req.HttpUrl,
		ShortName: req.ShortName,
	}
	err := s.cell.ProposeMembership(ctx, req.AgentId, info)
	if err != nil {
		glog.Errorf("gRPC: JoinCluster failed: %v", err)
		return &paxosv1.JoinClusterResponse{
			AgentId: s.agentID,
			Success: false,
			Message: err.Error(),
		}, nil
	}
	return &paxosv1.JoinClusterResponse{
		AgentId: s.agentID,
		Success: true,
		Message: "Join accepted",
	}, nil
}

func (s *PaxosServer) Sync(ctx context.Context, req *paxosv1.SyncRequest) (*paxosv1.SyncResponse, error) {
	keys, err := s.cell.GetSyncState()
	if err != nil {
		glog.Errorf("gRPC Sync failed to get sync state: %v", err)
	}
	return &paxosv1.SyncResponse{
		AgentId: s.agentID,
		Keys:    keys,
	}, nil
}

func (s *PaxosServer) GetKVEntry(ctx context.Context, req *paxosv1.GetKVEntryRequest) (*paxosv1.GetKVEntryResponse, error) {
	val, valType, propNum, _, err := s.store.GetKVEntry(req.Key)
	if err != nil {
		return nil, err
	}
	return &paxosv1.GetKVEntryResponse{
		Key:     req.Key,
		Value:   val,
		Type:    valType,
		Version: propNum,
	}, nil
}

func RunGRPCServer(lis net.Listener, srv *PaxosServer) error {
	s := grpc.NewServer()
	paxosv1.RegisterPaxosServiceServer(s, srv)
	return s.Serve(lis)
}
