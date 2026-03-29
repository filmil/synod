package server

import (
	"time"
	"context"
	"net"

	"github.com/filmil/synod/internal/paxos"
	"github.com/filmil/synod/internal/state"
	"github.com/filmil/synod/internal/userapi"
	paxosv1 "github.com/filmil/synod/proto/paxos/v1"
	"github.com/golang/glog"
	"google.golang.org/grpc"
)

type PaxosServer struct {
	paxosv1.UnimplementedPaxosServiceServer
	paxosv1.UnimplementedUserServiceServer
	agentID  string
	acceptor *paxos.Acceptor
	store    *state.Store
	cell     *paxos.Cell
	userAPI  *userapi.UserAPI
}

func NewPaxosServer(agentID string, store *state.Store, acceptor *paxos.Acceptor, cell *paxos.Cell) *PaxosServer {
	return &PaxosServer{
		agentID:  agentID,
		acceptor: acceptor,
		store:    store,
		cell:     cell,
		userAPI:  userapi.New(cell),
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
	// Update ephemeral map
	s.cell.UpdateEphemeralPeer(req.AgentId, req.HostPort, req.HttpUrl)

	info := state.PeerInfo{
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
	// Update ephemeral map
	s.cell.UpdateEphemeralPeer(req.AgentId, req.HostPort, req.HttpUrl)

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

func (s *PaxosServer) Ping(ctx context.Context, req *paxosv1.PingRequest) (*paxosv1.PingResponse, error) {
	// Update ephemeral map
	s.cell.UpdateEphemeralPeer(req.AgentId, req.HostPort, req.HttpUrl)

	// Get our own ephemeral info (set in main.go)
	eph, _ := s.cell.GetEphemeralPeer(s.agentID)

	return &paxosv1.PingResponse{
		AgentId:  s.agentID,
		Nonce:    req.Nonce,
		HostPort: eph.GRPCAddr,
		HttpUrl:  eph.HTTPURL,
	}, nil
}

func (s *PaxosServer) GetPeerEndpoints(ctx context.Context, req *paxosv1.GetPeerEndpointsRequest) (*paxosv1.GetPeerEndpointsResponse, error) {
	resp := &paxosv1.GetPeerEndpointsResponse{
		Endpoints: make(map[string]*paxosv1.EndpointInfo),
	}

	ephs := s.cell.GetEphemeralPeers()
	for id, info := range ephs {
		resp.Endpoints[id] = &paxosv1.EndpointInfo{
			HostPort: info.GRPCAddr,
			HttpUrl:  info.HTTPURL,
		}
	}
	return resp, nil
}

func (s *PaxosServer) Read(ctx context.Context, req *paxosv1.ReadRequest) (*paxosv1.ReadResponse, error) {
	return s.userAPI.Read(ctx, req.Key, req.Quorum)
}

func (s *PaxosServer) CompareAndWrite(ctx context.Context, req *paxosv1.CompareAndWriteRequest) (*paxosv1.CompareAndWriteResponse, error) {
	return s.userAPI.CompareAndWrite(ctx, req.Key, req.OldValue, req.NewValue)
}


func (s *PaxosServer) AcquireLock(ctx context.Context, req *paxosv1.AcquireLockRequest) (*paxosv1.AcquireLockResponse, error) {
	return s.userAPI.AcquireLock(ctx, req)
}

func (s *PaxosServer) ReleaseLock(ctx context.Context, req *paxosv1.ReleaseLockRequest) (*paxosv1.ReleaseLockResponse, error) {
	return s.userAPI.ReleaseLock(ctx, req)
}

func (s *PaxosServer) RenewLock(ctx context.Context, req *paxosv1.RenewLockRequest) (*paxosv1.RenewLockResponse, error) {
	return s.userAPI.RenewLock(ctx, req)
}


func timeoutInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	ctx, cancel := context.WithTimeout(ctx, 1*time.Minute)
	defer cancel()
	return handler(ctx, req)
}

func RunGRPCServer(ctx context.Context, lis net.Listener, srv *PaxosServer) error {
	s := grpc.NewServer(grpc.UnaryInterceptor(timeoutInterceptor))

	paxosv1.RegisterPaxosServiceServer(s, srv)
	paxosv1.RegisterUserServiceServer(s, srv)

	go func() {
		<-ctx.Done()
		glog.Infof("gRPC server: shutting down")
		s.Stop()
	}()

	return s.Serve(lis)
}
