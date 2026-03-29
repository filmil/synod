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
	"google.golang.org/grpc/channelz/service"
)

// PaxosServer implements both the internal PaxosService and the client-facing UserService over gRPC.
type PaxosServer struct {
	paxosv1.UnimplementedPaxosServiceServer
	paxosv1.UnimplementedUserServiceServer
	agentID  string
	acceptor *paxos.Acceptor
	store    *state.Store
	cell     *paxos.Cell
	userAPI  *userapi.UserAPI
}

// NewPaxosServer initializes a new PaxosServer.
func NewPaxosServer(agentID string, store *state.Store, acceptor *paxos.Acceptor, cell *paxos.Cell) *PaxosServer {
	return &PaxosServer{
		agentID:  agentID,
		acceptor: acceptor,
		store:    store,
		cell:     cell,
		userAPI:  userapi.New(cell),
	}
}

// Prepare handles the Phase 1a of the Paxos protocol.
func (s *PaxosServer) Prepare(ctx context.Context, req *paxosv1.PrepareRequest) (*paxosv1.PromiseResponse, error) {
	return s.acceptor.Prepare(ctx, req)
}

// Accept handles the Phase 2a of the Paxos protocol.
func (s *PaxosServer) Accept(ctx context.Context, req *paxosv1.AcceptRequest) (*paxosv1.AcceptedResponse, error) {
	return s.acceptor.Accept(ctx, req)
}

// JoinCluster processes a request from a new node attempting to join the cluster.
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

// Sync returns the highest known version numbers for all keys in the local store.
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

// GetKVEntry returns the current state of a specific key from the local store.
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

// Ping responds to a liveness check and exchanges endpoint information.
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

// GetPeerEndpoints returns a snapshot of all known peers and their endpoints.
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

// Read handles a client request to read a key with a specified consistency quorum.
func (s *PaxosServer) Read(ctx context.Context, req *paxosv1.ReadRequest) (*paxosv1.ReadResponse, error) {
	return s.userAPI.Read(ctx, req.Key, req.Quorum)
}

// CompareAndWrite handles a client request to atomically update a key's value.
func (s *PaxosServer) CompareAndWrite(ctx context.Context, req *paxosv1.CompareAndWriteRequest) (*paxosv1.CompareAndWriteResponse, error) {
	qt := paxos.QuorumMajority
	if req.Quorum == paxosv1.WriteQuorum_WRITE_QUORUM_ALL {
		qt = paxos.QuorumAll
	}
	return s.userAPI.CompareAndWrite(ctx, req.Key, req.OldValue, req.NewValue, qt)
}


// AcquireLock handles a client request to acquire a distributed lock.
func (s *PaxosServer) AcquireLock(ctx context.Context, req *paxosv1.AcquireLockRequest) (*paxosv1.AcquireLockResponse, error) {
	return s.userAPI.AcquireLock(ctx, req)
}

// ReleaseLock handles a client request to release a distributed lock.
func (s *PaxosServer) ReleaseLock(ctx context.Context, req *paxosv1.ReleaseLockRequest) (*paxosv1.ReleaseLockResponse, error) {
	return s.userAPI.ReleaseLock(ctx, req)
}

// RenewLock handles a client request to extend a distributed lock's duration.
func (s *PaxosServer) RenewLock(ctx context.Context, req *paxosv1.RenewLockRequest) (*paxosv1.RenewLockResponse, error) {
	return s.userAPI.RenewLock(ctx, req)
}

func (s *PaxosServer) Shutdown(ctx context.Context, req *paxosv1.ShutdownRequest) (*paxosv1.ShutdownResponse, error) {
	return s.userAPI.Shutdown(ctx, req)
}


func timeoutInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	ctx, cancel := context.WithTimeout(ctx, 1*time.Minute)
	defer cancel()
	return handler(ctx, req)
}

// RunGRPCServer starts the gRPC server and registers the Paxos and User APIs.
func RunGRPCServer(ctx context.Context, lis net.Listener, srv *PaxosServer) error {
	s := grpc.NewServer(grpc.UnaryInterceptor(timeoutInterceptor))

	paxosv1.RegisterPaxosServiceServer(s, srv)
	paxosv1.RegisterUserServiceServer(s, srv)
	service.RegisterChannelzServiceToServer(s)

	go func() {
		<-ctx.Done()
		glog.Infof("gRPC server: shutting down")
		s.Stop()
	}()

	return s.Serve(lis)
}
