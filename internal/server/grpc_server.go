// SPDX-License-Identifier: Apache-2.0

package server

import (
	"time"
	"context"
	"net"

	"github.com/filmil/synod/internal/identity"
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
	ident    *identity.Identity
	acceptor *paxos.Acceptor
	store    *state.Store
	cell     *paxos.Cell
	userAPI  *userapi.UserAPI
}

// NewPaxosServer initializes a new PaxosServer.
func NewPaxosServer(agentID string, ident *identity.Identity, store *state.Store, acceptor *paxos.Acceptor, cell *paxos.Cell) *PaxosServer {
	return &PaxosServer{
		agentID:  agentID,
		ident:    ident,
		acceptor: acceptor,
		store:    store,
		cell:     cell,
		userAPI:  userapi.New(cell),
	}
}

// Prepare handles the Phase 1a of the Paxos protocol.
func (s *PaxosServer) Prepare(ctx context.Context, req *paxosv1.PrepareRequest) (*paxosv1.PromiseResponse, error) {
	if req.Auth != nil {
		if cert, err := identity.UnmarshalCertificate(req.Auth.Certificate); err == nil {
			if err := identity.VerifyMessage(cert, req, req.Auth.Signature); err != nil {
				glog.Warningf("gRPC: Prepare from %s: signature verification failed: %v", req.AgentId, err)
			} else {
				glog.V(2).Infof("gRPC: Prepare from %s: signature verified", req.AgentId)
			}
		}
	}
	return s.acceptor.Prepare(ctx, req)
}

// Accept handles the Phase 2a of the Paxos protocol.
func (s *PaxosServer) Accept(ctx context.Context, req *paxosv1.AcceptRequest) (*paxosv1.AcceptedResponse, error) {
	if req.Auth != nil {
		if cert, err := identity.UnmarshalCertificate(req.Auth.Certificate); err == nil {
			if err := identity.VerifyMessage(cert, req, req.Auth.Signature); err != nil {
				glog.Warningf("gRPC: Accept from %s: signature verification failed: %v", req.AgentId, err)
			} else {
				glog.V(2).Infof("gRPC: Accept from %s: signature verified", req.AgentId)
			}
		}
	}
	return s.acceptor.Accept(ctx, req)
}

// JoinCluster processes a request from a new node attempting to join the cluster.
func (s *PaxosServer) JoinCluster(ctx context.Context, req *paxosv1.JoinClusterRequest) (*paxosv1.JoinClusterResponse, error) {
	if req.Auth != nil {
		if cert, err := identity.UnmarshalCertificate(req.Auth.Certificate); err == nil {
			if err := identity.VerifyMessage(cert, req, req.Auth.Signature); err != nil {
				glog.Warningf("gRPC: JoinCluster from %s: signature verification failed: %v", req.AgentId, err)
			} else {
				glog.V(2).Infof("gRPC: JoinCluster from %s: signature verified", req.AgentId)
			}
		}
	}
	glog.Infof("gRPC: Received JoinCluster from %s at %s", req.AgentId, req.HostPort)

	var cert []byte
	if req.Auth != nil {
		cert = req.Auth.Certificate
	}

	info := state.PeerInfo{
		ShortName:   req.ShortName,
		GRPCAddr:    req.HostPort,
		HTTPURL:     req.HttpUrl,
		Certificate: cert,
	}
	err := s.cell.ProposeMembership(ctx, req.AgentId, info)

	resp := &paxosv1.JoinClusterResponse{
		AgentId: s.agentID,
		Success: err == nil,
	}
	if err != nil {
		glog.Errorf("gRPC: JoinCluster failed: %v", err)
		resp.Message = err.Error()
	} else {
		resp.Message = "Join accepted"
	}

	if s.ident != nil {
		sig, cert, err := s.ident.SignMessage(resp)
		if err == nil {
			resp.Auth = &paxosv1.Authentication{
				Signature:   sig,
				Certificate: cert,
			}
		}
	}

	return resp, nil
}

// Sync returns the highest known version numbers for all keys in the local store.
func (s *PaxosServer) Sync(ctx context.Context, req *paxosv1.SyncRequest) (*paxosv1.SyncResponse, error) {
	if req.Auth != nil {
		if cert, err := identity.UnmarshalCertificate(req.Auth.Certificate); err == nil {
			if err := identity.VerifyMessage(cert, req, req.Auth.Signature); err != nil {
				glog.Warningf("gRPC: Sync from %s: signature verification failed: %v", req.AgentId, err)
			} else {
				glog.V(2).Infof("gRPC: Sync from %s: signature verified", req.AgentId)
			}
		}
	}
	members, err := s.store.GetMembers()
	if err == nil {
		if info, ok := members[req.AgentId]; ok {
			if info.GRPCAddr != req.HostPort || info.HTTPURL != req.HttpUrl {
				info.GRPCAddr = req.HostPort
				info.HTTPURL = req.HttpUrl
				s.cell.ProposeMembership(context.Background(), req.AgentId, info)
			}
		}
	}

	keys, err := s.cell.GetSyncState()
	if err != nil {
		glog.Errorf("gRPC Sync failed to get sync state: %v", err)
	}
	resp := &paxosv1.SyncResponse{
		AgentId: s.agentID,
		Keys:    keys,
	}

	if s.ident != nil {
		sig, cert, err := s.ident.SignMessage(resp)
		if err == nil {
			resp.Auth = &paxosv1.Authentication{
				Signature:   sig,
				Certificate: cert,
			}
		}
	}

	return resp, nil
}

// GetKVEntry returns the current state of a specific key from the local store.
func (s *PaxosServer) GetKVEntry(ctx context.Context, req *paxosv1.GetKVEntryRequest) (*paxosv1.GetKVEntryResponse, error) {
	if req.Auth != nil {
		if cert, err := identity.UnmarshalCertificate(req.Auth.Certificate); err == nil {
			if err := identity.VerifyMessage(cert, req, req.Auth.Signature); err != nil {
				glog.Warningf("gRPC: GetKVEntry for %s: signature verification failed: %v", req.Key, err)
			} else {
				glog.V(2).Infof("gRPC: GetKVEntry for %s: signature verified", req.Key)
			}
		}
	}
	val, valType, propNum, _, err := s.store.GetKVEntry(req.Key)
	if err != nil {
		return nil, err
	}
	resp := &paxosv1.GetKVEntryResponse{
		Key:     req.Key,
		Value:   val,
		Type:    valType,
		Version: propNum,
	}

	if s.ident != nil {
		sig, cert, err := s.ident.SignMessage(resp)
		if err == nil {
			resp.Auth = &paxosv1.Authentication{
				Signature:   sig,
				Certificate: cert,
			}
		}
	}

	return resp, nil
}

// Ping responds to a liveness check and exchanges endpoint information.
func (s *PaxosServer) Ping(ctx context.Context, req *paxosv1.PingRequest) (*paxosv1.PingResponse, error) {
	if req.Auth != nil {
		if cert, err := identity.UnmarshalCertificate(req.Auth.Certificate); err == nil {
			if err := identity.VerifyMessage(cert, req, req.Auth.Signature); err != nil {
				glog.Warningf("gRPC: Ping from %s: signature verification failed: %v", req.AgentId, err)
			} else {
				glog.V(2).Infof("gRPC: Ping from %s: signature verified", req.AgentId)
			}
		}
	}
	members, err := s.store.GetMembers()
	if err == nil {
		if info, ok := members[req.AgentId]; ok {
			if info.GRPCAddr != req.HostPort || info.HTTPURL != req.HttpUrl {
				info.GRPCAddr = req.HostPort
				info.HTTPURL = req.HttpUrl
				s.cell.ProposeMembership(context.Background(), req.AgentId, info)
			}
		}
	}

	var grpcAddr, httpUrl string
	if selfInfo, ok := members[s.agentID]; ok {
		grpcAddr = selfInfo.GRPCAddr
		httpUrl = selfInfo.HTTPURL
	}

	resp := &paxosv1.PingResponse{
		AgentId:  s.agentID,
		Nonce:    req.Nonce,
		HostPort: grpcAddr,
		HttpUrl:  httpUrl,
	}

	if s.ident != nil {
		sig, cert, err := s.ident.SignMessage(resp)
		if err == nil {
			resp.Auth = &paxosv1.Authentication{
				Signature:   sig,
				Certificate: cert,
			}
		}
	}

	return resp, nil
}

// GetPeerEndpoints returns a snapshot of all known peers and their endpoints.
func (s *PaxosServer) GetPeerEndpoints(ctx context.Context, req *paxosv1.GetPeerEndpointsRequest) (*paxosv1.GetPeerEndpointsResponse, error) {
	if req.Auth != nil {
		if cert, err := identity.UnmarshalCertificate(req.Auth.Certificate); err == nil {
			if err := identity.VerifyMessage(cert, req, req.Auth.Signature); err != nil {
				glog.Warningf("gRPC: GetPeerEndpoints from %s: signature verification failed: %v", req.AgentId, err)
			} else {
				glog.V(2).Infof("gRPC: GetPeerEndpoints from %s: signature verified", req.AgentId)
			}
		}
	}
	resp := &paxosv1.GetPeerEndpointsResponse{
		Endpoints: make(map[string]*paxosv1.EndpointInfo),
	}

	members, err := s.store.GetMembers()
	if err == nil {
		for id, info := range members {
			resp.Endpoints[id] = &paxosv1.EndpointInfo{
				HostPort: info.GRPCAddr,
				HttpUrl:  info.HTTPURL,
			}
		}
	}

	if s.ident != nil {
		sig, cert, err := s.ident.SignMessage(resp)
		if err == nil {
			resp.Auth = &paxosv1.Authentication{
				Signature:   sig,
				Certificate: cert,
			}
		}
	}

	return resp, nil
}

// Read handles a client request to read a key with a specified consistency quorum.
func (s *PaxosServer) Read(ctx context.Context, req *paxosv1.ReadRequest) (*paxosv1.ReadResponse, error) {
	if req.Auth != nil {
		if cert, err := identity.UnmarshalCertificate(req.Auth.Certificate); err == nil {
			if err := identity.VerifyMessage(cert, req, req.Auth.Signature); err != nil {
				glog.Warningf("gRPC: Read for %s: signature verification failed: %v", req.Key, err)
			} else {
				glog.V(2).Infof("gRPC: Read for %s: signature verified", req.Key)
			}
		}
	}
	resp, err := s.userAPI.Read(ctx, req.Key, req.Quorum)
	if err != nil {
		return nil, err
	}

	if s.ident != nil {
		sig, cert, err := s.ident.SignMessage(resp)
		if err == nil {
			resp.Auth = &paxosv1.Authentication{
				Signature:   sig,
				Certificate: cert,
			}
		}
	}

	return resp, nil
}

// CompareAndWrite handles a client request to atomically update a key's value.
func (s *PaxosServer) CompareAndWrite(ctx context.Context, req *paxosv1.CompareAndWriteRequest) (*paxosv1.CompareAndWriteResponse, error) {
	if req.Auth != nil {
		if cert, err := identity.UnmarshalCertificate(req.Auth.Certificate); err == nil {
			if err := identity.VerifyMessage(cert, req, req.Auth.Signature); err != nil {
				glog.Warningf("gRPC: CompareAndWrite for %s: signature verification failed: %v", req.Key, err)
			} else {
				glog.V(2).Infof("gRPC: CompareAndWrite for %s: signature verified", req.Key)
			}
		}
	}
	qt := paxos.QuorumMajority
	if req.Quorum == paxosv1.WriteQuorum_WRITE_QUORUM_ALL {
		qt = paxos.QuorumAll
	}
	resp, err := s.userAPI.CompareAndWrite(ctx, req.Key, req.OldValue, req.NewValue, qt)
	if err != nil {
		return nil, err
	}

	if s.ident != nil {
		sig, cert, err := s.ident.SignMessage(resp)
		if err == nil {
			resp.Auth = &paxosv1.Authentication{
				Signature:   sig,
				Certificate: cert,
			}
		}
	}

	return resp, nil
}

// ReadPrefix handles a client request to read all entries with a specified prefix.
func (s *PaxosServer) ReadPrefix(ctx context.Context, req *paxosv1.ReadPrefixRequest) (*paxosv1.ReadPrefixResponse, error) {
	if req.Auth != nil {
		if cert, err := identity.UnmarshalCertificate(req.Auth.Certificate); err == nil {
			if err := identity.VerifyMessage(cert, req, req.Auth.Signature); err != nil {
				glog.Warningf("gRPC: ReadPrefix for %s: signature verification failed: %v", req.Prefix, err)
			} else {
				glog.V(2).Infof("gRPC: ReadPrefix for %s: signature verified", req.Prefix)
			}
		}
	}

	resp, err := s.userAPI.ReadPrefix(ctx, req.Prefix)
	if err != nil {
		return nil, err
	}

	if s.ident != nil {
		sig, cert, err := s.ident.SignMessage(resp)
		if err == nil {
			resp.Auth = &paxosv1.Authentication{
				Signature:   sig,
				Certificate: cert,
			}
		}
	}

	return resp, nil
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
