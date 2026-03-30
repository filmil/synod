package paxos

import (
	"context"
	"os"
	"sync"
	"testing"

	"github.com/filmil/synod/internal/state"
	paxosv1 "github.com/filmil/synod/proto/paxos/v1"
)

type mockPeer struct {
	agentID     string
	prepareResp *paxosv1.PromiseResponse
	acceptResp  *paxosv1.AcceptedResponse
	err         error
	mu          sync.Mutex
	calls       []string
}

func (m *mockPeer) Prepare(ctx context.Context, req *paxosv1.PrepareRequest) (*paxosv1.PromiseResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, "Prepare")
	if m.err != nil {
		return nil, m.err
	}
	if m.prepareResp != nil {
		return m.prepareResp, nil
	}
	return &paxosv1.PromiseResponse{Promised: true, AgentId: m.agentID}, nil
}

func (m *mockPeer) Accept(ctx context.Context, req *paxosv1.AcceptRequest) (*paxosv1.AcceptedResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, "Accept")
	if m.err != nil {
		return nil, m.err
	}
	if m.acceptResp != nil {
		return m.acceptResp, nil
	}
	return &paxosv1.AcceptedResponse{Accepted: true, AgentId: m.agentID}, nil
}

func (m *mockPeer) JoinCluster(ctx context.Context, req *paxosv1.JoinClusterRequest) (*paxosv1.JoinClusterResponse, error) {
	return nil, nil
}

func (m *mockPeer) Sync(ctx context.Context, req *paxosv1.SyncRequest) (*paxosv1.SyncResponse, error) {
	return nil, nil
}

func (m *mockPeer) GetKVEntry(ctx context.Context, req *paxosv1.GetKVEntryRequest) (*paxosv1.GetKVEntryResponse, error) {
	return nil, nil
}

func (m *mockPeer) Ping(ctx context.Context, req *paxosv1.PingRequest) (*paxosv1.PingResponse, error) {
	if m.err != nil {
		return nil, m.err
	}
	return &paxosv1.PingResponse{
		AgentId:  m.agentID,
		Nonce:    req.Nonce,
		HostPort: "localhost:1234",
		HttpUrl:  "http://localhost:1234",
	}, nil
}

func (m *mockPeer) GetPeerEndpoints(ctx context.Context, req *paxosv1.GetPeerEndpointsRequest) (*paxosv1.GetPeerEndpointsResponse, error) {
	return &paxosv1.GetPeerEndpointsResponse{
		Endpoints: make(map[string]*paxosv1.EndpointInfo),
	}, nil
}
func (m *mockPeer) AgentID() string {
	return m.agentID
}

func (m *mockPeer) Close() error {
	return nil
}

func TestProposer_Success(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "proposer-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := state.NewStore(tmpDir)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	agentID := "proposer-agent"
	acceptor := NewAcceptor(agentID, nil, store)

	// Setup 2 mock peers to make a total of 3 agents (quorum = 2)
	peers := []PeerClient{
		&mockPeer{
			agentID:     "peer-1",
			prepareResp: &paxosv1.PromiseResponse{Promised: true, AgentId: "peer-1"},
			acceptResp:  &paxosv1.AcceptedResponse{Accepted: true, AgentId: "peer-1"},
		},
		&mockPeer{
			agentID:     "peer-2",
			prepareResp: &paxosv1.PromiseResponse{Promised: true, AgentId: "peer-2"},
			acceptResp:  &paxosv1.AcceptedResponse{Accepted: true, AgentId: "peer-2"},
		},
	}

	p := NewProposer(agentID, nil, peers, acceptor)
	ctx := context.Background()

	_, err = p.Propose(ctx, "/test/key", []byte("consensus-value"), QuorumMajority)
	if err != nil {
		t.Fatalf("Propose failed: %v", err)
	}
}

func TestProposer_NoQuorum(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "proposer-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)
	store, err := state.NewStore(tmpDir)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	agentID := "proposer-agent"
	acceptor := NewAcceptor(agentID, nil, store)

	// Peers return failure or no promise
	peers := []PeerClient{
		&mockPeer{
			agentID:     "peer-1",
			prepareResp: &paxosv1.PromiseResponse{Promised: false, AgentId: "peer-1"},
		},
		&mockPeer{
			agentID:     "peer-2",
			prepareResp: &paxosv1.PromiseResponse{Promised: false, AgentId: "peer-2"},
		},
	}

	p := NewProposer(agentID, nil, peers, acceptor)
	ctx := context.Background()

	_, err = p.Propose(ctx, "/test/key", []byte("val"), QuorumMajority)
	if err == nil {
		t.Fatalf("Expected Propose to fail due to lack of quorum")
	}
}

func TestProposer_FastForwardNextNum(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "proposer-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)
	store, err := state.NewStore(tmpDir)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	agentID := "proposer-agent"
	acceptor := NewAcceptor(agentID, nil, store)

	// Peers reject with a higher Promised ID
	peers := []PeerClient{
		&mockPeer{
			agentID: "peer-1",
			prepareResp: &paxosv1.PromiseResponse{
				Promised: false,
				AgentId:  "peer-1",
				HighestPromisedId: &paxosv1.ProposalID{
					Number:  42,
					AgentId: "other-agent",
				},
			},
		},
		&mockPeer{
			agentID: "peer-2",
			prepareResp: &paxosv1.PromiseResponse{
				Promised: false,
				AgentId:  "peer-2",
				HighestPromisedId: &paxosv1.ProposalID{
					Number:  15,
					AgentId: "another-agent",
				},
			},
		},
	}

	p := NewProposer(agentID, nil, peers, acceptor)
	ctx := context.Background()

	// Initial nextNum is 1
	if p.nextNum != 1 {
		t.Fatalf("Expected initial nextNum to be 1, got %d", p.nextNum)
	}

	_, err = p.Propose(ctx, "/test/key", []byte("val"), QuorumMajority)
	
	// Phase 1 should fail because peer-1 rejected and quorum is 2
	if err == nil {
		t.Fatalf("Expected Propose to fail due to lack of quorum")
	}

	// But nextNum should be fast-forwarded to 43 (highest Promised ID seen + 1)
	if p.nextNum != 43 {
		t.Fatalf("Expected nextNum to fast-forward to 43, got %d", p.nextNum)
	}
}
