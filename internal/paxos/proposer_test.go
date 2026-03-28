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
	return m.prepareResp, nil
}

func (m *mockPeer) Accept(ctx context.Context, req *paxosv1.AcceptRequest) (*paxosv1.AcceptedResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, "Accept")
	if m.err != nil {
		return nil, m.err
	}
	return m.acceptResp, nil
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

func (m *mockPeer) AgentID() string {
	return m.agentID
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
	acceptor := NewAcceptor(agentID, store)
	
	// Setup 2 mock peers to make a total of 3 agents (quorum = 2)
	peers := []PeerClient{
		&mockPeer{
			agentID: "peer-1",
			prepareResp: &paxosv1.PromiseResponse{Promised: true, AgentId: "peer-1"},
			acceptResp:  &paxosv1.AcceptedResponse{Accepted: true, AgentId: "peer-1"},
		},
		&mockPeer{
			agentID: "peer-2",
			prepareResp: &paxosv1.PromiseResponse{Promised: true, AgentId: "peer-2"},
			acceptResp:  &paxosv1.AcceptedResponse{Accepted: true, AgentId: "peer-2"},
		},
	}

	p := NewProposer(agentID, peers, acceptor)
	ctx := context.Background()

	_, err = p.Propose(ctx, "/test/key", []byte("consensus-value"))
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
	acceptor := NewAcceptor(agentID, store)
	
	// Peers return failure or no promise
	peers := []PeerClient{
		&mockPeer{
			agentID: "peer-1",
			prepareResp: &paxosv1.PromiseResponse{Promised: false, AgentId: "peer-1"},
		},
		&mockPeer{
			agentID: "peer-2",
			prepareResp: &paxosv1.PromiseResponse{Promised: false, AgentId: "peer-2"},
		},
	}

	p := NewProposer(agentID, peers, acceptor)
	ctx := context.Background()

	_, err = p.Propose(ctx, "/test/key", []byte("val"))
	if err == nil {
		t.Fatalf("Expected Propose to fail due to lack of quorum")
	}
}
