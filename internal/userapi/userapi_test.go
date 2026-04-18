// SPDX-License-Identifier: Apache-2.0

package userapi

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/filmil/synod/internal/paxos"
	"github.com/filmil/synod/internal/state"
	paxosv1 "github.com/filmil/synod/proto/paxos/v1"
)

type mockPeer struct {
	agentID     string
	kvEntries   map[string]*paxosv1.GetKVEntryResponse
	prepareResp *paxosv1.PromiseResponse
	acceptResp  *paxosv1.AcceptedResponse
	err         error
	mu          sync.Mutex
}

func (m *mockPeer) Prepare(ctx context.Context, req *paxosv1.PrepareRequest) (*paxosv1.PromiseResponse, error) {
	if m.err != nil {
		return nil, m.err
	}
	if m.prepareResp != nil {
		return m.prepareResp, nil
	}
	return &paxosv1.PromiseResponse{Promised: true, AgentId: m.agentID}, nil
}

func (m *mockPeer) Accept(ctx context.Context, req *paxosv1.AcceptRequest) (*paxosv1.AcceptedResponse, error) {
	if m.err != nil {
		return nil, m.err
	}
	if m.acceptResp != nil {
		return m.acceptResp, nil
	}
	return &paxosv1.AcceptedResponse{Accepted: true, AgentId: m.agentID}, nil
}

func (m *mockPeer) JoinCluster(ctx context.Context, req *paxosv1.JoinClusterRequest) (*paxosv1.JoinClusterResponse, error) {
	return &paxosv1.JoinClusterResponse{Success: true}, nil
}

func (m *mockPeer) Sync(ctx context.Context, req *paxosv1.SyncRequest) (*paxosv1.SyncResponse, error) {
	return &paxosv1.SyncResponse{AgentId: m.agentID}, nil
}

func (m *mockPeer) GetKVEntry(ctx context.Context, req *paxosv1.GetKVEntryRequest) (*paxosv1.GetKVEntryResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return nil, m.err
	}
	if resp, ok := m.kvEntries[req.Key]; ok {
		return resp, nil
	}
	return &paxosv1.GetKVEntryResponse{}, nil
}

func (m *mockPeer) Ping(ctx context.Context, req *paxosv1.PingRequest) (*paxosv1.PingResponse, error) {
	if m.err != nil {
		return nil, m.err
	}
	return &paxosv1.PingResponse{AgentId: m.agentID}, nil
}

func (m *mockPeer) GetPeerEndpoints(ctx context.Context, req *paxosv1.GetPeerEndpointsRequest) (*paxosv1.GetPeerEndpointsResponse, error) {
	return &paxosv1.GetPeerEndpointsResponse{}, nil
}

func (m *mockPeer) AgentID() string {
	return m.agentID
}

func (m *mockPeer) Close() error {
	return nil
}

func setupTest(t *testing.T) (*UserAPI, *state.Store, func()) {
	tmpDir, err := os.MkdirTemp("", "userapi-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	store, err := state.NewStore(tmpDir)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to create store: %v", err)
	}

	agentID, _, err := store.InitializeAgentID("")
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to initialize agent ID: %v", err)
	}
	acceptor := paxos.NewAcceptor(agentID, nil, store)
	cell := paxos.NewCell(agentID, store, nil, acceptor, nil, "localhost:50051", "http://localhost:8080")
	api := New(cell)

	cleanup := func() {
		store.Close()
		os.RemoveAll(tmpDir)
	}

	return api, store, cleanup
}

func TestUserAPI_Read(t *testing.T) {
	api, store, cleanup := setupTest(t)
	defer cleanup()

	key := "/test/key"
	value := []byte("test-value")
	version := uint64(1)

	if err := store.CommitKV(key, value, "data", version); err != nil {
		t.Fatalf("failed to commit kv: %v", err)
	}

	ctx := context.Background()

	// 1. Local Read
	resp, err := api.Read(ctx, key, paxosv1.ReadQuorum_READ_QUORUM_LOCAL)
	if err != nil {
		t.Errorf("Local Read failed: %v", err)
	}
	if string(resp.Value) != string(value) {
		t.Errorf("Expected value %s, got %s", value, resp.Value)
	}

	// 2. Majority Read
	peer1 := &mockPeer{
		agentID: "peer-1",
		kvEntries: map[string]*paxosv1.GetKVEntryResponse{
			key: {Value: value, Version: version},
		},
	}
	peer2 := &mockPeer{
		agentID: "peer-2",
		kvEntries: map[string]*paxosv1.GetKVEntryResponse{
			key: {Value: []byte("different"), Version: version},
		},
	}

	// Add peers to store so they are not wiped by refreshPeers
	store.AddMember(peer1.agentID, state.PeerInfo{ShortName: "Peer1", GRPCAddr: "localhost:1111"})
	store.AddMember(peer2.agentID, state.PeerInfo{ShortName: "Peer2", GRPCAddr: "localhost:2222"})

	api.cell.SetPeers([]paxos.PeerClient{peer1, peer2})

	// Total nodes: 3 (self + 2 peers). Majority is 2.
	// Self (matches) + Peer 1 (matches) = 2 matches. Should succeed.
	resp, err = api.Read(ctx, key, paxosv1.ReadQuorum_READ_QUORUM_MAJORITY)
	if err != nil {
		t.Errorf("Majority Read failed: %v", err)
	}
	if string(resp.Value) != string(value) {
		t.Errorf("Expected value %s, got %s", value, resp.Value)
	}

	// 3. All Read
	// Peer 2 does not match, so "All" should fail.
	_, err = api.Read(ctx, key, paxosv1.ReadQuorum_READ_QUORUM_ALL)
	if err == nil {
		t.Error("Expected All Read to fail, but it succeeded")
	}

	// Make Peer 2 match
	peer2.kvEntries[key] = &paxosv1.GetKVEntryResponse{Value: value, Version: version}
	resp, err = api.Read(ctx, key, paxosv1.ReadQuorum_READ_QUORUM_ALL)
	if err != nil {
		t.Errorf("All Read failed: %v", err)
	}
	if string(resp.Value) != string(value) {
		t.Errorf("Expected value %s, got %s", value, resp.Value)
	}
}

func TestUserAPI_Read_NoMatch(t *testing.T) {
	api, store, cleanup := setupTest(t)
	defer cleanup()

	key := "/test/key"
	value := []byte("test-value")
	version := uint64(1)

	if err := store.CommitKV(key, value, "data", version); err != nil {
		t.Fatalf("failed to commit kv: %v", err)
	}

	ctx := context.Background()

	peer1 := &mockPeer{
		agentID: "peer-1",
		kvEntries: map[string]*paxosv1.GetKVEntryResponse{
			key: {Value: []byte("different"), Version: version},
		},
	}
	peer2 := &mockPeer{
		agentID: "peer-2",
		kvEntries: map[string]*paxosv1.GetKVEntryResponse{
			key: {Value: []byte("different-again"), Version: version},
		},
	}
	store.AddMember(peer1.agentID, state.PeerInfo{ShortName: "Peer1", GRPCAddr: "localhost:1111"})
	store.AddMember(peer2.agentID, state.PeerInfo{ShortName: "Peer2", GRPCAddr: "localhost:2222"})

	api.cell.SetPeers([]paxos.PeerClient{peer1, peer2})

	// Total nodes: 3. Majority: 2. Matches: 1 (self). Should fail.
	_, err := api.Read(ctx, key, paxosv1.ReadQuorum_READ_QUORUM_MAJORITY)
	if err == nil {
		t.Error("Expected Majority Read to fail when no majority matches")
	}
}

func TestUserAPI_CompareAndWrite(t *testing.T) {
	api, store, cleanup := setupTest(t)
	defer cleanup()

	key := "/test/key"
	oldValue := []byte("old-value")
	newValue := []byte("new-value")
	version := uint64(1)

	if err := store.CommitKV(key, oldValue, "data", version); err != nil {
		t.Fatalf("failed to commit kv: %v", err)
	}

	ctx := context.Background()

	// CAS success
	resp, err := api.CompareAndWrite(ctx, key, oldValue, newValue, paxos.QuorumMajority)
	if err != nil {
		t.Fatalf("CompareAndWrite failed: %v", err)
	}
	if !resp.Success {
		t.Errorf("CompareAndWrite returned success=false: %s", resp.Message)
	}

	// CAS failure (old value mismatch)
	resp, err = api.CompareAndWrite(ctx, key, []byte("wrong-old"), []byte("even-newer"), paxos.QuorumMajority)
	if err != nil {
		t.Fatalf("CompareAndWrite failed with error: %v", err)
	}
	if resp.Success {
		t.Error("Expected CompareAndWrite to fail due to old value mismatch")
	}
}

func TestUserAPI_ReadPrefix(t *testing.T) {
	api, store, cleanup := setupTest(t)
	defer cleanup()

	keys := []string{"/prefix/a", "/prefix/b", "/other/c"}
	for _, k := range keys {
		store.CommitKV(k, []byte("val"), "data", 1)
	}

	ctx := context.Background()
	resp, err := api.ReadPrefix(ctx, "/prefix")
	if err != nil {
		t.Fatalf("ReadPrefix failed: %v", err)
	}

	if len(resp.Entries) != 2 {
		t.Errorf("Expected 2 entries, got %d", len(resp.Entries))
	}
}

func TestUserAPI_Shutdown(t *testing.T) {
	api, _, cleanup := setupTest(t)
	defer cleanup()

	ctx := context.Background()
	resp, err := api.Shutdown(ctx, &paxosv1.ShutdownRequest{})
	if err != nil {
		t.Fatalf("Shutdown failed: %v", err)
	}

	if !resp.Success {
		t.Errorf("Shutdown returned success=false: %s", resp.Message)
	}

	// Wait for the async shutdown trigger
	select {
	case <-api.cell.ShutdownChan:
		// Success
	case <-time.After(2 * time.Second):
		t.Error("Timed out waiting for shutdown signal")
	}
}

func TestUserAPI_Locking(t *testing.T) {
	api, _, cleanup := setupTest(t)
	defer cleanup()

	ctx := context.Background()
	lockablePath := "/a/_lockable/b"

	// 1. Acquire Lock
	resp, err := api.AcquireLock(ctx, &paxosv1.AcquireLockRequest{
		KeyPath:    lockablePath,
		DurationMs: 10000,
	})
	if err != nil {
		t.Fatalf("AcquireLock failed: %v", err)
	}
	if !resp.Success {
		t.Fatalf("AcquireLock returned success=false: %s", resp.Message)
	}

	// 2. Check Lock
	if err := api.CheckLocks(ctx, lockablePath); err != nil {
		t.Errorf("CheckLocks failed for owner: %v", err)
	}

	// 3. Renew Lock
	renewResp, err := api.RenewLock(ctx, &paxosv1.RenewLockRequest{
		KeyPath:    lockablePath,
		DurationMs: 20000,
	})
	if err != nil {
		t.Fatalf("RenewLock failed: %v", err)
	}
	if !renewResp.Success {
		t.Errorf("RenewLock returned success=false: %s", renewResp.Message)
	}

	// 4. Release Lock
	releaseResp, err := api.ReleaseLock(ctx, &paxosv1.ReleaseLockRequest{
		KeyPath: lockablePath,
	})
	if err != nil {
		t.Fatalf("ReleaseLock failed: %v", err)
	}
	if !releaseResp.Success {
		t.Errorf("ReleaseLock returned success=false: %s", releaseResp.Message)
	}

	// 5. Check Lock after release (should fail)
	if err := api.CheckLocks(ctx, lockablePath); err == nil {
		t.Error("CheckLocks should have failed after release, but it succeeded")
	}
}
