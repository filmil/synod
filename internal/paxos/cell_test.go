package paxos

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/filmil/synod/internal/state"
)

func TestCell_ProposeRemoval(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "cell-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := state.NewStore(tmpDir)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	agentID := "agent-1"
	acceptor := NewAcceptor(agentID, store)
	
	// Pre-populate membership
	peerID := "peer-to-remove"
	peerInfo := state.PeerInfo{GRPCAddr: "localhost:1234", ShortName: "Peer"}
	peersMap := map[string]state.PeerInfo{
		agentID: {GRPCAddr: "localhost:50051", ShortName: "Self"},
		peerID:  peerInfo,
	}
	peersData, _ := json.Marshal(peersMap)
	if err := store.CommitKV("/_internal/peers", peersData, "membership", 1); err != nil {
		t.Fatalf("failed to set initial peers: %v", err)
	}
	if err := store.AddMember(agentID, peersMap[agentID]); err != nil {
		t.Fatalf("failed to add self to membership: %v", err)
	}
	if err := store.AddMember(peerID, peerInfo); err != nil {
		t.Fatalf("failed to add peer to membership: %v", err)
	}

	cell := NewCell(agentID, store, acceptor, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := cell.ProposeRemoval(ctx, peerID); err != nil {
		t.Fatalf("ProposeRemoval failed: %v", err)
	}

	// Verify peer is removed from DB
	members, err := store.GetMembers()
	if err != nil {
		t.Fatalf("Failed to get members: %v", err)
	}
	if _, ok := members[peerID]; ok {
		t.Errorf("Peer %s was not removed from DB", peerID)
	}

	// Verify /_internal/peers KV is updated
	val, _, _, _, err := store.GetKVEntry("/_internal/peers")
	if err != nil {
		t.Fatalf("Failed to get KV entry: %v", err)
	}
	var updatedPeers map[string]state.PeerInfo
	json.Unmarshal(val, &updatedPeers)
	if _, ok := updatedPeers[peerID]; ok {
		t.Errorf("Peer %s was not removed from KV store", peerID)
	}
}

func TestCell_PingPeers(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "cell-ping-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := state.NewStore(tmpDir)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	agentID := "agent-1"
	acceptor := NewAcceptor(agentID, store)

	// Pre-populate membership with a responsive and an unresponsive peer
	peer1ID := "responsive-peer"
	peer2ID := "unresponsive-peer"
	
	peersMap := map[string]state.PeerInfo{
		agentID: {GRPCAddr: "localhost:50051", ShortName: "Self"},
		peer1ID: {GRPCAddr: "localhost:1111", ShortName: "Peer1"},
		peer2ID: {GRPCAddr: "localhost:2222", ShortName: "Peer2"},
	}
	peersData, _ := json.Marshal(peersMap)
	store.CommitKV("/_internal/peers", peersData, "membership", 1)
	store.AddMember(agentID, peersMap[agentID])
	store.AddMember(peer1ID, peersMap[peer1ID])
	store.AddMember(peer2ID, peersMap[peer2ID])

	peer1 := &mockPeer{
		agentID: peer1ID,
		// Ping success by default (err is nil)
	}
	peer2 := &mockPeer{
		agentID: peer2ID,
		err:     context.DeadlineExceeded, // Ping failure
	}

	factory := func(id, addr string) (PeerClient, error) {
		if id == peer1ID {
			return peer1, nil
		}
		if id == peer2ID {
			return peer2, nil
		}
		return nil, nil
	}

	cell := NewCell(agentID, store, acceptor, factory)
	
	// Mock peers for proposer quorum (agent-1 and responsive-peer should make quorum of 2 out of 3)
	// Actually we need to set the peers in the cell so the proposer knows about them.
	// But refreshPeers will do that.
	
	// We need to mock the proposer's behavior because ProposeRemoval uses p.proposer.Propose
	// ProposeRemoval uses p.proposer, which is set by refreshPeers.
	
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cell.PingPeers(ctx)

	// After PingPeers, peer2ID should be proposed for removal.
	// We check if peer2ID is gone from the database.
	members, _ := store.GetMembers()
	if _, ok := members[peer2ID]; ok {
		t.Errorf("Unresponsive peer %s was not removed from DB", peer2ID)
	}
	if _, ok := members[peer1ID]; !ok {
		t.Errorf("Responsive peer %s was incorrectly removed from DB", peer1ID)
	}
}
