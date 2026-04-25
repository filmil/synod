// SPDX-License-Identifier: Apache-2.0

package server

import (
	"os"
	"testing"

	"github.com/filmil/synod/internal/paxos"
	"github.com/filmil/synod/internal/state"
)

func setupTestGRPCServer(t *testing.T) (string, *state.Store, *paxos.Acceptor, *paxos.Cell, func()) {
	stateDir, err := os.MkdirTemp("", "synod-grpc-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	store, err := state.NewStore(stateDir)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	agentID, _, err := store.InitializeAgentID("")
	if err != nil {
		t.Fatalf("failed to initialize agent ID: %v", err)
	}

	ident, _ := store.GetIdentity("")

	acceptor := paxos.NewAcceptor(agentID, ident, store)
	factory := func(agentID, addr string) (paxos.PeerClient, error) {
		return nil, nil
	}
	cell := paxos.NewCell(agentID, store, ident, acceptor, factory, "localhost:50051", "http://localhost:8081")

	cleanup := func() {
		store.Close()
		os.RemoveAll(stateDir)
	}

	return agentID, store, acceptor, cell, cleanup
}

func TestNewPaxosServer(t *testing.T) {
	agentID, store, acceptor, cell, cleanup := setupTestGRPCServer(t)
	defer cleanup()

	ident, _ := store.GetIdentity("")

	srv := NewPaxosServer(agentID, ident, store, acceptor, cell)

	if srv == nil {
		t.Fatal("NewPaxosServer returned nil")
	}

	if srv.agentID != agentID {
		t.Errorf("expected agentID %s, got %s", agentID, srv.agentID)
	}

	if srv.ident != ident {
		t.Errorf("expected ident %v, got %v", ident, srv.ident)
	}

	if srv.store != store {
		t.Errorf("expected store %v, got %v", store, srv.store)
	}

	if srv.acceptor != acceptor {
		t.Errorf("expected acceptor %v, got %v", acceptor, srv.acceptor)
	}

	if srv.cell != cell {
		t.Errorf("expected cell %v, got %v", cell, srv.cell)
	}

	if srv.userAPI == nil {
		t.Error("expected userAPI to be initialized, got nil")
	}
}
