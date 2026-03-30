package paxos

import (
	"context"
	"os"
	"testing"

	"github.com/filmil/synod/internal/state"
	paxosv1 "github.com/filmil/synod/proto/paxos/v1"
)

func TestAcceptor(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "paxos-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := state.NewStore(tmpDir)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	agentID := "test-agent"
	a := NewAcceptor(agentID, nil, store)
	ctx := context.Background()

	// Initial prepare
	req1 := &paxosv1.PrepareRequest{
		AgentId:    "other-agent",
		ProposalId: &paxosv1.ProposalID{Number: 1, AgentId: "a"},
		Key:        "/test/key",
	}
	resp1, err := a.Prepare(ctx, req1)
	if err != nil {
		t.Errorf("Prepare failed: %v", err)
	}
	if !resp1.Promised {
		t.Errorf("Expected Promised=true")
	}
	if resp1.AgentId != agentID {
		t.Errorf("Expected AgentId=%s, got %s", agentID, resp1.AgentId)
	}

	// Prepare with lower ID should fail
	req2 := &paxosv1.PrepareRequest{
		AgentId:    "other-agent",
		ProposalId: &paxosv1.ProposalID{Number: 0, AgentId: "a"},
		Key:        "/test/key",
	}
	resp2, err := a.Prepare(ctx, req2)
	if err != nil {
		t.Errorf("Prepare failed: %v", err)
	}
	if resp2.Promised {
		t.Errorf("Expected Promised=false for lower ID")
	}
	if resp2.AgentId != agentID {
		t.Errorf("Expected AgentId=%s, got %s", agentID, resp2.AgentId)
	}

	// Accept with promised ID should succeed
	val := []byte("hello")
	req3 := &paxosv1.AcceptRequest{
		AgentId:    "other-agent",
		ProposalId: req1.ProposalId,
		Key:        "/test/key",
		Value:      val,
	}
	resp3, err := a.Accept(ctx, req3)
	if err != nil {
		t.Errorf("Accept failed: %v", err)
	}
	if !resp3.Accepted {
		t.Errorf("Expected Accepted=true")
	}
	if resp3.AgentId != agentID {
		t.Errorf("Expected AgentId=%s, got %s", agentID, resp3.AgentId)
	}
}
