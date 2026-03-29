package integration

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/filmil/synod/internal/server"
	"github.com/filmil/synod/internal/state"
	paxosv1 "github.com/filmil/synod/proto/paxos/v1"
	"github.com/golang/glog"
)

func TestIntegration_Locks(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	numAgents := 3
	agents := make([]*agentInstance, numAgents)

	// Start 3 agents and form a cluster (copied from ping_test logic)
	for i := 0; i < numAgents; i++ {
		tmpDir, err := os.MkdirTemp("", fmt.Sprintf("paxos-lock-test-%d-*", i))
		if err != nil {
			t.Fatalf("failed to create temp dir: %v", err)
		}
		defer os.RemoveAll(tmpDir)

		agents[i] = newAgentInstance(t, fmt.Sprintf("agent-%d", i), tmpDir, "127.0.0.1:0")
		defer agents[i].stop()
		go agents[i].run()
		time.Sleep(500 * time.Millisecond) // Wait for server to be up
	}

	addr0 := agents[0].grpcAddr
	info0 := state.PeerInfo{ShortName: "agent-0"}
	agents[0].store.AddMember("agent-0", info0)
	agents[0].cell.ProposeMembership(context.Background(), "agent-0", info0)

	for i := 1; i < numAgents; i++ {
		infoI := state.PeerInfo{ShortName: fmt.Sprintf("agent-%d", i)}
		agents[i].store.AddMember(agents[i].id, infoI)

		client, _ := server.NewPaxosClient("temp-joiner", addr0)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		client.JoinCluster(ctx, &paxosv1.JoinClusterRequest{
			AgentId:   agents[i].id,
			HostPort:  agents[i].grpcAddr,
			ShortName: infoI.ShortName,
			HttpUrl:   agents[i].httpURL,
		})
		cancel()

		kvResp, _ := client.GetKVEntry(context.Background(), &paxosv1.GetKVEntryRequest{Key: "/_internal/peers"})
		agents[i].store.SetAcceptedValue("/_internal/peers", &paxosv1.ProposalID{Number: kvResp.Version, AgentId: "agent-0"}, kvResp.Value)
		agents[i].store.CommitKV("/_internal/peers", kvResp.Value, "membership", kvResp.Version)
		agents[i].cell.ApplyMembershipChange(kvResp.Value)
		agents[i].cell.UpdateEphemeralPeer("agent-0", addr0, agents[0].httpURL)
		client.Close()

		agents[0].cell.ProposeMembership(context.Background(), agents[i].id, infoI)
	}

	glog.V(1).Infof("Waiting for some time for the cell to stabilize.")
	time.Sleep(20 * time.Second)
	glog.V(1).Infof("DONE Waiting for some time for the cell to stabilize.")

	// Test 1: Acquire lock on /foo/_lockable/bar
	// This should acquire lock on /foo/_lockable
	ctx := context.Background()

	resp, err := agents[0].srv.AcquireLock(ctx, &paxosv1.AcquireLockRequest{
		KeyPath:    "/foo/_lockable/bar",
		DurationMs: 10000, // 10 seconds
	})
	if err != nil {
		t.Fatalf("AcquireLock failed: %v", err)
	}
	if !resp.Success {
		t.Fatalf("AcquireLock rejected: %s", resp.Message)
	}

	// Wait for sync
	time.Sleep(2 * time.Second)

	// Test 2: Try to write with another agent (should fail because lock on /foo/_lockable is held by agent-0)
	writeResp, err := agents[1].srv.CompareAndWrite(ctx, &paxosv1.CompareAndWriteRequest{
		Key:      "/foo/_lockable/bar",
		OldValue: nil,
		NewValue: []byte("hacked"),
	})
	if err == nil && writeResp.Success {
		t.Fatalf("Agent 1 successfully wrote to locked path /foo/_lockable/bar without owning the lock!")
	}

	// Test 3: Write with the owning agent (should succeed)
	writeResp, err = agents[0].srv.CompareAndWrite(ctx, &paxosv1.CompareAndWriteRequest{
		Key:      "/foo/_lockable/bar",
		OldValue: nil,
		NewValue: []byte("legit"),
	})
	if err != nil || !writeResp.Success {
		t.Fatalf("Agent 0 failed to write to its own locked path: %v / %v", err, writeResp.Message)
	}

	// Test 4: Release lock
	relResp, err := agents[0].srv.ReleaseLock(ctx, &paxosv1.ReleaseLockRequest{
		KeyPath: "/foo/_lockable/bar",
	})
	if err != nil || !relResp.Success {
		t.Fatalf("ReleaseLock failed: %v", err)
	}

	// Test 5: Write with another agent (should now succeed because lock is released)
	time.Sleep(2 * time.Second) // wait for sync
	writeResp, err = agents[1].srv.CompareAndWrite(ctx, &paxosv1.CompareAndWriteRequest{
		Key:      "/foo/_lockable/bar",
		OldValue: []byte("legit"),
		NewValue: []byte("hacked-legally"),
	})
	if err != nil || !writeResp.Success {
		t.Fatalf("Agent 1 failed to write to unlocked path: %v / %s", err, writeResp.GetMessage())
	}
}
