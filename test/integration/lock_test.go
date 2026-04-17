// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/filmil/synod/internal/backoff"
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
	info0 := state.PeerInfo{ShortName: "agent-0", GRPCAddr: addr0, HTTPURL: agents[0].httpURL}
	agents[0].store.AddMember(agents[0].id, info0)
	agents[0].cell.ProposeMembership(context.Background(), agents[0].id, info0)

	for i := 1; i < numAgents; i++ {
		infoI := state.PeerInfo{ShortName: fmt.Sprintf("agent-%d", i), GRPCAddr: agents[i].grpcAddr, HTTPURL: agents[i].httpURL}
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
		agents[i].store.SetAcceptedValue("/_internal/peers", &paxosv1.ProposalID{Number: kvResp.Version, AgentId: agents[0].id}, kvResp.Value)
		agents[i].store.CommitKV("/_internal/peers", kvResp.Value, "membership", kvResp.Version)
		agents[i].cell.ApplyMembershipChange(kvResp.Value)
		client.Close()

		agents[0].cell.ProposeMembership(context.Background(), agents[i].id, infoI)
	}

	glog.V(1).Infof("Waiting for some time for the cell to stabilize.")
	time.Sleep(20 * time.Second)
	glog.V(1).Infof("DONE Waiting for some time for the cell to stabilize.")

	// Test 1: Acquire lock on /foo/_lockable/bar
	// This should acquire lock on /foo/_lockable
	glog.V(1).Infof("Test 1")
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute+30*time.Second)
	defer cancel()

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
	glog.V(1).Infof("Test 4: release lock")
	relResp, err := agents[0].srv.ReleaseLock(ctx, &paxosv1.ReleaseLockRequest{
		KeyPath: "/foo/_lockable/bar",
	})
	if err != nil || !relResp.Success {
		t.Fatalf("ReleaseLock failed: %v", err)
	}
	glog.V(1).Infof("Test 4: lock hopefully released")

	// Test 5: Write with another agent (should now succeed because lock is released)
	time.Sleep(2 * time.Second) // wait for sync

	glog.V(1).Infof("Test 4: attempt to reacquire the lock via a different agent.")
	// Agent 1 must acquire the lock first because /foo/_lockable/bar is a lockable path.
	bo := backoff.New()
	bo.MaxElapsedTime = 30 * time.Second

	err = bo.Retry(ctx, "Test5_AcquireLock", func() error {
		resp, err = agents[1].srv.AcquireLock(ctx, &paxosv1.AcquireLockRequest{
			KeyPath:    "/foo/_lockable/bar",
			DurationMs: 10000,
		})
		if err != nil {
			return err
		}
		if !resp.Success {
			return fmt.Errorf("lock acquire rejected: %s", resp.GetMessage())
		}
		return nil
	})

	if err != nil {
		t.Fatalf("Agent 1 failed to acquire lock after retries: %v", err)
	}

	writeResp, err = agents[1].srv.CompareAndWrite(ctx, &paxosv1.CompareAndWriteRequest{
		Key:      "/foo/_lockable/bar",
		OldValue: []byte("legit"),
		NewValue: []byte("hacked-legally"),
	})
	if err != nil || !writeResp.Success {
		t.Fatalf("Agent 1 failed to write to unlocked path: %v / %s", err, writeResp.GetMessage())
	}

	// Test 6: Nested locks (multiple _lockable components)
	glog.V(1).Infof("Test 6: Acquire nested lock on /nested/_lockable/path/_lockable/baz")
	nestedResp, err := agents[0].srv.AcquireLock(ctx, &paxosv1.AcquireLockRequest{
		KeyPath:    "/nested/_lockable/path/_lockable/baz",
		DurationMs: 10000,
	})
	if err != nil || !nestedResp.Success {
		t.Fatalf("Agent 0 failed to acquire nested lock: %v / %s", err, nestedResp.GetMessage())
	}

	time.Sleep(2 * time.Second)

	// Agent 1 tries to write to the full nested path (should fail because it doesn't hold the lock)
	nestedWriteResp, err := agents[1].srv.CompareAndWrite(ctx, &paxosv1.CompareAndWriteRequest{
		Key:      "/nested/_lockable/path/_lockable/baz",
		OldValue: nil,
		NewValue: []byte("hacked-nested"),
	})
	if err == nil && nestedWriteResp.Success {
		t.Fatalf("Agent 1 successfully wrote to locked nested path without owning the lock!")
	}

	// Agent 1 tries to write to a path protected by ONLY the first level lock (should fail)
	nestedWriteResp, err = agents[1].srv.CompareAndWrite(ctx, &paxosv1.CompareAndWriteRequest{
		Key:      "/nested/_lockable/other",
		OldValue: nil,
		NewValue: []byte("hacked-first-level"),
	})
	if err == nil && nestedWriteResp.Success {
		t.Fatalf("Agent 1 successfully wrote to first-level locked path without owning the lock!")
	}

	// Agent 0 writes to the full nested path (should succeed)
	nestedWriteResp, err = agents[0].srv.CompareAndWrite(ctx, &paxosv1.CompareAndWriteRequest{
		Key:      "/nested/_lockable/path/_lockable/baz",
		OldValue: nil,
		NewValue: []byte("legit-nested"),
	})
	if err != nil || !nestedWriteResp.Success {
		t.Fatalf("Agent 0 failed to write to its own locked nested path: %v / %v", err, nestedWriteResp.Message)
	}

	// Release nested locks
	nestedRelResp, err := agents[0].srv.ReleaseLock(ctx, &paxosv1.ReleaseLockRequest{
		KeyPath: "/nested/_lockable/path/_lockable/baz",
	})
	if err != nil || !nestedRelResp.Success {
		t.Fatalf("Release nested lock failed: %v", err)
	}
}
