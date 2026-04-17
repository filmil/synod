// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"github.com/filmil/synod/internal/constants"
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

func TestIntegration_PeerRemovalOnFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	numAgents := 3
	agents := make([]*agentInstance, numAgents)

	// Start 3 agents and form a cluster
	for i := 0; i < numAgents; i++ {
		tmpDir, err := os.MkdirTemp("", fmt.Sprintf("paxos-ping-test-%d-*", i))
		if err != nil {
			t.Fatalf("failed to create temp dir: %v", err)
		}
		defer os.RemoveAll(tmpDir)

		agents[i] = newAgentInstance(t, fmt.Sprintf("agent-%d", i), tmpDir, "127.0.0.1:0")
		defer agents[i].stop()
		go agents[i].run()
		time.Sleep(500 * time.Millisecond) // Wait for server to be up
	}

	// Bootstrap the cluster
	addr0 := agents[0].grpcAddr
	info0 := state.PeerInfo{
		ShortName: "agent-0",
		GRPCAddr:  addr0,
		HTTPURL:   agents[0].httpURL,
	}
	agents[0].store.AddMember(agents[0].id, info0)
	agents[0].cell.ProposeMembership(context.Background(), agents[0].id, info0)

	for i := 1; i < numAgents; i++ {
		infoI := state.PeerInfo{
			ShortName: fmt.Sprintf("agent-%d", i),
			GRPCAddr:  agents[i].grpcAddr,
			HTTPURL:   agents[i].httpURL,
		}
		agents[i].store.AddMember(agents[i].id, infoI)

		// Join via agent-0
		client, _ := server.NewPaxosClient("temp-joiner", addr0)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		client.JoinCluster(ctx, &paxosv1.JoinClusterRequest{
			AgentId:   agents[i].id,
			HostPort:  agents[i].grpcAddr,
			ShortName: infoI.ShortName,
			HttpUrl:   agents[i].httpURL,
		})
		cancel()

		// Set membership for new agent so it can sync
		kvResp, _ := client.GetKVEntry(context.Background(), &paxosv1.GetKVEntryRequest{Key: constants.PeersKey})
		agents[i].store.SetAcceptedValue(constants.PeersKey, &paxosv1.ProposalID{Number: kvResp.Version, AgentId: agents[0].id}, kvResp.Value)
		agents[i].store.CommitKV(constants.PeersKey, kvResp.Value, "membership", kvResp.Version)
		agents[i].cell.ApplyMembershipChange(kvResp.Value)

		client.Close()

		// Propose the new agent to the cluster
		agents[0].cell.ProposeMembership(context.Background(), agents[i].id, infoI)
	}



	// Wait for all agents to sync up the membership
	time.Sleep(5 * time.Second)

	// Verify all 3 agents are present
	members, _ := agents[0].store.GetMembers()
	if len(members) != 3 {
		t.Fatalf("Expected 3 members, got %d", len(members))
	}

	// Now "kill" agent-2
	glog.Infof("Killing agent-2...")
	agent2ID := agents[2].id
	agents[2].stop()

	// Wait for agent-0 or agent-1 to detect the failure and remove agent-2
	// Ping interval is 2s, plus time for paxos proposal
	glog.Infof("Waiting for failure detection and removal...")
	time.Sleep(10 * time.Second)

	// Verify agent-2 is gone from agent-0's view
	members, _ = agents[0].store.GetMembers()
	if _, ok := members[agent2ID]; ok {
		t.Errorf("Unresponsive agent-2 was not removed from agent-0's membership")
	}

	// Verify agent-2 is gone from agent-1's view
	members, _ = agents[1].store.GetMembers()
	if _, ok := members[agent2ID]; ok {
		t.Errorf("Unresponsive agent-2 was not removed from agent-1's membership")
	}

	// Verify total members is 2
	if len(members) != 2 {
		t.Errorf("Expected 2 members, got %d", len(members))
	}


}
