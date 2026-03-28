package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/filmil/synod/internal/paxos"
	"github.com/filmil/synod/internal/server"
	"github.com/filmil/synod/internal/state"
	paxosv1 "github.com/filmil/synod/proto/paxos/v1"
	"github.com/golang/glog"
)

func TestIntegration_5Agents(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	numAgents := 5
	agents := make([]*agentInstance, numAgents)

	// Start the first agent (bootstrap)
	tmpDir0, err := os.MkdirTemp("", "paxos-int-test-0-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir0)
	addr0 := "127.0.0.1:50100"
	agents[0] = newAgentInstance(t, "agent-0", tmpDir0, addr0)
	go agents[0].run()

	info0 := state.PeerInfo{
		GRPCAddr:  addr0,
		HTTPURL:   "",
		ShortName: "agent-0-name",
	}

	// Ensure self is in membership for agent-0
	if err := agents[0].store.AddMember("agent-0", info0); err != nil {
		t.Fatalf("failed to add member: %v", err)
	}

	// Bootstrap agent-0's KV store with itself
	if err := agents[0].cell.ProposeMembership(context.Background(), "agent-0", info0); err != nil {
		t.Fatalf("failed to bootstrap membership for agent-0: %v", err)
	}

	// Start other agents and join them to agent-0
	for i := 1; i < numAgents; i++ {
		tmpDir, err := os.MkdirTemp("", fmt.Sprintf("paxos-int-test-%d-*", i))
		if err != nil {
			t.Fatalf("failed to create temp dir: %v", err)
		}
		defer os.RemoveAll(tmpDir)

		addr := fmt.Sprintf("127.0.0.1:%d", 50100+i)
		agents[i] = newAgentInstance(t, fmt.Sprintf("agent-%d", i), tmpDir, addr)
		
		infoI := state.PeerInfo{
			GRPCAddr:  addr,
			HTTPURL:   "",
			ShortName: fmt.Sprintf("agent-%d-name", i),
		}

		// Ensure self is in membership
		if err := agents[i].store.AddMember(agents[i].id, infoI); err != nil {
			t.Fatalf("failed to add member: %v", err)
		}
		go agents[i].run()

		// Join agent-0
		time.Sleep(500 * time.Millisecond) // Wait for server to be up
		client, err := server.NewPaxosClient("temp-joiner", addr0)
		if err != nil {
			t.Fatalf("failed to create join client: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := client.JoinCluster(ctx, &paxosv1.JoinClusterRequest{
			AgentId:   agents[i].id,
			HostPort:  addr,
			ShortName: infoI.ShortName,
			HttpUrl:   infoI.HTTPURL,
		})
		cancel()
		if err != nil {
			t.Fatalf("JoinCluster failed for agent-%d: %v", i, err)
		}
		if !resp.Success {
			t.Fatalf("JoinCluster rejected for agent-%d: %s", i, resp.Message)
		}
		// Add agent-0 to local membership so we can sync
		if err := agents[i].store.AddMember(resp.AgentId, info0); err != nil {
			t.Fatalf("Failed to add join peer to membership for agent-%d: %v", i, err)
		}

		// Download consensus value of peers
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		kvResp, err := client.GetKVEntry(ctx, &paxosv1.GetKVEntryRequest{Key: "/_internal/peers"})
		cancel()
		if err != nil {
			t.Fatalf("Failed to get peers from join node: %v", err)
		} else if kvResp.Value != nil {
			if err := agents[i].store.SetAcceptedValue("/_internal/peers", &paxosv1.ProposalID{Number: kvResp.Version, AgentId: resp.AgentId}, kvResp.Value); err != nil {
				t.Fatalf("Failed to set accepted value for peers: %v", err)
			}
			if err := agents[i].store.CommitKV("/_internal/peers", kvResp.Value, "membership", kvResp.Version); err != nil {
				t.Fatalf("Failed to commit peers KV: %v", err)
			}
			agents[i].cell.ApplyMembershipChange(kvResp.Value)
		}
		
		client.Close()
	}

	// Start sync loops for all agents
	for i := 0; i < numAgents; i++ {
		agents[i].cell.StartSyncLoop(context.Background(), 1*time.Second)
	}

	// Wait for all agents to sync up the membership
	time.Sleep(5 * time.Second)

	// Propose "exit" command from agent-0
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	err = agents[0].cell.Propose(ctx, "/app/state", []byte("exit"))
	if err != nil {
		t.Fatalf("Consensus on '/app/state' failed: %v", err)
	}

	// Wait for others to catch up the "exit" command
	time.Sleep(3 * time.Second)

	// Verify that all agents reached consensus on the "exit" command
	for i := 0; i < numAgents; i++ {
		val, valType, _, _, getErr := agents[i].store.GetKVEntry("/app/state")
		if getErr != nil {
			t.Errorf("Agent %d: failed to get KV entry for /app/state: %v", i, getErr)
			continue
		}
		if string(val) != "exit" {
			t.Errorf("Agent %d: expected 'exit' for /app/state, got '%s' (type %s)", i, string(val), valType)
		}

		// Verify that all 5 agents are mentioned in /_internal/peers
		peerVal, _, _, _, getErr := agents[i].store.GetKVEntry("/_internal/peers")
		if getErr != nil {
			t.Errorf("Agent %d: failed to get KV entry for /_internal/peers: %v", i, getErr)
			continue
		}
		var peers map[string]state.PeerInfo
		if err := json.Unmarshal(peerVal, &peers); err != nil {
			t.Errorf("Agent %d: failed to unmarshal peers: %v", i, err)
			continue
		}
		if len(peers) != numAgents {
			t.Errorf("Agent %d: expected %d peers, got %d", i, numAgents, len(peers))
		}
		for j := 0; j < numAgents; j++ {
			expectedID := fmt.Sprintf("agent-%d", j)
			if _, ok := peers[expectedID]; !ok {
				t.Errorf("Agent %d: missing expected peer %s", i, expectedID)
			}
		}
	}
}

type agentInstance struct {
	id    string
	addr  string
	dir   string
	store *state.Store
	cell  *paxos.Cell
	srv   *server.PaxosServer
}

func newAgentInstance(t *testing.T, id, dir, addr string) *agentInstance {
	store, err := state.NewStore(dir)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	peerFactory := func(id, addr string) (paxos.PeerClient, error) {
		return server.NewPaxosClient(id, addr)
	}

	acceptor := paxos.NewAcceptor(id, store)
	cell := paxos.NewCell(id, store, acceptor, peerFactory)
	srv := server.NewPaxosServer(id, store, acceptor, cell)

	return &agentInstance{
		id:    id,
		addr:  addr,
		dir:   dir,
		store: store,
		cell:  cell,
		srv:   srv,
	}
}

func (a *agentInstance) run() {
	lis, err := server.ListenWithRetry(a.addr)
	if err != nil {
		glog.Errorf("failed to listen: %v", err)
		return
	}
	server.RunGRPCServer(lis, a.srv)
}
