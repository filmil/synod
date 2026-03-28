package integration

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/filmil/synod/internal/paxos"
	"github.com/filmil/synod/internal/state"
	"github.com/filmil/synod/internal/server"
	paxosv1 "github.com/filmil/synod/proto/paxos/v1"
)

func TestIntegration_5Agents(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	numAgents := 5
	agents := make([]*agentInstance, numAgents)
	
	// Start the first agent (bootstrap)
	tmpDir0, _ := os.MkdirTemp("", "paxos-int-test-0-*")
	defer os.RemoveAll(tmpDir0)
	addr0 := "127.0.0.1:50100"
	agents[0] = newAgentInstance(t, "agent-0", tmpDir0, addr0)
	go agents[0].run()
	
	// Ensure self is in membership for agent-0
	agents[0].store.AddMember("agent-0", addr0)

	// Start other agents and join them to agent-0
	for i := 1; i < numAgents; i++ {
		tmpDir, _ := os.MkdirTemp("", fmt.Sprintf("paxos-int-test-%d-*", i))
		defer os.RemoveAll(tmpDir)

		addr := fmt.Sprintf("127.0.0.1:%d", 50100+i)
		agents[i] = newAgentInstance(t, fmt.Sprintf("agent-%d", i), tmpDir, addr)
		// Ensure self is in membership
		agents[i].store.AddMember(agents[i].id, addr)
		
		go agents[i].run()

		// Join agent-0
		time.Sleep(500 * time.Millisecond) // Wait for server to be up
		client, err := server.NewPaxosClient("temp-joiner", addr0)
		if err != nil {
			t.Fatalf("failed to create join client: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := client.JoinCluster(ctx, &paxosv1.JoinClusterRequest{
			AgentId:  agents[i].id,
			HostPort: addr,
		})
		cancel()
		if err != nil {
			t.Fatalf("JoinCluster failed for agent-%d: %v", i, err)
		}
		if !resp.Success {
			t.Fatalf("JoinCluster rejected for agent-%d: %s", i, resp.Message)
		}
		// Add agent-0 to local membership so we can sync
		if err := agents[i].store.AddMember(resp.AgentId, addr0); err != nil {
			t.Fatalf("Failed to add join peer to membership for agent-%d: %v", i, err)
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

	err := agents[0].cell.Propose(ctx, []byte("exit"))
	if err != nil {
		t.Fatalf("Consensus on 'exit' failed: %v", err)
	}
	
	// Wait for others to catch up the "exit" command
	time.Sleep(3 * time.Second)

	// Verify that all agents reached consensus on the "exit" command
	for i := 0; i < numAgents; i++ {
		index, _ := agents[i].store.GetHighestLedgerIndex()
		// Depending on how many joins happened, the "exit" command might be at different indices.
		// But it should be the LAST one.
		val, valType, err := agents[i].store.GetLedgerEntry(index)
		if err != nil {
			t.Errorf("Agent %d: failed to get ledger entry at %d: %v", i, index, err)
			continue
		}
		if string(val) != "exit" {
			t.Errorf("Agent %d: expected 'exit' at index %d, got '%s' (type %s)", i, index, string(val), valType)
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
	server.RunGRPCServer(a.addr, a.srv)
}
