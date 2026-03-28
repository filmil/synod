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
)

func TestIntegration_5Agents(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	numAgents := 5
	agents := make([]*agentInstance, numAgents)
	
	// Create temp directories and start agents
	for i := 0; i < numAgents; i++ {
		tmpDir, _ := os.MkdirTemp("", fmt.Sprintf("paxos-int-test-%d-*", i))
		defer os.RemoveAll(tmpDir)

		addr := fmt.Sprintf("127.0.0.1:%d", 50100+i)
		agents[i] = newAgentInstance(t, fmt.Sprintf("agent-%d", i), tmpDir, addr)
		go agents[i].run()
	}

	// Wait for servers to start
	time.Sleep(2 * time.Second)

	// Inter-connect agents
	for i := 0; i < numAgents; i++ {
		var peers []paxos.PeerClient
		for j := 0; j < numAgents; j++ {
			if i == j {
				continue
			}
			client, err := server.NewPaxosClient(agents[j].id, agents[j].addr)
			if err != nil {
				t.Fatalf("failed to connect peer: %v", err)
			}
			peers = append(peers, client)
		}
		agents[i].cell.SetPeers(peers)
	}

	// Propose "exit" command from agent-0
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	err := agents[0].cell.Propose(ctx, []byte("exit"))
	if err != nil {
		t.Fatalf("Consensus on 'exit' failed: %v", err)
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

	acceptor := paxos.NewAcceptor(id, store)
	cell := paxos.NewCell(id, store, acceptor)
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
