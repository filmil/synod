package integration

import (
	"github.com/filmil/synod/internal/constants"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/filmil/synod/internal/paxos"
	"github.com/filmil/synod/internal/server"
	"github.com/filmil/synod/internal/userapi"
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
	addr0 := "127.0.0.1:0"
	agents[0] = newAgentInstance(t, "agent-0", tmpDir0, addr0)
	defer agents[0].stop()
	go agents[0].run()

	// Wait for agent-0 to be up and get its actual address
	time.Sleep(500 * time.Millisecond)
	addr0 = agents[0].grpcAddr

	info0 := state.PeerInfo{
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

		agents[i] = newAgentInstance(t, fmt.Sprintf("agent-%d", i), tmpDir, "127.0.0.1:0")
		defer agents[i].stop()
		go agents[i].run()
		time.Sleep(500 * time.Millisecond) // Wait for server to be up

		infoI := state.PeerInfo{
			ShortName: fmt.Sprintf("agent-%d-name", i),
		}

		// Ensure self is in membership
		if err := agents[i].store.AddMember(agents[i].id, infoI); err != nil {
			t.Fatalf("failed to add member: %v", err)
		}

		// Join agent-0
		client, err := server.NewPaxosClient("temp-joiner", addr0)
		if err != nil {
			t.Fatalf("failed to create join client: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := client.JoinCluster(ctx, &paxosv1.JoinClusterRequest{
			AgentId:   agents[i].id,
			HostPort:  agents[i].grpcAddr,
			ShortName: infoI.ShortName,
			HttpUrl:   agents[i].httpURL,
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
		// Also add agent-0 to ephemeral map so we can talk to it
		agents[i].cell.UpdateEphemeralPeer(resp.AgentId, addr0, agents[0].httpURL)

		// Download consensus value of peers
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		kvResp, err := client.GetKVEntry(ctx, &paxosv1.GetKVEntryRequest{Key: constants.PeersKey})
		cancel()
		if err != nil {
			t.Fatalf("Failed to get peers from join node: %v", err)
		} else if kvResp.Value != nil {
			if err := agents[i].store.SetAcceptedValue(constants.PeersKey, &paxosv1.ProposalID{Number: kvResp.Version, AgentId: resp.AgentId}, kvResp.Value); err != nil {
				t.Fatalf("Failed to set accepted value for peers: %v", err)
			}
			if err := agents[i].store.CommitKV(constants.PeersKey, kvResp.Value, "membership", kvResp.Version); err != nil {
				t.Fatalf("Failed to commit peers KV: %v", err)
			}
			agents[i].cell.ApplyMembershipChange(kvResp.Value)
		}

		client.Close()

		// Propose the new agent to the cluster via agent-0 (since we already joined it)
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		if err := agents[0].cell.ProposeMembership(ctx, agents[i].id, infoI); err != nil {
			t.Fatalf("Failed to propose membership for agent-%d: %v", i, err)
		}
		cancel()
	}



	// Wait for all agents to sync up the membership
	time.Sleep(5 * time.Second)

	// Verify that all agents have all 5 agents in their peer list via HTTP
	for i := 0; i < numAgents; i++ {
		resp, err := http.Get(agents[i].httpURL + "/peers")
		if err != nil {
			t.Errorf("Agent %d: failed to GET /peers: %v", i, err)
			continue
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Errorf("Agent %d: failed to read /peers body: %v", i, err)
			continue
		}

		content := string(body)
		for j := 0; j < numAgents; j++ {
			expectedID := fmt.Sprintf("agent-%d", j)
			// Agent i's /peers page lists OTHER peers. The main page lists all.
			// Let's check both or just the main page.
			// Actually handlePeers excludes self: if id != agentID { ids = append(ids, id) }
			if i == j {
				continue
			}
			if !strings.Contains(content, expectedID) {
				t.Errorf("Agent %d: /peers does not contain expected peer %s", i, expectedID)
			}
		}

		// Also check index page which should have all participants including self
		resp, err = http.Get(agents[i].httpURL + "/")
		if err != nil {
			t.Errorf("Agent %d: failed to GET /: %v", i, err)
			continue
		}
		body, err = io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Errorf("Agent %d: failed to read / body: %v", i, err)
			continue
		}
		content = string(body)
		for j := 0; j < numAgents; j++ {
			expectedID := fmt.Sprintf("agent-%d", j)
			if !strings.Contains(content, expectedID) {
				t.Errorf("Agent %d: / does not contain expected participant %s", i, expectedID)
			}
		}
	}

	// Propose "exit" command from agent-0
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	err = agents[0].cell.Propose(ctx, "/app/state", []byte("exit"), paxos.QuorumMajority)
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
	}
}

type agentInstance struct {
	id         string
	grpcAddr   string
	httpURL    string
	dir        string
	store      *state.Store
	cell       *paxos.Cell
	srv        *server.PaxosServer
	cancelFunc context.CancelFunc
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
	cell := paxos.NewCell(id, store, acceptor, peerFactory, "", "")
	srv := server.NewPaxosServer(id, store, acceptor, cell)

	uAPI := userapi.New(cell)
	cell.SetLockChecker(func(ctx context.Context, key string) error {
		return uAPI.CheckLocks(ctx, key)
	})

	return &agentInstance{
		id:    id,
		dir:   dir,
		store: store,
		cell:  cell,
		srv:   srv,
	}
}

func (a *agentInstance) run() {
	ctx, cancel := context.WithCancel(context.Background())
	a.cancelFunc = cancel

	grpcLis, err := server.ListenWithRetry("127.0.0.1:0")
	if err != nil {
		glog.Errorf("failed to listen gRPC: %v", err)
		return
	}
	a.grpcAddr = grpcLis.Addr().String()

	httpLis, err := server.ListenWithRetry("127.0.0.1:0")
	if err != nil {
		glog.Errorf("failed to listen HTTP: %v", err)
		return
	}
	a.httpURL = fmt.Sprintf("http://%s", httpLis.Addr().String())

	a.cell.SetSelfAddress(a.grpcAddr, a.httpURL)

	// Start agent background loops
	a.cell.StartSyncLoop(ctx, 1*time.Second)
	a.cell.StartPingLoop(ctx, 2*time.Second)
	a.cell.StartEndpointSyncLoop(ctx, 2*time.Second)

	go func() {
		server.RunGRPCServer(ctx, grpcLis, a.srv)
	}()

	go func() {
		httpSrv := server.NewHTTPServer(a.httpURL, a.store, a.cell)
		httpSrv.Run(httpLis)
	}()

	<-ctx.Done()
	grpcLis.Close()
	httpLis.Close()
}

func (a *agentInstance) stop() {
	if a.cancelFunc != nil {
		a.cancelFunc()
	}
	a.store.Close()
}
