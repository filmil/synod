package integration

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/filmil/synod/internal/state"
)

func TestHTTPTemplate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	numAgents := 3
	agents := make([]*agentInstance, numAgents)

	// Start the first agent (bootstrap)
	tmpDir0 := t.TempDir()
	addr0 := "127.0.0.1:0"
	agents[0] = newAgentInstance(t, "agent-0", tmpDir0, addr0)
	defer agents[0].stop()
	go agents[0].run()

	// Wait for agent-0 to be up and get its actual address
	time.Sleep(500 * time.Millisecond)

	info0 := state.PeerInfo{
		ShortName: "agent-0-name",
	}

	// Ensure self is in membership for agent-0
	if err := agents[0].store.AddMember("agent-0", info0); err != nil {
		t.Fatalf("failed to add self to membership: %v", err)
	}

	// Wait for cluster to settle
	time.Sleep(2 * time.Second)

	endpoints := []string{
		"/",
		"/messages",
		"/peers",
		"/store",
		"/system",
		"/grpc",
		"/userapi",
	}

	client := &http.Client{Timeout: 5 * time.Second}

	for _, u := range endpoints {
		resp, err := client.Get(agents[0].httpURL + u)
		if err != nil {
			t.Fatalf("Failed to fetch %s: %v", u, err)
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatalf("Failed to read body for %s: %v", u, err)
		}

		content := string(body)

		// Basic structural checks
		if !strings.Contains(content, "<!doctype html>") {
			t.Errorf("Response for %s missing doctype", u)
		}
		if !strings.Contains(content, "<nav class=\"navbar") {
			t.Errorf("Response for %s missing navbar", u)
		}
		if !strings.Contains(content, "Synod Paxos Agent:") {
			t.Errorf("Response for %s missing title", u)
		}

		t.Logf("Fetched %s, length: %d", u, len(body))
	}
}
