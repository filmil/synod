// SPDX-License-Identifier: Apache-2.0

package server

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/filmil/synod/internal/paxos"
	"github.com/filmil/synod/internal/state"
)

func setupTestServer(t *testing.T) (*HTTPServer, func()) {
	stateDir, err := os.MkdirTemp("", "synod-test-*")
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

	// Initialize cell
	acceptor := paxos.NewAcceptor(agentID, ident, store)
	factory := func(agentID, addr string) (paxos.PeerClient, error) {
		return nil, nil // Dummy factory
	}
	cell := paxos.NewCell(agentID, store, ident, acceptor, factory, ":50101", "http://localhost:8081")

	server := NewHTTPServer(":8081", store, cell)

	cleanup := func() {
		store.Close()
		os.RemoveAll(stateDir)
	}

	return server, cleanup
}

func assertHTML(t *testing.T, body string, expectedTitle string) {
	if !strings.Contains(body, "<!doctype html>") {
		t.Errorf("expected <!doctype html>, got %v", body)
	}
	if !strings.Contains(body, expectedTitle) {
		t.Errorf("expected title %q, not found in body", expectedTitle)
	}
	if !strings.Contains(body, "</body>") {
		t.Errorf("expected </body>, got %v", body)
	}
	if !strings.Contains(body, "</html>") {
		t.Errorf("expected </html>, got %v", body)
	}
}

func TestHTTPServer_handleIndex(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()

	server.handleIndex(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	assertHTML(t, string(body), "<title>Status -")
	if !strings.Contains(string(body), "Agent Info") {
		t.Errorf("expected 'Agent Info' in body")
	}
}

func TestHTTPServer_handleMessages(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/messages", nil)
	w := httptest.NewRecorder()

	server.handleMessages(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	assertHTML(t, string(body), "<title>Messages")
	if !strings.Contains(string(body), "Recent Messages") {
		t.Errorf("expected 'Recent Messages' in body")
	}
}

func TestHTTPServer_handlePeers(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/peers", nil)
	w := httptest.NewRecorder()

	server.handlePeers(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	assertHTML(t, string(body), "<title>Peers")
	if !strings.Contains(string(body), "Known Peers") {
		t.Errorf("expected 'Known Peers' in body")
	}
}

func TestHTTPServer_handleStore(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/store", nil)
	w := httptest.NewRecorder()

	server.handleStore(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	assertHTML(t, string(body), "<title>KV Store")
	if !strings.Contains(string(body), "Key-Value Store") {
		t.Errorf("expected 'Key-Value Store' in body")
	}
}

func TestHTTPServer_handleUserAPI(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/userapi", nil)
	w := httptest.NewRecorder()

	server.handleUserAPI(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	assertHTML(t, string(body), "<title>User API")
	if !strings.Contains(string(body), "Read Key") {
		t.Errorf("expected 'Read Key' in body")
	}
}

func TestHTTPServer_handleSystem(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/system", nil)
	w := httptest.NewRecorder()

	server.handleSystem(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	assertHTML(t, string(body), "<title>System Introspection")
	if !strings.Contains(string(body), "Go Runtime Statistics") {
		t.Errorf("expected 'Go Runtime Statistics' in body")
	}
}
