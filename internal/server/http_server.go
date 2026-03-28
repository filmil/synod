package server

import (
	"fmt"
	"net/http"
	"github.com/golang/glog"
	"github.com/filmil/synod/internal/state"
)

type HTTPServer struct {
	store *state.Store
	addr  string
}

func NewHTTPServer(addr string, store *state.Store) *HTTPServer {
	return &HTTPServer{
		addr:  addr,
		store: store,
	}
}

func (s *HTTPServer) Run() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/api/command", s.handleCommand)

	glog.Infof("HTTP server listening at %v", s.addr)
	return http.ListenAndServe(s.addr, mux)
}

func (s *HTTPServer) handleIndex(w http.ResponseWriter, r *http.Request) {
	agentID, _ := s.store.GetAgentID()
	// Rudimentary HTML page as requested
	fmt.Fprintf(w, "<html><body>")
	fmt.Fprintf(w, "<h1>Synod Agent</h1>")
	fmt.Fprintf(w, "<p>Agent ID: %s</p>", agentID)
	fmt.Fprintf(w, "<h2>Participants</h2>")
	fmt.Fprintf(w, "<ul>")
	// TODO: List actual participants from membership table
	fmt.Fprintf(w, "</ul>")
	fmt.Fprintf(w, "</body></html>")
}

func (s *HTTPServer) handleCommand(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// TODO: Initiate paxos consensus for the provided command
	fmt.Fprintf(w, "Command received (consensus not yet implemented)")
}
