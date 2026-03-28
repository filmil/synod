package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/golang/glog"
	"github.com/filmil/synod/internal/paxos"
	"github.com/filmil/synod/internal/server"
	"github.com/filmil/synod/internal/state"
)

var (
	stateDir = flag.String("state_dir", "", "Directory for state files (required)")
	grpcAddr = flag.String("grpc_addr", ":50051", "gRPC address to listen on")
	httpAddr = flag.String("http_addr", ":8080", "HTTP address to listen on")
	peerAddr = flag.String("peer", "", "Address of an existing peer to join the cell")
)

func main() {
	flag.Parse()

	if *stateDir == "" {
		glog.Error("--state_dir is required")
		os.Exit(1)
	}

	// Ensure stateDir is absolute
	absStateDir, err := filepath.Abs(*stateDir)
	if err != nil {
		glog.Fatalf("Failed to resolve absolute path for state_dir: %v", err)
	}

	store, err := state.NewStore(absStateDir)
	if err != nil {
		glog.Fatalf("Failed to initialize state store: %v", err)
	}
	defer store.Close()

	agentID, err := store.GetAgentID()
	if err != nil {
		glog.Fatalf("Failed to get agent ID: %v", err)
	}
	glog.Infof("Starting Synod agent with ID: %s", agentID)

	acceptor := paxos.NewAcceptor(store)
	paxosSrv := server.NewPaxosServer(store, acceptor)

	// In a real implementation, we would manage peers dynamically.
	// For now, if a peer is provided, we add it.
	var peers []paxos.PeerClient
	if *peerAddr != "" {
		peerClient, err := server.NewPaxosClient("remote-peer", *peerAddr)
		if err != nil {
			glog.Errorf("Failed to connect to peer %s: %v", *peerAddr, err)
		} else {
			peers = append(peers, peerClient)
		}
	}

	// proposer := paxos.NewProposer(agentID, peers)
	// _ = proposer

	// Run servers
	errChan := make(chan error, 2)

	go func() {
		errChan <- server.RunGRPCServer(*grpcAddr, paxosSrv)
	}()

	go func() {
		httpSrv := server.NewHTTPServer(*httpAddr, store)
		errChan <- httpSrv.Run()
	}()

	glog.Infof("Synod agent is up and running")
	
	if err := <-errChan; err != nil {
		glog.Fatalf("Server error: %v", err)
	}
}
