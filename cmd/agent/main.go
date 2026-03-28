package main

import (
	"context"
	"flag"
	"os"
	"path/filepath"
	"time"

	"github.com/golang/glog"
	"github.com/filmil/synod/internal/paxos"
	"github.com/filmil/synod/internal/server"
	"github.com/filmil/synod/internal/state"
	paxosv1 "github.com/filmil/synod/proto/paxos/v1"
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

	// Ensure self is in membership
	if err := store.AddMember(agentID, *grpcAddr); err != nil {
		glog.Fatalf("Failed to add self to membership: %v", err)
	}

	peerFactory := func(id, addr string) (paxos.PeerClient, error) {
		return server.NewPaxosClient(id, addr)
	}

	acceptor := paxos.NewAcceptor(agentID, store)
	cell := paxos.NewCell(agentID, store, acceptor, peerFactory)
	paxosSrv := server.NewPaxosServer(agentID, store, acceptor, cell)

	// If a peer is provided, we join the cluster.
	if *peerAddr != "" {
		glog.Infof("Attempting to join cluster via peer: %s", *peerAddr)
		client, err := server.NewPaxosClient("temp-peer", *peerAddr)
		if err != nil {
			glog.Errorf("Failed to connect to join peer %s: %v", *peerAddr, err)
		} else {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			resp, err := client.JoinCluster(ctx, &paxosv1.JoinClusterRequest{
				AgentId:  agentID,
				HostPort: *grpcAddr,
			})
			cancel()
			if err != nil {
				glog.Errorf("JoinCluster failed: %v", err)
			} else if !resp.Success {
				glog.Errorf("JoinCluster rejected: %s", resp.Message)
			} else {
				glog.Infof("Successfully joined cluster via %s", resp.AgentId)
				// Add the peer we joined to membership so we can sync
				if err := store.AddMember(resp.AgentId, *peerAddr); err != nil {
					glog.Errorf("Failed to add join peer to membership: %v", err)
				}
			}
			client.Close()
		}
	}

	// Start sync loop
	cell.StartSyncLoop(context.Background(), 5*time.Second)

	// Run servers
	errChan := make(chan error, 2)

	go func() {
		errChan <- server.RunGRPCServer(*grpcAddr, paxosSrv)
	}()

	go func() {
		httpSrv := server.NewHTTPServer(*httpAddr, store, cell)
		errChan <- httpSrv.Run()
	}()

	glog.Infof("Synod agent is up and running")
	
	if err := <-errChan; err != nil {
		glog.Fatalf("Server error: %v", err)
	}
}
