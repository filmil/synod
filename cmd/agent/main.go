package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/filmil/synod/internal/names"
	"github.com/filmil/synod/internal/paxos"
	"github.com/filmil/synod/internal/server"
	"github.com/filmil/synod/internal/state"
	paxosv1 "github.com/filmil/synod/proto/paxos/v1"
	"github.com/golang/glog"
)

var (
	stateDir     = flag.String("state_dir", "", "Directory for state files (required)")
	grpcAddr     = flag.String("grpc_addr", ":50051", "gRPC address to listen on")
	httpAddr     = flag.String("http_addr", ":8080", "HTTP address to listen on")
	peerAddr     = flag.String("peer", "", "Address of an existing peer to join the cell")
	pingInterval = flag.Duration("ping_interval", 2*time.Minute, "Interval to ping peers")
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
		glog.Errorf("Failed to resolve absolute path for state_dir: %v", err)
		os.Exit(1)
	}

	store, err := state.NewStore(absStateDir)
	if err != nil {
		glog.Errorf("Failed to initialize state store: %v", err)
		os.Exit(1)
	}
	defer store.Close()

	agentID, shortName, err := store.GetAgentID()
	if err != nil {
		glog.Errorf("Failed to get agent ID: %v", err)
		os.Exit(1)
	}

	// If the node has no short name, generate one now.
	if shortName == "PendingName" || shortName == "" {
		shortName = names.Generate()
		if err := store.SetShortName(shortName); err != nil {
			glog.Errorf("Failed to set new short name: %v", err)
		}
	}

	glog.Infof("Starting Synod agent with ID: %s, Name: %s", agentID, shortName)

	// Create listeners with retry logic
	grpcLis, err := server.ListenWithRetry(*grpcAddr)
	if err != nil {
		glog.Errorf("Failed to create gRPC listener: %v", err)
		os.Exit(1)
	}
	finalGrpcAddr := grpcLis.Addr().String()

	httpLis, err := server.ListenWithRetry(*httpAddr)
	if err != nil {
		glog.Errorf("Failed to create HTTP listener: %v", err)
		os.Exit(1)
	}
	// finalHttpAddr := httpLis.Addr().String() // Not strictly needed for paxos consensus but good to know

	// Ensure self is in membership with the ACTUAL port we are listening on
	selfInfo := state.PeerInfo{
		GRPCAddr:  finalGrpcAddr,
		HTTPURL:   fmt.Sprintf("http://%s", httpLis.Addr().String()),
		ShortName: shortName,
	}
	if err := store.AddMember(agentID, selfInfo); err != nil {
		glog.Errorf("Failed to add self to membership: %v", err)
		os.Exit(1)
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
				AgentId:   agentID,
				HostPort:  finalGrpcAddr,
				ShortName: shortName,
				HttpUrl:   selfInfo.HTTPURL,
			})
			cancel()
			if err != nil {
				glog.Errorf("JoinCluster failed: %v", err)
			} else if !resp.Success {
				glog.Errorf("JoinCluster rejected: %s", resp.Message)
			} else {
				glog.Infof("Successfully joined cluster via %s", resp.AgentId)
				// Add the peer we joined to membership so we can sync. Since we don't have its full info, we store a stub.
				// It will be overwritten by the KV consensus download immediately after.
				if err := store.AddMember(resp.AgentId, state.PeerInfo{GRPCAddr: *peerAddr, ShortName: "Unknown"}); err != nil {
					glog.Errorf("Failed to add join peer to membership: %v", err)
				}

				// Download the consensus value of the list of peers
				ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
				kvResp, err := client.GetKVEntry(ctx, &paxosv1.GetKVEntryRequest{Key: "/_internal/peers"})
				cancel()
				if err != nil {
					glog.Errorf("Failed to get peers from join node: %v", err)
				} else if kvResp.Value != nil {
					if err := store.SetAcceptedValue("/_internal/peers", &paxosv1.ProposalID{Number: kvResp.Version, AgentId: resp.AgentId}, kvResp.Value); err != nil {
						glog.Errorf("Failed to set accepted value for peers: %v", err)
					}
					if err := store.CommitKV("/_internal/peers", kvResp.Value, "membership", kvResp.Version); err != nil {
						glog.Errorf("Failed to commit peers KV: %v", err)
					}
					cell.ApplyMembershipChange(kvResp.Value)
				}

				// After joining and getting the latest map, we must propose *ourselves* to the map!
				glog.Infof("Proposing newly joined node self to /_internal/peers")
				if err := cell.ProposeMembership(context.Background(), agentID, selfInfo); err != nil {
					glog.Errorf("Failed to add self to membership after joining: %v", err)
				}
			}
			client.Close()
		}
	} else {
		// Bootstrap node: propose ourselves to the KV store
		glog.Infof("Bootstrapping new cluster, proposing self to /_internal/peers")
		if err := cell.ProposeMembership(context.Background(), agentID, selfInfo); err != nil {
			glog.Errorf("Failed to bootstrap membership: %v", err)
			os.Exit(1)
		}
	}

	// Start sync loop
	cell.StartSyncLoop(context.Background(), 5*time.Second)

	// Start ping loop
	cell.StartPingLoop(context.Background(), *pingInterval)

	// Run servers
	errChan := make(chan error, 2)
	ctx := context.Background()

	go func() {
		errChan <- server.RunGRPCServer(ctx, grpcLis, paxosSrv)
	}()

	go func() {
		httpSrv := server.NewHTTPServer(*httpAddr, store, cell)
		errChan <- httpSrv.Run(httpLis)
	}()

	glog.Infof("Synod agent is up and running")

	if err := <-errChan; err != nil {
		glog.Errorf("Server error: %v", err)
		os.Exit(1)
	}
}
