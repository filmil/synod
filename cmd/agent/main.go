package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/filmil/synod/internal/backoff"
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

	// Ensure glog outputs to the state directory by default,
	// unless the user explicitly provided a log_dir flag.
	logDirSet := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "log_dir" {
			logDirSet = true
		}
	})
	if !logDirSet {
		flag.Set("log_dir", absStateDir)
	}

	// Force glog to create the log files (if it hasn't already)
	glog.CopyStandardLogTo("INFO")

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

	peerFactory := func(id, addr string) (paxos.PeerClient, error) {
		return server.NewPaxosClient(id, addr)
	}

	acceptor := paxos.NewAcceptor(agentID, store)
	cell := paxos.NewCell(agentID, store, acceptor, peerFactory, finalGrpcAddr, fmt.Sprintf("http://%s", httpLis.Addr().String()))
	paxosSrv := server.NewPaxosServer(agentID, store, acceptor, cell)

	var existingPeers map[string]state.PeerInfo
	var joinClient *server.PaxosClient

	if *peerAddr != "" {
		glog.Infof("Attempting to fetch peer info from: %s", *peerAddr)
		joinClient, err = server.NewPaxosClient("temp-peer", *peerAddr)
		if err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			kvResp, err := joinClient.GetKVEntry(ctx, &paxosv1.GetKVEntryRequest{Key: "/_internal/peers"})
			cancel()
			if err == nil && kvResp.Value != nil {
				json.Unmarshal(kvResp.Value, &existingPeers)
			}
		} else {
			glog.Errorf("Failed to connect to join peer %s: %v", *peerAddr, err)
		}
	}

	index := len(existingPeers)

	// If the node has no short name, generate one based on the cluster index.
	for {
		if shortName == "PendingName" || shortName == "" {
			shortName = names.GenerateForIndex(index)
		}
		taken := false
		for id, p := range existingPeers {
			if id != agentID && p.ShortName == shortName {
				taken = true
				break
			}
		}
		if !taken {
			break
		}
		shortName = "" // Regenerate
	}

	if err := store.SetShortName(shortName); err != nil {
		glog.Errorf("Failed to set new short name: %v", err)
	}

	glog.Infof("Starting Synod agent with ID: %s, Name: %s", agentID, shortName)

	selfInfo := state.PeerInfo{
		ShortName: shortName,
	}
	if err := store.AddMember(agentID, selfInfo); err != nil {
		glog.Errorf("Failed to add self to membership: %v", err)
		os.Exit(1)
	}

	// If a peer is provided, we join the cluster.
	if *peerAddr != "" {
		if joinClient != nil {
			var joinedAgentID string

			bo := backoff.New()
			bo.MaxElapsedTime = 2 * time.Minute

			err := bo.Retry(context.Background(), "JoinCluster", func() error {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()

				resp, err := joinClient.JoinCluster(ctx, &paxosv1.JoinClusterRequest{
					AgentId:   agentID,
					HostPort:  finalGrpcAddr,
					ShortName: shortName,
					HttpUrl:   fmt.Sprintf("http://%s", httpLis.Addr().String()),
				})

				if err != nil {
					return fmt.Errorf("JoinCluster failed: %w", err)
				}

				if !resp.Success {
					glog.Warningf("JoinCluster rejected: %s. Generating new name and retrying...", resp.Message)
					shortName = names.GenerateForIndex(index)
					selfInfo.ShortName = shortName
					store.SetShortName(shortName)
					glog.V(2).Infof("Returns error to trigger backoff retry: JoinCluster rejected: %s", resp.Message)
					return fmt.Errorf("JoinCluster rejected: %s", resp.Message)
				}

				glog.Infof("Successfully joined cluster via %s", resp.AgentId)
				joinedAgentID = resp.AgentId
				return nil
			})

			if err != nil {
				glog.Errorf("Failed to join cluster after retries: %v", err)
			}

			if joinedAgentID != "" {
				if err := store.AddMember(joinedAgentID, state.PeerInfo{ShortName: "Unknown"}); err != nil {
					glog.Errorf("Failed to add join peer to membership: %v", err)
				}
				// Also add the join peer to ephemeral map so we can talk to it
				cell.UpdateEphemeralPeer(joinedAgentID, *peerAddr, "")

				// Download the consensus value of the list of peers
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				kvResp, err := joinClient.GetKVEntry(ctx, &paxosv1.GetKVEntryRequest{Key: "/_internal/peers"})
				cancel()
				if err != nil {
					glog.Errorf("Failed to get peers from join node: %v", err)
				} else if kvResp.Value != nil {
					if err := store.SetAcceptedValue("/_internal/peers", &paxosv1.ProposalID{Number: kvResp.Version, AgentId: joinedAgentID}, kvResp.Value); err != nil {
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
			joinClient.Close()
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

	// Start endpoint sync loop
	cell.StartEndpointSyncLoop(context.Background(), 30*time.Second)

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
