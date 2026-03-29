package paxos

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/filmil/synod/internal/constants"

	"github.com/filmil/synod/internal/backoff"
	"github.com/filmil/synod/internal/state"
	paxosv1 "github.com/filmil/synod/proto/paxos/v1"
	"github.com/golang/glog"
	"github.com/google/uuid"
)

type MembershipChange struct {
	Action  string `json:"action"`
	AgentID string `json:"agent_id"`
	Address string `json:"address"`
}

type PeerClient interface {
	Prepare(ctx context.Context, req *paxosv1.PrepareRequest) (*paxosv1.PromiseResponse, error)
	Accept(ctx context.Context, req *paxosv1.AcceptRequest) (*paxosv1.AcceptedResponse, error)
	JoinCluster(ctx context.Context, req *paxosv1.JoinClusterRequest) (*paxosv1.JoinClusterResponse, error)
	Sync(ctx context.Context, req *paxosv1.SyncRequest) (*paxosv1.SyncResponse, error)
	GetKVEntry(ctx context.Context, req *paxosv1.GetKVEntryRequest) (*paxosv1.GetKVEntryResponse, error)
	Ping(ctx context.Context, req *paxosv1.PingRequest) (*paxosv1.PingResponse, error)
	GetPeerEndpoints(ctx context.Context, req *paxosv1.GetPeerEndpointsRequest) (*paxosv1.GetPeerEndpointsResponse, error)
	AgentID() string
}

type PeerFactory func(agentID, address string) (PeerClient, error)

type ConnectionInfo struct {
	GRPCAddr string
	HTTPURL  string
}

type Cell struct {
	agentID  string
	store    *state.Store
	acceptor *Acceptor
	proposer *Proposer

	mu             sync.Mutex
	peers          map[string]PeerClient
	ephemeralPeers map[string]ConnectionInfo // Key: AgentID (UUID)
	peerFactory    PeerFactory

	selfGRPCAddr string
	selfHTTPURL  string

	// Optional hook for lock checking during Propose
	lockChecker func(ctx context.Context, key string) error
}

func NewCell(agentID string, store *state.Store, acceptor *Acceptor, factory PeerFactory, selfGRPCAddr, selfHTTPURL string) *Cell {
	c := &Cell{
		agentID:        agentID,
		store:          store,
		acceptor:       acceptor,
		peers:          make(map[string]PeerClient),
		ephemeralPeers: make(map[string]ConnectionInfo),
		peerFactory:    factory,
		selfGRPCAddr:   selfGRPCAddr,
		selfHTTPURL:    selfHTTPURL,
	}
	if selfGRPCAddr != "" || selfHTTPURL != "" {
		c.ephemeralPeers[agentID] = ConnectionInfo{
			GRPCAddr: selfGRPCAddr,
			HTTPURL:  selfHTTPURL,
		}
	}
	c.proposer = NewProposer(agentID, nil, acceptor)
	return c
}

func (c *Cell) SetLockChecker(checker func(ctx context.Context, key string) error) {
	c.lockChecker = checker
}

func (c *Cell) SetSelfAddress(grpcAddr, httpURL string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.selfGRPCAddr = grpcAddr
	c.selfHTTPURL = httpURL
	c.ephemeralPeers[c.agentID] = ConnectionInfo{
		GRPCAddr: grpcAddr,
		HTTPURL:  httpURL,
	}
}

func (c *Cell) UpdateEphemeralPeer(agentID, grpcAddr, httpURL string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ephemeralPeers[agentID] = ConnectionInfo{
		GRPCAddr: grpcAddr,
		HTTPURL:  httpURL,
	}
}

func (c *Cell) GetEphemeralPeers() map[string]ConnectionInfo {
	c.mu.Lock()
	defer c.mu.Unlock()
	res := make(map[string]ConnectionInfo)
	for k, v := range c.ephemeralPeers {
		res[k] = v
	}
	return res
}

func (c *Cell) GetEphemeralPeer(agentID string) (ConnectionInfo, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	info, ok := c.ephemeralPeers[agentID]
	return info, ok
}

func (c *Cell) SetPeers(peers []PeerClient) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.peers = make(map[string]PeerClient)
	for _, p := range peers {
		c.peers[p.AgentID()] = p
	}
	c.proposer = NewProposer(c.agentID, peers, c.acceptor)
}

func (c *Cell) Propose(ctx context.Context, key string, value []byte, qt QuorumType) error {
	bo := backoff.New()
	bo.MaxElapsedTime = 2 * time.Minute

	return bo.Retry(ctx, "Propose", func() error {
		// Verify locks before proceeding
		if c.lockChecker != nil {
			if err := c.lockChecker(ctx, key); err != nil {
				return fmt.Errorf("write refused by lock policy: %w", err)
			}
		}

		_, _, version, _, err := c.store.GetKVEntry(key)
		if err != nil {
			return fmt.Errorf("failed to get KV entry for %s: %w", key, err)
		}
		nextVersion := version + 1
		instanceKey := fmt.Sprintf("%s@%d", key, nextVersion)

		glog.Infof("Cell(%s): Proposing data for %s", c.agentID, instanceKey)

		chosenValue, err := c.proposer.Propose(ctx, instanceKey, value, qt)
		if err != nil {
			return err
		}

		if err := c.store.CommitKV(key, chosenValue, "data", nextVersion); err != nil {
			return fmt.Errorf("failed to commit kv: %w", err)
		}

		if bytes.Equal(chosenValue, value) {
			return nil
		}
		glog.Infof("Cell(%s): Value at %s was overwritten by someone else, retrying via backoff...", c.agentID, instanceKey)
		return fmt.Errorf("concurrent data update during proposal")
	})
}

func (c *Cell) ProposeMembership(ctx context.Context, agentID string, info state.PeerInfo) error {
	key := constants.PeersKey
	bo := backoff.New()
	bo.MaxElapsedTime = 2 * time.Minute

	return bo.Retry(ctx, "ProposeMembership", func() error {
		val, _, version, _, err := c.store.GetKVEntry(key)
		if err != nil {
			return fmt.Errorf("failed to get KV entry for peers: %w", err)
		}
		var peers map[string]state.PeerInfo
		if val != nil {
			if err := json.Unmarshal(val, &peers); err != nil {
				glog.Errorf("Cell(%s): Failed to unmarshal peers map: %v", c.agentID, err)
				peers = make(map[string]state.PeerInfo)
			}
		} else {
			peers = make(map[string]state.PeerInfo)
		}

		// Check if already correct
		if existing, ok := peers[agentID]; ok && existing == info {
			glog.V(2).Infof("Cell(%s): Membership for %s is already correct", c.agentID, agentID)
			return nil
		}

		// Enforce cell-unique short name
		for id, peerInfo := range peers {
			if id != agentID && peerInfo.ShortName == info.ShortName {
				glog.V(2).Infof("Cell(%s): Enforce cell-unique short name: name rejected %q is already taken", c.agentID, info.ShortName)
				return fmt.Errorf("name rejected: short name %q is already taken", info.ShortName)
			}
		}

		peers[agentID] = info
		newVal, err := json.Marshal(peers)
		if err != nil {
			return fmt.Errorf("failed to marshal membership: %w", err)
		}

		nextVersion := version + 1
		instanceKey := fmt.Sprintf("%s@%d", key, nextVersion)

		glog.Infof("Cell(%s): Proposing membership for %s", c.agentID, instanceKey)

		chosenValue, err := c.proposer.Propose(ctx, instanceKey, newVal, QuorumMajority)
		if err != nil {
			return err
		}

		if err := c.store.CommitKV(key, chosenValue, "membership", nextVersion); err != nil {
			return fmt.Errorf("failed to commit kv: %w", err)
		}

		c.ApplyMembershipChange(chosenValue)

		if bytes.Equal(chosenValue, newVal) {
			return nil
		}
		glog.Infof("Cell(%s): Membership at %s was updated concurrently, retrying via backoff...", c.agentID, instanceKey)
		return fmt.Errorf("concurrent membership update")
	})
}

func (c *Cell) ProposeRemoval(ctx context.Context, agentID string) error {
	key := constants.PeersKey
	bo := backoff.New()
	bo.MaxElapsedTime = 2 * time.Minute

	return bo.Retry(ctx, "ProposeRemoval", func() error {
		val, _, version, _, err := c.store.GetKVEntry(key)
		if err != nil {
			return fmt.Errorf("failed to get KV entry for peers: %w", err)
		}
		var peers map[string]state.PeerInfo
		if val != nil {
			if err := json.Unmarshal(val, &peers); err != nil {
				glog.Errorf("Cell(%s): Failed to unmarshal peers map: %v", c.agentID, err)
				peers = make(map[string]state.PeerInfo)
			}
		} else {
			peers = make(map[string]state.PeerInfo)
		}

		// Check if already correct
		if _, ok := peers[agentID]; !ok {
			glog.V(2).Infof("Cell(%s): Peer %s is already removed", c.agentID, agentID)
			return nil
		}

		delete(peers, agentID)
		newVal, err := json.Marshal(peers)
		if err != nil {
			return fmt.Errorf("failed to marshal membership removal: %w", err)
		}

		nextVersion := version + 1
		instanceKey := fmt.Sprintf("%s@%d", key, nextVersion)

		glog.Infof("Cell(%s): Proposing removal for %s", c.agentID, agentID)

		chosenValue, err := c.proposer.Propose(ctx, instanceKey, newVal, QuorumMajority)
		if err != nil {
			return err
		}

		if err := c.store.CommitKV(key, chosenValue, "membership", nextVersion); err != nil {
			return fmt.Errorf("failed to commit kv: %w", err)
		}

		c.ApplyMembershipChange(chosenValue)

		if bytes.Equal(chosenValue, newVal) {
			return nil
		}
		glog.Infof("Cell(%s): Membership at %s was updated concurrently, retrying via backoff...", c.agentID, instanceKey)
		return fmt.Errorf("concurrent membership update during removal")
	})
}

func (c *Cell) ApplyMembershipChange(value []byte) {
	var peers map[string]state.PeerInfo
	if err := json.Unmarshal(value, &peers); err != nil {
		glog.Errorf("Cell(%s): Failed to unmarshal membership map: %v", c.agentID, err)
		return
	}

	// Current membership in DB
	currentMembers, err := c.store.GetMembers()
	if err != nil {
		glog.Errorf("Cell(%s): Failed to get current members from DB: %v", c.agentID, err)
		return
	}

	// Delete members that are not in the new map
	for id := range currentMembers {
		if _, ok := peers[id]; !ok {
			if err := c.store.RemoveMember(id); err != nil {
				glog.Errorf("Cell(%s): Failed to remove member %s from DB: %v", c.agentID, id, err)
			}
		}
	}

	// Add/Update members
	for id, info := range peers {
		if err := c.store.AddMember(id, info); err != nil {
			glog.Errorf("Cell(%s): Failed to add member %s to DB: %v", c.agentID, id, err)
		}
	}
}

func (c *Cell) refreshPeers(ctx context.Context) {
	members, err := c.store.GetMembers()
	if err != nil {
		glog.Errorf("Cell(%s): Failed to get members: %v", c.agentID, err)
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	newPeers := make(map[string]PeerClient)
	var peerList []PeerClient

	for id := range members {
		if id == c.agentID {
			continue
		}
		if p, ok := c.peers[id]; ok {
			newPeers[id] = p
			peerList = append(peerList, p)
		} else if c.peerFactory != nil {
			if eph, ok := c.ephemeralPeers[id]; ok {
				glog.Infof("Cell(%s): Creating new client for peer %s at %s", c.agentID, id, eph.GRPCAddr)
				p, err := c.peerFactory(id, eph.GRPCAddr)
				if err != nil {
					glog.Errorf("Cell(%s): Failed to create peer client for %s: %v", c.agentID, id, err)
					continue
				}
				newPeers[id] = p
				peerList = append(peerList, p)
			}
		}
	}
	c.peers = newPeers
	c.proposer = NewProposer(c.agentID, peerList, c.acceptor)
}

func (c *Cell) StartSyncLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		for {
			select {
			case <-ticker.C:
				c.SyncWithPeers(ctx)
			case <-ctx.Done():
				ticker.Stop()
				return
			}
		}
	}()
}

func (c *Cell) StartPingLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		for {
			select {
			case <-ticker.C:
				c.PingPeers(ctx)
			case <-ctx.Done():
				ticker.Stop()
				return
			}
		}
	}()
}

func (c *Cell) PingPeers(ctx context.Context) {
	glog.V(3).Infof("Cell(%s): Pinging peers", c.agentID)
	c.refreshPeers(ctx)

	c.mu.Lock()
	peers := make([]PeerClient, 0, len(c.peers))
	for _, p := range c.peers {
		peers = append(peers, p)
	}
	c.mu.Unlock()

	for _, p := range peers {
		nonce := uuid.New().String()
		pingCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
		glog.V(3).Infof("Cell(%s): Pinging peer %s", c.agentID, p.AgentID())
		resp, err := p.Ping(pingCtx, &paxosv1.PingRequest{
			AgentId:  c.agentID,
			Nonce:    nonce,
			HostPort: c.selfGRPCAddr,
			HttpUrl:  c.selfHTTPURL,
		})
		cancel()
		if err != nil {
			glog.Warningf("Cell(%s): Peer %s is not responding: %v. Proposing removal.", c.agentID, p.AgentID(), err)
			if err := c.ProposeRemoval(ctx, p.AgentID()); err != nil {
				glog.Errorf("Cell(%s): Failed to propose removal of peer %s: %v", c.agentID, p.AgentID(), err)
			}
		} else {
			// Update ephemeral map from response
			c.UpdateEphemeralPeer(resp.AgentId, resp.HostPort, resp.HttpUrl)
		}
	}
}

func (c *Cell) SyncWithPeers(ctx context.Context) {
	c.refreshPeers(ctx)

	c.mu.Lock()
	peers := make([]PeerClient, 0, len(c.peers))
	for _, p := range c.peers {
		peers = append(peers, p)
	}
	c.mu.Unlock()

	localState, err := c.store.GetKVState()
	if err != nil {
		glog.Errorf("Cell(%s): Failed to get local KV state: %v", c.agentID, err)
		return
	}

	for _, p := range peers {
		resp, err := p.Sync(ctx, &paxosv1.SyncRequest{
			AgentId:  c.agentID,
			HostPort: c.selfGRPCAddr,
			HttpUrl:  c.selfHTTPURL,
		})
		if err != nil {
			continue
		}

		for key, peerPropNum := range resp.Keys {
			localPropNum, exists := localState[key]
			if !exists || peerPropNum > localPropNum {
				glog.Infof("Cell(%s): Peer %s has newer state for key %s (%d > %d), catching up...",
					c.agentID, p.AgentID(), key, peerPropNum, localPropNum)
				c.CatchUp(ctx, p, key)
				// Update local state map after catch up
				_, _, newPropNum, _, err := c.store.GetKVEntry(key)
				if err != nil {
					glog.Errorf("Cell(%s): Failed to get updated KV entry for %s after catchup: %v", c.agentID, key, err)
				} else {
					localState[key] = newPropNum
				}
			}
		}
	}
}

func (c *Cell) CatchUp(ctx context.Context, p PeerClient, key string) {
	resp, err := p.GetKVEntry(ctx, &paxosv1.GetKVEntryRequest{Key: key})
	if err != nil {
		glog.Errorf("Cell(%s): Failed to get KV entry %s from %s: %v", c.agentID, key, p.AgentID(), err)
		return
	}
	if resp.Value == nil {
		return
	}
	glog.Infof("Cell(%s): Catching up entry %s type %s", c.agentID, key, resp.Type)

	// Ensure we also save the accepted value in the acceptor state so we don't propose over it with an older number
	if err := c.store.SetAcceptedValue(key, &paxosv1.ProposalID{Number: resp.Version, AgentId: p.AgentID()}, resp.Value); err != nil {
		glog.Errorf("Cell(%s): Failed to set accepted value during catchup: %v", c.agentID, err)
	}

	if err := c.store.CommitKV(key, resp.Value, resp.Type, resp.Version); err != nil {
		glog.Errorf("Cell(%s): Failed to commit caught up KV entry %s: %v", c.agentID, key, err)
		return
	}
	if resp.Type == "membership" {
		c.ApplyMembershipChange(resp.Value)
	}
}

func (c *Cell) GetSyncState() (map[string]uint64, error) {
	keys, err := c.store.GetKVState()
	if err != nil {
		return nil, fmt.Errorf("failed to get KV state: %w", err)
	}
	return keys, nil
}

// GetActivePeers returns a snapshot of the current connected peers.
func (c *Cell) GetActivePeers() []PeerClient {
	c.mu.Lock()
	defer c.mu.Unlock()
	peers := make([]PeerClient, 0, len(c.peers))
	for _, p := range c.peers {
		peers = append(peers, p)
	}
	return peers
}

// GetStore returns the underlying state store.
func (c *Cell) GetStore() *state.Store {
	return c.store
}

func (c *Cell) StartEndpointSyncLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		for {
			select {
			case <-ticker.C:
				c.syncMissingEndpoints(ctx)
			case <-ctx.Done():
				ticker.Stop()
				return
			}
		}
	}()
}

func (c *Cell) syncMissingEndpoints(ctx context.Context) {
	members, err := c.store.GetMembers()
	if err != nil {
		glog.Errorf("Cell(%s): EndpointSync: Failed to get members: %v", c.agentID, err)
		return
	}

	c.mu.Lock()
	var missing []string
	var knownPeers []PeerClient

	for id := range members {
		if id == c.agentID {
			continue
		}
		if _, ok := c.ephemeralPeers[id]; !ok {
			missing = append(missing, id)
		}
		if p, ok := c.peers[id]; ok {
			knownPeers = append(knownPeers, p)
		}
	}
	c.mu.Unlock()

	if len(missing) == 0 || len(knownPeers) == 0 {
		glog.V(3).Infof("Cell(%s): EndpointSync: Nothing to do or no one to ask", c.agentID)
		return
	}

	bo := backoff.New()
	bo.MaxElapsedTime = 5 * time.Minute

	bo.Retry(ctx, "SyncEndpoints", func() error {
		// Pick a random known peer
		// (rand is seeded globally)
		targetPeer := knownPeers[rand.Intn(len(knownPeers))]

		glog.V(2).Infof("Cell(%s): EndpointSync: Asking %s for endpoints of %v", c.agentID, targetPeer.AgentID(), missing)

		reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		resp, err := targetPeer.GetPeerEndpoints(reqCtx, &paxosv1.GetPeerEndpointsRequest{
			AgentId: c.agentID,
		})

		if err != nil {
			return fmt.Errorf("failed to get endpoints from %s: %w", targetPeer.AgentID(), err)
		}

		foundAny := false
		for missingID, info := range resp.Endpoints {
			if info.HostPort != "" {
				c.UpdateEphemeralPeer(missingID, info.HostPort, info.HttpUrl)
				glog.Infof("Cell(%s): EndpointSync: Discovered endpoints for %s via %s: %s", c.agentID, missingID, targetPeer.AgentID(), info.HostPort)
				foundAny = true
			}
		}

		if !foundAny {
			return fmt.Errorf("no missing endpoints were resolved by %s", targetPeer.AgentID())
		}
		return nil
	})
}
