// SPDX-License-Identifier: Apache-2.0

package paxos

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/filmil/synod/internal/constants"

	"github.com/filmil/synod/internal/backoff"
	"github.com/filmil/synod/internal/identity"
	"github.com/filmil/synod/internal/state"
	paxosv1 "github.com/filmil/synod/proto/paxos/v1"
	"github.com/golang/glog"
	"github.com/google/uuid"
)

// MembershipChange describes a cluster membership change.
type MembershipChange struct {
	// Action is the type of change (e.g. "add", "remove").
	Action string `json:"action"`
	// AgentID is the ID of the agent being changed.
	AgentID string `json:"agent_id"`
	// Address is the address of the agent being added.
	Address string `json:"address"`
}

// PeerClient represents an interface for communicating with a remote Paxos peer.
type PeerClient interface {
	// Prepare sends a PrepareRequest to the remote peer.
	Prepare(ctx context.Context, req *paxosv1.PrepareRequest) (*paxosv1.PromiseResponse, error)
	// Accept sends an AcceptRequest to the remote peer.
	Accept(ctx context.Context, req *paxosv1.AcceptRequest) (*paxosv1.AcceptedResponse, error)
	// JoinCluster requests the remote peer to allow this agent to join the cluster.
	JoinCluster(ctx context.Context, req *paxosv1.JoinClusterRequest) (*paxosv1.JoinClusterResponse, error)
	// Sync synchronizes data with the remote peer.
	Sync(ctx context.Context, req *paxosv1.SyncRequest) (*paxosv1.SyncResponse, error)
	// GetKVEntry retrieves a key-value entry from the remote peer.
	GetKVEntry(ctx context.Context, req *paxosv1.GetKVEntryRequest) (*paxosv1.GetKVEntryResponse, error)
	// Ping sends a PingRequest to check if the peer is alive.
	Ping(ctx context.Context, req *paxosv1.PingRequest) (*paxosv1.PingResponse, error)
	// GetPeerEndpoints fetches endpoint information from the peer.
	GetPeerEndpoints(ctx context.Context, req *paxosv1.GetPeerEndpointsRequest) (*paxosv1.GetPeerEndpointsResponse, error)
	// AgentID returns the ID of the remote peer.
	AgentID() string
	// Close terminates the connection to the peer.
	Close() error
}

// PeerFactory creates a PeerClient given an agent ID and address.
type PeerFactory func(agentID, address string) (PeerClient, error)

// ConnectionInfo encapsulates network connection details for an agent.
type ConnectionInfo struct {
	// GRPCAddr is the host:port for gRPC communication.
	GRPCAddr string
	// HTTPURL is the base URL for the agent's HTTP dashboard.
	HTTPURL string
}

// Cell coordinates the Paxos consensus process for this agent, managing
// communication with peers, handling proposals, and maintaining state.
type Cell struct {
	agentID  string
	store    *state.Store
	ident    *identity.Identity
	acceptor *Acceptor
	proposer *Proposer

	mu          sync.Mutex
	peers       map[string]PeerClient
	peerFactory PeerFactory

	selfGRPCAddr string
	selfHTTPURL  string

	// Optional hook for lock checking during Propose
	lockChecker func(ctx context.Context, key string) error

	ShutdownChan        chan struct{}
	shuttingDown        bool
	selfInfo            state.PeerInfo
	missedEndpointCount map[string]int
}

func NewCell(agentID string, store *state.Store, ident *identity.Identity, acceptor *Acceptor, factory PeerFactory, selfGRPCAddr, selfHTTPURL string) *Cell {
	c := &Cell{
		agentID:             agentID,
		store:               store,
		ident:               ident,
		acceptor:            acceptor,
		peers:               make(map[string]PeerClient),
		peerFactory:         factory,
		selfGRPCAddr:        selfGRPCAddr,
		selfHTTPURL:         selfHTTPURL,
		ShutdownChan:        make(chan struct{}),
		proposer:            NewProposer(agentID, ident, []PeerClient{}, acceptor),
		missedEndpointCount: make(map[string]int),
	}
	c.acceptor.SetValidator(c.validateAccept)
	return c
}

func (c *Cell) validateAccept(ctx context.Context, req *paxosv1.AcceptRequest) error {
	// Only validate membership updates
	if !strings.HasPrefix(req.Key, constants.PeersKey+"@") {
		return nil
	}

	var newPeers map[string]state.PeerInfo
	if err := json.Unmarshal(req.Value, &newPeers); err != nil {
		return fmt.Errorf("failed to unmarshal proposed peers: %w", err)
	}

	members, err := c.store.GetMembers()
	if err != nil {
		return fmt.Errorf("failed to read current peers: %w", err)
	}

	// Check if any peer is being removed
	for currID, currInfo := range members {
		if _, ok := newPeers[currID]; !ok {
			// currID is being removed.
			// If the proposal comes from the node being removed (voluntary shutdown), allow it.
			if currID == req.AgentId {
				continue
			}

			// Otherwise, verify if it's truly unreachable
			glog.Infof("Cell(%s): Validating removal of peer %s proposed by %s", c.agentID, currID, req.AgentId)
			if err := c.pingNode(ctx, currID, currInfo); err == nil {
				return fmt.Errorf("refusing removal of reachable peer %s", currID)
			} else {
				glog.Infof("Cell(%s): Peer %s confirmed unreachable (%v), allowing removal", c.agentID, currID, err)
			}
		}
	}
	return nil
}

func (c *Cell) pingNode(ctx context.Context, id string, info state.PeerInfo) error {
	if id == c.agentID {
		return nil
	}

	c.mu.Lock()
	client, ok := c.peers[id]
	c.mu.Unlock()

	var tempClient bool
	if !ok {
		if c.peerFactory == nil || info.GRPCAddr == "" {
			return fmt.Errorf("cannot ping node: no factory or address")
		}
		var err error
		client, err = c.peerFactory(id, info.GRPCAddr)
		if err != nil {
			return err
		}
		tempClient = true
	}
	if tempClient {
		defer client.Close()
	}

	pingCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	nonce := uuid.New().String()
	req := &paxosv1.PingRequest{
		AgentId:  c.agentID,
		Nonce:    nonce,
		HostPort: c.selfGRPCAddr,
		HttpUrl:  c.selfHTTPURL,
	}

	if c.ident != nil {
		sig, cert, err := c.ident.SignMessage(req)
		if err == nil {
			req.Auth = &paxosv1.Authentication{
				Signature:   sig,
				Certificate: cert,
			}
		}
	}

	_, err := client.Ping(pingCtx, req)
	return err
}

func (c *Cell) SetSelfInfo(info state.PeerInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.selfInfo = info
}

func (c *Cell) TriggerShutdown() {
	c.mu.Lock()
	if c.shuttingDown {
		c.mu.Unlock()
		return
	}
	c.shuttingDown = true
	c.mu.Unlock()
	close(c.ShutdownChan)
}

func (c *Cell) StartSelfCheckLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		for {
			select {
			case <-ticker.C:
				c.checkAndRejoin(ctx)
			case <-ctx.Done():
				ticker.Stop()
				return
			}
		}
	}()
}

func (c *Cell) checkAndRejoin(ctx context.Context) {
	c.mu.Lock()
	if c.shuttingDown {
		c.mu.Unlock()
		return
	}
	selfInfo := c.selfInfo
	c.mu.Unlock()

	members, err := c.store.GetMembers()
	if err != nil {
		glog.Errorf("Cell(%s): Failed to get members for self check: %v", c.agentID, err)
		return
	}

	if _, ok := members[c.agentID]; !ok {
		glog.Warningf("Cell(%s): Agent not found in peer set! Proposing auto-rejoin.", c.agentID)
		err := c.ProposeMembership(ctx, c.agentID, selfInfo)
		if err != nil {
			glog.Errorf("Cell(%s): Auto-rejoin failed: %v", c.agentID, err)
		} else {
			glog.Infof("Cell(%s): Auto-rejoin proposed successfully", c.agentID)
		}
	}
}

// SetLockChecker configures an optional hook for checking locks during proposals.
func (c *Cell) SetLockChecker(checker func(ctx context.Context, key string) error) {
	c.lockChecker = checker
}

// SetSelfAddress updates the node's own address information.
func (c *Cell) SetSelfAddress(grpcAddr, httpURL string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.selfGRPCAddr = grpcAddr
	c.selfHTTPURL = httpURL
}

// SetPeers directly sets the internal list of connected peers.
func (c *Cell) SetPeers(peers []PeerClient) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.peers = make(map[string]PeerClient)
	for _, p := range peers {
		c.peers[p.AgentID()] = p
	}
	c.proposer = NewProposer(c.agentID, c.ident, peers, c.acceptor)
}

// Propose attempts to reach consensus on updating a key with a new value using the
// specified quorum constraint. It automatically retries on concurrent modification errors.
func (c *Cell) Propose(ctx context.Context, key string, value []byte, qt QuorumType) error {
	if err := state.ValidateKey(key); err != nil {
		return err
	}
	bo := backoff.New()
	bo.MaxElapsedTime = 2 * time.Minute

	return bo.Retry(ctx, "Propose", func() error {
		// Verify locks before proceeding
		if c.lockChecker != nil {
			if err := c.lockChecker(ctx, key); err != nil {
				return backoff.Permanent(fmt.Errorf("write refused by lock policy: %w", err))
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

// ProposeMembership initiates a proposal to add or update an agent's membership in the cluster.
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
		if existing, ok := peers[agentID]; ok && existing.Equal(info) {
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

// ProposeRemoval initiates a proposal to remove an agent from the cluster's membership.
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

// ApplyMembershipChange updates the local state to reflect a new cluster membership map.
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

	for id, info := range members {
		if id == c.agentID {
			continue
		}
		if p, ok := c.peers[id]; ok {
			newPeers[id] = p
			peerList = append(peerList, p)
		} else if c.peerFactory != nil {
			if info.GRPCAddr != "" {
				glog.Infof("Cell(%s): Creating new client for peer %s at %s", c.agentID, id, info.GRPCAddr)
				p, err := c.peerFactory(id, info.GRPCAddr)
				if err != nil {
					glog.Errorf("Cell(%s): Failed to create peer client for %s: %v", c.agentID, id, err)
					continue
				}
				newPeers[id] = p
				peerList = append(peerList, p)
			}
		}
	}

	// Close old peers that are no longer active
	for id, p := range c.peers {
		if _, ok := newPeers[id]; !ok {
			glog.Infof("Cell(%s): Closing connection to removed peer %s", c.agentID, id)
			if err := p.Close(); err != nil {
				glog.Errorf("Cell(%s): Error closing connection to peer %s: %v", c.agentID, id, err)
			}
		}
	}

	c.peers = newPeers
	c.proposer = NewProposer(c.agentID, c.ident, peerList, c.acceptor)
}

// StartSyncLoop periodically triggers synchronization of state with all active peers.
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

// StartPingLoop periodically triggers liveness checks and endpoint updates for all active peers.
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

// PingPeers performs a one-off ping to all active peers.
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
		req := &paxosv1.PingRequest{
			AgentId:  c.agentID,
			Nonce:    nonce,
			HostPort: c.selfGRPCAddr,
			HttpUrl:  c.selfHTTPURL,
		}

		if c.ident != nil {
			sig, cert, err := c.ident.SignMessage(req)
			if err == nil {
				req.Auth = &paxosv1.Authentication{
					Signature:   sig,
					Certificate: cert,
				}
			}
		}

		resp, err := p.Ping(pingCtx, req)
		cancel()
		if err != nil {
			glog.Warningf("Cell(%s): Peer %s is not responding: %v. Proposing removal.", c.agentID, p.AgentID(), err)
			if err := c.ProposeRemoval(ctx, p.AgentID()); err != nil {
				glog.Errorf("Cell(%s): Failed to propose removal of peer %s: %v", c.agentID, p.AgentID(), err)
			}
		} else {
			// Update membership with the latest address info from Ping response
			members, err := c.store.GetMembers()
			if err == nil {
				if info, ok := members[resp.AgentId]; ok {
					if info.GRPCAddr != resp.HostPort || info.HTTPURL != resp.HttpUrl {
						info.GRPCAddr = resp.HostPort
						info.HTTPURL = resp.HttpUrl
						c.ProposeMembership(context.Background(), resp.AgentId, info)
					}
				}
			}
		}
	}
}

// SyncWithPeers exchanges version information with peers and downloads updated data if
// a peer has a more recent version of any key.
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
		req := &paxosv1.SyncRequest{
			AgentId:  c.agentID,
			HostPort: c.selfGRPCAddr,
			HttpUrl:  c.selfHTTPURL,
		}

		if c.ident != nil {
			sig, cert, err := c.ident.SignMessage(req)
			if err == nil {
				req.Auth = &paxosv1.Authentication{
					Signature:   sig,
					Certificate: cert,
				}
			}
		}

		resp, err := p.Sync(ctx, req)
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

// CatchUp fetches the latest value of a specific key from a peer and applies it locally.
func (c *Cell) CatchUp(ctx context.Context, p PeerClient, key string) {
	req := &paxosv1.GetKVEntryRequest{Key: key}

	if c.ident != nil {
		sig, cert, err := c.ident.SignMessage(req)
		if err == nil {
			req.Auth = &paxosv1.Authentication{
				Signature:   sig,
				Certificate: cert,
			}
		}
	}

	resp, err := p.GetKVEntry(ctx, req)
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

// GetSyncState retrieves a map of all keys and their highest known consensus version numbers.
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

// StartEndpointSyncLoop periodically requests updated endpoints for missing peers from active peers.
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

	for id, info := range members {
		if id == c.agentID {
			continue
		}
		if info.GRPCAddr == "" {
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

		req := &paxosv1.GetPeerEndpointsRequest{
			AgentId: c.agentID,
		}

		if c.ident != nil {
			sig, cert, err := c.ident.SignMessage(req)
			if err == nil {
				req.Auth = &paxosv1.Authentication{
					Signature:   sig,
					Certificate: cert,
				}
			}
		}

		resp, err := targetPeer.GetPeerEndpoints(reqCtx, req)

		if err != nil {
			return fmt.Errorf("failed to get endpoints from %s: %w", targetPeer.AgentID(), err)
		}

		foundAny := false
		for missingID, info := range resp.Endpoints {
			if info.HostPort != "" {
				// Discovered via endpoint sync, propose membership update
				if existing, ok := members[missingID]; ok {
					existing.GRPCAddr = info.HostPort
					existing.HTTPURL = info.HttpUrl
					c.ProposeMembership(context.Background(), missingID, existing)
				}
				glog.Infof("Cell(%s): EndpointSync: Discovered endpoints for %s via %s: %s", c.agentID, missingID, targetPeer.AgentID(), info.HostPort)
				foundAny = true

				c.mu.Lock()
				delete(c.missedEndpointCount, missingID)
				c.mu.Unlock()
			}
		}

		// Track misses for unresolved peers
		c.mu.Lock()
		for _, mID := range missing {
			if info, ok := members[mID]; ok && info.GRPCAddr == "" {
				c.missedEndpointCount[mID]++
				glog.Warningf("Cell(%s): EndpointSync: Missing endpoint for %s (miss count: %d)", c.agentID, mID, c.missedEndpointCount[mID])
				if c.missedEndpointCount[mID] >= 5 {
					glog.Errorf("Cell(%s): EndpointSync: Peer %s missed 5 times, proposing removal", c.agentID, mID)
					// We launch this in a goroutine so it doesn't block the sync loop or fail the current bo.Retry explicitly
					go func(removeID string) {
						removeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
						defer cancel()
						if err := c.ProposeRemoval(removeCtx, removeID); err != nil {
							glog.Errorf("Cell(%s): Failed to propose removal for dead peer %s: %v", c.agentID, removeID, err)
						}
					}(mID)
					// Reset count to prevent spamming proposals
					c.missedEndpointCount[mID] = 0
				}
			}
		}
		c.mu.Unlock()

		if !foundAny {
			return fmt.Errorf("no missing endpoints were resolved by %s", targetPeer.AgentID())
		}
		return nil
	})
}
