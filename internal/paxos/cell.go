package paxos

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/filmil/synod/internal/state"
	paxosv1 "github.com/filmil/synod/proto/paxos/v1"
	"github.com/golang/glog"
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
	AgentID() string
}

type PeerFactory func(agentID, address string) (PeerClient, error)

type Cell struct {
	agentID  string
	store    *state.Store
	acceptor *Acceptor
	proposer *Proposer

	mu          sync.Mutex
	peers       map[string]PeerClient
	peerFactory PeerFactory
}

func NewCell(agentID string, store *state.Store, acceptor *Acceptor, factory PeerFactory) *Cell {
	c := &Cell{
		agentID:     agentID,
		store:       store,
		acceptor:    acceptor,
		peers:       make(map[string]PeerClient),
		peerFactory: factory,
	}
	c.proposer = NewProposer(agentID, nil, acceptor)
	return c
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

func (c *Cell) Propose(ctx context.Context, key string, value []byte) error {
	for {
		_, _, version, err := c.store.GetKVEntry(key)
		if err != nil {
			return fmt.Errorf("failed to get KV entry for %s: %w", key, err)
		}
		nextVersion := version + 1
		instanceKey := fmt.Sprintf("%s@%d", key, nextVersion)

		glog.Infof("Cell(%s): Proposing data for %s", c.agentID, instanceKey)

		chosenValue, err := c.proposer.Propose(ctx, instanceKey, value)
		if err != nil {
			return err
		}

		if err := c.store.CommitKV(key, chosenValue, "data", nextVersion); err != nil {
			return fmt.Errorf("failed to commit kv: %w", err)
		}

		if bytes.Equal(chosenValue, value) {
			return nil
		}
		glog.Infof("Cell(%s): Value at %s was overwritten by someone else, retrying...", c.agentID, instanceKey)
	}
}

func (c *Cell) ProposeMembership(ctx context.Context, agentID, address string) error {
	key := "/_internal/peers"
	for {
		val, _, version, err := c.store.GetKVEntry(key)
		if err != nil {
			return fmt.Errorf("failed to get KV entry for peers: %w", err)
		}
		var peers map[string]string
		if val != nil {
			if err := json.Unmarshal(val, &peers); err != nil {
				glog.Errorf("Cell(%s): Failed to unmarshal peers map: %v", c.agentID, err)
				peers = make(map[string]string)
			}
		} else {
			peers = make(map[string]string)
		}

		// Check if already correct
		if peers[agentID] == address {
			return nil
		}

		peers[agentID] = address
		newVal, err := json.Marshal(peers)
		if err != nil {
			return fmt.Errorf("failed to marshal membership: %w", err)
		}

		nextVersion := version + 1
		instanceKey := fmt.Sprintf("%s@%d", key, nextVersion)
		
		glog.Infof("Cell(%s): Proposing membership for %s", c.agentID, instanceKey)

		chosenValue, err := c.proposer.Propose(ctx, instanceKey, newVal)
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
		glog.Infof("Cell(%s): Membership at %s was updated concurrently, retrying...", c.agentID, instanceKey)
	}
}

func (c *Cell) ApplyMembershipChange(value []byte) {
	var peers map[string]string
	if err := json.Unmarshal(value, &peers); err != nil {
		glog.Errorf("Cell(%s): Failed to unmarshal membership map: %v", c.agentID, err)
		return
	}

	for id, addr := range peers {
		c.store.AddMember(id, addr)
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

	for id, addr := range members {
		if id == c.agentID {
			continue
		}
		if p, ok := c.peers[id]; ok {
			newPeers[id] = p
			peerList = append(peerList, p)
		} else if c.peerFactory != nil {
			glog.Infof("Cell(%s): Creating new client for peer %s at %s", c.agentID, id, addr)
			p, err := c.peerFactory(id, addr)
			if err != nil {
				glog.Errorf("Cell(%s): Failed to create peer client for %s: %v", c.agentID, id, err)
				continue
			}
			newPeers[id] = p
			peerList = append(peerList, p)
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
			AgentId: c.agentID,
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
				_, _, newPropNum, err := c.store.GetKVEntry(key)
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
