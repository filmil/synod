package paxos

import (
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
	GetLedgerEntry(ctx context.Context, req *paxosv1.GetLedgerEntryRequest) (*paxosv1.GetLedgerEntryResponse, error)
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

func (c *Cell) Propose(ctx context.Context, value []byte) error {
	return c.proposeWithType(ctx, value, "data")
}

func (c *Cell) ProposeMembership(ctx context.Context, agentID, address string) error {
	change := MembershipChange{
		Action:  "ADD",
		AgentID: agentID,
		Address: address,
	}
	value, err := json.Marshal(change)
	if err != nil {
		return fmt.Errorf("failed to marshal membership change: %w", err)
	}
	return c.proposeWithType(ctx, value, "membership")
}

func (c *Cell) proposeWithType(ctx context.Context, value []byte, valType string) error {
	index, err := c.store.GetHighestLedgerIndex()
	if err != nil {
		return fmt.Errorf("failed to get highest ledger index: %w", err)
	}

	nextIndex := index + 1
	glog.Infof("Proposing %s for ledger index %d", valType, nextIndex)

	if err := c.proposer.Propose(ctx, nextIndex, value); err != nil {
		return err
	}

	// Once paxos consensus is reached, commit to ledger.
	if err := c.store.CommitLedger(nextIndex, value, valType); err != nil {
		return fmt.Errorf("failed to commit ledger: %w", err)
	}

	// If it was a membership change, update local view.
	if valType == "membership" {
		c.ApplyMembershipChange(value)
	}

	return nil
}

func (c *Cell) ApplyMembershipChange(value []byte) {
	var change MembershipChange
	if err := json.Unmarshal(value, &change); err != nil {
		glog.Errorf("Cell(%s): Failed to unmarshal membership change: %v", c.agentID, err)
		return
	}

	if change.Action == "ADD" {
		glog.Infof("Cell(%s): Applying membership change: add %s at %s", c.agentID, change.AgentID, change.Address)
		c.store.AddMember(change.AgentID, change.Address)
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

	localIndex, _ := c.store.GetHighestLedgerIndex()

	for _, p := range peers {
		resp, err := p.Sync(ctx, &paxosv1.SyncRequest{
			AgentId:            c.agentID,
			HighestLedgerIndex: localIndex,
		})
		if err != nil {
			continue
		}

		if resp.HighestLedgerIndex > localIndex {
			glog.Infof("Cell(%s): Peer %s has higher ledger index %d (local %d), catching up...", 
				c.agentID, p.AgentID(), resp.HighestLedgerIndex, localIndex)
			c.CatchUp(ctx, p, localIndex+1, resp.HighestLedgerIndex)
			// Update local index after catch up
			localIndex, _ = c.store.GetHighestLedgerIndex()
		}
	}
}

func (c *Cell) CatchUp(ctx context.Context, p PeerClient, from, to uint64) {
	for i := from; i <= to; i++ {
		resp, err := p.GetLedgerEntry(ctx, &paxosv1.GetLedgerEntryRequest{LedgerIndex: i})
		if err != nil {
			glog.Errorf("Cell(%s): Failed to get ledger entry %d from %s: %v", c.agentID, i, p.AgentID(), err)
			break
		}
		if resp.Value == nil {
			continue
		}
		glog.Infof("Cell(%s): Catching up entry %d type %s", c.agentID, i, resp.Type)
		if err := c.store.CommitLedger(i, resp.Value, resp.Type); err != nil {
			glog.Errorf("Cell(%s): Failed to commit caught up ledger entry %d: %v", c.agentID, i, err)
			break
		}
		if resp.Type == "membership" {
			c.ApplyMembershipChange(resp.Value)
		}
	}
}

func (c *Cell) GetSyncState() (uint64, []string) {
	index, _ := c.store.GetHighestLedgerIndex()
	members, _ := c.store.GetMembers()
	var ids []string
	for id := range members {
		ids = append(ids, id)
	}
	return index, ids
}
