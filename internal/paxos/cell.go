package paxos

import (
	"context"
	"fmt"
	"sync"

	"github.com/filmil/synod/internal/state"
	"github.com/golang/glog"
)

type Cell struct {
	agentID  string
	store    *state.Store
	acceptor *Acceptor
	proposer *Proposer

	mu    sync.Mutex
	peers map[string]PeerClient
}

func NewCell(agentID string, store *state.Store, acceptor *Acceptor) *Cell {
	c := &Cell{
		agentID:  agentID,
		store:    store,
		acceptor: acceptor,
		peers:    make(map[string]PeerClient),
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
	index, err := c.store.GetHighestLedgerIndex()
	if err != nil {
		return fmt.Errorf("failed to get highest ledger index: %w", err)
	}

	nextIndex := index + 1
	glog.Infof("Proposing value for ledger index %d", nextIndex)

	if err := c.proposer.Propose(ctx, nextIndex, value); err != nil {
		return err
	}

	// Once paxos consensus is reached, commit to ledger.
	// In a real implementation, a Learner role would handle this.
	if err := c.store.CommitLedger(nextIndex, value, "data"); err != nil {
		return fmt.Errorf("failed to commit ledger: %w", err)
	}

	return nil
}
