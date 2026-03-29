package paxos

import (
	"context"
	"fmt"
	"sync"

	paxosv1 "github.com/filmil/synod/proto/paxos/v1"
	"github.com/golang/glog"
	"github.com/google/uuid"
)

type Proposer struct {
	agentID  string
	peers    []PeerClient
	acceptor *Acceptor
	mu       sync.Mutex
	nextNum  uint64
}

func NewProposer(agentID string, peers []PeerClient, acceptor *Acceptor) *Proposer {
	return &Proposer{
		agentID:  agentID,
		peers:    peers,
		acceptor: acceptor,
		nextNum:  1,
	}
}

type QuorumType int

const (
	QuorumMajority QuorumType = iota
	QuorumAll
)

func (p *Proposer) Propose(ctx context.Context, key string, value []byte, qt QuorumType) ([]byte, error) {
	p.mu.Lock()
	proposalID := &paxosv1.ProposalID{
		Number:  p.nextNum,
		AgentId: p.agentID,
	}
	p.nextNum++
	p.mu.Unlock()

	nonce := uuid.New().String()
	glog.Infof("Proposer(%s): Starting proposal for key %s, proposal %v, nonce %s, quorum %v",
		p.agentID, key, proposalID, nonce, qt)

	required := p.required(qt)

	// Phase 1: Prepare
	promises := p.sendPrepare(ctx, key, proposalID, nonce)
	if len(promises) < required {
		glog.Warningf("Proposer(%s): Phase 1 failed for key %s: no quorum (got %d, need %d)", p.agentID, key, len(promises), required)
		return nil, fmt.Errorf("failed to reach quorum in Phase 1 (Prepare): got %d, need %d", len(promises), required)
	}

	// Pick the value from the highest accepted proposal
	var highestID *paxosv1.ProposalID
	for _, promise := range promises {
		if promise.HighestAcceptedId != nil {
			if isGreater(promise.HighestAcceptedId, highestID) {
				highestID = promise.HighestAcceptedId
				value = promise.HighestAcceptedValue
			}
		}
	}

	// Phase 2: Accept
	acceptedCount := p.sendAccept(ctx, key, proposalID, value, nonce)
	if acceptedCount < required {
		glog.Warningf("Proposer(%s): Phase 2 failed for key %s: no quorum (got %d, need %d)", p.agentID, key, acceptedCount, required)
		return nil, fmt.Errorf("failed to reach quorum in Phase 2 (Accept): got %d, need %d", acceptedCount, required)
	}

	glog.Infof("Proposer(%s): Consensus reached for key %s", p.agentID, key)
	return value, nil
}

func (p *Proposer) required(qt QuorumType) int {
	total := len(p.peers) + 1
	if qt == QuorumAll {
		return total
	}
	return total/2 + 1
}

func (p *Proposer) quorum() int {
	return p.required(QuorumMajority)
}

func (p *Proposer) sendPrepare(ctx context.Context, key string, id *paxosv1.ProposalID, nonce string) []*paxosv1.PromiseResponse {
	var mu sync.Mutex
	var results []*paxosv1.PromiseResponse
	var wg sync.WaitGroup

	req := &paxosv1.PrepareRequest{
		AgentId:    p.agentID,
		ProposalId: id,
		Key:        key,
		Nonce:      nonce,
	}

	// Prepare self
	resp, err := p.acceptor.Prepare(ctx, req)
	if err == nil && resp != nil && resp.Promised {
		results = append(results, resp)
	}

	for _, peer := range p.peers {
		wg.Add(1)
		go func(pc PeerClient) {
			defer wg.Add(-1)
			resp, err := pc.Prepare(ctx, req)
			if err != nil {
				glog.V(2).Infof("Prepare failed for peer %s: %v", pc.AgentID(), err)
				return
			}
			if resp != nil && resp.Promised {
				mu.Lock()
				results = append(results, resp)
				mu.Unlock()
			}
		}(peer)
	}
	wg.Wait()
	return results
}

func (p *Proposer) sendAccept(ctx context.Context, key string, id *paxosv1.ProposalID, value []byte, nonce string) int {
	var mu sync.Mutex
	count := 0
	var wg sync.WaitGroup

	req := &paxosv1.AcceptRequest{
		AgentId:    p.agentID,
		ProposalId: id,
		Key:        key,
		Value:      value,
		Nonce:      nonce,
	}

	// Accept self
	resp, err := p.acceptor.Accept(ctx, req)
	if err == nil && resp != nil && resp.Accepted {
		count++
	}

	for _, peer := range p.peers {
		wg.Add(1)
		go func(pc PeerClient) {
			defer wg.Add(-1)
			resp, err := pc.Accept(ctx, req)
			if err != nil {
				glog.V(2).Infof("Accept failed for peer %s: %v", pc.AgentID(), err)
				return
			}
			if resp != nil && resp.Accepted {
				mu.Lock()
				count++
				mu.Unlock()
			}
		}(peer)
	}
	wg.Wait()
	return count
}
