package paxos

import (
	"context"
	"fmt"
	"sync"

	"github.com/golang/glog"
	paxosv1 "github.com/filmil/synod/proto/paxos/v1"
)

type PeerClient interface {
	Prepare(ctx context.Context, req *paxosv1.PrepareRequest) (*paxosv1.PromiseResponse, error)
	Accept(ctx context.Context, req *paxosv1.AcceptRequest) (*paxosv1.AcceptedResponse, error)
	AgentID() string
}

type Proposer struct {
	agentID string
	peers   []PeerClient
	mu      sync.Mutex
	nextNum uint64
}

func NewProposer(agentID string, peers []PeerClient) *Proposer {
	return &Proposer{
		agentID: agentID,
		peers:   peers,
		nextNum: 1,
	}
}

func (p *Proposer) Propose(ctx context.Context, index uint64, value []byte) error {
	p.mu.Lock()
	proposalID := &paxosv1.ProposalID{
		Number:  p.nextNum,
		AgentId: p.agentID,
	}
	p.nextNum++
	p.mu.Unlock()

	// Phase 1: Prepare
	promises := p.sendPrepare(ctx, index, proposalID)
	if len(promises) < p.quorum() {
		return fmt.Errorf("failed to reach quorum in Phase 1 (Prepare)")
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
	acceptedCount := p.sendAccept(ctx, index, proposalID, value)
	if acceptedCount < p.quorum() {
		return fmt.Errorf("failed to reach quorum in Phase 2 (Accept)")
	}

	return nil
}

func (p *Proposer) quorum() int {
	return (len(p.peers)+1)/2 + 1
}

func (p *Proposer) sendPrepare(ctx context.Context, index uint64, id *paxosv1.ProposalID) []*paxosv1.PromiseResponse {
	var mu sync.Mutex
	var results []*paxosv1.PromiseResponse
	var wg sync.WaitGroup

	req := &paxosv1.PrepareRequest{
		ProposalId:  id,
		LedgerIndex: index,
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
			if resp.Promised {
				mu.Lock()
				results = append(results, resp)
				mu.Unlock()
			}
		}(peer)
	}
	wg.Wait()
	return results
}

func (p *Proposer) sendAccept(ctx context.Context, index uint64, id *paxosv1.ProposalID, value []byte) int {
	var mu sync.Mutex
	count := 0
	var wg sync.WaitGroup

	req := &paxosv1.AcceptRequest{
		ProposalId:  id,
		LedgerIndex: index,
		Value:       value,
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
			if resp.Accepted {
				mu.Lock()
				count++
				mu.Unlock()
			}
		}(peer)
	}
	wg.Wait()
	return count
}
