// SPDX-License-Identifier: Apache-2.0

package paxos

import (
	"context"
	"fmt"
	"sync"

	"github.com/filmil/synod/internal/identity"
	paxosv1 "github.com/filmil/synod/proto/paxos/v1"
	"github.com/golang/glog"
	"github.com/google/uuid"
)

// Proposer coordinates the proposal process for the Paxos protocol across the cluster.
type Proposer struct {
	agentID  string
	ident    *identity.Identity
	peers    []PeerClient
	acceptor *Acceptor
	mu       sync.Mutex
	nextNum  uint64
}

// NewProposer constructs a new Proposer.
func NewProposer(agentID string, ident *identity.Identity, peers []PeerClient, acceptor *Acceptor) *Proposer {
	return &Proposer{
		agentID:  agentID,
		ident:    ident,
		peers:    peers,
		acceptor: acceptor,
		nextNum:  1,
	}
}

// QuorumType indicates the required level of agreement for a successful proposal.
type QuorumType int

const (
	// QuorumMajority requires consensus from >50% of nodes.
	QuorumMajority QuorumType = iota
	// QuorumAll requires consensus from 100% of nodes.
	QuorumAll
)

// Propose executes the full Paxos protocol (Phase 1 and Phase 2) to establish consensus on a value for a key.
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
	promises, rejections := p.sendPrepare(ctx, key, proposalID, nonce)

	// Fast-forward our proposal number to be strictly greater than any promised number we saw
	var highestPromised uint64
	for _, resp := range promises {
		if resp.HighestPromisedId != nil && resp.HighestPromisedId.Number > highestPromised {
			highestPromised = resp.HighestPromisedId.Number
		}
	}
	for _, resp := range rejections {
		if resp.HighestPromisedId != nil && resp.HighestPromisedId.Number > highestPromised {
			highestPromised = resp.HighestPromisedId.Number
		}
	}

	if highestPromised >= p.nextNum {
		p.mu.Lock()
		if highestPromised >= p.nextNum {
			p.nextNum = highestPromised + 1
			glog.Infof("Proposer(%s): Fast-forwarded next proposal number to %d based on observed promises", p.agentID, p.nextNum)
		}
		p.mu.Unlock()
	}

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
	accepts, acceptRejections := p.sendAccept(ctx, key, proposalID, value, nonce)

	highestPromised = 0
	for _, resp := range accepts {
		if resp.HighestPromisedId != nil && resp.HighestPromisedId.Number > highestPromised {
			highestPromised = resp.HighestPromisedId.Number
		}
	}
	for _, resp := range acceptRejections {
		if resp.HighestPromisedId != nil && resp.HighestPromisedId.Number > highestPromised {
			highestPromised = resp.HighestPromisedId.Number
		}
	}

	if highestPromised >= p.nextNum {
		p.mu.Lock()
		if highestPromised >= p.nextNum {
			p.nextNum = highestPromised + 1
			glog.Infof("Proposer(%s): Fast-forwarded next proposal number to %d based on observed accept rejections", p.agentID, p.nextNum)
		}
		p.mu.Unlock()
	}

	if len(accepts) < required {
		glog.Warningf("Proposer(%s): Phase 2 failed for key %s: no quorum (got %d, need %d)", p.agentID, key, len(accepts), required)
		return nil, fmt.Errorf("failed to reach quorum in Phase 2 (Accept): got %d, need %d", len(accepts), required)
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

func (p *Proposer) sendPrepare(ctx context.Context, key string, id *paxosv1.ProposalID, nonce string) ([]*paxosv1.PromiseResponse, []*paxosv1.PromiseResponse) {
	var mu sync.Mutex
	var promises []*paxosv1.PromiseResponse
	var rejections []*paxosv1.PromiseResponse
	var wg sync.WaitGroup

	req := &paxosv1.PrepareRequest{
		AgentId:    p.agentID,
		ProposalId: id,
		Key:        key,
		Nonce:      nonce,
	}

	p.ident.Authenticate(req)

	// Prepare self
	resp, err := p.acceptor.Prepare(ctx, req)
	if err == nil && resp != nil {
		if resp.Promised {
			promises = append(promises, resp)
		} else {
			rejections = append(rejections, resp)
		}
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
			if resp != nil {
				mu.Lock()
				if resp.Promised {
					promises = append(promises, resp)
				} else {
					rejections = append(rejections, resp)
				}
				mu.Unlock()
			}
		}(peer)
	}
	wg.Wait()
	return promises, rejections
}

func (p *Proposer) sendAccept(ctx context.Context, key string, id *paxosv1.ProposalID, value []byte, nonce string) ([]*paxosv1.AcceptedResponse, []*paxosv1.AcceptedResponse) {
	var mu sync.Mutex
	var accepts []*paxosv1.AcceptedResponse
	var rejections []*paxosv1.AcceptedResponse
	var wg sync.WaitGroup

	req := &paxosv1.AcceptRequest{
		AgentId:    p.agentID,
		ProposalId: id,
		Key:        key,
		Value:      value,
		Nonce:      nonce,
	}

	p.ident.Authenticate(req)

	// Accept self
	resp, err := p.acceptor.Accept(ctx, req)
	if err == nil && resp != nil {
		if resp.Accepted {
			accepts = append(accepts, resp)
		} else {
			rejections = append(rejections, resp)
		}
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
			if resp != nil {
				mu.Lock()
				if resp.Accepted {
					accepts = append(accepts, resp)
				} else {
					rejections = append(rejections, resp)
				}
				mu.Unlock()
			}
		}(peer)
	}
	wg.Wait()
	return accepts, rejections
}
