package paxos

import (
	"context"
	"fmt"
	"sync"

	"github.com/golang/glog"
	"github.com/filmil/synod/internal/state"
	paxosv1 "github.com/filmil/synod/proto/paxos/v1"
)

type Acceptor struct {
	agentID string
	store   *state.Store
	mu      sync.Mutex
}

func NewAcceptor(agentID string, store *state.Store) *Acceptor {
	return &Acceptor{
		agentID: agentID,
		store:   store,
	}
}

func (a *Acceptor) Prepare(ctx context.Context, req *paxosv1.PrepareRequest) (*paxosv1.PromiseResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	glog.Infof("Acceptor(%s): Prepare request from %s for index %d, proposal %v, nonce %s", 
		a.agentID, req.AgentId, req.LedgerIndex, req.ProposalId, req.Nonce)

	promisedID, acceptedID, acceptedValue, err := a.store.GetAcceptorState(req.LedgerIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to get acceptor state: %w", err)
	}

	var resp *paxosv1.PromiseResponse
	if isGreater(req.ProposalId, promisedID) {
		glog.Infof("Acceptor(%s): Promised index %d to proposal %v", a.agentID, req.LedgerIndex, req.ProposalId)
		if err := a.store.SetPromisedID(req.LedgerIndex, req.ProposalId); err != nil {
			return nil, fmt.Errorf("failed to set promised ID: %w", err)
		}
		resp = &paxosv1.PromiseResponse{
			AgentId:             a.agentID,
			Promised:            true,
			HighestAcceptedId:   acceptedID,
			HighestAcceptedValue: acceptedValue,
		}
	} else {
		glog.Warningf("Acceptor(%s): Rejected Prepare for index %d: proposal %v <= promised %v", 
			a.agentID, req.LedgerIndex, req.ProposalId, promisedID)
		resp = &paxosv1.PromiseResponse{
			AgentId:  a.agentID,
			Promised: false,
		}
	}

	// Log the message
	a.store.LogMessage("Prepare", req.AgentId, a.agentID, []byte(req.String()), []byte(resp.String()))
	return resp, nil
}

func (a *Acceptor) Accept(ctx context.Context, req *paxosv1.AcceptRequest) (*paxosv1.AcceptedResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	glog.Infof("Acceptor(%s): Accept request from %s for index %d, proposal %v, nonce %s", 
		a.agentID, req.AgentId, req.LedgerIndex, req.ProposalId, req.Nonce)

	promisedID, _, _, err := a.store.GetAcceptorState(req.LedgerIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to get acceptor state: %w", err)
	}

	var resp *paxosv1.AcceptedResponse
	// Accept if proposal ID is >= promised ID
	if isGreaterEqual(req.ProposalId, promisedID) {
		glog.Infof("Acceptor(%s): Accepted proposal %v for index %d", a.agentID, req.ProposalId, req.LedgerIndex)
		if err := a.store.SetAcceptedValue(req.LedgerIndex, req.ProposalId, req.Value); err != nil {
			return nil, fmt.Errorf("failed to set accepted value: %w", err)
		}
		resp = &paxosv1.AcceptedResponse{
			AgentId:  a.agentID,
			Accepted: true,
		}
	} else {
		glog.Warningf("Acceptor(%s): Rejected Accept for index %d: proposal %v < promised %v", 
			a.agentID, req.LedgerIndex, req.ProposalId, promisedID)
		resp = &paxosv1.AcceptedResponse{
			AgentId:  a.agentID,
			Accepted: false,
		}
	}

	// Log the message
	a.store.LogMessage("Accept", req.AgentId, a.agentID, []byte(req.String()), []byte(resp.String()))
	return resp, nil
}

func isGreater(a, b *paxosv1.ProposalID) bool {
	if b == nil {
		return true
	}
	if a.Number > b.Number {
		return true
	}
	if a.Number < b.Number {
		return false
	}
	return a.AgentId > b.AgentId
}

func isGreaterEqual(a, b *paxosv1.ProposalID) bool {
	if b == nil {
		return true
	}
	if a.Number > b.Number {
		return true
	}
	if a.Number < b.Number {
		return false
	}
	return a.AgentId >= b.AgentId
}
