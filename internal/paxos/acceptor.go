package paxos

import (
	"context"
	"fmt"
	"sync"

	"github.com/filmil/synod/internal/state"
	paxosv1 "github.com/filmil/synod/proto/paxos/v1"
)

type Acceptor struct {
	store *state.Store
	mu    sync.Mutex
}

func NewAcceptor(store *state.Store) *Acceptor {
	return &Acceptor{store: store}
}

func (a *Acceptor) Prepare(ctx context.Context, req *paxosv1.PrepareRequest) (*paxosv1.PromiseResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	promisedID, acceptedID, acceptedValue, err := a.store.GetAcceptorState(req.LedgerIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to get acceptor state: %w", err)
	}

	if isGreater(req.ProposalId, promisedID) {
		if err := a.store.SetPromisedID(req.LedgerIndex, req.ProposalId); err != nil {
			return nil, fmt.Errorf("failed to set promised ID: %w", err)
		}
		return &paxosv1.PromiseResponse{
			Promised:            true,
			HighestAcceptedId:   acceptedID,
			HighestAcceptedValue: acceptedValue,
		}, nil
	}

	return &paxosv1.PromiseResponse{Promised: false}, nil
}

func (a *Acceptor) Accept(ctx context.Context, req *paxosv1.AcceptRequest) (*paxosv1.AcceptedResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	promisedID, _, _, err := a.store.GetAcceptorState(req.LedgerIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to get acceptor state: %w", err)
	}

	// Accept if proposal ID is >= promised ID
	if isGreaterEqual(req.ProposalId, promisedID) {
		if err := a.store.SetAcceptedValue(req.LedgerIndex, req.ProposalId, req.Value); err != nil {
			return nil, fmt.Errorf("failed to set accepted value: %w", err)
		}
		return &paxosv1.AcceptedResponse{Accepted: true}, nil
	}

	return &paxosv1.AcceptedResponse{Accepted: false}, nil
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
