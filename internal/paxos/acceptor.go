// SPDX-License-Identifier: Apache-2.0

package paxos

import (
	"context"
	"fmt"
	"sync"

	"github.com/filmil/synod/internal/identity"
	"github.com/filmil/synod/internal/state"
	paxosv1 "github.com/filmil/synod/proto/paxos/v1"
	"github.com/golang/glog"
	"google.golang.org/protobuf/encoding/prototext"
)

// Acceptor handles the receiving end of the Paxos protocol, promising and accepting proposals.
type Acceptor struct {
	agentID   string
	ident     *identity.Identity
	store     *state.Store
	validator func(ctx context.Context, req *paxosv1.AcceptRequest) error
	mu        sync.Mutex
}

// NewAcceptor creates and returns a new Acceptor.
// agentID is the ID of the acceptor, and store is the database to use.
func NewAcceptor(agentID string, ident *identity.Identity, store *state.Store) *Acceptor {
	return &Acceptor{
		agentID: agentID,
		ident:   ident,
		store:   store,
	}
}

// SetValidator configures a validation hook for AcceptRequests.
func (a *Acceptor) SetValidator(v func(ctx context.Context, req *paxosv1.AcceptRequest) error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.validator = v
}

// Prepare processes a PrepareRequest (Phase 1a) and determines whether to issue a promise (Phase 1b).
func (a *Acceptor) Prepare(ctx context.Context, req *paxosv1.PrepareRequest) (*paxosv1.PromiseResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	glog.Infof("Acceptor(%s): Prepare request from %s for key %s, proposal %v, nonce %s",
		a.agentID, req.AgentId, req.Key, req.ProposalId, req.Nonce)

	promisedID, acceptedID, acceptedValue, err := a.store.GetAcceptorState(req.Key)
	if err != nil {
		return nil, fmt.Errorf("failed to get acceptor state: %w", err)
	}

	var resp *paxosv1.PromiseResponse
	if isGreater(req.ProposalId, promisedID) {
		glog.Infof("Acceptor(%s): Promised key %s to proposal %v", a.agentID, req.Key, req.ProposalId)
		if err := a.store.SetPromisedID(req.Key, req.ProposalId); err != nil {
			return nil, fmt.Errorf("failed to set promised ID: %w", err)
		}
		resp = &paxosv1.PromiseResponse{
			AgentId:              a.agentID,
			Promised:             true,
			HighestAcceptedId:    acceptedID,
			HighestAcceptedValue: acceptedValue,
			HighestPromisedId:    req.ProposalId,
		}
	} else {
		glog.Warningf("Acceptor(%s): Rejected Prepare for key %s: proposal %v <= promised %v",
			a.agentID, req.Key, req.ProposalId, promisedID)
		resp = &paxosv1.PromiseResponse{
			AgentId:           a.agentID,
			Promised:          false,
			HighestPromisedId: promisedID,
		}
	}

	if a.ident != nil {
		sig, cert, err := a.ident.SignMessage(resp)
		if err == nil {
			resp.Auth = &paxosv1.Authentication{
				Signature:   sig,
				Certificate: cert,
			}
		}
	}

	// Log the message
	opts := prototext.MarshalOptions{Multiline: true}
	reqBytes := []byte(opts.Format(req))
	respBytes := []byte(opts.Format(resp))
	if err := a.store.LogMessage("Prepare", req.AgentId, a.agentID, reqBytes, respBytes); err != nil {
		glog.Errorf("Acceptor(%s): Failed to log Prepare message: %v", a.agentID, err)
	}
	return resp, nil
}

// Accept processes an AcceptRequest (Phase 2a) and determines whether to accept the proposed value (Phase 2b).
func (a *Acceptor) Accept(ctx context.Context, req *paxosv1.AcceptRequest) (*paxosv1.AcceptedResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	glog.Infof("Acceptor(%s): Accept request from %s for key %s, proposal %v, nonce %s",
		a.agentID, req.AgentId, req.Key, req.ProposalId, req.Nonce)

	promisedID, _, _, err := a.store.GetAcceptorState(req.Key)
	if err != nil {
		return nil, fmt.Errorf("failed to get acceptor state: %w", err)
	}

	var resp *paxosv1.AcceptedResponse
	
	// Execute external validation hook if present
	if a.validator != nil {
		if err := a.validator(ctx, req); err != nil {
			glog.Warningf("Acceptor(%s): Validator rejected Accept for key %s: %v", a.agentID, req.Key, err)
			resp = &paxosv1.AcceptedResponse{
				AgentId:           a.agentID,
				Accepted:          false,
				HighestPromisedId: promisedID,
			}
			
			// Log the rejection
			opts := prototext.MarshalOptions{Multiline: true}
			reqBytes := []byte(opts.Format(req))
			respBytes := []byte(opts.Format(resp))
			if logErr := a.store.LogMessage("Accept", req.AgentId, a.agentID, reqBytes, respBytes); logErr != nil {
				glog.Errorf("Acceptor(%s): Failed to log Accept message: %v", a.agentID, logErr)
			}
			return resp, nil
		}
	}

	// Accept if proposal ID is >= promised ID
	if isGreaterEqual(req.ProposalId, promisedID) {
		glog.Infof("Acceptor(%s): Accepted proposal %v for key %s", a.agentID, req.ProposalId, req.Key)
		if err := a.store.SetAcceptedValue(req.Key, req.ProposalId, req.Value); err != nil {
			return nil, fmt.Errorf("failed to set accepted value: %w", err)
		}
		resp = &paxosv1.AcceptedResponse{
			AgentId:           a.agentID,
			Accepted:          true,
			HighestPromisedId: req.ProposalId,
		}
	} else {
		glog.Warningf("Acceptor(%s): Rejected Accept for key %s: proposal %v < promised %v",
			a.agentID, req.Key, req.ProposalId, promisedID)
		resp = &paxosv1.AcceptedResponse{
			AgentId:           a.agentID,
			Accepted:          false,
			HighestPromisedId: promisedID,
		}
	}

	if a.ident != nil {
		sig, cert, err := a.ident.SignMessage(resp)
		if err == nil {
			resp.Auth = &paxosv1.Authentication{
				Signature:   sig,
				Certificate: cert,
			}
		}
	}

	// Log the message
	opts := prototext.MarshalOptions{Multiline: true}
	reqBytes := []byte(opts.Format(req))
	respBytes := []byte(opts.Format(resp))
	if err := a.store.LogMessage("Accept", req.AgentId, a.agentID, reqBytes, respBytes); err != nil {
		glog.Errorf("Acceptor(%s): Failed to log Accept message: %v", a.agentID, err)
	}
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
