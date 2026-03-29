package userapi

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/filmil/synod/internal/backoff"
	"github.com/filmil/synod/internal/paxos"
	paxosv1 "github.com/filmil/synod/proto/paxos/v1"
	"github.com/golang/glog"
)

// UserAPI provides client-facing operations for interacting with the Key-Value store.
type UserAPI struct {
	cell *paxos.Cell
}

// New creates a new UserAPI instance backed by the given Paxos cell.
func New(cell *paxos.Cell) *UserAPI {
	return &UserAPI{cell: cell}
}

// Read implements the quorum read logic.
func (a *UserAPI) Read(ctx context.Context, key string, quorum paxosv1.ReadQuorum) (*paxosv1.ReadResponse, error) {
	// First sync to ensure we are as fresh as possible before evaluating quorum locally
	a.cell.SyncWithPeers(ctx)

	val, valType, version, _, err := a.cell.GetStore().GetKVEntry(key)
	if err != nil {
		return nil, fmt.Errorf("failed to read key %s: %w", key, err)
	}

	if quorum == paxosv1.ReadQuorum_READ_QUORUM_LOCAL || quorum == paxosv1.ReadQuorum_READ_QUORUM_UNSPECIFIED {
		return &paxosv1.ReadResponse{
			Key:     key,
			Value:   val,
			Type:    valType,
			Version: version,
		}, nil
	}

	peers := a.cell.GetActivePeers()
	totalNodes := len(peers) + 1 // including self

	matchCount := 1 // self matches
	for _, p := range peers {
		resp, err := p.GetKVEntry(ctx, &paxosv1.GetKVEntryRequest{Key: key})
		if err != nil {
			glog.Warningf("UserAPI: failed to read key %s from peer %s: %v", key, p.AgentID(), err)
			continue
		}
		if bytes.Equal(resp.Value, val) && resp.Version == version {
			matchCount++
		}
	}

	glog.V(2).Infof("UserAPI: Read %s: matches %d / %d", key, matchCount, totalNodes)

	if quorum == paxosv1.ReadQuorum_READ_QUORUM_MAJORITY {
		if matchCount > totalNodes/2 {
			return &paxosv1.ReadResponse{
				Key:     key,
				Value:   val,
				Type:    valType,
				Version: version,
			}, nil
		}
		return nil, fmt.Errorf("failed to achieve majority quorum for read: %d/%d", matchCount, totalNodes)
	}

	if quorum == paxosv1.ReadQuorum_READ_QUORUM_ALL {
		if matchCount == totalNodes {
			return &paxosv1.ReadResponse{
				Key:     key,
				Value:   val,
				Type:    valType,
				Version: version,
			}, nil
		}
		return nil, fmt.Errorf("failed to achieve all peers quorum for read: %d/%d", matchCount, totalNodes)
	}

	return nil, fmt.Errorf("unknown read quorum: %v", quorum)
}

// CompareAndWrite implements CAS using paxos proposal and backoff.
func (a *UserAPI) CompareAndWrite(ctx context.Context, key string, oldValue, newValue []byte, qt paxos.QuorumType) (*paxosv1.CompareAndWriteResponse, error) {
	bo := backoff.New()

	var success bool
	var version uint64

	// Sync once at the start
	a.cell.SyncWithPeers(ctx)
	err := bo.Retry(ctx, "CompareAndWrite", func() error {
		// Verify locks before attempting write
		if err := a.CheckLocks(ctx, key); err != nil {
			return backoff.Permanent(fmt.Errorf("lock check failed: %w", err))
		}

		// Read current value
		currentVal, _, v, _, err := a.cell.GetStore().GetKVEntry(key)
		if err != nil {
			return fmt.Errorf("failed to read key %s: %w", key, err)
		}

		if !bytes.Equal(currentVal, oldValue) {
			return backoff.Permanent(fmt.Errorf("CompareAndWrite failed: old value mismatch"))
		}

		// Issue proposal
		err = a.cell.Propose(ctx, key, newValue, qt)
		if err != nil {
			return err
		}

		success = true
		version = v + 1
		return nil
	})

	if err != nil {
		return &paxosv1.CompareAndWriteResponse{
			Success: false,
			Message: err.Error(),
		}, nil
	}

	return &paxosv1.CompareAndWriteResponse{
		Success: success,
		Message: "Write successful",
		Version: version,
	}, nil
}

// Shutdown initiates a graceful shutdown sequence.
// It proposes the removal of this node from the cluster and signals the main server to enter lame duck mode.
func (a *UserAPI) Shutdown(ctx context.Context, req *paxosv1.ShutdownRequest) (*paxosv1.ShutdownResponse, error) {
	agentID, _, err := a.cell.GetStore().GetAgentID()
	if err != nil {
		return &paxosv1.ShutdownResponse{Success: false, Message: fmt.Sprintf("failed to get agent ID: %v", err)}, nil
	}

	glog.Infof("UserAPI: Shutdown requested. Proposing removal of agent %s", agentID)

	// Attempt to propose our own removal. If it fails, we still proceed with shutdown.
	if err := a.cell.ProposeRemoval(ctx, agentID); err != nil {
		glog.Warningf("UserAPI: Failed to propose removal during shutdown: %v", err)
	}

	// Trigger the actual process shutdown asynchronously to allow this RPC to return successfully.
	go func() {
		// Small delay to allow the gRPC response to be written to the wire
		time.Sleep(500 * time.Millisecond)
		a.cell.TriggerShutdown()
	}()

	return &paxosv1.ShutdownResponse{Success: true, Message: "Shutdown initiated. Entering lame duck mode."}, nil
}

