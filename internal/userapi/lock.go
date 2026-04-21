// SPDX-License-Identifier: Apache-2.0

package userapi

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/filmil/synod/internal/constants"
	"github.com/filmil/synod/internal/paxos"
	paxosv1 "github.com/filmil/synod/proto/paxos/v1"
	"github.com/golang/glog"
)

// LockData represents the contents of a _lock file.
type LockData struct {
	AgentID   string `json:"agent_id"`
	ExpiresAt int64  `json:"expires_at"` // Unix timestamp in nanoseconds
}

// extractLockPaths returns all `_lockable` paths that need to be acquired for the given key path.
// For example, if key_path is `/a/_lockable/b/_lockable/c`, it returns:
// [`/a/_lockable`, `/a/_lockable/b/_lockable`]
func extractLockPaths(keyPath string) []string {
	var lockPaths []string
	parts := strings.Split(keyPath, "/")

	currentPath := ""
	for i, part := range parts {
		if part == "" && i == 0 {
			continue // Handle leading slash
		}

		if currentPath == "" {
			currentPath = "/" + part
		} else {
			if currentPath == "/" {
				currentPath = currentPath + part
			} else {
				currentPath = currentPath + "/" + part
			}
		}

		if part == constants.LockableSegment {
			// Don't require a lock on the lock file itself when we are mutating it
			if keyPath != currentPath {
				lockPaths = append(lockPaths, currentPath)
			}
		}
	}
	return lockPaths
}

// AcquireLock attempts to acquire locks for all lockable segments in a key path.
func (a *UserAPI) AcquireLock(ctx context.Context, req *paxosv1.AcquireLockRequest) (*paxosv1.AcquireLockResponse, error) {
	lockPaths := extractLockPaths(req.KeyPath)
	if len(lockPaths) == 0 {
		// If the path itself ends with _lockable, we might want to lock it directly.
		if strings.HasSuffix(req.KeyPath, "/"+constants.LockableSegment) || req.KeyPath == constants.LockableSegment {
			lockPaths = []string{req.KeyPath}
		} else {
			return &paxosv1.AcquireLockResponse{
				Success: true,
				Message: "No _lockable segments found in path",
			}, nil
		}
	}

	agentID, _ := a.cell.GetStore().GetAgentID()
	duration := time.Duration(req.DurationMs) * time.Millisecond

	// Acquire locks sequentially
	for _, lockPath := range lockPaths {
		success, err := a.acquireSingleLock(ctx, lockPath, agentID, duration)
		if err != nil {
			return nil, fmt.Errorf("failed to acquire lock at %s: %w", lockPath, err)
		}
		if !success {
			return &paxosv1.AcquireLockResponse{Success: false, Message: fmt.Sprintf("lock at %s is held by another agent", lockPath)}, nil
		}
	}

	return &paxosv1.AcquireLockResponse{Success: true, Message: "Locks acquired"}, nil
}

func (a *UserAPI) acquireSingleLock(ctx context.Context, lockPath, agentID string, duration time.Duration) (bool, error) {
	// Sync state before reading

	val, _, _, _, err := a.cell.GetStore().GetKVEntry(lockPath)
	if err != nil {
		return false, fmt.Errorf("failed to read lock path %s: %w", lockPath, err)
	}

	now := time.Now().UnixNano()
	expiresAt := time.Now().Add(duration).UnixNano()

	newLock := LockData{
		AgentID:   agentID,
		ExpiresAt: expiresAt,
	}
	newVal, _ := json.Marshal(newLock)

	if val != nil && len(val) > 0 {
		var currentLock LockData
		if err := json.Unmarshal(val, &currentLock); err == nil {
			// Check if lock is active and held by someone else
			if currentLock.AgentID != agentID && currentLock.ExpiresAt > now {
				return false, nil // Lock is held and not expired
			}
		}
	}

	// Lock is either empty, invalid, expired, or held by us.
	// Attempt CompareAndWrite to acquire/renew.
	// Note: We bypass CheckLocks here because we are writing to the lock key itself.
	resp, err := a.CompareAndWrite(ctx, lockPath, val, newVal, paxos.QuorumMajority)
	if err != nil {
		return false, err
	}
	return resp.Success, nil
}

// ReleaseLock releases locks for all lockable segments in a key path that are held by this agent.
func (a *UserAPI) ReleaseLock(ctx context.Context, req *paxosv1.ReleaseLockRequest) (*paxosv1.ReleaseLockResponse, error) {
	lockPaths := extractLockPaths(req.KeyPath)
	if len(lockPaths) == 0 {
		if strings.HasSuffix(req.KeyPath, "/"+constants.LockableSegment) || req.KeyPath == constants.LockableSegment {
			lockPaths = []string{req.KeyPath}
		}
	}
	agentID, _ := a.cell.GetStore().GetAgentID()

	for i := len(lockPaths) - 1; i >= 0; i-- {
		lockPath := lockPaths[i]
		val, _, _, _, _ := a.cell.GetStore().GetKVEntry(lockPath)

		if val != nil && len(val) > 0 {
			var currentLock LockData
			if err := json.Unmarshal(val, &currentLock); err == nil {
				// Only release if we own it
				if currentLock.AgentID == agentID {
					// Restrict retryability to the lock's expiration time
					releaseCtx, cancel := context.WithDeadline(ctx, time.Unix(0, currentLock.ExpiresAt))
					resp, err := a.CompareAndWrite(releaseCtx, lockPath, val, []byte{}, paxos.QuorumAll)
					cancel()

					if err != nil {
						return &paxosv1.ReleaseLockResponse{Success: false, Message: fmt.Sprintf("failed to release lock at %s: %v", lockPath, err)}, nil
					}
					if !resp.Success {
						return &paxosv1.ReleaseLockResponse{Success: false, Message: fmt.Sprintf("failed to release lock at %s: %s", lockPath, resp.Message)}, nil
					}
				}
			}
		}
	}

	return &paxosv1.ReleaseLockResponse{Success: true, Message: "Locks released"}, nil
}

// RenewLock attempts to renew the lease duration for all lockable segments in a key path.
func (a *UserAPI) RenewLock(ctx context.Context, req *paxosv1.RenewLockRequest) (*paxosv1.RenewLockResponse, error) {
	lockPaths := extractLockPaths(req.KeyPath)
	if len(lockPaths) == 0 {
		if strings.HasSuffix(req.KeyPath, "/"+constants.LockableSegment) || req.KeyPath == constants.LockableSegment {
			lockPaths = []string{req.KeyPath}
		}
	}
	agentID, _ := a.cell.GetStore().GetAgentID()
	duration := time.Duration(req.DurationMs) * time.Millisecond

	now := time.Now().UnixNano()
	expiresAt := time.Now().Add(duration).UnixNano()

	for _, lockPath := range lockPaths {
		val, _, _, _, _ := a.cell.GetStore().GetKVEntry(lockPath)

		if val != nil && len(val) > 0 {
			var currentLock LockData
			if err := json.Unmarshal(val, &currentLock); err == nil {
				if currentLock.AgentID != agentID && currentLock.ExpiresAt > now {
					return &paxosv1.RenewLockResponse{Success: false, Message: fmt.Sprintf("cannot renew lock at %s: owned by %s", lockPath, currentLock.AgentID)}, nil
				}

				if currentLock.ExpiresAt < now {
					glog.Warningf("Renewing expired lock at %s", lockPath)
				}

				newLock := LockData{
					AgentID:   agentID,
					ExpiresAt: expiresAt,
				}
				newVal, _ := json.Marshal(newLock)

				resp, err := a.CompareAndWrite(ctx, lockPath, val, newVal, paxos.QuorumMajority)
				if err != nil || !resp.Success {
					return &paxosv1.RenewLockResponse{Success: false, Message: fmt.Sprintf("failed to renew lock at %s", lockPath)}, nil
				}
			} else {
				return &paxosv1.RenewLockResponse{Success: false, Message: fmt.Sprintf("invalid lock data at %s", lockPath)}, nil
			}
		} else {
			return &paxosv1.RenewLockResponse{Success: false, Message: fmt.Sprintf("lock at %s does not exist", lockPath)}, nil
		}
	}

	return &paxosv1.RenewLockResponse{Success: true, Message: "Locks renewed"}, nil
}

// CheckLocks verifies that all locks for a path are held by this agent.
func (a *UserAPI) CheckLocks(ctx context.Context, keyPath string) error {
	lockPaths := extractLockPaths(keyPath)
	if len(lockPaths) == 0 {
		return nil
	}

	agentID, _ := a.cell.GetStore().GetAgentID()
	now := time.Now().UnixNano()

	for _, lockPath := range lockPaths {
		val, _, _, _, _ := a.cell.GetStore().GetKVEntry(lockPath)
		if val == nil || len(val) == 0 {
			return fmt.Errorf("lock required at %s but is empty", lockPath)
		}

		var currentLock LockData
		if err := json.Unmarshal(val, &currentLock); err != nil {
			return fmt.Errorf("invalid lock data at %s", lockPath)
		}

		if currentLock.AgentID != agentID {
			return fmt.Errorf("lock at %s held by %s", lockPath, currentLock.AgentID)
		}

		if currentLock.ExpiresAt <= now {
			return fmt.Errorf("lock at %s has expired", lockPath)
		}
	}

	return nil
}
