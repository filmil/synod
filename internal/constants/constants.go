// SPDX-License-Identifier: Apache-2.0

// Package constants defines global constants used across the application.
package constants

const (
	// PeersKey is the KV store key used to persist the cluster membership.
	PeersKey = "/_internal/peers"
	// LockableSegment is the path component that identifies a directory as being lockable.
	LockableSegment = "_lockable"
)
