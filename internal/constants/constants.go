// Package constants defines global constants used across the application.
package constants

const (
	// PeersKey is the KV store key used to persist the cluster membership.
	PeersKey = "/_internal/peers"
	// LockSuffix is the name of the file used to represent a lock in a _lockable directory.
	LockSuffix = "_lock"
	// LockableSegment is the path component that identifies a directory as being lockable.
	LockableSegment = "_lockable"
)
