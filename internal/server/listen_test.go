// SPDX-License-Identifier: Apache-2.0

package server

import (
	"errors"
	"net"
	"testing"
	"time"
)

func TestListenWithRetry_Success(t *testing.T) {
	// 127.0.0.1:0 asks the OS to pick an available port on the local loopback.
	// It should succeed immediately.
	addr := "127.0.0.1:0"
	lis, err := ListenWithRetry(addr)
	if err != nil {
		t.Fatalf("ListenWithRetry failed: %v", err)
	}
	defer lis.Close()

	if lis.Addr() == nil {
		t.Errorf("expected non-nil address")
	}
}

func TestListenWithRetry_Occupied(t *testing.T) {
	// First, occupy a port.
	occupyingLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen initially: %v", err)
	}
	defer occupyingLis.Close()

	occupiedAddr := occupyingLis.Addr().String()

	// Now call ListenWithRetry with the occupied port.
	// It should fail initially, backoff, and eventually find a free port.
	newLis, err := ListenWithRetry(occupiedAddr)
	if err != nil {
		t.Fatalf("ListenWithRetry failed when port was occupied: %v", err)
	}
	defer newLis.Close()

	newAddr := newLis.Addr().String()
	if newAddr == occupiedAddr {
		t.Errorf("ListenWithRetry returned the same occupied address: %v", newAddr)
	}
}

func TestListenWithRetry_Exhaustion(t *testing.T) {
	// Save the original values and restore them after the test.
	origNetListen := netListen
	origBackoffMaxTime := backoffMaxTime
	origBackoffInitial := backoffInitial
	origBackoffMaxInterval := backoffMaxInterval
	defer func() {
		netListen = origNetListen
		backoffMaxTime = origBackoffMaxTime
		backoffInitial = origBackoffInitial
		backoffMaxInterval = origBackoffMaxInterval
	}()

	// Mock netListen to always fail
	netListen = func(network, address string) (net.Listener, error) {
		return nil, errors.New("mock listen error")
	}

	// Reduce backoff parameters to make the test run quickly
	backoffMaxTime = 10 * time.Millisecond
	backoffInitial = 1 * time.Millisecond
	backoffMaxInterval = 2 * time.Millisecond

	lis, err := ListenWithRetry("127.0.0.1:0")
	if err == nil {
		if lis != nil {
			lis.Close()
		}
		t.Fatalf("expected ListenWithRetry to fail due to timeout, but it succeeded")
	}

	if lis != nil {
		t.Errorf("expected listener to be nil on error, got %v", lis)
	}
}
