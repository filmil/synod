package state

import (
	"os"
	"strings"
	"testing"

	paxosv1 "github.com/filmil/synod/proto/paxos/v1"
)

func TestStoreInternalKeys(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "state-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Step 1: Create a store and save some keys
	s, err := NewStore(tmpDir)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	// Normal key
	if err := s.CommitKV("/normal", []byte("normal-val"), "data", 1); err != nil {
		t.Fatalf("failed to commit normal KV: %v", err)
	}

	// Internal key
	if err := s.CommitKV("/_internal/test", []byte("internal-val"), "data", 1); err != nil {
		t.Fatalf("failed to commit internal KV: %v", err)
	}

	// Acceptor state for internal key
	pID := &paxosv1.ProposalID{Number: 1, AgentId: "agent-1"}
	if err := s.SetAcceptedValue("/_internal/test", pID, []byte("internal-val")); err != nil {
		t.Fatalf("failed to set accepted value for internal key: %v", err)
	}

	// Verify they are present
	val, _, version, _, err := s.GetKVEntry("/normal")
	if err != nil || string(val) != "normal-val" || version != 1 {
		t.Fatalf("failed to get normal KV: %v, val: %s, version: %d", err, string(val), version)
	}

	val, _, version, _, err = s.GetKVEntry("/_internal/test")
	if err != nil || string(val) != "internal-val" || version != 1 {
		t.Fatalf("failed to get internal KV: %v, val: %s, version: %d", err, string(val), version)
	}

	_, aID, aVal, err := s.GetAcceptorState("/_internal/test")
	if err != nil || aID.Number != 1 || string(aVal) != "internal-val" {
		t.Fatalf("failed to get internal acceptor state: %v, aID: %+v, aVal: %s", err, aID, string(aVal))
	}

	s.Close()

	// Step 2: Create a NEW store in the same directory
	s2, err := NewStore(tmpDir)
	if err != nil {
		t.Fatalf("failed to create second store: %v", err)
	}
	defer s2.Close()

	// Verify normal key is STILL there
	val, _, version, _, err = s2.GetKVEntry("/normal")
	if err != nil || string(val) != "normal-val" || version != 1 {
		t.Fatalf("failed to get normal KV after restart: %v, val: %s, version: %d", err, string(val), version)
	}

	// Verify internal key is GONE
	val, _, version, _, err = s2.GetKVEntry("/_internal/test")
	if err != nil {
		t.Fatalf("failed to get internal KV after restart: %v", err)
	}
	if val != nil {
		t.Errorf("internal KV should be GONE after restart, but got: %s", string(val))
	}

	// Verify internal acceptor state is GONE
	_, aID, aVal, err = s2.GetAcceptorState("/_internal/test")
	if err != nil {
		t.Fatalf("failed to get internal acceptor state after restart: %v", err)
	}
	if aID != nil || aVal != nil {
		t.Errorf("internal acceptor state should be GONE after restart")
	}
}

func TestStoreExclusiveMode(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "state-exclusive-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	s1, err := NewStore(tmpDir)
	if err != nil {
		t.Fatalf("failed to create store 1: %v", err)
	}
	defer s1.Close()

	// Try to open a second store in the same directory
	// In EXCLUSIVE mode, this should fail because the first store has an exclusive lock.
	_, err = NewStore(tmpDir)
	if err == nil {
		t.Fatalf("should have failed to open second store in exclusive mode")
	}
	if !strings.Contains(err.Error(), "database is locked") {
		t.Errorf("expected 'database is locked' error, got: %v", err)
	}
}

func TestValidateKey(t *testing.T) {
	tests := []struct {
		key     string
		wantErr bool
	}{
		{"/foo", false},
		{"/foo/bar", false},
		{"/", false},
		{"/_internal/peers", false},
		{"foo", true},       // No leading slash
		{"/foo/./bar", true}, // Dot segment
		{"/foo/../bar", true}, // Dot-dot segment
		{"/foo//bar", true},  // Empty segment
		{"/foo/", false},      // Trailing slash (allowed but maybe we should decide)
	}

	for _, tt := range tests {
		err := ValidateKey(tt.key)
		if (err != nil) != tt.wantErr {
			t.Errorf("ValidateKey(%q) error = %v, wantErr %v", tt.key, err, tt.wantErr)
		}
	}
}

func TestGetKVsByPrefix(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "state-prefix-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	s, err := NewStore(tmpDir)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer s.Close()

	keys := []string{
		"/a/1",
		"/a/2",
		"/b/1",
		"/_internal/a/1",
		"/_internal/b/1",
	}

	for _, k := range keys {
		if err := s.CommitKV(k, []byte("val"), "data", 1); err != nil {
			t.Fatalf("failed to commit KV %s: %v", k, err)
		}
	}

	tests := []struct {
		prefix string
		want   int
	}{
		{"/a", 2},
		{"/b", 1},
		{"/_internal", 2},
		{"/_internal/a", 1},
		{"/", 5},
	}

	for _, tt := range tests {
		entries, err := s.GetKVsByPrefix(tt.prefix)
		if err != nil {
			t.Errorf("GetKVsByPrefix(%q) error = %v", tt.prefix, err)
			continue
		}
		if len(entries) != tt.want {
			t.Errorf("GetKVsByPrefix(%q) got %d entries, want %d", tt.prefix, len(entries), tt.want)
		}
	}
}
