// SPDX-License-Identifier: Apache-2.0

package state

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/filmil/synod/internal/identity"
	"github.com/golang/glog"
	_ "github.com/mattn/go-sqlite3"

	paxosv1 "github.com/filmil/synod/proto/paxos/v1"
)

const schemaVersion = 1

// ValidateKey ensures the provided key is a valid absolute Unix path.
// It does not support '.' or '..' segments.
func ValidateKey(key string) error {
	if !strings.HasPrefix(key, "/") {
		return fmt.Errorf("key must be an absolute path (starting with /): %s", key)
	}
	parts := strings.Split(key, "/")
	for _, part := range parts {
		if part == "." || part == ".." {
			return fmt.Errorf("key cannot contain '.' or '..' segments: %s", key)
		}
	}
	// Also ensure no double slashes, though Split handles them as empty strings
	// If we want to be strict about no empty segments (other than the first one)
	for i, part := range parts {
		if i > 0 && part == "" && i < len(parts)-1 {
			// This would catch /foo//bar
			return fmt.Errorf("key cannot contain empty segments: %s", key)
		}
	}
	return nil
}

// Store manages the persistence of agent state, including Paxos variables,
// Key-Value data, cluster membership, and a message log. It is backed by SQLite.
type Store struct {
	db *sql.DB
	mu sync.RWMutex

	// In-memory storage for keys starting with /_internal
	internalKVs           map[string]KVEntry
	internalAcceptorState map[string]acceptorState

	// Identity of the agent
	ident *identity.Identity
}

type acceptorState struct {
	promisedID    *paxosv1.ProposalID
	acceptedID    *paxosv1.ProposalID
	acceptedValue []byte
}

// NewStore initializes a new Store, creating the necessary database files and schema.
func NewStore(stateDir string) (*Store, error) {
	if stateDir == "" {
		return nil, fmt.Errorf("state directory is required")
	}

	if err := os.MkdirAll(stateDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create state directory: %w", err)
	}

	dbPath := filepath.Join(stateDir, "paxos.db")
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Set SQLite to exclusive locking mode
	if _, err := db.Exec("PRAGMA locking_mode = EXCLUSIVE;"); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to set exclusive locking mode: %w", err)
	}

	// Limit to a single connection to avoid "database is locked" when using EXCLUSIVE mode.
	db.SetMaxOpenConns(1)

	s := &Store{
		db:                    db,
		internalKVs:           make(map[string]KVEntry),
		internalAcceptorState: make(map[string]acceptorState),
	}
	if err := s.init(); err != nil {
		db.Close()
		return nil, err
	}

	return s, nil
}

func (s *Store) init() error {
	// Check if schema_version table exists
	var tableName string
	err := s.db.QueryRow("SELECT name FROM sqlite_master WHERE type='table' AND name='schema_version'").Scan(&tableName)
	if err == sql.ErrNoRows {
		// Table doesn't exist, this is a fresh database (or one from before versioning was added)
		// We'll proceed with creating all tables, including schema_version.
	} else if err != nil {
		return fmt.Errorf("failed to check for schema_version table: %w", err)
	} else {
		// Table exists, check the version
		var currentVersion int
		err = s.db.QueryRow("SELECT version FROM schema_version LIMIT 1").Scan(&currentVersion)
		if err != nil {
			return fmt.Errorf("failed to read schema version: %w", err)
		}
		if currentVersion != schemaVersion {
			return fmt.Errorf("schema version mismatch: expected %d, got %d. Please clear your state directory or run migrations", schemaVersion, currentVersion)
		}
	}

	schema := `
	CREATE TABLE IF NOT EXISTS schema_version (
		version INTEGER PRIMARY KEY
	);

	CREATE TABLE IF NOT EXISTS agent_info (
		key TEXT PRIMARY KEY,
		value TEXT
	);

	CREATE TABLE IF NOT EXISTS acceptor_state (
		key TEXT PRIMARY KEY,
		promised_id_num INTEGER,
		promised_id_agent TEXT,
		accepted_id_num INTEGER,
		accepted_id_agent TEXT,
		accepted_value BLOB
	);

	CREATE TABLE IF NOT EXISTS kv_store (
		key TEXT PRIMARY KEY,
		value BLOB,
		type TEXT,
		version INTEGER,
		deleted BOOLEAN DEFAULT FALSE
	);

	CREATE TABLE IF NOT EXISTS membership (
		agent_id TEXT PRIMARY KEY,
		address TEXT
	);

	CREATE TABLE IF NOT EXISTS message_log (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
		type TEXT,
		sender TEXT,
		receiver TEXT,
		message BLOB,
		reply BLOB
	);
	`
	if _, err := s.db.Exec(schema); err != nil {
		return fmt.Errorf("failed to initialize database schema: %w", err)
	}

	// Insert the current version if it doesn't exist (e.g. fresh DB)
	if _, err := s.db.Exec("INSERT OR IGNORE INTO schema_version (version) VALUES (?)", schemaVersion); err != nil {
		return fmt.Errorf("failed to set schema version: %w", err)
	}

	return nil
}

// InitializeAgentID retrieves the persistent agent ID and short name for this node.
// It also initializes the cryptographic identity for this agent if it doesn't exist.
func (s *Store) InitializeAgentID(passphrase string) (string, string, error) {
	ident, err := s.GetIdentity(passphrase)
	if err != nil {
		return "", "", fmt.Errorf("failed to get cryptographic identity: %w", err)
	}

	agentID := ident.AgentID()

	var id, shortName string
	err = s.db.QueryRow("SELECT value FROM agent_info WHERE key = 'agent_id'").Scan(&id)
	err2 := s.db.QueryRow("SELECT value FROM agent_info WHERE key = 'short_name'").Scan(&shortName)

	if err == sql.ErrNoRows || err2 == sql.ErrNoRows {
		if id != agentID {
			if _, e := s.db.Exec("INSERT INTO agent_info (key, value) VALUES ('agent_id', ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value", agentID); e != nil {
				return "", "", fmt.Errorf("failed to insert new agent ID: %w", e)
			}
			glog.Infof("Set agent ID to: %s", agentID)
		}
		if shortName == "" {
			shortName = "PendingName" // We will replace this with a real name via the caller
			if _, e := s.db.Exec("INSERT INTO agent_info (key, value) VALUES ('short_name', ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value", shortName); e != nil {
				return "", "", fmt.Errorf("failed to insert new short name: %w", e)
			}
		}
		return agentID, shortName, nil
	} else if err != nil {
		return "", "", fmt.Errorf("failed to query agent ID: %w", err)
	} else if err2 != nil {
		return "", "", fmt.Errorf("failed to query short name: %w", err2)
	}

	if id != agentID {
		glog.Infof("Updating stored agent ID from %s to %s to match cryptographic identity", id, agentID)
		if _, e := s.db.Exec("UPDATE agent_info SET value = ? WHERE key = 'agent_id'", agentID); e != nil {
			return "", "", fmt.Errorf("failed to update agent ID: %w", e)
		}
	}

	return agentID, shortName, nil
}

// GetAgentID returns the established agent ID.
func (s *Store) GetAgentID() (string, error) {
	if s.ident != nil {
		return s.ident.AgentID(), nil
	}
	var id string
	err := s.db.QueryRow("SELECT value FROM agent_info WHERE key = 'agent_id'").Scan(&id)
	if err != nil {
		return "", err
	}
	return id, nil
}

// GetShortName returns the established short name for this agent.
func (s *Store) GetShortName() (string, error) {
	var shortName string
	err := s.db.QueryRow("SELECT value FROM agent_info WHERE key = 'short_name'").Scan(&shortName)
	if err != nil {
		return "", err
	}
	return shortName, nil
}

// GetIdentity retrieves the cryptographic identity for this agent.
func (s *Store) GetIdentity(passphrase string) (*identity.Identity, error) {
	if s.ident != nil {
		return s.ident, nil
	}

	var certPEM, keyPEM []byte
	err := s.db.QueryRow("SELECT value FROM agent_info WHERE key = 'identity_cert'").Scan(&certPEM)
	err2 := s.db.QueryRow("SELECT value FROM agent_info WHERE key = 'identity_key'").Scan(&keyPEM)

	if err == sql.ErrNoRows || err2 == sql.ErrNoRows {
		glog.Info("Generating new cryptographic identity")
		ident, err := identity.Generate("Synod Agent") // Default name, caller can update shortName
		if err != nil {
			return nil, err
		}
		if err := s.SaveIdentity(ident, passphrase); err != nil {
			return nil, err
		}
		s.ident = ident
		return ident, nil
	} else if err != nil {
		return nil, fmt.Errorf("failed to query identity certificate: %w", err)
	} else if err2 != nil {
		return nil, fmt.Errorf("failed to query identity key: %w", err2)
	}

	cert, err := identity.UnmarshalCertificate(certPEM)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal certificate: %w", err)
	}

	key, err := identity.UnmarshalPrivateKey(keyPEM, passphrase)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal private key: %w", err)
	}

	s.ident = &identity.Identity{
		Certificate: cert,
		PrivateKey:  key,
	}
	return s.ident, nil
}

// SaveIdentity saves the cryptographic identity for this agent to the store.
func (s *Store) SaveIdentity(ident *identity.Identity, passphrase string) error {
	certPEM := identity.MarshalCertificate(ident.Certificate)
	keyPEM, err := identity.MarshalPrivateKey(ident.PrivateKey, passphrase)
	if err != nil {
		return fmt.Errorf("failed to marshal private key: %w", err)
	}

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec("INSERT INTO agent_info (key, value) VALUES ('identity_cert', ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value", certPEM); err != nil {
		return fmt.Errorf("failed to save identity certificate: %w", err)
	}
	if _, err := tx.Exec("INSERT INTO agent_info (key, value) VALUES ('identity_key', ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value", keyPEM); err != nil {
		return fmt.Errorf("failed to save identity key: %w", err)
	}

	return tx.Commit()
}

// SetShortName updates the human-readable short name for this node.
func (s *Store) SetShortName(shortName string) error {
	_, err := s.db.Exec("INSERT INTO agent_info (key, value) VALUES ('short_name', ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value", shortName)
	if err != nil {
		return fmt.Errorf("failed to set short name: %w", err)
	}
	return nil
}

// Close closes the underlying database connection.
func (s *Store) Close() error {
	return s.db.Close()
}

// GetAcceptorState retrieves the highest promised and accepted proposal IDs, and the
// accepted value for a given key.
func (s *Store) GetAcceptorState(key string) (promisedID *paxosv1.ProposalID, acceptedID *paxosv1.ProposalID, acceptedValue []byte, err error) {
	if err := ValidateKey(key); err != nil {
		return nil, nil, nil, err
	}
	if strings.HasPrefix(key, "/_internal") {
		s.mu.RLock()
		defer s.mu.RUnlock()
		if state, ok := s.internalAcceptorState[key]; ok {
			return state.promisedID, state.acceptedID, state.acceptedValue, nil
		}
		return nil, nil, nil, nil
	}

	var pIDNum, aIDNum sql.NullInt64
	var pIDAgent, aIDAgent sql.NullString

	err = s.db.QueryRow(`
		SELECT promised_id_num, promised_id_agent, accepted_id_num, accepted_id_agent, accepted_value 
		FROM acceptor_state WHERE key = ?`, key).Scan(
		&pIDNum, &pIDAgent, &aIDNum, &aIDAgent, &acceptedValue)

	if err == sql.ErrNoRows {
		return nil, nil, nil, nil
	}
	if err != nil {
		return nil, nil, nil, err
	}

	if pIDNum.Valid {
		promisedID = &paxosv1.ProposalID{Number: uint64(pIDNum.Int64), AgentId: pIDAgent.String}
	}
	if aIDNum.Valid {
		acceptedID = &paxosv1.ProposalID{Number: uint64(aIDNum.Int64), AgentId: aIDAgent.String}
	}

	return promisedID, acceptedID, acceptedValue, nil
}

// SetPromisedID records a promise not to accept proposals with a lower ID for a given key.
func (s *Store) SetPromisedID(key string, id *paxosv1.ProposalID) error {
	if err := ValidateKey(key); err != nil {
		return err
	}
	if strings.HasPrefix(key, "/_internal") {
		s.mu.Lock()
		defer s.mu.Unlock()
		st := s.internalAcceptorState[key]
		st.promisedID = id
		s.internalAcceptorState[key] = st
		return nil
	}

	_, err := s.db.Exec(`
		INSERT INTO acceptor_state (key, promised_id_num, promised_id_agent)
		VALUES (?, ?, ?)
		ON CONFLICT(key) DO UPDATE SET
			promised_id_num = excluded.promised_id_num,
			promised_id_agent = excluded.promised_id_agent`,
		key, id.Number, id.AgentId)
	if err != nil {
		return fmt.Errorf("failed to set promised ID: %w", err)
	}
	return nil
}

// SetAcceptedValue records the acceptance of a proposal for a given key.
func (s *Store) SetAcceptedValue(key string, id *paxosv1.ProposalID, value []byte) error {
	if err := ValidateKey(key); err != nil {
		return err
	}
	if strings.HasPrefix(key, "/_internal") {
		s.mu.Lock()
		defer s.mu.Unlock()
		st := s.internalAcceptorState[key]
		st.promisedID = id
		st.acceptedID = id
		st.acceptedValue = value
		s.internalAcceptorState[key] = st
		return nil
	}

	_, err := s.db.Exec(`
		INSERT INTO acceptor_state (key, promised_id_num, promised_id_agent, accepted_id_num, accepted_id_agent, accepted_value)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(key) DO UPDATE SET
			promised_id_num = excluded.promised_id_num,
			promised_id_agent = excluded.promised_id_agent,
			accepted_id_num = excluded.accepted_id_num,
			accepted_id_agent = excluded.accepted_id_agent,
			accepted_value = excluded.accepted_value`,
		key, id.Number, id.AgentId, id.Number, id.AgentId, value)
	if err != nil {
		return fmt.Errorf("failed to set accepted value: %w", err)
	}
	return nil
}

// PeerInfo contains metadata about a cluster member.
type PeerInfo struct {
	// ShortName is the human-readable name of the peer.
	ShortName string `json:"short_name"`
	// GRPCAddr is the gRPC host:port for the peer.
	GRPCAddr  string `json:"grpc_addr"`
	// HTTPURL is the HTTP dashboard URL for the peer.
	HTTPURL   string `json:"http_url"`
	// Certificate is the DER-encoded X.509 certificate of the peer.
	Certificate []byte `json:"certificate"`
}

// Equal checks if two PeerInfo structs are identical.
func (p PeerInfo) Equal(other PeerInfo) bool {
	if p.ShortName != other.ShortName || p.GRPCAddr != other.GRPCAddr || p.HTTPURL != other.HTTPURL {
		return false
	}
	return bytes.Equal(p.Certificate, other.Certificate)
}

// AddMember adds or updates a peer in the local membership list.
func (s *Store) AddMember(agentID string, info PeerInfo) error {
	b, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("failed to marshal peer info: %w", err)
	}
	_, err = s.db.Exec(`
		INSERT INTO membership (agent_id, address) VALUES (?, ?)
		ON CONFLICT(agent_id) DO UPDATE SET address = excluded.address`,
		agentID, string(b))
	if err != nil {
		return fmt.Errorf("failed to add member: %w", err)
	}
	return nil
}

// RemoveMember removes a peer from the local membership list.
func (s *Store) RemoveMember(agentID string) error {
	_, err := s.db.Exec("DELETE FROM membership WHERE agent_id = ?", agentID)
	if err != nil {
		return fmt.Errorf("failed to remove member: %w", err)
	}
	return nil
}

// GetMembers retrieves the entire local membership list.
func (s *Store) GetMembers() (map[string]PeerInfo, error) {
	rows, err := s.db.Query("SELECT agent_id, address FROM membership")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	members := make(map[string]PeerInfo)
	for rows.Next() {
		var id, infoStr string
		if err := rows.Scan(&id, &infoStr); err != nil {
			return nil, err
		}
		var info PeerInfo
		if err := json.Unmarshal([]byte(infoStr), &info); err != nil {
			// Fallback if it was just a string (though it shouldn't be anymore)
			info = PeerInfo{ShortName: "Unknown"}
		}
		members[id] = info
	}
	return members, nil
}

// CommitKV saves a value to the Key-Value store after consensus is reached.
// If the value is empty, the entry is marked as deleted.
func (s *Store) CommitKV(key string, value []byte, valType string, version uint64) error {
	if err := ValidateKey(key); err != nil {
		return err
	}
	deleted := len(value) == 0
	if strings.HasPrefix(key, "/_internal") {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.internalKVs[key] = KVEntry{
			Key:     key,
			Value:   value,
			Type:    valType,
			Version: version,
			Deleted: deleted,
		}
		return nil
	}

	_, err := s.db.Exec(`
		INSERT INTO kv_store (key, value, type, version, deleted) VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(key) DO UPDATE SET
			value = excluded.value,
			type = excluded.type,
			version = excluded.version,
			deleted = excluded.deleted`,
		key, value, valType, version, deleted)
	if err != nil {
		return fmt.Errorf("failed to commit KV: %w", err)
	}
	return nil
}

// GetKVState returns a map of all keys and their corresponding consensus version numbers.
func (s *Store) GetKVState() (map[string]uint64, error) {
	rows, err := s.db.Query("SELECT key, version FROM kv_store")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	keys := make(map[string]uint64)
	for rows.Next() {
		var k string
		var pn sql.NullInt64
		if err := rows.Scan(&k, &pn); err != nil {
			return nil, err
		}
		if pn.Valid {
			keys[k] = uint64(pn.Int64)
		} else {
			keys[k] = 0
		}
	}

	// Merge with in-memory internal KVs
	s.mu.RLock()
	defer s.mu.RUnlock()
	for k, e := range s.internalKVs {
		keys[k] = e.Version
	}

	return keys, nil
}

// GetKVEntry retrieves the value, type, version, and deletion status of a specific key.
func (s *Store) GetKVEntry(key string) (value []byte, valType string, version uint64, deleted bool, err error) {
	if err := ValidateKey(key); err != nil {
		return nil, "", 0, false, err
	}
	if strings.HasPrefix(key, "/_internal") {
		s.mu.RLock()
		defer s.mu.RUnlock()
		if e, ok := s.internalKVs[key]; ok {
			return e.Value, e.Type, e.Version, e.Deleted, nil
		}
		return nil, "", 0, false, nil
	}

	var pn sql.NullInt64
	err = s.db.QueryRow("SELECT value, type, version, deleted FROM kv_store WHERE key = ?", key).Scan(&value, &valType, &pn, &deleted)
	if err == sql.ErrNoRows {
		return nil, "", 0, false, nil
	}
	if pn.Valid {
		version = uint64(pn.Int64)
	}
	return value, valType, version, deleted, err
}

// KVEntry represents a single entry in the Key-Value store.
type KVEntry struct {
	// Key is the unique identifier for the entry.
	Key     string
	// Value is the raw byte content of the entry.
	Value   []byte
	// Type classifies the entry (e.g., "data", "membership").
	Type    string
	// Version is the consensus sequence number for this specific update.
	Version uint64
	// Deleted indicates whether this entry is marked as deleted.
	Deleted bool
}

// GetAllKVs retrieves all non-deleted entries from the Key-Value store.
func (s *Store) GetAllKVs() ([]KVEntry, error) {
	rows, err := s.db.Query("SELECT key, value, type, version, deleted FROM kv_store WHERE deleted = FALSE ORDER BY key")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []KVEntry
	for rows.Next() {
		var e KVEntry
		var pn sql.NullInt64
		if err := rows.Scan(&e.Key, &e.Value, &e.Type, &pn, &e.Deleted); err != nil {
			return nil, err
		}
		if pn.Valid {
			e.Version = uint64(pn.Int64)
		}
		entries = append(entries, e)
	}

	// Add in-memory internal KVs
	s.mu.RLock()
	for _, e := range s.internalKVs {
		if !e.Deleted {
			entries = append(entries, e)
		}
	}
	s.mu.RUnlock()

	// Sort the merged entries by key
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Key < entries[j].Key
	})

	return entries, nil
}

// GetKVsByPrefix retrieves all non-deleted entries from the Key-Value store
// that start with the specified prefix.
func (s *Store) GetKVsByPrefix(prefix string) ([]KVEntry, error) {
	if err := ValidateKey(prefix); err != nil {
		return nil, err
	}

	// Calculate the end range for the prefix to allow efficient SQLite indexing.
	// E.g. prefix "/foo" -> range ["/foo", "/fop")
	prefixEnd := prefix[:len(prefix)-1] + string(prefix[len(prefix)-1]+1)

	rows, err := s.db.Query(`
		SELECT key, value, type, version, deleted 
		FROM kv_store 
		WHERE key >= ? AND key < ? AND deleted = FALSE 
		ORDER BY key`, prefix, prefixEnd)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []KVEntry
	for rows.Next() {
		var e KVEntry
		var pn sql.NullInt64
		if err := rows.Scan(&e.Key, &e.Value, &e.Type, &pn, &e.Deleted); err != nil {
			return nil, err
		}
		if pn.Valid {
			e.Version = uint64(pn.Int64)
		}
		entries = append(entries, e)
	}

	// Add in-memory internal KVs that match the prefix
	if strings.HasPrefix("/_internal", prefix) || strings.HasPrefix(prefix, "/_internal") {
		s.mu.RLock()
		for k, e := range s.internalKVs {
			if strings.HasPrefix(k, prefix) && !e.Deleted {
				// Avoid duplicates if also in DB (though internal shouldn't be)
				found := false
				for _, existing := range entries {
					if existing.Key == k {
						found = true
						break
					}
				}
				if !found {
					entries = append(entries, e)
				}
			}
		}
		s.mu.RUnlock()
	}

	// Sort the merged entries by key
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Key < entries[j].Key
	})

	return entries, nil
}

// LogMessage records an incoming or outgoing message and its reply to the database.
func (s *Store) LogMessage(msgType, sender, receiver string, message, reply []byte) error {
	_, err := s.db.Exec(`
		INSERT INTO message_log (type, sender, receiver, message, reply)
		VALUES (?, ?, ?, ?, ?)`,
		msgType, sender, receiver, message, reply)
	if err != nil {
		return fmt.Errorf("failed to log message: %w", err)
	}
	return nil
}

// LogEntry represents a single recorded message exchange.
type LogEntry struct {
	// ID is the sequential identifier for the log entry.
	ID        int64
	// Timestamp is the time the message was recorded.
	Timestamp string
	// Type classifies the message (e.g., Prepare, Accept).
	Type      string
	// Sender is the agent ID that initiated the message.
	Sender    string
	// Receiver is the agent ID that received the message.
	Receiver  string
	// Message contains a text representation of the request.
	Message   string
	// Reply contains a text representation of the response.
	Reply     string
}

// GetRecentMessages retrieves the most recent message log entries up to the specified limit.
func (s *Store) GetRecentMessages(limit int) ([]LogEntry, error) {
	rows, err := s.db.Query(`
		SELECT id, timestamp, type, sender, receiver, message, reply 
		FROM message_log ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []LogEntry
	for rows.Next() {
		var e LogEntry
		var msg, reply []byte
		if err := rows.Scan(&e.ID, &e.Timestamp, &e.Type, &e.Sender, &e.Receiver, &msg, &reply); err != nil {
			return nil, err
		}
		e.Message = string(msg)
		e.Reply = string(reply)
		entries = append(entries, e)
	}
	return entries, nil
}
