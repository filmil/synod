package state

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/golang/glog"
	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"

	paxosv1 "github.com/filmil/synod/proto/paxos/v1"
)

const schemaVersion = 1

// Store manages the persistence of agent state, including Paxos variables,
// Key-Value data, cluster membership, and a message log. It is backed by SQLite.
type Store struct {
	db *sql.DB
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

	s := &Store{db: db}
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

// GetAgentID retrieves the persistent agent ID and short name for this node.
// If they do not exist, a new agent ID is generated and saved.
func (s *Store) GetAgentID() (string, string, error) {
	var id, shortName string
	err := s.db.QueryRow("SELECT value FROM agent_info WHERE key = 'agent_id'").Scan(&id)
	err2 := s.db.QueryRow("SELECT value FROM agent_info WHERE key = 'short_name'").Scan(&shortName)

	if err == sql.ErrNoRows || err2 == sql.ErrNoRows {
		if id == "" {
			id = uuid.New().String()
			if _, e := s.db.Exec("INSERT INTO agent_info (key, value) VALUES ('agent_id', ?)", id); e != nil {
				return "", "", fmt.Errorf("failed to insert new agent ID: %w", e)
			}
			glog.Infof("Generated new agent ID: %s", id)
		}
		if shortName == "" {
			shortName = "PendingName" // We will replace this with a real name via the caller
			if _, e := s.db.Exec("INSERT INTO agent_info (key, value) VALUES ('short_name', ?)", shortName); e != nil {
				return "", "", fmt.Errorf("failed to insert new short name: %w", e)
			}
		}
		return id, shortName, nil
	} else if err != nil {
		return "", "", fmt.Errorf("failed to query agent ID: %w", err)
	} else if err2 != nil {
		return "", "", fmt.Errorf("failed to query short name: %w", err2)
	}
	return id, shortName, nil
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
	deleted := len(value) == 0
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
	return keys, nil
}

// GetKVEntry retrieves the value, type, version, and deletion status of a specific key.
func (s *Store) GetKVEntry(key string) (value []byte, valType string, version uint64, deleted bool, err error) {
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
