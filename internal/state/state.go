package state

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	"github.com/golang/glog"
	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"

	paxosv1 "github.com/filmil/synod/proto/paxos/v1"
)

type Store struct {
	db *sql.DB
}

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
	schema := `
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
		version INTEGER
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
	return nil
}

func (s *Store) GetAgentID() (string, error) {
	var id string
	err := s.db.QueryRow("SELECT value FROM agent_info WHERE key = 'agent_id'").Scan(&id)
	if err == sql.ErrNoRows {
		id = uuid.New().String()
		if _, err := s.db.Exec("INSERT INTO agent_info (key, value) VALUES ('agent_id', ?)", id); err != nil {
			return "", fmt.Errorf("failed to insert new agent ID: %w", err)
		}
		glog.Infof("Generated new agent ID: %s", id)
		return id, nil
	} else if err != nil {
		return "", fmt.Errorf("failed to query agent ID: %w", err)
	}
	return id, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

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

func (s *Store) AddMember(agentID, address string) error {
	_, err := s.db.Exec(`
		INSERT INTO membership (agent_id, address) VALUES (?, ?)
		ON CONFLICT(agent_id) DO UPDATE SET address = excluded.address`,
		agentID, address)
	if err != nil {
		return fmt.Errorf("failed to add member: %w", err)
	}
	return nil
}

func (s *Store) GetMembers() (map[string]string, error) {
	rows, err := s.db.Query("SELECT agent_id, address FROM membership")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	members := make(map[string]string)
	for rows.Next() {
		var id, addr string
		if err := rows.Scan(&id, &addr); err != nil {
			return nil, err
		}
		members[id] = addr
	}
	return members, nil
}

func (s *Store) CommitKV(key string, value []byte, valType string, version uint64) error {
	_, err := s.db.Exec(`
		INSERT INTO kv_store (key, value, type, version) VALUES (?, ?, ?, ?)
		ON CONFLICT(key) DO UPDATE SET
			value = excluded.value,
			type = excluded.type,
			version = excluded.version`,
		key, value, valType, version)
	if err != nil {
		return fmt.Errorf("failed to commit KV: %w", err)
	}
	return nil
}

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

func (s *Store) GetKVEntry(key string) (value []byte, valType string, version uint64, err error) {
	var pn sql.NullInt64
	err = s.db.QueryRow("SELECT value, type, version FROM kv_store WHERE key = ?", key).Scan(&value, &valType, &pn)
	if err == sql.ErrNoRows {
		return nil, "", 0, nil
	}
	if pn.Valid {
		version = uint64(pn.Int64)
	}
	return value, valType, version, err
}

type KVEntry struct {
	Key     string
	Value   []byte
	Type    string
	Version uint64
}

func (s *Store) GetAllKVs() ([]KVEntry, error) {
	rows, err := s.db.Query("SELECT key, value, type, version FROM kv_store ORDER BY key")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []KVEntry
	for rows.Next() {
		var e KVEntry
		var pn sql.NullInt64
		if err := rows.Scan(&e.Key, &e.Value, &e.Type, &pn); err != nil {
			return nil, err
		}
		if pn.Valid {
			e.Version = uint64(pn.Int64)
		}
		entries = append(entries, e)
	}
	return entries, nil
}

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

type LogEntry struct {
	ID        int64
	Timestamp string
	Type      string
	Sender    string
	Receiver  string
	Message   string
	Reply     string
}

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
