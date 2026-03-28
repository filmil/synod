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
		ledger_index INTEGER PRIMARY KEY,
		promised_id_num INTEGER,
		promised_id_agent TEXT,
		accepted_id_num INTEGER,
		accepted_id_agent TEXT,
		accepted_value BLOB
	);

	CREATE TABLE IF NOT EXISTS ledger (
		ledger_index INTEGER PRIMARY KEY,
		value BLOB,
		type TEXT
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
	_, err := s.db.Exec(schema)
	return err
}

func (s *Store) GetAgentID() (string, error) {
	var id string
	err := s.db.QueryRow("SELECT value FROM agent_info WHERE key = 'agent_id'").Scan(&id)
	if err == sql.ErrNoRows {
		id = uuid.New().String()
		_, err = s.db.Exec("INSERT INTO agent_info (key, value) VALUES ('agent_id', ?)", id)
		if err != nil {
			return "", err
		}
		glog.Infof("Generated new agent ID: %s", id)
		return id, nil
	}
	return id, err
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) GetAcceptorState(index uint64) (promisedID *paxosv1.ProposalID, acceptedID *paxosv1.ProposalID, acceptedValue []byte, err error) {
	var pIDNum, aIDNum sql.NullInt64
	var pIDAgent, aIDAgent sql.NullString

	err = s.db.QueryRow(`
		SELECT promised_id_num, promised_id_agent, accepted_id_num, accepted_id_agent, accepted_value 
		FROM acceptor_state WHERE ledger_index = ?`, index).Scan(
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

func (s *Store) SetPromisedID(index uint64, id *paxosv1.ProposalID) error {
	_, err := s.db.Exec(`
		INSERT INTO acceptor_state (ledger_index, promised_id_num, promised_id_agent)
		VALUES (?, ?, ?)
		ON CONFLICT(ledger_index) DO UPDATE SET
			promised_id_num = excluded.promised_id_num,
			promised_id_agent = excluded.promised_id_agent`,
		index, id.Number, id.AgentId)
	return err
}

func (s *Store) SetAcceptedValue(index uint64, id *paxosv1.ProposalID, value []byte) error {
	_, err := s.db.Exec(`
		INSERT INTO acceptor_state (ledger_index, promised_id_num, promised_id_agent, accepted_id_num, accepted_id_agent, accepted_value)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(ledger_index) DO UPDATE SET
			promised_id_num = excluded.promised_id_num,
			promised_id_agent = excluded.promised_id_agent,
			accepted_id_num = excluded.accepted_id_num,
			accepted_id_agent = excluded.accepted_id_agent,
			accepted_value = excluded.accepted_value`,
		index, id.Number, id.AgentId, id.Number, id.AgentId, value)
	return err
}

func (s *Store) AddMember(agentID, address string) error {
	_, err := s.db.Exec(`
		INSERT INTO membership (agent_id, address) VALUES (?, ?)
		ON CONFLICT(agent_id) DO UPDATE SET address = excluded.address`,
		agentID, address)
	return err
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

func (s *Store) CommitLedger(index uint64, value []byte, valType string) error {
	_, err := s.db.Exec(`
		INSERT INTO ledger (ledger_index, value, type) VALUES (?, ?, ?)
		ON CONFLICT(ledger_index) DO UPDATE SET
			value = excluded.value,
			type = excluded.type`,
		index, value, valType)
	return err
}

func (s *Store) GetHighestLedgerIndex() (uint64, error) {
	var index uint64
	err := s.db.QueryRow("SELECT MAX(ledger_index) FROM ledger").Scan(&index)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	// Handle the case where the table is empty (MAX returns NULL)
	var nullIndex sql.NullInt64
	err = s.db.QueryRow("SELECT MAX(ledger_index) FROM ledger").Scan(&nullIndex)
	if err != nil {
		return 0, err
	}
	if !nullIndex.Valid {
		return 0, nil
	}
	return uint64(nullIndex.Int64), nil
}

func (s *Store) LogMessage(msgType, sender, receiver string, message, reply []byte) error {
	_, err := s.db.Exec(`
		INSERT INTO message_log (type, sender, receiver, message, reply)
		VALUES (?, ?, ?, ?, ?)`,
		msgType, sender, receiver, message, reply)
	return err
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
