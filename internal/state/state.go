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
