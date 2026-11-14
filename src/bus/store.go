package bus

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

// store 是总线的持久化后端。nil 表示纯内存。
//
// SQLite 实现:dir/yi.db,三张表。
//   - events      只追加,seq 是总序
//   - consumers   每个消费者的游标与统计
//   - failures    Nack 记录
//
// inflight 不落盘:未确认的事件在重启后会重新投递,这正是 at-least-once。
type store interface {
	loadEvents() ([]storedEvent, int64, error)
	appendEvent(storedEvent) (bool, error)
	archiveEvents(upto int64) error
	loadState() (persistedState, error)
	saveConsumer(ConsumerState) error
	deleteConsumer(name string) error
	appendFailure(Failure) error
	close() error
}

// ConsumerState 是消费者可持久化的部分。
type ConsumerState struct {
	Name      string      `json:"name"`
	Types     []EventType `json:"types"`
	Cursor    int64       `json:"cursor"`
	Delivered uint64      `json:"delivered"`
	Failed    uint64      `json:"failed"`
	LastError string      `json:"last_error,omitempty"`
	LastEvent string      `json:"last_event,omitempty"`
	LastAt    string      `json:"last_at,omitempty"`
}

type persistedState struct {
	Consumers []ConsumerState `json:"consumers"`
	Failures  []Failure       `json:"failures"`
}

const schema = `
CREATE TABLE IF NOT EXISTS events (
	seq         INTEGER PRIMARY KEY,
	received_at TEXT NOT NULL,
	id          TEXT NOT NULL,
	source      TEXT NOT NULL,
	type        TEXT NOT NULL,
	types       TEXT NOT NULL DEFAULT '[]',
	"time"      TEXT NOT NULL,
	data        TEXT NOT NULL,
	archived    INTEGER NOT NULL DEFAULT 0,
	UNIQUE (source, id)
);

CREATE INDEX IF NOT EXISTS idx_events_type ON events(type, seq);

CREATE TABLE IF NOT EXISTS consumers (
	name       TEXT PRIMARY KEY,
	types      TEXT NOT NULL,
	cursor     INTEGER NOT NULL DEFAULT 0,
	delivered  INTEGER NOT NULL DEFAULT 0,
	failed     INTEGER NOT NULL DEFAULT 0,
	last_error TEXT NOT NULL DEFAULT '',
	last_event TEXT NOT NULL DEFAULT '',
	last_at    TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS failures (
	seq      INTEGER PRIMARY KEY AUTOINCREMENT,
	consumer TEXT NOT NULL,
	event_id TEXT NOT NULL,
	source   TEXT NOT NULL,
	error    TEXT NOT NULL,
	at       TEXT NOT NULL
);
`

type sqliteStore struct {
	db *sql.DB
}

func openSQLiteStore(dir string) (*sqliteStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("bus: create data dir: %w", err)
	}

	path := filepath.Join(dir, "yi.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("bus: open sqlite: %w", err)
	}
	// SQLite 一次只允许一个写者;单连接避免 BUSY 抖动。
	db.SetMaxOpenConns(1)

	for _, pragma := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA busy_timeout=5000",
		"PRAGMA synchronous=NORMAL",
	} {
		if _, err := db.Exec(pragma); err != nil {
			db.Close()
			return nil, fmt.Errorf("bus: %s: %w", pragma, err)
		}
	}
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("bus: create schema: %w", err)
	}

	// 老库没有 archived 列,补上。
	var hasArchived int
	if err := db.QueryRow(
		`SELECT count(*) FROM pragma_table_info('events') WHERE name = 'archived'`,
	).Scan(&hasArchived); err != nil {
		db.Close()
		return nil, fmt.Errorf("bus: inspect events schema: %w", err)
	}
	if hasArchived == 0 {
		if _, err := db.Exec(`ALTER TABLE events ADD COLUMN archived INTEGER NOT NULL DEFAULT 0`); err != nil {
			db.Close()
			return nil, fmt.Errorf("bus: migrate events.archived: %w", err)
		}
	}

	// 老库没有 types 列,补上并从 type 回填。
	var hasTypes int
	if err := db.QueryRow(
		`SELECT count(*) FROM pragma_table_info('events') WHERE name = 'types'`,
	).Scan(&hasTypes); err != nil {
		db.Close()
		return nil, fmt.Errorf("bus: inspect events.types: %w", err)
	}
	if hasTypes == 0 {
		if _, err := db.Exec(`ALTER TABLE events ADD COLUMN types TEXT NOT NULL DEFAULT '[]'`); err != nil {
			db.Close()
			return nil, fmt.Errorf("bus: migrate events.types: %w", err)
		}
	}
	if err := backfillTypes(db); err != nil {
		db.Close()
		return nil, err
	}

	var hasLastAt int
	if err := db.QueryRow(
		`SELECT count(*) FROM pragma_table_info('consumers') WHERE name = 'last_at'`,
	).Scan(&hasLastAt); err != nil {
		db.Close()
		return nil, fmt.Errorf("bus: inspect consumers.last_at: %w", err)
	}
	if hasLastAt == 0 {
		if _, err := db.Exec(`ALTER TABLE consumers ADD COLUMN last_at TEXT NOT NULL DEFAULT ''`); err != nil {
			db.Close()
			return nil, fmt.Errorf("bus: migrate consumers.last_at: %w", err)
		}
	}

	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_events_active ON events(archived, seq)`); err != nil {
		db.Close()
		return nil, fmt.Errorf("bus: create active index: %w", err)
	}

	return &sqliteStore{db: db}, nil
}

// backfillTypes 把旧行里单值的 type 写成 types 数组。
func backfillTypes(db *sql.DB) error {
	rows, err := db.Query(`SELECT seq, type FROM events WHERE types = '[]' AND type <> ''`)
	if err != nil {
		return fmt.Errorf("bus: query legacy types: %w", err)
	}
	type legacy struct {
		seq int64
		typ string
	}
	var rowsToFix []legacy
	for rows.Next() {
		var l legacy
		if err := rows.Scan(&l.seq, &l.typ); err != nil {
			rows.Close()
			return fmt.Errorf("bus: scan legacy type: %w", err)
		}
		rowsToFix = append(rowsToFix, l)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("bus: query legacy types: %w", err)
	}
	rows.Close()

	for _, l := range rowsToFix {
		raw, err := json.Marshal([]EventType{EventType(l.typ)})
		if err != nil {
			return fmt.Errorf("bus: encode legacy type: %w", err)
		}
		if _, err := db.Exec(`UPDATE events SET types = ? WHERE seq = ?`, string(raw), l.seq); err != nil {
			return fmt.Errorf("bus: backfill event %d types: %w", l.seq, err)
		}
	}
	return nil
}

// loadEvents 只加载未归档的事件,并返回归档水位(已归档的最大 seq)。
func (s *sqliteStore) loadEvents() ([]storedEvent, int64, error) {
	var base int64
	if err := s.db.QueryRow(
		`SELECT COALESCE(MAX(seq), 0) FROM events WHERE archived = 1`,
	).Scan(&base); err != nil {
		return nil, 0, fmt.Errorf("bus: archive floor: %w", err)
	}

	rows, err := s.db.Query(`
		SELECT seq, received_at, id, source, type, types, "time", data
		FROM events WHERE archived = 0 ORDER BY seq`)
	if err != nil {
		return nil, 0, fmt.Errorf("bus: query events: %w", err)
	}
	defer rows.Close()

	out := make([]storedEvent, 0)
	for rows.Next() {
		var (
			seq        int64
			receivedAt string
			id         string
			source     string
			legacyType string
			typesRaw   string
			occurredAt string
			data       string
		)
		if err := rows.Scan(&seq, &receivedAt, &id, &source, &legacyType, &typesRaw, &occurredAt, &data); err != nil {
			return nil, 0, fmt.Errorf("bus: scan event: %w", err)
		}

		received, err := time.Parse(time.RFC3339Nano, receivedAt)
		if err != nil {
			return nil, 0, fmt.Errorf("bus: event %d received_at: %w", seq, err)
		}
		occurred, err := time.Parse(time.RFC3339Nano, occurredAt)
		if err != nil {
			return nil, 0, fmt.Errorf("bus: event %d time: %w", seq, err)
		}

		var types []EventType
		if err := json.Unmarshal([]byte(typesRaw), &types); err != nil {
			return nil, 0, fmt.Errorf("bus: event %d types: %w", seq, err)
		}
		if len(types) == 0 && legacyType != "" {
			types = []EventType{EventType(legacyType)}
		}

		ev := Event{
			ID:     id,
			Source: source,
			Types:  types,
			Time:   occurred,
			Data:   json.RawMessage(data),
		}
		ev.Normalize()
		if len(ev.Types) == 0 {
			return nil, 0, fmt.Errorf("bus: event %d has no types", seq)
		}

		out = append(out, storedEvent{
			seq:        seq,
			receivedAt: received,
			event:      ev,
		})
	}
	return out, base, rows.Err()
}

// appendEvent 返回 false 表示 (source, id) 已存在——可能是一条已归档的事件。
func (s *sqliteStore) appendEvent(se storedEvent) (bool, error) {
	types, err := json.Marshal(se.event.Types)
	if err != nil {
		return false, fmt.Errorf("bus: encode event types: %w", err)
	}
	legacy := ""
	if len(se.event.Types) > 0 {
		legacy = string(se.event.Types[0])
	}

	res, err := s.db.Exec(`
		INSERT OR IGNORE INTO events (seq, received_at, id, source, type, types, "time", data)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		se.seq,
		se.receivedAt.Format(time.RFC3339Nano),
		se.event.ID,
		se.event.Source,
		legacy,
		string(types),
		se.event.Time.Format(time.RFC3339Nano),
		string(se.event.Data),
	)
	if err != nil {
		return false, fmt.Errorf("bus: insert event: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("bus: insert event rows: %w", err)
	}
	return n > 0, nil
}

func (s *sqliteStore) archiveEvents(upto int64) error {
	if _, err := s.db.Exec(
		`UPDATE events SET archived = 1 WHERE archived = 0 AND seq <= ?`, upto,
	); err != nil {
		return fmt.Errorf("bus: archive events: %w", err)
	}
	return nil
}

func (s *sqliteStore) loadState() (persistedState, error) {
	state := persistedState{
		Consumers: []ConsumerState{},
		Failures:  []Failure{},
	}

	rows, err := s.db.Query(`
		SELECT name, types, cursor, delivered, failed, last_error, last_event, last_at
		FROM consumers ORDER BY name`)
	if err != nil {
		return state, fmt.Errorf("bus: query consumers: %w", err)
	}
	for rows.Next() {
		var (
			cs    ConsumerState
			types string
		)
		if err := rows.Scan(&cs.Name, &types, &cs.Cursor, &cs.Delivered, &cs.Failed, &cs.LastError, &cs.LastEvent, &cs.LastAt); err != nil {
			rows.Close()
			return state, fmt.Errorf("bus: scan consumer: %w", err)
		}
		if err := json.Unmarshal([]byte(types), &cs.Types); err != nil {
			rows.Close()
			return state, fmt.Errorf("bus: consumer %q types: %w", cs.Name, err)
		}
		state.Consumers = append(state.Consumers, cs)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return state, fmt.Errorf("bus: query consumers: %w", err)
	}
	rows.Close()

	failRows, err := s.db.Query(`
		SELECT consumer, event_id, source, error, at
		FROM failures ORDER BY seq`)
	if err != nil {
		return state, fmt.Errorf("bus: query failures: %w", err)
	}
	defer failRows.Close()

	for failRows.Next() {
		var (
			f  Failure
			at string
		)
		if err := failRows.Scan(&f.Consumer, &f.EventID, &f.Source, &f.Error, &at); err != nil {
			return state, fmt.Errorf("bus: scan failure: %w", err)
		}
		f.At, err = time.Parse(time.RFC3339Nano, at)
		if err != nil {
			return state, fmt.Errorf("bus: failure time: %w", err)
		}
		state.Failures = append(state.Failures, f)
	}
	return state, failRows.Err()
}

func (s *sqliteStore) saveConsumer(c ConsumerState) error {
	types, err := json.Marshal(c.Types)
	if err != nil {
		return fmt.Errorf("bus: encode types: %w", err)
	}

	if _, err := s.db.Exec(`
		INSERT INTO consumers (name, types, cursor, delivered, failed, last_error, last_event, last_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(name) DO UPDATE SET
			types      = excluded.types,
			cursor     = excluded.cursor,
			delivered  = excluded.delivered,
			failed     = excluded.failed,
			last_error = excluded.last_error,
			last_event = excluded.last_event,
			last_at    = excluded.last_at`,
		c.Name, string(types), c.Cursor, c.Delivered, c.Failed, c.LastError, c.LastEvent, c.LastAt,
	); err != nil {
		return fmt.Errorf("bus: save consumer %q: %w", c.Name, err)
	}
	return nil
}

func (s *sqliteStore) deleteConsumer(name string) error {
	if _, err := s.db.Exec(`DELETE FROM consumers WHERE name = ?`, name); err != nil {
		return fmt.Errorf("bus: delete consumer %q: %w", name, err)
	}
	return nil
}

func (s *sqliteStore) appendFailure(f Failure) error {
	if _, err := s.db.Exec(`
		INSERT INTO failures (consumer, event_id, source, error, at)
		VALUES (?, ?, ?, ?, ?)`,
		f.Consumer, f.EventID, f.Source, f.Error, f.At.Format(time.RFC3339Nano),
	); err != nil {
		return fmt.Errorf("bus: append failure: %w", err)
	}
	return nil
}

func (s *sqliteStore) close() error { return s.db.Close() }
