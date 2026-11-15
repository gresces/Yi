package bus

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestOpenPersistsSubscriptionPatterns(t *testing.T) {
	dir := t.TempDir()

	b, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.SubscribePatterns("regex", nil, []string{`^mail\.`, `^note$`}); err != nil {
		t.Fatal(err)
	}
	mustPublish(t, b, eventOf("mail", "mail.received"))
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	restored, ok := reopened.Consumer("regex")
	if !ok {
		t.Fatal("pattern consumer missing after reopen")
	}
	if got := restored.Patterns(); len(got) != 2 || got[0] != "^mail\\." || got[1] != "^note$" {
		t.Fatalf("patterns = %#v", got)
	}
	if got := restored.Stats().Patterns; len(got) != 2 {
		t.Fatalf("stats patterns = %#v", got)
	}
	m := mustNext(t, restored)
	if m.Event.ID != "mail" {
		t.Fatalf("next = %s, want mail", m.Event.ID)
	}
}

func TestOpenMigratesLegacyEventsWithoutDeleted(t *testing.T) {
	dir := t.TempDir()
	db, err := sql.Open("sqlite", filepath.Join(dir, "yi.db"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		CREATE TABLE events (
			seq         INTEGER PRIMARY KEY,
			received_at TEXT NOT NULL,
			id          TEXT NOT NULL,
			source      TEXT NOT NULL,
			type        TEXT NOT NULL,
			types       TEXT NOT NULL DEFAULT '[]',
			"time"      TEXT NOT NULL,
			data        TEXT NOT NULL,
			UNIQUE (source, id)
		)`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO events (seq, received_at, id, source, type, types, "time", data)
		VALUES (1, '2026-11-15T10:00:00Z', 'legacy', 'test', 'raw.captured',
		        '["raw.captured"]', '2026-11-15T10:00:00Z', '{"text":"x"}')`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	b, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := b.Len(); got != 1 {
		t.Fatalf("len = %d, want 1", got)
	}
	if _, err := b.DeleteEvents([]EventRef{{ID: "legacy", Source: "test"}}); err != nil {
		t.Fatal(err)
	}
	if got := b.Len(); got != 0 {
		t.Fatalf("len after delete = %d, want 0", got)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got := reopened.Len(); got != 0 {
		t.Fatalf("len after reopen = %d, want 0", got)
	}
}

func TestOpenMigratesLegacyConsumersWithoutPatterns(t *testing.T) {
	dir := t.TempDir()
	db, err := sql.Open("sqlite", filepath.Join(dir, "yi.db"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		CREATE TABLE consumers (
			name       TEXT PRIMARY KEY,
			types      TEXT NOT NULL,
			cursor     INTEGER NOT NULL DEFAULT 0,
			delivered  INTEGER NOT NULL DEFAULT 0,
			failed     INTEGER NOT NULL DEFAULT 0,
			last_error TEXT NOT NULL DEFAULT '',
			last_event TEXT NOT NULL DEFAULT '',
			last_at    TEXT NOT NULL DEFAULT ''
		)`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO consumers (name, types) VALUES ('legacy', '["raw.captured"]')`,
	); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	b, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	c, ok := b.Consumer("legacy")
	if !ok {
		t.Fatal("legacy consumer missing after migration")
	}
	if got := c.Patterns(); len(got) != 0 {
		t.Fatalf("patterns = %#v", got)
	}
	if got := c.Types(); len(got) != 1 || got[0] != TypeRawCaptured {
		t.Fatalf("types = %#v", got)
	}
	if _, err := b.SubscribePatterns("pattern", nil, []string{`^mail\.`}); err != nil {
		t.Fatal(err)
	}
}

func TestOpenPersistsEventsAndCursors(t *testing.T) {
	dir := t.TempDir()

	b, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	c, err := b.Subscribe("c", TypeRawCaptured)
	if err != nil {
		t.Fatal(err)
	}
	// 一个停在原地的消费者把水位压在 0,关闭时不会触发归档。
	if _, err := b.Subscribe("idle", TypeRawCaptured); err != nil {
		t.Fatal(err)
	}
	mustPublish(t, b, event("a"))
	mustPublish(t, b, event("b"))

	if err := mustNext(t, c).Ack(); err != nil {
		t.Fatal(err)
	}
	if err := mustNext(t, c).Nack(errors.New("bad")); err != nil {
		t.Fatal(err)
	}

	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(filepath.Join(dir, "yi.db")); err != nil {
		t.Fatalf("sqlite file missing: %v", err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	if reopened.Len() != 2 {
		t.Fatalf("len = %d, want 2", reopened.Len())
	}
	recent := reopened.Recent(0)
	if recent[0].ID != "a" || recent[1].ID != "b" {
		t.Fatalf("unexpected order: %+v", recent)
	}
	if recent[1].Data.String() != `{"text":"hello"}` {
		t.Fatalf("payload not preserved: %s", recent[1].Data)
	}

	c2, ok := reopened.Consumer("c")
	if !ok {
		t.Fatal("consumer not restored")
	}
	stats := c2.Stats()
	if stats.Cursor != 2 || stats.Delivered != 1 || stats.Failed != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
	if len(stats.Types) != 1 || stats.Types[0] != TypeRawCaptured {
		t.Fatalf("types not restored: %+v", stats)
	}
	if got := reopened.Failures(); len(got) != 1 || got[0].EventID != "b" {
		t.Fatalf("unexpected failures: %+v", got)
	}

	// 去重信息从数据库重建,重启后仍生效。
	res, err := reopened.Publish(event("a"))
	if err != nil {
		t.Fatal(err)
	}
	if !res.Duplicate {
		t.Fatal("duplicate not detected after reopen")
	}
	if reopened.Len() != 2 {
		t.Fatalf("duplicate was appended: len = %d", reopened.Len())
	}
}

func TestOpenResumesAtCursor(t *testing.T) {
	dir := t.TempDir()

	b, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	c, err := b.Subscribe("c", TypeRawCaptured)
	if err != nil {
		t.Fatal(err)
	}
	mustPublish(t, b, event("a"))
	if err := mustNext(t, c).Ack(); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	c2, _ := reopened.Consumer("c")
	mustPublish(t, reopened, event("b"))
	m := mustNext(t, c2)
	if m.Event.ID != "b" {
		t.Fatalf("got %s, want b (cursor did not persist)", m.Event.ID)
	}
}

func TestArchiveOnClose(t *testing.T) {
	dir := t.TempDir()

	b, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	c, err := b.Subscribe("c", TypeRawCaptured)
	if err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= 3; i++ {
		mustPublish(t, b, event(fmt.Sprintf("e%d", i)))
	}
	for i := 0; i < 2; i++ {
		if err := mustNext(t, c).Ack(); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	if got := reopened.Len(); got != 1 {
		t.Fatalf("active events = %d, want 1", got)
	}
	recent := reopened.Recent(0)
	if len(recent) != 1 || recent[0].ID != "e3" {
		t.Fatalf("unexpected active events: %+v", recent)
	}

	c2, ok := reopened.Consumer("c")
	if !ok {
		t.Fatal("consumer not restored")
	}
	if got := c2.Stats().Cursor; got != 2 {
		t.Fatalf("cursor = %d, want 2", got)
	}
	if m := mustNext(t, c2); m.Event.ID != "e3" {
		t.Fatalf("got %s, want e3", m.Event.ID)
	}
}

func TestArchiveRunsInBatches(t *testing.T) {
	old := archiveBatch
	archiveBatch = 1
	defer func() { archiveBatch = old }()

	dir := t.TempDir()
	b, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	c, err := b.Subscribe("c", TypeRawCaptured)
	if err != nil {
		t.Fatal(err)
	}

	mustPublish(t, b, event("a"))
	mustPublish(t, b, event("b"))

	if err := mustNext(t, c).Ack(); err != nil {
		t.Fatal(err)
	}
	if got := b.Len(); got != 1 {
		t.Fatalf("active events = %d, want 1", got)
	}
	if m := mustNext(t, c); m.Event.ID != "b" {
		t.Fatalf("got %s, want b (index base broken?)", m.Event.ID)
	}
}

func TestArchiveKeepsDedupe(t *testing.T) {
	old := archiveBatch
	archiveBatch = 1
	defer func() { archiveBatch = old }()

	dir := t.TempDir()
	b, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	c, err := b.Subscribe("c", TypeRawCaptured)
	if err != nil {
		t.Fatal(err)
	}

	mustPublish(t, b, event("a"))
	if err := mustNext(t, c).Ack(); err != nil {
		t.Fatal(err)
	}
	if got := b.Len(); got != 0 {
		t.Fatalf("active events = %d, want 0", got)
	}

	res, err := b.Publish(event("a"))
	if err != nil {
		t.Fatal(err)
	}
	if !res.Duplicate {
		t.Fatal("duplicate of archived event was not detected")
	}
	if got := b.Len(); got != 0 {
		t.Fatalf("duplicate was appended: %d", got)
	}

	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
}
