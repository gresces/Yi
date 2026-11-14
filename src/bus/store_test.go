package bus

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

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
