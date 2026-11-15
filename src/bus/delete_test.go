package bus

import "testing"

func TestDeleteEventsHidesAndRecomputes(t *testing.T) {
	b := New()
	defer b.Close()

	c := mustSubscribe(t, b, "c", TypeRawCaptured)
	mustPublish(t, b, event("a"))
	mustPublish(t, b, event("b"))
	mustPublish(t, b, event("c"))
	if err := mustNext(t, c).Ack(); err != nil {
		t.Fatal(err)
	}

	deleted, err := b.DeleteEvents([]EventRef{{ID: "b", Source: "test"}})
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 1 {
		t.Fatalf("deleted = %d, want 1", deleted)
	}
	if got := b.Len(); got != 2 {
		t.Fatalf("len = %d, want 2", got)
	}
	if got := c.Stats().Pending; got != 1 {
		t.Fatalf("pending = %d, want 1", got)
	}

	m := mustNext(t, c)
	if m.Event.ID != "c" {
		t.Fatalf("next = %s, want c", m.Event.ID)
	}
	if err := m.Ack(); err != nil {
		t.Fatal(err)
	}

	page, total := b.Page(10, 0, "")
	if total != 2 || len(page) != 2 || page[0].ID != "a" || page[1].ID != "c" {
		t.Fatalf("page total=%d events=%+v", total, page)
	}
	if filtered, filteredTotal := b.Page(10, 0, TypeRawCaptured); filteredTotal != 2 || len(filtered) != 2 {
		t.Fatalf("filtered total=%d events=%+v", filteredTotal, filtered)
	}
	snapshot := b.Snapshot(10)
	if snapshot.Total != 2 {
		t.Fatalf("snapshot total = %d", snapshot.Total)
	}
	for _, typ := range snapshot.Types {
		if typ.Type == TypeRawCaptured && typ.Count != 2 {
			t.Fatalf("type count = %d, want 2", typ.Count)
		}
	}
	for _, producer := range snapshot.Producers {
		if producer.Source == "test" && producer.Count != 2 {
			t.Fatalf("producer count = %d, want 2", producer.Count)
		}
	}

	// Idempotent: deleting the same event again reports zero.
	deleted, err = b.DeleteEvents([]EventRef{{ID: "b", Source: "test"}})
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 0 {
		t.Fatalf("second deleted = %d, want 0", deleted)
	}
}

func TestDeleteEventDoesNotDeleteSameIDFromAnotherSource(t *testing.T) {
	b := New()
	defer b.Close()

	e1 := eventOf("same", TypeRawCaptured)
	e1.Source = "one"
	e2 := eventOf("same", TypeRawCaptured)
	e2.Source = "two"
	mustPublish(t, b, e1)
	mustPublish(t, b, e2)

	deleted, err := b.DeleteEvents([]EventRef{{ID: "same", Source: "one"}})
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 1 {
		t.Fatalf("deleted = %d, want 1", deleted)
	}
	page, total := b.Page(10, 0, "")
	if total != 1 || len(page) != 1 || page[0].Source != "two" {
		t.Fatalf("page total=%d events=%+v", total, page)
	}
}

func TestDeleteEventsClearsInflight(t *testing.T) {
	b := New()
	defer b.Close()

	c := mustSubscribe(t, b, "c", TypeRawCaptured)
	mustPublish(t, b, event("a"))
	m := mustNext(t, c) // inflight "a"

	if _, err := b.DeleteEvents([]EventRef{{ID: "a", Source: "test"}}); err != nil {
		t.Fatal(err)
	}
	if _, ok := c.Inflight(); ok {
		t.Fatal("deleted inflight event should be cleared")
	}
	// Deleting an inflight event must not deadlock or leave a ghost message.
	if err := m.Ack(); err != nil {
		t.Fatal(err)
	}
	if _, ok := c.Inflight(); ok {
		t.Fatal("ack after delete should be a no-op")
	}
}

func TestDeleteProducersAndConsumersBatch(t *testing.T) {
	b := New()
	defer b.Close()

	one := eventOf("a", TypeRawCaptured)
	one.Source = "one"
	two := eventOf("b", TypeRawCaptured)
	two.Source = "two"
	mustPublish(t, b, one)
	mustPublish(t, b, two)
	mustSubscribe(t, b, "c1", TypeRawCaptured)
	mustSubscribe(t, b, "c2", TypeRawCaptured)

	deleted, err := b.DeleteProducers([]string{"one", "missing"})
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 1 {
		t.Fatalf("producer deleted = %d, want 1", deleted)
	}
	if got := b.Len(); got != 1 {
		t.Fatalf("len = %d, want 1", got)
	}
	snapshot := b.Snapshot(10)
	for _, producer := range snapshot.Producers {
		if producer.Source == "one" {
			t.Fatalf("producer one still present: %+v", snapshot.Producers)
		}
	}

	deleted, err = b.DeleteConsumers([]string{"c1", "c2", "c1", "missing"})
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 2 {
		t.Fatalf("consumer deleted = %d, want 2", deleted)
	}
	if _, ok := b.Consumer("c1"); ok {
		t.Fatal("c1 still registered")
	}
	if _, ok := b.Consumer("c2"); ok {
		t.Fatal("c2 still registered")
	}
}

func TestArchiveAfterDeleteKeepsActiveCount(t *testing.T) {
	SetArchiveBatch(1)
	defer SetArchiveBatch(256)

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
	if _, err := b.DeleteEvents([]EventRef{{ID: "a", Source: "test"}}); err != nil {
		t.Fatal(err)
	}
	if err := mustNext(t, c).Ack(); err != nil {
		t.Fatal(err)
	}
	if got := b.Len(); got != 0 {
		t.Fatalf("len after archive = %d, want 0", got)
	}
	if page, total := b.Page(10, 0, ""); total != 0 || len(page) != 0 {
		t.Fatalf("page total=%d events=%+v", total, page)
	}
	if got := len(b.Snapshot(10).Producers); got != 0 {
		t.Fatalf("producers after archive = %d", got)
	}
}

func TestDeletedEventsPersistAsHidden(t *testing.T) {
	dir := t.TempDir()
	b, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Subscribe("c", TypeRawCaptured); err != nil {
		t.Fatal(err)
	}
	mustPublish(t, b, event("a"))
	mustPublish(t, b, event("b"))
	if _, err := b.DeleteEvents([]EventRef{{ID: "a", Source: "test"}}); err != nil {
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
	if got := reopened.Len(); got != 1 {
		t.Fatalf("len after reopen = %d, want 1", got)
	}
	page, total := reopened.Page(10, 0, "")
	if total != 1 || len(page) != 1 || page[0].ID != "b" {
		t.Fatalf("page total=%d events=%+v", total, page)
	}
	c, ok := reopened.Consumer("c")
	if !ok {
		t.Fatal("consumer missing after reopen")
	}
	if got := c.Stats().Pending; got != 1 {
		t.Fatalf("pending after reopen = %d, want 1", got)
	}
}
