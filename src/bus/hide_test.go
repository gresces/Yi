package bus

import "testing"

func eventFrom(source, id string) Event {
	e := eventOf(id, TypeRawCaptured)
	e.Source = source
	return e
}

func producerCount(t *testing.T, b *Bus, source string) int {
	t.Helper()
	for _, producer := range b.Snapshot(10).Producers {
		if producer.Source == source {
			return producer.Count
		}
	}
	return 0
}

func TestHideProducersKeepsEvents(t *testing.T) {
	b := New()
	defer b.Close()

	mustPublish(t, b, eventFrom("one", "a"))
	mustPublish(t, b, eventFrom("one", "b"))
	mustPublish(t, b, eventFrom("two", "c"))

	hidden, err := b.HideProducers([]string{"one", "one", "missing"})
	if err != nil {
		t.Fatal(err)
	}
	if hidden != 1 {
		t.Fatalf("hidden = %d, want 1", hidden)
	}

	// 事件一条都没少。
	if got := b.Len(); got != 3 {
		t.Fatalf("len = %d, want 3", got)
	}
	page, total := b.Page(10, 0, "")
	if total != 3 || len(page) != 3 {
		t.Fatalf("page total=%d events=%d", total, len(page))
	}
	if got := producerCount(t, b, "one"); got != 0 {
		t.Fatalf("producer one still listed with %d events", got)
	}
	if got := producerCount(t, b, "two"); got != 1 {
		t.Fatalf("producer two = %d, want 1", got)
	}

	// 再次发布即恢复。
	mustPublish(t, b, eventFrom("one", "d"))
	if got := producerCount(t, b, "one"); got != 3 {
		t.Fatalf("producer one after publish = %d, want 3", got)
	}
	if got := b.Len(); got != 4 {
		t.Fatalf("len = %d, want 4", got)
	}
}

func TestHideProducersSkipsUnknownSource(t *testing.T) {
	b := New()
	defer b.Close()

	mustPublish(t, b, eventFrom("one", "a"))

	hidden, err := b.HideProducers([]string{"", "missing"})
	if err != nil {
		t.Fatal(err)
	}
	if hidden != 0 {
		t.Fatalf("hidden = %d, want 0", hidden)
	}
	if got := producerCount(t, b, "one"); got != 1 {
		t.Fatalf("producer one = %d, want 1", got)
	}
}

func TestHideProducersPersists(t *testing.T) {
	dir := t.TempDir()

	b, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	mustPublish(t, b, eventFrom("one", "a"))
	mustPublish(t, b, eventFrom("two", "b"))
	if _, err := b.HideProducers([]string{"one"}); err != nil {
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

	if got := reopened.Len(); got != 2 {
		t.Fatalf("len after reopen = %d, want 2", got)
	}
	if got := producerCount(t, reopened, "one"); got != 0 {
		t.Fatalf("producer one reappeared after reopen: %d", got)
	}
	if got := producerCount(t, reopened, "two"); got != 1 {
		t.Fatalf("producer two = %d, want 1", got)
	}
}
