package cmd

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/nbd-wtf/go-nostr"
)

func TestNostrOutboxReplacesQueuedURIAndCountsAsPublished(t *testing.T) {
	appDir := t.TempDir()
	t.Setenv("APP_DATA_DIR", appDir)

	first := &nostr.Event{ID: "event-one", Kind: 1111}
	second := &nostr.Event{ID: "event-two", Kind: 1111}
	if err := enqueueSignedNostrEvent("stripe:txn:txn_1", first); err != nil {
		t.Fatalf("enqueue first: %v", err)
	}
	if err := enqueueSignedNostrEvent("stripe:txn:txn_1", second); err != nil {
		t.Fatalf("enqueue replacement: %v", err)
	}

	if _, err := os.Stat(nostrEventQueuePath(nostrOutboxDir(), first.ID)); !os.IsNotExist(err) {
		t.Fatalf("first queued event should have been replaced, stat err=%v", err)
	}
	if _, err := os.Stat(nostrEventQueuePath(nostrOutboxDir(), second.ID)); err != nil {
		t.Fatalf("second queued event missing: %v", err)
	}

	published := loadPublishedEventIDs()
	if !published["stripe:txn:txn_1"] {
		t.Fatalf("queued URI should count as pending/published: %#v", published)
	}
}

func TestMarkQueuedNostrEventSentMovesFile(t *testing.T) {
	appDir := t.TempDir()
	t.Setenv("APP_DATA_DIR", appDir)

	ev := &nostr.Event{ID: "event-sent", Kind: 1111}
	if err := enqueueSignedNostrEvent("stripe:txn:txn_1", ev); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if err := markQueuedNostrEventSent(ev.ID, []string{"wss://relay.example"}); err != nil {
		t.Fatalf("mark sent: %v", err)
	}

	if _, err := os.Stat(nostrEventQueuePath(nostrOutboxDir(), ev.ID)); !os.IsNotExist(err) {
		t.Fatalf("outbox file should be removed, stat err=%v", err)
	}
	if _, err := os.Stat(nostrEventQueuePath(nostrSentDir(), ev.ID)); err != nil {
		t.Fatalf("sent file missing: %v", err)
	}

	item, err := readQueuedNostrEvent(nostrEventQueuePath(nostrSentDir(), ev.ID))
	if err != nil {
		t.Fatalf("read sent event: %v", err)
	}
	if item.URI != "stripe:txn:txn_1" || item.SentAt == "" || len(item.Relays) != 1 {
		t.Fatalf("sent item = %#v", item)
	}
}

func TestFlushNostrOutboxWithoutKeysLeavesQueued(t *testing.T) {
	appDir := t.TempDir()
	t.Setenv("APP_DATA_DIR", appDir)

	ev := &nostr.Event{ID: "event-offline", Kind: 1111}
	if err := enqueueSignedNostrEvent("stripe:txn:txn_1", ev); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	result := FlushNostrOutbox()
	if result.Queued != 1 || result.Published != 0 || result.Failed != 1 || result.Err == nil {
		t.Fatalf("flush result = %#v", result)
	}
	if _, err := os.Stat(filepath.Join(nostrOutboxDir(), "event-offline.json")); err != nil {
		t.Fatalf("outbox file should remain: %v", err)
	}
}
