package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/nbd-wtf/go-nostr"
)

type queuedNostrEvent struct {
	URI       string      `json:"uri,omitempty"`
	Event     nostr.Event `json:"event"`
	QueuedAt  string      `json:"queuedAt"`
	SentAt    string      `json:"sentAt,omitempty"`
	Relays    []string    `json:"relays,omitempty"`
	LastError string      `json:"lastError,omitempty"`
	Attempts  int         `json:"attempts,omitempty"`
}

type nostrOutboxResult struct {
	Queued    int
	Published int
	Failed    int
	Relays    int
	Err       error
}

func nostrQueueDir(kind string) string {
	return filepath.Join(AppDataDir(), "nostr", kind)
}

func nostrOutboxDir() string {
	return nostrQueueDir("outbox")
}

func nostrSentDir() string {
	return nostrQueueDir("sent")
}

func nostrEventQueuePath(dir, eventID string) string {
	return filepath.Join(dir, sanitizeNostrEventFileName(eventID)+".json")
}

func sanitizeNostrEventFileName(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "event"
	}
	var b strings.Builder
	for _, r := range s {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' {
			b.WriteRune(r)
		}
	}
	if b.Len() == 0 {
		return "event"
	}
	return b.String()
}

func enqueueSignedNostrEvent(uri string, ev *nostr.Event) error {
	if ev == nil || ev.ID == "" {
		return fmt.Errorf("cannot enqueue unsigned Nostr event")
	}
	if err := os.MkdirAll(nostrOutboxDir(), 0755); err != nil {
		return err
	}
	if uri != "" {
		_ = removeQueuedNostrEventsForURI(uri)
	}
	item := queuedNostrEvent{
		URI:      uri,
		Event:    *ev,
		QueuedAt: time.Now().UTC().Format(time.RFC3339),
	}
	data, err := json.MarshalIndent(item, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(nostrEventQueuePath(nostrOutboxDir(), ev.ID), data, 0644)
}

func removeQueuedNostrEventsForURI(uri string) error {
	entries, err := os.ReadDir(nostrOutboxDir())
	if err != nil {
		return nil
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		path := filepath.Join(nostrOutboxDir(), entry.Name())
		item, err := readQueuedNostrEvent(path)
		if err == nil && item.URI == uri {
			_ = os.Remove(path)
		}
	}
	return nil
}

func readQueuedNostrEvent(path string) (queuedNostrEvent, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return queuedNostrEvent{}, err
	}
	var item queuedNostrEvent
	if err := json.Unmarshal(data, &item); err != nil {
		return queuedNostrEvent{}, err
	}
	return item, nil
}

func writeQueuedNostrEvent(path string, item queuedNostrEvent) error {
	data, err := json.MarshalIndent(item, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

func moveQueuedNostrEventToSent(path string, item queuedNostrEvent, relays []string) error {
	if err := os.MkdirAll(nostrSentDir(), 0755); err != nil {
		return err
	}
	item.SentAt = time.Now().UTC().Format(time.RFC3339)
	item.Relays = relays
	item.LastError = ""
	data, err := json.MarshalIndent(item, "", "  ")
	if err != nil {
		return err
	}
	sentPath := nostrEventQueuePath(nostrSentDir(), item.Event.ID)
	if err := os.WriteFile(sentPath, data, 0644); err != nil {
		return err
	}
	return os.Remove(path)
}

func loadQueuedNostrEventURIs() map[string]bool {
	uris := map[string]bool{}
	addNostrEventURIsFromDir(uris, nostrOutboxDir())
	return uris
}

func loadSentNostrEventURIs() map[string]bool {
	uris := map[string]bool{}
	addNostrEventURIsFromDir(uris, nostrSentDir())
	return uris
}

func addNostrEventURIsFromDir(uris map[string]bool, dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		item, err := readQueuedNostrEvent(filepath.Join(dir, entry.Name()))
		if err == nil && item.URI != "" {
			uris[item.URI] = true
		}
	}
}

func publishNostrEventWithOutbox(keys *NostrKeys, uri string, ev *nostr.Event) ([]string, error) {
	if keys == nil {
		return nil, fmt.Errorf("no Nostr identity configured")
	}
	SignNostrEvent(keys, ev)
	if err := enqueueSignedNostrEvent(uri, ev); err != nil {
		return nil, err
	}
	accepted, err := PublishSignedNostrEvent(keys, ev)
	if err != nil {
		return nil, err
	}
	if err := markQueuedNostrEventSent(ev.ID, accepted); err != nil {
		return accepted, err
	}
	return accepted, nil
}

func markQueuedNostrEventSent(eventID string, relays []string) error {
	path := nostrEventQueuePath(nostrOutboxDir(), eventID)
	item, err := readQueuedNostrEvent(path)
	if err != nil {
		return err
	}
	return moveQueuedNostrEventToSent(path, item, relays)
}

func FlushNostrOutbox() nostrOutboxResult {
	keys := LoadNostrKeys()
	result := nostrOutboxResult{}
	if keys != nil {
		if len(keys.Relays) > 0 {
			result.Relays = len(keys.Relays)
		} else {
			result.Relays = len(nostrRelays)
		}
	}
	entries, err := os.ReadDir(nostrOutboxDir())
	if err != nil {
		return result
	}
	result.Queued = len(entries)
	if result.Queued == 0 {
		return result
	}
	if keys == nil {
		result.Failed = result.Queued
		result.Err = fmt.Errorf("no Nostr identity configured")
		return result
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			result.Queued--
			continue
		}
		path := filepath.Join(nostrOutboxDir(), entry.Name())
		item, err := readQueuedNostrEvent(path)
		if err != nil {
			result.Failed++
			if result.Err == nil {
				result.Err = err
			}
			continue
		}
		item.Attempts++
		accepted, err := PublishSignedNostrEvent(keys, &item.Event)
		if err != nil {
			result.Failed++
			item.LastError = err.Error()
			_ = writeQueuedNostrEvent(path, item)
			if result.Err == nil {
				result.Err = err
			}
			continue
		}
		if err := moveQueuedNostrEventToSent(path, item, accepted); err != nil {
			result.Failed++
			if result.Err == nil {
				result.Err = err
			}
			continue
		}
		result.Published++
	}
	return result
}

func flushNostrOutboxWithStatus() error {
	result := FlushNostrOutbox()
	if result.Queued == 0 {
		return nil
	}
	fmt.Printf("  %sOutbox:%s %d queued, %d sent", Fmt.Dim, Fmt.Reset, result.Queued, result.Published)
	if result.Failed > 0 {
		fmt.Printf(", %s%d failed%s", Fmt.Red, result.Failed, Fmt.Reset)
	}
	fmt.Println()
	if result.Err != nil && result.Published == 0 {
		return result.Err
	}
	return nil
}
