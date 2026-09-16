package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestEventsSinceSortsAscending(t *testing.T) {
	events := sampleEventEntries()
	filter, err := parseEventListFilter([]string{"--since", "20260110"}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}

	got := filterAndSortEvents(events, filter)
	assertEventIDs(t, got, []string{"b", "c", "d"})
}

func TestEventsUntilSortsDescending(t *testing.T) {
	events := sampleEventEntries()
	filter, err := parseEventListFilter([]string{"--until", "20260110"}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}

	got := filterAndSortEvents(events, filter)
	assertEventIDs(t, got, []string{"b", "a"})
}

func TestEventsSinceUntilSortsAscending(t *testing.T) {
	events := sampleEventEntries()
	filter, err := parseEventListFilter([]string{"--since", "20260110", "--until", "20260120"}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}

	got := filterAndSortEvents(events, filter)
	assertEventIDs(t, got, []string{"b", "c"})
}

func TestEventsSinceBeforeLatestOldestLoadsMonthlyEvents(t *testing.T) {
	dataDir := t.TempDir()
	writeEventsFixture(t, dataDir, "latest", stewardsDirName, []EventEntry{
		{ID: "latest-a", Name: "Latest A", StartAt: "2026-04-10T09:00:00Z"},
	})
	writeEventsFixture(t, dataDir, "2026", "03", []EventEntry{
		{ID: "month-a", Name: "Month A", StartAt: "2026-03-05T09:00:00Z"},
	})
	writeEventsFixture(t, dataDir, "2026", "04", []EventEntry{
		{ID: "month-b", Name: "Month B", StartAt: "2026-04-12T09:00:00Z"},
	})

	filter, err := parseEventListFilter([]string{"--since", "20260301"}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}

	got := filterAndSortEvents(loadEventsForList(dataDir, filter), filter)
	assertEventIDs(t, got, []string{"month-a", "month-b"})
}

func TestEventsSince20260401MonthlyFallbackSortsAscending(t *testing.T) {
	dataDir := t.TempDir()
	writeEventsFixture(t, dataDir, "latest", stewardsDirName, []EventEntry{
		{ID: "latest-a", Name: "Latest A", StartAt: "2026-04-29T09:00:00Z"},
	})
	writeEventsFixture(t, dataDir, "2026", "04", []EventEntry{
		{ID: "apr-29", Name: "Apr 29", StartAt: "2026-04-29T09:00:00Z"},
		{ID: "apr-10", Name: "Apr 10", StartAt: "2026-04-10T09:00:00Z"},
		{ID: "apr-20", Name: "Apr 20", StartAt: "2026-04-20T09:00:00Z"},
	})
	writeEventsFixture(t, dataDir, "2026", "05", []EventEntry{
		{ID: "may-01", Name: "May 01", StartAt: "2026-05-01T09:00:00Z"},
	})

	filter, err := parseEventListFilter([]string{"--since", "20260401"}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}

	got := filterAndSortEvents(loadEventsForList(dataDir, filter), filter)
	assertEventIDs(t, got, []string{"apr-10", "apr-20", "apr-29", "may-01"})
}

func TestEventsUntilBeforeLatestOldestLoadsMonthlyEvents(t *testing.T) {
	dataDir := t.TempDir()
	writeEventsFixture(t, dataDir, "latest", stewardsDirName, []EventEntry{
		{ID: "latest-a", Name: "Latest A", StartAt: "2026-04-10T09:00:00Z"},
	})
	writeEventsFixture(t, dataDir, "2026", "02", []EventEntry{
		{ID: "month-a", Name: "Month A", StartAt: "2026-02-05T09:00:00Z"},
	})
	writeEventsFixture(t, dataDir, "2026", "03", []EventEntry{
		{ID: "month-b", Name: "Month B", StartAt: "2026-03-05T09:00:00Z"},
	})
	writeEventsFixture(t, dataDir, "2026", "04", []EventEntry{
		{ID: "month-c", Name: "Month C", StartAt: "2026-04-12T09:00:00Z"},
	})

	filter, err := parseEventListFilter([]string{"--until", "20260331"}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}

	got := filterAndSortEvents(loadEventsForList(dataDir, filter), filter)
	assertEventIDs(t, got, []string{"month-b", "month-a"})
}

func TestEventsSinceInsideLatestUsesLatestSnapshot(t *testing.T) {
	dataDir := t.TempDir()
	writeEventsFixture(t, dataDir, "latest", stewardsDirName, []EventEntry{
		{ID: "latest-a", Name: "Latest A", StartAt: "2026-04-10T09:00:00Z"},
	})
	writeEventsFixture(t, dataDir, "2026", "04", []EventEntry{
		{ID: "month-a", Name: "Month A", StartAt: "2026-04-10T09:00:00Z"},
	})

	filter, err := parseEventListFilter([]string{"--since", "20260410"}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}

	got := filterAndSortEvents(loadEventsForList(dataDir, filter), filter)
	assertEventIDs(t, got, []string{"latest-a"})
}

func sampleEventEntries() []EventEntry {
	return []EventEntry{
		{ID: "c", Name: "Third", StartAt: "2026-01-20T09:00:00Z"},
		{ID: "a", Name: "First", StartAt: "2026-01-05T09:00:00Z"},
		{ID: "d", Name: "Fourth", StartAt: "2026-01-25T09:00:00Z"},
		{ID: "b", Name: "Second", StartAt: "2026-01-10T20:00:00Z"},
	}
}

func assertEventIDs(t *testing.T, got []EventEntry, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d events, want %d: %#v", len(got), len(want), got)
	}
	for i := range want {
		if got[i].ID != want[i] {
			t.Fatalf("event IDs = %#v, want %#v", eventIDs(got), want)
		}
	}
}

func eventIDs(events []EventEntry) []string {
	ids := make([]string, len(events))
	for i, e := range events {
		ids[i] = e.ID
	}
	return ids
}

func writeEventsFixture(t *testing.T, dataDir, year, month string, events []EventEntry) {
	t.Helper()
	dir := filepath.Join(dataDir, year, month, stewardsDirName)
	if year == "latest" {
		dir = filepath.Join(dataDir, "latest", stewardsDirName)
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	payload := EventsFile{
		Month:       year + "-" + month,
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Events:      events,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "events.json"), data, 0644); err != nil {
		t.Fatal(err)
	}
}
