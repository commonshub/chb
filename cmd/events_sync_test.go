package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/CommonsHub/chb/ical"
	"github.com/CommonsHub/chb/og"
)

func TestStoreOGResultCachesPositiveImageResult(t *testing.T) {
	runCache := &eventSyncRunCache{
		og: &eventOGCache{Entries: map[string]eventOGCacheItem{}},
	}
	url := "https://luma.com/u3kbetd4"
	result := og.FetchResult{
		URL:  url,
		Meta: og.Meta{Title: "Event", Image: "https://images.example.com/event.jpg"},
	}

	runCache.storeOGResult(url, result)

	cached, ok := runCache.getCachedOGResult(url)
	if !ok {
		t.Fatal("expected positive og:image result to be cached")
	}
	if cached.Meta.Image != result.Meta.Image {
		t.Fatalf("expected cached image %q, got %q", result.Meta.Image, cached.Meta.Image)
	}
}

func TestStoreOGResultDoesNotCacheMissingImage(t *testing.T) {
	runCache := &eventSyncRunCache{
		og: &eventOGCache{Entries: map[string]eventOGCacheItem{}},
	}
	url := "https://luma.com/u3kbetd4"
	result := og.FetchResult{
		URL:       url,
		HTMLTitle: "OpenClaworking Day",
		Meta:      og.Meta{Title: "OpenClaworking Day", Description: "Coworking day"},
	}

	runCache.storeOGResult(url, result)

	if _, ok := runCache.getCachedOGResult(url); ok {
		t.Fatal("expected missing-image result to not be cached")
	}
}

func TestStoreOGResultRemovesExistingNegativeOrStaleEntry(t *testing.T) {
	runCache := &eventSyncRunCache{
		og: &eventOGCache{
			Entries: map[string]eventOGCacheItem{
				"https://luma.com/u3kbetd4": {
					Title:     strPtr("OpenClaworking Day"),
					CheckedAt: "2026-04-15T12:00:00Z",
				},
			},
		},
	}
	url := "https://luma.com/u3kbetd4"
	result := og.FetchResult{
		URL:          url,
		ErrorKind:    "request_failed",
		ErrorMessage: "timeout",
	}

	runCache.storeOGResult(url, result)

	if _, ok := runCache.og.Entries[url]; ok {
		t.Fatal("expected non-cacheable result to remove existing cached entry")
	}
}

func TestCountICALEventsInMonthRange(t *testing.T) {
	events := []ical.Event{
		{Start: time.Date(2026, 3, 31, 23, 0, 0, 0, time.UTC)},
		{Start: time.Date(2026, 4, 1, 9, 0, 0, 0, time.UTC)},
		{Start: time.Date(2026, 4, 30, 18, 0, 0, 0, time.UTC)},
		{Start: time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC)},
	}

	got := countICALEventsInMonthRange(events, "2026-04", "2026-04")
	if got != 2 {
		t.Fatalf("countICALEventsInMonthRange() = %d, want 2", got)
	}
}

func TestCountPublicICALEventsInMonthRange(t *testing.T) {
	events := []ical.Event{
		{Start: time.Date(2026, 4, 1, 9, 0, 0, 0, time.UTC), URL: "https://luma.com/april"},
		{Start: time.Date(2026, 4, 2, 9, 0, 0, 0, time.UTC), URL: "https://calendar.google.com/event"},
		{Start: time.Date(2026, 4, 3, 9, 0, 0, 0, time.UTC)},
		{Start: time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC), URL: "https://luma.com/may"},
	}

	got := countPublicICALEventsInMonthRange(events, "2026-04", "2026-04", CalendarVisibilityAuto)
	if got != 1 {
		t.Fatalf("countPublicICALEventsInMonthRange() = %d, want 1", got)
	}
}

func TestCalendarVisibilityClassifiesEvents(t *testing.T) {
	withURL := ical.Event{Start: time.Date(2026, 4, 1, 9, 0, 0, 0, time.UTC), URL: "https://luma.com/april"}
	withoutURL := ical.Event{Start: time.Date(2026, 4, 2, 9, 0, 0, 0, time.UTC)}
	withDescriptionURL := ical.Event{Start: time.Date(2026, 4, 3, 9, 0, 0, 0, time.UTC), Description: "Join at https://example.com/event"}
	withLocationURL := ical.Event{Start: time.Date(2026, 4, 4, 9, 0, 0, 0, time.UTC), Location: "https://example.com/location"}

	if !calendarEventIsPublic(withURL, CalendarVisibilityAuto) {
		t.Fatal("auto calendar with URL should be public")
	}
	if !calendarEventIsPublic(withDescriptionURL, CalendarVisibilityAuto) {
		t.Fatal("auto calendar with description URL should be public")
	}
	if calendarEventIsPublic(withLocationURL, CalendarVisibilityAuto) {
		t.Fatal("auto calendar with only a location URL should stay private")
	}
	if !calendarEventIsBooking(withoutURL, CalendarVisibilityAuto, false) {
		t.Fatal("auto calendar without URL should be a private booking")
	}
	if calendarEventIsPublic(withURL, CalendarVisibilityPrivate) {
		t.Fatal("private calendar should not produce public events")
	}
	if calendarEventIsBooking(withURL, CalendarVisibilityPublic, false) {
		t.Fatal("public calendar should not produce private bookings")
	}
	if !calendarEventIsBooking(withURL, CalendarVisibilityPublic, true) {
		t.Fatal("room calendars should still be written as bookings")
	}
}

func TestCountCachedEventsInMonthRange(t *testing.T) {
	dataDir := t.TempDir()
	writeFullEventsFixture(t, dataDir, "2026", "03", 1)
	writeFullEventsFixture(t, dataDir, "2026", "04", 3)
	writeFullEventsFixture(t, dataDir, "2026", "05", 2)

	got := countCachedEventsInMonthRange(dataDir, "2026-04", "2026-04")
	if got != 3 {
		t.Fatalf("countCachedEventsInMonthRange(month) = %d, want 3", got)
	}

	got = countCachedEventsInMonthRange(dataDir, "2026-04", "2026-05")
	if got != 5 {
		t.Fatalf("countCachedEventsInMonthRange(range) = %d, want 5", got)
	}
}

func TestEventSyncRangeLabel(t *testing.T) {
	if got := eventSyncRangeLabel("2026-04", "2026-04"); got != "in 2026-04" {
		t.Fatalf("eventSyncRangeLabel(month) = %q", got)
	}
	if got := eventSyncRangeLabel("2026-04", "2026-06"); got != "in 2026-04 → 2026-06" {
		t.Fatalf("eventSyncRangeLabel(range) = %q", got)
	}
}

func writeFullEventsFixture(t *testing.T, dataDir, year, month string, count int) {
	t.Helper()
	events := make([]FullEvent, 0, count)
	for i := 0; i < count; i++ {
		events = append(events, FullEvent{ID: string(rune('a' + i))})
	}
	payload := FullEventsFile{
		Month:  year + "-" + month,
		Events: events,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal events fixture: %v", err)
	}
	path := filepath.Join(dataDir, year, month, "generated", "events.json")
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("mkdir events fixture: %v", err)
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("write events fixture: %v", err)
	}
}

func strPtr(s string) *string {
	return &s
}
