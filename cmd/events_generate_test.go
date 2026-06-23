package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFormatEventTimeBrusselsHardcodesBrusselsOffset(t *testing.T) {
	utc := time.Date(2026, 7, 1, 17, 0, 0, 0, time.UTC)
	if got := formatEventTimeBrussels(utc); got != "2026-07-01T19:00:00+02:00" {
		t.Fatalf("formatEventTimeBrussels() = %q", got)
	}
}

func TestParseEventTimeBrusselsHardcodesBrusselsOffset(t *testing.T) {
	got := parseEventTimeBrussels("2026-01-01T12:00:00Z")
	if got.Location().String() != TIMEZONE {
		t.Fatalf("location = %q, want %q", got.Location(), TIMEZONE)
	}
	if got.Format(time.RFC3339) != "2026-01-01T13:00:00+01:00" {
		t.Fatalf("time = %s", got.Format(time.RFC3339))
	}
}

func TestGenerateEventsWritesLatestMarkdown(t *testing.T) {
	dataDir := t.TempDir()
	t.Setenv("DATA_DIR", dataDir)

	eventsDir := filepath.Join(dataDir, "2099", "01", "generated")
	if err := os.MkdirAll(eventsDir, 0755); err != nil {
		t.Fatalf("mkdir events dir: %v", err)
	}
	eventsJSON := `{
	  "month": "2099-01",
	  "events": [{
	    "id": "evt-1",
	    "name": "Future Brussels Event",
	    "description": "A generated event.",
	    "startAt": "2099-01-10T19:00:00+01:00",
	    "endAt": "2099-01-10T21:00:00+01:00",
	    "location": "Commons Hub Brussels",
	    "url": "https://example.com/event"
	  }]
	}`
	if err := os.WriteFile(filepath.Join(eventsDir, "events.json"), []byte(eventsJSON), 0644); err != nil {
		t.Fatalf("write events fixture: %v", err)
	}

	if err := GenerateEvents(nil); err != nil {
		t.Fatalf("GenerateEvents: %v", err)
	}

	mdPath := filepath.Join(dataDir, "latest", "generated", "events.md")
	data, err := os.ReadFile(mdPath)
	if err != nil {
		t.Fatalf("expected latest generated events.md: %v", err)
	}
	if !strings.Contains(string(data), "Future Brussels Event") {
		t.Fatalf("events.md missing event:\n%s", data)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "generated")); !os.IsNotExist(err) {
		t.Fatalf("GenerateEvents should not create DATA_DIR/generated, got err=%v", err)
	}
}

func TestGenerateEventsWritesLatestJSONFromSameUpcomingSetAsMarkdown(t *testing.T) {
	dataDir := t.TempDir()
	t.Setenv("DATA_DIR", dataDir)

	eventsDir := filepath.Join(dataDir, "2099", "02", "generated")
	if err := os.MkdirAll(eventsDir, 0755); err != nil {
		t.Fatalf("mkdir events dir: %v", err)
	}
	eventsJSON := `{
	  "month": "2099-02",
	  "events": [{
	    "id": "evt-with-url",
	    "name": "Future Event With URL",
	    "startAt": "2099-02-10T19:00:00+01:00",
	    "url": "https://example.com/event"
	  }, {
	    "id": "evt-without-url",
	    "name": "Future Event Without URL",
	    "startAt": "2099-02-11T19:00:00+01:00"
	  }]
	}`
	if err := os.WriteFile(filepath.Join(eventsDir, "events.json"), []byte(eventsJSON), 0644); err != nil {
		t.Fatalf("write events fixture: %v", err)
	}

	if err := GenerateEvents(nil); err != nil {
		t.Fatalf("GenerateEvents: %v", err)
	}

	latestData, err := os.ReadFile(filepath.Join(dataDir, "latest", "generated", "events.json"))
	if err != nil {
		t.Fatalf("read latest events.json: %v", err)
	}
	var latest LatestEventsFile
	if err := json.Unmarshal(latestData, &latest); err != nil {
		t.Fatalf("parse latest events.json: %v", err)
	}
	if latest.Count != 2 || len(latest.Events) != 2 {
		t.Fatalf("latest events.json count=%d len=%d, want 2; payload=%s", latest.Count, len(latest.Events), latestData)
	}

	mdData, err := os.ReadFile(filepath.Join(dataDir, "latest", "generated", "events.md"))
	if err != nil {
		t.Fatalf("read events.md: %v", err)
	}
	md := string(mdData)
	for _, want := range []string{"Future Event With URL", "Future Event Without URL"} {
		if !strings.Contains(md, want) {
			t.Fatalf("events.md missing %q:\n%s", want, md)
		}
	}
}
