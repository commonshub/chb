package cmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunDoctorChecksHealthyData(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	dataDir := filepath.Join(home, ".chb", "data")

	if err := os.MkdirAll(filepath.Join(home, ".chb"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, ".chb", "settings.json"), []byte(`{"discord":{"guildId":"g","roles":{},"channels":{}}}`), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, ".chb", "rooms.json"), []byte(`{"rooms":[{"id":"ostrom","slug":"ostrom","discordChannelId":"1443322327159803945"}]}`), 0644); err != nil {
		t.Fatal(err)
	}

	latestChannelDir := filepath.Join(dataDir, "latest", "sources", "discord", "1443322327159803945")
	if err := os.MkdirAll(latestChannelDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(latestChannelDir, "messages.json"), []byte(`{"messages":[]}`), 0644); err != nil {
		t.Fatal(err)
	}

	monthDir := filepath.Join(dataDir, "2026", "04")
	if err := os.MkdirAll(filepath.Join(monthDir, "sources", "discord", "chan-1"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(monthDir, "sources", "discord", "chan-1", "messages.json"), []byte(`{"messages":[{}]}`), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(monthDir, "generated"), 0755); err != nil {
		t.Fatal(err)
	}
	imageRel := "2026/04/sources/discord/images/att-1.png"
	if err := os.MkdirAll(filepath.Join(dataDir, "2026", "04", "sources", "discord", "images"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, filepath.FromSlash(imageRel)), []byte("png"), 0644); err != nil {
		t.Fatal(err)
	}
	imagesJSON := `{
	  "images": [
	    {
	      "id": "att-1",
	      "url": "https://cdn.discordapp.com/file.png",
	      "filePath": "2026/04/sources/discord/images/att-1.png",
	      "timestamp": "2026-04-13T12:00:00.000000+00:00"
	    }
	  ]
	}`
	if err := os.WriteFile(filepath.Join(monthDir, "generated", "images.json"), []byte(imagesJSON), 0644); err != nil {
		t.Fatal(err)
	}
	monthEventsJSON := `{
	  "events": [
	    {
	      "id": "event-1",
	      "name": "Healthy Event",
	      "description": "A complete homepage-ready event description with enough detail to show on the card.",
	      "startAt": "2099-04-13T12:00:00Z",
	      "url": "https://lu.ma/healthy-event",
	      "coverImage": "https://images.luma.com/cover.jpg",
	      "coverImageLocal": "2026/04/generated/events/images/event-1.jpg"
	    }
	  ]
	}`
	if err := os.WriteFile(filepath.Join(monthDir, "generated", "events.json"), []byte(monthEventsJSON), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dataDir, "latest", "generated"), 0755); err != nil {
		t.Fatal(err)
	}
	latestEventsJSON := `{
	  "count": 1,
	  "events": [
	    {
	      "id": "event-1",
	      "name": "Healthy Event",
	      "description": "A complete homepage-ready event description with enough detail to show on the card.",
	      "startAt": "2099-04-13T12:00:00Z",
	      "url": "https://lu.ma/healthy-event",
	      "coverImage": "https://images.luma.com/cover.jpg",
	      "coverImageLocal": "2026/04/generated/events/images/event-1.jpg"
	    }
	  ]
	}`
	if err := os.MkdirAll(filepath.Join(dataDir, "2026", "04", "generated", "events", "images"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "2026", "04", "generated", "events", "images", "event-1.jpg"), []byte("jpg"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "latest", "generated", "events.json"), []byte(latestEventsJSON), 0644); err != nil {
		t.Fatal(err)
	}

	report := runDoctorChecks(dataDir)
	if len(report.Findings) != 0 {
		t.Fatalf("expected no findings, got %+v", report.Findings)
	}
	if report.ImagesChecked != 1 {
		t.Fatalf("expected 1 image checked, got %d", report.ImagesChecked)
	}
}

func TestRunDoctorChecksFindsMissingRoomChannelDir(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	dataDir := filepath.Join(home, ".chb", "data")

	if err := os.MkdirAll(filepath.Join(home, ".chb"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, ".chb", "settings.json"), []byte(`{"discord":{"guildId":"g","roles":{},"channels":{}}}`), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, ".chb", "rooms.json"), []byte(`{"rooms":[{"id":"ostrom","slug":"ostrom","discordChannelId":"1443322327159803945"}]}`), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		t.Fatal(err)
	}

	report := runDoctorChecks(dataDir)
	if len(report.Findings) == 0 {
		t.Fatal("expected findings")
	}
	if !containsDoctorMessage(report.Findings, "missing Discord channel directory") {
		t.Fatalf("expected missing room channel finding, got %+v", report.Findings)
	}
}

func TestRunDoctorChecksFindsBrokenImagesJSON(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	dataDir := filepath.Join(home, ".chb", "data")

	if err := os.MkdirAll(filepath.Join(home, ".chb"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, ".chb", "settings.json"), []byte(`{"discord":{"guildId":"g","roles":{},"channels":{}}}`), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, ".chb", "rooms.json"), []byte(`{"rooms":[]}`), 0644); err != nil {
		t.Fatal(err)
	}

	scopeDir := filepath.Join(dataDir, "latest")
	if err := os.MkdirAll(filepath.Join(scopeDir, "generated"), 0755); err != nil {
		t.Fatal(err)
	}
	badJSON := `{
	  "images": [
	    {
	      "id": "att-1",
	      "url": "",
	      "filePath": "latest/sources/discord/images/att-1.png",
	      "timestamp": "2026-04-13T12:00:00.000000+00:00",
	      "proxyUrl": "/api/discord-image-proxy"
	    }
	  ],
	  "message": "\u003ctag\u003e"
	}`
	if err := os.WriteFile(filepath.Join(scopeDir, "generated", "images.json"), []byte(badJSON), 0644); err != nil {
		t.Fatal(err)
	}

	report := runDoctorChecks(dataDir)
	if len(report.Findings) == 0 {
		t.Fatal("expected findings")
	}
	wantSubs := []string{
		"deprecated proxyUrl",
		"escaped unicode sequences",
		"missing url",
		"uses latest/",
		"non-canonical filePath/localPath",
		"references missing local file",
	}
	for _, sub := range wantSubs {
		if !containsDoctorMessage(report.Findings, sub) {
			t.Fatalf("expected finding containing %q, got %+v", sub, report.Findings)
		}
	}
}

func TestRunDoctorChecksIgnoresRoomICSOnlyMonth(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	dataDir := filepath.Join(home, ".chb", "data")

	if err := os.MkdirAll(filepath.Join(home, ".chb"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, ".chb", "settings.json"), []byte(`{"discord":{"guildId":"g","roles":{},"channels":{}}}`), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, ".chb", "rooms.json"), []byte(`{"rooms":[]}`), 0644); err != nil {
		t.Fatal(err)
	}

	icsDir := filepath.Join(dataDir, "2026", "07", "sources", "ics")
	if err := os.MkdirAll(icsDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(icsDir, "ostrom.ics"), []byte("BEGIN:VCALENDAR"), 0644); err != nil {
		t.Fatal(err)
	}

	report := runDoctorChecks(dataDir)
	if containsDoctorMessage(report.Findings, "generated/events.json is missing") {
		t.Fatalf("expected no events.json finding for room ICS only month, got %+v", report.Findings)
	}
}

func TestRunDoctorChecksFindsBrokenHomepageLatestEvents(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	dataDir := filepath.Join(home, ".chb", "data")

	if err := os.MkdirAll(filepath.Join(home, ".chb"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, ".chb", "settings.json"), []byte(`{"discord":{"guildId":"g","roles":{},"channels":{}}}`), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, ".chb", "rooms.json"), []byte(`{"rooms":[]}`), 0644); err != nil {
		t.Fatal(err)
	}

	monthDir := filepath.Join(dataDir, "2026", "04", "generated")
	if err := os.MkdirAll(monthDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(monthDir, "events.json"), []byte(`{"events":[{"id":"monthly-1"}]}`), 0644); err != nil {
		t.Fatal(err)
	}

	latestDir := filepath.Join(dataDir, "latest", "generated")
	if err := os.MkdirAll(latestDir, 0755); err != nil {
		t.Fatal(err)
	}
	badLatest := `{
	  "count": 1,
	  "events": [
	    {
	      "id": "event-1",
	      "name": "",
	      "description": "Get up-to-date information at: https://lu.ma/event-1",
	      "startAt": "2099-04-13T12:00:00Z",
	      "url": "https://lu.ma/event-1",
	      "coverImage": "",
	      "coverImageLocal": "2026/04/generated/events/images/missing.jpg"
	    }
	  ]
	}`
	if err := os.WriteFile(filepath.Join(latestDir, "events.json"), []byte(badLatest), 0644); err != nil {
		t.Fatal(err)
	}

	report := runDoctorChecks(dataDir)
	wantSubs := []string{
		"missing title",
		"missing coverImage",
		"missing local cover image",
		"has a thin description",
	}
	for _, sub := range wantSubs {
		if !containsDoctorMessage(report.Findings, sub) {
			t.Fatalf("expected finding containing %q, got %+v", sub, report.Findings)
		}
	}
}

func TestRunDoctorChecksFindsMissingLatestEventsFileWhenMonthlyEventsExist(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	dataDir := filepath.Join(home, ".chb", "data")

	if err := os.MkdirAll(filepath.Join(home, ".chb"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, ".chb", "settings.json"), []byte(`{"discord":{"guildId":"g","roles":{},"channels":{}}}`), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, ".chb", "rooms.json"), []byte(`{"rooms":[]}`), 0644); err != nil {
		t.Fatal(err)
	}

	monthDir := filepath.Join(dataDir, "2026", "04", "generated")
	if err := os.MkdirAll(monthDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(monthDir, "events.json"), []byte(`{"events":[{"id":"monthly-1","name":"Test"}]}`), 0644); err != nil {
		t.Fatal(err)
	}

	report := runDoctorChecks(dataDir)
	if !containsDoctorMessage(report.Findings, "latest/generated/events.json is missing") {
		t.Fatalf("expected missing latest events finding, got %+v", report.Findings)
	}
}

func containsDoctorMessage(findings []doctorFinding, sub string) bool {
	for _, finding := range findings {
		if strings.Contains(finding.Message, sub) {
			return true
		}
	}
	return false
}
