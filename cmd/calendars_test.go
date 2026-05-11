package cmd

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestCalendarsShowsSummaryAndOptionalMonthlyBreakdown(t *testing.T) {
	home := t.TempDir()
	dataDir := filepath.Join(home, "data")
	t.Setenv("APP_DATA_DIR", filepath.Join(home, "app"))
	t.Setenv("DATA_DIR", dataDir)

	writeJSONFixture(t, filepath.Join(home, "app", "settings", "settings.json"), `{}`)
	writeJSONFixture(t, filepath.Join(home, "app", "settings", "calendars.json"), `{
	  "sources": [
	    {"slug":"ostrom","name":"Ostrom","provider":"ics","url":"https://example.com/ostrom.ics","visibility":"private","room":"ostrom"},
	    {"slug":"luma","name":"Luma","provider":"ics","url":"https://example.com/luma.ics","visibility":"public"},
	    {"slug":"privatecal","name":"Private Calendar","provider":"ics","url":"https://example.com/private.ics","visibility":"private"}
	  ]
	}`)
	writeJSONFixture(t, filepath.Join(home, "app", "settings", "rooms.json"), `{
	  "rooms": [{
	    "name": "Ostrom",
	    "slug": "ostrom",
	    "calendar": "ostrom"
	  }]
	}`)
	writeJSONFixture(t, filepath.Join(dataDir, "2026", "04", "sources", "ics", "ostrom.ics"), `BEGIN:VCALENDAR
VERSION:2.0
BEGIN:VEVENT
UID:room-private-1
SUMMARY:Room private 1
DTSTART:20260410T100000Z
END:VEVENT
BEGIN:VEVENT
UID:room-private-2
SUMMARY:Room private 2
DTSTART:20260411T100000Z
END:VEVENT
BEGIN:VEVENT
UID:room-public-1
SUMMARY:Room public
DTSTART:20260412T100000Z
END:VEVENT
END:VCALENDAR`)
	writeJSONFixture(t, filepath.Join(dataDir, "2026", "04", "sources", "ics", "privatecal.ics"), `BEGIN:VCALENDAR
VERSION:2.0
BEGIN:VEVENT
UID:private-source-1
SUMMARY:Private source
DTSTART:20260413T100000Z
END:VEVENT
END:VCALENDAR`)
	writeJSONFixture(t, filepath.Join(dataDir, "2026", "04", "generated", "events.json"), `{
	  "events": [
	    {"id":"room-public-1","name":"Room public","calendarSource":"ostrom","startAt":"2026-04-12T10:00:00Z"},
	    {"id":"luma-1","name":"Luma 1","calendarSource":"luma","startAt":"2026-04-14T10:00:00Z"},
	    {"id":"luma-2","name":"Luma 2","calendarSource":"luma","startAt":"2026-04-15T10:00:00Z"}
	  ]
	}`)
	writeJSONFixture(t, filepath.Join(dataDir, "2026", "05", "sources", "ics", "ostrom.ics"), `BEGIN:VCALENDAR
VERSION:2.0
BEGIN:VEVENT
UID:room-private-may
SUMMARY:Room private May
DTSTART:20260510T100000Z
END:VEVENT
END:VCALENDAR`)

	out := captureStdout(t, func() {
		Calendars(nil)
	})

	for _, want := range []string{
		"Calendars",
		"7 events between 2026-04-01 and 2026-05-31 across 3 calendars.",
		"PROVIDER",
		"VISIBILITY",
		"Ostrom",
		"ostrom",
		"ics",
		"auto",
		"Luma",
		"luma",
		"public",
		"Private Calendar",
		"privatecal",
		"private",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("calendar output missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "public   1  private   2") {
		t.Fatalf("calendar output should not show monthly breakdown by default:\n%s", out)
	}

	monthlyOut := captureStdout(t, func() {
		Calendars([]string{"--months"})
	})
	for _, want := range []string{
		"2026-04",
		"public   1  private   2",
		"public   2  private   0",
		"public   0  private   1",
	} {
		if !strings.Contains(monthlyOut, want) {
			t.Fatalf("calendar monthly output missing %q:\n%s", want, monthlyOut)
		}
	}

	sinceOut := captureStdout(t, func() {
		Calendars([]string{"--months", "--since", "20260501"})
	})
	if strings.Contains(sinceOut, "2026-04") || !strings.Contains(sinceOut, "2026-05") {
		t.Fatalf("calendar --since output has wrong months:\n%s", sinceOut)
	}
	if !strings.Contains(sinceOut, "1 event between 2026-05-01 and 2026-05-31 across 1 calendar.") {
		t.Fatalf("calendar --since output missing summary:\n%s", sinceOut)
	}

	untilOut := captureStdout(t, func() {
		Calendars([]string{"--months", "--until=2026/04"})
	})
	if !strings.Contains(untilOut, "2026-04") || strings.Contains(untilOut, "2026-05") {
		t.Fatalf("calendar --until output has wrong months:\n%s", untilOut)
	}
	if !strings.Contains(untilOut, "6 events between 2026-04-01 and 2026-04-30 across 3 calendars.") {
		t.Fatalf("calendar --until output missing summary:\n%s", untilOut)
	}

	exactSinceOut := captureStdout(t, func() {
		Calendars([]string{"--since", "20260511"})
	})
	if !strings.Contains(exactSinceOut, "No calendar data found") {
		t.Fatalf("calendar --since should filter by exact date:\n%s", exactSinceOut)
	}
}

func TestLoadAllBookingsUsesRoomCalendarReference(t *testing.T) {
	home := t.TempDir()
	dataDir := filepath.Join(home, "data")
	t.Setenv("APP_DATA_DIR", filepath.Join(home, "app"))
	t.Setenv("DATA_DIR", dataDir)

	writeJSONFixture(t, filepath.Join(home, "app", "settings", "settings.json"), `{}`)
	writeJSONFixture(t, filepath.Join(home, "app", "settings", "calendars.json"), `{
	  "sources": [
	    {"slug":"ostrom-calendar","name":"Ostrom Feed","provider":"ics","url":"https://example.com/ostrom.ics","visibility":"auto","room":"ostrom"},
	    {"slug":"privatecal","name":"Private Calendar","provider":"ics","url":"https://example.com/private.ics","visibility":"private"}
	  ]
	}`)
	writeJSONFixture(t, filepath.Join(home, "app", "settings", "rooms.json"), `{
	  "rooms": [{
	    "name": "Ostrom",
	    "slug": "ostrom",
	    "calendar": "ostrom-calendar"
	  }]
	}`)
	writeJSONFixture(t, filepath.Join(dataDir, "2026", "04", "sources", "ics", "ostrom-calendar.ics"), `BEGIN:VCALENDAR
VERSION:2.0
BEGIN:VEVENT
UID:room-booking
SUMMARY:Room booking
DTSTART:20260410T100000Z
END:VEVENT
END:VCALENDAR`)
	writeJSONFixture(t, filepath.Join(dataDir, "2026", "04", "sources", "ics", "privatecal.ics"), `BEGIN:VCALENDAR
VERSION:2.0
BEGIN:VEVENT
UID:private-source
SUMMARY:Private source
DTSTART:20260411T100000Z
END:VEVENT
END:VCALENDAR`)

	bookings, err := loadAllBookings()
	if err != nil {
		t.Fatalf("loadAllBookings: %v", err)
	}
	if len(bookings) != 1 {
		t.Fatalf("bookings length = %d, want 1: %#v", len(bookings), bookings)
	}
	if bookings[0].UID != "room-booking" || bookings[0].Room != "Ostrom" {
		t.Fatalf("booking = %#v", bookings[0])
	}
}
