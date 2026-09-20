package ical

import (
	"testing"
	"time"
)

func TestParseICalDateDefaultsFloatingTimesToBrussels(t *testing.T) {
	got, allDay := parseICalDate("20260701T190000", nil)
	if allDay {
		t.Fatal("floating datetime parsed as all-day")
	}
	if got.Location().String() != defaultTimezone {
		t.Fatalf("location = %q, want %q", got.Location(), defaultTimezone)
	}
	if got.Format(time.RFC3339) != "2026-07-01T19:00:00+02:00" {
		t.Fatalf("time = %s", got.Format(time.RFC3339))
	}
}

func TestParseICalDateKeepsUTCInstantsButYearMonthUsesBrussels(t *testing.T) {
	start, allDay := parseICalDate("20260531T223000Z", nil)
	if allDay {
		t.Fatal("UTC datetime parsed as all-day")
	}
	event := Event{Start: start}
	if got := event.YearMonth(); got != "2026-06" {
		t.Fatalf("YearMonth() = %q, want 2026-06", got)
	}
}

func TestParseICSUnfoldsContinuationLines(t *testing.T) {
	// As Google Calendar exports it: DESCRIPTION and URL folded at 75 octets,
	// continuation lines starting with a single space.
	ics := "BEGIN:VCALENDAR\r\n" +
		"BEGIN:VEVENT\r\n" +
		"UID:abc@google.com\r\n" +
		"DTSTART:20260929T153000Z\r\n" +
		"DTEND:20260929T190000Z\r\n" +
		"SUMMARY:Curiosity Talks : Wellbeing and Music\r\n" +
		"DESCRIPTION:Paid booking via Ralph. Details to follow. <a href=\"https://lum\r\n" +
		" a.com/curiosity-talks\">https://luma.com/curiosity-talks</a>\r\n" +
		"URL:https://luma.com/curiosity-t\r\n" +
		" alks\r\n" +
		"END:VEVENT\r\n" +
		"END:VCALENDAR\r\n"

	events, err := ParseICS(ics)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	ev := events[0]
	wantDesc := "Paid booking via Ralph. Details to follow. <a href=\"https://luma.com/curiosity-talks\">https://luma.com/curiosity-talks</a>"
	if ev.Description != wantDesc {
		t.Fatalf("description = %q\nwant %q", ev.Description, wantDesc)
	}
	if ev.URL != "https://luma.com/curiosity-talks" {
		t.Fatalf("url = %q", ev.URL)
	}
	if ev.Summary != "Curiosity Talks : Wellbeing and Music" {
		t.Fatalf("summary = %q", ev.Summary)
	}
}
