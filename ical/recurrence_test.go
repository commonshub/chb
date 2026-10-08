package ical

import (
	"testing"
	"time"
)

const recurringICS = `BEGIN:VCALENDAR
BEGIN:VEVENT
UID:tedx
DTSTART;TZID=Europe/Brussels:20261006T180000
DTEND;TZID=Europe/Brussels:20261006T200000
RRULE:FREQ=WEEKLY;UNTIL=20261105T225959Z
SUMMARY:TedX Brussels
END:VEVENT
BEGIN:VEVENT
UID:impro
DTSTART;TZID=Europe/Brussels:20260825T183000
DTEND;TZID=Europe/Brussels:20260825T210000
RRULE:FREQ=WEEKLY;UNTIL=20261005T215959Z
EXDATE;TZID=Europe/Brussels:20260901T183000,20260908T183000
SUMMARY:Impro
END:VEVENT
BEGIN:VEVENT
UID:impro
RECURRENCE-ID;TZID=Europe/Brussels:20260915T183000
DTSTART;TZID=Europe/Brussels:20260916T190000
DTEND;TZID=Europe/Brussels:20260916T210000
SUMMARY:Impro (moved)
END:VEVENT
BEGIN:VEVENT
UID:impro
RECURRENCE-ID;TZID=Europe/Brussels:20260922T183000
DTSTART;TZID=Europe/Brussels:20260922T183000
STATUS:CANCELLED
SUMMARY:Impro
END:VEVENT
BEGIN:VEVENT
UID:corr
DTSTART:20261127T090000Z
DTEND:20261127T160000Z
RRULE:FREQ=DAILY;COUNT=2
SUMMARY:Correspondents
END:VEVENT
BEGIN:VEVENT
UID:once
DTSTART:20261008T070000Z
DTEND:20261008T150000Z
SUMMARY:Ecofirst Meeting
END:VEVENT
BEGIN:VEVENT
UID:monthly
DTSTART;TZID=Europe/Brussels:20260901T100000
RRULE:FREQ=MONTHLY;BYDAY=-1FR;COUNT=3
SUMMARY:Last Friday
END:VEVENT
END:VCALENDAR
`

func starts(evs []Event, uid string) []string {
	var out []string
	for _, e := range evs {
		if e.UID == uid {
			out = append(out, e.Start.In(defaultLocation()).Format("01-02 15:04"))
		}
	}
	return out
}

func TestExpand(t *testing.T) {
	evs, err := ParseICS(recurringICS)
	if err != nil {
		t.Fatal(err)
	}
	oct := ExpandMonth(evs, 2026, 10)
	if got := starts(oct, "tedx"); len(got) != 4 || got[0] != "10-06 18:00" || got[3] != "10-27 18:00" {
		t.Errorf("tedx october (18:00 across the DST change): %v", got)
	}
	if got := starts(oct, "once"); len(got) != 1 || got[0] != "10-08 09:00" {
		t.Errorf("one-off: %v", got)
	}
	if got := starts(ExpandMonth(evs, 2026, 11), "tedx"); len(got) != 1 || got[0] != "11-03 18:00" {
		t.Errorf("tedx november (until 5 Nov): %v", got)
	}
	// impro: Aug 25, Sep 1/8 excluded, Sep 15 moved to Sep 16 19:00, Sep 22 cancelled, Sep 29.
	sep := ExpandMonth(evs, 2026, 9)
	if got := starts(sep, "impro"); len(got) != 2 || got[0] != "09-16 19:00" || got[1] != "09-29 18:30" {
		t.Errorf("impro september: %v", got)
	}
	if got := starts(ExpandMonth(evs, 2026, 11), "corr"); len(got) != 2 || got[1] != "11-28 10:00" {
		t.Errorf("daily count: %v", got)
	}
	if got := starts(Expand(evs, time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)), "monthly"); len(got) != 3 || got[0] != "09-25 10:00" || got[2] != "11-27 10:00" {
		t.Errorf("monthly last friday: %v", got)
	}
	// Archive grouping: the TedX master is filed in October and November.
	g := GroupByMonthExpanded(evs, "2026-08", "2026-12")
	has := func(ym, uid string) bool {
		for _, e := range g[ym] {
			if e.UID == uid {
				return true
			}
		}
		return false
	}
	if !has("2026-10", "tedx") || !has("2026-11", "tedx") || has("2026-12", "tedx") || !has("2026-09", "impro") || !has("2026-10", "once") {
		t.Errorf("grouping: %v", g)
	}
}
