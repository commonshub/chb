package ical

// Recurring events (RFC 5545 RRULE). A Google room calendar stores a weekly
// series as one VEVENT with an RRULE, plus one VEVENT per moved or
// cancelled instance (same UID, RECURRENCE-ID). Expand turns them into the
// concrete occurrences of a time window.
//
// Supported: FREQ=DAILY|WEEKLY|MONTHLY|YEARLY, INTERVAL, COUNT, UNTIL,
// BYDAY (weekly: MO,WE…; monthly: 2TU, -1FR), BYMONTHDAY, EXDATE,
// RECURRENCE-ID overrides and STATUS:CANCELLED. Occurrences keep the wall
// clock of DTSTART in its time zone (so 18:00 stays 18:00 across DST). An
// unsupported rule yields its first occurrence only, as before.

import (
	"sort"
	"strconv"
	"strings"
	"time"
)

// maxOccurrences bounds one series' expansion (a daily rule over 10 years
// is ~3650).
const maxOccurrences = 5000

type rrule struct {
	freq       string
	interval   int
	count      int
	until      time.Time
	byDay      []weekdayN
	byMonthDay []int
}

type weekdayN struct {
	n   int // 0 = every; 2 = second; -1 = last
	day time.Weekday
}

var weekdays = map[string]time.Weekday{"SU": time.Sunday, "MO": time.Monday, "TU": time.Tuesday,
	"WE": time.Wednesday, "TH": time.Thursday, "FR": time.Friday, "SA": time.Saturday}

func parseRRule(s string, loc *time.Location) (rrule, bool) {
	r := rrule{interval: 1}
	for _, part := range strings.Split(s, ";") {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			continue
		}
		k, v := strings.ToUpper(strings.TrimSpace(kv[0])), strings.TrimSpace(kv[1])
		switch k {
		case "FREQ":
			r.freq = strings.ToUpper(v)
		case "INTERVAL":
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				r.interval = n
			}
		case "COUNT":
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				r.count = n
			}
		case "UNTIL":
			if t, _ := parseICalDate(v, nil); !t.IsZero() {
				if len(v) == 8 { // a date: until the end of that day
					t = time.Date(t.Year(), t.Month(), t.Day(), 23, 59, 59, 0, loc)
				}
				r.until = t
			}
		case "BYDAY":
			for _, d := range strings.Split(v, ",") {
				d = strings.ToUpper(strings.TrimSpace(d))
				if len(d) < 2 {
					continue
				}
				wd, ok := weekdays[d[len(d)-2:]]
				if !ok {
					return r, false
				}
				n := 0
				if pre := d[:len(d)-2]; pre != "" {
					x, err := strconv.Atoi(strings.TrimPrefix(pre, "+"))
					if err != nil {
						return r, false
					}
					n = x
				}
				r.byDay = append(r.byDay, weekdayN{n, wd})
			}
		case "BYMONTHDAY":
			for _, d := range strings.Split(v, ",") {
				x, err := strconv.Atoi(strings.TrimSpace(d))
				if err != nil {
					return r, false
				}
				r.byMonthDay = append(r.byMonthDay, x)
			}
		case "WKST", "BYMONTH":
			// WKST only matters for INTERVAL>1 with several BYDAY; BYMONTH
			// for yearly rules on the start month — both fine to ignore here.
		default:
			return r, false // BYSETPOS, BYHOUR…: not supported
		}
	}
	switch r.freq {
	case "DAILY", "WEEKLY", "MONTHLY", "YEARLY":
		return r, true
	}
	return r, false
}

// occurrences lists the series' starts from DTSTART until `to` (exclusive),
// honouring COUNT and UNTIL (EXDATE is applied by the caller).
func (r rrule) occurrences(start, to time.Time) []time.Time {
	var out []time.Time
	emitted := 0
	emit := func(t time.Time) bool {
		if t.Before(start) {
			return true
		}
		if !r.until.IsZero() && t.After(r.until) {
			return false
		}
		if r.count > 0 && emitted >= r.count {
			return false
		}
		if !t.Before(to) {
			return false
		}
		emitted++
		out = append(out, t)
		return emitted < maxOccurrences
	}
	h, m, sec := start.Clock()
	at := func(y int, mo time.Month, d int) time.Time {
		return time.Date(y, mo, d, h, m, sec, 0, start.Location())
	}
	switch r.freq {
	case "DAILY":
		for i := 0; ; i++ {
			if !emit(start.AddDate(0, 0, i*r.interval)) {
				return out
			}
		}
	case "WEEKLY":
		days := r.byDay
		if len(days) == 0 {
			days = []weekdayN{{0, start.Weekday()}}
		}
		// Week starting Monday that contains DTSTART.
		offset := (int(start.Weekday()) + 6) % 7
		weekStart := at(start.Year(), start.Month(), start.Day()-offset)
		for w := 0; ; w++ {
			base := weekStart.AddDate(0, 0, 7*w*r.interval)
			var week []time.Time
			for _, d := range days {
				week = append(week, base.AddDate(0, 0, (int(d.day)+6)%7))
			}
			sort.Slice(week, func(i, j int) bool { return week[i].Before(week[j]) })
			for _, t := range week {
				if !emit(t) {
					return out
				}
			}
		}
	case "MONTHLY", "YEARLY":
		step := r.interval
		if r.freq == "YEARLY" {
			step = 12 * r.interval
		}
		for i := 0; ; i++ {
			first := at(start.Year(), start.Month(), 1).AddDate(0, i*step, 0)
			var month []time.Time
			switch {
			case len(r.byMonthDay) > 0:
				for _, d := range r.byMonthDay {
					if t, ok := monthDay(first, d); ok {
						month = append(month, t)
					}
				}
			case len(r.byDay) > 0:
				for _, wd := range r.byDay {
					month = append(month, nthWeekdays(first, wd)...)
				}
			default:
				if t, ok := monthDay(first, start.Day()); ok {
					month = append(month, t)
				}
			}
			sort.Slice(month, func(i, j int) bool { return month[i].Before(month[j]) })
			for _, t := range month {
				if !emit(t) {
					return out
				}
			}
			if !first.Before(to) || i > maxOccurrences {
				return out
			}
		}
	}
	return out
}

func monthDay(first time.Time, d int) (time.Time, bool) {
	days := first.AddDate(0, 1, -1).Day()
	if d < 0 {
		d = days + d + 1
	}
	if d < 1 || d > days {
		return time.Time{}, false
	}
	return first.AddDate(0, 0, d-1), true
}

func nthWeekdays(first time.Time, wd weekdayN) []time.Time {
	var all []time.Time
	for t := first; t.Month() == first.Month(); t = t.AddDate(0, 0, 1) {
		if t.Weekday() == wd.day {
			all = append(all, t)
		}
	}
	switch {
	case wd.n == 0:
		return all
	case wd.n > 0 && wd.n <= len(all):
		return []time.Time{all[wd.n-1]}
	case wd.n < 0 && -wd.n <= len(all):
		return []time.Time{all[len(all)+wd.n]}
	}
	return nil
}

// Expand returns the concrete occurrences starting in [from, to): plain
// events as they are, each series expanded (EXDATEs dropped, instances
// replaced by their RECURRENCE-ID override), cancelled ones left out.
func Expand(events []Event, from, to time.Time) []Event {
	overridden := map[string]bool{} // UID|instance start (unix)
	for _, e := range events {
		if !e.RecurrenceID.IsZero() {
			overridden[e.UID+"|"+strconv.FormatInt(e.RecurrenceID.Unix(), 10)] = true
		}
	}
	var out []Event
	for _, e := range events {
		if e.Status == "CANCELLED" {
			continue
		}
		if e.RRule == "" || !e.RecurrenceID.IsZero() {
			if !e.Start.Before(from) && e.Start.Before(to) {
				out = append(out, e)
			}
			continue
		}
		r, ok := parseRRule(e.RRule, e.Start.Location())
		if !ok {
			if !e.Start.Before(from) && e.Start.Before(to) {
				out = append(out, e)
			}
			continue
		}
		dur := e.End.Sub(e.Start)
		ex := map[int64]bool{}
		for _, x := range e.ExDates {
			ex[x.Unix()] = true
		}
		for _, t := range r.occurrences(e.Start, to) {
			if t.Before(from) || ex[t.Unix()] || overridden[e.UID+"|"+strconv.FormatInt(t.Unix(), 10)] {
				continue
			}
			occ := e
			occ.Start = t
			if !e.End.IsZero() {
				occ.End = t.Add(dur)
			}
			out = append(out, occ)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Start.Before(out[j].Start) })
	return out
}

// ExpandMonth: the occurrences starting in YYYY-MM (Europe/Brussels).
func ExpandMonth(events []Event, year, month int) []Event {
	from := time.Date(year, time.Month(month), 1, 0, 0, 0, 0, defaultLocation())
	return Expand(events, from, from.AddDate(0, 1, 0))
}

// GroupByMonthExpanded groups raw VEVENTs (RawLines unchanged) by every
// month in [sinceMonth, untilMonth] where they have an occurrence: a
// series master goes to each month it recurs in, an override to the month
// of its instance and of its new start. Readers expand each month file
// with ExpandMonth, so nothing is counted twice.
func GroupByMonthExpanded(events []Event, sinceMonth, untilMonth string) map[string][]Event {
	result := map[string][]Event{}
	add := func(ym string, e Event) {
		if ym < sinceMonth || ym > untilMonth {
			return
		}
		for _, x := range result[ym] {
			if sameRaw(x, e) {
				return
			}
		}
		result[ym] = append(result[ym], e)
	}
	ymOf := func(t time.Time) string { return t.In(defaultLocation()).Format("2006-01") }
	to, err := time.ParseInLocation("2006-01", untilMonth, defaultLocation())
	if err != nil {
		return GroupByMonth(events)
	}
	to = to.AddDate(0, 1, 0)
	for _, e := range events {
		if e.RRule != "" && e.RecurrenceID.IsZero() {
			if r, ok := parseRRule(e.RRule, e.Start.Location()); ok {
				add(ymOf(e.Start), e)
				for _, t := range r.occurrences(e.Start, to) {
					add(ymOf(t), e)
				}
				continue
			}
		}
		add(ymOf(e.Start), e)
		if !e.RecurrenceID.IsZero() {
			add(ymOf(e.RecurrenceID), e)
		}
	}
	return result
}

func sameRaw(a, b Event) bool {
	if a.UID != b.UID || len(a.RawLines) != len(b.RawLines) {
		return false
	}
	for i := range a.RawLines {
		if a.RawLines[i] != b.RawLines[i] {
			return false
		}
	}
	return true
}

// parseExDates parses one EXDATE property stored as "tzid|value-type|v1,v2".
func parseExDates(s string) []time.Time {
	parts := strings.SplitN(s, "|", 3)
	if len(parts) != 3 {
		return nil
	}
	params := map[string]string{}
	if parts[0] != "" {
		params["TZID"] = parts[0]
	}
	var out []time.Time
	for _, v := range strings.Split(parts[2], ",") {
		if t, _ := parseICalDate(v, params); !t.IsZero() {
			out = append(out, t)
		}
	}
	return out
}

// ParseMonthICS parses a month archive (YYYY/MM/providers/ics/<slug>.ics)
// and returns the occurrences starting in that month, series expanded.
func ParseMonthICS(data string, year, month string) ([]Event, error) {
	events, err := ParseICS(data)
	if err != nil {
		return nil, err
	}
	y, err1 := strconv.Atoi(year)
	m, err2 := strconv.Atoi(month)
	if err1 != nil || err2 != nil {
		return events, nil
	}
	return ExpandMonth(events, y, m), nil
}
