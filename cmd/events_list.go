package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// EventEntry matches the events.json structure
type EventEntry struct {
	ID             string        `json:"id"`
	Name           string        `json:"name"`
	StartAt        string        `json:"startAt"`
	EndAt          string        `json:"endAt,omitempty"`
	URL            string        `json:"url,omitempty"`
	Source         string        `json:"source"`
	CalendarSource string        `json:"calendarSource,omitempty"`
	Tags           []EventTag    `json:"tags,omitempty"`
	Metadata       EventMetadata `json:"metadata"`
}

type EventTag struct {
	Name  string `json:"name"`
	Color string `json:"color,omitempty"`
}

type EventsFile struct {
	Month       string       `json:"month"`
	GeneratedAt string       `json:"generatedAt"`
	Events      []EventEntry `json:"events"`
}

type eventListFilter struct {
	Since        time.Time
	Until        time.Time
	HasSince     bool
	HasUntil     bool
	Desc         bool
	ForceMonthly bool
}

func loadAllEvents() []EventEntry {
	return loadMonthlyEvents(DataDir(), eventListFilter{})
}

func loadEventsForList(dataDir string, filter eventListFilter) []EventEntry {
	latest := loadLatestEvents(dataDir)
	if len(latest) > 0 && !shouldLoadMonthlyEvents(latest, filter) {
		return latest
	}
	return loadMonthlyEvents(dataDir, filter)
}

func loadLatestEvents(dataDir string) []EventEntry {
	eventsPath := filepath.Join(dataDir, "latest", "generated", "events.json")
	data, err := os.ReadFile(eventsPath)
	if err != nil {
		return nil
	}
	var ef EventsFile
	if err := json.Unmarshal(data, &ef); err != nil {
		return nil
	}
	return ef.Events
}

func shouldLoadMonthlyEvents(latest []EventEntry, filter eventListFilter) bool {
	if filter.ForceMonthly {
		return true
	}
	oldest, ok := oldestEventStart(latest)
	if !ok {
		return true
	}
	oldestDay := eventDayStart(oldest)
	if filter.HasSince && filter.Since.Before(oldestDay) {
		return true
	}
	if filter.HasUntil && filter.Until.Before(oldestDay) {
		return true
	}
	return false
}

func oldestEventStart(events []EventEntry) (time.Time, bool) {
	var oldest time.Time
	found := false
	for _, e := range events {
		t, ok := parseEventStart(e.StartAt)
		if !ok {
			continue
		}
		if !found || t.Before(oldest) {
			oldest = t
			found = true
		}
	}
	return oldest, found
}

func loadMonthlyEvents(dataDir string, filter eventListFilter) []EventEntry {
	var events []EventEntry

	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		return events
	}

	yearDirs, _ := os.ReadDir(dataDir)
	var years []string
	for _, d := range yearDirs {
		if d.IsDir() && len(d.Name()) == 4 && d.Name()[0] >= '0' && d.Name()[0] <= '9' {
			years = append(years, d.Name())
		}
	}
	sort.Strings(years)

	for _, year := range years {
		yearPath := filepath.Join(dataDir, year)
		monthDirs, _ := os.ReadDir(yearPath)
		var months []string
		for _, d := range monthDirs {
			if d.IsDir() && len(d.Name()) == 2 && d.Name()[0] >= '0' && d.Name()[0] <= '9' {
				months = append(months, d.Name())
			}
		}
		sort.Strings(months)

		for _, month := range months {
			if !filter.monthMayMatch(year, month) {
				continue
			}
			eventsPath := filepath.Join(yearPath, month, "generated", "events.json")
			data, err := os.ReadFile(eventsPath)
			if err != nil {
				continue
			}
			var ef EventsFile
			if err := json.Unmarshal(data, &ef); err != nil {
				continue
			}
			events = append(events, ef.Events...)
		}
	}

	sort.Slice(events, func(i, j int) bool {
		return events[i].StartAt < events[j].StartAt
	})
	return events
}

func EventsList(args []string) {
	if HasFlag(args, "--help", "-h", "help") {
		PrintEventsHelp()
		return
	}

	n := GetNumber(args, []string{"-n"}, 10)
	skip := GetNumber(args, []string{"--skip"}, 0)
	filter, err := parseEventListFilter(args, time.Now())
	if err != nil {
		fmt.Printf("%sError: %v%s\n", Fmt.Red, err, Fmt.Reset)
		return
	}

	filtered := filterAndSortEvents(loadEventsForList(DataDir(), filter), filter)

	sliced := filtered
	if skip < len(sliced) {
		sliced = sliced[skip:]
	} else {
		sliced = nil
	}
	if len(sliced) > n {
		sliced = sliced[:n]
	}

	if len(sliced) == 0 {
		fmt.Printf("\n%sNo events found.%s\n", Fmt.Dim, Fmt.Reset)
		if _, err := os.Stat(DataDir()); os.IsNotExist(err) {
			fmt.Printf("%sDATA_DIR not found:%s %s\n", Fmt.Yellow, Fmt.Reset, DataDir())
			fmt.Printf("%sRun 'chb calendars sync' to fetch calendar data, then `chb generate events` if needed.%s\n", Fmt.Dim, Fmt.Reset)
		}
		fmt.Println()
		return
	}

	type row struct {
		date, time_, title, tags, url string
	}
	var rows []row
	for _, e := range sliced {
		t, _ := parseEventStart(e.StartAt)
		var tagNames []string
		for _, tag := range e.Tags {
			tagNames = append(tagNames, tag.Name)
		}
		rows = append(rows, row{
			date:  FmtDate(t),
			time_: FmtTime(t),
			title: e.Name,
			tags:  strings.Join(tagNames, ", "),
			url:   e.URL,
		})
	}

	maxTitle := 5
	maxTags := 4
	for _, r := range rows {
		maxTitle = Max(maxTitle, len(r.title))
		maxTags = Max(maxTags, len(r.tags))
	}
	maxTitle = Min(maxTitle, 45)
	maxTags = Min(maxTags, 25)

	skipStr := ""
	if skip > 0 {
		skipStr = fmt.Sprintf(", skip %d", skip)
	}

	fmt.Printf("\n%s📅 Events%s %s(%d of %d%s)%s\n\n",
		Fmt.Bold, Fmt.Reset, Fmt.Dim, len(sliced), len(filtered), skipStr, Fmt.Reset)
	fmt.Printf("%s%s %s %s %s URL%s\n",
		Fmt.Dim, Pad("DATE", 16), Pad("TIME", 6), Pad("TITLE", maxTitle), Pad("TAGS", maxTags), Fmt.Reset)

	for _, r := range rows {
		tagsStr := ""
		if r.tags != "" {
			tagsStr = fmt.Sprintf("%s%s%s", Fmt.Dim, Truncate(r.tags, maxTags), Fmt.Reset)
		}
		fmt.Printf("%s%s%s %s%s%s %s %s %s%s%s\n",
			Fmt.Green, Pad(r.date, 16), Fmt.Reset,
			Fmt.Cyan, Pad(r.time_, 6), Fmt.Reset,
			Pad(Truncate(r.title, maxTitle), maxTitle),
			Pad(tagsStr, maxTags+len(Fmt.Dim)+len(Fmt.Reset)),
			Fmt.Dim, r.url, Fmt.Reset)
	}

	remaining := len(filtered) - skip - n
	if remaining > 0 {
		fmt.Printf("\n%s… %d more. Use -n or --skip to paginate.%s\n", Fmt.Dim, remaining, Fmt.Reset)
	}
	fmt.Println()
}

func parseEventListFilter(args []string, now time.Time) (eventListFilter, error) {
	filter := eventListFilter{}

	if sinceStr := GetOption(args, "--since"); sinceStr != "" {
		d, ok := parseEventListDate(sinceStr)
		if !ok {
			return filter, fmt.Errorf("invalid --since value %q (expected YYYYMMDD)", sinceStr)
		}
		filter.Since = d
		filter.HasSince = true
	}

	if untilStr := GetOption(args, "--until"); untilStr != "" {
		d, ok := parseEventListDate(untilStr)
		if !ok {
			return filter, fmt.Errorf("invalid --until value %q (expected YYYYMMDD)", untilStr)
		}
		filter.Until = d.Add(24*time.Hour - time.Second)
		filter.HasUntil = true
	}

	if !filter.HasSince && !filter.HasUntil && !HasFlag(args, "--all") {
		filter.Since = now
		filter.HasSince = true
	}
	filter.ForceMonthly = HasFlag(args, "--all")

	// `--until` by itself is a "latest before date" view, so newest first.
	// `--since`, and bounded `--since --until`, are chronological views.
	filter.Desc = filter.HasUntil && !filter.HasSince
	return filter, nil
}

func (filter eventListFilter) monthMayMatch(year, month string) bool {
	ym := year + "-" + month
	if filter.HasSince {
		sinceYM := fmt.Sprintf("%04d-%02d", filter.Since.Year(), filter.Since.Month())
		if ym < sinceYM {
			return false
		}
	}
	if filter.HasUntil {
		untilYM := fmt.Sprintf("%04d-%02d", filter.Until.Year(), filter.Until.Month())
		if ym > untilYM {
			return false
		}
	}
	return true
}

func parseEventListDate(s string) (time.Time, bool) {
	clean := strings.ReplaceAll(s, "-", "")
	if len(clean) != 8 {
		return time.Time{}, false
	}
	t, err := time.ParseInLocation("20060102", clean, BrusselsTZ())
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

func eventDayStart(t time.Time) time.Time {
	local := t.In(BrusselsTZ())
	return time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, BrusselsTZ())
}

func filterAndSortEvents(events []EventEntry, filter eventListFilter) []EventEntry {
	type datedEvent struct {
		event EventEntry
		start time.Time
	}
	var dated []datedEvent
	for _, e := range events {
		t, ok := parseEventStart(e.StartAt)
		if !ok {
			continue
		}
		if filter.HasSince && t.Before(filter.Since) {
			continue
		}
		if filter.HasUntil && t.After(filter.Until) {
			continue
		}
		dated = append(dated, datedEvent{event: e, start: t})
	}

	sort.Slice(dated, func(i, j int) bool {
		if filter.Desc {
			return dated[i].start.After(dated[j].start)
		}
		return dated[i].start.Before(dated[j].start)
	})

	out := make([]EventEntry, len(dated))
	for i := range dated {
		out[i] = dated[i].event
	}
	return out
}

func parseEventStart(startAt string) (time.Time, bool) {
	t, err := time.Parse(time.RFC3339, startAt)
	if err == nil {
		return t, true
	}
	t, err = time.Parse("2006-01-02T15:04:05.000Z", startAt)
	if err == nil {
		return t, true
	}
	return time.Time{}, false
}
