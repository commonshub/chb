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
	ID             string     `json:"id"`
	Name           string     `json:"name"`
	StartAt        string     `json:"startAt"`
	EndAt          string     `json:"endAt,omitempty"`
	URL            string     `json:"url,omitempty"`
	Source         string     `json:"source"`
	CalendarSource string     `json:"calendarSource,omitempty"`
	Tags           []EventTag `json:"tags,omitempty"`
	Metadata       EventMetadata `json:"metadata"`
}



type EventTag struct {
	Name  string `json:"name"`
	Color string `json:"color,omitempty"`
}

type EventsFile struct {
	Month      string       `json:"month"`
	GeneratedAt string      `json:"generatedAt"`
	Events     []EventEntry `json:"events"`
}

func loadAllEvents() []EventEntry {
	dataDir := DataDir()
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
	showAll := HasFlag(args, "--all")

	var sinceDate time.Time
	if sinceStr := GetOption(args, "--since"); sinceStr != "" {
		if d, ok := ParseSinceDate(sinceStr); ok {
			sinceDate = d
		} else {
			sinceDate = time.Now()
		}
	} else if showAll {
		sinceDate = time.Time{} // epoch
	} else {
		sinceDate = time.Now()
	}

	allEvents := loadAllEvents()
	var filtered []EventEntry
	for _, e := range allEvents {
		t, err := time.Parse(time.RFC3339, e.StartAt)
		if err != nil {
			t, err = time.Parse("2006-01-02T15:04:05.000Z", e.StartAt)
			if err != nil {
				continue
			}
		}
		if t.Before(sinceDate) {
			continue
		}
		filtered = append(filtered, e)
	}

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
			fmt.Printf("%sRun 'chb events sync' to fetch events.%s\n", Fmt.Dim, Fmt.Reset)
		}
		fmt.Println()
		return
	}

	type row struct {
		date, time_, title, tags, url string
	}
	var rows []row
	for _, e := range sliced {
		t, _ := time.Parse(time.RFC3339, e.StartAt)
		if t.IsZero() {
			t, _ = time.Parse("2006-01-02T15:04:05.000Z", e.StartAt)
		}
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
