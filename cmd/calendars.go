package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/CommonsHub/chb/ical"
	icssource "github.com/CommonsHub/chb/sources/ics"
)

type calendarSummary struct {
	Slug       string
	Name       string
	Visibility string
	Provider   string
	Total      int
	Public     int
	Private    int
	Months     map[string]calendarMonthSummary
}

type calendarMonthSummary struct {
	ICS     int
	Public  int
	Private int
}

func Calendars(args []string) {
	if HasFlag(args, "--help", "-h", "help") {
		PrintCalendarsHelp()
		return
	}
	showMonths := HasFlag(args, "--months", "--breakdown")
	dateRange, err := calendarSummaryDateRange(args)
	if err != nil {
		fmt.Printf("\n%sError:%s %v\n\n", Fmt.Red, Fmt.Reset, err)
		return
	}

	dataDir := DataDir()
	summaries := buildCalendarSummaries(dataDir, dateRange)
	if len(summaries) == 0 {
		fmt.Printf("\n%sNo calendar data found.%s\n", Fmt.Dim, Fmt.Reset)
		fmt.Printf("%sRun 'chb calendars sync' to fetch calendar sources.%s\n\n", Fmt.Dim, Fmt.Reset)
		return
	}

	maxName := len("CALENDAR")
	maxSlug := len("SLUG")
	for _, cal := range summaries {
		maxName = Max(maxName, len(cal.Name))
		maxSlug = Max(maxSlug, len(cal.Slug))
	}
	maxName = Min(maxName, 28)
	maxSlug = Min(maxSlug, 20)

	fmt.Printf("\n%s📅 Calendars%s\n", Fmt.Bold, Fmt.Reset)
	fmt.Printf("%s%s%s\n\n", Fmt.Dim, calendarSummaryHeadline(summaries, dateRange), Fmt.Reset)
	fmt.Printf("%s%s  %s  %8s  %7s  %7s  %-8s  VISIBILITY%s\n",
		Fmt.Dim, Pad("CALENDAR", maxName), Pad("SLUG", maxSlug), "TOTAL", "PUBLIC", "PRIVATE", "PROVIDER", Fmt.Reset)
	for _, cal := range summaries {
		fmt.Printf("%s%s%s  %s  %8d  %7d  %7d  %-8s  %s\n",
			Fmt.Bold, Pad(Truncate(cal.Name, maxName), maxName), Fmt.Reset,
			Pad(Truncate(cal.Slug, maxSlug), maxSlug),
			cal.Total, cal.Public, cal.Private, cal.Provider, cal.Visibility)
		if showMonths {
			for _, month := range sortedCalendarMonths(cal.Months) {
				ms := cal.Months[month]
				fmt.Printf("  %s%s%s  public %3d  private %3d\n", Fmt.Dim, month, Fmt.Reset, ms.Public, ms.Private)
			}
		}
	}
	fmt.Println()
}

func buildCalendarSummaries(dataDir string, dateRange calendarSummaryRange) []calendarSummary {
	defs := calendarDefinitions()
	summaries := map[string]*calendarSummary{}
	ensure := func(slug string) *calendarSummary {
		slug = strings.TrimSpace(slug)
		if slug == "" {
			slug = "unknown"
		}
		if summaries[slug] == nil {
			def := defs[slug]
			name := def.Name
			if name == "" {
				name = slug
			}
			visibility := def.Visibility
			if visibility == "" {
				visibility = CalendarVisibilityAuto
			}
			provider := def.Provider
			if provider == "" {
				provider = CalendarProviderICS
			}
			summaries[slug] = &calendarSummary{
				Slug:       slug,
				Name:       name,
				Visibility: visibility,
				Provider:   provider,
				Months:     map[string]calendarMonthSummary{},
			}
		}
		return summaries[slug]
	}

	for slug := range defs {
		ensure(slug)
	}

	for _, month := range dataMonths(dataDir) {
		monthKey := month.year + "-" + month.month
		if dateRange.sinceMonth != "" && monthKey < dateRange.sinceMonth {
			continue
		}
		if dateRange.untilMonth != "" && monthKey > dateRange.untilMonth {
			continue
		}
		icsCounts := loadICSCountsForMonth(dataDir, month.year, month.month, dateRange)
		publicCounts := loadPublicEventCountsForMonth(dataDir, month.year, month.month, dateRange)
		seen := map[string]bool{}
		for slug := range icsCounts {
			seen[slug] = true
		}
		for slug := range publicCounts {
			seen[slug] = true
		}
		for slug := range seen {
			cal := ensure(slug)
			ms := cal.Months[monthKey]
			ms.ICS += icsCounts[slug]
			ms.Public += publicCounts[slug]
			switch normalizeCalendarVisibility(cal.Visibility) {
			case CalendarVisibilityPublic:
				ms.Private = 0
			case CalendarVisibilityPrivate:
				ms.Public = 0
				ms.Private = ms.ICS
			default:
				ms.Private = Max(ms.ICS-ms.Public, 0)
			}
			cal.Months[monthKey] = ms
		}
	}

	out := make([]calendarSummary, 0, len(summaries))
	for _, cal := range summaries {
		for _, ms := range cal.Months {
			cal.Public += ms.Public
			cal.Private += ms.Private
		}
		cal.Total = cal.Public + cal.Private
		if cal.Total == 0 {
			continue
		}
		out = append(out, *cal)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Visibility != out[j].Visibility {
			return out[i].Visibility < out[j].Visibility
		}
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}
		return out[i].Slug < out[j].Slug
	})
	return out
}

func calendarSummaryHeadline(summaries []calendarSummary, dateRange calendarSummaryRange) string {
	total := 0
	minMonth := ""
	maxMonth := ""
	for _, cal := range summaries {
		total += cal.Total
		for month := range cal.Months {
			if minMonth == "" || month < minMonth {
				minMonth = month
			}
			if maxMonth == "" || month > maxMonth {
				maxMonth = month
			}
		}
	}

	startDate := dateRange.sinceDate
	if startDate == "" && minMonth != "" {
		startDate = minMonth + "-01"
	}
	endDate := dateRange.untilDate
	if endDate == "" && maxMonth != "" {
		endDate = calendarMonthEndDate(maxMonth)
	}
	if startDate == "" {
		startDate = "unknown"
	}
	if endDate == "" {
		endDate = "unknown"
	}

	return fmt.Sprintf("%d %s between %s and %s across %d %s.",
		total,
		pluralize(total, "event", "events"),
		startDate,
		endDate,
		len(summaries),
		pluralize(len(summaries), "calendar", "calendars"))
}

func calendarMonthEndDate(month string) string {
	t, err := time.Parse("2006-01-02", month+"-01")
	if err != nil {
		return month
	}
	return t.AddDate(0, 1, -1).Format("2006-01-02")
}

func pluralize(count int, singular, plural string) string {
	if count == 1 {
		return singular
	}
	return plural
}

type calendarDefinition struct {
	Name       string
	Visibility string
	Provider   string
}

func calendarDefinitions() map[string]calendarDefinition {
	defs := map[string]calendarDefinition{}
	roomsBySlug := map[string]RoomInfo{}
	roomCalendarRefs := map[string]bool{}
	if rooms, err := LoadRooms(); err == nil {
		for _, room := range rooms {
			roomsBySlug[room.Slug] = room
			if room.Calendar != "" {
				defs[room.Calendar] = calendarDefinition{Name: room.Name, Visibility: CalendarVisibilityAuto, Provider: CalendarProviderICS}
				roomCalendarRefs[room.Calendar] = true
			} else if room.GoogleCalendarID != nil {
				defs[room.Slug] = calendarDefinition{Name: room.Name, Visibility: CalendarVisibilityAuto, Provider: CalendarProviderICS}
				roomCalendarRefs[room.Slug] = true
			}
		}
	}
	if settings, err := LoadSettings(); err == nil {
		for _, source := range settings.Calendars.Sources {
			name := source.Name
			if name == "" {
				name = source.Slug
			}
			visibility := normalizeCalendarVisibility(source.Visibility)
			if source.Room != "" || roomCalendarRefs[source.Slug] {
				visibility = CalendarVisibilityAuto
				if room, ok := roomsBySlug[source.Room]; ok && strings.TrimSpace(source.Name) == "" {
					name = room.Name
				}
			}
			provider := normalizeCalendarProvider(source.Provider, source.URL)
			defs[source.Slug] = calendarDefinition{Name: name, Visibility: visibility, Provider: provider}
		}
	}
	return defs
}

type calendarMonth struct {
	year  string
	month string
}

type calendarSummaryRange struct {
	sinceMonth string
	untilMonth string
	sinceDate  string
	untilDate  string
}

func calendarSummaryDateRange(args []string) (calendarSummaryRange, error) {
	var out calendarSummaryRange
	sinceMonth, sinceDate, err := parseCalendarSummaryDateOption(GetOption(args, "--since"), "--since", false)
	if err != nil {
		return calendarSummaryRange{}, err
	}
	untilMonth, untilDate, err := parseCalendarSummaryDateOption(GetOption(args, "--until"), "--until", true)
	if err != nil {
		return calendarSummaryRange{}, err
	}
	if sinceMonth != "" && untilMonth != "" && untilMonth < sinceMonth {
		return calendarSummaryRange{}, fmt.Errorf("--until must be the same as or after --since")
	}
	if sinceDate != "" && untilDate != "" && untilDate < sinceDate {
		return calendarSummaryRange{}, fmt.Errorf("--until must be the same as or after --since")
	}
	out.sinceMonth = sinceMonth
	out.untilMonth = untilMonth
	out.sinceDate = sinceDate
	out.untilDate = untilDate
	return out, nil
}

func parseCalendarSummaryDateOption(value, label string, until bool) (string, string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", "", nil
	}
	if y, m, ok := ParseSinceMonth(value); ok {
		month := y + "-" + m
		if until {
			t, _ := time.Parse("2006-01-02", y+"-"+m+"-01")
			return month, t.AddDate(0, 1, -1).Format("2006-01-02"), nil
		}
		return month, y + "-" + m + "-01", nil
	}
	if d, ok := ParseSinceDate(value); ok {
		return fmt.Sprintf("%d-%02d", d.Year(), d.Month()), d.Format("2006-01-02"), nil
	}
	return "", "", fmt.Errorf("invalid %s value %q (expected YYYY/MM, YYYYMM, or YYYYMMDD)", label, value)
}

func dataMonths(dataDir string) []calendarMonth {
	var out []calendarMonth
	yearDirs, err := os.ReadDir(dataDir)
	if err != nil {
		return out
	}
	for _, yd := range yearDirs {
		if !yd.IsDir() || len(yd.Name()) != 4 {
			continue
		}
		monthDirs, _ := os.ReadDir(filepath.Join(dataDir, yd.Name()))
		for _, md := range monthDirs {
			if !md.IsDir() || len(md.Name()) != 2 {
				continue
			}
			out = append(out, calendarMonth{year: yd.Name(), month: md.Name()})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].year != out[j].year {
			return out[i].year < out[j].year
		}
		return out[i].month < out[j].month
	})
	return out
}

func loadICSCountsForMonth(dataDir, year, month string, dateRange calendarSummaryRange) map[string]int {
	out := map[string]int{}
	root := icssource.Path(dataDir, year, month)
	entries, err := os.ReadDir(root)
	if err != nil {
		return out
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(strings.ToLower(entry.Name()), ".ics") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(root, entry.Name()))
		if err != nil {
			continue
		}
		events, err := ical.ParseICS(string(data))
		if err != nil {
			continue
		}
		slug := strings.TrimSuffix(entry.Name(), filepath.Ext(entry.Name()))
		for _, event := range events {
			if calendarTimeInDateRange(event.Start, dateRange) {
				out[slug]++
			}
		}
	}
	return out
}

func loadPublicEventCountsForMonth(dataDir, year, month string, dateRange calendarSummaryRange) map[string]int {
	out := map[string]int{}
	path := filepath.Join(dataDir, year, month, "generated", "events.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return out
	}
	var f FullEventsFile
	if json.Unmarshal(data, &f) != nil {
		return out
	}
	for _, event := range f.Events {
		if !calendarFullEventInDateRange(event, dateRange) {
			continue
		}
		slug := event.CalendarSource
		if slug == "" {
			slug = event.Source
		}
		if slug == "" {
			slug = "unknown"
		}
		out[slug]++
	}
	return out
}

func calendarFullEventInDateRange(event FullEvent, dateRange calendarSummaryRange) bool {
	if dateRange.sinceDate == "" && dateRange.untilDate == "" {
		return true
	}
	for _, layout := range []string{time.RFC3339, "2006-01-02T15:04:05.000Z"} {
		if t, err := time.Parse(layout, event.StartAt); err == nil {
			return calendarTimeInDateRange(t, dateRange)
		}
	}
	return false
}

func calendarTimeInDateRange(t time.Time, dateRange calendarSummaryRange) bool {
	if t.IsZero() {
		return dateRange.sinceDate == "" && dateRange.untilDate == ""
	}
	day := t.In(BrusselsTZ()).Format("2006-01-02")
	if dateRange.sinceDate != "" && day < dateRange.sinceDate {
		return false
	}
	if dateRange.untilDate != "" && day > dateRange.untilDate {
		return false
	}
	return true
}

func sortedCalendarMonths(months map[string]calendarMonthSummary) []string {
	out := make([]string, 0, len(months))
	for month := range months {
		out = append(out, month)
	}
	sort.Strings(out)
	return out
}
