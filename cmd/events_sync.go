package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/CommonsHub/chb/ical"
	"github.com/CommonsHub/chb/og"
)

// FullEvent is the rich event structure written to events.json
type FullEvent struct {
	ID              string          `json:"id"`
	Name            string          `json:"name"`
	Description     string          `json:"description,omitempty"`
	StartAt         string          `json:"startAt"`
	EndAt           string          `json:"endAt,omitempty"`
	Timezone        string          `json:"timezone,omitempty"`
	Location        string          `json:"location,omitempty"`
	URL             string          `json:"url,omitempty"`
	CoverImage      string          `json:"coverImage,omitempty"`
	CoverImageLocal string          `json:"coverImageLocal,omitempty"`
	Source          string          `json:"source"`
	CalendarSource  string          `json:"calendarSource,omitempty"`
	Tags            json.RawMessage `json:"tags,omitempty"`
	Guests          json.RawMessage `json:"guests,omitempty"`
	LumaData        json.RawMessage `json:"lumaData,omitempty"`
	Metadata        EventMetadata   `json:"metadata"`
}

type EventMetadata struct {
	Host          *string  `json:"host,omitempty"`
	Attendance    *int     `json:"attendance,omitempty"`
	FridgeIncome  *float64 `json:"fridgeIncome,omitempty"`
	RentalIncome  *float64 `json:"rentalIncome,omitempty"`
	TicketsSold   *int     `json:"ticketsSold,omitempty"`
	TicketRevenue *float64 `json:"ticketRevenue,omitempty"`
	Note          *string  `json:"note,omitempty"`
}

type FullEventsFile struct {
	Month       string      `json:"month"`
	GeneratedAt string      `json:"generatedAt"`
	Events      []FullEvent `json:"events"`
}

type newEventInfo struct {
	name           string
	startAt        string
	metadataSource string
}

type monthResult struct {
	yearMonth  string
	totalEvents int
	newEvents  []newEventInfo
}

func EventsSync(args []string, version string) error {
	if HasFlag(args, "--help", "-h", "help") {
		PrintEventsSyncHelp()
		return nil
	}

	force := HasFlag(args, "--force")
	sinceStr := GetOption(args, "--since")

	// Positional year/month arg (e.g. "2025" or "2025/11")
	posYear, posMonth, posFound := ParseYearMonthArg(args)

	dataDir := DataDir()

	rooms, err := LoadRooms()
	if err != nil {
		return fmt.Errorf("failed to load rooms: %w", err)
	}

	// Show env info
	fmt.Printf("\n%sDATA_DIR: %s%s\n", Fmt.Dim, dataDir, Fmt.Reset)

	now := time.Now()

	// Determine time range
	var sinceMonth, untilMonth string
	resolvedSince, isSince := ResolveSinceMonth(args, "events")

	if posFound {
		if posMonth != "" {
			sinceMonth = fmt.Sprintf("%s-%s", posYear, posMonth)
			untilMonth = sinceMonth
		} else {
			sinceMonth = fmt.Sprintf("%s-01", posYear)
			untilMonth = fmt.Sprintf("%s-12", posYear)
		}
	} else if isSince {
		sinceMonth = resolvedSince
	} else if sinceStr != "" {
		if d, ok := ParseSinceDate(sinceStr); ok {
			sinceMonth = fmt.Sprintf("%d-%02d", d.Year(), d.Month())
		}
	}

	if untilMonth == "" {
		future := time.Date(now.Year(), now.Month()+3, 1, 0, 0, 0, 0, time.UTC)
		untilMonth = fmt.Sprintf("%d-%02d", future.Year(), future.Month())
	}
	if sinceMonth == "" {
		prev := time.Date(now.Year(), now.Month()-1, 1, 0, 0, 0, 0, time.UTC)
		sinceMonth = fmt.Sprintf("%d-%02d", prev.Year(), prev.Month())
	}

	// Step 1: Fetch ICS from all room calendars
	fmt.Printf("\n📅 Fetching room calendars...\n")

	// Collect all events with URLs from room calendars
	var allRoomEvents []roomEvent
	roomsWithCalendar := 0

	for _, room := range rooms {
		if room.GoogleCalendarID == nil {
			continue
		}
		roomsWithCalendar++

		calURL := getGoogleCalendarURL(*room.GoogleCalendarID)
		fmt.Printf("  Fetching %s...\n", room.Slug)

		icsData, err := fetchURL(calURL)
		if err != nil {
			fmt.Printf("  %sWarning: failed to fetch %s: %v%s\n", Fmt.Yellow, room.Slug, err, Fmt.Reset)
			continue
		}

		events, err := ical.ParseICS(icsData)
		if err != nil {
			fmt.Printf("  %sWarning: failed to parse %s ICS: %v%s\n", Fmt.Yellow, room.Slug, err, Fmt.Reset)
			continue
		}

		eventCount := 0
		for _, ev := range events {
			// Only include events that have a URL (website) defined
			eventURL := ev.URL
			if eventURL == "" {
				// Check if location is a URL
				if ev.Location != "" && (strings.HasPrefix(ev.Location, "http://") || strings.HasPrefix(ev.Location, "https://")) {
					eventURL = ev.Location
				}
			}
			if eventURL == "" {
				// Try to extract URL from description
				if ev.Description != "" {
					re := regexp.MustCompile(`https?://[^\s\n<>"']+`)
					if m := re.FindString(ev.Description); m != "" {
						eventURL = strings.TrimRight(m, ".,;:!?")
					}
				}
			}
			if eventURL == "" {
				continue // Skip events without a URL
			}
			eventCount++
			allRoomEvents = append(allRoomEvents, roomEvent{event: ev, roomSlug: room.Slug, roomName: room.Name})
		}
		fmt.Printf("  %s✓%s %s: %d events with URLs\n", Fmt.Green, Fmt.Reset, room.Slug, eventCount)
	}

	if roomsWithCalendar == 0 {
		fmt.Printf("  %sNo rooms with Google Calendar IDs found.%s\n", Fmt.Yellow, Fmt.Reset)
		return nil
	}

	// Group events by month and save ICS files
	affectedMonths := map[string]bool{}
	eventsByMonth := map[string][]roomEvent{}

	for _, re := range allRoomEvents {
		ym := re.event.YearMonth()
		if ym < sinceMonth || ym > untilMonth {
			continue
		}
		affectedMonths[ym] = true
		eventsByMonth[ym] = append(eventsByMonth[ym], re)
	}

	sortedMonths := []string{}
	for ym := range affectedMonths {
		sortedMonths = append(sortedMonths, ym)
	}
	sort.Strings(sortedMonths)

	// Save ICS files per month (public.ics = all events with URLs across rooms)
	for _, ym := range sortedMonths {
		parts := strings.SplitN(ym, "-", 2)
		year, month := parts[0], parts[1]

		var icsEvents []ical.Event
		for _, re := range eventsByMonth[ym] {
			icsEvents = append(icsEvents, re.event)
		}

		content := ical.WrapICS(icsEvents, "-//Commons Hub Brussels//Room Calendars//EN")
		writeMonthFile(dataDir, year, month, filepath.Join("calendars", "ics", "public.ics"), []byte(content))
	}

	// Process each month
	var results []monthResult
	for _, ym := range sortedMonths {
		parts := strings.SplitN(ym, "-", 2)
		year, month := parts[0], parts[1]

		r, err := processMonthFromRooms(dataDir, year, month, eventsByMonth[ym], force)
		if err != nil {
			fmt.Fprintf(os.Stderr, "  Warning: error processing %s-%s: %v\n", year, month, err)
			continue
		}
		if r != nil {
			results = append(results, *r)
		}
	}

	// Generate yearly aggregates for affected years
	years := map[string]bool{}
	for _, ym := range sortedMonths {
		parts := strings.SplitN(ym, "-", 2)
		years[parts[0]] = true
	}
	for year := range years {
		generateYearlyEvents(dataDir, year)
		generateYearlyCSV(dataDir, year)
	}

	// Generate markdown files
	generateMarkdownFiles(dataDir)

	// Print new events
	monthsWithNew := []monthResult{}
	for _, r := range results {
		if len(r.newEvents) > 0 {
			monthsWithNew = append(monthsWithNew, r)
		}
	}

	if len(monthsWithNew) > 0 {
		fmt.Printf("\n📊 Processing months...\n")
		for _, r := range monthsWithNew {
			count := len(r.newEvents)
			plural := "s"
			if count == 1 {
				plural = ""
			}
			fmt.Printf("  %s %s✓%s %d new event%s\n", r.yearMonth, Fmt.Green, Fmt.Reset, count, plural)
			for _, evt := range r.newEvents {
				t, _ := time.Parse(time.RFC3339, evt.startAt)
				if t.IsZero() {
					t, _ = time.Parse("2006-01-02T15:04:05.000Z", evt.startAt)
				}
				dateStr := fmt.Sprintf("%02d/%02d", t.Month(), t.Day())
				fmt.Printf("    + %s%s%s %s %s(via %s)%s\n",
					Fmt.Dim, dateStr, Fmt.Reset, evt.name, Fmt.Dim, evt.metadataSource, Fmt.Reset)
			}
		}
	}

	// Final summary
	allEvents := loadAllEvents()
	var futureEvents []EventEntry
	for _, e := range allEvents {
		t, _ := time.Parse(time.RFC3339, e.StartAt)
		if t.IsZero() {
			t, _ = time.Parse("2006-01-02T15:04:05.000Z", e.StartAt)
		}
		if t.After(now) {
			futureEvents = append(futureEvents, e)
		}
	}

	// Domain breakdown
	domainCounts := map[string]int{}
	for _, e := range futureEvents {
		domain := "no url"
		if e.URL != "" {
			if u, err := url.Parse(e.URL); err == nil {
				domain = strings.TrimPrefix(u.Hostname(), "www.")
			}
		}
		domainCounts[domain]++
	}

	// By room breakdown
	roomCounts := map[string]int{}
	for _, e := range futureEvents {
		src := e.CalendarSource
		if src == "" {
			src = "unknown"
		}
		roomCounts[src]++
	}

	// Count events.md entries
	eventsMdPath := filepath.Join(dataDir, "latest", "events.md")
	eventsMdCount := 0
	if data, err := os.ReadFile(eventsMdPath); err == nil {
		re := regexp.MustCompile(`(?m)^### `)
		eventsMdCount = len(re.FindAll(data, -1))
	}

	fmt.Printf("\n%s✓ Done!%s %d total events, %d upcoming\n", Fmt.Green, Fmt.Reset, len(allEvents), len(futureEvents))

	// Room breakdown
	var roomParts []string
	for room, count := range roomCounts {
		roomParts = append(roomParts, fmt.Sprintf("%s: %d", room, count))
	}
	sort.Strings(roomParts)
	if len(roomParts) > 0 {
		fmt.Printf("  %s%s%s\n", Fmt.Dim, strings.Join(roomParts, ", "), Fmt.Reset)
	}

	// Domain breakdown sorted by count desc
	type domainCount struct {
		domain string
		count  int
	}
	var sorted []domainCount
	for d, c := range domainCounts {
		sorted = append(sorted, domainCount{d, c})
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].count > sorted[j].count })
	var domainParts []string
	for _, dc := range sorted {
		domainParts = append(domainParts, fmt.Sprintf("%s: %d", dc.domain, dc.count))
	}
	fmt.Printf("  %s%s%s\n", Fmt.Dim, strings.Join(domainParts, ", "), Fmt.Reset)

	absMdPath, _ := filepath.Abs(eventsMdPath)
	fmt.Printf("  %d events written to %s\n\n", eventsMdCount, absMdPath)

	return nil
}

func fetchURL(rawURL string) (string, error) {
	resp, err := http.Get(rawURL)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// roomEvent pairs an ical event with its room source
type roomEvent struct {
	event    ical.Event
	roomSlug string
	roomName string
}

func processMonthFromRooms(dataDir, year, month string, roomEvents []roomEvent, force bool) (*monthResult, error) {
	monthPath := filepath.Join(dataDir, year, month)

	// Load existing event IDs to detect new ones
	existingIDs := map[string]bool{}
	existingMetadata := map[string]EventMetadata{}
	existingPath := filepath.Join(monthPath, "generated", "events.json")
	if data, err := os.ReadFile(existingPath); err == nil {
		var ef FullEventsFile
		if json.Unmarshal(data, &ef) == nil {
			for _, e := range ef.Events {
				existingIDs[e.ID] = true
				existingMetadata[e.ID] = e.Metadata
			}
		}
	}

	// Check if we can skip (same event count, not forced)
	if !force && len(existingIDs) == len(roomEvents) && len(existingIDs) > 0 {
		return nil, nil
	}

	processedIDs := map[string]bool{}
	var newEvents []newEventInfo
	var fullEvents []FullEvent

	for _, re := range roomEvents {
		icsEv := re.event
		eventID := icsEv.UID
		name := icsEv.Summary
		eventURL := icsEv.URL
		location := icsEv.Location

		// Check if location is a URL
		if location != "" && (strings.HasPrefix(location, "http://") || strings.HasPrefix(location, "https://")) {
			eventURL = location
			location = "Commons Hub Brussels, Rue de la Madeleine 51, 1000 Bruxelles, Belgium"
		}

		// Try to extract URL from description if none set
		if eventURL == "" && icsEv.Description != "" {
			urlRe := regexp.MustCompile(`https?://[^\s\n<>"']+`)
			if m := urlRe.FindString(icsEv.Description); m != "" {
				eventURL = strings.TrimRight(m, ".,;:!?")
			}
		}

		// Skip events without a URL (these are regular bookings, not public events)
		if eventURL == "" {
			continue
		}

		startAt := icsEv.Start.Format(time.RFC3339)
		endAt := ""
		if !icsEv.End.IsZero() {
			endAt = icsEv.End.Format(time.RFC3339)
		}

		// Scrape og:image for cover
		var coverImage string
		ogImg := og.FetchOGImage(eventURL)
		if ogImg != "" {
			coverImage = ogImg
		}

		if processedIDs[eventID] {
			continue
		}
		processedIDs[eventID] = true

		// Track new events
		if !existingIDs[eventID] {
			newEvents = append(newEvents, newEventInfo{name, startAt, re.roomSlug})
		}

		// Preserve existing metadata
		metadata := existingMetadata[eventID]

		// Default location if not set
		if location == "" {
			location = "Commons Hub Brussels, Rue de la Madeleine 51, 1000 Bruxelles, Belgium"
		}

		fullEvents = append(fullEvents, FullEvent{
			ID:             eventID,
			Name:           name,
			Description:    icsEv.Description,
			StartAt:        startAt,
			EndAt:          endAt,
			Location:       location,
			URL:            eventURL,
			CoverImage:     coverImage,
			Source:          "calendar",
			CalendarSource: re.roomSlug,
			Metadata:       metadata,
		})
	}

	// Sort by start date
	sort.Slice(fullEvents, func(i, j int) bool {
		return fullEvents[i].StartAt < fullEvents[j].StartAt
	})

	// Write events.json
	ef := FullEventsFile{
		Month:       fmt.Sprintf("%s-%s", year, month),
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Events:      fullEvents,
	}
	data, _ := json.MarshalIndent(ef, "", "  ")
	writeMonthFile(dataDir, year, month, filepath.Join("generated", "events.json"), data)

	return &monthResult{
		yearMonth:   fmt.Sprintf("%s-%s", year, month),
		totalEvents: len(fullEvents),
		newEvents:   newEvents,
	}, nil
}

func generateYearlyEvents(dataDir, year string) {
	yearPath := filepath.Join(dataDir, year)
	if _, err := os.Stat(yearPath); os.IsNotExist(err) {
		return
	}

	monthDirs, _ := os.ReadDir(yearPath)
	var allEvents []FullEvent

	for _, d := range monthDirs {
		if !d.IsDir() || len(d.Name()) != 2 {
			continue
		}
		eventsPath := filepath.Join(yearPath, d.Name(), "generated", "events.json")
		data, err := os.ReadFile(eventsPath)
		if err != nil {
			continue
		}
		var ef FullEventsFile
		if json.Unmarshal(data, &ef) == nil {
			allEvents = append(allEvents, ef.Events...)
		}
	}

	sort.Slice(allEvents, func(i, j int) bool {
		return allEvents[i].StartAt < allEvents[j].StartAt
	})

	ef := FullEventsFile{
		Month:       year,
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Events:      allEvents,
	}
	data, _ := json.MarshalIndent(ef, "", "  ")
	os.WriteFile(filepath.Join(yearPath, "generated", "events.json"), data, 0644)
}

func generateYearlyCSV(dataDir, year string) {
	yearPath := filepath.Join(dataDir, year)
	eventsPath := filepath.Join(yearPath, "generated", "events.json")
	data, err := os.ReadFile(eventsPath)
	if err != nil {
		return
	}

	var ef FullEventsFile
	if json.Unmarshal(data, &ef) != nil {
		return
	}

	headers := "Event ID,Calendar Source,Date,Time,Event Name,Host,Attendance,Tickets Sold,Ticket Revenue (EUR),Fridge Income (EUR),Rental Income (EUR),Location,URL,Note"
	var rows []string
	for _, e := range ef.Events {
		t, _ := time.Parse(time.RFC3339, e.StartAt)
		if t.IsZero() {
			t, _ = time.Parse("2006-01-02T15:04:05.000Z", e.StartAt)
		}
		dateStr := t.In(BrusselsTZ()).Format("02/01/2006")
		timeStr := t.In(BrusselsTZ()).Format("15:04")

		host := ""
		attendance := ""
		ticketsSold := ""
		ticketRevenue := ""
		fridgeIncome := ""
		rentalIncome := ""
		note := ""
		if e.Metadata.Host != nil {
			host = *e.Metadata.Host
		}
		if e.Metadata.Attendance != nil {
			attendance = fmt.Sprintf("%d", *e.Metadata.Attendance)
		}
		if e.Metadata.TicketsSold != nil {
			ticketsSold = fmt.Sprintf("%d", *e.Metadata.TicketsSold)
		}
		if e.Metadata.TicketRevenue != nil {
			ticketRevenue = fmt.Sprintf("%.2f", *e.Metadata.TicketRevenue)
		}
		if e.Metadata.FridgeIncome != nil {
			fridgeIncome = fmt.Sprintf("%.2f", *e.Metadata.FridgeIncome)
		}
		if e.Metadata.RentalIncome != nil {
			rentalIncome = fmt.Sprintf("%.2f", *e.Metadata.RentalIncome)
		}
		if e.Metadata.Note != nil {
			note = *e.Metadata.Note
		}

		rows = append(rows, strings.Join([]string{
			csvEscape(e.ID),
			csvEscape(e.CalendarSource),
			csvEscape(dateStr),
			csvEscape(timeStr),
			csvEscape(e.Name),
			csvEscape(host),
			csvEscape(attendance),
			csvEscape(ticketsSold),
			csvEscape(ticketRevenue),
			csvEscape(fridgeIncome),
			csvEscape(rentalIncome),
			csvEscape(e.Location),
			csvEscape(e.URL),
			csvEscape(note),
		}, ","))
	}

	csvContent := headers + "\n" + strings.Join(rows, "\n") + "\n"
	os.WriteFile(filepath.Join(yearPath, "generated", "events.csv"), []byte(csvContent), 0644)
}

func csvEscape(s string) string {
	if strings.ContainsAny(s, ",\"\n") {
		return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
	}
	return s
}

func generateMarkdownFiles(dataDir string) {
	generateEventsMd(dataDir)
	generateRoomsMd(dataDir)
}

func generateEventsMd(dataDir string) {
	now := time.Now()
	var events []FullEvent

	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		return
	}

	yearDirs, _ := os.ReadDir(dataDir)
	for _, yd := range yearDirs {
		if !yd.IsDir() || len(yd.Name()) != 4 {
			continue
		}
		yearPath := filepath.Join(dataDir, yd.Name())
		monthDirs, _ := os.ReadDir(yearPath)
		for _, md := range monthDirs {
			if !md.IsDir() || len(md.Name()) != 2 {
				continue
			}
			eventsPath := filepath.Join(yearPath, md.Name(), "generated", "events.json")
			data, err := os.ReadFile(eventsPath)
			if err != nil {
				continue
			}
			var ef FullEventsFile
			if json.Unmarshal(data, &ef) != nil {
				continue
			}
			for _, e := range ef.Events {
				t, _ := time.Parse(time.RFC3339, e.StartAt)
				if t.IsZero() {
					t, _ = time.Parse("2006-01-02T15:04:05.000Z", e.StartAt)
				}
				if t.After(now) || t.Equal(now) {
					events = append(events, e)
				}
			}
		}
	}

	sort.Slice(events, func(i, j int) bool {
		return events[i].StartAt < events[j].StartAt
	})

	baseURL := "https://commonshub.brussels"

	var eventsMarkdown string
	if len(events) == 0 {
		eventsMarkdown = fmt.Sprintf("No upcoming events found. Check our [website](%s) for the latest updates.", baseURL)
	} else {
		var parts []string
		for _, e := range events {
			t, _ := time.Parse(time.RFC3339, e.StartAt)
			if t.IsZero() {
				t, _ = time.Parse("2006-01-02T15:04:05.000Z", e.StartAt)
			}

			lines := []string{fmt.Sprintf("### %s", e.Name), ""}
			lines = append(lines, fmt.Sprintf("- **Date**: %s", FormatDateLong(t)))

			startTime := FormatTimeBrussels(t)
			if e.EndAt != "" {
				endT, _ := time.Parse(time.RFC3339, e.EndAt)
				if endT.IsZero() {
					endT, _ = time.Parse("2006-01-02T15:04:05.000Z", e.EndAt)
				}
				if !endT.IsZero() {
					lines = append(lines, fmt.Sprintf("- **Time**: %s - %s (Brussels time)", startTime, FormatTimeBrussels(endT)))
				} else {
					lines = append(lines, fmt.Sprintf("- **Time**: %s (Brussels time)", startTime))
				}
			} else {
				lines = append(lines, fmt.Sprintf("- **Time**: %s (Brussels time)", startTime))
			}

			if e.Location != "" && !strings.Contains(strings.ToLower(e.Location), "commons hub") {
				lines = append(lines, fmt.Sprintf("- **Location**: %s", e.Location))
			} else {
				lines = append(lines, "- **Location**: Commons Hub Brussels, Rue de la Madeleine 51, 1000 Brussels")
			}

			if e.URL != "" {
				lines = append(lines, fmt.Sprintf("- **Link**: [Event page](%s)", e.URL))
			}

			desc := TruncateDescription(e.Description, 200)
			if desc != "" {
				lines = append(lines, "", desc)
			}

			parts = append(parts, strings.Join(lines, "\n"))
		}
		eventsMarkdown = strings.Join(parts, "\n\n---\n\n")
	}

	icsLine := ""

	content := fmt.Sprintf(`# Upcoming Events at Commons Hub Brussels

> Events and community gatherings at Commons Hub Brussels, Rue de la Madeleine 51, 1000 Brussels.

This file is automatically generated. Last updated: %s

%s## Upcoming Events

%s

---

## Host Your Own Event

Want to host an event at Commons Hub Brussels? [Contact us](%s/contact) or [book a room](%s/rooms).
`, time.Now().UTC().Format(time.RFC3339), icsLine, eventsMarkdown, baseURL, baseURL)

	latestDir := filepath.Join(dataDir, "latest")
	os.MkdirAll(latestDir, 0755)
	os.WriteFile(filepath.Join(latestDir, "events.md"), []byte(content), 0644)
}

func generateRoomsMd(dataDir string) {
	rooms, err := LoadRooms()
	if err != nil || len(rooms) == 0 {
		return
	}

	baseURL := "https://commonshub.brussels"

	var parts []string
	for _, room := range rooms {
		lines := []string{
			fmt.Sprintf("### %s", room.Name),
			"",
			room.Description,
			"",
			fmt.Sprintf("- **Capacity**: Up to %d people", room.Capacity),
		}

		if room.PricePerHour > 0 {
			lines = append(lines, fmt.Sprintf("- **Price**: %.0f EUR/hour + VAT", room.PricePerHour))
			if room.TokensPerHour != float64(int(room.TokensPerHour)) {
				lines = append(lines, fmt.Sprintf("- **Token price**: %.1f CHT/hour", room.TokensPerHour))
			} else {
				lines = append(lines, fmt.Sprintf("- **Token price**: %.0f CHT/hour", room.TokensPerHour))
			}
		}

		if room.MembershipReq {
			lines = append(lines, "- **Access**: Members only")
		}

		if len(room.Features) > 0 {
			lines = append(lines, fmt.Sprintf("- **Features**: %s", strings.Join(room.Features, ", ")))
		}

		if len(room.IdealFor) > 0 {
			lines = append(lines, fmt.Sprintf("- **Ideal for**: %s", strings.Join(room.IdealFor, ", ")))
		}

		lines = append(lines, fmt.Sprintf("- **Details**: [%s](%s/rooms/%s)", room.Name, baseURL, room.Slug))

		if room.GoogleCalendarID != nil {
			lines = append(lines, fmt.Sprintf("- **Calendar (ICS)**: [%s.ics](%s/rooms/%s.ics)", room.Slug, baseURL, room.Slug))
		}

		parts = append(parts, strings.Join(lines, "\n"))
	}

	roomsMarkdown := strings.Join(parts, "\n\n---\n\n")

	content := fmt.Sprintf(`# Rooms at Commons Hub Brussels

> Versatile spaces for events, workshops, meetings, and community gatherings at Rue de la Madeleine 51, 1000 Brussels.

This file is automatically generated. Last updated: %s

## Available Spaces

%s

---

## Booking

Rooms can be booked by visiting the individual room pages above and filling out the booking form. Members can also pay with Commons Hub Tokens (CHT).

For questions about bookings, contact us at hello@commonshub.brussels or visit [commonshub.brussels/contact](%s/contact).
`, time.Now().UTC().Format(time.RFC3339), roomsMarkdown, baseURL)

	latestDir := filepath.Join(dataDir, "latest")
	os.MkdirAll(latestDir, 0755)
	os.WriteFile(filepath.Join(latestDir, "rooms.md"), []byte(content), 0644)
}

