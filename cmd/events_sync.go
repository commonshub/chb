package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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

// CalendarsSync fetches room calendars once and produces both bookings (all events)
// and events (bookings with a public URL, enriched with og:image).
// Returns (newBookings, newEvents, error).
func CalendarsSync(args []string) (int, int, error) {
	if HasFlag(args, "--help", "-h", "help") {
		PrintEventsSyncHelp()
		return 0, 0, nil
	}

	force := HasFlag(args, "--force")
	sinceStr := GetOption(args, "--since")

	// Positional year/month arg (e.g. "2025" or "2025/11")
	posYear, posMonth, posFound := ParseYearMonthArg(args)

	dataDir := DataDir()

	rooms, err := LoadRooms()
	if err != nil {
		return 0, 0, fmt.Errorf("failed to load rooms: %w", err)
	}

	now := time.Now()

	// Determine time range
	var sinceMonth, untilMonth string
	resolvedSince, isSince := ResolveSinceMonth(args, "calendars")
	isFullSync := false

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
		isFullSync = true
	} else if sinceStr != "" {
		if d, ok := ParseSinceDate(sinceStr); ok {
			sinceMonth = fmt.Sprintf("%d-%02d", d.Year(), d.Month())
		}
	}

	// Default: use last sync month, or full history if never synced
	if sinceMonth == "" {
		lastSync := LastSyncMonth("calendars")
		if lastSync == "" {
			// Never synced before — do a full sync
			sinceMonth = "2024-01"
			isFullSync = true
		} else {
			sinceMonth = lastSync
		}
	}

	if untilMonth == "" {
		future := time.Date(now.Year(), now.Month()+3, 1, 0, 0, 0, 0, time.UTC)
		untilMonth = fmt.Sprintf("%d-%02d", future.Year(), future.Month())
	}

	fmt.Printf("\n%sDATA_DIR: %s%s\n", Fmt.Dim, dataDir, Fmt.Reset)
	fmt.Printf("%sMonth range: %s → %s%s\n", Fmt.Dim, sinceMonth, untilMonth, Fmt.Reset)

	// Fetch ICS from all room calendars (once)
	fmt.Printf("\n📅 Fetching room calendars...\n")

	type roomFetch struct {
		room   RoomInfo
		events []ical.Event
	}
	var fetched []roomFetch
	totalBookings := 0

	for _, room := range rooms {
		if room.GoogleCalendarID == nil {
			continue
		}

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

		fetched = append(fetched, roomFetch{room: room, events: events})
		fmt.Printf("  %s✓%s %s: %d events\n", Fmt.Green, Fmt.Reset, room.Slug, len(events))
	}

	if len(fetched) == 0 {
		fmt.Printf("  %sNo rooms with Google Calendar IDs found.%s\n", Fmt.Yellow, Fmt.Reset)
		return 0, 0, nil
	}

	// --- Bookings: write {room}.ics per room per month ---
	newBookingCount := 0
	for _, rf := range fetched {
		byMonth := ical.GroupByMonth(rf.events)
		for ym, monthEvents := range byMonth {
			if ym < sinceMonth || ym > untilMonth {
				continue
			}
			parts := strings.SplitN(ym, "-", 2)
			year, month := parts[0], parts[1]

			relPath := filepath.Join("calendars", "ics", rf.room.Slug+".ics")
			filePath := filepath.Join(dataDir, year, month, relPath)

			if !force {
				if _, err := os.Stat(filePath); err == nil {
					// File exists — count bookings but don't rewrite
					totalBookings += len(monthEvents)
					continue
				}
			}

			content := ical.WrapICS(monthEvents, fmt.Sprintf("-//Commons Hub Brussels//%s//EN", rf.room.Name))
			writeMonthFile(dataDir, year, month, relPath, []byte(content))
			newBookingCount += len(monthEvents)
			totalBookings += len(monthEvents)
		}
	}

	// --- Fetch event calendars (Luma, Google) from settings ---
	settings, _ := LoadSettings()
	type calSource struct {
		slug string
		url  string
	}
	var calSources []calSource
	if settings.Calendars.Luma != "" {
		calSources = append(calSources, calSource{"luma", settings.Calendars.Luma})
	}
	if settings.Calendars.Google != "" {
		calSources = append(calSources, calSource{"google", settings.Calendars.Google})
	}

	for _, cs := range calSources {
		fmt.Printf("  Fetching %s calendar...\n", cs.slug)
		icsData, err := fetchURL(cs.url)
		if err != nil {
			fmt.Printf("  %sWarning: failed to fetch %s calendar: %v%s\n", Fmt.Yellow, cs.slug, err, Fmt.Reset)
			continue
		}
		events, err := ical.ParseICS(icsData)
		if err != nil {
			fmt.Printf("  %sWarning: failed to parse %s calendar: %v%s\n", Fmt.Yellow, cs.slug, err, Fmt.Reset)
			continue
		}
		fmt.Printf("  %s✓%s %s calendar: %d events\n", Fmt.Green, Fmt.Reset, cs.slug, len(events))
		fetched = append(fetched, roomFetch{
			room:   RoomInfo{Slug: cs.slug, Name: cs.slug},
			events: events,
		})
	}

	// --- Events: filter bookings with URLs, enrich with og:image ---
	var allRoomEvents []roomEvent
	for _, rf := range fetched {
		for _, ev := range rf.events {
			eventURL := extractEventURL(ev)
			if eventURL == "" {
				continue
			}
			allRoomEvents = append(allRoomEvents, roomEvent{event: ev, roomSlug: rf.room.Slug, roomName: rf.room.Name})
		}
	}

	// Group events by month
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

	// Save public.ics per month (events with URLs only)
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

	// Process each month (og:image scraping, events.json generation)
	var results []monthResult
	newEventCount := 0
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
			newEventCount += len(r.newEvents)
		}
	}

	// Generate yearly aggregates
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

	// Print new events detail
	for _, r := range results {
		if len(r.newEvents) > 0 {
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

	bookings, _ := loadAllBookings()
	var upcomingBookings []BookingEntry
	for _, b := range bookings {
		if b.Start.After(now) {
			upcomingBookings = append(upcomingBookings, b)
		}
	}

	fmt.Printf("\n%s✓ Done!%s %d bookings (%d upcoming), %d events (%d upcoming)\n",
		Fmt.Green, Fmt.Reset,
		totalBookings, len(upcomingBookings),
		len(allEvents), len(futureEvents))

	// Update sync state
	UpdateSyncSource("calendars", isFullSync)

	return newBookingCount, newEventCount, nil
}

// extractEventURL returns the public URL for an ICS event, or empty string if none.
func extractEventURL(ev ical.Event) string {
	eventURL := ev.URL
	if eventURL == "" && ev.Location != "" &&
		(strings.HasPrefix(ev.Location, "http://") || strings.HasPrefix(ev.Location, "https://")) {
		eventURL = ev.Location
	}
	if eventURL == "" && ev.Description != "" {
		re := regexp.MustCompile(`https?://[^\s\n<>"']+`)
		if m := re.FindString(ev.Description); m != "" {
			eventURL = strings.TrimRight(m, ".,;:!?")
		}
	}
	return eventURL
}

// EventsSync is an alias for CalendarsSync for backwards compatibility.
func EventsSync(args []string, version string) error {
	_, _, err := CalendarsSync(args)
	return err
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
	existingEvents := map[string]FullEvent{}
	existingPath := filepath.Join(monthPath, "generated", "events.json")
	if data, err := os.ReadFile(existingPath); err == nil {
		var ef FullEventsFile
		if json.Unmarshal(data, &ef) == nil {
			for _, e := range ef.Events {
				existingIDs[e.ID] = true
				existingMetadata[e.ID] = e.Metadata
				existingEvents[e.ID] = e
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
		eventURL := extractEventURL(icsEv)
		location := icsEv.Location

		// If location is a URL, use default address instead
		if location != "" && (strings.HasPrefix(location, "http://") || strings.HasPrefix(location, "https://")) {
			location = "Commons Hub Brussels, Rue de la Madeleine 51, 1000 Bruxelles, Belgium"
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
		var coverImage, coverImageLocal string
		// Preserve existing cover image if already synced
		if existing, ok := existingEvents[eventID]; ok {
			coverImage = existing.CoverImage
			coverImageLocal = existing.CoverImageLocal
		}
		if coverImage == "" {
			ogImg := og.FetchOGImage(eventURL)
			if ogImg != "" {
				coverImage = ogImg
			}
		}
		// Download cover image locally if we have a URL but no local copy
		if coverImage != "" && coverImageLocal == "" {
			ext := extFromURL(coverImage, ".jpg")
			localRelPath := filepath.Join("images", "covers", eventID+ext)
			localAbsPath := filepath.Join(dataDir, year, month, localRelPath)
			os.MkdirAll(filepath.Dir(localAbsPath), 0755)
			if err := downloadFile(coverImage, localAbsPath); err == nil {
				coverImageLocal = filepath.Join(year, month, localRelPath)
				fmt.Printf("    ↳ downloaded cover for %s\n", name)
			}
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
			ID:              eventID,
			Name:            name,
			Description:     icsEv.Description,
			StartAt:         startAt,
			EndAt:           endAt,
			Location:        location,
			URL:             eventURL,
			CoverImage:      coverImage,
			CoverImageLocal: coverImageLocal,
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

	latestDir := filepath.Join(dataDir, "latest", "generated")
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

	latestDir := filepath.Join(dataDir, "latest", "generated")
	os.MkdirAll(latestDir, 0755)
	os.WriteFile(filepath.Join(latestDir, "rooms.md"), []byte(content), 0644)
}

