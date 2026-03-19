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
	"github.com/CommonsHub/chb/luma"
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
	lumaAPIKey := os.Getenv("LUMA_API_KEY")

	settings, err := LoadSettings()
	if err != nil {
		return fmt.Errorf("failed to load settings: %w", err)
	}

	lumaIcsURL := settings.Calendars.Luma
	calendarID := settings.Luma.CalendarID

	// Show env info
	fmt.Printf("\n%sDATA_DIR: %s%s\n", Fmt.Dim, dataDir, Fmt.Reset)
	lumaKeyStatus := "missing (falling back to OG scraping)"
	if lumaAPIKey != "" {
		lumaKeyStatus = "set"
	}
	fmt.Printf("%sLUMA_API_KEY: %s%s\n", Fmt.Dim, lumaKeyStatus, Fmt.Reset)

	// Step 1: Fetch ICS feed
	fmt.Printf("\n📅 Fetching Luma calendar...\n")
	fmt.Printf("  %s%s%s\n", Fmt.Dim, lumaIcsURL, Fmt.Reset)

	icsData, err := fetchURL(lumaIcsURL)
	if err != nil {
		return fmt.Errorf("failed to fetch ICS: %w", err)
	}

	events, err := ical.ParseICS(icsData)
	if err != nil {
		return fmt.Errorf("failed to parse ICS: %w", err)
	}

	// Count upcoming
	now := time.Now()
	upcoming := 0
	for _, e := range events {
		if e.Start.After(now) {
			upcoming++
		}
	}
	fmt.Printf("  %d events in ICS (%d upcoming)\n", len(events), upcoming)

	// Group by month and save ICS files
	byMonth := ical.GroupByMonth(events)

	// Determine which months to process based on --since/--history or positional year/month
	var sinceMonth string
	var untilMonth string // exclusive upper bound (empty = no upper bound)

	// Check --since / --history
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
		// Legacy --since YYYYMMDD support
		if d, ok := ParseSinceDate(sinceStr); ok {
			sinceMonth = fmt.Sprintf("%d-%02d", d.Year(), d.Month())
		}
	}

	// Save ICS files per month
	affectedMonths := []string{}
	icsCountsByMonth := map[string]int{}

	for ym, monthEvents := range byMonth {
		if sinceMonth != "" && ym < sinceMonth {
			continue
		}
		if untilMonth != "" && ym > untilMonth {
			continue
		}
		affectedMonths = append(affectedMonths, ym)
		icsCountsByMonth[ym] = len(monthEvents)

		parts := strings.SplitN(ym, "-", 2)
		year, month := parts[0], parts[1]

		icsDir := filepath.Join(dataDir, year, month, "calendars", "ics")
		os.MkdirAll(icsDir, 0755)

		icsPath := filepath.Join(icsDir, "public.ics")
		if !force {
			if _, err := os.Stat(icsPath); err == nil {
				// Already exists, still overwrite to keep fresh
			}
		}

		content := ical.WrapICS(monthEvents, "-//Commons Hub Brussels//Luma//EN")
		writeMonthFile(dataDir, year, month, filepath.Join("calendars", "ics", "public.ics"), []byte(content))
	}
	sort.Strings(affectedMonths)

	// Fetch Luma API data per month if key is set
	if lumaAPIKey != "" && calendarID != "" {
		for _, ym := range affectedMonths {
			parts := strings.SplitN(ym, "-", 2)
			year, month := parts[0], parts[1]
			fetchLumaForMonth(dataDir, calendarID, year, month, force)
		}
	}

	// Determine which months need regeneration
	monthsToProcess := []struct{ year, month string }{}
	skippedCount := 0

	for _, ym := range affectedMonths {
		parts := strings.SplitN(ym, "-", 2)
		year, month := parts[0], parts[1]
		icsCount := icsCountsByMonth[ym]

		if !force {
			existingPath := filepath.Join(dataDir, year, month, "generated", "events.json")
			if data, err := os.ReadFile(existingPath); err == nil {
				var ef FullEventsFile
				if json.Unmarshal(data, &ef) == nil {
					if len(ef.Events) == icsCount {
						skippedCount++
						continue
					}
				}
			}
		}
		monthsToProcess = append(monthsToProcess, struct{ year, month string }{year, month})
	}

	if skippedCount > 0 {
		fmt.Printf("  %s%d months unchanged, %d to process%s\n", Fmt.Dim, skippedCount, len(monthsToProcess), Fmt.Reset)
	}

	// Process each month
	var results []monthResult
	for _, m := range monthsToProcess {
		r, err := processMonth(dataDir, calendarID, m.year, m.month)
		if err != nil {
			fmt.Fprintf(os.Stderr, "  Warning: error processing %s-%s: %v\n", m.year, m.month, err)
			continue
		}
		if r != nil {
			results = append(results, *r)
		}
	}

	// Generate yearly aggregates for affected years
	years := map[string]bool{}
	for _, m := range monthsToProcess {
		years[m.year] = true
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

	ownCount := 0
	communityCount := 0
	for _, e := range futureEvents {
		if e.CalendarSource == "luma-api" {
			ownCount++
		} else {
			communityCount++
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

	// Count events.md entries
	eventsMdPath := filepath.Join("public", "events.md")
	eventsMdCount := 0
	if data, err := os.ReadFile(eventsMdPath); err == nil {
		re := regexp.MustCompile(`(?m)^### `)
		eventsMdCount = len(re.FindAll(data, -1))
	}

	fmt.Printf("\n%s✓ Done!%s %d total events, %d upcoming\n", Fmt.Green, Fmt.Reset, len(allEvents), len(futureEvents))
	fmt.Printf("  %sown: %d (via Luma API) · community: %d%s\n", Fmt.Dim, ownCount, communityCount, Fmt.Reset)

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

func fetchLumaForMonth(dataDir, calendarID, year, month string, force bool) {
	lumaDir := filepath.Join(dataDir, year, month, "calendars", "luma")
	lumaPath := filepath.Join(lumaDir, calendarID+".json")

	if !force {
		if _, err := os.Stat(lumaPath); err == nil {
			return // Already exists
		}
	}

	monthStart := fmt.Sprintf("%s-%s-01T00:00:00Z", year, month)
	nextMonth := month
	nextYear := year
	if month == "12" {
		nextMonth = "01"
		y := 0
		fmt.Sscanf(year, "%d", &y)
		nextYear = fmt.Sprintf("%d", y+1)
	} else {
		m := 0
		fmt.Sscanf(month, "%d", &m)
		nextMonth = fmt.Sprintf("%02d", m+1)
	}
	monthEnd := fmt.Sprintf("%s-%s-01T00:00:00Z", nextYear, nextMonth)

	entries, err := luma.GetAllCalendarEvents(calendarID, monthStart, monthEnd)
	if err != nil || entries == nil {
		return
	}

	// Flatten: extract event + tags from entries
	type flatEvent struct {
		luma.Event
		Tags json.RawMessage `json:"tags,omitempty"`
	}
	var flat []flatEvent
	for _, entry := range entries {
		e := entry.Event
		if e.APIID == "" {
			e.APIID = entry.APIID
		}
		flat = append(flat, flatEvent{Event: e, Tags: entry.Tags})
	}

	data, _ := json.MarshalIndent(flat, "", "  ")
	writeMonthFile(dataDir, year, month, filepath.Join("calendars", "luma", calendarID+".json"), data)
}

func processMonth(dataDir, calendarID, year, month string) (*monthResult, error) {
	monthPath := filepath.Join(dataDir, year, month)
	if _, err := os.Stat(monthPath); os.IsNotExist(err) {
		return nil, nil
	}

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

	// Load Luma API cached data
	lumaEventsMap := map[string]*luma.Event{}
	lumaEventsNameMap := map[string]*luma.Event{}
	if calendarID != "" {
		lumaPath := filepath.Join(dataDir, year, month, "calendars", "luma", calendarID+".json")
		if data, err := os.ReadFile(lumaPath); err == nil {
			var lumaEvents []luma.Event
			if json.Unmarshal(data, &lumaEvents) == nil {
				for i := range lumaEvents {
					e := &lumaEvents[i]
					lumaEventsMap[e.APIID] = e
					if e.Name != "" {
						lumaEventsNameMap[strings.ToLower(e.Name)] = e
					}
				}
			}
		}
	}

	// Load ICS events
	icsPath := filepath.Join(dataDir, year, month, "calendars", "ics", "public.ics")
	icsData, err := os.ReadFile(icsPath)
	if err != nil {
		return nil, nil // No ICS data for this month
	}

	icsEvents, err := ical.ParseICS(string(icsData))
	if err != nil {
		return nil, err
	}

	processedIDs := map[string]bool{}
	var newEvents []newEventInfo
	var fullEvents []FullEvent

	for _, icsEv := range icsEvents {
		eventID := strings.TrimSuffix(icsEv.UID, "@events.lu.ma")
		name := icsEv.Summary
		eventURL := icsEv.URL
		location := icsEv.Location
		source := "ical"
		calSrc := "luma"
		metadataSource := "none"

		// Check if location is a URL
		if location != "" && (strings.HasPrefix(location, "http://") || strings.HasPrefix(location, "https://")) {
			eventURL = location
			location = "Commons Hub Brussels, Rue de la Madeleine 51, 1000 Bruxelles, Belgium"
		}

		// Try to find Luma data — check cache first, avoid API calls
		var lumaEv *luma.Event

		// 1. Direct match by evt- API ID (most reliable)
		if strings.HasPrefix(eventID, "evt-") {
			lumaEv = lumaEventsMap[eventID]
		}

		// 2. Match by slug extracted from URL
		if lumaEv == nil {
			lumaSlug := extractLumaSlug(eventURL)
			if lumaSlug == "" {
				lumaSlug = extractLumaSlug(icsEv.Location)
			}
			if lumaSlug != "" {
				lumaEv = lumaEventsMap[lumaSlug]
			}
		}

		// 3. Match by name
		if lumaEv == nil {
			lumaEv = lumaEventsNameMap[strings.ToLower(name)]
		}

		// 4. No API call — events not in cache are community events
		//    we don't own (GetEvent returns 403 anyway). Fall through
		//    to OG scraping instead.

		var coverImage string
		startAt := icsEv.Start.Format(time.RFC3339)
		endAt := ""
		if !icsEv.End.IsZero() {
			endAt = icsEv.End.Format(time.RFC3339)
		}

		if lumaEv != nil {
			eventID = lumaEv.APIID
			source = "luma"
			calSrc = "luma-api"
			coverImage = lumaEv.CoverURL
			eventURL = lumaEv.URL
			if lumaEv.StartAt != "" {
				startAt = lumaEv.StartAt
			}
			if lumaEv.EndAt != "" {
				endAt = lumaEv.EndAt
			}
			metadataSource = "Luma API"

			// Parse geo address
			if lumaEv.GeoAddressJSON != nil {
				var geo luma.GeoAddress
				if json.Unmarshal(lumaEv.GeoAddressJSON, &geo) == nil && geo.FullAddress != "" {
					location = geo.FullAddress
				}
			}
		} else {
			// Try to extract URL from description if none
			if eventURL == "" && icsEv.Description != "" {
				re := regexp.MustCompile(`https?://[^\s\n<>"']+`)
				if m := re.FindString(icsEv.Description); m != "" {
					eventURL = strings.TrimRight(m, ".,;:!?")
				}
			}

			// Fallback: scrape og:image
			if eventURL != "" {
				ogImg := og.FetchOGImage(eventURL)
				if ogImg != "" {
					coverImage = ogImg
					metadataSource = "scraping"
				}
			}
		}

		if processedIDs[eventID] {
			continue
		}
		processedIDs[eventID] = true

		// Track new events
		if !existingIDs[eventID] {
			newEvents = append(newEvents, newEventInfo{name, startAt, metadataSource})
		}

		// Preserve existing metadata
		metadata := existingMetadata[eventID]

		// Build tags JSON from lumaEv
		var tagsJSON json.RawMessage
		if lumaEv != nil && lumaEv.Tags != nil {
			tagsJSON = lumaEv.Tags
		}

		// Build lumaData JSON
		var lumaDataJSON json.RawMessage
		if lumaEv != nil {
			ld := map[string]interface{}{
				"api_id":           lumaEv.APIID,
				"start_at":         lumaEv.StartAt,
				"end_at":           lumaEv.EndAt,
				"timezone":         lumaEv.Timezone,
				"url":              lumaEv.URL,
				"cover_url":        lumaEv.CoverURL,
				"geo_address_json": lumaEv.GeoAddressJSON,
				"meeting_url":      lumaEv.MeetingURL,
				"visibility":       lumaEv.Visibility,
				"event_type":       lumaEv.EventType,
				"capacity":         lumaEv.Capacity,
				"guest_count":      lumaEv.GuestCount,
				"hosts":            lumaEv.Hosts,
				"hosted_by":        lumaEv.HostedBy,
			}
			lumaDataJSON, _ = json.Marshal(ld)
		}

		// Description priority
		desc := ""
		if lumaEv != nil && lumaEv.Description != "" {
			desc = lumaEv.Description
		} else {
			desc = icsEv.Description
		}

		tz := ""
		if lumaEv != nil {
			tz = lumaEv.Timezone
		}

		fullEvents = append(fullEvents, FullEvent{
			ID:             eventID,
			Name:           name,
			Description:    desc,
			StartAt:        startAt,
			EndAt:          endAt,
			Timezone:       tz,
			Location:       location,
			URL:            eventURL,
			CoverImage:     coverImage,
			Source:          source,
			CalendarSource: calSrc,
			Tags:           tagsJSON,
			LumaData:       lumaDataJSON,
			Metadata:       metadata,
		})
	}

	// Add Luma API events not in ICS
	if calendarID != "" {
		lumaPath := filepath.Join(dataDir, year, month, "calendars", "luma", calendarID+".json")
		if data, err := os.ReadFile(lumaPath); err == nil {
			var lumaAPIEvents []luma.Event
			if json.Unmarshal(data, &lumaAPIEvents) == nil {
				for _, le := range lumaAPIEvents {
					if le.APIID == "" || processedIDs[le.APIID] {
						continue
					}
					if le.StartAt == "" {
						continue
					}
					processedIDs[le.APIID] = true

					metadata := existingMetadata[le.APIID]

					var tagsJSON json.RawMessage
					if le.Tags != nil {
						tagsJSON = le.Tags
					}

					ld := map[string]interface{}{
						"api_id":           le.APIID,
						"start_at":         le.StartAt,
						"end_at":           le.EndAt,
						"timezone":         le.Timezone,
						"url":              le.URL,
						"cover_url":        le.CoverURL,
						"geo_address_json": le.GeoAddressJSON,
						"meeting_url":      le.MeetingURL,
						"visibility":       le.Visibility,
						"event_type":       le.EventType,
						"capacity":         le.Capacity,
						"guest_count":      le.GuestCount,
						"hosts":            le.Hosts,
						"hosted_by":        le.HostedBy,
					}
					lumaDataJSON, _ := json.Marshal(ld)

					location := ""
					if le.GeoAddressJSON != nil {
						var geo luma.GeoAddress
						if json.Unmarshal(le.GeoAddressJSON, &geo) == nil {
							location = geo.FullAddress
						}
					}

					fullEvents = append(fullEvents, FullEvent{
						ID:             le.APIID,
						Name:           le.Name,
						Description:    le.Description,
						StartAt:        le.StartAt,
						EndAt:          le.EndAt,
						Timezone:       le.Timezone,
						Location:       location,
						URL:            le.URL,
						CoverImage:     le.CoverURL,
						Source:         "luma",
						CalendarSource: "luma-api",
						Tags:           tagsJSON,
						LumaData:       lumaDataJSON,
						Metadata:       metadata,
					})

					if !existingIDs[le.APIID] {
						newEvents = append(newEvents, newEventInfo{le.Name, le.StartAt, "Luma API"})
					}
				}
			}
		}
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
	generateRoomsMd()
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

	settings, _ := LoadSettings()
	icsURL := ""
	if settings != nil {
		icsURL = settings.Calendars.Google
	}

	baseURL := "https://commonshub.brussels"

	var eventsMarkdown string
	if len(events) == 0 {
		eventsMarkdown = fmt.Sprintf("No upcoming events found. Check our [Luma calendar](https://lu.ma/commonshub) or [website](%s) for the latest updates.", baseURL)
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
	if icsURL != "" {
		icsLine = fmt.Sprintf("- [Google Calendar (ICS)](%s)\n", icsURL)
	}

	content := fmt.Sprintf(`# Upcoming Events at Commons Hub Brussels

> Events and community gatherings at Commons Hub Brussels, Rue de la Madeleine 51, 1000 Brussels.

This file is automatically generated. Last updated: %s

## Calendar

You can subscribe to our calendar:
- [Luma calendar](https://lu.ma/commonshub)
%s
## Upcoming Events

%s

---

## Host Your Own Event

Want to host an event at Commons Hub Brussels? [Contact us](%s/contact) or [book a room](%s/rooms).
`, time.Now().UTC().Format(time.RFC3339), icsLine, eventsMarkdown, baseURL, baseURL)

	os.MkdirAll("public", 0755)
	os.WriteFile(filepath.Join("public", "events.md"), []byte(content), 0644)
}

func generateRoomsMd() {
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

	os.MkdirAll("public", 0755)
	os.WriteFile(filepath.Join("public", "rooms.md"), []byte(content), 0644)
}

var lumaSlugRe = regexp.MustCompile(`lu\.ma/([a-zA-Z0-9-]+)`)

func extractLumaSlug(text string) string {
	if text == "" {
		return ""
	}
	m := lumaSlugRe.FindStringSubmatch(text)
	if m != nil {
		return m[1]
	}
	return ""
}
