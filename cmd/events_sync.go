package cmd

// events_sync.go owns the *sync* side of the events pipeline: fetching raw
// ICS feeds from configured calendar providers and archiving them under
// providers/ics/<slug>.ics. The only generated artifact written here is the
// per-month bookings archive (filtered VEVENTs we want to keep alongside the
// raw feed). Every other derived output — events.json, public.ics, yearly
// aggregates, CSV, markdown — lives in events_generate.go. See
// docs/philosophy.md.

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
	icssource "github.com/CommonsHub/chb/providers/ics"
)

var calendarHTTPClient = &http.Client{Timeout: 20 * time.Second}

// ── Diagnostics (shared with events_generate.go) ─────────────────────────────

type calendarSyncDiagnostics struct {
	warnings map[string]int
	errors   map[string]int
}

func newCalendarSyncDiagnostics() *calendarSyncDiagnostics {
	return &calendarSyncDiagnostics{
		warnings: map[string]int{},
		errors:   map[string]int{},
	}
}

func (d *calendarSyncDiagnostics) Warn(kind, format string, args ...interface{}) {
	if d == nil {
		return
	}
	kind = strings.TrimSpace(kind)
	if kind == "" {
		kind = "warning"
	}
	d.warnings[kind]++
	LogWarningf("calendars sync: %s: %s", kind, fmt.Sprintf(format, args...))
}

func (d *calendarSyncDiagnostics) Error(kind, format string, args ...interface{}) {
	if d == nil {
		return
	}
	kind = strings.TrimSpace(kind)
	if kind == "" {
		kind = "error"
	}
	d.errors[kind]++
	LogErrorf("calendars sync: %s: %s", kind, fmt.Sprintf(format, args...))
}

func (d *calendarSyncDiagnostics) PrintSummary() {
	if d == nil || (len(d.warnings) == 0 && len(d.errors) == 0) {
		return
	}
	fmt.Printf("\n%sCalendar sync diagnostics%s\n", Fmt.Bold, Fmt.Reset)
	for _, kind := range sortedDiagnosticKinds(d.errors) {
		fmt.Printf("  %s%s%s: %s\n", Fmt.Red, Pluralize(d.errors[kind], "error", ""), Fmt.Reset, kind)
	}
	for _, kind := range sortedDiagnosticKinds(d.warnings) {
		fmt.Printf("  %s%s%s: %s\n", Fmt.Yellow, Pluralize(d.warnings[kind], "warning", ""), Fmt.Reset, kind)
	}
	if path := DiagnosticsLogPath(); path != "" {
		fmt.Printf("  %sDetails: %s%s\n", Fmt.Dim, path, Fmt.Reset)
	}
}

func sortedDiagnosticKinds(counts map[string]int) []string {
	kinds := make([]string, 0, len(counts))
	for kind := range counts {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	return kinds
}

// ── CalendarsSync orchestrator ──────────────────────────────────────────────

// CalendarsSync fetches every configured ICS feed, archives the booking events
// per month under providers/ics/<slug>.ics, then hands the in-memory parsed
// events over to events_generate.go to produce every derived artifact under
// generated/.
//
// Returns (newBookings, newEvents, error).
func CalendarsSync(args []string) (int, int, error) {
	if HasFlag(args, "--help", "-h", "help") {
		PrintCalendarsSyncHelp()
		return 0, 0, nil
	}

	force := HasFlag(args, "--force")
	debug := HasFlag(args, "--debug")

	// Positional date/month/year range arg (e.g. "2025", "2025/11", "2025/Q1")
	posStartMonth, posEndMonth, posFound := ParseMonthRangeArg(args)

	dataDir := DataDir()
	runCache := newEventSyncRunCache(dataDir, debug)
	defer runCache.save()
	diagnostics := newCalendarSyncDiagnostics()

	rooms, err := LoadRooms()
	if err != nil {
		return 0, 0, fmt.Errorf("failed to load rooms: %w", err)
	}

	now := time.Now().In(BrusselsTZ())

	// Determine time range
	var sinceMonth, untilMonth string
	resolvedSince, isSince := ResolveSinceMonth(args, icssource.RelPath())
	isFullSync := false

	if posFound {
		sinceMonth = posStartMonth
		untilMonth = posEndMonth
	} else if isSince {
		sinceMonth = resolvedSince
		isFullSync = true
	}

	// Default: keep the recent window plus upcoming events.
	// Historical backfills require an explicit --history, --since, or YYYY[/MM].
	if sinceMonth == "" {
		sinceMonth = DefaultRecentStartMonth(now)
	}

	if untilMonth == "" {
		future := time.Date(now.Year(), now.Month()+3, 1, 0, 0, 0, 0, time.UTC)
		untilMonth = fmt.Sprintf("%d-%02d", future.Year(), future.Month())
	}

	fmt.Printf("\n%sDATA_DIR: %s%s\n", Fmt.Dim, dataDir, Fmt.Reset)
	fmt.Printf("%sMonth range: %s → %s%s\n", Fmt.Dim, sinceMonth, untilMonth, Fmt.Reset)
	rangeLabel := eventSyncRangeLabel(sinceMonth, untilMonth)

	// Fetch configured calendar resources once. The provider selects the parser;
	// the optional room reference controls whether entries are room bookings.
	fmt.Printf("\n📅 Fetching calendars...\n")

	type calendarFetch struct {
		slug           string
		name           string
		visibility     string
		events         []ical.Event
		allRoomBooking bool
	}
	var fetched []calendarFetch
	totalBookings := 0
	newBookingCount := 0

	settings, _ := LoadSettings()
	var calSources []CalendarSourceConfig
	if settings != nil {
		calSources = settings.Calendars.Sources
	}
	calSources = append(calSources, legacyRoomCalendarSources(rooms, calSources)...)
	roomCalendarRefs := roomCalendarSourceSlugs(rooms)

	for _, cs := range calSources {
		roomSource := strings.TrimSpace(cs.Room) != "" || roomCalendarRefs[cs.Slug]
		visibility := cs.Visibility
		if roomSource {
			visibility = CalendarVisibilityAuto
		}
		provider := normalizeCalendarProvider(cs.Provider, cs.URL)
		if provider != CalendarProviderICS {
			diagnostics.Warn("unsupported provider", "%s uses provider %q", cs.Slug, cs.Provider)
			continue
		}

		fmt.Printf("  Fetching %s calendar...\n", cs.Slug)
		icsData, err := fetchURL(cs.URL)
		if err != nil {
			diagnostics.Warn("fetch calendar failed", "%s: %v", cs.Slug, err)
			continue
		}
		events, err := ical.ParseICS(icsData)
		if err != nil {
			diagnostics.Warn("parse calendar failed", "%s: %v", cs.Slug, err)
			continue
		}
		eventsInRange := countICALEventsInMonthRange(events, sinceMonth, untilMonth)
		publicInRange := countPublicICALEventsInMonthRange(events, sinceMonth, untilMonth, visibility)
		bookingInRange := countBookingEventsInMonthRange(events, sinceMonth, untilMonth, visibility, roomSource)
		fmt.Printf("  %s✓%s %s calendar: %s %s, %d public, %d private (%s/%s, %d in feed)\n",
			Fmt.Green, Fmt.Reset, cs.Slug, Pluralize(eventsInRange, "event", ""), rangeLabel, publicInRange, bookingInRange, provider, visibility, len(events))
		fetched = append(fetched, calendarFetch{
			slug:           cs.Slug,
			name:           cs.Name,
			visibility:     visibility,
			events:         events,
			allRoomBooking: roomSource,
		})
	}

	if len(fetched) == 0 {
		fmt.Printf("  %sNo calendar sources found.%s\n", Fmt.Yellow, Fmt.Reset)
	}

	// --- Bookings: write private bookings per source per month ---
	for _, rf := range fetched {
		bookingEvents := filterBookingEvents(rf.events, rf.visibility, rf.allRoomBooking)
		byMonth := ical.GroupByMonth(bookingEvents)
		for ym, monthEvents := range byMonth {
			if ym < sinceMonth || ym > untilMonth {
				continue
			}
			parts := strings.SplitN(ym, "-", 2)
			year, month := parts[0], parts[1]

			relPath := icssource.RelPath(icssource.FileName(rf.slug))
			filePath := icssource.Path(dataDir, year, month, icssource.FileName(rf.slug))

			if !force {
				if _, err := os.Stat(filePath); err == nil {
					// File exists — count bookings but don't rewrite
					totalBookings += len(monthEvents)
					continue
				}
			}

			content := ical.WrapICS(monthEvents, fmt.Sprintf("-//Commons Hub Brussels//%s//EN", rf.name))
			writeMonthFile(dataDir, year, month, relPath, []byte(content))
			newBookingCount += len(monthEvents)
			totalBookings += len(monthEvents)
		}
	}

	// --- Events: classify public events and hand off to the generate phase ---
	var allRoomEvents []roomEvent
	for _, rf := range fetched {
		for _, ev := range rf.events {
			if !calendarEventIsPublic(ev, rf.visibility) {
				continue
			}
			allRoomEvents = append(allRoomEvents, roomEvent{event: ev, roomSlug: rf.slug, roomName: rf.name})
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

	filteredPublicEvents := 0
	for _, events := range eventsByMonth {
		filteredPublicEvents += len(events)
	}
	fmt.Printf("\n📎 Found %s %s across %s to process\n",
		Pluralize(filteredPublicEvents, "public event", ""), rangeLabel, Pluralize(len(sortedMonths), "month", ""))

	// Generate phase: produce every derived calendar artifact.
	newEventCount := generateCalendarsForMonths(dataDir, eventsByMonth, sortedMonths, force, runCache, diagnostics)

	// Final summary
	if posFound {
		scopedEvents := countCachedEventsInMonthRange(dataDir, sinceMonth, untilMonth)
		fmt.Printf("\n%s✓ Done!%s %d bookings, %d public events %s\n",
			Fmt.Green, Fmt.Reset,
			totalBookings, scopedEvents, rangeLabel)
		diagnostics.PrintSummary()

		// Update sync state
		UpdateSyncSource("calendars", isFullSync)
		UpdateSyncActivity(isFullSync)

		return newBookingCount, newEventCount, nil
	}

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
	diagnostics.PrintSummary()

	// Update sync state
	UpdateSyncSource("calendars", isFullSync)
	UpdateSyncActivity(isFullSync)

	return newBookingCount, newEventCount, nil
}

// ── URL classification helpers ──────────────────────────────────────────────

// extractEventURL returns the public URL for an ICS event, or empty string if none.
func extractEventURL(ev ical.Event) string {
	if isAllowedPublicEventURL(ev.URL) {
		return ev.URL
	}
	if ev.Location != "" &&
		(strings.HasPrefix(ev.Location, "http://") || strings.HasPrefix(ev.Location, "https://")) &&
		isAllowedPublicEventURL(ev.Location) {
		return ev.Location
	}
	return extractPublicURLFromText(ev.Description)
}

func autoCalendarEventHasPublicURL(ev ical.Event) bool {
	return isAllowedPublicEventURL(ev.URL) || extractPublicURLFromText(ev.Description) != ""
}

func extractPublicURLFromText(text string) string {
	if text == "" {
		return ""
	}
	re := regexp.MustCompile(`https?://[^\s\n<>"']+`)
	for _, m := range re.FindAllString(text, -1) {
		candidate := strings.TrimRight(m, ".,;:!?")
		if isAllowedPublicEventURL(candidate) {
			return candidate
		}
	}
	return ""
}

func isAllowedPublicEventURL(raw string) bool {
	if strings.TrimSpace(raw) == "" {
		return false
	}
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return false
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return false
	}
	host := strings.ToLower(u.Hostname())
	switch host {
	case "", "mail.google.com", "calendar.google.com", "meet.google.com":
		return false
	}
	// Luma host-only admin URLs (https://lu.ma/event/manage/<id> or
	// https://luma.com/event/manage/<id>) are not public — only the event
	// organizer can view them. Treat them as if no URL was provided.
	if (host == "lu.ma" || host == "luma.com") &&
		strings.HasPrefix(strings.ToLower(u.Path), "/event/manage/") {
		return false
	}
	return true
}

func fetchURL(rawURL string) (string, error) {
	resp, err := calendarHTTPClient.Get(rawURL)
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

// ── Calendar source resolution ──────────────────────────────────────────────

func legacyRoomCalendarSources(rooms []RoomInfo, configured []CalendarSourceConfig) []CalendarSourceConfig {
	configuredBySlug := map[string]bool{}
	configuredByRoom := map[string]bool{}
	for _, source := range configured {
		if source.Slug != "" {
			configuredBySlug[source.Slug] = true
		}
		if source.Room != "" {
			configuredByRoom[source.Room] = true
		}
	}

	var out []CalendarSourceConfig
	for _, room := range rooms {
		if room.GoogleCalendarID == nil {
			continue
		}
		slug := firstNonEmptyStr(room.Calendar, room.Slug)
		if configuredByRoom[room.Slug] || configuredBySlug[slug] {
			continue
		}
		source, ok := normalizeCalendarSource(CalendarSourceConfig{
			Slug:       slug,
			Name:       room.Name,
			Provider:   CalendarProviderICS,
			URL:        getGoogleCalendarURL(*room.GoogleCalendarID),
			Visibility: CalendarVisibilityAuto,
			Room:       room.Slug,
		})
		if ok {
			out = append(out, source)
		}
	}
	return out
}

func roomCalendarSourceSlugs(rooms []RoomInfo) map[string]bool {
	out := map[string]bool{}
	for _, room := range rooms {
		if room.Calendar != "" {
			out[room.Calendar] = true
		} else if room.GoogleCalendarID != nil {
			out[room.Slug] = true
		}
	}
	return out
}

// ── Event classification + counters ─────────────────────────────────────────

// roomEvent pairs an ical event with its room source.
type roomEvent struct {
	event    ical.Event
	roomSlug string
	roomName string
}

func countICALEventsInMonthRange(events []ical.Event, sinceMonth, untilMonth string) int {
	count := 0
	for _, ev := range events {
		ym := ev.YearMonth()
		if ym >= sinceMonth && ym <= untilMonth {
			count++
		}
	}
	return count
}

func countPublicICALEventsInMonthRange(events []ical.Event, sinceMonth, untilMonth, visibility string) int {
	count := 0
	for _, ev := range events {
		ym := ev.YearMonth()
		if ym >= sinceMonth && ym <= untilMonth && calendarEventIsPublic(ev, visibility) {
			count++
		}
	}
	return count
}

func countBookingEventsInMonthRange(events []ical.Event, sinceMonth, untilMonth, visibility string, allRoomBooking bool) int {
	count := 0
	for _, ev := range events {
		ym := ev.YearMonth()
		if ym >= sinceMonth && ym <= untilMonth && calendarEventIsBooking(ev, visibility, allRoomBooking) {
			count++
		}
	}
	return count
}

func filterBookingEvents(events []ical.Event, visibility string, allRoomBooking bool) []ical.Event {
	out := make([]ical.Event, 0, len(events))
	for _, ev := range events {
		if calendarEventIsBooking(ev, visibility, allRoomBooking) {
			out = append(out, ev)
		}
	}
	return out
}

func calendarEventIsBooking(ev ical.Event, visibility string, allRoomBooking bool) bool {
	if allRoomBooking {
		return true
	}
	switch normalizeCalendarVisibility(visibility) {
	case CalendarVisibilityPrivate:
		return true
	case CalendarVisibilityPublic:
		return false
	default:
		return !autoCalendarEventHasPublicURL(ev)
	}
}

func calendarEventIsPublic(ev ical.Event, visibility string) bool {
	switch normalizeCalendarVisibility(visibility) {
	case CalendarVisibilityPrivate:
		return false
	case CalendarVisibilityPublic:
		return extractEventURL(ev) != ""
	default:
		return autoCalendarEventHasPublicURL(ev)
	}
}

func countCachedEventsInMonthRange(dataDir, sinceMonth, untilMonth string) int {
	total := 0
	for _, ym := range expandMonthRange(sinceMonth, untilMonth) {
		parts := strings.SplitN(ym, "-", 2)
		if len(parts) != 2 {
			continue
		}
		path := filepath.Join(dataDir, parts[0], parts[1], "generated", "events.json")
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		var ef FullEventsFile
		if json.Unmarshal(data, &ef) != nil {
			continue
		}
		total += len(ef.Events)
	}
	return total
}

func eventSyncRangeLabel(sinceMonth, untilMonth string) string {
	if sinceMonth == untilMonth {
		return "in " + sinceMonth
	}
	return "in " + sinceMonth + " → " + untilMonth
}
