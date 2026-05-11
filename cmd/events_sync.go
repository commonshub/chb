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
	"sync"
	"time"

	"github.com/CommonsHub/chb/ical"
	"github.com/CommonsHub/chb/og"
	icssource "github.com/CommonsHub/chb/sources/ics"
)

var calendarHTTPClient = &http.Client{Timeout: 20 * time.Second}

// FullEvent is the rich event structure written to events.json
type FullEvent struct {
	ID              string            `json:"id"`
	Name            string            `json:"name"`
	Description     string            `json:"description,omitempty"`
	StartAt         string            `json:"startAt"`
	EndAt           string            `json:"endAt,omitempty"`
	Timezone        string            `json:"timezone,omitempty"`
	Location        string            `json:"location,omitempty"`
	URL             string            `json:"url,omitempty"`
	CoverImage      string            `json:"coverImage,omitempty"`
	CoverImageLocal string            `json:"coverImageLocal,omitempty"`
	Source          string            `json:"source"`
	CalendarSource  string            `json:"calendarSource,omitempty"`
	Tags            json.RawMessage   `json:"tags,omitempty"`
	Guests          json.RawMessage   `json:"guests,omitempty"`
	LumaData        json.RawMessage   `json:"lumaData,omitempty"`
	Metadata        EventMetadata     `json:"metadata"`
	TicketSales     *EventTicketSales `json:"ticketSales,omitempty"`
}

// EventTicketSales summarises transactions tagged with this event. Populated
// during `chb generate` after transactions are regenerated — see
// enrichEventsWithTicketSales. Tickets for a given event can be sold months
// before (or after) the event date, so the enrichment walks every month.
type EventTicketSales struct {
	TxCount      int                `json:"txCount"`
	RefundCount  int                `json:"refundCount,omitempty"`
	Gross        map[string]float64 `json:"gross"`                  // currency → sum of CREDIT amounts
	Net          map[string]float64 `json:"net"`                    // currency → gross + signed DEBIT/refunds
	FirstTx      string             `json:"firstTx,omitempty"`      // earliest tx date (YYYY-MM-DD)
	LastTx       string             `json:"lastTx,omitempty"`       // latest tx date (YYYY-MM-DD)
	Transactions []EventTicketTx    `json:"transactions,omitempty"` // raw refs, ordered by date
}

// EventTicketTx is a lightweight pointer to a transaction from EventTicketSales.
// Keeps the full tx out of events.json — consumers needing more can look it up
// in <YYYY>/<MM>/generated/transactions.json by ID.
type EventTicketTx struct {
	ID       string  `json:"id"`
	Provider string  `json:"provider"`
	Date     string  `json:"date"`
	Amount   float64 `json:"amount"`
	Currency string  `json:"currency"`
	Type     string  `json:"type"`
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
	yearMonth   string
	totalEvents int
	newEvents   []newEventInfo
}

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
		fmt.Printf("  %s%d error(s)%s: %s\n", Fmt.Red, d.errors[kind], Fmt.Reset, kind)
	}
	for _, kind := range sortedDiagnosticKinds(d.warnings) {
		fmt.Printf("  %s%d warning(s)%s: %s\n", Fmt.Yellow, d.warnings[kind], Fmt.Reset, kind)
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

type eventSyncRunCache struct {
	dataDir string
	og      *eventOGCache
	dirty   bool
	debug   bool
}

type eventOGCache struct {
	Version   int                         `json:"version"`
	UpdatedAt string                      `json:"updatedAt"`
	Entries   map[string]eventOGCacheItem `json:"entries"`
}

type eventOGCacheItem struct {
	Title               *string `json:"title,omitempty"`
	Description         *string `json:"description,omitempty"`
	ImageURL            *string `json:"imageUrl,omitempty"`
	FinalURL            *string `json:"finalUrl,omitempty"`
	ContentType         *string `json:"contentType,omitempty"`
	HTMLTitle           *string `json:"htmlTitle,omitempty"`
	StatusCode          *int    `json:"statusCode,omitempty"`
	ErrorKind           *string `json:"errorKind,omitempty"`
	ErrorMessage        *string `json:"errorMessage,omitempty"`
	CloudflareChallenge bool    `json:"cloudflareChallenge,omitempty"`
	CheckedAt           string  `json:"checkedAt"`
}

type ogFetchTask struct {
	EventID string
	Name    string
	URL     string
	Domain  string
}

type ogFetchResult struct {
	URL    string
	Result og.FetchResult
}

const eventOGCacheVersion = 3

var (
	eventOGPositiveTTL = 30 * 24 * time.Hour
)

// CalendarsSync fetches room calendars once and produces both bookings (all events)
// and events (bookings with a public URL, enriched with og:image).
// Returns (newBookings, newEvents, error).
func CalendarsSync(args []string) (int, int, error) {
	if HasFlag(args, "--help", "-h", "help") {
		PrintCalendarsSyncHelp()
		return 0, 0, nil
	}

	force := HasFlag(args, "--force")
	debug := HasFlag(args, "--debug")
	sinceStr := GetOption(args, "--since")

	// Positional year/month arg (e.g. "2025" or "2025/11")
	posYear, posMonth, posFound := ParseYearMonthArg(args)

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
		fmt.Printf("  %s✓%s %s calendar: %d event(s) %s, %d public, %d private (%s/%s, %d in feed)\n",
			Fmt.Green, Fmt.Reset, cs.Slug, eventsInRange, rangeLabel, publicInRange, bookingInRange, provider, visibility, len(events))
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

	// --- Events: filter bookings with URLs, enrich with og:image ---
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
	fmt.Printf("\n📎 Found %d public event(s) %s across %d month(s) to process\n",
		filteredPublicEvents, rangeLabel, len(sortedMonths))

	// Save public.ics per month (events with URLs only)
	for _, ym := range sortedMonths {
		parts := strings.SplitN(ym, "-", 2)
		year, month := parts[0], parts[1]

		var icsEvents []ical.Event
		for _, re := range eventsByMonth[ym] {
			icsEvents = append(icsEvents, re.event)
		}
		content := ical.WrapICS(icsEvents, "-//Commons Hub Brussels//Public Calendar Events//EN")
		writeMonthFile(dataDir, year, month, filepath.Join("generated", "calendars", "public.ics"), []byte(content))
	}

	// Process each month (og:image scraping, events.json generation)
	newEventCount := 0
	for _, ym := range sortedMonths {
		parts := strings.SplitN(ym, "-", 2)
		year, month := parts[0], parts[1]

		r, err := processMonthFromRooms(dataDir, year, month, eventsByMonth[ym], force, runCache, diagnostics)
		if err != nil {
			diagnostics.Error("generate month failed", "%s-%s: %v", year, month, err)
			continue
		}
		if r != nil {
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

// roomEvent pairs an ical event with its room source
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
		if json.Unmarshal(data, &ef) == nil {
			total += len(ef.Events)
		}
	}
	return total
}

func eventSyncRangeLabel(sinceMonth, untilMonth string) string {
	if sinceMonth == untilMonth {
		return "in " + sinceMonth
	}
	return "in " + sinceMonth + " → " + untilMonth
}

func newEventSyncRunCache(dataDir string, debug bool) *eventSyncRunCache {
	return &eventSyncRunCache{
		dataDir: dataDir,
		og:      loadEventOGCache(dataDir),
		debug:   debug,
	}
}

func (c *eventSyncRunCache) save() {
	if c == nil || !c.dirty {
		return
	}
	if err := saveEventOGCache(c.dataDir, c.og); err == nil {
		c.dirty = false
	}
}

func (c *eventSyncRunCache) getOGResult(eventURL string) og.FetchResult {
	if c == nil || c.og == nil {
		return og.FetchDetailed(eventURL)
	}
	now := time.Now().UTC()
	if entry, ok := c.og.Entries[eventURL]; ok {
		if !eventOGCacheEntryHasMeta(entry) {
			delete(c.og.Entries, eventURL)
			c.dirty = true
		} else {
			if checkedAt, err := time.Parse(time.RFC3339, entry.CheckedAt); err == nil {
				ttl := eventOGPositiveTTL
				if now.Sub(checkedAt) < ttl {
					return eventOGCacheEntryResult(eventURL, entry)
				}
			}
		}
	}

	result := c.fetchOGResult(eventURL)
	c.storeOGResult(eventURL, result)
	return result
}

func (c *eventSyncRunCache) getCachedOGResult(eventURL string) (og.FetchResult, bool) {
	if c == nil || c.og == nil {
		return og.FetchResult{}, false
	}
	now := time.Now().UTC()
	entry, ok := c.og.Entries[eventURL]
	if !ok {
		return og.FetchResult{}, false
	}
	if !eventOGCacheEntryHasMeta(entry) {
		delete(c.og.Entries, eventURL)
		c.dirty = true
		return og.FetchResult{}, false
	}
	checkedAt, err := time.Parse(time.RFC3339, entry.CheckedAt)
	if err != nil {
		return og.FetchResult{}, false
	}
	if now.Sub(checkedAt) >= eventOGPositiveTTL {
		return og.FetchResult{}, false
	}
	return eventOGCacheEntryResult(eventURL, entry), true
}

func (c *eventSyncRunCache) storeOGResult(eventURL string, result og.FetchResult) {
	if c == nil || c.og == nil {
		return
	}
	if !shouldCacheOGResult(result) {
		if _, ok := c.og.Entries[eventURL]; ok {
			delete(c.og.Entries, eventURL)
			c.og.Version = eventOGCacheVersion
			c.og.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
			c.dirty = true
		}
		return
	}
	now := time.Now().UTC()
	entry := eventOGCacheItem{CheckedAt: now.Format(time.RFC3339)}
	setEventOGCacheEntryResult(&entry, result)
	c.og.Entries[eventURL] = entry
	c.og.Version = eventOGCacheVersion
	c.og.UpdatedAt = now.Format(time.RFC3339)
	c.dirty = true
}

func eventOGCacheEntryHasMeta(entry eventOGCacheItem) bool {
	return entry.ImageURL != nil && *entry.ImageURL != ""
}

func shouldCacheOGResult(result og.FetchResult) bool {
	return strings.TrimSpace(result.ErrorKind) == "" && strings.TrimSpace(result.Meta.Image) != ""
}

func eventOGCacheEntryResult(eventURL string, entry eventOGCacheItem) og.FetchResult {
	result := og.FetchResult{URL: eventURL}
	if entry.Title != nil {
		result.Meta.Title = *entry.Title
	}
	if entry.Description != nil {
		result.Meta.Description = *entry.Description
	}
	if entry.ImageURL != nil {
		result.Meta.Image = *entry.ImageURL
	}
	if entry.FinalURL != nil {
		result.FinalURL = *entry.FinalURL
	}
	if entry.ContentType != nil {
		result.ContentType = *entry.ContentType
	}
	if entry.HTMLTitle != nil {
		result.HTMLTitle = *entry.HTMLTitle
	}
	if entry.StatusCode != nil {
		result.StatusCode = *entry.StatusCode
	}
	if entry.ErrorKind != nil {
		result.ErrorKind = *entry.ErrorKind
	}
	if entry.ErrorMessage != nil {
		result.ErrorMessage = *entry.ErrorMessage
	}
	result.CloudflareChallenge = entry.CloudflareChallenge
	if result.FinalURL == "" {
		result.FinalURL = eventURL
	}
	return result
}

func setEventOGCacheEntryResult(entry *eventOGCacheItem, result og.FetchResult) {
	if entry == nil {
		return
	}
	if result.Meta.Title != "" {
		title := result.Meta.Title
		entry.Title = &title
	}
	if result.Meta.Description != "" {
		desc := result.Meta.Description
		entry.Description = &desc
	}
	if result.Meta.Image != "" {
		image := result.Meta.Image
		entry.ImageURL = &image
	}
	if result.FinalURL != "" {
		finalURL := result.FinalURL
		entry.FinalURL = &finalURL
	}
	if result.ContentType != "" {
		contentType := result.ContentType
		entry.ContentType = &contentType
	}
	if result.HTMLTitle != "" {
		htmlTitle := result.HTMLTitle
		entry.HTMLTitle = &htmlTitle
	}
	if result.StatusCode != 0 {
		statusCode := result.StatusCode
		entry.StatusCode = &statusCode
	}
	if result.ErrorKind != "" {
		errorKind := result.ErrorKind
		entry.ErrorKind = &errorKind
	}
	if result.ErrorMessage != "" {
		errorMessage := result.ErrorMessage
		entry.ErrorMessage = &errorMessage
	}
	entry.CloudflareChallenge = result.CloudflareChallenge
}

func eventOGCachePath(dataDir string) string {
	return filepath.Join(dataDir, "generated", "cache", "event-og-images.json")
}

func loadEventOGCache(dataDir string) *eventOGCache {
	cache := &eventOGCache{
		Version: eventOGCacheVersion,
		Entries: map[string]eventOGCacheItem{},
	}
	data, err := os.ReadFile(eventOGCachePath(dataDir))
	if err != nil {
		return cache
	}
	if json.Unmarshal(data, cache) != nil || cache.Entries == nil {
		cache.Entries = map[string]eventOGCacheItem{}
	}
	return cache
}

func saveEventOGCache(dataDir string, cache *eventOGCache) error {
	if cache == nil {
		return nil
	}
	if cache.Entries == nil {
		cache.Entries = map[string]eventOGCacheItem{}
	}
	cache.Version = eventOGCacheVersion
	if cache.UpdatedAt == "" {
		cache.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	}
	path := eventOGCachePath(dataDir)
	data, err := json.MarshalIndent(cache, "", "  ")
	if err != nil {
		return err
	}
	return writeDataFile(path, data)
}

func processMonthFromRooms(dataDir, year, month string, roomEvents []roomEvent, force bool, runCache *eventSyncRunCache, diagnostics *calendarSyncDiagnostics) (*monthResult, error) {
	monthPath := filepath.Join(dataDir, year, month)
	label := fmt.Sprintf("%s-%s", year, month)

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

	// Check if we can skip (same event count, not forced, and existing records are already enriched)
	if !force && len(existingIDs) == len(roomEvents) && len(existingIDs) > 0 && !existingEventsNeedRefresh(existingEvents) {
		fmt.Printf("  %s: %d public event(s), unchanged\n", label, len(roomEvents))
		return nil, nil
	}

	needsOGFetch := 0
	needsCoverSync := 0
	var ogTasks []ogFetchTask
	for _, re := range roomEvents {
		eventURL := extractEventURL(re.event)
		if eventURL == "" {
			continue
		}
		eventID := re.event.UID
		existing := existingEvents[eventID]
		if existing.CoverImage == "" || eventNeedsOGRefresh(existing) {
			needsOGFetch++
			ogTasks = append(ogTasks, ogFetchTask{
				EventID: eventID,
				Name:    re.event.Summary,
				URL:     eventURL,
				Domain:  eventHostFromURL(eventURL),
			})
		}
		if existing.CoverImage != "" && existing.CoverImageLocal == "" {
			needsCoverSync++
		}
	}
	ogResults := map[string]og.FetchResult{}
	if len(ogTasks) > 0 {
		ogResults = runCache.prefetchOGResults(label, ogTasks, 4)
	}

	processedIDs := map[string]bool{}
	var newEvents []newEventInfo
	var fullEvents []FullEvent
	processed := 0

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
		processed++

		startAt := icsEv.Start.Format(time.RFC3339)
		endAt := ""
		if !icsEv.End.IsZero() {
			endAt = icsEv.End.Format(time.RFC3339)
		}

		// Enrich from cached/full OG metadata
		var coverImage, coverImageLocal string
		description := icsEv.Description
		// Preserve existing cover image if already synced
		if existing, ok := existingEvents[eventID]; ok {
			coverImage = existing.CoverImage
			coverImageLocal = existing.CoverImageLocal
			if description == "" {
				description = existing.Description
			}
		}
		ogResult, ok := ogResults[eventURL]
		if !ok {
			ogResult = runCache.getOGResult(eventURL)
		}
		meta := ogResult.Meta
		if coverImage == "" && meta.Image != "" {
			coverImage = meta.Image
		}
		if coverImage == "" {
			diagnostics.Warn("missing og:image", "%s %s: %s", label, eventID, describeOGImageIssue(ogResult))
		}
		description = chooseEventDescription(description, meta.Description)
		name = chooseEventTitle(name, meta.Title)

		if processedIDs[eventID] {
			continue
		}
		processedIDs[eventID] = true

		// Preserve existing metadata
		metadata := existingMetadata[eventID]

		// Default location if not set
		if location == "" {
			location = "Commons Hub Brussels, Rue de la Madeleine 51, 1000 Bruxelles, Belgium"
		}

		fullEvents = append(fullEvents, FullEvent{
			ID:              eventID,
			Name:            name,
			Description:     description,
			StartAt:         startAt,
			EndAt:           endAt,
			Location:        location,
			URL:             eventURL,
			CoverImage:      coverImage,
			CoverImageLocal: coverImageLocal,
			Source:          "calendar",
			CalendarSource:  re.roomSlug,
			Metadata:        metadata,
		})
	}

	// Dedup events sharing URL + start + end (e.g. a manually-added Google
	// Calendar entry alongside the same event imported from Luma). Keep the
	// record with a cover image and the longest description.
	fullEvents = dedupeFullEvents(fullEvents)

	runEventProcessors(dataDir, year, month, fullEvents)

	// Track new events from dedup survivors so we don't announce a duplicate
	// that was discarded.
	for _, e := range fullEvents {
		if !existingIDs[e.ID] {
			newEvents = append(newEvents, newEventInfo{e.Name, e.StartAt, e.CalendarSource})
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
	fmt.Printf("  %s: %d public event(s), %d page fetch(es), %d cover(s) pending, %d new, wrote %d\n",
		label, len(roomEvents), needsOGFetch, needsCoverSync, len(newEvents), len(fullEvents))

	return &monthResult{
		yearMonth:   fmt.Sprintf("%s-%s", year, month),
		totalEvents: len(fullEvents),
		newEvents:   newEvents,
	}, nil
}

func existingEventsNeedRefresh(existingEvents map[string]FullEvent) bool {
	type dupKey struct{ url, start, end string }
	seen := map[dupKey]bool{}
	for _, existing := range existingEvents {
		if eventNeedsOGRefresh(existing) || !isAllowedPublicEventURL(existing.URL) {
			return true
		}
		k := dupKey{existing.URL, existing.StartAt, existing.EndAt}
		if seen[k] {
			// Duplicate event sharing URL + start + end already on disk —
			// trigger a refresh so dedupeFullEvents can collapse it.
			return true
		}
		seen[k] = true
	}
	return false
}

func eventNeedsOGRefresh(existing FullEvent) bool {
	return existing.CoverImage == "" || looksLikeThinEventDescription(existing.Description)
}

func chooseEventTitle(icsTitle, ogTitle string) string {
	if strings.TrimSpace(icsTitle) != "" {
		return icsTitle
	}
	return strings.TrimSpace(ogTitle)
}

// dedupeFullEvents collapses events that share (URL, StartAt, EndAt) — for
// instance, the same Luma event imported via its public ICS feed and also
// added manually to a Google Calendar. The "best" record wins: prefer one
// with a cover image, then the longer description; ties go to the first.
func dedupeFullEvents(events []FullEvent) []FullEvent {
	type key struct{ url, start, end string }
	idxByKey := map[key]int{}
	out := make([]FullEvent, 0, len(events))
	for _, e := range events {
		k := key{e.URL, e.StartAt, e.EndAt}
		if idx, ok := idxByKey[k]; ok {
			if isRicherEvent(e, out[idx]) {
				out[idx] = e
			}
			continue
		}
		idxByKey[k] = len(out)
		out = append(out, e)
	}
	return out
}

func isRicherEvent(candidate, current FullEvent) bool {
	candHasCover := candidate.CoverImage != "" || candidate.CoverImageLocal != ""
	currHasCover := current.CoverImage != "" || current.CoverImageLocal != ""
	if candHasCover != currHasCover {
		return candHasCover
	}
	return len(candidate.Description) > len(current.Description)
}

func chooseEventDescription(icsDescription, ogDescription string) string {
	icsDescription = strings.TrimSpace(icsDescription)
	ogDescription = strings.TrimSpace(ogDescription)
	if ogDescription == "" {
		return icsDescription
	}
	if icsDescription == "" || looksLikeThinEventDescription(icsDescription) {
		return ogDescription
	}
	return icsDescription
}

func looksLikeThinEventDescription(desc string) bool {
	desc = strings.TrimSpace(strings.ToLower(desc))
	if desc == "" {
		return true
	}
	return strings.HasPrefix(desc, "get up-to-date information at:")
}

func (c *eventSyncRunCache) prefetchOGResults(label string, tasks []ogFetchTask, maxWorkers int) map[string]og.FetchResult {
	results := map[string]og.FetchResult{}
	if len(tasks) == 0 {
		return results
	}

	grouped := map[string][]ogFetchTask{}
	var domains []string
	for _, task := range tasks {
		if result, ok := c.getCachedOGResult(task.URL); ok {
			results[task.URL] = result
			continue
		}
		domain := task.Domain
		if domain == "" {
			domain = "<unknown>"
		}
		if _, ok := grouped[domain]; !ok {
			domains = append(domains, domain)
		}
		grouped[domain] = append(grouped[domain], task)
	}

	if len(grouped) == 0 {
		return results
	}

	sort.Strings(domains)
	workerCount := maxWorkers
	if len(domains) < workerCount {
		workerCount = len(domains)
	}
	if workerCount < 1 {
		workerCount = 1
	}
	pending := 0
	for _, domain := range domains {
		pending += len(grouped[domain])
	}
	domainCh := make(chan []ogFetchTask)
	resultCh := make(chan ogFetchResult, pending)

	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for domainTasks := range domainCh {
				for _, task := range domainTasks {
					resultCh <- ogFetchResult{URL: task.URL, Result: c.fetchOGResult(task.URL)}
				}
			}
		}()
	}

	for _, domain := range domains {
		domainCh <- grouped[domain]
	}
	close(domainCh)
	wg.Wait()
	close(resultCh)

	for res := range resultCh {
		c.storeOGResult(res.URL, res.Result)
		results[res.URL] = res.Result
	}

	return results
}

func describeOGImageIssue(result og.FetchResult) string {
	targetURL := strings.TrimSpace(result.FinalURL)
	if targetURL == "" {
		targetURL = strings.TrimSpace(result.URL)
	}
	logSuffix := ""
	if result.DebugLogPath != "" {
		logSuffix = fmt.Sprintf(" (see %s)", result.DebugLogPath)
	}
	switch {
	case result.ErrorKind != "":
		return fmt.Sprintf("failed to fetch metadata from %s: %s%s", targetURL, result.ErrorMessage, logSuffix)
	case result.CloudflareChallenge:
		return fmt.Sprintf("fetched %s but got a Cloudflare challenge page instead of event metadata%s", targetURL, logSuffix)
	case result.ContentType != "" && !strings.Contains(strings.ToLower(result.ContentType), "html"):
		return fmt.Sprintf("fetched %s but got non-HTML content (%s)%s", targetURL, result.ContentType, logSuffix)
	case result.Meta.Image == "":
		if result.HTMLTitle != "" {
			return fmt.Sprintf("fetched %s but found no og:image meta tag (HTML title: %q)%s", targetURL, result.HTMLTitle, logSuffix)
		}
		return fmt.Sprintf("fetched %s but found no og:image meta tag%s", targetURL, logSuffix)
	default:
		return fmt.Sprintf("no og:image for %s%s", targetURL, logSuffix)
	}
}

func (c *eventSyncRunCache) fetchOGResult(eventURL string) og.FetchResult {
	if c != nil && c.debug {
		return og.FetchDetailedWithOptions(eventURL, og.FetchOptions{Debug: true})
	}
	return og.FetchDetailed(eventURL)
}

func eventHostFromURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return strings.ToLower(u.Hostname())
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
	_ = writeDataFile(filepath.Join(yearPath, "generated", "events.json"), data)
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
	_ = writeDataFile(filepath.Join(yearPath, "generated", "events.csv"), []byte(csvContent))
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
	_ = mkdirAllManagedData(latestDir)
	_ = writeDataFile(filepath.Join(latestDir, "events.md"), []byte(content))
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

		if room.Calendar != "" || room.GoogleCalendarID != nil {
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
	_ = mkdirAllManagedData(latestDir)
	_ = writeDataFile(filepath.Join(latestDir, "rooms.md"), []byte(content))
}
