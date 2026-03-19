package cmd

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/CommonsHub/chb/ical"
)

type BookingEntry struct {
	UID   string
	Title string
	Start time.Time
	End   time.Time
	Room  string
}

func loadAllBookings() ([]BookingEntry, error) {
	dataDir := DataDir()
	var bookings []BookingEntry

	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		return bookings, nil
	}

	rooms, err := LoadRooms()
	if err != nil {
		return bookings, nil
	}
	roomSlugs := map[string]string{} // slug -> name
	for _, r := range rooms {
		roomSlugs[r.Slug] = r.Name
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
			icsDir := filepath.Join(yearPath, md.Name(), "calendars", "ics")
			if _, err := os.Stat(icsDir); os.IsNotExist(err) {
				continue
			}
			files, _ := os.ReadDir(icsDir)
			for _, f := range files {
				if f.IsDir() || !strings.HasSuffix(f.Name(), ".ics") {
					continue
				}
				slug := strings.TrimSuffix(f.Name(), ".ics")
				roomName, ok := roomSlugs[slug]
				if !ok {
					continue // not a room calendar
				}

				data, err := os.ReadFile(filepath.Join(icsDir, f.Name()))
				if err != nil {
					continue
				}

				events, err := ical.ParseICS(string(data))
				if err != nil {
					continue
				}

				for _, ev := range events {
					bookings = append(bookings, BookingEntry{
						UID:   ev.UID,
						Title: ev.Summary,
						Start: ev.Start,
						End:   ev.End,
						Room:  roomName,
					})
				}
			}
		}
	}

	sort.Slice(bookings, func(i, j int) bool {
		return bookings[i].Start.Before(bookings[j].Start)
	})
	return bookings, nil
}

func BookingsList(args []string) {
	if HasFlag(args, "--help", "-h", "help") {
		PrintBookingsHelp()
		return
	}

	n := GetNumber(args, []string{"-n"}, 10)
	skip := GetNumber(args, []string{"--skip"}, 0)
	showAll := HasFlag(args, "--all")
	dateStr := GetOption(args, "--date")
	roomFilter := GetOption(args, "--room")

	var filterDate, filterDateEnd time.Time
	if dateStr != "" {
		if d, ok := ParseSinceDate(dateStr); ok {
			filterDate = d
			filterDateEnd = d.AddDate(0, 0, 1)
		}
	}

	sinceDate := time.Now()
	if showAll {
		sinceDate = time.Time{}
	}
	if !filterDate.IsZero() {
		sinceDate = filterDate
	}

	bookings, _ := loadAllBookings()

	var filtered []BookingEntry
	for _, b := range bookings {
		if !filterDate.IsZero() && !filterDateEnd.IsZero() {
			if b.Start.Before(filterDate) || !b.Start.Before(filterDateEnd) {
				continue
			}
		} else {
			if b.Start.Before(sinceDate) {
				continue
			}
		}
		filtered = append(filtered, b)
	}

	if roomFilter != "" {
		rooms, _ := LoadRooms()
		var room *RoomInfo
		for i := range rooms {
			if rooms[i].Slug == roomFilter {
				room = &rooms[i]
				break
			}
		}
		if room == nil {
			fmt.Printf("\n%sUnknown room: %s%s. Run 'chb rooms' to see available rooms.\n\n", Fmt.Red, roomFilter, Fmt.Reset)
			return
		}
		var roomFiltered []BookingEntry
		for _, b := range filtered {
			if b.Room == room.Name {
				roomFiltered = append(roomFiltered, b)
			}
		}
		filtered = roomFiltered
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
		fmt.Printf("\n%sNo bookings found.%s\n", Fmt.Dim, Fmt.Reset)
		if _, err := os.Stat(DataDir()); os.IsNotExist(err) {
			fmt.Printf("%sRun 'chb bookings sync' to fetch room calendars.%s\n", Fmt.Dim, Fmt.Reset)
		}
		fmt.Println()
		return
	}

	maxRoom := 4
	maxTitle := 5
	for _, b := range sliced {
		maxRoom = Max(maxRoom, len(b.Room))
		maxTitle = Max(maxTitle, len(b.Title))
	}
	maxRoom = Min(maxRoom, 20)
	maxTitle = Min(maxTitle, 40)

	label := "Upcoming bookings"
	if dateStr != "" {
		label = fmt.Sprintf("Bookings on %s", FmtDate(sinceDate))
	}

	skipStr := ""
	if skip > 0 {
		skipStr = fmt.Sprintf(", skip %d", skip)
	}

	fmt.Printf("\n%s📋 %s%s %s(%d of %d%s)%s\n\n",
		Fmt.Bold, label, Fmt.Reset, Fmt.Dim, len(sliced), len(filtered), skipStr, Fmt.Reset)
	fmt.Printf("%s%s %s %s TITLE%s\n",
		Fmt.Dim, Pad("DATE", 16), Pad("TIME", 14), Pad("ROOM", maxRoom), Fmt.Reset)

	for _, b := range sliced {
		timeRange := fmt.Sprintf("%s–%s", FmtTime(b.Start), FmtTime(b.End))
		fmt.Printf("%s%s%s %s%s%s %s %s\n",
			Fmt.Green, Pad(FmtDate(b.Start), 16), Fmt.Reset,
			Fmt.Cyan, Pad(timeRange, 14), Fmt.Reset,
			Pad(Truncate(b.Room, maxRoom), maxRoom),
			Truncate(b.Title, maxTitle))
	}

	remaining := len(filtered) - skip - n
	if remaining > 0 {
		fmt.Printf("\n%s… %d more. Use -n or --skip to paginate.%s\n", Fmt.Dim, remaining, Fmt.Reset)
	}
	fmt.Println()
}

func BookingsSync(args []string) error {
	if HasFlag(args, "--help", "-h", "help") {
		PrintBookingsSyncHelp()
		return nil
	}

	force := HasFlag(args, "--force")
	roomSlug := GetOption(args, "--room")
	sinceStr := GetOption(args, "--since")

	// Positional year/month arg (e.g. "2025" or "2025/06")
	posYear, posMonth, posFound := ParseYearMonthArg(args)

	now := time.Now()
	var startMonth, endMonth string

	// Check --since / --history first
	sinceMonth, isSince := ResolveSinceMonth(args, "calendars")

	if isSince {
		startMonth = sinceMonth
		future := time.Date(now.Year(), now.Month()+2, 1, 0, 0, 0, 0, time.UTC)
		endMonth = fmt.Sprintf("%d-%02d", future.Year(), future.Month())
	} else if posFound {
		if posMonth != "" {
			startMonth = fmt.Sprintf("%s-%s", posYear, posMonth)
			endMonth = startMonth
		} else {
			startMonth = fmt.Sprintf("%s-01", posYear)
			endMonth = fmt.Sprintf("%s-12", posYear)
		}
	} else if sinceStr != "" {
		if d, ok := ParseSinceDate(sinceStr); ok {
			startMonth = fmt.Sprintf("%d-%02d", d.Year(), d.Month())
		} else {
			prev := time.Date(now.Year(), now.Month()-1, 1, 0, 0, 0, 0, time.UTC)
			startMonth = fmt.Sprintf("%d-%02d", prev.Year(), prev.Month())
		}
		future := time.Date(now.Year(), now.Month()+2, 1, 0, 0, 0, 0, time.UTC)
		endMonth = fmt.Sprintf("%d-%02d", future.Year(), future.Month())
	} else {
		prev := time.Date(now.Year(), now.Month()-1, 1, 0, 0, 0, 0, time.UTC)
		startMonth = fmt.Sprintf("%d-%02d", prev.Year(), prev.Month())
		future := time.Date(now.Year(), now.Month()+2, 1, 0, 0, 0, 0, time.UTC)
		endMonth = fmt.Sprintf("%d-%02d", future.Year(), future.Month())
	}

	target := "all rooms"
	if roomSlug != "" {
		target = fmt.Sprintf("room \"%s\"", roomSlug)
	}

	fmt.Printf("\n%s📋 Syncing bookings%s %s(%s, %s → %s)%s\n\n",
		Fmt.Bold, Fmt.Reset, Fmt.Dim, target, startMonth, endMonth, Fmt.Reset)

	rooms, err := LoadRooms()
	if err != nil {
		return fmt.Errorf("failed to load rooms: %w", err)
	}

	dataDir := DataDir()

	for _, room := range rooms {
		if room.GoogleCalendarID == nil {
			continue
		}
		if roomSlug != "" && room.Slug != roomSlug {
			continue
		}

		calURL := getGoogleCalendarURL(*room.GoogleCalendarID)
		fmt.Printf("  Fetching %s calendar...\n", room.Slug)

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

		byMonth := ical.GroupByMonth(events)
		for ym, monthEvents := range byMonth {
			if ym < startMonth || ym > endMonth {
				continue
			}
			parts := strings.SplitN(ym, "-", 2)
			year, month := parts[0], parts[1]

			relPath := filepath.Join("calendars", "ics", room.Slug+".ics")
			filePath := filepath.Join(dataDir, year, month, relPath)

			if !force {
				if _, err := os.Stat(filePath); err == nil {
					continue // already exists
				}
			}

			content := ical.WrapICS(monthEvents, fmt.Sprintf("-//Commons Hub Brussels//%s//EN", room.Name))
			writeMonthFile(dataDir, year, month, relPath, []byte(content))
		}
		fmt.Printf("  %s✓%s %s: %d events\n", Fmt.Green, Fmt.Reset, room.Slug, len(events))
	}

	// Show stats
	bookings, _ := loadAllBookings()
	var upcoming []BookingEntry
	for _, b := range bookings {
		if b.Start.After(now) {
			upcoming = append(upcoming, b)
		}
	}

	byRoom := map[string]int{}
	for _, b := range upcoming {
		byRoom[b.Room]++
	}

	fmt.Printf("\n%s━━━ Booking Statistics ━━━%s\n\n", Fmt.Bold, Fmt.Reset)
	var roomNames []string
	for r := range byRoom {
		roomNames = append(roomNames, r)
	}
	sort.Strings(roomNames)
	for _, r := range roomNames {
		fmt.Printf("  %s %d upcoming\n", Pad(r, 20), byRoom[r])
	}
	fmt.Printf("\n  %sTotal: %d upcoming bookings%s\n", Fmt.Bold, len(upcoming), Fmt.Reset)
	fmt.Printf("\n%s✓ Done!%s\n\n", Fmt.Green, Fmt.Reset)

	return nil
}

func getGoogleCalendarURL(calendarID string) string {
	encoded := url.QueryEscape(calendarID)
	return fmt.Sprintf("https://calendar.google.com/calendar/ical/%s/public/basic.ics", encoded)
}
