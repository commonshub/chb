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

// BookingsSync is an alias for CalendarsSync for backwards compatibility.
func BookingsSync(args []string) error {
	_, _, err := CalendarsSync(args)
	return err
}

func getGoogleCalendarURL(calendarID string) string {
	encoded := url.QueryEscape(calendarID)
	return fmt.Sprintf("https://calendar.google.com/calendar/ical/%s/public/basic.ics", encoded)
}
