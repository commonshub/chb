package cmd

import "fmt"

func Rooms(args []string) {
	if HasFlag(args, "--help", "-h") {
		f := Fmt
		fmt.Printf(`
%schb rooms%s — List all rooms with pricing

%sUSAGE%s
  %schb rooms%s
`, f.Bold, f.Reset, f.Bold, f.Reset, f.Cyan, f.Reset)
		return
	}

	rooms, err := LoadRooms()
	if err != nil || len(rooms) == 0 {
		fmt.Printf("\n%sNo rooms found. Place rooms.json in ~/.chb/ or run from the website repo.%s\n\n", Fmt.Dim, Fmt.Reset)
		return
	}

	fmt.Printf("\n%s🏠 Rooms%s\n\n", Fmt.Bold, Fmt.Reset)

	maxName := 4
	maxSlug := 4
	for _, r := range rooms {
		maxName = Max(maxName, len(r.Name))
		maxSlug = Max(maxSlug, len(r.Slug))
	}

	fmt.Printf("%s%s  %s  %s  %s  %s  CALENDAR%s\n",
		Fmt.Dim,
		Pad("NAME", maxName), Pad("SLUG", maxSlug),
		Pad("CAP", 4), Pad("EUR/h", 6), Pad("CHT/h", 6),
		Fmt.Reset)

	for _, r := range rooms {
		var eur string
		if r.PricePerHour > 0 {
			eur = fmt.Sprintf("€%.0f", r.PricePerHour)
		} else {
			eur = fmt.Sprintf("%sfree%s", Fmt.Dim, Fmt.Reset)
		}

		var cht string
		if r.TokensPerHour > 0 {
			cht = fmt.Sprintf("%.0f", r.TokensPerHour)
			if r.TokensPerHour != float64(int(r.TokensPerHour)) {
				cht = fmt.Sprintf("%.1f", r.TokensPerHour)
			}
		} else {
			cht = fmt.Sprintf("%s—%s", Fmt.Dim, Fmt.Reset)
		}

		var cal string
		if r.GoogleCalendarID != nil {
			cal = fmt.Sprintf("%s✓%s", Fmt.Green, Fmt.Reset)
		} else {
			cal = fmt.Sprintf("%s—%s", Fmt.Dim, Fmt.Reset)
		}

		fmt.Printf("%s%s%s  %s  %s  %s  %s  %s\n",
			Fmt.Bold, Pad(r.Name, maxName), Fmt.Reset,
			Pad(r.Slug, maxSlug),
			Pad(fmt.Sprintf("%d", r.Capacity), 4),
			Pad(eur, 6),
			Pad(cht, 6),
			cal)
	}
	fmt.Println()
}
