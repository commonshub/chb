package cmd

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/charmbracelet/huh"
)

// runField runs a huh field and exits cleanly on Ctrl+C.
func runField(field huh.Field) {
	err := huh.NewForm(huh.NewGroup(field)).Run()
	if err != nil {
		if errors.Is(err, huh.ErrUserAborted) {
			fmt.Printf("\n%sCancelled.%s\n\n", Fmt.Dim, Fmt.Reset)
			os.Exit(0)
		}
	}
}

// pickCollective shows a collective selector with ability to enter new ones.
// Returns "" if skipped/cancelled.
func pickCollective(defaultCollective string) string {
	var colOptions []huh.Option[string]
	colOptions = append(colOptions, huh.NewOption("(skip)", ""))
	if defaultCollective != "" {
		colOptions = append(colOptions, huh.NewOption(defaultCollective+" (default)", defaultCollective))
	}
	for _, slug := range CollectiveSlugs() {
		if slug != defaultCollective {
			colOptions = append(colOptions, huh.NewOption(slug, slug))
		}
	}
	colOptions = append(colOptions, huh.NewOption("+ New collective...", "__new__"))

	var collective string
	err := huh.NewForm(huh.NewGroup(
		huh.NewSelect[string]().Title("Collective").Options(colOptions...).Value(&collective),
	)).Run()
	if err != nil {
		return "" // Esc/abort
	}

	if collective == "__new__" {
		var newSlug string
		runField(huh.NewInput().Title("Collective slug (e.g. regensunite)").Value(&newSlug))
		if newSlug != "" {
			AddCollective(newSlug)
			fmt.Printf("  %s✓ Created collective: %s%s\n", Fmt.Green, newSlug, Fmt.Reset)
			return newSlug
		}
		return ""
	}

	return collective
}

// pickEvent shows an event selector filtered by proximity to a given date.
// Returns the event UID or "" if skipped.
func pickEvent(txTimestamp int64) string {
	tz := BrusselsTZ()
	txDate := time.Unix(txTimestamp, 0).In(tz)

	// Load all events
	allEvents := loadAllEvents()
	if len(allEvents) == 0 {
		return ""
	}

	// Sort by date proximity to the transaction
	type eventDist struct {
		id   string
		name string
		date time.Time
		dist time.Duration
	}
	var sorted []eventDist
	for _, e := range allEvents {
		t, _ := time.Parse(time.RFC3339, e.StartAt)
		if t.IsZero() {
			t, _ = time.Parse("2006-01-02T15:04:05.000Z", e.StartAt)
		}
		if t.IsZero() {
			continue
		}
		dist := txDate.Sub(t)
		if dist < 0 {
			dist = -dist
		}
		sorted = append(sorted, eventDist{e.ID, e.Name, t, dist})
	}

	// Sort closest first, limit to 20
	for i := 0; i < len(sorted); i++ {
		for j := i + 1; j < len(sorted); j++ {
			if sorted[j].dist < sorted[i].dist {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}
	if len(sorted) > 20 {
		sorted = sorted[:20]
	}

	var evtOptions []huh.Option[string]
	evtOptions = append(evtOptions, huh.NewOption("(skip)", ""))
	for _, e := range sorted {
		label := fmt.Sprintf("%s — %s", e.date.In(tz).Format("02/01"), e.name)
		if len(label) > 60 {
			label = label[:57] + "..."
		}
		evtOptions = append(evtOptions, huh.NewOption(label, e.id))
	}

	var eventID string
	err := huh.NewForm(huh.NewGroup(
		huh.NewSelect[string]().Title("Link to event?").Options(evtOptions...).Value(&eventID),
	)).Run()
	if err != nil {
		return ""
	}
	return eventID
}

// checkAbort checks if a huh error is a user abort (Ctrl+C) and exits cleanly.
func checkAbort(err error) {
	if err != nil && errors.Is(err, huh.ErrUserAborted) {
		fmt.Printf("\n%sCancelled.%s\n\n", Fmt.Dim, Fmt.Reset)
		os.Exit(0)
	}
}
