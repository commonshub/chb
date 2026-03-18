package cmd

import (
	"fmt"
	"strconv"
	"time"
)

// SyncAll runs all sync commands sequentially.
func SyncAll(args []string, version string) error {
	if HasFlag(args, "--help", "-h", "help") {
		PrintSyncAllHelp()
		return nil
	}

	if HasFlag(args, "--all") {
		return syncAllHistory(args, version)
	}

	fmt.Printf("\n%s🔄 Syncing everything...%s\n", Fmt.Bold, Fmt.Reset)
	runAllSyncs(args, version)
	return nil
}

func syncAllHistory(args []string, version string) error {
	// Remove --all from args so it doesn't confuse sub-commands
	var cleanArgs []string
	for _, a := range args {
		if a != "--all" {
			cleanArgs = append(cleanArgs, a)
		}
	}

	currentYear := time.Now().Year()
	startYear := 2023 // Commons Hub founding year

	fmt.Printf("\n%s🔄 Syncing entire history (%d–%d)...%s\n", Fmt.Bold, startYear, currentYear, Fmt.Reset)

	for year := startYear; year <= currentYear; year++ {
		yearStr := strconv.Itoa(year)
		for month := 1; month <= 12; month++ {
			// Don't sync future months
			if year == currentYear && month > int(time.Now().Month()) {
				break
			}
			monthStr := fmt.Sprintf("%02d", month)
			monthArgs := append([]string{yearStr + "/" + monthStr}, cleanArgs...)

			fmt.Printf("\n%s━━━ %s/%s ━━━%s\n", Fmt.Bold, yearStr, monthStr, Fmt.Reset)
			runAllSyncs(monthArgs, version)
		}
	}

	fmt.Printf("\n%s✓ Full history sync complete!%s\n\n", Fmt.Green, Fmt.Reset)
	return nil
}

func runAllSyncs(args []string, version string) {
	if err := EventsSync(args, version); err != nil {
		fmt.Printf("%s⚠ Events: %v%s\n", Fmt.Yellow, err, Fmt.Reset)
	}

	if err := TransactionsSync(args); err != nil {
		fmt.Printf("%s⚠ Transactions: %v%s\n", Fmt.Yellow, err, Fmt.Reset)
	}

	if err := BookingsSync(args); err != nil {
		fmt.Printf("%s⚠ Bookings: %v%s\n", Fmt.Yellow, err, Fmt.Reset)
	}

	if err := MessagesSync(args); err != nil {
		fmt.Printf("%s⚠ Messages: %v%s\n", Fmt.Yellow, err, Fmt.Reset)
	}

	if err := Generate(args); err != nil {
		fmt.Printf("%s⚠ Generate: %v%s\n", Fmt.Yellow, err, Fmt.Reset)
	}
}
