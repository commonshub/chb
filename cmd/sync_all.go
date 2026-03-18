package cmd

import "fmt"

// SyncAll runs all sync commands sequentially.
// Each sync function fetches all data in one API call (or paginated),
// then distributes to year/month folders. --all removes month filters
// so all fetched data gets saved.
func SyncAll(args []string, version string) error {
	if HasFlag(args, "--help", "-h", "help") {
		PrintSyncAllHelp()
		return nil
	}

	if HasFlag(args, "--history") || GetOption(args, "--since") != "" {
		fmt.Printf("\n%s🔄 Syncing history...%s\n", Fmt.Bold, Fmt.Reset)
	} else {
		fmt.Printf("\n%s🔄 Syncing everything...%s\n", Fmt.Bold, Fmt.Reset)
	}

	fmt.Printf("\n%s━━━ Events ━━━%s\n", Fmt.Bold, Fmt.Reset)
	if err := EventsSync(args, version); err != nil {
		fmt.Printf("%s⚠ Events: %v%s\n", Fmt.Yellow, err, Fmt.Reset)
	}

	fmt.Printf("\n%s━━━ Transactions ━━━%s\n", Fmt.Bold, Fmt.Reset)
	if err := TransactionsSync(args); err != nil {
		fmt.Printf("%s⚠ Transactions: %v%s\n", Fmt.Yellow, err, Fmt.Reset)
	}

	fmt.Printf("\n%s━━━ Bookings ━━━%s\n", Fmt.Bold, Fmt.Reset)
	if err := BookingsSync(args); err != nil {
		fmt.Printf("%s⚠ Bookings: %v%s\n", Fmt.Yellow, err, Fmt.Reset)
	}

	fmt.Printf("\n%s━━━ Messages ━━━%s\n", Fmt.Bold, Fmt.Reset)
	if err := MessagesSync(args); err != nil {
		fmt.Printf("%s⚠ Messages: %v%s\n", Fmt.Yellow, err, Fmt.Reset)
	}

	fmt.Printf("\n%s━━━ Generate ━━━%s\n", Fmt.Bold, Fmt.Reset)
	if err := Generate(args); err != nil {
		fmt.Printf("%s⚠ Generate: %v%s\n", Fmt.Yellow, err, Fmt.Reset)
	}

	fmt.Printf("\n%s━━━ Images ━━━%s\n", Fmt.Bold, Fmt.Reset)
	if err := ImagesSync(args); err != nil {
		fmt.Printf("%s⚠ Images: %v%s\n", Fmt.Yellow, err, Fmt.Reset)
	}

	fmt.Printf("\n%s✓ All syncs complete!%s\n\n", Fmt.Green, Fmt.Reset)
	return nil
}
