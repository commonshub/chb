package cmd

import "fmt"

// SyncAll runs all sync commands sequentially.
func SyncAll(args []string, version string) error {
	if HasFlag(args, "--help", "-h") {
		PrintSyncAllHelp()
		return nil
	}

	fmt.Printf("\n%s🔄 Syncing everything...%s\n", Fmt.Bold, Fmt.Reset)

	fmt.Printf("\n%s━━━ Events ━━━%s\n", Fmt.Bold, Fmt.Reset)
	if err := EventsSync(args, version); err != nil {
		fmt.Printf("%s⚠ Events sync failed: %v%s\n", Fmt.Yellow, err, Fmt.Reset)
	}

	fmt.Printf("\n%s━━━ Transactions ━━━%s\n", Fmt.Bold, Fmt.Reset)
	if err := TransactionsSync(args); err != nil {
		fmt.Printf("%s⚠ Transactions sync failed: %v%s\n", Fmt.Yellow, err, Fmt.Reset)
	}

	fmt.Printf("\n%s━━━ Bookings ━━━%s\n", Fmt.Bold, Fmt.Reset)
	if err := BookingsSync(args); err != nil {
		fmt.Printf("%s⚠ Bookings sync failed: %v%s\n", Fmt.Yellow, err, Fmt.Reset)
	}

	fmt.Printf("\n%s━━━ Messages ━━━%s\n", Fmt.Bold, Fmt.Reset)
	if err := MessagesSync(args); err != nil {
		fmt.Printf("%s⚠ Messages sync failed: %v%s\n", Fmt.Yellow, err, Fmt.Reset)
	}

	fmt.Printf("\n%s━━━ Generate ━━━%s\n", Fmt.Bold, Fmt.Reset)
	if err := Generate(args); err != nil {
		fmt.Printf("%s⚠ Generate failed: %v%s\n", Fmt.Yellow, err, Fmt.Reset)
	}

	fmt.Printf("\n%s✓ All syncs complete!%s\n\n", Fmt.Green, Fmt.Reset)
	return nil
}
