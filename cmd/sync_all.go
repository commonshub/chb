package cmd

import (
	"fmt"
	"os"
	"time"
)

// SyncAll runs all sync commands sequentially.
// Each sync function fetches all data in one API call (or paginated),
// then distributes to year/month folders.
func SyncAll(args []string, version string) error {
	if HasFlag(args, "--help", "-h", "help") {
		PrintSyncAllHelp()
		return nil
	}

	startedAt := time.Now()

	if HasFlag(args, "--history") || GetOption(args, "--since") != "" {
		fmt.Printf("\n%s🔄 Syncing history...%s\n", Fmt.Bold, Fmt.Reset)
	} else {
		fmt.Printf("\n%s🔄 Syncing everything...%s\n", Fmt.Bold, Fmt.Reset)
	}

	var newBookings, newEvents, newTx, newMessages, newImages int

	fmt.Printf("\n%s━━━ Calendars ━━━%s\n", Fmt.Bold, Fmt.Reset)
	b, e, err := CalendarsSync(args)
	if err != nil {
		fmt.Printf("%s⚠ Calendars: %v%s\n", Fmt.Yellow, err, Fmt.Reset)
	}
	newBookings = b
	newEvents = e

	fmt.Printf("\n%s━━━ Transactions ━━━%s\n", Fmt.Bold, Fmt.Reset)
	n, err := TransactionsSync(args)
	if err != nil {
		fmt.Printf("%s⚠ Transactions: %v%s\n", Fmt.Yellow, err, Fmt.Reset)
	}
	newTx = n

	fmt.Printf("\n%s━━━ Messages ━━━%s\n", Fmt.Bold, Fmt.Reset)
	n, err = MessagesSync(args)
	if err != nil {
		fmt.Printf("%s⚠ Messages: %v%s\n", Fmt.Yellow, err, Fmt.Reset)
	}
	newMessages = n

	// Odoo analytic sync (optional, only if configured)
	if os.Getenv("ODOO_URL") != "" {
		fmt.Printf("\n%s━━━ Odoo ━━━%s\n", Fmt.Bold, Fmt.Reset)
		if _, err := OdooAnalyticSync(args); err != nil {
			fmt.Printf("%s⚠ Odoo: %v%s\n", Fmt.Yellow, err, Fmt.Reset)
		}
	}

	// Members sync (optional, only if Stripe or Odoo configured)
	if os.Getenv("STRIPE_SECRET_KEY") != "" || os.Getenv("ODOO_URL") != "" {
		fmt.Printf("\n%s━━━ Members ━━━%s\n", Fmt.Bold, Fmt.Reset)
		if err := MembersSync(args); err != nil {
			fmt.Printf("%s⚠ Members: %v%s\n", Fmt.Yellow, err, Fmt.Reset)
		}
	}

	fmt.Printf("\n%s━━━ Generate ━━━%s\n", Fmt.Bold, Fmt.Reset)
	if err := Generate(args); err != nil {
		fmt.Printf("%s⚠ Generate: %v%s\n", Fmt.Yellow, err, Fmt.Reset)
	}

	fmt.Printf("\n%s━━━ Images ━━━%s\n", Fmt.Bold, Fmt.Reset)
	n, err = ImagesSync(args)
	if err != nil {
		fmt.Printf("%s⚠ Images: %v%s\n", Fmt.Yellow, err, Fmt.Reset)
	}
	newImages = n

	// Print summary
	hasAny := newBookings > 0 || newTx > 0 || newMessages > 0 || newImages > 0
	elapsed := time.Since(startedAt).Round(100 * time.Millisecond)
	if hasAny {
		fmt.Printf("\n%s✓ Sync complete in %s%s\n", Fmt.Green, elapsed, Fmt.Reset)
		if newBookings > 0 {
			if newEvents > 0 {
				fmt.Printf("  📅 %d new bookings, including %d events\n", newBookings, newEvents)
			} else {
				fmt.Printf("  📅 %d new bookings\n", newBookings)
			}
		}
		if newTx > 0 {
			fmt.Printf("  💰 %d new transactions\n", newTx)
		}
		if newMessages > 0 {
			fmt.Printf("  💬 %d new messages\n", newMessages)
		}
		if newImages > 0 {
			fmt.Printf("  📸 %d new images\n", newImages)
		}
	} else {
		fmt.Printf("\n%s✓ Everything up to date in %s%s\n", Fmt.Green, elapsed, Fmt.Reset)
	}
	fmt.Println()

	return nil
}
