package cmd

import (
	"fmt"
	"os"
	"time"
)

// SyncSummary is the JSON shape returned by `chb sync --json`.
type SyncSummary struct {
	ElapsedMS       int64             `json:"elapsedMs"`
	NewBookings     int               `json:"newBookings"`
	NewEvents       int               `json:"newEvents"`
	NewTransactions int               `json:"newTransactions"`
	NewInvoices     int               `json:"newInvoices"`
	NewBills        int               `json:"newBills"`
	NewAttachments  int               `json:"newAttachments"`
	NewMessages     int               `json:"newMessages"`
	NewImages       int               `json:"newImages"`
	Errors          map[string]string `json:"errors,omitempty"`
}

// SyncAll runs all sync commands sequentially.
// Each sync function fetches all data in one API call (or paginated),
// then distributes to year/month folders.
func SyncAll(args []string) error {
	if HasFlag(args, "--help", "-h", "help") {
		PrintSyncAllHelp()
		return nil
	}

	// In --json mode, silence every sub-sync's progress output by redirecting
	// stdout to /dev/null. Errors are captured into the summary and also
	// echoed to stderr by recordErr so they're visible without parsing JSON.
	jsonMode := JSONMode(args)
	var origStdout *os.File
	if jsonMode {
		origStdout = os.Stdout
		if devnull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0); err == nil {
			os.Stdout = devnull
			defer func() {
				os.Stdout = origStdout
				_ = devnull.Close()
			}()
		}
	}

	startedAt := time.Now()

	if HasFlag(args, "--history") || GetOption(args, "--since") != "" {
		fmt.Printf("\n%s🔄 Syncing history...%s\n", Fmt.Bold, Fmt.Reset)
	} else {
		fmt.Printf("\n%s🔄 Syncing everything...%s\n", Fmt.Bold, Fmt.Reset)
	}

	var newBookings, newEvents, newTx, newInvoices, newBills, newAttachments, newMessages, newImages int
	errs := map[string]string{}
	recordErr := func(source string, err error) {
		if err == nil {
			return
		}
		errs[source] = err.Error()
		if jsonMode {
			// Stdout is silenced; surface the error directly on stderr.
			fmt.Fprintf(os.Stderr, "⚠ %s: %v\n", source, err)
			return
		}
		fmt.Printf("%s⚠ %s: %v%s\n", Fmt.Yellow, source, err, Fmt.Reset)
	}

	fmt.Printf("\n%s━━━ Calendars ━━━%s\n", Fmt.Bold, Fmt.Reset)
	b, e, err := CalendarsSync(args)
	recordErr("calendars", err)
	newBookings = b
	newEvents = e

	fmt.Printf("\n%s━━━ Transactions ━━━%s\n", Fmt.Bold, Fmt.Reset)
	n, err := TransactionsSync(args)
	recordErr("transactions", err)
	newTx = n

	fmt.Printf("\n%s━━━ Messages ━━━%s\n", Fmt.Bold, Fmt.Reset)
	n, err = MessagesSync(args)
	recordErr("messages", err)
	newMessages = n

	// Odoo analytic sync (optional, only if configured)
	if os.Getenv("ODOO_URL") != "" {
		fmt.Printf("\n%s━━━ Odoo categories ━━━%s\n", Fmt.Bold, Fmt.Reset)
		_, err := OdooAnalyticSync(args)
		recordErr("odoo-categories", err)

		fmt.Printf("\n%s━━━ Invoices ━━━%s\n", Fmt.Bold, Fmt.Reset)
		n, err = InvoicesSync(args)
		recordErr("invoices", err)
		newInvoices = n

		fmt.Printf("\n%s━━━ Bills ━━━%s\n", Fmt.Bold, Fmt.Reset)
		n, err = BillsSync(args)
		recordErr("bills", err)
		newBills = n
	}

	// Members sync (optional, only if Stripe or Odoo configured)
	if os.Getenv("STRIPE_SECRET_KEY") != "" || os.Getenv("ODOO_URL") != "" {
		fmt.Printf("\n%s━━━ Members ━━━%s\n", Fmt.Bold, Fmt.Reset)
		recordErr("members", MembersSync(args))
	}

	if os.Getenv("ODOO_URL") != "" {
		fmt.Printf("\n%s━━━ Attachments ━━━%s\n", Fmt.Bold, Fmt.Reset)
		n, err = AttachmentsSync(args)
		recordErr("attachments", err)
		newAttachments = n
	}

	fmt.Printf("\n%s━━━ Images ━━━%s\n", Fmt.Bold, Fmt.Reset)
	n, err = ImagesSync(args)
	recordErr("images", err)
	newImages = n

	fmt.Printf("\n%s━━━ Generate ━━━%s\n", Fmt.Bold, Fmt.Reset)
	recordErr("generate", Generate(args))

	// Push generated transactions to Odoo journals after Generate has
	// rebuilt <year>/<month>/generated/transactions.json. Running it earlier
	// would push stale data (or skip months whose generated file is missing).
	if os.Getenv("ODOO_URL") != "" {
		fmt.Printf("\n%s━━━ Odoo journals (push) ━━━%s\n", Fmt.Bold, Fmt.Reset)
		recordErr("odoo-journals", odooJournalsSyncAll(args))
	}

	// Print summary
	hasAny := newBookings > 0 || newTx > 0 || newInvoices > 0 || newBills > 0 || newAttachments > 0 || newMessages > 0 || newImages > 0
	elapsed := time.Since(startedAt).Round(100 * time.Millisecond)

	if jsonMode {
		os.Stdout = origStdout
		summary := SyncSummary{
			ElapsedMS:      elapsed.Milliseconds(),
			NewBookings:    newBookings,
			NewEvents:      newEvents,
			NewTransactions: newTx,
			NewInvoices:    newInvoices,
			NewBills:       newBills,
			NewAttachments: newAttachments,
			NewMessages:    newMessages,
			NewImages:      newImages,
			Errors:         errs,
		}
		_ = EmitJSON(summary)
		return nil
	}
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
		if newInvoices > 0 {
			fmt.Printf("  🧾 %d invoices\n", newInvoices)
		}
		if newBills > 0 {
			fmt.Printf("  🧾 %d bills\n", newBills)
		}
		if newAttachments > 0 {
			fmt.Printf("  📎 %d attachments\n", newAttachments)
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
