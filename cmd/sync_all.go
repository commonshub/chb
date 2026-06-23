package cmd

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// formatCountSummary returns a short "N <noun>" phrase for the per-step
// final line — always non-empty so the operator sees a count instead of
// just the elapsed time, even when the answer is zero new items.
func formatCountSummary(n int, singular, plural string) string {
	if n == 1 {
		return fmt.Sprintf("1 %s", singular)
	}
	return fmt.Sprintf("%d %s", n, plural)
}

// formatNewSummary builds a summary for steps that produce two counts (e.g.
// CalendarsSync returns new bookings + new events). Always non-empty.
func formatNewSummary(a int, aSing, aPlu string, b int, bSing, bPlu string) string {
	return formatCountSummary(a, aSing, aPlu) + ", " + formatCountSummary(b, bSing, bPlu)
}

// truncErr returns a single-line summary of err suitable for embedding in a
// status line. Long multi-line errors get clipped — the full text already
// went to stderr via Warnf.
func truncErr(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	if i := strings.IndexByte(msg, '\n'); i >= 0 {
		msg = msg[:i]
	}
	if len(msg) > 60 {
		msg = msg[:57] + "…"
	}
	return msg
}

// silenceStdout redirects os.Stdout to /dev/null and returns a closure
// that restores it. Used by the compact pull output to swallow each
// sub-sync's chatter and keep the per-step layout to one line.
// Stderr is intentionally left alone so Warnf / deferred warnings
// still reach the operator.
func silenceStdout() func() {
	devnull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		return func() {}
	}
	origOut := os.Stdout
	os.Stdout = devnull
	_ = io.Discard
	return func() {
		os.Stdout = origOut
		_ = devnull.Close()
	}
}

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
// syncStepTimeout bounds a single network push step. A non-responding remote
// (Odoo, a Nostr relay) would otherwise block the whole `chb sync` loop.
const syncStepTimeout = 30 * time.Second

// runWithTimeout runs fn but stops waiting after d, returning a clear timeout
// error so the caller can move on to the next step. Go can't force-cancel
// blocking I/O, so the abandoned goroutine keeps running until the underlying
// client's own timeout fires; the buffered channel keeps it from leaking. Use
// only for steps whose fn returns just an error and writes no shared state
// (Odoo push, Nostr push) — otherwise the late write would race.
func runWithTimeout(label string, d time.Duration, fn func() error) error {
	done := make(chan error, 1)
	go func() { done <- fn() }()
	select {
	case err := <-done:
		return err
	case <-time.After(d):
		return fmt.Errorf("%s did not respond within %s — the remote may be down or slow; skipping so the next step can run", label, d)
	}
}

func SyncAll(args []string) error {
	if HasFlag(args, "--help", "-h", "help") {
		PrintSyncAllHelp()
		return nil
	}

	// Verbose mode keeps the current per-step header banners and the
	// full sub-sync output. The default is compact: redirect each sub-
	// sync's stdout into a buffer, then print one summary line per step
	// with the returned counts. Errors are always surfaced via
	// recordErr regardless of verbosity.
	verbose := HasFlag(args, "--verbose", "-v") || HasFlag(args, "--debug")
	jsonMode := JSONMode(args)
	compact := !verbose && !jsonMode

	// In --json mode, silence every sub-sync's progress output by redirecting
	// stdout to /dev/null. Errors are captured into the summary and also
	// echoed to stderr by recordErr so they're visible without parsing JSON.
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
	ResetCapturedStepDiagnostics()

	providerCount := countActivePullProviders()
	sinceLabel := pullSinceLabel(args)
	fmt.Printf("\n%sPulling latest data from %d provider%s since %s%s\n\n",
		Fmt.Bold, providerCount, plural(providerCount), sinceLabel, Fmt.Reset)
	renderSyncHeader("Sources:", pullProviderHeaderItems(), verbose)

	var newBookings, newEvents, newTx, newInvoices, newBills, newAttachments, newMessages, newImages int
	errs := map[string]string{}
	recordErr := func(source string, err error) {
		if err == nil {
			return
		}
		errs[source] = err.Error()
		if jsonMode {
			// Stdout is silenced; surface the error directly on stderr.
			Warnf("⚠ %s: %v", source, err)
			return
		}
		// Inline-quiet: the step's own ✗ mark already flagged this on
		// the row, and the consolidated footer (PrintCapturedDiagnostics
		// after the loop) will print the full detail. Logging via
		// LogErrorf keeps the daily log honest without spamming stderr.
		LogErrorf("%s: %v", source, err)
	}

	// step runs fn with stdout swallowed (compact mode) or untouched
	// (verbose), times it, and prints a live status line via StatusLine
	// (compact) or a per-step banner (verbose). The fn returns a short
	// summary string to embed in the final line (e.g. "12 new, 3 updated").
	step := func(label string, fn func() (string, error)) {
		key := strings.ToLower(label)
		if !compact {
			fmt.Printf("\n%s━━━ %s ━━━%s\n", Fmt.Bold, label, Fmt.Reset)
			_, err := fn()
			recordErr(key, err)
			return
		}
		// Compact mode: live status line on stderr, sub-sync stdout
		// silenced so its chatter doesn't break the layout. Diagnostic
		// capture buffers any warning emitted inside fn() so the row
		// can surface a ⚠ mark and the footer renders the detail.
		diag := BeginStepDiagnostics(label)
		sl := NewStatusLine(label)
		SetActiveStatusLine(sl)
		restore := silenceStdout()
		summary, err := fn()
		restore()
		SetActiveStatusLine(nil)
		// Attach the returned error onto the step bucket BEFORE ending
		// it, so the footer surfaces it under this step's label.
		if err != nil {
			diag.Errors = append(diag.Errors, err.Error())
		}
		EndStepDiagnostics()
		recordErr(key, err)
		if err != nil && summary == "" {
			summary = "error: " + truncErr(err)
		}
		sl.Final(StepMark(err, diag), summary)
	}

	step("Calendars", func() (string, error) {
		b, e, err := CalendarsSync(args)
		newBookings = b
		newEvents = e
		return formatNewSummary(b, "booking", "bookings", e, "event", "events"), err
	})
	step("Transactions", func() (string, error) {
		n, err := TransactionsSync(args)
		newTx = n
		return formatCountSummary(n, "new transaction", "new transactions"), err
	})
	step("Messages", func() (string, error) {
		n, err := MessagesSync(args)
		newMessages = n
		return formatCountSummary(n, "new message", "new messages"), err
	})

	if os.Getenv("ODOO_URL") != "" {
		step("Odoo categories", func() (string, error) {
			n, err := OdooAnalyticSync(args)
			return formatCountSummary(n, "new category", "new categories"), err
		})
		step("Invoices", func() (string, error) {
			n, err := InvoicesSync(args)
			newInvoices = n
			return formatCountSummary(n, "invoice", "invoices"), err
		})
		step("Bills", func() (string, error) {
			n, err := BillsSync(args)
			newBills = n
			return formatCountSummary(n, "bill", "bills"), err
		})
	}

	if os.Getenv("STRIPE_SECRET_KEY") != "" || os.Getenv("ODOO_URL") != "" {
		step("Members", func() (string, error) { return "", MembersSync(args) })
	}

	if os.Getenv("ODOO_URL") != "" {
		step("Attachments", func() (string, error) {
			n, err := AttachmentsSync(args)
			newAttachments = n
			return formatCountSummary(n, "attachment", "attachments"), err
		})
	}

	step("Images", func() (string, error) {
		n, err := ImagesSync(args)
		newImages = n
		return formatCountSummary(n, "new image", "new images"), err
	})
	step("Generate", func() (string, error) { return "", Generate(args) })

	// Note: pushing local data to Odoo is intentionally NOT part of
	// `chb pull`. Pull is read-only (fetch from providers + local
	// transform). To push to Odoo journals, run
	// `chb odoo journals push` explicitly after this finishes.
	if os.Getenv("ODOO_URL") != "" {
		fmt.Printf("\n  %sTo push to Odoo: chb odoo journals push%s\n", Fmt.Dim, Fmt.Reset)
	}

	elapsed := time.Since(startedAt).Round(100 * time.Millisecond)

	if jsonMode {
		os.Stdout = origStdout
		summary := SyncSummary{
			ElapsedMS:       elapsed.Milliseconds(),
			NewBookings:     newBookings,
			NewEvents:       newEvents,
			NewTransactions: newTx,
			NewInvoices:     newInvoices,
			NewBills:        newBills,
			NewAttachments:  newAttachments,
			NewMessages:     newMessages,
			NewImages:       newImages,
			Errors:          errs,
		}
		_ = EmitJSON(summary)
		return nil
	}
	touchedFiles, touchedPaths := countTouchedProviderFiles(startedAt, verbose)
	fmt.Printf("\n%s✓ Updated %d file%s in %s in %s%s\n\n",
		Fmt.Green, touchedFiles, plural(touchedFiles),
		touchedPaths,
		FormatElapsedFixed(elapsed),
		Fmt.Reset)
	// Captured warnings/errors are visible via the row marks; the
	// detailed per-phase Issues block has been removed in favour of
	// the single "N errors and M warnings, written in <log>" tail
	// printed at process exit.

	// Record the pull's completion time so the next pull's header can
	// show "since <last pull>". Persisted via the same mechanism every
	// other sub-sync uses.
	UpdateSyncSource("pull", HasFlag(args, "--history") || GetOption(args, "--since") != "")
	UpdateSyncActivity(false)

	return nil
}

// PushAllTargets is the entry point for `chb push`: pushes every local
// pending change to every configured target (Odoo journals + Nostr).
// Each step prints its own header / summary; the function returns the
// first error so cron jobs can detect failures via exit code.
//
// Skipped silently when the relevant target isn't configured (no Odoo
// creds → no Odoo push, no Nostr keys → no Nostr push). The expectation
// is "push what's pushable", not "push everything or fail".
func PushAllTargets(args []string) error {
	if HasFlag(args, "--help", "-h", "help") {
		printPushAllHelp()
		return nil
	}
	verbose := HasFlag(args, "--verbose", "-v") || HasFlag(args, "--debug")
	dryRun := HasFlag(args, "--dry-run")
	assumeYes := HasFlag(args, "--yes", "-y")
	// Reset captured-diagnostics for a fresh footer at the end. When
	// PushAllTargets is invoked as the second half of `chb sync`, the
	// pull half has already printed its own footer, so it's safe to
	// clear here.
	ResetCapturedStepDiagnostics()
	startedAt := time.Now()
	plan, err := buildPushConfirmationPlan(args)
	ResetCapturedStepDiagnostics()
	if err != nil {
		return err
	}
	if !dryRun && !assumeYes {
		if err := confirmPushPlan(plan, args); err != nil {
			return err
		}
	}
	fmt.Printf("\n  %sPushing changes%s%s\n", Fmt.Bold, Fmt.Reset, odooTargetHeaderSuffix())
	renderSyncHeader("", pushTargetHeaderItems(), verbose)
	var firstErr error
	if os.Getenv("ODOO_URL") != "" {
		// Odoo target (URL + DB) is already shown in the "Pushing
		// changes — Odoo: <db>" banner above and on the row labels,
		// so the standalone "Odoo target: …" banner that
		// printOdooWriteBannerOnce used to add is suppressed here.
		// Bounded so an unresponsive Odoo can't freeze the whole push;
		// after syncStepTimeout we surface a clear error and move on to Nostr.
		if err := runWithTimeout("Odoo push", syncStepTimeout, func() error {
			return odooJournalsSyncAll(args)
		}); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	// Nostr push uses signing keys configured via `chb setup nostr`. The
	// inner NostrPush logs and returns nil when nothing to push, so it's
	// safe to call unconditionally; the no-op case is a one-liner.
	//
	// Wrap in a quiet-context + status-line block so its (potentially
	// long) per-event progress shows as a single live line instead of a
	// wall of "... N/M" prints. quietOdooContext also tells NostrPush
	// to skip its standalone preview banner.
	wasQuiet := quietOdooContext()
	setQuietOdooContext(true)
	diag := BeginStepDiagnostics("Nostr")
	sl := NewStatusLine("Nostr")
	SetActiveStatusLine(sl)
	var nostrErr error
	// Bounded so an unreachable relay can't freeze the loop at the Nostr step.
	nostrPush := func() error {
		return runWithTimeout("Nostr push", syncStepTimeout, func() error { return NostrPush(args) })
	}
	if verbose {
		nostrErr = nostrPush()
	} else {
		restore := silenceStdout()
		nostrErr = nostrPush()
		restore()
	}
	SetActiveStatusLine(nil)
	if nostrErr != nil {
		diag.Errors = append(diag.Errors, nostrErr.Error())
	}
	EndStepDiagnostics()
	sl.Final(StepMark(nostrErr, diag), "outbox flushed")
	setQuietOdooContext(wasQuiet)
	if nostrErr != nil && firstErr == nil {
		firstErr = nostrErr
	}

	// Per-phase wall-clock footer. Warnings/errors are surfaced via
	// the row's ⚠/✗ mark and the daily log; the per-phase "Issues"
	// block has been removed — the single tail summary printed by
	// PrintDiagnosticsSummary at process exit ("N errors and M
	// warnings, written in <log>") is the one summary line we keep.
	fmt.Fprintf(os.Stderr, "\n  %sPush done in %s%s\n",
		Fmt.Dim, FormatElapsedFixed(time.Since(startedAt).Round(100*time.Millisecond)), Fmt.Reset)

	return firstErr
}

type pushConfirmationJournal struct {
	Slug       string
	JournalID  int
	Count      int
	OtherCount int
	Status     string
}

type pushConfirmationPlan struct {
	OdooURL                 string
	OdooDB                  string
	Journals                []pushConfirmationJournal
	NostrOutbox             int
	NostrOutboxUniqueURIs   int
	NostrOutboxDuplicateURI int
	NostrOutboxWithoutURI   int
	NostrTransactions       int
	NostrInvoices           int
	NostrBills              int
}

func buildPushConfirmationPlan(args []string) (pushConfirmationPlan, error) {
	var plan pushConfirmationPlan
	if creds, err := ResolveOdooCredentials(); err == nil {
		plan.OdooURL = creds.URL
		plan.OdooDB = creds.DB
		journals, err := previewOdooPushJournals(args)
		if err != nil {
			return plan, err
		}
		plan.Journals = journals
	}
	if LoadNostrKeys() != nil {
		outbox := countQueuedNostrOutbox()
		plan.NostrOutbox = outbox.Total
		plan.NostrOutboxUniqueURIs = outbox.UniqueURIs
		plan.NostrOutboxDuplicateURI = outbox.DuplicateURIs
		plan.NostrOutboxWithoutURI = outbox.WithoutURI
		plan.NostrTransactions = countPendingTransactionAnnotations(args)
		if n, err := countPendingMoveAnnotations(moveKindInvoice, args); err == nil {
			plan.NostrInvoices = n
		}
		if n, err := countPendingMoveAnnotations(moveKindBill, args); err == nil {
			plan.NostrBills = n
		}
	}
	return plan, nil
}

func previewOdooPushJournals(args []string) ([]pushConfirmationJournal, error) {
	if err := RequireOdooWriteCapability(); err != nil {
		return nil, err
	}
	configs := LoadAccountConfigs()
	dryArgs := appendFlagIfMissing(args, "--dry-run")
	dryArgs = appendFlagIfMissing(dryArgs, "--yes")
	wasQuiet := quietOdooContext()
	setQuietOdooContext(true)
	defer setQuietOdooContext(wasQuiet)

	var out []pushConfirmationJournal
	for _, acc := range configs {
		if acc.OdooJournalID == 0 || acc.IsOdooSourceOfTruth() {
			continue
		}
		var row journalSyncRow
		journalRowSink = &row
		restore := silenceStdout()
		diag := BeginStepDiagnostics(fmt.Sprintf("#%d %s preflight", acc.OdooJournalID, acc.Slug))
		err := AccountOdooPush(acc.Slug, dryArgs)
		EndStepDiagnostics()
		restore()
		journalRowSink = nil
		if err != nil {
			return out, err
		}
		if row.Status == "" && len(diag.Warnings) > 0 {
			row.Status = strings.Join(diag.Warnings, "; ")
		}
		status := addOdooPushPlanNotes(row.Status, args)
		count := plannedOdooWriteCount(status)
		otherCount := plannedOdooOtherWriteCount(status, args)
		if count+otherCount == 0 {
			continue
		}
		out = append(out, pushConfirmationJournal{
			Slug:       acc.Slug,
			JournalID:  acc.OdooJournalID,
			Count:      count,
			OtherCount: otherCount,
			Status:     status,
		})
	}
	return out, nil
}

func appendFlagIfMissing(args []string, flag string) []string {
	if HasFlag(args, flag) {
		return args
	}
	out := make([]string, 0, len(args)+1)
	out = append(out, args...)
	out = append(out, flag)
	return out
}

func plannedOdooWriteCount(status string) int {
	var n int
	if _, err := fmt.Sscanf(status, "dry-run: %d tx would be uploaded", &n); err == nil {
		return n
	}
	if _, err := fmt.Sscanf(status, "dry-run: %d line would be merged", &n); err == nil {
		return n
	}
	if _, err := fmt.Sscanf(status, "dry-run: %d lines would be merged", &n); err == nil {
		return n
	}
	if _, err := fmt.Sscanf(status, "%d new", &n); err == nil {
		return n
	}
	return 0
}

func plannedOdooOtherWriteCount(status string, args []string) int {
	total := 0
	for _, label := range []string{"partners", "accounts", "metadata"} {
		if i := strings.Index(status, label+" "); i >= 0 {
			var updated, reviewed int
			if _, err := fmt.Sscanf(status[i:], label+" %d/%d", &updated, &reviewed); err == nil {
				total += updated
			}
		}
	}
	if HasFlag(args, "--force") {
		total++
	}
	if GetOption(args, "--startingBalance", "--starting-balance") != "" && !HasFlag(args, "--force") {
		total++
	}
	return total
}

func addOdooPushPlanNotes(status string, args []string) string {
	var notes []string
	if status != "" {
		notes = append(notes, status)
	}
	if HasFlag(args, "--force") {
		notes = append(notes, "journal reset/rebuild")
	}
	if GetOption(args, "--startingBalance", "--starting-balance") != "" && !HasFlag(args, "--force") {
		notes = append(notes, "starting-balance convergence")
	}
	return strings.Join(notes, ", ")
}

type nostrOutboxPlanStats struct {
	Total         int
	UniqueURIs    int
	DuplicateURIs int
	WithoutURI    int
}

func countQueuedNostrOutbox() nostrOutboxPlanStats {
	entries, err := os.ReadDir(nostrOutboxDir())
	if err != nil {
		return nostrOutboxPlanStats{}
	}
	stats := nostrOutboxPlanStats{}
	seen := map[string]bool{}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		stats.Total++
		item, err := readQueuedNostrEvent(filepath.Join(nostrOutboxDir(), e.Name()))
		if err != nil || item.URI == "" {
			stats.WithoutURI++
			continue
		}
		if seen[item.URI] {
			stats.DuplicateURIs++
			continue
		}
		seen[item.URI] = true
		stats.UniqueURIs++
	}
	return stats
}

func (p pushConfirmationPlan) totalWrites() int {
	total := p.NostrOutbox + p.NostrTransactions + p.NostrInvoices + p.NostrBills
	for _, j := range p.Journals {
		total += j.Count + j.OtherCount
	}
	return total
}

func confirmPushPlan(plan pushConfirmationPlan, args []string) error {
	if plan.totalWrites() == 0 {
		return nil
	}
	fmt.Printf("\n  %sPush confirmation%s\n", Fmt.Bold, Fmt.Reset)
	if plan.OdooURL != "" || plan.OdooDB != "" {
		fmt.Printf("  %sOdoo target:%s %s (DB: %s)\n", Fmt.Dim, Fmt.Reset, plan.OdooURL, plan.OdooDB)
	}
	if len(plan.Journals) > 0 {
		fmt.Printf("  %sOdoo writes:%s\n", Fmt.Dim, Fmt.Reset)
		for _, j := range plan.Journals {
			other := ""
			if j.OtherCount > 0 {
				other = fmt.Sprintf(", %s", Pluralize(j.OtherCount, "other Odoo write", ""))
			}
			fmt.Printf("    #%d %s: %s%s\n", j.JournalID, j.Slug, Pluralize(j.Count, "new transaction", ""), other)
		}
		if !HasFlag(args, "--skip-reconcile", "--skip-reconciliation") {
			fmt.Printf("    %sAuto-reconcile may run on journals with %d or fewer new transactions.%s\n", Fmt.Dim, reconcileAutoThreshold, Fmt.Reset)
		}
	}
	if plan.NostrOutbox+plan.NostrTransactions+plan.NostrInvoices+plan.NostrBills > 0 {
		fmt.Printf("  %sNostr posts:%s %s already queued in outbox; new annotations: %s transactions, %s invoices, %s bills\n",
			Fmt.Dim, Fmt.Reset,
			Pluralize(plan.NostrOutbox, "event", ""),
			formatThousands(plan.NostrTransactions),
			formatThousands(plan.NostrInvoices),
			formatThousands(plan.NostrBills))
		if plan.NostrOutbox > 0 {
			details := []string{fmt.Sprintf("%s unique URIs", formatThousands(plan.NostrOutboxUniqueURIs))}
			if plan.NostrOutboxDuplicateURI > 0 {
				details = append(details, fmt.Sprintf("%s duplicate URI events", formatThousands(plan.NostrOutboxDuplicateURI)))
			}
			if plan.NostrOutboxWithoutURI > 0 {
				details = append(details, fmt.Sprintf("%s without URI", formatThousands(plan.NostrOutboxWithoutURI)))
			}
			fmt.Printf("    %sOutbox detail: %s.%s\n", Fmt.Dim, strings.Join(details, ", "), Fmt.Reset)
		}
	}
	if !isInteractiveTTY() {
		return fmt.Errorf("refusing to push without --yes on a non-interactive shell")
	}
	fmt.Printf("\n  %sApply these changes? [y/N] %s", Fmt.Bold, Fmt.Reset)
	resp, _ := bufio.NewReader(os.Stdin).ReadString('\n')
	resp = strings.ToLower(strings.TrimSpace(resp))
	if resp != "y" && resp != "yes" {
		return fmt.Errorf("cancelled")
	}
	return nil
}

// PrintSyncCronHelp is the help shown for the all-in-one `chb sync`
// command — the recommended cron entrypoint.
func PrintSyncCronHelp() {
	printMirrorModeHelpBanner()
	fmt.Printf(`
%schb sync%s — Full cron loop: pull from every source, then push to every target

%sUSAGE%s
  %schb sync%s [options]

%sDESCRIPTION%s
  Runs the two halves of the pipeline in order. Equivalent to:
    %schb pull%s && %schb push%s

  Designed to be safe to run unattended every hour:
    - Pull is read-only on remotes; cannot create duplicates.
    - Push auto-reconciles only when ≤ %d new lines were created
      (large back-fills skip auto-reconcile; run
      %schb odoo journals <id> reconcile%s to handle them explicitly).

%sOPTIONS%s  (forwarded to both pull and push)
  %s--dry-run%s          Preview without writing anything
  %s--skip-reconcile%s   Skip the auto-reconcile after push
  %s--since <date>%s     Pull from this date onwards (full history if older than cache)
  %s--history%s          Pull from the earliest cached month
  %s--verbose, -v%s      Show per-step progress instead of the compact view
  %s--help, -h%s         Show this help

%sCRON EXAMPLE%s
  %s# /etc/cron.d/chb%s
  %s0 * * * *  user  chb sync%s
`,
		Fmt.Bold, Fmt.Reset,
		Fmt.Bold, Fmt.Reset,
		Fmt.Cyan, Fmt.Reset,
		Fmt.Bold, Fmt.Reset,
		Fmt.Cyan, Fmt.Reset,
		Fmt.Cyan, Fmt.Reset,
		reconcileAutoThreshold,
		Fmt.Cyan, Fmt.Reset, // chb odoo journals <id> reconcile (inline hint)
		Fmt.Bold, Fmt.Reset,
		Fmt.Yellow, Fmt.Reset, // --dry-run
		Fmt.Yellow, Fmt.Reset, // --skip-reconcile
		Fmt.Yellow, Fmt.Reset, // --since
		Fmt.Yellow, Fmt.Reset, // --history
		Fmt.Yellow, Fmt.Reset, // --verbose, -v
		Fmt.Yellow, Fmt.Reset, // --help, -h
		Fmt.Bold, Fmt.Reset,
		Fmt.Dim, Fmt.Reset,
		Fmt.Cyan, Fmt.Reset,
	)
}

func printPushAllHelp() {
	fmt.Printf(`
%schb push%s — Push local changes to every configured target

%sUSAGE%s
  %schb push%s [options]

%sDESCRIPTION%s
  Walks every configured push target (Odoo journals, then Nostr outbox)
  and publishes whatever pending changes are waiting locally. By default,
  live mode previews the planned writes and asks for confirmation; use
  --yes for unattended runs. A failure on one target does not abort the
  others.

  Equivalent to:
    %schb odoo journals push%s
    %schb nostr push%s

%sOPTIONS%s
  %s--dry-run%s          Preview what would be pushed
  %s--yes, -y%s          Push without the confirmation prompt
  %s--skip-reconcile%s   Skip the auto-reconcile after push
  %s--help, -h%s         Show this help

%sCRON%s
  Pair with %schb pull%s for the full loop:
    %schb sync%s         # = chb pull && chb push --yes
`,
		Fmt.Bold, Fmt.Reset,
		Fmt.Bold, Fmt.Reset,
		Fmt.Cyan, Fmt.Reset,
		Fmt.Bold, Fmt.Reset,
		Fmt.Cyan, Fmt.Reset,
		Fmt.Cyan, Fmt.Reset,
		Fmt.Bold, Fmt.Reset,
		Fmt.Yellow, Fmt.Reset, // --dry-run
		Fmt.Yellow, Fmt.Reset, // --yes
		Fmt.Yellow, Fmt.Reset, // --skip-reconcile
		Fmt.Yellow, Fmt.Reset, // --help, -h
		Fmt.Bold, Fmt.Reset,
		Fmt.Cyan, Fmt.Reset,
		Fmt.Cyan, Fmt.Reset,
	)
}

// countActivePullProviders returns how many sub-syncs will actually run,
// driven by env vars (sources that need credentials only count when those
// credentials are present). Used in the header to give the operator an
// honest preview of what's about to be pulled.
func countActivePullProviders() int {
	n := 3 // Calendars, Transactions, Messages — always attempted
	if os.Getenv("ODOO_URL") != "" {
		n += 4 // Odoo categories, Invoices, Bills, Attachments
	}
	if os.Getenv("STRIPE_SECRET_KEY") != "" || os.Getenv("ODOO_URL") != "" {
		n++ // Members
	}
	n++ // Images
	return n
}

// pullSinceLabel formats the "since" suffix used in the pull header. Uses
// explicit --since when present, --history when requested, otherwise the
// last successful pull's recorded timestamp.
func pullSinceLabel(args []string) string {
	if HasFlag(args, "--history") {
		return "the beginning of recorded history"
	}
	if v := GetOption(args, "--since"); v != "" {
		return v
	}
	if last := LastSyncTime("pull"); !last.IsZero() {
		return last.In(BrusselsTZ()).Format("2006-01-02 15:04")
	}
	return "the start of the default recent window"
}

// countTouchedProviderFiles walks DATA_DIR/latest/providers and
// DATA_DIR/<year>/<month>/providers, counting files whose mtime is newer
// than startedAt. Returns the count and a human-readable path label —
// relative by default, absolute with --verbose. Cross-month pulls
// summarise as ".../YYYY/MM/providers" if all updates landed in one month,
// or ".../YYYY/" if they spread, so the footer stays short.
func countTouchedProviderFiles(startedAt time.Time, verbose bool) (int, string) {
	dataDir := DataDir()
	count := 0
	monthsHit := map[string]bool{}
	latestHit := false

	walk := func(root string, recordMonth func(string)) {
		filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err != nil || info == nil || info.IsDir() {
				return nil
			}
			if info.ModTime().Before(startedAt) {
				return nil
			}
			count++
			if recordMonth != nil {
				recordMonth(path)
			}
			return nil
		})
	}

	latestPath := filepath.Join(dataDir, "latest", "providers")
	if _, err := os.Stat(latestPath); err == nil {
		before := count
		walk(latestPath, nil)
		if count > before {
			latestHit = true
		}
	}

	// Walk each <year>/<month>/providers to identify which months received
	// updates. Cheap: we only descend two levels before reaching providers/.
	years, _ := os.ReadDir(dataDir)
	for _, y := range years {
		if !y.IsDir() || len(y.Name()) != 4 {
			continue
		}
		months, _ := os.ReadDir(filepath.Join(dataDir, y.Name()))
		for _, m := range months {
			if !m.IsDir() || len(m.Name()) != 2 {
				continue
			}
			provPath := filepath.Join(dataDir, y.Name(), m.Name(), "providers")
			if _, err := os.Stat(provPath); err != nil {
				continue
			}
			before := count
			walk(provPath, nil)
			if count > before {
				monthsHit[y.Name()+"/"+m.Name()] = true
			}
		}
	}

	// Compose the path label. Goal: read like the spec the operator wrote
	// — `DATA_DIR/latest/providers and DATA_DIR/2026/05/providers`.
	root := "DATA_DIR"
	if verbose {
		root = dataDir
	}
	parts := []string{}
	if latestHit {
		parts = append(parts, root+"/latest/providers")
	}
	switch len(monthsHit) {
	case 0:
		// nothing per-month
	case 1:
		for ym := range monthsHit {
			parts = append(parts, root+"/"+ym+"/providers")
		}
	default:
		// Multiple months — list them compactly.
		sorted := make([]string, 0, len(monthsHit))
		for ym := range monthsHit {
			sorted = append(sorted, ym)
		}
		sort.Strings(sorted)
		parts = append(parts, fmt.Sprintf("%s/{%s}/providers", root, strings.Join(sorted, ", ")))
	}
	if len(parts) == 0 {
		return count, root + "/providers"
	}
	if len(parts) == 1 {
		return count, parts[0]
	}
	return count, strings.Join(parts[:len(parts)-1], ", ") + " and " + parts[len(parts)-1]
}
