package cmd

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"
)

const odooInternalTransferAccountCode = "580000"

var odooInvoiceReferencePattern = regexp.MustCompile(`\b[A-Z]+/\d{4}/\d+\b`)

type odooStatementLineForReconcile struct {
	ID                int
	Date              string
	Amount            float64
	PaymentRef        string
	Narration         string
	UniqueImportID    string
	PartnerID         int
	PartnerName       string
	PartnerBankID     int
	StatementID       int
	MoveID            int
	IsReconciled      bool
	ReconciledTo      string
	ReconciledLineIDs []int
}

type odooMoveCandidate struct {
	ID             int
	Name           string
	InvoiceDate    string
	Date           string
	MoveType       string
	PartnerID      int
	PartnerName    string
	AmountResidual float64
}

type odooLineReconcileResult struct {
	Reconciled        bool
	InternalTransfer  bool
	Ambiguous         bool
	NoPartner         bool
	NoMatch           bool
	Err               error
	Message           string
	CandidateCount    int
	CandidateMoveName string
}

func odooJournalReconcile(creds *OdooCredentials, uid int, journalID int, assumeYes, dryRun, verbose bool) error {
	// Auto-reconcile after a push: do NOT run the partner-linking pass. It's a
	// deliberate, journal-wide "create partners" operation that scans every
	// no-partner line (thousands on a Stripe journal) and belongs in the
	// explicit `chb odoo journals <id> reconcile` command, not on every push.
	return odooJournalReconcileInteractive(creds, uid, journalID, assumeYes, dryRun, verbose, false, false)
}

// odooJournalReconcileInteractive is the variant that surfaces the
// `-i` / `--interactive` flag — used by `chb odoo journals <id> reconcile`
// from the CLI dispatch. Auto-reconcile-after-push calls (cron path) use
// the non-interactive odooJournalReconcile so they never block on stdin.
//
// Both --dry-run and live mode share the same matcher (computeReconcileMatches),
// so the summary you see before confirming is what gets applied. Live mode adds
// a counterpart-reset pre-pass (only run after confirmation) and an apply phase
// that actually calls account.move.line.reconcile via XML-RPC.
func odooJournalReconcileInteractive(creds *OdooCredentials, uid int, journalID int, assumeYes, dryRun, verbose, interactive, linkPartners bool) error {
	header := "Reconcile preview for journal #%d (local-only)"
	if !dryRun {
		// Show the write target up front — before the confirm prompt, so
		// the operator can abort if it's the wrong DB.
		printOdooWriteBannerOnce(creds.URL, creds.DB)
		header = "Reconciling journal #%d"
	}
	fmt.Printf("\n  %s"+header+"%s\n\n", Fmt.Bold, journalID, Fmt.Reset)

	// Ensure every line is linked to a partner before matching invoices —
	// reconciliation keys off the partner, and a freshly-linked partner lets
	// the matcher below find that counterparty's open invoices/bills. Only on
	// the explicit reconcile command (linkPartners); the post-push auto-pass
	// skips it to stay fast.
	if linkPartners {
		if err := linkStatementLinePartners(creds, uid, journalID, assumeYes, dryRun); err != nil {
			Warnf("  %s⚠ Partner linking skipped: %v%s", Fmt.Yellow, err, Fmt.Reset)
		}
		// Durably link every Stripe customer to its partner by attaching the
		// customer id as a bank account (so future lines match on the id).
		if !dryRun {
			if acc := linkedAccountForJournal(journalID); acc != nil && acc.Provider == "stripe" {
				if err := attachStripeCustomerIDs(creds, uid, journalID, acc); err != nil {
					Warnf("  %s⚠ Stripe customer-id attach skipped: %v%s", Fmt.Yellow, err, Fmt.Reset)
				}
			}
		}
	}

	Progress("computing reconcile matches")
	set, _, err := computeReconcileMatches(journalID, interactive)
	if err != nil {
		if err == errNoLocalCandidates {
			fmt.Printf("  %sNo open invoices or bills in the local private cache.%s\n", Fmt.Yellow, Fmt.Reset)
			fmt.Printf("  %s(Run `chb odoo pull` to refresh, or check that ODOO_URL gives access to the records.)%s\n\n", Fmt.Dim, Fmt.Reset)
			return nil
		}
		return err
	}

	if dryRun {
		printReconcileMatches(set, verbose)
		fmt.Printf("  %s(local-only preview — no Odoo calls. Re-run without --dry-run to apply.)%s\n\n",
			Fmt.Dim, Fmt.Reset)
		return nil
	}

	// Live mode: show counts before confirming so the operator sees the
	// same numbers --dry-run does (matched / ambiguous / no-match / dupes).
	printReconcileSummary(set)
	winners := set.unambiguousWinners()
	if len(winners) == 0 {
		fmt.Printf("\n  %sNothing to reconcile.%s Re-run with %s-i%s to resolve ambiguous matches.\n\n",
			Fmt.Dim, Fmt.Reset, Fmt.Bold, Fmt.Reset)
		return nil
	}

	if !assumeYes && isInteractiveTTY() {
		fmt.Printf("\n  %sReconcile %d match%s on Odoo?%s [Y/n] ",
			Fmt.Bold, len(winners), plural(len(winners)), Fmt.Reset)
		reader := bufio.NewReader(os.Stdin)
		resp, _ := reader.ReadString('\n')
		resp = strings.TrimSpace(strings.ToLower(resp))
		if resp == "n" || resp == "no" {
			fmt.Println("  Aborted.")
			return nil
		}
	}

	// Pre-pass: reset counterparts that landed on A/R or A/P (typically
	// from a catch-all OdooMapping rule). Reconcile won't replace those
	// without first putting them back on the journal's suspense account.
	// Only touches the bank lines we're about to act on.
	resetTargets := make([]odooStatementLineForReconcile, 0, len(winners))
	for _, w := range winners {
		resetTargets = append(resetTargets, odooStatementLineForReconcile{ID: w.Line.ID, MoveID: w.Line.MoveID})
	}
	Progress(fmt.Sprintf("resetting %d counterpart(s) to suspense", len(resetTargets)))
	if _, err := resetOverCategorizedToSuspense(creds, uid, journalID, resetTargets, false, verbose); err != nil {
		return fmt.Errorf("reset over-categorized lines: %v", err)
	}

	// Apply phase: build adapter structs from the local cache + matcher
	// results and call reconcileStatementLineWithMove for each winner.
	// MoveID + Amount are the only fields it actually uses on `line`;
	// candidate.ID + AmountResidual are the only fields it uses on `move`.
	var reconciled, alreadyDone, failed int
	touchedInvoiceMoveIDs := make([]int, 0, len(winners))
	for i, w := range winners {
		Progress(fmt.Sprintf("reconciling %d/%d", i+1, len(winners)))
		line := odooStatementLineForReconcile{
			ID:     w.Line.ID,
			MoveID: w.Line.MoveID,
			Amount: w.Line.Amount,
		}
		cand := w.Hits[0]
		move := odooMoveCandidate{
			ID:             cand.ID,
			Name:           cand.Number,
			PartnerID:      cand.PartnerID,
			PartnerName:    cand.PartnerName,
			AmountResidual: cand.Residual,
		}
		err := reconcileStatementLineWithMove(creds, uid, line, move)
		if err == nil {
			reconciled++
			touchedInvoiceMoveIDs = append(touchedInvoiceMoveIDs, cand.ID)
			if verbose {
				fmt.Printf("  %s✓%s line #%d %s %s → %s %s\n",
					Fmt.Green, Fmt.Reset, line.ID, w.Line.Date,
					formatBalancePlain(line.Amount, "EUR"), cand.Kind, cand.label())
			}
			continue
		}
		// Cache-staleness post-conditions (invoice already paid, line
		// already reconciled). Local cache showed open but Odoo finished
		// it in another session. Treat as a no-op rather than failure.
		if reconcileStaleCacheError(err) {
			alreadyDone++
			touchedInvoiceMoveIDs = append(touchedInvoiceMoveIDs, cand.ID)
			if verbose {
				fmt.Printf("  %s·%s line #%d → %s %s: %s\n",
					Fmt.Dim, Fmt.Reset, line.ID, cand.Kind, cand.label(),
					"already reconciled in Odoo (cache stale)")
			}
			continue
		}
		failed++
		// Always record the per-line failure in the daily log so it
		// survives the compact UI (status lines don't preserve detail).
		// Verbose mode additionally echoes to stderr.
		LogErrorf("reconcile failed: journal #%d line #%d → %s %s: %v",
			journalID, line.ID, cand.Kind, cand.label(), err)
		if verbose {
			fmt.Printf("  %s✗%s line #%d → %s %s: %v\n", Fmt.Red, Fmt.Reset, line.ID, cand.Kind, cand.label(), err)
		}
	}

	fmt.Printf("\n  %sReconciled %d match%s%s",
		Fmt.Green, reconciled, plural(reconciled), Fmt.Reset)
	if alreadyDone > 0 {
		fmt.Printf(" (%s%d already reconciled — cache was stale%s)", Fmt.Dim, alreadyDone, Fmt.Reset)
	}
	if failed > 0 {
		fmt.Printf(" (%s%d failed%s)", Fmt.Red, failed, Fmt.Reset)
	}
	fmt.Println()
	if failed > 0 && !verbose {
		fmt.Printf("  %sRe-run with --verbose to see per-line failures.%s\n", Fmt.Dim, Fmt.Reset)
	}

	// Refresh local caches so they reflect the writes we just made:
	//   - journal-lines cache: counterpart account_id + reconciled flags
	//   - private invoice/bill caches: payment_state + residual
	// Without this the next dry-run would keep showing already-applied
	// matches as "open" and `chb pull` would be required to recover.
	if reconciled > 0 || alreadyDone > 0 {
		if count, err := writeOdooJournalLinesCache(creds, uid, journalID); err != nil {
			fmt.Printf("  %s⚠ journal cache refresh failed: %v%s\n", Fmt.Yellow, err, Fmt.Reset)
		} else {
			fmt.Printf("  %s↻ Refreshed local cache for journal #%d (%d line%s)%s\n",
				Fmt.Dim, journalID, count, plural(count), Fmt.Reset)
		}
		if patched, err := refreshTouchedInvoiceCache(creds, uid, touchedInvoiceMoveIDs); err != nil {
			fmt.Printf("  %s⚠ invoice cache refresh failed: %v%s\n", Fmt.Yellow, err, Fmt.Reset)
		} else if patched > 0 {
			noun := "entries"
			if patched == 1 {
				noun = "entry"
			}
			fmt.Printf("  %s↻ Patched %d invoice/bill %s in local cache%s\n",
				Fmt.Dim, patched, noun, Fmt.Reset)
		}
	}
	fmt.Println()
	return nil
}

// reconcileStaleCacheError reports whether err is one of Odoo's
// "you tried to reconcile something that's already reconciled / paid"
// errors. Those indicate the local match was real at pull time but
// Odoo settled the invoice in the meantime — not a real failure.
func reconcileStaleCacheError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "no open a/r or a/p line") ||
		strings.Contains(msg, "écritures comptables qui le sont déjà") ||
		strings.Contains(msg, "already reconciled") ||
		strings.Contains(msg, "déjà lettré")
}

func reconcileCreatedStatementLine(creds *OdooCredentials, uid int, lineID int, dryRun bool, stats *syncStats) {
	if lineID == 0 || stats == nil || dryRun {
		return
	}
	line, err := readStatementLineForReconcile(creds, uid, lineID)
	if err != nil {
		stats.ReconcileErrors++
		stats.ReconcileDetails = append(stats.ReconcileDetails, fmt.Sprintf("line #%d: %v", lineID, err))
		return
	}
	result := tryReconcileStatementLine(creds, uid, line, false)
	recordSyncReconcileResult(stats, line, result)
}

func reconcileCreatedStatementLines(creds *OdooCredentials, uid int, lineIDs []int, dryRun bool, stats *syncStats) {
	status := newStatusLine()
	if !quietOdooContext() && len(lineIDs) > 1 {
		status.Update("Reconciling statement lines 0/%d", len(lineIDs))
		defer status.Clear()
	}
	for i, lineID := range lineIDs {
		if !quietOdooContext() && len(lineIDs) > 1 {
			status.Update("Reconciling statement lines %d/%d", i+1, len(lineIDs))
		}
		reconcileCreatedStatementLine(creds, uid, lineID, dryRun, stats)
	}
}

func reconcileCreatedStatementLinesByImportID(creds *OdooCredentials, uid int, importIDs []string, dryRun bool, stats *syncStats) {
	if len(importIDs) == 0 || dryRun || stats == nil {
		return
	}
	lines, err := fetchStatementLinesByImportID(creds, uid, importIDs)
	if err != nil {
		stats.ReconcileErrors++
		stats.ReconcileDetails = append(stats.ReconcileDetails, fmt.Sprintf("fetch created lines: %v", err))
		return
	}
	for _, line := range lines {
		result := tryReconcileStatementLine(creds, uid, line, false)
		recordSyncReconcileResult(stats, line, result)
	}
}

func markCreatedStatementLinesInternal(creds *OdooCredentials, uid int, lineIDs []int, dryRun bool, stats *syncStats) {
	if len(lineIDs) == 0 || dryRun || stats == nil {
		return
	}
	for _, lineID := range lineIDs {
		line, err := readStatementLineForReconcile(creds, uid, lineID)
		if err != nil {
			stats.ReconcileErrors++
			stats.ReconcileDetails = append(stats.ReconcileDetails, fmt.Sprintf("line #%d: %v", lineID, err))
			continue
		}
		if err := markStatementLineInternalTransfer(creds, uid, line, false); err != nil {
			stats.ReconcileErrors++
			stats.ReconcileDetails = append(stats.ReconcileDetails, fmt.Sprintf("line #%d internal transfer: %v", line.ID, err))
			continue
		}
		stats.InternalTransfers++
	}
}

func recordSyncReconcileResult(stats *syncStats, line odooStatementLineForReconcile, result odooLineReconcileResult) {
	switch {
	case result.Err != nil:
		stats.ReconcileErrors++
		stats.ReconcileDetails = append(stats.ReconcileDetails, formatOdooReconcileDetail(line, result))
	case result.InternalTransfer:
		stats.InternalTransfers++
	case result.Reconciled:
		stats.LinesReconciled++
	case result.Ambiguous:
		stats.ReconcileAmbiguous++
		stats.ReconcileDetails = append(stats.ReconcileDetails, formatOdooReconcileDetail(line, result))
	case result.NoPartner:
		stats.ReconcileNoPartner++
	case result.NoMatch:
		stats.ReconcileNoMatch++
	}
}

func tryReconcileStatementLine(creds *OdooCredentials, uid int, line odooStatementLineForReconcile, dryRun bool) odooLineReconcileResult {
	refCandidates, err := findOpenMoveCandidatesByReferenceForStatementLine(creds, uid, line)
	if err != nil {
		return odooLineReconcileResult{Err: err, Message: "find invoice/bill by reference"}
	}
	return tryReconcileStatementLineWithReferenceCandidates(creds, uid, line, dryRun, refCandidates)
}

func tryReconcileStatementLineWithReferenceCandidates(creds *OdooCredentials, uid int, line odooStatementLineForReconcile, dryRun bool, refCandidates []odooMoveCandidate) odooLineReconcileResult {
	if line.IsReconciled {
		return odooLineReconcileResult{}
	}
	if looksLikeInternalTransferLine(line) {
		if dryRun {
			return odooLineReconcileResult{InternalTransfer: true, Message: "would mark as internal transfer"}
		}
		if err := markStatementLineInternalTransfer(creds, uid, line, false); err != nil {
			return odooLineReconcileResult{Err: err, Message: "mark internal transfer"}
		}
		return odooLineReconcileResult{InternalTransfer: true, Message: "marked as internal transfer"}
	}

	if len(refCandidates) > 1 {
		return odooLineReconcileResult{
			Ambiguous:      true,
			CandidateCount: len(refCandidates),
			Message:        fmt.Sprintf("%d matching open invoices/bills by reference", len(refCandidates)),
		}
	}
	if len(refCandidates) == 1 {
		candidate := refCandidates[0]
		if dryRun {
			return odooLineReconcileResult{
				Reconciled:        true,
				Message:           "would reconcile by reference",
				CandidateMoveName: candidateDisplayName(candidate),
			}
		}
		if candidate.PartnerID > 0 && line.PartnerID != candidate.PartnerID {
			_, _ = odooExec(creds.URL, creds.DB, uid, creds.Password,
				"account.bank.statement.line", "write",
				[]interface{}{[]interface{}{line.ID}, map[string]interface{}{"partner_id": candidate.PartnerID}}, nil)
		}
		if err := reconcileStatementLineWithMove(creds, uid, line, candidate); err != nil {
			return odooLineReconcileResult{Err: err, Message: fmt.Sprintf("reconcile by reference with %s", candidateDisplayName(candidate))}
		}
		return odooLineReconcileResult{
			Reconciled:        true,
			Message:           "reconciled by reference",
			CandidateMoveName: candidateDisplayName(candidate),
		}
	}

	partnerID, err := partnerIDForStatementLine(creds, uid, line, !dryRun)
	if err != nil {
		return odooLineReconcileResult{Err: err, Message: "lookup partner"}
	}
	if partnerID == 0 {
		return odooLineReconcileResult{NoPartner: true, Message: "no partner or partner bank account"}
	}

	candidates, err := findOpenMoveCandidatesForStatementLine(creds, uid, line, partnerID)
	if err != nil {
		return odooLineReconcileResult{Err: err, Message: "find matching invoice/bill"}
	}
	if len(candidates) == 0 {
		return odooLineReconcileResult{NoMatch: true, Message: "no matching open invoice/bill"}
	}
	if len(candidates) > 1 {
		return odooLineReconcileResult{
			Ambiguous:      true,
			CandidateCount: len(candidates),
			Message:        fmt.Sprintf("%d matching open invoices/bills", len(candidates)),
		}
	}
	candidate := candidates[0]
	if dryRun {
		return odooLineReconcileResult{
			Reconciled:        true,
			Message:           "would reconcile",
			CandidateMoveName: candidateDisplayName(candidate),
		}
	}
	if err := reconcileStatementLineWithMove(creds, uid, line, candidate); err != nil {
		return odooLineReconcileResult{Err: err, Message: fmt.Sprintf("reconcile with %s", candidateDisplayName(candidate))}
	}
	return odooLineReconcileResult{
		Reconciled:        true,
		Message:           "reconciled",
		CandidateMoveName: candidateDisplayName(candidate),
	}
}

// resetOverCategorizedToSuspense finds unreconciled lines whose
// counterpart move.line is already on an A/R or A/P account (likely set
// by a catch-all OdooMapping rule) and rewrites the counterpart back to
// the journal's suspense account. That makes the line eligible for the
// normal reconcile flow that follows.
//
// Reconciled counterparts are skipped — those are successfully-matched
// payments, not over-categorization, and resetting them would undo a
// completed reconcile.
//
// Returns the number of lines reset (0 on no-op or dry-run).
func resetOverCategorizedToSuspense(creds *OdooCredentials, uid int, journalID int, lines []odooStatementLineForReconcile, dryRun, verbose bool) (int, error) {
	if len(lines) == 0 {
		return 0, nil
	}
	moveIDs := make([]int, 0, len(lines))
	moveByID := map[int]int{} // move_id → bank-statement-line id, for the verbose log
	for _, ln := range lines {
		if ln.MoveID > 0 {
			moveIDs = append(moveIDs, ln.MoveID)
			moveByID[ln.MoveID] = ln.ID
		}
	}
	if len(moveIDs) == 0 {
		return 0, nil
	}

	// Query directly for unreconciled A/R or A/P counterparts. This
	// avoids the "first non-cash line wins" heuristic in
	// fetchCounterpartMoveLinesByMoveID, which on a partially-reconciled
	// move would surface the already-reconciled line.
	rows, err := odooSearchReadAllMaps(creds, uid, "account.move.line",
		[]interface{}{
			[]interface{}{"move_id", "in", intsToInterfaces(moveIDs)},
			[]interface{}{"account_type", "in", []interface{}{"asset_receivable", "liability_payable"}},
			[]interface{}{"reconciled", "=", false},
		},
		[]string{"id", "move_id", "account_type"},
		"id asc",
	)
	if err != nil {
		return 0, err
	}
	movesToReset := make([]int, 0)
	counterpartsToReset := make([]int, 0)
	seenMove := map[int]bool{}
	for _, row := range rows {
		moveID := odooFieldID(row["move_id"])
		if moveID == 0 || seenMove[moveID] {
			continue
		}
		seenMove[moveID] = true
		lineID := odooInt(row["id"])
		accountType := odooString(row["account_type"])
		movesToReset = append(movesToReset, moveID)
		counterpartsToReset = append(counterpartsToReset, lineID)
		if verbose {
			fmt.Printf("  %s↻ resetting line #%d (move #%d, counterpart on %s)%s\n",
				Fmt.Dim, moveByID[moveID], moveID, accountType, Fmt.Reset)
		}
	}
	if len(movesToReset) == 0 {
		return 0, nil
	}
	fmt.Printf("  %s↻ %s over-categorized to A/R or A/P — resetting to suspense before matching against invoices…%s\n",
		Fmt.Yellow, Pluralize(len(movesToReset), "line is", "lines are"), Fmt.Reset)
	if dryRun {
		return 0, nil
	}

	suspenseID, err := fetchJournalSuspenseAccount(creds, uid, journalID)
	if err != nil {
		return 0, err
	}
	if suspenseID == 0 {
		return 0, fmt.Errorf("journal #%d has no suspense account configured", journalID)
	}
	// Reuse the batched draft → write → repost helper — counterpart
	// already known per move, so we can skip the per-line read.
	if err := applyOdooMappingAccountBatch(creds, uid, movesToReset, counterpartsToReset, suspenseID, "suspense", nil); err != nil {
		return 0, fmt.Errorf("apply suspense: %v", err)
	}
	return len(movesToReset), nil
}

// fetchJournalSuspenseAccount reads the journal's suspense_account_id —
// the account Odoo uses as the counterpart for newly-created bank
// statement lines until they get reconciled. Used to revert
// over-categorized lines back to the natural pre-reconcile state.
func fetchJournalSuspenseAccount(creds *OdooCredentials, uid int, journalID int) (int, error) {
	rows, err := odooReadMapsByIDs(creds, uid, "account.journal", []int{journalID}, []string{"id", "suspense_account_id"})
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, fmt.Errorf("journal #%d not found", journalID)
	}
	return odooFieldID(rows[0]["suspense_account_id"]), nil
}

func fetchStatementLinesByImportID(creds *OdooCredentials, uid int, importIDs []string) ([]odooStatementLineForReconcile, error) {
	values := make([]interface{}, 0, len(importIDs))
	seen := map[string]bool{}
	for _, id := range importIDs {
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		values = append(values, id)
	}
	if len(values) == 0 {
		return nil, nil
	}
	rows, err := odooSearchReadAllMaps(creds, uid, "account.bank.statement.line",
		[]interface{}{[]interface{}{"unique_import_id", "in", values}},
		statementLineReconcileFields(),
		"date asc, id asc",
	)
	if err != nil {
		return nil, err
	}
	return parseStatementLineRows(rows), nil
}

func readStatementLineForReconcile(creds *OdooCredentials, uid int, lineID int) (odooStatementLineForReconcile, error) {
	rows, err := odooReadMapsByIDs(creds, uid, "account.bank.statement.line", []int{lineID}, statementLineReconcileFields())
	if err != nil {
		return odooStatementLineForReconcile{}, err
	}
	lines := parseStatementLineRows(rows)
	if len(lines) == 0 {
		return odooStatementLineForReconcile{}, fmt.Errorf("statement line #%d not found", lineID)
	}
	return lines[0], nil
}

func statementLineReconcileFields() []string {
	return []string{
		"id", "date", "amount", "payment_ref", "narration", "unique_import_id",
		"partner_id", "partner_bank_id", "statement_id", "move_id", "is_reconciled",
	}
}

func parseStatementLineRows(rows []map[string]interface{}) []odooStatementLineForReconcile {
	out := make([]odooStatementLineForReconcile, 0, len(rows))
	for _, row := range rows {
		out = append(out, odooStatementLineForReconcile{
			ID:             odooInt(row["id"]),
			Date:           odooString(row["date"]),
			Amount:         odooFloat(row["amount"]),
			PaymentRef:     odooString(row["payment_ref"]),
			Narration:      odooString(row["narration"]),
			UniqueImportID: odooString(row["unique_import_id"]),
			PartnerID:      odooFieldID(row["partner_id"]),
			PartnerName:    odooFieldName(row["partner_id"]),
			PartnerBankID:  odooFieldID(row["partner_bank_id"]),
			StatementID:    odooFieldID(row["statement_id"]),
			MoveID:         odooFieldID(row["move_id"]),
			IsReconciled:   odooBool(row["is_reconciled"]),
		})
	}
	return out
}

func partnerIDForStatementLine(creds *OdooCredentials, uid int, line odooStatementLineForReconcile, writeBack bool) (int, error) {
	if line.PartnerID > 0 {
		return line.PartnerID, nil
	}
	if line.PartnerBankID == 0 {
		return 0, nil
	}
	rows, err := odooReadMapsByIDs(creds, uid, "res.partner.bank", []int{line.PartnerBankID}, []string{"partner_id", "acc_number", "sanitized_acc_number"})
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, nil
	}
	partnerID := odooFieldID(rows[0]["partner_id"])
	if partnerID > 0 && writeBack {
		_, _ = odooExec(creds.URL, creds.DB, uid, creds.Password,
			"account.bank.statement.line", "write",
			[]interface{}{[]interface{}{line.ID}, map[string]interface{}{"partner_id": partnerID}}, nil)
	}
	return partnerID, nil
}

func resolveOdooPartnerBankForTransaction(creds *OdooCredentials, uid int, tx TransactionEntry) (int, int) {
	if isEVMAddress(tx.Counterparty) {
		bankID, partnerID, err := resolveOdooCryptoPartnerBank(creds, uid, tx)
		if err == nil || partnerID > 0 {
			return bankID, partnerID
		}
	}
	accountNumber := transactionBankAccountNumber(tx)
	if accountNumber == "" {
		return 0, 0
	}
	bankID, partnerID, err := findOdooPartnerBankByAccountNumber(creds, uid, accountNumber)
	if err != nil {
		return 0, 0
	}
	return bankID, partnerID
}

func resolveOdooCryptoPartnerBank(creds *OdooCredentials, uid int, tx TransactionEntry) (int, int, error) {
	address := normalizeEVMAddress(tx.Counterparty)
	chain := transactionChain(tx)
	accountNumber := cryptoBankAccountNumber(chain, address)
	bankID, partnerID, err := findOdooPartnerBankByAccountNumber(creds, uid, accountNumber)
	if err != nil {
		return 0, 0, err
	}
	if bankID > 0 && partnerID > 0 {
		return bankID, partnerID, nil
	}
	// Backward compatibility for any manually-created partner banks that
	// stored the bare address before CHB started using chain-prefixed keys.
	bankID, partnerID, err = findOdooPartnerBankByAccountNumber(creds, uid, address)
	if err != nil {
		return 0, 0, err
	}
	if bankID > 0 && partnerID > 0 {
		return bankID, partnerID, nil
	}

	name := cryptoCounterpartyName(tx, chain, address)
	partnerID, err = createOdooPartner(creds, uid, name)
	if err != nil || partnerID == 0 {
		return 0, 0, err
	}
	bankID, err = createOdooPartnerBank(creds, uid, partnerID, accountNumber)
	if err != nil {
		return 0, 0, err
	}
	return bankID, partnerID, nil
}

func ensureOdooStatementLinePartnerBank(creds *OdooCredentials, uid int, journalID int, importID string, tx TransactionEntry) (bool, error) {
	if importID == "" || !isEVMAddress(tx.Counterparty) {
		return false, nil
	}
	bankID, partnerID, err := resolveOdooCryptoPartnerBank(creds, uid, tx)
	if err != nil && partnerID == 0 {
		return false, err
	}
	if bankID == 0 && partnerID == 0 {
		return false, nil
	}
	rows, err := odooSearchReadAllMaps(creds, uid, "account.bank.statement.line",
		[]interface{}{
			[]interface{}{"journal_id", "=", journalID},
			[]interface{}{"unique_import_id", "=", importID},
		},
		[]string{"id", "partner_id", "partner_bank_id"},
		"id desc",
	)
	if err != nil {
		return false, err
	}
	if len(rows) == 0 {
		return false, nil
	}
	lineID := odooInt(rows[0]["id"])
	if lineID == 0 {
		return false, nil
	}
	update := map[string]interface{}{}
	if partnerID > 0 && odooFieldID(rows[0]["partner_id"]) != partnerID {
		update["partner_id"] = partnerID
	}
	if bankID > 0 && odooFieldID(rows[0]["partner_bank_id"]) != bankID {
		update["partner_bank_id"] = bankID
	}
	if len(update) == 0 {
		return false, nil
	}
	_, err = odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement.line", "write",
		[]interface{}{[]interface{}{lineID}, update}, nil)
	if err != nil {
		return false, err
	}
	return true, nil
}

func createOdooPartner(creds *OdooCredentials, uid int, name string) (int, error) {
	name = titleCaseName(name)
	values := map[string]interface{}{"name": name}
	for k, v := range odooPartnerDefaultLanguageValues(creds, uid) {
		values[k] = v
	}
	result, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"res.partner", "create",
		[]interface{}{[]interface{}{values}}, nil)
	if err != nil {
		return 0, err
	}
	ids := parseOdooCreatedIDs(result)
	if len(ids) == 0 {
		return 0, fmt.Errorf("Odoo did not return a partner id")
	}
	return ids[0], nil
}

func createOdooPartnerBank(creds *OdooCredentials, uid int, partnerID int, accountNumber string) (int, error) {
	// Pre-check: if this (partner, IBAN) row already exists, reuse it.
	// Odoo enforces a (acc_number, partner_id, company_id) unique
	// constraint, and the SAME IBAN can also already be linked to a
	// DIFFERENT partner in the same company — in that case we surface a
	// clear error instead of letting the unique-violation bubble up. The
	// merge step expects callers to have already tried IBAN→partner
	// lookup; this is the last line of defense.
	normalized := normalizeBankAccountNumber(accountNumber)
	if normalized != "" {
		rows, err := odooSearchReadAllMaps(creds, uid, "res.partner.bank",
			[]interface{}{[]interface{}{"sanitized_acc_number", "=", normalized}},
			[]string{"id", "partner_id"},
			"id asc",
		)
		if err == nil {
			for _, row := range rows {
				pid := odooFieldID(row["partner_id"])
				if pid == partnerID {
					return odooInt(row["id"]), nil
				}
			}
			if len(rows) > 0 {
				existingPID := odooFieldID(rows[0]["partner_id"])
				return 0, fmt.Errorf("IBAN %s already linked to partner #%d (caller asked for #%d); reuse that partner instead", accountNumber, existingPID, partnerID)
			}
		}
	}
	result, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"res.partner.bank", "create",
		[]interface{}{[]interface{}{map[string]interface{}{
			"partner_id": partnerID,
			"acc_number": accountNumber,
		}}}, nil)
	if err != nil {
		return 0, err
	}
	ids := parseOdooCreatedIDs(result)
	if len(ids) == 0 {
		return 0, fmt.Errorf("Odoo did not return a partner bank id")
	}
	return ids[0], nil
}

func cryptoCounterpartyName(tx TransactionEntry, chain, address string) string {
	if isZeroEVMAddress(address) {
		symbol := tx.Currency
		if symbol == "" {
			symbol = "Token"
		}
		return fmt.Sprintf("%s/%s Minter", chain, symbol)
	}
	if ens := resolveENSNameForAddress(address); ens != "" {
		return ens
	}
	return truncateAddr(address)
}

func transactionChain(tx TransactionEntry) string {
	if tx.Chain != nil && *tx.Chain != "" {
		return strings.ToLower(*tx.Chain)
	}
	if tx.Provider != "" {
		return strings.ToLower(tx.Provider)
	}
	return "ethereum"
}

func cryptoBankAccountNumber(chain, address string) string {
	return fmt.Sprintf("%s:%s", strings.ToLower(chain), normalizeEVMAddress(address))
}

func transactionBankAccountNumber(tx TransactionEntry) string {
	keys := []string{
		"iban", "IBAN", "bankAccount", "bank_account",
		"counterpartyIban", "counterparty_iban", "counterpartIban", "counterpart_iban",
	}
	for _, key := range keys {
		if value, ok := tx.Metadata[key].(string); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	if ibanLikePattern.MatchString(tx.Counterparty) {
		return strings.TrimSpace(tx.Counterparty)
	}
	return ""
}

func isEVMAddress(value string) bool {
	value = strings.TrimPrefix(strings.TrimPrefix(strings.TrimSpace(value), "0x"), "0X")
	if len(value) != 40 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func normalizeEVMAddress(value string) string {
	return "0x" + strings.ToLower(strings.TrimPrefix(strings.TrimPrefix(strings.TrimSpace(value), "0x"), "0X"))
}

func isZeroEVMAddress(value string) bool {
	return normalizeEVMAddress(value) == "0x0000000000000000000000000000000000000000"
}

func findOdooPartnerBankByAccountNumber(creds *OdooCredentials, uid int, accountNumber string) (int, int, error) {
	normalized := normalizeBankAccountNumber(accountNumber)
	if normalized == "" {
		return 0, 0, nil
	}
	// Try sanitized first, then a fuzzier ilike on the raw acc_number.
	// We accept any number of matches: if multiple banks exist for the
	// same IBAN (sometimes possible when partners get merged or archived
	// records are kept), we still want to reuse one rather than slip
	// through to "create" and trip the unique (partner, account)
	// constraint. Prefer the active one with the lowest id for
	// determinism. We do NOT include archived banks here — the create
	// path further down already handles that case by catching the
	// duplicate-create error.
	rows, err := odooSearchReadAllMaps(creds, uid, "res.partner.bank",
		[]interface{}{[]interface{}{"sanitized_acc_number", "=", normalized}},
		[]string{"id", "partner_id", "acc_number", "sanitized_acc_number"},
		"id asc",
	)
	if err != nil {
		return 0, 0, err
	}
	if len(rows) == 0 {
		rows, err = odooSearchReadAllMaps(creds, uid, "res.partner.bank",
			[]interface{}{[]interface{}{"acc_number", "ilike", accountNumber}},
			[]string{"id", "partner_id", "acc_number", "sanitized_acc_number"},
			"id asc",
		)
		if err != nil {
			return 0, 0, err
		}
	}
	if len(rows) == 0 {
		return 0, 0, nil
	}
	// Pick the first row that has a partner. Filtering by partner_id > 0
	// guards against orphaned bank rows (rare but they exist).
	for _, row := range rows {
		pid := odooFieldID(row["partner_id"])
		if pid > 0 {
			return odooInt(row["id"]), pid, nil
		}
	}
	return odooInt(rows[0]["id"]), odooFieldID(rows[0]["partner_id"]), nil
}

func normalizeBankAccountNumber(value string) string {
	value = strings.ToUpper(strings.TrimSpace(value))
	replacer := strings.NewReplacer(" ", "", "-", "", ".", "", ":", "")
	return replacer.Replace(value)
}

func resolveENSNameForAddress(address string) string {
	rpcURL := defaultRPCForChainID(1)
	if rpcURL == "" || !isEVMAddress(address) {
		return ""
	}
	node := ensNamehash(strings.TrimPrefix(normalizeEVMAddress(address), "0x") + ".addr.reverse")
	resolverData := "0x0178b8bf" + hex.EncodeToString(node[:])
	resolverResult, err := ethCallHex(rpcURL, "0x00000000000C2E074eC69A0dFb2997BA6C7d2e1e", resolverData)
	if err != nil {
		return ""
	}
	resolver := evmAddressFromCallResult(resolverResult)
	if resolver == "" || isZeroEVMAddress(resolver) {
		return ""
	}
	nameData := "0x691f3431" + hex.EncodeToString(node[:])
	nameResult, err := ethCallHex(rpcURL, resolver, nameData)
	if err != nil {
		return ""
	}
	return decodeABIString(nameResult)
}

func ensNamehash(name string) [32]byte {
	var node [32]byte
	labels := strings.Split(strings.ToLower(strings.TrimSuffix(name, ".")), ".")
	for i := len(labels) - 1; i >= 0; i-- {
		labelHash := keccak256([]byte(labels[i]))
		buf := make([]byte, 0, 64)
		buf = append(buf, node[:]...)
		buf = append(buf, labelHash...)
		copy(node[:], keccak256(buf))
	}
	return node
}

func ethCallHex(rpcURL, to, data string) (string, error) {
	payload := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "eth_call",
		"params": []interface{}{
			map[string]string{"to": to, "data": data},
			"latest",
		},
	}
	body, _ := json.Marshal(payload)
	resp, err := http.Post(rpcURL, "application/json", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var result struct {
		Result string `json:"result"`
		Error  *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}
	if result.Error != nil {
		return "", fmt.Errorf("%s", result.Error.Message)
	}
	if result.Result == "" || result.Result == "0x" {
		return "", nil
	}
	return result.Result, nil
}

func evmAddressFromCallResult(result string) string {
	result = strings.TrimPrefix(result, "0x")
	if len(result) < 40 {
		return ""
	}
	return normalizeEVMAddress(result[len(result)-40:])
}

func decodeABIString(result string) string {
	data, err := hex.DecodeString(strings.TrimPrefix(result, "0x"))
	if err != nil || len(data) < 64 {
		return ""
	}
	offset := new(big.Int).SetBytes(data[:32]).Int64()
	if offset < 0 || int(offset)+32 > len(data) {
		return ""
	}
	length := new(big.Int).SetBytes(data[offset : offset+32]).Int64()
	start := int(offset) + 32
	end := start + int(length)
	if length < 0 || end > len(data) {
		return ""
	}
	return string(data[start:end])
}

func findOpenMoveCandidatesForStatementLine(creds *OdooCredentials, uid int, line odooStatementLineForReconcile, partnerID int) ([]odooMoveCandidate, error) {
	return findOpenMoveCandidates(creds, uid, line, partnerID)
}

func findOpenMoveCandidatesByReferenceForStatementLine(creds *OdooCredentials, uid int, line odooStatementLineForReconcile) ([]odooMoveCandidate, error) {
	refs := extractOdooInvoiceReferencesFromStatementLine(line)
	if len(refs) == 0 {
		return nil, nil
	}
	absAmount := math.Abs(line.Amount)
	if absAmount < 0.005 {
		return nil, nil
	}
	moveTypes := []interface{}{"out_invoice"}
	if line.Amount < 0 {
		moveTypes = []interface{}{"in_invoice"}
	}
	minAmount := roundCents(absAmount - 0.01)
	maxAmount := roundCents(absAmount + 0.01)
	values := make([]interface{}, 0, len(refs))
	for _, ref := range refs {
		values = append(values, ref)
	}
	rows, err := odooSearchReadAllMaps(creds, uid, "account.move",
		[]interface{}{
			[]interface{}{"state", "=", "posted"},
			[]interface{}{"move_type", "in", moveTypes},
			[]interface{}{"payment_state", "not in", []interface{}{"paid", "in_payment", "reversed"}},
			[]interface{}{"amount_residual", ">=", minAmount},
			[]interface{}{"amount_residual", "<=", maxAmount},
			[]interface{}{"name", "in", values},
		},
		[]string{"id", "name", "invoice_date", "date", "move_type", "partner_id", "amount_residual"},
		"invoice_date desc, id desc",
	)
	if err != nil {
		return nil, err
	}
	candidates := parseOdooMoveCandidates(rows)
	filtered := candidates[:0]
	for _, candidate := range candidates {
		if odooReferenceCandidateMatchesLine(line, candidate) {
			filtered = append(filtered, candidate)
		}
	}
	return filtered, nil
}

func odooReferenceCandidateMatchesLine(line odooStatementLineForReconcile, candidate odooMoveCandidate) bool {
	if line.Amount >= 0 && candidate.MoveType != "out_invoice" {
		return false
	}
	if line.Amount < 0 && candidate.MoveType != "in_invoice" {
		return false
	}
	return math.Abs(math.Abs(candidate.AmountResidual)-math.Abs(line.Amount)) <= 0.01
}

func extractOdooInvoiceReferencesFromStatementLine(line odooStatementLineForReconcile) []string {
	text := line.PaymentRef + " " + line.Narration + " " + line.UniqueImportID
	matches := odooInvoiceReferencePattern.FindAllString(strings.ToUpper(text), -1)
	if len(matches) == 0 {
		return nil
	}
	seen := map[string]bool{}
	refs := make([]string, 0, len(matches))
	for _, ref := range matches {
		if seen[ref] {
			continue
		}
		seen[ref] = true
		refs = append(refs, ref)
	}
	return refs
}

func findOpenMoveCandidates(creds *OdooCredentials, uid int, line odooStatementLineForReconcile, partnerID int) ([]odooMoveCandidate, error) {
	lineDate, err := time.Parse("2006-01-02", line.Date)
	if err != nil {
		lineDate = time.Now()
	}
	startDate := lineDate.AddDate(0, -3, 0).Format("2006-01-02")
	endDate := lineDate.Format("2006-01-02")
	absAmount := math.Abs(line.Amount)
	if absAmount < 0.005 {
		return nil, nil
	}

	moveTypes := []interface{}{"out_invoice"}
	if line.Amount < 0 {
		moveTypes = []interface{}{"in_invoice"}
	}
	minAmount := roundCents(absAmount - 0.01)
	maxAmount := roundCents(absAmount + 0.01)
	domain := []interface{}{
		[]interface{}{"state", "=", "posted"},
		[]interface{}{"move_type", "in", moveTypes},
		[]interface{}{"payment_state", "not in", []interface{}{"paid", "in_payment", "reversed"}},
		[]interface{}{"amount_residual", ">=", minAmount},
		[]interface{}{"amount_residual", "<=", maxAmount},
		[]interface{}{"invoice_date", ">=", startDate},
		[]interface{}{"invoice_date", "<=", endDate},
	}
	if partnerID > 0 {
		domain = append(domain, []interface{}{"partner_id", "=", partnerID})
	}
	rows, err := odooSearchReadAllMaps(creds, uid, "account.move",
		domain,
		[]string{"id", "name", "invoice_date", "date", "move_type", "partner_id", "amount_residual"},
		"invoice_date desc, id desc",
	)
	if err != nil {
		return nil, err
	}
	return parseOdooMoveCandidates(rows), nil
}

func parseOdooMoveCandidates(rows []map[string]interface{}) []odooMoveCandidate {
	candidates := make([]odooMoveCandidate, 0, len(rows))
	for _, row := range rows {
		candidates = append(candidates, odooMoveCandidate{
			ID:             odooInt(row["id"]),
			Name:           odooString(row["name"]),
			InvoiceDate:    odooString(row["invoice_date"]),
			Date:           odooString(row["date"]),
			MoveType:       odooString(row["move_type"]),
			PartnerID:      odooFieldID(row["partner_id"]),
			PartnerName:    odooFieldName(row["partner_id"]),
			AmountResidual: odooFloat(row["amount_residual"]),
		})
	}
	return candidates
}

// reconcileStatementLineWithMove reconciles a bank statement line's
// counterpart with the invoice/bill's receivable/payable line.
//
// Odoo's bank suspense account is intentionally NOT marked reconcile=true
// (it's a holding bucket, not a journal-itemizable account), so we can't
// just call account.move.line.reconcile on the suspense line. The
// canonical flow is: draft the bank move → rewrite the suspense
// counterpart's account_id to the invoice's A/R (or A/P) account →
// repost the bank move → reconcile the now-on-A/R counterpart line with
// the invoice's A/R line.
//
// This matches what `markStatementLineInternalTransfer` does for
// transfers (it rewrites the counterpart to the internal-transfer
// account instead of an A/R one) and what Odoo's bank reconciliation
// widget does under the hood.
func reconcileStatementLineWithMove(creds *OdooCredentials, uid int, line odooStatementLineForReconcile, move odooMoveCandidate) error {
	if line.MoveID == 0 {
		return fmt.Errorf("statement line has no move")
	}

	// Find the invoice/bill's open A/R or A/P line. Filtering by
	// account_type (instead of the "reconcile=true on account" flag)
	// avoids false positives from revenue / VAT lines that happen to be
	// flagged reconcilable on this Odoo instance.
	invoiceLineID, arAccountID, err := findInvoiceReceivablePayableLine(creds, uid, move.ID)
	if err != nil {
		// Fallback A — "in_payment" invoices: a payment record was
		// already registered against the invoice's A/R line (so the
		// A/R is reconciled and there's nothing open to match), but
		// the payment's outstanding-receipts line is still waiting
		// for the bank statement line — reconcile against that.
		if oLineID, oAccountID, found, ferr := findOutstandingPaymentLineForInvoice(creds, uid, move.ID); ferr == nil && found {
			invoiceLineID = oLineID
			arAccountID = oAccountID
		} else if unrecLineID, unrecAccountID, unrecCount, uerr := unreconcileInvoiceAndGetOpenLine(creds, uid, move.ID); uerr == nil && unrecLineID > 0 {
			// Fallback B — "paid" invoice: the operator explicitly picked
			// it from the interactive prompt's top-5 list, signalling
			// that a previous reconciliation was wrong. Unreconcile the
			// existing match and reuse the now-reopened A/R line.
			fmt.Printf("  %s↻ unreconciled %d existing match%s on invoice/bill #%d before re-attaching%s\n",
				Fmt.Yellow, unrecCount, plural(unrecCount), move.ID, Fmt.Reset)
			invoiceLineID = unrecLineID
			arAccountID = unrecAccountID
		} else {
			return fmt.Errorf("invoice/bill #%d: %v", move.ID, err)
		}
	}

	// Find the bank move's non-bank counterpart (currently on suspense).
	counterpartID, err := findStatementCounterpartMoveLine(creds, uid, line)
	if err != nil {
		return fmt.Errorf("find counterpart: %v", err)
	}
	if counterpartID == 0 {
		return fmt.Errorf("could not identify counterpart move line on move #%d", line.MoveID)
	}

	// Defensive: verify the counterpart we're about to rewrite is NOT
	// the bank/cash side of the move. Rewriting that line removes the
	// only line on journal.default_account_id and trips Odoo's "exactly
	// one bank/cash entry" constraint. findStatementCounterpartMoveLine
	// already excludes those by account_type, so this catches the
	// "fetched a stale or unexpected line" case before we attempt the
	// write — the dry-run preview would have looked clean but the live
	// apply would have failed with an opaque French error.
	if err := assertNotBankCashLine(creds, uid, counterpartID); err != nil {
		return fmt.Errorf("counterpart line #%d: %v", counterpartID, err)
	}

	// Draft → rewrite counterpart account → repost. Same shape as
	// applyOdooMappingAccount / markStatementLineInternalTransfer.
	// withOdooMoveTemporarilyDraft handles the "already in draft" case
	// (Odoo otherwise rejects button_draft) and re-posts only when the
	// move was posted to begin with.
	//
	// The write passes odooStatementLineMetadataWriteContext() as kwargs
	// — same bypass the metadata stage uses — so Odoo's mid-write
	// _check_journal_consistency doesn't trip while the move is in a
	// transient state. The trip was observed during batch reconciles
	// where an earlier line's draft+write+repost cycle left the move's
	// related records (partial.reconcile, statement_line synchronization)
	// in a state that Odoo's recompute interprets as "zero lines on the
	// bank account" mid-write. The reconcile call below still runs the
	// full validation, so skipping the intermediate check is safe.
	if err := withOdooMoveTemporarilyDraft(creds, uid, line.MoveID, func() error {
		_, werr := odooExec(creds.URL, creds.DB, uid, creds.Password,
			"account.move.line", "write",
			[]interface{}{[]interface{}{counterpartID}, map[string]interface{}{"account_id": arAccountID}},
			odooStatementLineMetadataWriteContext())
		if werr != nil {
			return fmt.Errorf("rewrite counterpart line #%d: %v", counterpartID, werr)
		}
		return nil
	}); err != nil {
		return err
	}

	// Now counterpart and invoice line are on the same A/R account.
	if _, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.move.line", "reconcile",
		[]interface{}{[]interface{}{counterpartID, invoiceLineID}}, nil); err != nil {
		return fmt.Errorf("reconcile lines: %v", err)
	}

	// Attribute the bank line to the invoice's partner + register the
	// counterparty's IBAN on that partner. Future payments from the same
	// IBAN will then auto-attribute and Odoo's reconcile widget proposes
	// the right invoices without manual searching. Best-effort: errors
	// downgrade to warnings since the reconcile itself already succeeded.
	if move.PartnerID > 0 {
		if err := attributeBankLineToPartnerAfterReconcile(creds, uid, line.ID, move.PartnerID); err != nil {
			fmt.Printf("  %s⚠ post-reconcile partner attribution on line #%d failed: %v%s\n",
				Fmt.Dim, line.ID, err, Fmt.Reset)
		}
	}
	return nil
}

// attributeBankLineToPartnerAfterReconcile pins the invoice's partner on
// the bank statement line (when empty) and ensures a res.partner.bank
// record links the counterparty's IBAN to that partner. The next time
// money flows from the same IBAN, Odoo's bank-reconciliation widget
// suggests this partner — and the right invoices — without any operator
// hint.
//
// Non-destructive: a partner_id or partner_bank_id already set on the
// statement line is preserved (the operator may have set it deliberately).
// The res.partner.bank record itself is always ensured (search-then-
// create), so even pre-attributed lines contribute to future auto-match.
func attributeBankLineToPartnerAfterReconcile(creds *OdooCredentials, uid int, statementLineID, partnerID int) error {
	if statementLineID == 0 || partnerID <= 0 {
		return nil
	}
	rows, err := odooReadMapsByIDs(creds, uid, "account.bank.statement.line",
		[]int{statementLineID},
		[]string{"partner_id", "partner_bank_id", "account_number"})
	if err != nil {
		return fmt.Errorf("read statement line: %v", err)
	}
	if len(rows) == 0 {
		return nil
	}
	currentPartnerID := odooFieldID(rows[0]["partner_id"])
	currentBankID := odooFieldID(rows[0]["partner_bank_id"])
	iban := normalizeBankAccountNumber(odooString(rows[0]["account_number"]))

	// Fall back to the linked partner_bank's IBAN when the raw
	// account_number column is empty (some imports populate one but not
	// the other).
	if iban == "" && currentBankID > 0 {
		bankRows, _ := odooReadMapsByIDs(creds, uid, "res.partner.bank",
			[]int{currentBankID},
			[]string{"acc_number", "sanitized_acc_number"})
		if len(bankRows) > 0 {
			iban = normalizeBankAccountNumber(odooString(bankRows[0]["sanitized_acc_number"]))
			if iban == "" {
				iban = normalizeBankAccountNumber(odooString(bankRows[0]["acc_number"]))
			}
		}
	}

	// Ensure (IBAN, invoice partner) record. Skipped silently when no
	// IBAN is available (Stripe / blockchain journals).
	var newBankID int
	if iban != "" {
		newBankID, err = ensurePartnerBankAccount(creds, uid, iban, partnerID)
		if err != nil {
			return fmt.Errorf("ensure partner bank account: %v", err)
		}
	}

	updates := map[string]interface{}{}
	if currentPartnerID == 0 {
		updates["partner_id"] = partnerID
	}
	if currentBankID == 0 && newBankID > 0 {
		updates["partner_bank_id"] = newBankID
	}
	if len(updates) == 0 {
		return nil
	}
	// Pass the bypass context (same as v3.4.11's reconcile write):
	// the statement line's move was reposted by the reconcile dance
	// just above, and writing partner_id / partner_bank_id triggers
	// Odoo's account.move synchronization which tries to unlink+
	// recreate the underlying move.lines. On a posted move that
	// fails with "Vous ne pouvez pas supprimer une écriture
	// comptable validée. Veuillez d'abord la mettre en mode
	// brouillon." Skipping the sync is safe — partner_id is metadata
	// on the statement line, not part of the accounting balance.
	if _, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement.line", "write",
		[]interface{}{[]interface{}{statementLineID}, updates},
		odooStatementLineMetadataWriteContext()); err != nil {
		return fmt.Errorf("write statement line: %v", err)
	}
	return nil
}

// ensurePartnerBankAccount finds or creates a res.partner.bank record
// linking the given IBAN to the given partner. Returns the record id.
//
// Searches sanitized_acc_number first (Odoo's normalized form), then
// the raw acc_number with ilike to catch records that imported the IBAN
// with formatting whitespace. Falls through to create when nothing
// matches, populating just acc_number — Odoo computes sanitized form on
// save.
func ensurePartnerBankAccount(creds *OdooCredentials, uid int, iban string, partnerID int) (int, error) {
	if iban == "" || partnerID <= 0 {
		return 0, nil
	}
	rows, err := odooSearchReadAllMaps(creds, uid, "res.partner.bank",
		[]interface{}{
			[]interface{}{"sanitized_acc_number", "=", iban},
			[]interface{}{"partner_id", "=", partnerID},
		},
		[]string{"id"}, "id asc")
	if err != nil {
		return 0, err
	}
	if len(rows) > 0 {
		return odooInt(rows[0]["id"]), nil
	}
	rows, err = odooSearchReadAllMaps(creds, uid, "res.partner.bank",
		[]interface{}{
			[]interface{}{"acc_number", "ilike", iban},
			[]interface{}{"partner_id", "=", partnerID},
		},
		[]string{"id"}, "id asc")
	if err != nil {
		return 0, err
	}
	if len(rows) > 0 {
		return odooInt(rows[0]["id"]), nil
	}
	result, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"res.partner.bank", "create",
		[]interface{}{map[string]interface{}{
			"acc_number": iban,
			"partner_id": partnerID,
		}}, nil)
	if err != nil {
		return 0, err
	}
	ids := parseOdooCreatedIDs(result)
	if len(ids) > 0 {
		return ids[0], nil
	}
	return 0, fmt.Errorf("res.partner.bank.create returned no id")
}

// findInvoiceReceivablePayableLine returns the (lineID, accountID) of an
// invoice/bill's single open A/R or A/P line — the one we need to
// reconcile the bank counterpart against. Filtering by account_type
// (rather than the per-account reconcile=true flag) keeps revenue / VAT
// lines from masquerading as A/R candidates on Odoo instances that
// flag many accounts reconcilable.
//
// Most invoices have exactly one A/R/A/P line. When multiple exist
// (partial reconciles, manual splits), we pick the one with the largest
// residual — almost always the original main A/R line.
func findInvoiceReceivablePayableLine(creds *OdooCredentials, uid int, moveID int) (int, int, error) {
	rows, err := odooSearchReadAllMaps(creds, uid, "account.move.line",
		[]interface{}{
			[]interface{}{"move_id", "=", moveID},
			[]interface{}{"account_type", "in", []interface{}{"asset_receivable", "liability_payable"}},
			[]interface{}{"reconciled", "=", false},
		},
		[]string{"id", "account_id", "amount_residual"},
		"id asc",
	)
	if err != nil {
		return 0, 0, err
	}
	if len(rows) == 0 {
		return 0, 0, fmt.Errorf("no open A/R or A/P line")
	}
	bestIdx := 0
	if len(rows) > 1 {
		bestResidual := math.Abs(odooFloat(rows[0]["amount_residual"]))
		for i := 1; i < len(rows); i++ {
			r := math.Abs(odooFloat(rows[i]["amount_residual"]))
			if r > bestResidual {
				bestResidual = r
				bestIdx = i
			}
		}
	}
	lineID := odooInt(rows[bestIdx]["id"])
	accountID := odooFieldID(rows[bestIdx]["account_id"])
	if accountID == 0 {
		return 0, 0, fmt.Errorf("A/R line #%d has no account", lineID)
	}
	return lineID, accountID, nil
}

// findOutstandingPaymentLineForInvoice locates the still-open
// "outstanding receipts/payments" line of the payment record that's
// reconciled with an in_payment invoice.
//
// Odoo's in_payment state: customer paid, a payment record exists, the
// payment's A/R credit is reconciled with the invoice's A/R debit — so
// the invoice's A/R appears closed. What remains open is the payment's
// outstanding-side line, waiting for the bank statement line to come in
// and finalize the cycle. That line is what we need to reconcile
// against, NOT the invoice's A/R (which is already taken).
//
// Reached via the partial.reconcile graph rather than account.payment's
// One2many fields, since the partial-reconcile traversal works on any
// Odoo version and doesn't depend on field-name conventions.
// unreconcileInvoiceAndGetOpenLine undoes every existing
// reconciliation on the invoice/bill's A/R or A/P line(s) and returns
// the (now-open) line's id + account. Used when the operator picks a
// "paid" candidate from the interactive prompt: the previous
// reconciliation was wrong, we cut it loose, and the next phase of
// reconcileStatementLineWithMove attaches the bank counterpart to the
// freshly-reopened line.
//
// Returns (lineID, accountID, partialsRemoved, error). lineID = 0 when
// no A/R line exists at all on the move (caller treats as "not
// recoverable").
func unreconcileInvoiceAndGetOpenLine(creds *OdooCredentials, uid int, invoiceMoveID int) (int, int, int, error) {
	rows, err := odooSearchReadAllMaps(creds, uid, "account.move.line",
		[]interface{}{
			[]interface{}{"move_id", "=", invoiceMoveID},
			[]interface{}{"account_type", "in", []interface{}{"asset_receivable", "liability_payable"}},
		},
		[]string{"id", "account_id", "amount_residual", "matched_debit_ids", "matched_credit_ids", "reconciled"},
		"id asc",
	)
	if err != nil {
		return 0, 0, 0, err
	}
	if len(rows) == 0 {
		return 0, 0, 0, nil
	}

	// Collect partial.reconcile ids on those A/R lines. We'll remove
	// them so the lines become open again.
	var lineIDs []int
	partialSet := map[int]bool{}
	for _, r := range rows {
		if id := odooInt(r["id"]); id > 0 {
			lineIDs = append(lineIDs, id)
		}
		for _, key := range []string{"matched_debit_ids", "matched_credit_ids"} {
			if arr, ok := r[key].([]interface{}); ok {
				for _, v := range arr {
					if id := odooInt(v); id > 0 {
						partialSet[id] = true
					}
				}
			}
		}
	}
	partials := make([]int, 0, len(partialSet))
	for id := range partialSet {
		partials = append(partials, id)
	}
	if len(partials) > 0 {
		// Call remove_move_reconcile on each A/R line — Odoo's official
		// method that drops the partial.reconciles AND repostings, so the
		// invoice transitions back to 'not_paid'. unlink on the partials
		// is also valid but doesn't trigger Odoo's reset hooks.
		if _, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
			"account.move.line", "remove_move_reconcile",
			[]interface{}{lineIDs}, nil); err != nil {
			return 0, 0, 0, fmt.Errorf("unreconcile A/R line(s) %v: %v", lineIDs, err)
		}
	}

	// Re-read the A/R line we'll attach to. After remove_move_reconcile
	// it should report reconciled=false; we pick the line with the
	// largest residual (typically the original main A/R line).
	rows, err = odooSearchReadAllMaps(creds, uid, "account.move.line",
		[]interface{}{
			[]interface{}{"move_id", "=", invoiceMoveID},
			[]interface{}{"account_type", "in", []interface{}{"asset_receivable", "liability_payable"}},
			[]interface{}{"reconciled", "=", false},
		},
		[]string{"id", "account_id", "amount_residual"},
		"id asc",
	)
	if err != nil {
		return 0, 0, len(partials), err
	}
	if len(rows) == 0 {
		return 0, 0, len(partials), fmt.Errorf("no A/R line open even after unreconcile on move #%d", invoiceMoveID)
	}
	bestIdx := 0
	if len(rows) > 1 {
		bestRes := math.Abs(odooFloat(rows[0]["amount_residual"]))
		for i := 1; i < len(rows); i++ {
			r := math.Abs(odooFloat(rows[i]["amount_residual"]))
			if r > bestRes {
				bestRes = r
				bestIdx = i
			}
		}
	}
	return odooInt(rows[bestIdx]["id"]), odooFieldID(rows[bestIdx]["account_id"]), len(partials), nil
}

func findOutstandingPaymentLineForInvoice(creds *OdooCredentials, uid int, invoiceMoveID int) (int, int, bool, error) {
	// 1. Invoice's A/R or A/P lines (any state — they're reconciled=true
	//    for in_payment invoices).
	arRows, err := odooSearchReadAllMaps(creds, uid, "account.move.line",
		[]interface{}{
			[]interface{}{"move_id", "=", invoiceMoveID},
			[]interface{}{"account_type", "in", []interface{}{"asset_receivable", "liability_payable"}},
		},
		[]string{"id", "matched_debit_ids", "matched_credit_ids"},
		"id asc",
	)
	if err != nil {
		return 0, 0, false, err
	}
	if len(arRows) == 0 {
		return 0, 0, false, nil
	}

	// 2. Collect partial.reconcile ids attached to those A/R lines, plus
	//    the set of invoice line ids (so we can identify the "other side"
	//    of each partial reconcile — i.e. the payment's A/R line).
	invLineIDs := map[int]bool{}
	var partialIDs []int
	for _, r := range arRows {
		invLineIDs[odooInt(r["id"])] = true
		for _, raw := range []interface{}{r["matched_debit_ids"], r["matched_credit_ids"]} {
			if arr, ok := raw.([]interface{}); ok {
				for _, v := range arr {
					if id := odooInt(v); id > 0 {
						partialIDs = append(partialIDs, id)
					}
				}
			}
		}
	}
	if len(partialIDs) == 0 {
		return 0, 0, false, nil
	}

	// 3. Read the partial.reconcile records to find the payment's A/R
	//    line on each — the line that isn't ours.
	partialRows, err := odooReadMapsByIDs(creds, uid, "account.partial.reconcile",
		uniquePositiveInts(partialIDs),
		[]string{"id", "credit_move_id", "debit_move_id"})
	if err != nil {
		return 0, 0, false, err
	}
	paymentARLineIDs := map[int]bool{}
	for _, pr := range partialRows {
		for _, key := range []string{"credit_move_id", "debit_move_id"} {
			if id := odooFieldID(pr[key]); id > 0 && !invLineIDs[id] {
				paymentARLineIDs[id] = true
			}
		}
	}
	if len(paymentARLineIDs) == 0 {
		return 0, 0, false, nil
	}

	// 4. Find the move each payment-A/R line belongs to — that's the
	//    payment's account.move.
	pLineIDList := make([]int, 0, len(paymentARLineIDs))
	for id := range paymentARLineIDs {
		pLineIDList = append(pLineIDList, id)
	}
	pLineRows, err := odooReadMapsByIDs(creds, uid, "account.move.line", pLineIDList,
		[]string{"id", "move_id"})
	if err != nil {
		return 0, 0, false, err
	}
	paymentMoveIDs := map[int]bool{}
	for _, r := range pLineRows {
		if mid := odooFieldID(r["move_id"]); mid > 0 {
			paymentMoveIDs[mid] = true
		}
	}
	if len(paymentMoveIDs) == 0 {
		return 0, 0, false, nil
	}

	// 4b. Filter out bank statement moves. A genuine payment record's
	// move has statement_line_id=false; a bank statement move has
	// statement_line_id != false. Without this filter, an invoice
	// that was paid DIRECTLY by a bank statement line (not via an
	// intermediate account.payment) would have its bank-move
	// counterpart misclassified as the "outstanding payment line",
	// and a downstream rewrite would land on the wrong account —
	// observed during journal merges where j30 was the bank move
	// reconciled directly to the invoice's A/R.
	pmIDFilter := make([]interface{}, 0, len(paymentMoveIDs))
	for id := range paymentMoveIDs {
		pmIDFilter = append(pmIDFilter, id)
	}
	bankMoveRows, err := odooSearchReadAllMaps(creds, uid, "account.move",
		[]interface{}{
			[]interface{}{"id", "in", pmIDFilter},
			[]interface{}{"statement_line_id", "!=", false},
		},
		[]string{"id"}, "")
	if err == nil {
		for _, r := range bankMoveRows {
			delete(paymentMoveIDs, odooInt(r["id"]))
		}
	}
	if len(paymentMoveIDs) == 0 {
		return 0, 0, false, nil
	}

	// 5. The payment move has 2 lines: the A/R side (reconciled=true,
	//    matched with the invoice) and the outstanding side
	//    (reconciled=false, waiting for the bank). Return the latter.
	pmIDList := make([]interface{}, 0, len(paymentMoveIDs))
	for id := range paymentMoveIDs {
		pmIDList = append(pmIDList, id)
	}
	outRows, err := odooSearchReadAllMaps(creds, uid, "account.move.line",
		[]interface{}{
			[]interface{}{"move_id", "in", pmIDList},
			[]interface{}{"reconciled", "=", false},
		},
		[]string{"id", "account_id"}, "id asc")
	if err != nil {
		return 0, 0, false, err
	}
	if len(outRows) == 0 {
		return 0, 0, false, nil
	}
	lineID := odooInt(outRows[0]["id"])
	accountID := odooFieldID(outRows[0]["account_id"])
	if lineID == 0 || accountID == 0 {
		return 0, 0, false, nil
	}
	return lineID, accountID, true, nil
}

func openReconcilableMoveLines(creds *OdooCredentials, uid int, moveID int, absAmount float64) ([]int, error) {
	rows, err := odooSearchReadAllMaps(creds, uid, "account.move.line",
		[]interface{}{
			[]interface{}{"move_id", "=", moveID},
			[]interface{}{"reconciled", "=", false},
			[]interface{}{"account_id.reconcile", "=", true},
		},
		[]string{"id", "amount_residual", "amount_residual_currency", "balance", "debit", "credit"},
		"id asc",
	)
	if err != nil {
		return nil, err
	}
	var all []int
	var exact []int
	for _, row := range rows {
		id := odooInt(row["id"])
		if id == 0 {
			continue
		}
		all = append(all, id)
		residual := math.Abs(odooFloat(row["amount_residual"]))
		if residual < 0.005 {
			residual = math.Abs(odooFloat(row["amount_residual_currency"]))
		}
		if residual < 0.005 {
			residual = math.Abs(odooFloat(row["balance"]))
		}
		if math.Abs(residual-absAmount) <= 0.01 {
			exact = append(exact, id)
		}
	}
	if len(exact) > 0 {
		return exact, nil
	}
	return all, nil
}

func markStatementLineInternalTransfer(creds *OdooCredentials, uid int, line odooStatementLineForReconcile, dryRun bool) error {
	accountID, err := findInternalTransferAccountID(creds, uid)
	if err != nil {
		return err
	}
	if accountID == 0 {
		return fmt.Errorf("Odoo account %s not found", odooInternalTransferAccountCode)
	}
	if line.MoveID == 0 {
		return fmt.Errorf("statement line has no move")
	}
	if dryRun {
		return nil
	}
	counterpartID, err := findStatementCounterpartMoveLine(creds, uid, line)
	if err != nil {
		return err
	}
	if counterpartID == 0 {
		return fmt.Errorf("could not identify counterpart move line")
	}
	return withOdooMoveTemporarilyDraft(creds, uid, line.MoveID, func() error {
		_, werr := odooExec(creds.URL, creds.DB, uid, creds.Password,
			"account.move.line", "write",
			[]interface{}{[]interface{}{counterpartID}, map[string]interface{}{"account_id": accountID}}, nil)
		return werr
	})
}

func findInternalTransferAccountID(creds *OdooCredentials, uid int) (int, error) {
	rows, err := odooSearchReadAllMaps(creds, uid, "account.account",
		[]interface{}{[]interface{}{"code", "=", odooInternalTransferAccountCode}},
		[]string{"id", "code", "name"},
		"id asc",
	)
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, nil
	}
	return odooInt(rows[0]["id"]), nil
}

func findStatementCounterpartMoveLine(creds *OdooCredentials, uid int, line odooStatementLineForReconcile) (int, error) {
	// Resolve the journal's default_account_id up front. Odoo's
	// _check_journal_consistency rule requires exactly one move.line on
	// this specific account — so we must NEVER return a line that's on
	// it (rewriting its account_id would leave the move with zero
	// matching lines and trip Odoo's "exactly one entry involving the
	// bank or cash account" constraint). Type-based filtering
	// (asset_cash / liability_credit_card) is insufficient because
	// custom charts of accounts can use other types for the journal's
	// default account.
	defaultAccountID, err := fetchJournalDefaultAccount(creds, uid, line.MoveID)
	if err != nil {
		return 0, fmt.Errorf("resolve journal default account: %v", err)
	}

	rows, err := odooSearchReadAllMaps(creds, uid, "account.move.line",
		[]interface{}{[]interface{}{"move_id", "=", line.MoveID}},
		[]string{"id", "balance", "debit", "credit", "reconciled", "account_id"},
		"id asc",
	)
	if err != nil {
		return 0, err
	}

	// Two passes: first prefer the still-unreconciled counterpart (the
	// typical "fresh bank line" case), then accept any if no unreconciled
	// candidate exists. The unreconciled pass handles partially-reconciled
	// moves where Odoo has already split the counterpart into two lines
	// (one matched against an invoice, one still on suspense).
	var unreconciled, all []int
	for _, row := range rows {
		id := odooInt(row["id"])
		accID := odooFieldID(row["account_id"])
		// Skip the bank line itself — the one on
		// journal.default_account_id — so a rewrite never violates the
		// one-bank-line-per-move invariant.
		if defaultAccountID > 0 && accID == defaultAccountID {
			continue
		}
		balance := odooFloat(row["balance"])
		debit := odooFloat(row["debit"])
		credit := odooFloat(row["credit"])
		matchesSign := (line.Amount > 0 && (balance < -0.005 || credit > 0.005)) ||
			(line.Amount < 0 && (balance > 0.005 || debit > 0.005))
		if !matchesSign {
			continue
		}
		all = append(all, id)
		if !odooBool(row["reconciled"]) {
			unreconciled = append(unreconciled, id)
		}
	}
	if len(unreconciled) == 1 {
		return unreconciled[0], nil
	}
	if len(all) == 1 {
		return all[0], nil
	}
	return 0, nil
}

// fetchJournalDefaultAccount returns the journal.default_account_id of
// the journal that owns the given move. Used to identify the bank line
// of a bank-statement move so reconcile code never picks it as the
// counterpart to rewrite. Returns 0 without error when the lookup
// fails (e.g. move not found, journal has no default account).
func fetchJournalDefaultAccount(creds *OdooCredentials, uid, moveID int) (int, error) {
	if moveID <= 0 {
		return 0, nil
	}
	moveRows, err := odooSearchReadAllMaps(creds, uid, "account.move",
		[]interface{}{[]interface{}{"id", "=", moveID}},
		[]string{"id", "journal_id"}, "")
	if err != nil {
		return 0, err
	}
	if len(moveRows) == 0 {
		return 0, nil
	}
	journalID := odooFieldID(moveRows[0]["journal_id"])
	if journalID == 0 {
		return 0, nil
	}
	jrnRows, err := odooSearchReadAllMaps(creds, uid, "account.journal",
		[]interface{}{[]interface{}{"id", "=", journalID}},
		[]string{"id", "default_account_id"}, "")
	if err != nil {
		return 0, err
	}
	if len(jrnRows) == 0 {
		return 0, nil
	}
	return odooFieldID(jrnRows[0]["default_account_id"]), nil
}

func looksLikeInternalTransferLine(line odooStatementLineForReconcile) bool {
	text := strings.ToLower(line.PaymentRef + " " + line.UniqueImportID)
	return strings.Contains(text, "stripe payout") ||
		strings.Contains(text, "auto payout") ||
		strings.Contains(text, "manual payout") ||
		strings.Contains(text, "internal transfer")
}

func formatOdooReconcileDetail(line odooStatementLineForReconcile, result odooLineReconcileResult) string {
	ref := line.PaymentRef
	if ref == "" {
		ref = line.UniqueImportID
	}
	if ref == "" {
		ref = fmt.Sprintf("line #%d", line.ID)
	}
	msg := result.Message
	if result.Err != nil {
		msg = fmt.Sprintf("%s: %v", msg, result.Err)
	}
	if result.CandidateMoveName != "" {
		msg = fmt.Sprintf("%s %s", msg, result.CandidateMoveName)
	}
	return fmt.Sprintf("%s %s %.2f: %s", line.Date, ref, line.Amount, msg)
}

func odooStatementLineCounterparty(line odooStatementLineForReconcile) string {
	if line.PartnerName != "" {
		return line.PartnerName
	}
	if line.PaymentRef != "" {
		return line.PaymentRef
	}
	if line.UniqueImportID != "" {
		return line.UniqueImportID
	}
	return fmt.Sprintf("line #%d", line.ID)
}

func candidateDisplayName(candidate odooMoveCandidate) string {
	if candidate.Name != "" {
		return candidate.Name
	}
	return fmt.Sprintf("#%d", candidate.ID)
}

func parseOdooCreatedIDs(raw json.RawMessage) []int {
	var ids []int
	if err := json.Unmarshal(raw, &ids); err == nil {
		return ids
	}
	var id int
	if err := json.Unmarshal(raw, &id); err == nil && id > 0 {
		return []int{id}
	}
	var f float64
	if err := json.Unmarshal(raw, &f); err == nil && f > 0 {
		return []int{int(f)}
	}
	return nil
}
