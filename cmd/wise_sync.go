package cmd

import (
	"fmt"
	"math"
	"sort"
	"strings"

	wisesource "github.com/CommonsHub/chb/providers/wise"
)

// WiseSync imports Wise statement-CSV transactions into their linked Odoo bank
// journals. Wise is a closed, CSV-only account (one balance/"jar" per Odoo
// journal); this is the Wise analogue of the kbcbrussels sync.
//
// For each Wise account it compares the CSV's transactions against the journal's
// existing lines and reports what's missing. Crucially it only judges
// completeness WITHIN the CSV's own date range — Odoo may legitimately hold
// older/newer lines the export doesn't cover ("that's ok"), but a CSV
// transaction with no Odoo counterpart inside the covered range is a real gap,
// and an in-range Odoo line that matches no CSV transaction is flagged as
// suspicious.
//
//	chb wise sync                 Preview all Wise accounts (dry-run)
//	chb wise sync wise-brusselspay   Preview one account
//	chb wise sync --apply         Create the missing lines in Odoo
func WiseSync(args []string) error {
	if HasFlag(args, "--help", "-h", "help") {
		printWiseSyncHelp()
		return nil
	}
	apply := HasFlag(args, "--apply")
	force := HasFlag(args, "--force")
	var slugFilter string
	for _, a := range args {
		if !strings.HasPrefix(a, "-") {
			slugFilter = a
			break
		}
	}

	var accounts []AccountConfig
	for _, a := range LoadAccountConfigs() {
		if a.Provider != wisesource.Source {
			continue
		}
		if slugFilter != "" && !strings.EqualFold(a.Slug, slugFilter) {
			continue
		}
		accounts = append(accounts, a)
	}
	if len(accounts) == 0 {
		if slugFilter != "" {
			return fmt.Errorf("no Wise account with slug %q", slugFilter)
		}
		return fmt.Errorf("no accounts with provider %q configured", wisesource.Source)
	}

	creds, err := ResolveOdooCredentials()
	if err != nil {
		return err
	}
	uid, err := odooAuth(creds.URL, creds.DB, creds.Login, creds.Password)
	if err != nil || uid == 0 {
		return fmt.Errorf("Odoo authentication failed: %v", err)
	}
	if apply {
		if err := RequireOdooWriteCapability(); err != nil {
			return err
		}
	}

	fmt.Printf("\n%s🅆 Wise → Odoo sync%s  %s%s (db: %s)%s\n",
		Fmt.Bold, Fmt.Reset, Fmt.Dim, creds.URL, creds.DB, Fmt.Reset)

	var grandMissing, grandCreated, grandClosed int
	for i := range accounts {
		acc := &accounts[i]
		created, missing, closed, err := wiseSyncAccount(creds, uid, acc, apply, force)
		if err != nil {
			Errorf("  %s✗ %s: %v%s", Fmt.Red, acc.Slug, err, Fmt.Reset)
			continue
		}
		grandMissing += missing
		grandCreated += created
		if closed {
			grandClosed++
		}
	}

	fmt.Println()
	switch {
	case grandCreated > 0:
		fmt.Printf("  %s✓ Created %s in Odoo.%s\n\n", Fmt.Green, Pluralize(grandCreated, "missing line", "missing lines"), Fmt.Reset)
	case grandMissing == 0 && grandClosed > 0:
		fmt.Printf("  %s✓ Wise journals reconciled. %s report-only (closed) — see notes above.%s\n\n",
			Fmt.Green, Pluralize(grandClosed, "journal is", "journals are"), Fmt.Reset)
	case grandMissing == 0:
		fmt.Printf("  %s✓ Every Wise CSV transaction is present in Odoo.%s\n\n", Fmt.Green, Fmt.Reset)
	default:
		fmt.Printf("  %s%s missing across open accounts — re-run with %s--apply%s to create them.%s\n\n",
			Fmt.Yellow, Pluralize(grandMissing, "transaction", "transactions"), Fmt.Cyan, Fmt.Yellow, Fmt.Reset)
	}
	return nil
}

// wiseSyncAccount reconciles one Wise balance against its Odoo journal and,
// when apply is set, creates the missing statement lines. Returns (created,
// missing, closed) — closed is true when the journal already carries a year-end
// closing plug, in which case sync is report-only (see wiseClosingPlug) unless
// force is set.
func wiseSyncAccount(creds *OdooCredentials, uid int, acc *AccountConfig, apply, force bool) (int, int, bool, error) {
	if acc.WiseBalanceID == "" {
		return 0, 0, false, fmt.Errorf("account has no wiseBalanceId")
	}
	if acc.OdooJournalID == 0 {
		return 0, 0, false, fmt.Errorf("account has no odooJournalId")
	}
	rows, err := wisesource.LoadForBalance(DataDir(), acc.WiseBalanceID)
	if err != nil {
		return 0, 0, false, fmt.Errorf("load CSV: %v", err)
	}

	fmt.Printf("\n  %s%s%s  %sbalance %s → journal #%d%s\n",
		Fmt.Bold, acc.Name, Fmt.Reset, Fmt.Dim, acc.WiseBalanceID, acc.OdooJournalID, Fmt.Reset)
	if len(rows) == 0 {
		fmt.Printf("    %sNo CSV rows for this balance (drop exports in %s).%s\n",
			Fmt.Dim, wisesource.LatestDir(DataDir()), Fmt.Reset)
		return 0, 0, false, nil
	}

	// CSV-covered date range — the only window we judge completeness in.
	minDate, maxDate := rows[0].Date, rows[0].Date
	for _, r := range rows {
		if r.Date < minDate {
			minDate = r.Date
		}
		if r.Date > maxDate {
			maxDate = r.Date
		}
	}

	lines, _, err := fetchOdooJournalLinesForCacheFull(creds, uid, acc.OdooJournalID)
	if err != nil {
		return 0, 0, false, fmt.Errorf("fetch journal #%d lines: %v", acc.OdooJournalID, err)
	}

	// A journal that already carries a year-end closing plug (a manual balance
	// entry dated after the last CSV transaction) is CLOSED/reconciled. Its lines
	// were imported at a different granularity than the raw CSV (Wise fees folded
	// into the gross payment, off-by-one dates), so the leftover "missing" rows
	// are NOT real gaps — re-creating them duplicates. Sync is report-only for a
	// closed journal, and no writes (opening/closing/create) run, unless --force.
	closingPlug := wiseClosingPlug(lines, maxDate)
	closed := closingPlug != nil
	writeAllowed := apply && (!closed || force)

	// Starting-balance model (like the other cutoff accounts): the journal must
	// hold ONE opening line at the cutoff (= the balance carried from before it)
	// and no transaction lines dated earlier. The Wise running balance is
	// authoritative — the opening is the balance just before the first CSV row.
	if err := wiseSyncStartingBalance(creds, uid, acc, rows, lines, writeAllowed); err != nil {
		return 0, 0, closed, err
	}

	// Match in strict priority passes across ALL rows, not row-by-row: a weaker
	// heuristic on an early row must never consume a line that a later row would
	// match exactly. Two legs of a sent-then-returned transfer share a date and
	// magnitude, so a per-row order would let one leg's date+amount fallback
	// steal the other leg's exact-id line. Every heuristic is sign-guarded — a
	// returned (+) leg can carry the historic ImportID / TransferID of its sent
	// (−) line, and must not match it.
	consumed := make([]bool, len(lines))
	matched := make([]bool, len(rows))
	sameDir := func(a, b float64) bool { return (a >= 0) == (b >= 0) }

	// Pass 1 — exact unique_import_id.
	for ri, r := range rows {
		for i, l := range lines {
			if !consumed[i] && l.UniqueImportID == r.ImportID() && sameDir(l.Amount, r.Amount) {
				consumed[i], matched[ri] = true, true
				break
			}
		}
	}
	// Pass 2 — transfer id appears in payment_ref / narration.
	for ri, r := range rows {
		if matched[ri] {
			continue
		}
		for i, l := range lines {
			if consumed[i] || !sameDir(l.Amount, r.Amount) {
				continue
			}
			if strings.Contains(l.PaymentRef, r.TransferID) || strings.Contains(l.Narration, r.TransferID) {
				consumed[i], matched[ri] = true, true
				break
			}
		}
	}
	// Pass 3 — same date + amount (covers accountant-imported lines).
	for ri, r := range rows {
		if matched[ri] {
			continue
		}
		for i, l := range lines {
			if !consumed[i] && l.Date == r.Date && math.Abs(l.Amount-r.Amount) < 0.005 {
				consumed[i], matched[ri] = true, true
				break
			}
		}
	}

	var missing []wisesource.Transaction
	for ri, r := range rows {
		if !matched[ri] {
			missing = append(missing, r)
		}
	}

	// In-range journal lines that matched no CSV row are suspicious (excluding
	// the manual opening/closing balance entries, which have no CSV counterpart
	// by design).
	var suspicious []OdooCacheLine
	for i, l := range lines {
		if consumed[i] || l.Date < minDate || l.Date > maxDate {
			continue
		}
		if wiseIsBalanceEntry(l) {
			continue
		}
		suspicious = append(suspicious, l)
	}

	fmt.Printf("    %sCSV range %s … %s · %d tx · %d already in Odoo · %s%d missing%s\n",
		Fmt.Dim, minDate, maxDate, len(rows), len(rows)-len(missing), Fmt.Reset+Fmt.Yellow, len(missing), Fmt.Reset)
	if len(suspicious) > 0 {
		fmt.Printf("    %s⚠ %s in Odoo within the CSV range with no matching CSV row (review, not touched):%s\n",
			Fmt.Yellow, Pluralize(len(suspicious), "line", "lines"), Fmt.Reset)
		for _, l := range suspicious {
			fmt.Printf("        %s  %s%12s%s  %s\n", l.Date,
				internalAmtColor(l.Amount), signPrefix(l.Amount)+fmtNumber(math.Abs(l.Amount)), Fmt.Reset,
				Truncate(l.PaymentRef, 44))
		}
	}

	if len(missing) == 0 {
		if closed {
			fmt.Printf("    %s✓ Journal closed (year-end plug %s) — reconciled, nothing to do.%s\n",
				Fmt.Dim, closingPlug.Date, Fmt.Reset)
		}
		return 0, 0, closed, nil
	}

	sort.SliceStable(missing, func(i, j int) bool { return missing[i].Date < missing[j].Date })
	verb := "Missing (will be created with --apply)"
	switch {
	case closed && !force:
		verb = "In CSV but not matched — journal is CLOSED, so NOT created (review only)"
	case writeAllowed:
		verb = "Missing — creating"
	}
	fmt.Printf("    %s%s:%s\n", Fmt.Bold, verb, Fmt.Reset)
	preview := missing
	if len(preview) > 12 && !writeAllowed {
		preview = preview[:12]
	}
	for _, r := range preview {
		fmt.Printf("        %s  %s%12s%s  %s\n", r.Date,
			internalAmtColor(r.Amount), signPrefix(r.Amount)+fmtNumber(math.Abs(r.Amount)), Fmt.Reset,
			Truncate(wiseLineLabel(r), 44))
	}
	if len(missing) > len(preview) {
		fmt.Printf("        %s… and %d more%s\n", Fmt.Dim, len(missing)-len(preview), Fmt.Reset)
	}

	// Closed journal: report-only. These "missing" rows are granularity artefacts
	// (Wise fees split out, off-by-one dates), not real gaps — creating them
	// duplicates the accountant's import. Don't advertise --apply; don't create.
	if closed && !force {
		fmt.Printf("    %s⚠ Journal #%d is closed (year-end 580000 plug). The Odoo import uses a different granularity than the Wise CSV, so these are not real gaps. Report-only — pass %s--force%s to import anyway.%s\n",
			Fmt.Yellow, acc.OdooJournalID, Fmt.Cyan, Fmt.Yellow, Fmt.Reset)
		return 0, 0, true, nil
	}

	if !apply {
		return 0, len(missing), closed, nil
	}

	created := 0
	for _, r := range missing {
		if err := wiseCreateStatementLine(creds, uid, acc, r); err != nil {
			Errorf("        %s✗ %s %s: %v%s", Fmt.Red, r.Date, r.TransferID, err, Fmt.Reset)
			continue
		}
		created++
	}
	fmt.Printf("    %s✓ Created %s in journal #%d.%s\n", Fmt.Green, Pluralize(created, "line", "lines"), acc.OdooJournalID, Fmt.Reset)
	return created, len(missing), closed, nil
}

// wiseClosingPlug returns the journal's year-end closing balance entry — a
// manual balance line (see wiseIsBalanceEntry) dated strictly after the last
// CSV transaction. Its presence marks the journal as closed/reconciled: the
// opening sits at the cutoff and transactions within the CSV range, so anything
// later is the closing plug. Returns nil for an open journal (no such entry),
// which keeps sync fully functional for a fresh/other journal.
func wiseClosingPlug(lines []OdooCacheLine, maxDate string) *OdooCacheLine {
	var plug *OdooCacheLine
	for i := range lines {
		l := lines[i]
		if wiseIsBalanceEntry(l) && l.Date > maxDate {
			if plug == nil || l.Date > plug.Date {
				plug = &lines[i]
			}
		}
	}
	return plug
}

// wiseSyncStartingBalance enforces the cutoff/opening-balance model on the
// journal: exactly one opening line at acc.OdooSyncSince equal to the balance
// carried into the period, and no transaction lines before it. It computes the
// opening from the CSV's authoritative running balance (balance just before the
// first row). Reports in dry-run; on apply creates a missing opening, corrects a
// wrong one, and deletes pre-cutoff transaction lines.
func wiseSyncStartingBalance(creds *OdooCredentials, uid int, acc *AccountConfig, rows []wisesource.Transaction, lines []OdooCacheLine, apply bool) error {
	if acc.OdooSyncSince == "" || len(rows) == 0 {
		return nil
	}
	cutoff := acc.OdooSyncSince
	// rows are sorted oldest-first; opening = running balance before the first.
	opening := roundCents(rows[0].Balance - rows[0].Amount)

	var openingLine *OdooCacheLine
	openingIsClosing := func(l OdooCacheLine) bool {
		return strings.Contains(strings.ToLower(l.PaymentRef), "compte") || strings.Contains(strings.ToLower(l.PaymentRef), "closing")
	}
	var preCutoff []OdooCacheLine
	for i := range lines {
		l := lines[i]
		switch {
		case l.Date == cutoff && wiseIsBalanceEntry(l) && !openingIsClosing(l):
			openingLine = &lines[i]
		case l.Date < cutoff && !wiseIsBalanceEntry(l):
			preCutoff = append(preCutoff, l)
		}
	}

	// Report.
	switch {
	case openingLine == nil:
		fmt.Printf("    %sStarting balance %s: %smissing%s → create %s\n", Fmt.Dim, cutoff, Fmt.Yellow,
			Fmt.Reset+Fmt.Dim, signPrefix(opening)+fmtNumber(math.Abs(opening))+Fmt.Reset)
	case math.Abs(openingLine.Amount-opening) > 0.005:
		fmt.Printf("    %s⚠ Starting balance %s: %s%s%s in Odoo vs %s%s%s from CSV (Δ %s) — review manually%s\n", Fmt.Yellow, cutoff,
			Fmt.Bold, signPrefix(openingLine.Amount)+fmtNumber(math.Abs(openingLine.Amount)), Fmt.Reset+Fmt.Yellow,
			Fmt.Bold, signPrefix(opening)+fmtNumber(math.Abs(opening)), Fmt.Reset+Fmt.Yellow,
			fmtNumber(math.Abs(openingLine.Amount-opening)), Fmt.Reset)
	default:
		fmt.Printf("    %s✓ Starting balance %s present: %s%s\n", Fmt.Dim, cutoff,
			signPrefix(opening)+fmtNumber(math.Abs(opening)), Fmt.Reset)
	}
	if len(preCutoff) > 0 {
		fmt.Printf("    %s%s dated before %s → delete%s\n", Fmt.Yellow,
			Pluralize(len(preCutoff), "transaction line", "transaction lines"), cutoff, Fmt.Reset)
	}

	if !apply {
		return nil
	}

	// Delete pre-cutoff transaction lines.
	if len(preCutoff) > 0 {
		ids := make([]interface{}, 0, len(preCutoff))
		for _, l := range preCutoff {
			ids = append(ids, l.ID)
		}
		if _, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
			"account.bank.statement.line", "unlink", []interface{}{ids}, nil); err != nil {
			return fmt.Errorf("delete pre-cutoff lines: %v", err)
		}
		fmt.Printf("    %s✓ Deleted %s before %s.%s\n", Fmt.Green, Pluralize(len(preCutoff), "line", "lines"), cutoff, Fmt.Reset)
	}

	// Create the opening line if absent (a zero opening needs none).
	if openingLine == nil && math.Abs(opening) > 0.005 {
		vals := map[string]interface{}{
			"journal_id":       acc.OdooJournalID,
			"date":             cutoff,
			"payment_ref":      fmt.Sprintf("Solde de départ %s", cutoff),
			"amount":           opening,
			"unique_import_id": fmt.Sprintf("%s:%s:opening", wisesource.Source, acc.WiseBalanceID),
			"narration":        fmt.Sprintf("<p>Opening balance at %s, carried from the Wise running balance before the first %s transaction.</p>", cutoff, acc.WiseBalanceID),
		}
		created, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
			"account.bank.statement.line", "create", []interface{}{vals}, nil)
		if err != nil {
			return fmt.Errorf("create opening line: %v", err)
		}
		if ids := parseOdooCreatedIDs(created); len(ids) > 0 {
			if err := postStatementLineMoves(creds, uid, ids); err != nil && !strings.Contains(err.Error(), "must be in draft") {
				return fmt.Errorf("opening created but not posted: %v", err)
			}
		}
		fmt.Printf("    %s✓ Created opening balance %s = %s.%s\n", Fmt.Green, cutoff, signPrefix(opening)+fmtNumber(math.Abs(opening)), Fmt.Reset)
	}
	return nil
}

// wiseIsBalanceEntry reports whether a journal line is a manual opening/closing
// balance entry (no CSV counterpart by design).
func wiseIsBalanceEntry(l OdooCacheLine) bool {
	ref := strings.ToLower(l.PaymentRef)
	for _, kw := range []string{"solde", "opening", "ouverture", "départ", "depart", "closing"} {
		if strings.Contains(ref, kw) {
			return true
		}
	}
	return l.UniqueImportID == "" && ref == ""
}

// wiseLineLabel is the human label for a Wise tx (counterparty + description).
func wiseLineLabel(r wisesource.Transaction) string {
	cp := r.Counterparty()
	desc := r.Description
	if r.Reference != "" && r.Reference != desc {
		desc = r.Reference
	}
	switch {
	case cp != "" && desc != "":
		return cp + " · " + desc
	case cp != "":
		return cp
	default:
		return desc
	}
}

// wiseCreateStatementLine creates one posted bank-statement line on the Wise
// journal, tagged with the Wise import id so re-runs are idempotent. The
// counterpart leg lands in the journal's suspense account (Odoo default) until
// reconciled/categorized — getting the transaction into the journal is the goal.
func wiseCreateStatementLine(creds *OdooCredentials, uid int, acc *AccountConfig, r wisesource.Transaction) error {
	narration := fmt.Sprintf(
		"<p>{\"source\":\"wise\",\"transferId\":%q,\"balanceId\":%q,\"counterparty\":%q,\"reference\":%q,\"runningBalance\":%.2f}</p>",
		r.TransferID, r.BalanceID, r.Counterparty(), r.Reference, r.Balance)
	vals := map[string]interface{}{
		"journal_id":       acc.OdooJournalID,
		"date":             r.Date,
		"payment_ref":      Truncate(wiseLineLabel(r), 200),
		"amount":           roundCents(r.Amount),
		"unique_import_id": r.ImportID(),
		"narration":        narration,
	}
	created, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement.line", "create", []interface{}{vals}, nil)
	if err != nil {
		return err
	}
	if ids := parseOdooCreatedIDs(created); len(ids) > 0 {
		// Some journals auto-post the move on creation; posting an
		// already-posted move raises "must be in draft", which is benign — the
		// line is in and posted, exactly what we want.
		if err := postStatementLineMoves(creds, uid, ids); err != nil && !strings.Contains(err.Error(), "must be in draft") {
			return fmt.Errorf("created but not posted: %v", err)
		}
	}
	return nil
}

func printWiseSyncHelp() {
	f := Fmt
	fmt.Printf(`
%schb wise sync%s — Import Wise statement-CSV transactions into their Odoo journals.

Wise is a closed, CSV-only account: one balance per Odoo journal. This compares
each balance's CSV against the linked journal and reports/creates what's missing.
Completeness is judged only WITHIN the CSV's date range — Odoo may hold
older/newer lines the export doesn't cover.

%sUSAGE%s
  %schb wise sync%s                  Preview every Wise account (dry-run)
  %schb wise sync <slug>%s           Preview one account
  %schb wise sync --apply%s          Create the missing lines in Odoo
  %schb wise sync --apply --force%s  Also import into CLOSED journals (rarely wanted)

%sNOTES%s
  Reads CSV exports from %s%s%s.
  Lines are tagged unique_import_id=%swise:<balance>:<transferId>%s so re-runs are
  idempotent. The counterpart leg lands in the journal's suspense account until
  reconciled/categorized.

  %sClosed journals are report-only.%s A journal that already carries a year-end
  closing plug (a balance entry dated after the last CSV row) is treated as
  closed/reconciled: %s--apply%s will NOT create lines for it, because such a
  journal was typically imported at a different granularity than the raw CSV
  (Wise fees folded into the gross payment, off-by-one dates) and re-creating the
  leftover "missing" rows would duplicate. Use %s--force%s to override.
`,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, wisesource.LatestDir(DataDir()), f.Reset,
		f.Dim, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
	)
}
