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

	var grandMissing, grandCreated int
	for i := range accounts {
		acc := &accounts[i]
		created, missing, err := wiseSyncAccount(creds, uid, acc, apply)
		if err != nil {
			Errorf("  %s✗ %s: %v%s", Fmt.Red, acc.Slug, err, Fmt.Reset)
			continue
		}
		grandMissing += missing
		grandCreated += created
	}

	fmt.Println()
	switch {
	case grandMissing == 0:
		fmt.Printf("  %s✓ Every Wise CSV transaction is present in Odoo.%s\n\n", Fmt.Green, Fmt.Reset)
	case apply:
		fmt.Printf("  %s✓ Created %s in Odoo.%s\n\n", Fmt.Green, Pluralize(grandCreated, "missing line", "missing lines"), Fmt.Reset)
	default:
		fmt.Printf("  %s%s missing across all accounts — re-run with %s--apply%s to create them.%s\n\n",
			Fmt.Yellow, Pluralize(grandMissing, "transaction", "transactions"), Fmt.Cyan, Fmt.Yellow, Fmt.Reset)
	}
	return nil
}

// wiseSyncAccount reconciles one Wise balance against its Odoo journal and,
// when apply is set, creates the missing statement lines. Returns (created,
// missing) counts.
func wiseSyncAccount(creds *OdooCredentials, uid int, acc *AccountConfig, apply bool) (int, int, error) {
	if acc.WiseBalanceID == "" {
		return 0, 0, fmt.Errorf("account has no wiseBalanceId")
	}
	if acc.OdooJournalID == 0 {
		return 0, 0, fmt.Errorf("account has no odooJournalId")
	}
	rows, err := wisesource.LoadForBalance(DataDir(), acc.WiseBalanceID)
	if err != nil {
		return 0, 0, fmt.Errorf("load CSV: %v", err)
	}

	fmt.Printf("\n  %s%s%s  %sbalance %s → journal #%d%s\n",
		Fmt.Bold, acc.Name, Fmt.Reset, Fmt.Dim, acc.WiseBalanceID, acc.OdooJournalID, Fmt.Reset)
	if len(rows) == 0 {
		fmt.Printf("    %sNo CSV rows for this balance (drop exports in %s).%s\n",
			Fmt.Dim, wisesource.LatestDir(DataDir()), Fmt.Reset)
		return 0, 0, nil
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
		return 0, 0, fmt.Errorf("fetch journal #%d lines: %v", acc.OdooJournalID, err)
	}

	// Greedy match: each CSV row consumes at most one journal line.
	consumed := make([]bool, len(lines))
	matchLine := func(r wisesource.Transaction) int {
		// 1. exact unique_import_id
		for i, l := range lines {
			if !consumed[i] && l.UniqueImportID == r.ImportID() {
				return i
			}
		}
		// 2. transfer id appears in payment_ref / narration
		for i, l := range lines {
			if consumed[i] {
				continue
			}
			if strings.Contains(l.PaymentRef, r.TransferID) || strings.Contains(l.Narration, r.TransferID) {
				return i
			}
		}
		// 3. same date + amount (covers accountant-imported lines)
		for i, l := range lines {
			if !consumed[i] && l.Date == r.Date && math.Abs(l.Amount-r.Amount) < 0.005 {
				return i
			}
		}
		return -1
	}

	var missing []wisesource.Transaction
	for _, r := range rows {
		if idx := matchLine(r); idx >= 0 {
			consumed[idx] = true
		} else {
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
		return 0, 0, nil
	}

	sort.SliceStable(missing, func(i, j int) bool { return missing[i].Date < missing[j].Date })
	verb := "Missing (will be created with --apply)"
	if apply {
		verb = "Missing — creating"
	}
	fmt.Printf("    %s%s:%s\n", Fmt.Bold, verb, Fmt.Reset)
	preview := missing
	if len(preview) > 12 && !apply {
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

	if !apply {
		return 0, len(missing), nil
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
	return created, len(missing), nil
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

%sNOTES%s
  Reads CSV exports from %s%s%s.
  Lines are tagged unique_import_id=%swise:<balance>:<transferId>%s so re-runs are
  idempotent. The counterpart leg lands in the journal's suspense account until
  reconciled/categorized.
`,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, wisesource.LatestDir(DataDir()), f.Reset,
		f.Dim, f.Reset,
	)
}
