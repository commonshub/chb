package cmd

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
)

// invoicePaymentCandidate pairs a cached unreconciled bank-statement
// line with the journal it lives on. Used by the invoice-side
// reconcile flow: starting from an open invoice/bill, surface every
// bank line whose absolute amount matches (signed correctly for the
// move kind), with partner-aware ordering so the most-likely match
// surfaces first in the TUI picker.
type invoicePaymentCandidate struct {
	JournalID   int
	JournalName string
	Line        OdooCacheLine

	// PartnerMatch is true when this candidate's partner_id resolves
	// to the same Odoo partner the invoice references, OR when a
	// token (≥3 chars) of the invoice's partner display name appears
	// in the bank line's payment_ref / narration / counterparty name.
	// The picker badges these and sorts them above amount-only matches.
	PartnerMatch bool

	// DaysDelta is |bank-line date − invoice date|, used for sorting
	// within the partner-match and non-partner-match tiers. Always ≥0.
	DaysDelta int
}

// findInvoicePaymentCandidates returns the cached bank-statement lines
// that could be the unattached payment for the given invoice / bill.
// Strict filters (always applied):
//
//   - IsReconciled lines are skipped (already attached elsewhere)
//   - direction is gated by kind: invoices want incoming bank lines
//     (positive amount); bills want outgoing (negative)
//   - absolute amount must equal the move's total within ±0.01 EUR
//   - synthetic (non-imported) cache rows are skipped
//
// Scoring (soft):
//
//   - partner match: try partner_id equality first via the local
//     partner index; fall back to a fuzzy token match (any ≥3-char
//     token of the invoice's partner name appearing in the bank
//     line's payment_ref + narration + cached counterparty name).
//   - date delta: absolute days between bank-line date and invoice date.
//
// Two-tier ordering: partner-matching candidates first (sorted by
// date proximity), then non-partner-matching candidates (also by date
// proximity). If the strict-partner pass returns no hits, the picker
// still has the date-sorted full list — the partner condition relaxes
// automatically rather than failing silently.
//
// Reads only local caches — no Odoo RPCs. Up-to-date results require
// a recent `chb pull`.
func findInvoicePaymentCandidates(row moveRow, kind moveKind) []invoicePaymentCandidate {
	amount := row.Move.TotalAmount
	if amount <= 0 {
		return nil
	}
	wantPositive := !kind.isBill // invoices = incoming, bills = outgoing
	moveDate := row.Move.Date

	// Resolve the invoice's partner_id once. We pull it from the
	// private side via the partner index when row.Partner is a name —
	// the public file doesn't carry partner_id directly, so we
	// reverse-look up by display name. Falls through to "no id" on
	// no match; the fuzzy token path still works.
	partnerIdx := loadLatestOdooPartnerIndex(DataDir())
	invoicePartnerID := partnerIDForMoveRow(row, partnerIdx)
	partnerTokens := partnerNameTokens(row.Partner)

	var out []invoicePaymentCandidate
	for _, jid := range allLinkedOdooJournalIDs() {
		lines, ok := loadLatestOdooJournalLinesCache(jid)
		if !ok {
			continue
		}
		journalName := ""
		if acc := linkedAccountForJournal(jid); acc != nil {
			journalName = acc.Slug
		}
		for _, ln := range lines {
			if ln.IsReconciled || isOdooSyntheticLine(ln) || ln.Amount == 0 {
				continue
			}
			if (ln.Amount > 0) != wantPositive {
				continue
			}
			if math.Abs(math.Abs(ln.Amount)-amount) > 0.01 {
				continue
			}
			out = append(out, invoicePaymentCandidate{
				JournalID:    jid,
				JournalName:  journalName,
				Line:         ln,
				PartnerMatch: bankLineMatchesPartner(ln, invoicePartnerID, partnerTokens, partnerIdx),
				DaysDelta:    dateDeltaDaysAbs(ln.Date, moveDate),
			})
		}
	}

	// Two-tier sort: partner-matching first (true > false), then by
	// date proximity within each tier (smaller delta = better). The
	// stable sort keeps cache order as the final tiebreaker.
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].PartnerMatch != out[j].PartnerMatch {
			return out[i].PartnerMatch
		}
		return out[i].DaysDelta < out[j].DaysDelta
	})
	return out
}

// partnerIDForMoveRow returns the Odoo partner_id of the invoice's
// customer / vendor, resolved via the local partner index. Falls back
// to 0 when the partner display name has zero or multiple matches in
// the index — the fuzzy-token path still works in those cases.
func partnerIDForMoveRow(row moveRow, idx *odooPartnerIndex) int {
	if idx == nil || row.Partner == "" {
		return 0
	}
	matches := idx.byName[strings.ToLower(strings.TrimSpace(row.Partner))]
	if len(matches) == 1 {
		return matches[0].ID
	}
	return 0
}

// partnerNameTokens returns the lower-cased ≥3-char word tokens of a
// partner display name. Drops common noise words (vzw, asbl, srl, …)
// and pure-digit tokens so the fuzzy match isn't tripped by legal
// suffixes that match every Belgian entity.
func partnerNameTokens(name string) []string {
	stop := map[string]bool{
		"vzw": true, "asbl": true, "srl": true, "sprl": true, "sa": true,
		"nv": true, "bv": true, "bvba": true, "ltd": true, "llc": true,
		"inc": true, "the": true, "and": true, "co": true,
	}
	var out []string
	for _, t := range strings.Fields(strings.ToLower(name)) {
		t = strings.Trim(t, ",.;:'\"()[]{}")
		if len(t) < 3 || stop[t] {
			continue
		}
		// Drop pure-numeric tokens (postcodes, account numbers leaked
		// into the name field).
		allDigits := true
		for _, r := range t {
			if r < '0' || r > '9' {
				allDigits = false
				break
			}
		}
		if allDigits {
			continue
		}
		out = append(out, t)
	}
	return out
}

// bankLineMatchesPartner reports whether the bank statement line
// plausibly came from / went to the invoice's partner. Tries strict
// partner_id equality first; falls back to a fuzzy substring of any
// invoice-partner-name token (≥3 chars) inside the bank line's
// payment_ref + narration + counterparty-name. Either tier is enough
// to badge the candidate as partner-matched for sorting.
func bankLineMatchesPartner(ln OdooCacheLine, invoicePartnerID int, tokens []string, idx *odooPartnerIndex) bool {
	if invoicePartnerID > 0 && ln.PartnerID > 0 && ln.PartnerID == invoicePartnerID {
		return true
	}
	if len(tokens) == 0 {
		return false
	}
	// Build the bank-line haystack: payment_ref + narration + (the
	// partner name from the partner index when ln.PartnerID is set).
	hay := strings.ToLower(ln.PaymentRef + " " + ln.Narration)
	if idx != nil && ln.PartnerID > 0 {
		if p, ok := idx.byID[ln.PartnerID]; ok {
			hay += " " + strings.ToLower(p.Name)
		}
	}
	for _, t := range tokens {
		if strings.Contains(hay, t) {
			return true
		}
	}
	return false
}

// attachMoveToBankLine wires up the chosen bank line to the
// invoice/bill via the same machinery the journal-side reconcile uses
// (draft → rewrite suspense counterpart → repost → reconcile). On
// success, the local journal-lines cache is patched so future runs
// don't propose the same line again, and the in-memory moveRow gets a
// synthetic ReconciledTransaction so the TUI reflects the change
// immediately.
func attachMoveToBankLine(creds *OdooCredentials, uid int, row *moveRow, cand invoicePaymentCandidate) error {
	line := odooStatementLineForReconcile{
		ID:     cand.Line.ID,
		MoveID: cand.Line.MoveID,
		Amount: cand.Line.Amount,
	}
	moveCand := odooMoveCandidate{
		ID:             row.Move.ID,
		Name:           row.Move.Title,
		PartnerName:    row.Partner,
		AmountResidual: row.Move.TotalAmount,
	}
	if err := reconcileStatementLineWithMove(creds, uid, line, moveCand); err != nil {
		return err
	}

	// Mark the line reconciled in the cache so a follow-up `chb
	// invoices reconcile` (or the interactive picker's next iteration)
	// doesn't surface it again. Cheap in-place patch.
	if cached, ok := loadLatestOdooJournalLinesCache(cand.JournalID); ok {
		patched := false
		for i := range cached {
			if cached[i].ID == cand.Line.ID {
				cached[i].IsReconciled = true
				patched = true
				break
			}
		}
		if patched {
			_, _ = writeOdooJournalLinesCacheFile(cand.JournalID, cached)
		}
	}

	// Synthesise a ReconciledTransaction so the row UI flips from "no
	// payment attached" to "linked tx" immediately. The next `chb pull
	// invoices` regenerates this field from Odoo's source-of-truth.
	row.Move.ReconciledTransaction = &OdooReconciledTransaction{
		ID:           cand.Line.UniqueImportID,
		Provider:     "odoo",
		Date:         cand.Line.Date,
		Amount:       cand.Line.Amount,
		Reference:    cand.Line.PaymentRef,
		AccountSlug:  cand.JournalName,
		Counterparty: row.Partner,
	}
	return nil
}

// MovesReconcileCommandInvoices is the `chb invoices reconcile` entry
// point. Thin wrapper around MovesReconcileCommand.
func MovesReconcileCommandInvoices(args []string) error {
	return MovesReconcileCommand(moveKindInvoice, args)
}

// MovesReconcileCommandBills is the `chb bills reconcile` entry point.
func MovesReconcileCommandBills(args []string) error {
	return MovesReconcileCommand(moveKindBill, args)
}

// MovesReconcileCommand is the entry point for `chb invoices reconcile`
// and `chb bills reconcile`. Reads unreconciled rows in the requested
// scope, matches each to bank lines using findInvoicePaymentCandidates,
// and either:
//
//   - dry-runs / applies unambiguous matches in batch (default), or
//   - opens the TUI when -i / --interactive is passed, letting the
//     operator step through every unreconciled row and pick a
//     candidate per invoice via the [r] hotkey.
//
// Flags:
//
//	-i, --interactive  open the TUI on the unreconciled subset
//	--yes, -y          apply changes (default is dry-run)
//	--dry-run          explicit dry-run (overrides --yes)
//	--verbose, -v      per-row outcome lines (ambiguous + no-match included)
func MovesReconcileCommand(kind moveKind, args []string) error {
	if HasFlag(args, "--help", "-h", "help") {
		printMovesReconcileHelp(kind)
		return nil
	}
	interactive := HasFlag(args, "-i", "--interactive")
	dryRun := HasFlag(args, "--dry-run")
	assumeYes := HasFlag(args, "--yes", "-y")
	verbose := HasFlag(args, "--verbose", "-v")
	if dryRun {
		assumeYes = false
	}
	posYear, posMonth, _ := ParseYearMonthArg(args)

	rows, err := loadMoveRows(kind, posYear, posMonth)
	if err != nil {
		return err
	}
	open := rows[:0]
	for _, r := range rows {
		if moveIsOpen(r.Move) {
			open = append(open, r)
		}
	}
	scope := counterpartiesScopeLabel(posYear, posMonth)
	if scope == "" {
		scope = "all time"
	}

	if interactive {
		if len(open) == 0 {
			fmt.Printf("\n  %sNo unreconciled %s in scope.%s\n\n", Fmt.Dim, kind.labelPl, Fmt.Reset)
			return nil
		}
		// Newest first — matches the regular `chb invoices -i` order
		// and surfaces the freshest unmatched items at the top.
		sort.SliceStable(open, func(i, j int) bool { return open[i].Move.Date > open[j].Move.Date })
		runMovesTUI(kind, scope, open)
		return nil
	}

	fmt.Printf("\n  %sReconcile %s — %s%s\n",
		Fmt.Bold, kind.labelPl, scope, Fmt.Reset)
	if len(open) == 0 {
		fmt.Printf("  %sNo unreconciled %s in scope.%s\n\n", Fmt.Dim, kind.labelPl, Fmt.Reset)
		return nil
	}
	sort.SliceStable(open, func(i, j int) bool { return open[i].Move.Date > open[j].Move.Date })

	type plan struct {
		Row      moveRow
		Cands    []invoicePaymentCandidate
		Decision string // "match" | "ambiguous" | "none"
	}
	plans := make([]plan, 0, len(open))
	var matched, ambiguous, none int
	for _, r := range open {
		cands := findInvoicePaymentCandidates(r, kind)
		p := plan{Row: r, Cands: cands}
		switch {
		case len(cands) == 0:
			p.Decision = "none"
			none++
		case len(cands) == 1:
			p.Decision = "match"
			matched++
		default:
			p.Decision = "ambiguous"
			ambiguous++
		}
		plans = append(plans, p)
	}

	fmt.Printf("  %sCandidates%s  matched: %d  ambiguous: %d  no-match: %d%s\n",
		Fmt.Bold, Fmt.Dim, matched, ambiguous, none, Fmt.Reset)

	if verbose {
		for _, p := range plans {
			printMovesReconcilePlanRow(kind, p.Row, p.Cands, p.Decision)
		}
	} else if matched > 0 {
		fmt.Println()
		for _, p := range plans {
			if p.Decision == "match" {
				printMovesReconcilePlanRow(kind, p.Row, p.Cands, p.Decision)
			}
		}
	}

	if dryRun || matched == 0 {
		if matched == 0 {
			fmt.Printf("\n  %sNothing to reconcile.%s\n", Fmt.Dim, Fmt.Reset)
		} else {
			fmt.Printf("\n  %s(dry-run — re-run with --yes to apply.)%s\n", Fmt.Dim, Fmt.Reset)
		}
		fmt.Println()
		return nil
	}

	if !assumeYes && isInteractiveTTY() {
		fmt.Printf("\n  %sReconcile %d %s on Odoo?%s [Y/n] ",
			Fmt.Bold, matched, kindLabelN(kind, matched), Fmt.Reset)
		reader := bufio.NewReader(os.Stdin)
		resp, _ := reader.ReadString('\n')
		resp = strings.TrimSpace(strings.ToLower(resp))
		if resp == "n" || resp == "no" {
			fmt.Println("  Aborted.")
			return nil
		}
	} else if !assumeYes {
		// Non-TTY caller didn't pass --yes — refuse to write.
		fmt.Printf("\n  %sRefusing to write on a non-interactive shell without --yes.%s\n\n",
			Fmt.Yellow, Fmt.Reset)
		return nil
	}

	creds, err := ResolveOdooCredentials()
	if err != nil {
		return err
	}
	uid, err := odooAuth(creds.URL, creds.DB, creds.Login, creds.Password)
	if err != nil || uid == 0 {
		return fmt.Errorf("Odoo authentication failed: %v", err)
	}
	printOdooWriteBannerOnce(creds.URL, creds.DB)

	var applied, failed int
	touched := make([]int, 0, matched)
	for i := range plans {
		p := &plans[i]
		if p.Decision != "match" {
			continue
		}
		Progress(fmt.Sprintf("attaching %d/%d", applied+failed+1, matched))
		row := p.Row
		if err := attachMoveToBankLine(creds, uid, &row, p.Cands[0]); err != nil {
			failed++
			LogErrorf("attach %s #%d to line #%d failed: %v",
				kind.label, row.Move.ID, p.Cands[0].Line.ID, err)
			if verbose {
				fmt.Printf("  %s✗%s %s #%d → line #%d: %v\n",
					Fmt.Red, Fmt.Reset, kind.label, row.Move.ID, p.Cands[0].Line.ID, err)
			}
			continue
		}
		applied++
		touched = append(touched, row.Move.ID)
		if verbose {
			fmt.Printf("  %s✓%s %s #%d ← line #%d (%s, %s)\n",
				Fmt.Green, Fmt.Reset, kind.label, row.Move.ID, p.Cands[0].Line.ID,
				p.Cands[0].JournalName, p.Cands[0].Line.Date)
		}
	}
	fmt.Printf("\n  %sReconciled %d %s%s",
		Fmt.Green, applied, kindLabelN(kind, applied), Fmt.Reset)
	if failed > 0 {
		fmt.Printf(" (%s%d failed%s)", Fmt.Red, failed, Fmt.Reset)
	}
	fmt.Println()

	if applied > 0 {
		if patched, err := refreshTouchedInvoiceCache(creds, uid, touched); err != nil {
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

func printMovesReconcilePlanRow(kind moveKind, row moveRow, cands []invoicePaymentCandidate, decision string) {
	icon, color := "?", Fmt.Yellow
	switch decision {
	case "match":
		icon, color = "✓", Fmt.Green
	case "none":
		icon, color = "·", Fmt.Dim
	}
	fmt.Printf("  %s%s%s %s  %10s  %s\n",
		color, icon, Fmt.Reset,
		row.Move.Date,
		fmtAmountCurrency(row.Move.TotalAmount, row.Move.Currency),
		Truncate(firstNonEmptyStr(row.Move.Title, fmt.Sprintf("#%d", row.Move.ID)), 50))
	switch decision {
	case "match":
		c := cands[0]
		fmt.Printf("      %s→%s line #%d (%s) %s  %s%s\n",
			Fmt.Dim, Fmt.Reset,
			c.Line.ID, c.JournalName, c.Line.Date,
			Truncate(c.Line.PaymentRef, 50),
			candidatePartnerBadge(c))
	case "ambiguous":
		fmt.Printf("      %s? %d matching unreconciled bank line(s) — skipped (pass -i for interactive resolution)%s\n",
			Fmt.Dim, len(cands), Fmt.Reset)
		limit := len(cands)
		if limit > 3 {
			limit = 3
		}
		for i := 0; i < limit; i++ {
			c := cands[i]
			fmt.Printf("          %s· line #%d (%s) %s  %s%s%s\n",
				Fmt.Dim, c.Line.ID, c.JournalName, c.Line.Date,
				Truncate(c.Line.PaymentRef, 40),
				candidatePartnerBadge(c),
				Fmt.Reset)
		}
		if limit < len(cands) {
			fmt.Printf("          %s… and %d more%s\n", Fmt.Dim, len(cands)-limit, Fmt.Reset)
		}
	}
}

// candidatePartnerBadge returns a colored "  [partner]" suffix when
// the candidate's PartnerMatch flag is set, empty string otherwise.
func candidatePartnerBadge(c invoicePaymentCandidate) string {
	if !c.PartnerMatch {
		return ""
	}
	return fmt.Sprintf("  %s[partner]%s", Fmt.Green, Fmt.Reset)
}

func printMovesReconcileHelp(kind moveKind) {
	f := Fmt
	noun := kind.labelPl
	fmt.Printf(`
%schb %s reconcile [YYYY[/MM]]%s — Attach unreconciled %s to matching
bank-statement lines from the local journal caches. The flip-side of
%schb odoo journals <id> reconcile%s: that one starts from bank lines and
hunts for invoices; this one starts from invoices/bills and hunts for
bank lines.

%sUSAGE%s
  %schb %s reconcile%s                Dry-run preview (all time)
  %schb %s reconcile 2025%s           Dry-run for year 2025
  %schb %s reconcile 2025/12 --yes%s  Apply unambiguous matches for December 2025
  %schb %s reconcile 2025 -i%s        Open the TUI on unreconciled %s only

%sOPTIONS%s
  %s-i%s, %s--interactive%s    Open the TUI on the unreconciled subset; press
                        [r] in the detail view to pick a candidate
                        bank line per row.
  %s--yes%s, %s-y%s             Apply the unambiguous matches (skips the y/N prompt)
  %s--dry-run%s             Force dry-run even when combined with --yes
  %s-v%s, %s--verbose%s        Per-row outcomes (ambiguous + no-match included)
  %s--help, -h%s           Show this help

%sBEHAVIOUR%s
  Non-interactive (default): only %sunambiguous%s matches (exactly one
  unreconciled bank line whose absolute amount equals the move's total)
  are eligible for write. Lines are pulled from
  %s~/.chb/data/latest/providers/odoo/journals/*.json%s, so the result
  mirrors what %schb odoo journals N reconcile%s would see.

  Interactive (-i): same scope, but step through each row in the TUI
  and pick from the full candidate list per invoice. Ambiguous /
  no-match rows that the batch flow would skip can be resolved by
  hand here.

  Run %schb pull%s first if you suspect the cache is stale.

`,
		f.Bold, kind.labelPl, f.Reset, noun,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, kind.labelPl, f.Reset,
		f.Cyan, kind.labelPl, f.Reset,
		f.Cyan, kind.labelPl, f.Reset,
		f.Cyan, kind.labelPl, f.Reset, kind.labelPl,
		f.Bold, f.Reset,
		f.Yellow, f.Reset, f.Yellow, f.Reset,
		f.Yellow, f.Reset, f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset, f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Dim, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
	)
}
