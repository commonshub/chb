package cmd

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
)

// collectFlagValues returns every value passed to a repeatable `--flag value`
// option (e.g. all `--pair a:b --pair c:d`).
func collectFlagValues(args []string, flag string) []string {
	var out []string
	for i := 0; i+1 < len(args); i++ {
		if args[i] == flag {
			out = append(out, args[i+1])
		}
	}
	return out
}

// applyReconcilePairs applies explicit move↔bank-line links chosen by a
// reviewer, given pairs of "<moveID>:<lineID>" (':' or '=' separated). It
// resolves each move from the local caches and each bank line from the linked
// journal caches, previews the plan, and (with --yes / a y/N confirm) attaches
// them on Odoo via the same path the interactive picker uses.
func applyReconcilePairs(kind moveKind, pairs []string, dryRun, assumeYes bool) error {
	type pp struct{ moveID, lineID int }
	var parsed []pp
	for _, p := range pairs {
		fields := strings.FieldsFunc(p, func(r rune) bool { return r == ':' || r == '=' })
		if len(fields) != 2 {
			return fmt.Errorf("bad --pair %q (want <moveID>:<lineID>)", p)
		}
		mv, e1 := strconv.Atoi(strings.TrimSpace(fields[0]))
		ln, e2 := strconv.Atoi(strings.TrimSpace(fields[1]))
		if e1 != nil || e2 != nil {
			return fmt.Errorf("bad --pair %q (want integer IDs)", p)
		}
		parsed = append(parsed, pp{mv, ln})
	}

	rows, err := loadMoveRows(kind, "", "")
	if err != nil {
		return err
	}
	rowByID := map[int]moveRow{}
	for _, r := range rows {
		rowByID[r.Move.ID] = r
	}

	type lineHit struct {
		ln    OdooCacheLine
		jid   int
		jname string
	}
	lineByID := map[int]lineHit{}
	for _, jid := range allLinkedOdooJournalIDs() {
		lines, ok := loadLatestOdooJournalLinesCache(jid)
		if !ok {
			continue
		}
		jname := ""
		if acc := linkedAccountForJournal(jid); acc != nil {
			jname = acc.Slug
		}
		for _, ln := range lines {
			lineByID[ln.ID] = lineHit{ln, jid, jname}
		}
	}

	type job struct {
		row  moveRow
		hit  lineHit
		sugg Suggestion
	}
	var jobs []job
	fmt.Printf("\n  %sApply %d reconcile pair(s) — %s%s\n", Fmt.Bold, len(parsed), kind.labelPl, Fmt.Reset)
	for _, pr := range parsed {
		row, okMove := rowByID[pr.moveID]
		hit, okLine := lineByID[pr.lineID]
		if !okMove {
			fmt.Printf("  %s✗ %s #%d not found in cache (run `chb %s pull`)%s\n", Fmt.Red, kind.label, pr.moveID, kind.labelPl, Fmt.Reset)
			continue
		}
		if !okLine {
			fmt.Printf("  %s✗ bank line #%d not found in any linked journal cache%s\n", Fmt.Red, pr.lineID, Fmt.Reset)
			continue
		}
		sugg := Suggestion{
			Kind: "bank-line", ID: hit.ln.ID, Date: hit.ln.Date,
			Amount: absFloat(hit.ln.Amount), Currency: "EUR",
			Reference: hit.ln.PaymentRef, Line: hit.ln,
			JournalID: hit.jid, JournalName: hit.jname,
			AlreadyAttached: hit.ln.IsReconciled,
		}
		flag := ""
		if hit.ln.IsReconciled {
			flag = Fmt.Yellow + " [already reconciled — will unreconcile + reattach]" + Fmt.Reset
		}
		fmt.Printf("  %s%s%s ← line #%d (%s, %s, %s) %s%s\n",
			Fmt.Bold, firstNonEmptyStr(row.Move.Title, fmt.Sprintf("#%d", row.Move.ID)), Fmt.Reset,
			hit.ln.ID, hit.jname, hit.ln.Date, fmtAmountCurrency(absFloat(hit.ln.Amount), "EUR"),
			Truncate(hit.ln.PaymentRef, 40), flag)
		jobs = append(jobs, job{row, hit, sugg})
	}

	if len(jobs) == 0 {
		fmt.Printf("\n  %sNothing to apply.%s\n\n", Fmt.Dim, Fmt.Reset)
		return nil
	}
	if dryRun {
		fmt.Printf("\n  %s(dry-run — re-run with --yes to apply.)%s\n\n", Fmt.Dim, Fmt.Reset)
		return nil
	}
	if !assumeYes {
		if !isInteractiveTTY() {
			fmt.Printf("\n  %sRefusing to write on a non-interactive shell without --yes.%s\n\n", Fmt.Yellow, Fmt.Reset)
			return nil
		}
		fmt.Printf("\n  %sApply %d pair(s) on Odoo?%s [y/N] ", Fmt.Bold, len(jobs), Fmt.Reset)
		resp, _ := bufio.NewReader(os.Stdin).ReadString('\n')
		if r := strings.TrimSpace(strings.ToLower(resp)); r != "y" && r != "yes" {
			fmt.Println("  Aborted.")
			return nil
		}
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
	touched := make([]int, 0, len(jobs))
	for i := range jobs {
		j := jobs[i]
		row := j.row
		if err := attachMoveToBankLine(creds, uid, &row, j.sugg); err != nil {
			failed++
			fmt.Printf("  %s✗%s %s #%d ← line #%d: %v\n", Fmt.Red, Fmt.Reset, kind.label, row.Move.ID, j.hit.ln.ID, err)
			continue
		}
		applied++
		touched = append(touched, row.Move.ID)
		fmt.Printf("  %s✓%s %s #%d ← line #%d (%s, %s)\n", Fmt.Green, Fmt.Reset,
			kind.label, row.Move.ID, j.hit.ln.ID, j.hit.jname, j.hit.ln.Date)
	}
	fmt.Printf("\n  %sReconciled %d %s%s", Fmt.Green, applied, kindLabelN(kind, applied), Fmt.Reset)
	if failed > 0 {
		fmt.Printf(" (%s%d failed%s)", Fmt.Red, failed, Fmt.Reset)
	}
	fmt.Println()
	if applied > 0 {
		if _, err := refreshTouchedInvoiceCache(creds, uid, touched); err != nil {
			fmt.Printf("  %s⚠ cache refresh failed: %v%s\n", Fmt.Yellow, err, Fmt.Reset)
		}
	}
	fmt.Println()
	return nil
}

// invoicePaymentCandidate + findInvoicePaymentCandidates have been
// replaced by Suggestion + SuggestForMove in cmd/reconcile_suggest.go,
// which adds two-pass widening (unreconciled → all-posted) so the
// picker can offer unreconcile + reattach for already-paid lines.
// The partner-matching + scoring helpers below (partnerIDForMoveRow,
// partnerNameTokens, bankLineMatchesPartner) are still used by the
// new suggester unchanged.

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
// (draft → rewrite suspense counterpart → repost → reconcile). When
// sugg is an AlreadyAttached suggestion, reconcileStatementLineWithMove
// detects the previous match and performs unreconcile + reattach.
//
// On success, the local journal-lines cache is patched so future runs
// don't propose the same line again, and the in-memory moveRow gets a
// synthetic ReconciledTransaction so the TUI reflects the change
// immediately.
func attachMoveToBankLine(creds *OdooCredentials, uid int, row *moveRow, sugg Suggestion) error {
	line := odooStatementLineForReconcile{
		ID:     sugg.Line.ID,
		MoveID: sugg.Line.MoveID,
		Amount: sugg.Line.Amount,
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
	if cached, ok := loadLatestOdooJournalLinesCache(sugg.JournalID); ok {
		patched := false
		for i := range cached {
			if cached[i].ID == sugg.Line.ID {
				cached[i].IsReconciled = true
				patched = true
				break
			}
		}
		if patched {
			_, _ = writeOdooJournalLinesCacheFile(sugg.JournalID, cached)
		}
	}

	// Synthesise a ReconciledTransaction so the row UI flips from "no
	// payment attached" to "linked tx" immediately. The next `chb pull
	// invoices` regenerates this field from Odoo's source-of-truth.
	row.Move.ReconciledTransaction = &OdooReconciledTransaction{
		ID:           sugg.Line.UniqueImportID,
		Provider:     "odoo",
		Date:         sugg.Line.Date,
		Amount:       sugg.Line.Amount,
		Reference:    sugg.Line.PaymentRef,
		AccountSlug:  sugg.JournalName,
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

	// Explicit pairs: `--pair <moveID>:<lineID>` (repeatable) applies a
	// reviewer's chosen move↔bank-line links directly — the apply path for a
	// reconcile worksheet. Dry-run by default; --yes (or a y/N prompt) writes.
	if pairs := collectFlagValues(args, "--pair"); len(pairs) > 0 {
		return applyReconcilePairs(kind, pairs, dryRun, assumeYes)
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
		// Newest first — surfaces the freshest unmatched items at the top.
		sort.SliceStable(open, func(i, j int) bool { return open[i].Move.Date > open[j].Move.Date })
		runReconcileReviewTUI(kind, scope, open)
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
		Pick     Suggestion   // the candidate to apply (Decision == "match")
		Cands    []Suggestion // all scored candidates (for review/verbose)
		Decision string       // "match" | "review" | "none"
	}
	plans := make([]plan, 0, len(open))
	var matched, review, none int
	for _, r := range open {
		// Batch is intentionally conservative: it only auto-applies when the
		// bank-line memo NAMES the invoice number (near-certain) and exactly
		// one such unreconciled line exists. Everything else — amount-only
		// matches, multiple memo hits, already-paid lines — is left for the
		// guided `-i` review where the operator confirms by hand.
		cands := SuggestBankLinesForMove(r, kind)
		var memo []Suggestion
		for _, s := range cands {
			if s.MemoConfirmed && !s.AlreadyAttached {
				memo = append(memo, s)
			}
		}
		p := plan{Row: r, Cands: cands}
		switch {
		case len(memo) == 1:
			p.Decision, p.Pick = "match", memo[0]
			matched++
		case len(cands) > 0:
			p.Decision = "review"
			review++
		default:
			p.Decision = "none"
			none++
		}
		plans = append(plans, p)
	}

	fmt.Printf("  %sCandidates%s  memo-confirmed: %d  needs review: %d  no-match: %d%s\n",
		Fmt.Bold, Fmt.Dim, matched, review, none, Fmt.Reset)
	if review > 0 {
		fmt.Printf("  %s↳ %d %s have candidates but no unique memo match — run with -i to review them.%s\n",
			Fmt.Dim, review, kindLabelN(kind, review), Fmt.Reset)
	}

	if verbose {
		for _, p := range plans {
			printMovesReconcilePlanRow(kind, p.Row, p.Cands, p.Decision)
		}
	} else if matched > 0 {
		fmt.Println()
		for _, p := range plans {
			if p.Decision == "match" {
				printMovesReconcilePlanRow(kind, p.Row, []Suggestion{p.Pick}, p.Decision)
			}
		}
	}

	if dryRun || matched == 0 {
		if matched == 0 {
			fmt.Printf("\n  %sNothing to auto-reconcile (use -i to review the rest).%s\n", Fmt.Dim, Fmt.Reset)
		} else {
			fmt.Printf("\n  %s(dry-run — re-run with --yes to apply the memo-confirmed matches.)%s\n", Fmt.Dim, Fmt.Reset)
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
		if err := attachMoveToBankLine(creds, uid, &row, p.Pick); err != nil {
			failed++
			LogErrorf("attach %s #%d to line #%d failed: %v",
				kind.label, row.Move.ID, p.Pick.Line.ID, err)
			if verbose {
				fmt.Printf("  %s✗%s %s #%d → line #%d: %v\n",
					Fmt.Red, Fmt.Reset, kind.label, row.Move.ID, p.Pick.Line.ID, err)
			}
			continue
		}
		applied++
		touched = append(touched, row.Move.ID)
		if verbose {
			fmt.Printf("  %s✓%s %s #%d ← line #%d (%s, %s)\n",
				Fmt.Green, Fmt.Reset, kind.label, row.Move.ID, p.Pick.Line.ID,
				p.Pick.JournalName, p.Pick.Line.Date)
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

func printMovesReconcilePlanRow(kind moveKind, row moveRow, cands []Suggestion, decision string) {
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
		if len(cands) == 0 {
			return
		}
		c := cands[0]
		reason := c.MatchReason
		if reason == "" {
			reason = "memo-confirmed"
		}
		fmt.Printf("      %s→%s line #%d (%s) %s  %s  %s(%s)%s\n",
			Fmt.Dim, Fmt.Reset,
			c.Line.ID, c.JournalName, c.Line.Date,
			Truncate(c.Line.PaymentRef, 44),
			Fmt.Green, reason, Fmt.Reset)
	case "review":
		fmt.Printf("      %s? %d candidate(s), no unique memo match — run -i to review%s\n",
			Fmt.Dim, len(cands), Fmt.Reset)
		limit := len(cands)
		if limit > 3 {
			limit = 3
		}
		for i := 0; i < limit; i++ {
			c := cands[i]
			fmt.Printf("          %s· %s line #%d (%s) %s  %s  [%s]%s\n",
				Fmt.Dim, suggestionConfidenceBadge(c), c.Line.ID, c.JournalName, c.Line.Date,
				Truncate(c.Line.PaymentRef, 36),
				firstNonEmptyStr(c.MatchReason, "—"),
				Fmt.Reset)
		}
		if limit < len(cands) {
			fmt.Printf("          %s… and %d more%s\n", Fmt.Dim, len(cands)-limit, Fmt.Reset)
		}
	}
}

// candidatePartnerBadge returns a colored "  [partner]" suffix when
// the candidate's PartnerMatch flag is set, empty string otherwise.
func candidatePartnerBadge(c Suggestion) string {
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
  %s-i%s, %s--interactive%s    Guided review: step through each open item, see
                        scored candidates (memo > amount > partner), and
                        [a]ccept / [s]kip / [/]search by amount, counterpart,
                        IBAN or memo.
  %s--yes%s, %s-y%s             Apply the memo-confirmed matches (skips the y/N prompt)
  %s--dry-run%s             Force dry-run even when combined with --yes
  %s--pair%s <id>:<id>      Apply a chosen move↔bank-line link (repeatable);
                        e.g. --pair 31168:14642. Dry-run unless --yes.
  %s-v%s, %s--verbose%s        Per-row outcomes (ambiguous + no-match included)
  %s--help, -h%s           Show this help

%sBEHAVIOUR%s
  Non-interactive (default): only %smemo-confirmed%s matches (exactly one
  unreconciled bank line whose memo names the invoice number) are
  eligible for write. Lines are pulled from
  %s~/.chb/data/latest/providers/odoo/journals/*.json%s, so the result
  mirrors what %schb odoo journals N reconcile%s would see.

  Interactive (-i): a guided review — one item at a time, with scored
  candidates (memo > exact amount > partner) and a [/] search across all
  bank lines (amount, counterpart, IBAN or memo) for anything the
  auto-matcher missed. Writes to Odoo only on an explicit [a]ccept.

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
