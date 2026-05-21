package cmd

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	odoosource "github.com/CommonsHub/chb/providers/odoo"
)

// isRefMatchable reports whether a candidate's number is safe to use as
// a substring needle against bank payment refs. We require ≥ 5 chars and
// at least one non-digit — pure short integers (Odoo internal IDs) cause
// far more false positives than real catches.
func isRefMatchable(s string) bool {
	if len(s) < 5 {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return true
		}
	}
	return false
}

// parseOdooDate accepts both "2026-05-04" and "2026-05-04 13:30:35" forms
// used across Odoo's local cache, returning the date portion as time.Time.
func parseOdooDate(s string) (time.Time, error) {
	if len(s) < 10 {
		return time.Time{}, fmt.Errorf("date %q too short", s)
	}
	return time.Parse("2006-01-02", s[:10])
}

// reconcileLineMatch pairs a bank-statement line with its candidate
// invoices/bills. Used by the interactive prompt + the print pass.
type reconcileLineMatch struct {
	Line OdooCacheLine
	Hits []reconcileCandidate
}

// reconcileDup is one group of bank lines that all matched the same
// candidate (invoice/bill). The line whose date is closest to the
// candidate's issue date wins; the others are flagged as possible
// duplicate payments.
type reconcileDup struct {
	Candidate reconcileCandidate
	Winner    OdooCacheLine
	Extras    []OdooCacheLine
}

// reconcileMatchSet is the full result of the local matcher: every
// unambiguous winner is in Matches; ambiguous lines stay in Matches
// with multiple Hits; duplicate-payment groups are in Duplicates; the
// indices of demoted (extra) lines in Matches are in DemotedToDuplicate
// so the print pass can skip them and the apply pass doesn't double-
// reconcile the same candidate.
type reconcileMatchSet struct {
	Matches            []reconcileLineMatch
	Duplicates         []reconcileDup
	DemotedToDuplicate map[int]bool
}

// reconcileCandidate is the local-cache view of an invoice or bill,
// kept narrow enough that the matcher doesn't drown in fields. Open
// candidates power the auto-matcher; reconciled / in_payment / paid
// candidates show up in the interactive prompt's "top 5 by date" list
// so the operator can override an existing match if needed.
type reconcileCandidate struct {
	ID           int    // Odoo account.move id — primary dedupe key
	Kind         string // "invoice" or "bill"
	Number       string
	Residual     float64
	PartnerID    int
	PartnerName  string
	Date         string
	State        string // Odoo move state ("posted", "draft", ...)
	PaymentState string // "not_paid" / "in_payment" / "partial" / "paid"
}

// IsOpen reports whether the candidate still has an unsettled balance,
// using the same rule as invoiceIsOpen but reading the candidate's own
// fields instead of the source-of-truth invoice record.
func (c reconcileCandidate) IsOpen() bool {
	if c.State != "" && c.State != "posted" {
		return false
	}
	if strings.EqualFold(c.PaymentState, "paid") {
		return false
	}
	return true
}

// key returns a stable identity for the candidate suitable for use as a
// map key. Falls back through Number then ID so candidates with the same
// printed number still dedupe correctly even when one is missing it.
func (c reconcileCandidate) key() string {
	return fmt.Sprintf("%s#%d", c.Kind, c.ID)
}

// label returns the operator-visible name for the candidate: the real
// Number / Reference when available, falling back to "<kind> #<id>" so
// candidates with no number still print something distinguishable.
func (c reconcileCandidate) label() string {
	if c.Number != "" {
		return c.Number
	}
	return fmt.Sprintf("#%d", c.ID)
}

// computeReconcileMatches builds the full match set from local caches:
// journal lines + open invoices/bills from the private projection.
// No RPCs — the result is the same data --dry-run shows and the live
// reconcile path applies. The optional interactive prompt collapses
// ambiguous lines to single winners before dedupe runs.
//
// Reads:
//   - Journal lines from providers/odoo/journals/<id>.json
//   - Open invoices + bills from providers/odoo/private/{invoices,bills}.json
//     (the public projection strips partner_id + residualAmount + reference,
//     which the matcher needs; private has them.)
//
// Matching strategy, per bank line, first match wins:
//
//  1. payment_ref contains the invoice/bill number → strong.
//  2. exact amount + same partner_id → strong.
//  3. exact amount + partner display-name substring in line ref →
//     medium. Catches "bank line says 'Innerpreneurs', invoice partner
//     is Veerle/Innerpreneurs" where partner_ids diverge but the human
//     label clearly matches.
//  4. amount-only with exactly one open candidate → likely.
//  5. multiple amount-only candidates → flagged ambiguous.
func computeReconcileMatches(journalID int, interactive bool) (*reconcileMatchSet, *odooPartnerIndex, error) {
	lines, ok := loadLatestOdooJournalLinesCache(journalID)
	if !ok {
		return nil, nil, fmt.Errorf("no local cache for journal #%d — run `chb odoo pull` first", journalID)
	}
	candidates, err := loadLocalOpenCandidates()
	if err != nil {
		return nil, nil, err
	}
	// Partner index (optional) so the interactive prompt can show the
	// bank line's counterparty name, not just the integer id.
	partnerIdx := loadLatestOdooPartnerIndex(DataDir())
	if len(candidates) == 0 {
		return &reconcileMatchSet{DemotedToDuplicate: map[int]bool{}}, partnerIdx, errNoLocalCandidates
	}

	byPartner := map[int][]reconcileCandidate{}
	byAmount := map[int64][]reconcileCandidate{}
	for _, c := range candidates {
		byPartner[c.PartnerID] = append(byPartner[c.PartnerID], c)
		byAmount[centsKey(c.Residual)] = append(byAmount[centsKey(c.Residual)], c)
	}

	// Phase 1: per-line matching (each line independent). Ambiguous hits
	// are sorted by date proximity to the bank line — closest first —
	// so the interactive prompt's first suggestion is the most likely.
	results := make([]reconcileLineMatch, 0, len(lines))
	for _, ln := range lines {
		// Already-reconciled lines are skipped. Without this the matcher
		// keeps proposing them every run: their payment_ref still matches
		// the invoice (now partial/paid) and we'd churn against Odoo's
		// "already reconciled" error indefinitely. The flag is populated
		// from account.bank.statement.line.is_reconciled when the journal
		// cache is written.
		if ln.IsReconciled || isOdooSyntheticLine(ln) || ln.Amount == 0 {
			continue
		}
		hits := matchLineToCandidates(ln, candidates, byPartner, byAmount)
		if len(hits) > 1 {
			lineDate := ln.Date
			sort.SliceStable(hits, func(i, j int) bool {
				return dateDeltaDaysAbs(hits[i].Date, lineDate) < dateDeltaDaysAbs(hits[j].Date, lineDate)
			})
		}
		results = append(results, reconcileLineMatch{Line: ln, Hits: hits})
	}

	// Interactive resolution of ambiguous matches. The user steps through
	// each one, sees the bank line + ordered candidates, picks a winner
	// or skips. Selected picks collapse to a single-hit (treated like an
	// unambiguous match downstream).
	if interactive {
		resolveAmbiguousInteractively(results, partnerIdx)
	}

	// Phase 2: dedupe. When several bank lines pick the same single
	// candidate (the unambiguous case), the line whose date is closest
	// to the invoice/bill date is the winning match — the others are
	// almost-certainly duplicate payments (customer paid twice, refund
	// pending, etc.) and we surface them separately at the end.
	groupedByCandidate := map[string][]int{} // candidate key → indices into results
	candidateByKey := map[string]reconcileCandidate{}
	for i, r := range results {
		if len(r.Hits) != 1 {
			continue
		}
		key := r.Hits[0].key()
		groupedByCandidate[key] = append(groupedByCandidate[key], i)
		candidateByKey[key] = r.Hits[0]
	}
	demotedToDuplicate := map[int]bool{}
	var duplicates []reconcileDup
	for key, idxs := range groupedByCandidate {
		if len(idxs) <= 1 {
			continue
		}
		cand := candidateByKey[key]
		// Pick the index whose line date is closest to the candidate date.
		// Falls back to the first index if dates are unparseable.
		winnerIdx := idxs[0]
		bestDelta := dateDeltaDaysAbs(results[idxs[0]].Line.Date, cand.Date)
		for _, i := range idxs[1:] {
			d := dateDeltaDaysAbs(results[i].Line.Date, cand.Date)
			if d < bestDelta {
				bestDelta = d
				winnerIdx = i
			}
		}
		var extras []OdooCacheLine
		for _, i := range idxs {
			if i == winnerIdx {
				continue
			}
			demotedToDuplicate[i] = true
			extras = append(extras, results[i].Line)
		}
		duplicates = append(duplicates, reconcileDup{
			Candidate: cand,
			Winner:    results[winnerIdx].Line,
			Extras:    extras,
		})
	}

	return &reconcileMatchSet{
		Matches:            results,
		Duplicates:         duplicates,
		DemotedToDuplicate: demotedToDuplicate,
	}, partnerIdx, nil
}

// errNoLocalCandidates signals that the private invoices/bills cache is
// empty — the caller should print a refresh hint instead of running the
// matcher with nothing to match against.
var errNoLocalCandidates = fmt.Errorf("no open invoices or bills in the local private cache")

// reconcileMatchCounts is a tiny aggregate used by both the live
// summary (counts-only, shown before the y/N prompt) and the dry-run
// summary footer.
type reconcileMatchCounts struct {
	Matched    int
	Ambiguous  int
	NoMatch    int
	Duplicates int
}

func (s *reconcileMatchSet) counts() reconcileMatchCounts {
	var c reconcileMatchCounts
	for i, r := range s.Matches {
		if s.DemotedToDuplicate[i] {
			continue
		}
		switch {
		case len(r.Hits) == 0:
			c.NoMatch++
		case len(r.Hits) == 1:
			c.Matched++
		default:
			c.Ambiguous++
		}
	}
	c.Duplicates = len(s.Duplicates)
	return c
}

// unambiguousWinners returns every match with exactly one candidate that
// has not been demoted to a duplicate-payment extra. These are the rows
// the live apply phase will reconcile.
func (s *reconcileMatchSet) unambiguousWinners() []reconcileLineMatch {
	out := make([]reconcileLineMatch, 0, len(s.Matches))
	for i, r := range s.Matches {
		if s.DemotedToDuplicate[i] {
			continue
		}
		if len(r.Hits) == 1 {
			out = append(out, r)
		}
	}
	return out
}

// printReconcileMatches prints the per-line view + duplicates section
// + summary footer for --dry-run.
//
// Default (compact) mode: only unambiguous "✓" matches show, each as
// two lines — bank line on top, invoice underneath. Ambiguous /
// no-match rows roll up into the summary count alone. `--verbose`
// brings back the full picture (ambiguous candidate list, no-match
// rows).
func printReconcileMatches(set *reconcileMatchSet, verbose bool) {
	counts := set.counts()
	matchPrinted := 0
	for i, r := range set.Matches {
		ln := r.Line
		hits := r.Hits
		if set.DemotedToDuplicate[i] {
			continue
		}
		switch {
		case len(hits) == 0:
			if verbose {
				fmt.Printf("  %s✗ %s  %10s  %s%s\n",
					Fmt.Dim, ln.Date, formatBalancePlain(ln.Amount, "EUR"),
					truncate(reconcileLineDescription(ln), 60), Fmt.Reset)
				fmt.Printf("      %sno matching invoice/bill%s\n", Fmt.Dim, Fmt.Reset)
			}
		case len(hits) == 1:
			c := hits[0]
			fmt.Printf("  %s✓%s %s  %10s  %s\n",
				Fmt.Green, Fmt.Reset,
				ln.Date, formatBalancePlain(ln.Amount, "EUR"),
				truncate(reconcileLineDescription(ln), 60))
			fmt.Printf("      %s→%s %-7s %-18s  %10s  %s%s%s\n",
				Fmt.Dim, Fmt.Reset,
				c.Kind, c.Number,
				formatBalancePlain(c.Residual, "EUR"),
				Fmt.Dim, truncate(c.PartnerName, 40), Fmt.Reset)
			matchPrinted++
		default:
			if !verbose {
				continue
			}
			fmt.Printf("  %s? %s  %10s  %s — %d candidates%s\n",
				Fmt.Yellow, ln.Date, formatBalancePlain(ln.Amount, "EUR"),
				truncate(reconcileLineDescription(ln), 60), len(hits), Fmt.Reset)
			limit := len(hits)
			if limit > 5 {
				limit = 5
			}
			for j := 0; j < limit; j++ {
				fmt.Printf("      %s%-7s %-18s  %10s  %s%s\n",
					Fmt.Dim, hits[j].Kind, hits[j].Number,
					formatBalancePlain(hits[j].Residual, "EUR"),
					truncate(hits[j].PartnerName, 40), Fmt.Reset)
			}
			if limit < len(hits) {
				fmt.Printf("      %s… and %d more%s\n", Fmt.Dim, len(hits)-limit, Fmt.Reset)
			}
		}
	}

	if !verbose && matchPrinted == 0 && counts.Matched == 0 {
		fmt.Printf("  %sNo perfect matches.%s\n", Fmt.Dim, Fmt.Reset)
	}

	if len(set.Duplicates) > 0 {
		extraTotal := 0
		for _, d := range set.Duplicates {
			extraTotal += len(d.Extras)
		}
		fmt.Printf("\n  %s⚠ Possible duplicate payments (%d invoice%s, %d extra line%s)%s\n",
			Fmt.Yellow, len(set.Duplicates), plural(len(set.Duplicates)), extraTotal, plural(extraTotal), Fmt.Reset)
		fmt.Printf("  %sSame invoice/bill claimed by more than one bank line — closest-to-issue-date wins; the rest may be duplicate payments to investigate.%s\n\n",
			Fmt.Dim, Fmt.Reset)
		for _, d := range set.Duplicates {
			fmt.Printf("  %s→ %s %-18s  %10s  %s%s%s  issued %s\n",
				Fmt.Cyan, d.Candidate.Kind, d.Candidate.label(),
				formatBalancePlain(d.Candidate.Residual, "EUR"),
				Fmt.Dim, truncate(d.Candidate.PartnerName, 40), Fmt.Reset, d.Candidate.Date)
			fmt.Printf("      %s✓ winner:%s %s  %s\n",
				Fmt.Green, Fmt.Reset,
				d.Winner.Date, formatBalancePlain(d.Winner.Amount, "EUR"))
			for _, e := range d.Extras {
				fmt.Printf("      %s⚠ extra:%s  %s  %s\n",
					Fmt.Yellow, Fmt.Reset,
					e.Date, formatBalancePlain(e.Amount, "EUR"))
			}
		}
	}

	fmt.Printf("\n  %sSummary%s  %s%d matched%s · %s%d ambiguous%s · %s%d no-match%s · %s%d duplicates%s\n",
		Fmt.Bold, Fmt.Reset,
		Fmt.Green, counts.Matched, Fmt.Reset,
		Fmt.Yellow, counts.Ambiguous, Fmt.Reset,
		Fmt.Dim, counts.NoMatch, Fmt.Reset,
		Fmt.Yellow, counts.Duplicates, Fmt.Reset)
	if !verbose && (counts.Ambiguous > 0 || counts.NoMatch > 0) {
		fmt.Printf("  %s(--verbose to list ambiguous / no-match lines; -i to resolve ambiguous interactively)%s\n",
			Fmt.Dim, Fmt.Reset)
	}
}

// printReconcileSummary prints a one-block headline used before the
// live confirmation prompt — just the counts, not every line.
func printReconcileSummary(set *reconcileMatchSet) {
	c := set.counts()
	fmt.Printf("  %sPlanned actions%s  matched: %d  ambiguous: %d  no-match: %d  duplicates: %d\n",
		Fmt.Bold, Fmt.Reset, c.Matched, c.Ambiguous, c.NoMatch, c.Duplicates)
	fmt.Printf("  %sOnly the %d unambiguous match%s will be reconciled. Ambiguous lines stay open until resolved with -i.%s\n",
		Fmt.Dim, c.Matched, plural(c.Matched), Fmt.Reset)
}

// reconcileDryRunLocal is the --dry-run orchestrator: compute → print
// full detail → footer.
func reconcileDryRunLocal(journalID int, verbose, interactive bool) error {
	set, _, err := computeReconcileMatches(journalID, interactive)
	if err != nil {
		if err == errNoLocalCandidates {
			fmt.Printf("  %sNo open invoices or bills in the local private cache.%s\n", Fmt.Yellow, Fmt.Reset)
			fmt.Printf("  %s(Run `chb odoo pull` to refresh, or check that ODOO_URL gives access to the records.)%s\n\n", Fmt.Dim, Fmt.Reset)
			return nil
		}
		return err
	}
	printReconcileMatches(set, verbose)
	fmt.Printf("  %s(local-only preview — no Odoo calls. Re-run without --dry-run to apply.)%s\n\n",
		Fmt.Dim, Fmt.Reset)
	return nil
}

// interactiveSuggestionCount caps how many candidates the -i prompt
// surfaces per ambiguous line. Five is the sweet spot: enough to cover
// the legitimate alternatives (typical partner has 2-3 open + 1-2 paid
// close in time), small enough to fit on screen and stay scannable.
const interactiveSuggestionCount = 5

// resolveAmbiguousInteractively walks every ambiguous lineMatch (>1
// candidate) and prompts the operator to pick one or skip. Selected
// candidates collapse the Hits slice to a single element so downstream
// logic treats it like a clean match. Skipped lines keep their full
// candidate list — they'll be reported as ambiguous in the summary.
//
// Suggestions per line are the 5 candidates whose absolute amount
// matches the bank line and whose date is closest to it — *regardless
// of payment state*. Paid / in_payment / partial invoices show up too,
// so the operator can override a previous (wrong) reconciliation when
// needed. The default pick (Enter) is candidate [1], the closest by
// date.
//
// Lines are walked newest-first: most recent bank activity is freshest
// in the operator's mind.
func resolveAmbiguousInteractively(results []reconcileLineMatch, partners *odooPartnerIndex) {
	reader := bufio.NewReader(os.Stdin)
	ambIdxs := make([]int, 0)
	for i, r := range results {
		if len(r.Hits) > 1 {
			ambIdxs = append(ambIdxs, i)
		}
	}
	if len(ambIdxs) == 0 {
		return
	}
	sort.SliceStable(ambIdxs, func(a, b int) bool {
		return results[ambIdxs[a]].Line.Date > results[ambIdxs[b]].Line.Date
	})

	// Load every posted invoice/bill (open AND closed) and index by
	// amount in cents — the interactive picker uses this to surface
	// top-5-by-date alternatives, even ones already reconciled.
	allCandidates, _ := loadLocalAllPostedCandidates()
	byAmountAll := map[int64][]reconcileCandidate{}
	for _, c := range allCandidates {
		byAmountAll[centsKey(c.Residual)] = append(byAmountAll[centsKey(c.Residual)], c)
	}

	odooBaseURL := strings.TrimRight(os.Getenv("ODOO_URL"), "/")

	fmt.Printf("\n  %s🔎 Interactive resolution of %d ambiguous line%s (newest first)%s\n",
		Fmt.Bold, len(ambIdxs), plural(len(ambIdxs)), Fmt.Reset)
	fmt.Printf("  %sEnter accepts [1] · 1-N picks a candidate · s skips · q quits%s\n",
		Fmt.Dim, Fmt.Reset)

	for cursor, i := range ambIdxs {
		ln := results[i].Line
		direction := "invoice"
		if ln.Amount < 0 {
			direction = "bill"
		}
		hits := topCandidatesByDateAmount(ln, byAmountAll, direction, interactiveSuggestionCount)
		if len(hits) == 0 {
			// Fall back to the matcher's own hits (sorted by date proximity).
			hits = results[i].Hits
		}

		fmt.Printf("\n  %s[%d/%d] line #%d%s\n",
			Fmt.Bold, cursor+1, len(ambIdxs), ln.ID, Fmt.Reset)
		fmt.Printf("        Date:         %s\n", ln.Date)
		fmt.Printf("        Amount:       %s\n", formatBalancePlain(ln.Amount, "EUR"))
		fmt.Printf("        Counterparty: %s\n", reconcileLineCounterparty(ln, partners))
		if d := reconcileLineDescription(ln); d != "" {
			fmt.Printf("        Description:  %s\n", d)
		}

		// Candidates. Column order: date · partner · ref (hyperlinked) ·
		// residual · date-delta · status badge (only when not "open").
		fmt.Println()
		limit := len(hits)
		if limit > 9 {
			limit = 9 // single-keystroke menu
		}
		for k := 0; k < limit; k++ {
			c := hits[k]
			rel := dateRelativePhrase(c.Date, ln.Date)
			status := candidateStatusBadge(c)
			fmt.Printf("        [%d] %s  %-30s  %s %s  %s  (%s)%s\n",
				k+1,
				c.Date,
				truncate(c.PartnerName, 28),
				c.Kind, formatCandidateRefLink(c, odooBaseURL, 18),
				formatBalancePlain(c.Residual, "EUR"),
				rel, status)
		}
		if limit < len(hits) {
			fmt.Printf("        %s… and %d more (skip to see all in summary)%s\n",
				Fmt.Dim, len(hits)-limit, Fmt.Reset)
		}

		fmt.Printf("\n        Pick [Enter=1 / 1-%d / s skip / q quit]: ", limit)
		raw, _ := reader.ReadString('\n')
		choice := strings.TrimSpace(strings.ToLower(raw))
		switch choice {
		case "":
			results[i].Hits = []reconcileCandidate{hits[0]}
			fmt.Printf("        %s↳ accepted [1] %s %s%s%s\n",
				Fmt.Dim, hits[0].Kind, hits[0].label(), candidateStatusBadge(hits[0]), Fmt.Reset)
			continue
		case "s":
			continue
		case "q":
			fmt.Printf("        %s(stopped interactive resolution at %d/%d)%s\n",
				Fmt.Dim, cursor+1, len(ambIdxs), Fmt.Reset)
			return
		}
		n, err := strconv.Atoi(choice)
		if err != nil || n < 1 || n > limit {
			fmt.Printf("        %sinvalid choice %q — skipped%s\n", Fmt.Yellow, choice, Fmt.Reset)
			continue
		}
		results[i].Hits = []reconcileCandidate{hits[n-1]}
	}
}

// topCandidatesByDateAmount returns up to `limit` candidates that
// match the bank line's absolute amount, direction-gated (incoming →
// invoice, outgoing → bill), ordered by absolute date distance from
// the line. Regardless of payment state — paid candidates surface
// alongside open ones so the operator can override an existing match.
func topCandidatesByDateAmount(ln OdooCacheLine, byAmount map[int64][]reconcileCandidate, wantKind string, limit int) []reconcileCandidate {
	amt := centsKey(ln.Amount)
	pool := byAmount[amt]
	matches := make([]reconcileCandidate, 0, len(pool))
	for _, c := range pool {
		if c.Kind != wantKind {
			continue
		}
		matches = append(matches, c)
	}
	sort.SliceStable(matches, func(i, j int) bool {
		return dateDeltaDaysAbs(matches[i].Date, ln.Date) < dateDeltaDaysAbs(matches[j].Date, ln.Date)
	})
	if len(matches) > limit {
		matches = matches[:limit]
	}
	return matches
}

// dateRelativePhrase returns "N days earlier" / "N days later" /
// "same day" for the candidate date relative to the line date. Falls
// back to "N days" when either date can't be parsed.
func dateRelativePhrase(candidateDate, lineDate string) string {
	delta := dateDeltaDaysAbs(candidateDate, lineDate)
	t1, err1 := parseOdooDate(candidateDate)
	t2, err2 := parseOdooDate(lineDate)
	if err1 != nil || err2 != nil {
		return fmt.Sprintf("%d days", delta)
	}
	switch {
	case t1.Before(t2):
		return fmt.Sprintf("%d days earlier", delta)
	case t1.After(t2):
		return fmt.Sprintf("%d days later", delta)
	default:
		return "same day"
	}
}

// candidateStatusBadge returns a dim-yellow suffix flagging the
// candidate's payment state when it's not open — paid / in_payment /
// partial. Empty string for open candidates (the default; no need to
// clutter the row).
func candidateStatusBadge(c reconcileCandidate) string {
	state := strings.ToLower(c.PaymentState)
	switch state {
	case "", "not_paid":
		return ""
	case "paid":
		return "  " + Fmt.Yellow + "[already paid — will unreconcile + reattach]" + Fmt.Reset
	case "in_payment":
		return "  " + Fmt.Yellow + "[in_payment]" + Fmt.Reset
	case "partial":
		return "  " + Fmt.Yellow + "[partial]" + Fmt.Reset
	}
	return "  " + Fmt.Dim + "[" + state + "]" + Fmt.Reset
}

// formatCandidateRefLink renders the candidate's reference as an OSC 8
// hyperlink to the Odoo form view, padded to a fixed width so the table
// columns stay aligned. Falls back to plain padded text when no Odoo
// base URL is configured. Terminals that don't support OSC 8 (rare on
// modern setups) show the escape sequences inline, harmless but ugly —
// users can set NO_COLOR=1 or just ignore them.
func formatCandidateRefLink(c reconcileCandidate, odooBaseURL string, width int) string {
	label := c.label()
	if odooBaseURL == "" || c.ID == 0 || !stdoutIsTTY() {
		return fmt.Sprintf("%-*s", width, label)
	}
	pad := width - len(label)
	if pad < 0 {
		pad = 0
	}
	url := fmt.Sprintf("%s/web#id=%d&model=account.move&view_type=form", odooBaseURL, c.ID)
	// OSC 8: \x1b]8;;<url>\x1b\\<text>\x1b]8;;\x1b\\ — pad goes *outside*
	// the hyperlink wrapper so trailing spaces aren't underlined.
	return fmt.Sprintf("\x1b]8;;%s\x1b\\%s\x1b]8;;\x1b\\%s",
		url, label, strings.Repeat(" ", pad))
}

// stdoutIsTTY reports whether stdout is an interactive terminal. Used to
// gate OSC 8 hyperlinks: when output is piped or redirected the escape
// sequences are visible as garbage, so we strip them.
func stdoutIsTTY() bool {
	info, err := os.Stdout.Stat()
	if err != nil {
		return false
	}
	return (info.Mode() & os.ModeCharDevice) != 0
}

func reconcileLineCounterparty(ln OdooCacheLine, partners *odooPartnerIndex) string {
	if partners != nil && ln.PartnerID > 0 {
		if p, ok := partners.byID[ln.PartnerID]; ok {
			if p.Name != "" {
				return fmt.Sprintf("%s (#%d)", p.Name, ln.PartnerID)
			}
		}
	}
	if ln.PartnerID > 0 {
		return fmt.Sprintf("#%d (no name in local partner cache)", ln.PartnerID)
	}
	return "(no partner)"
}

func reconcileLineDescription(ln OdooCacheLine) string {
	if ln.PaymentRef != "" {
		return ln.PaymentRef
	}
	// Narration is often HTML or JSON-encoded; show the first 60 chars
	// only as a fallback when there's nothing better.
	if ln.Narration != "" {
		s := strings.ReplaceAll(ln.Narration, "\n", " ")
		if len(s) > 80 {
			s = s[:77] + "…"
		}
		return s
	}
	return ""
}

// dateDeltaDaysAbs returns |a - b| in days. Returns a large fallback when
// either date can't be parsed so unparseable rows sort last.
func dateDeltaDaysAbs(a, b string) int {
	const fallback = 1 << 30
	ta, err1 := parseOdooDate(a)
	tb, err2 := parseOdooDate(b)
	if err1 != nil || err2 != nil {
		return fallback
	}
	d := int(ta.Sub(tb).Hours() / 24)
	if d < 0 {
		d = -d
	}
	return d
}

func matchLineToCandidates(ln OdooCacheLine, all []reconcileCandidate, byPartner map[int][]reconcileCandidate, byAmount map[int64][]reconcileCandidate) []reconcileCandidate {
	amt := math.Abs(ln.Amount)
	ref := strings.ToLower(ln.PaymentRef)

	// Direction-gate: incoming bank lines (positive amount) match
	// customer invoices; outgoing lines match vendor bills. Crossing
	// directions never reconciles in real bookkeeping and just creates
	// noise in the preview.
	wantKind := "invoice"
	if ln.Amount < 0 {
		wantKind = "bill"
	}
	filter := func(in []reconcileCandidate) []reconcileCandidate {
		out := in[:0:0]
		for _, c := range in {
			if c.Kind == wantKind {
				out = append(out, c)
			}
		}
		return out
	}

	// 1. payment_ref contains the candidate number. Skip number tokens
	// that are short or purely digits — Odoo bill IDs sometimes leak
	// through as a 2-digit "number" (e.g. "40") which then matches every
	// payment_ref containing those digits ("140.00", "240 EUR donation",
	// etc.) and produces false positives.
	if ref != "" {
		var refHits []reconcileCandidate
		for _, c := range all {
			if c.Kind != wantKind || !isRefMatchable(c.Number) {
				continue
			}
			if strings.Contains(ref, strings.ToLower(c.Number)) {
				refHits = append(refHits, c)
			}
		}
		if len(refHits) > 0 {
			return refHits
		}
	}

	// 2. partner_id + amount.
	if ln.PartnerID > 0 {
		for _, c := range filter(byPartner[ln.PartnerID]) {
			if math.Abs(c.Residual-amt) < 0.005 {
				return []reconcileCandidate{c}
			}
		}
	}

	// 3. amount + partner-name substring in line ref.
	amountHits := filter(byAmount[centsKey(amt)])
	if ref != "" && len(amountHits) > 0 {
		var nameHits []reconcileCandidate
		for _, c := range amountHits {
			if c.PartnerName == "" {
				continue
			}
			for _, t := range strings.Fields(strings.ToLower(c.PartnerName)) {
				if len(t) >= 4 && strings.Contains(ref, t) {
					nameHits = append(nameHits, c)
					break
				}
			}
		}
		if len(nameHits) > 0 {
			return nameHits
		}
	}

	// 4 + 5. amount-only.
	return amountHits
}

func centsKey(v float64) int64 { return int64(math.Round(math.Abs(v) * 100)) }

// loadLocalOpenCandidates walks every month's private invoice + bill
// cache and returns records that are still open (state=posted,
// paymentState ≠ paid, residual > 0). Used by the matcher's normal
// auto-match path.
func loadLocalOpenCandidates() ([]reconcileCandidate, error) {
	return loadLocalCandidates(true)
}

// loadLocalAllPostedCandidates returns every posted invoice/bill —
// open AND closed (paid / in_payment / partial). Used by the
// interactive prompt's "top 5 by date" suggestion list so the operator
// can override an existing reconciliation when needed.
func loadLocalAllPostedCandidates() ([]reconcileCandidate, error) {
	return loadLocalCandidates(false)
}

func loadLocalCandidates(onlyOpen bool) ([]reconcileCandidate, error) {
	dataDir := DataDir()
	var out []reconcileCandidate

	walkPath := func(path string, kind string) {
		data, err := os.ReadFile(path)
		if err != nil {
			return
		}
		if kind == "invoice" {
			var file OdooOutgoingInvoicesPrivateFile
			if err := json.Unmarshal(data, &file); err != nil {
				return
			}
			for _, inv := range file.Invoices {
				if inv.State != "posted" {
					continue // drop drafts / cancellations
				}
				c := reconcileCandidate{
					ID:           inv.ID,
					Kind:         "invoice",
					Number:       firstNonEmpty(inv.Number, inv.Reference),
					Residual:     candidateResidual(inv.ResidualAmount, inv.TotalSignedAmount),
					PartnerID:    inv.Partner.ID,
					PartnerName:  firstNonEmpty(inv.Partner.DisplayName, inv.Partner.Name),
					Date:         firstNonEmpty(inv.InvoiceDate, inv.Date),
					State:        inv.State,
					PaymentState: inv.PaymentState,
				}
				if onlyOpen && !invoiceIsOpen(inv) {
					continue
				}
				out = append(out, c)
			}
			return
		}
		var file OdooVendorBillsPrivateFile
		if err := json.Unmarshal(data, &file); err != nil {
			return
		}
		for _, b := range file.Bills {
			if b.State != "posted" {
				continue
			}
			c := reconcileCandidate{
				ID:           b.ID,
				Kind:         "bill",
				Number:       firstNonEmpty(b.Number, b.Reference),
				Residual:     candidateResidual(b.ResidualAmount, b.TotalSignedAmount),
				PartnerID:    b.Partner.ID,
				PartnerName:  firstNonEmpty(b.Partner.DisplayName, b.Partner.Name),
				Date:         firstNonEmpty(b.InvoiceDate, b.Date),
				State:        b.State,
				PaymentState: b.PaymentState,
			}
			if onlyOpen && !invoiceIsOpen(b) {
				continue
			}
			out = append(out, c)
		}
	}

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
			walkPath(filepath.Join(dataDir, y.Name(), m.Name(), odoosource.PrivateRelPath(odoosource.InvoicesFile)), "invoice")
			walkPath(filepath.Join(dataDir, y.Name(), m.Name(), odoosource.PrivateRelPath(odoosource.BillsFile)), "bill")
		}
	}
	return out, nil
}

// candidateResidual returns the residual we should index for amount-based
// matching. Falls back to the invoice's total signed amount when Odoo
// reports residual=0 — typical for "in_payment" / "partial" states where
// the A/R side is internally settled but the bank line is still open.
func candidateResidual(residual, total float64) float64 {
	if math.Abs(residual) > 0.005 {
		return residual
	}
	return total
}

func invoiceIsOpen(inv OdooOutgoingInvoicePrivate) bool {
	if inv.State != "posted" {
		return false
	}
	if strings.EqualFold(inv.PaymentState, "paid") {
		return false
	}
	if math.Abs(inv.ResidualAmount) > 0.005 {
		return true
	}
	// "in_payment" and "partial" mean a payment has been registered
	// against the invoice's A/R line but the bank statement line hasn't
	// been reconciled yet — exactly what we want to surface. Odoo
	// reports residual=0 in those states (the A/R side is settled
	// internally) so the residual check above misses them; fall back to
	// the invoice's total amount instead.
	switch strings.ToLower(inv.PaymentState) {
	case "in_payment", "partial":
		return math.Abs(inv.TotalSignedAmount) > 0.005
	}
	return false
}

// invoiceCacheUpdate captures the post-write fields we refresh in the
// local private invoice/bill cache after a reconcile.
type invoiceCacheUpdate struct {
	State          string
	PaymentState   string
	ResidualAmount float64
}

// refreshTouchedInvoiceCache re-fetches payment_state + amount_residual
// for the given account.move ids and patches every local monthly cache
// file (public + private, invoices + bills) that contains them.
//
// Keeps `chb odoo journals N reconcile --dry-run` honest after a live
// reconcile run: invoices we just paid no longer surface as open
// candidates without needing a full `chb pull`.
func refreshTouchedInvoiceCache(creds *OdooCredentials, uid int, moveIDs []int) (int, error) {
	moveIDs = uniquePositiveInts(moveIDs)
	if len(moveIDs) == 0 {
		return 0, nil
	}
	rows, err := odooReadMapsByIDs(creds, uid, "account.move", moveIDs,
		[]string{"id", "state", "payment_state", "amount_residual", "amount_residual_signed"})
	if err != nil {
		return 0, fmt.Errorf("read updated moves: %v", err)
	}
	updates := map[int]invoiceCacheUpdate{}
	for _, r := range rows {
		id := odooInt(r["id"])
		if id == 0 {
			continue
		}
		residual := odooFloat(r["amount_residual_signed"])
		if math.Abs(residual) < 0.005 {
			residual = odooFloat(r["amount_residual"])
		}
		updates[id] = invoiceCacheUpdate{
			State:          odooString(r["state"]),
			PaymentState:   odooString(r["payment_state"]),
			ResidualAmount: residual,
		}
	}
	if len(updates) == 0 {
		return 0, nil
	}

	patched := 0
	dataDir := DataDir()
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
			invPath := filepath.Join(dataDir, y.Name(), m.Name(), odoosource.PrivateRelPath(odoosource.InvoicesFile))
			billPath := filepath.Join(dataDir, y.Name(), m.Name(), odoosource.PrivateRelPath(odoosource.BillsFile))
			patched += patchPrivateInvoiceCacheFile(invPath, updates, true)
			patched += patchPrivateInvoiceCacheFile(billPath, updates, false)
		}
	}
	return patched, nil
}

// patchPrivateInvoiceCacheFile rewrites a single monthly invoice or
// bill cache file in place, applying any updates whose move id appears
// in it. Returns the number of entries patched. Silently skips missing
// or unparseable files — the caller surveys the whole tree.
func patchPrivateInvoiceCacheFile(path string, updates map[int]invoiceCacheUpdate, isInvoice bool) int {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	if isInvoice {
		var file OdooOutgoingInvoicesPrivateFile
		if json.Unmarshal(data, &file) != nil {
			return 0
		}
		patched := 0
		for i := range file.Invoices {
			u, ok := updates[file.Invoices[i].ID]
			if !ok {
				continue
			}
			file.Invoices[i].State = u.State
			file.Invoices[i].PaymentState = u.PaymentState
			file.Invoices[i].ResidualAmount = u.ResidualAmount
			patched++
		}
		if patched == 0 {
			return 0
		}
		if buf, err := json.MarshalIndent(file, "", "  "); err == nil {
			_ = os.WriteFile(path, buf, 0o644)
		}
		return patched
	}
	var file OdooVendorBillsPrivateFile
	if json.Unmarshal(data, &file) != nil {
		return 0
	}
	patched := 0
	for i := range file.Bills {
		u, ok := updates[file.Bills[i].ID]
		if !ok {
			continue
		}
		file.Bills[i].State = u.State
		file.Bills[i].PaymentState = u.PaymentState
		file.Bills[i].ResidualAmount = u.ResidualAmount
		patched++
	}
	if patched == 0 {
		return 0
	}
	if buf, err := json.MarshalIndent(file, "", "  "); err == nil {
		_ = os.WriteFile(path, buf, 0o644)
	}
	return patched
}
