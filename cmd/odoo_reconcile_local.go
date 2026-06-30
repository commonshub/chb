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
	ID                 int    // Odoo account.move id — primary dedupe key
	Kind               string // "invoice" or "bill"
	Number             string
	Residual           float64
	SignedTotal        float64 // total_signed (signed; negative for credit notes / refunds)
	PartnerID          int
	PartnerName        string
	Date               string
	State              string // Odoo move state ("posted", "draft", ...)
	PaymentState       string // "not_paid" / "in_payment" / "partial" / "paid"
	LastPayment        string // most recent payment date (YYYY-MM-DD), empty when none
	LastPaymentBy      string // journal name of the latest payment (e.g. "Stripe"), empty when none
	LastPaymentPayer   string // partner name from account.payment (who actually paid)
	LastPaymentAccount string // IBAN / acc_number the payment was made from
	FirstLineItem      string // first non-section line item title — preview in -i list
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
// reconcileMatcherSkipsLine reports whether the reconcile matcher should ignore a
// cached bank line. Synthetic and zero-amount lines are always skipped. A
// reconciled line is skipped ONLY when it's truly matched to an invoice/bill (its
// counterpart is on A/R or A/P) — re-proposing those churns against Odoo's
// "already reconciled" error. A line merely categorized to an income/expense
// account is still offered: attaching it to an invoice/bill is always preferred
// over a bare category, and the apply phase rewrites its counterpart to the
// receivable/payable account. Lines whose counterpart type is unknown (a cache
// written before the field existed) keep the old conservative behaviour — skip
// when reconciled — until the next `chb odoo pull` populates the type.
func reconcileMatcherSkipsLine(ln OdooCacheLine) bool {
	if isOdooSyntheticLine(ln) || ln.Amount == 0 {
		return true
	}
	return ln.IsReconciled && !reconcileCounterpartIsCategorized(ln.CounterpartType)
}

// reconcileCounterpartIsCategorized reports whether a reconciled bank line's
// counterpart sits on an income/expense account — i.e. the line was merely
// categorized, not matched to an invoice/bill. Such lines are still offered to
// the reconcile matcher (invoice attachment is always preferred). Receivable /
// payable counterparts (real matches) and unknown types (old cache) return false
// so they keep being skipped.
func reconcileCounterpartIsCategorized(accountType string) bool {
	switch accountType {
	case "income", "income_other", "expense", "expense_direct_cost", "expense_depreciation":
		return true
	}
	return false
}

// bankLineMatchedToDocument reports whether a bank line is genuinely matched to
// an invoice/bill — its counterpart sits on a receivable/payable account — as
// opposed to merely categorized to a GL income/expense account. Only a real
// document match should trigger the "already attached" warning + reattach flow:
// a categorized line is the normal input to invoice/bill reconciliation, so
// attaching it must NOT warn. A reconciled line whose counterpart type is
// unknown (cache written before the field existed) is treated conservatively as
// a match until the next `chb odoo pull` populates the type.
func bankLineMatchedToDocument(ln OdooCacheLine) bool {
	return ln.IsReconciled && !reconcileCounterpartIsCategorized(ln.CounterpartType)
}

func computeReconcileMatches(journalID int, interactive bool) (*reconcileMatchSet, *odooPartnerIndex, error) {
	Progress("loading journal-lines cache")
	lines, ok := loadLatestOdooJournalLinesCache(journalID)
	if !ok {
		return nil, nil, fmt.Errorf("no local cache for journal #%d — run `chb odoo pull` first", journalID)
	}
	// Strategy 1 (ref-substring match) also looks at paid/in_payment
	// candidates: the bank line's payment_ref carries the invoice
	// number regardless of whether the invoice was already settled by
	// another (potentially wrong) reconciliation. Picking it triggers
	// unreconcile+reattach in the apply phase — the operator's
	// surgical override stays trustworthy.
	Progress("scanning local invoices/bills")
	candidates, allCandidates, err := loadLocalCandidatePartitions()
	if err != nil {
		return nil, nil, err
	}
	// Partner index (optional) so the interactive prompt can show the
	// bank line's counterparty name, not just the integer id.
	partnerIdx := loadLatestOdooPartnerIndex(DataDir())
	if len(candidates) == 0 && len(allCandidates) == 0 {
		return &reconcileMatchSet{DemotedToDuplicate: map[int]bool{}}, partnerIdx, errNoLocalCandidates
	}

	byPartner := map[int][]reconcileCandidate{}
	byAmount := map[int64][]reconcileCandidate{}
	for _, c := range candidates {
		byPartner[c.PartnerID] = append(byPartner[c.PartnerID], c)
		byAmount[centsKey(c.Residual)] = append(byAmount[centsKey(c.Residual)], c)
	}
	// Precompute the ref-matchable subset (numbers with at least one
	// non-digit, length ≥5) with lowercased Number once. The matcher's
	// strategy-1 loop otherwise re-filters + ToLower'd this on every
	// bank line — ~lines × allPosted allocations.
	refMatchable := buildRefMatchIndex(allCandidates)

	// Phase 1: per-line matching (each line independent). Ambiguous hits
	// are sorted by date proximity to the bank line — closest first —
	// so the interactive prompt's first suggestion is the most likely.
	Progress(fmt.Sprintf("matching %d lines against %d open / %d posted candidates",
		len(lines), len(candidates), len(allCandidates)))
	results := make([]reconcileLineMatch, 0, len(lines))
	for _, ln := range lines {
		if reconcileMatcherSkipsLine(ln) {
			continue
		}
		hits := matchLineToCandidates(ln, candidates, refMatchable, byPartner, byAmount)
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
		resolveAmbiguousInteractively(journalID, results, partnerIdx)
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
// surfaces per ambiguous line. Eight is comfortable: enough to cover
// the legitimate alternatives across open/in_payment/paid status
// without falling off the screen, and keeps the single-keystroke menu
// (1-9) usable.
const interactiveSuggestionCount = 8

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
func resolveAmbiguousInteractively(journalID int, results []reconcileLineMatch, partners *odooPartnerIndex) {
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
	// top-N-by-date alternatives, even ones already reconciled.
	allCandidates, _ := loadLocalAllPostedCandidates()
	byAmountAll := map[int64][]reconcileCandidate{}
	for _, c := range allCandidates {
		byAmountAll[centsKey(c.Residual)] = append(byAmountAll[centsKey(c.Residual)], c)
	}

	// Index the linked account's local transactions by unique_import_id
	// so we can render a "Local tx:" preview alongside the Odoo bank
	// line — counterparty name + IBAN/address from the source-of-truth
	// (Monerium memo, KBC counterparty, Stripe customer), which is
	// usually richer than what made it onto the Odoo bank line.
	localByImportID := loadLocalTxIndexForJournal(journalID)

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
		if localCp, localURI := localTxPreview(localByImportID, ln.UniqueImportID); localCp != "" || localURI != "" {
			if localCp != "" {
				fmt.Printf("        %sLocal tx:%s     %s\n", Fmt.Dim, Fmt.Reset, localCp)
			}
			if localURI != "" {
				fmt.Printf("        %sURI:%s          %s\n", Fmt.Dim, Fmt.Reset, localURI)
			}
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
			// Row 1: [N] date (delta)   Partner   firstLine   ref   amount
			// ref hyperlinks to the Odoo form, "invoice"/"bill" prefix
			// removed (operator can tell from context).
			fmt.Printf("        [%d] %s %s%-18s%s  %-28s  %-30s  %s  %s\n",
				k+1,
				c.Date,
				Fmt.Dim, "("+rel+")", Fmt.Reset,
				truncate(c.PartnerName, 28),
				truncate(c.FirstLineItem, 30),
				formatCandidateRefLink(c, odooBaseURL, 18),
				formatBalancePlain(c.Residual, "EUR"))
			// Row 2: optional status line for paid / in_payment / partial.
			if badge := candidateStatusLine(c); badge != "" {
				fmt.Printf("            %s\n", badge)
			}
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
			suffix := ""
			if line := candidateStatusLine(hits[0]); line != "" {
				suffix = "  " + line
			}
			fmt.Printf("        %s↳ accepted [1] %s %s%s%s\n",
				Fmt.Dim, hits[0].Kind, hits[0].label(), suffix, Fmt.Reset)
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
		// Direction-gate by signed total: incoming bank lines should
		// only show invoices the customer owes us (positive signed
		// total), outgoing bank lines should only show bills / credit
		// notes we owe (negative signed total). SignedTotal == 0
		// falls through (some imports leave it blank).
		if !candidateSignMatches(c, ln.Amount) {
			continue
		}
		matches = append(matches, c)
	}
	sort.SliceStable(matches, func(i, j int) bool {
		return dateDeltaDaysAbs(matches[i].Date, ln.Date) < dateDeltaDaysAbs(matches[j].Date, ln.Date)
	})

	// Date-direction filter:
	//   - Outgoing (bill payment) → bill must be dated BEFORE the bank
	//     line; we can't pay a bill that hasn't been issued yet. Keep
	//     past candidates only.
	//   - Incoming (invoice receipt) → prefer past invoices (customer
	//     paid an outstanding invoice) but allow up to 3 future ones
	//     (sometimes we issue the invoice after receiving payment).
	const futureCap = 3
	out := make([]reconcileCandidate, 0, len(matches))
	future := 0
	for _, c := range matches {
		isFuture := c.Date > ln.Date
		if isFuture {
			if ln.Amount < 0 {
				continue // outgoing → no future bills
			}
			if future >= futureCap {
				continue
			}
			future++
		}
		out = append(out, c)
		if len(out) >= limit {
			break
		}
	}
	return out
}

// candidateSignMatches reports whether the candidate's signed total
// aligns with the bank line's direction. SignedTotal == 0 is treated
// as a wildcard (some imports leave it blank).
func candidateSignMatches(c reconcileCandidate, lineAmount float64) bool {
	if c.SignedTotal == 0 {
		return true
	}
	return (lineAmount > 0 && c.SignedTotal > 0) || (lineAmount < 0 && c.SignedTotal < 0)
}

// candidateLastActivity returns the most recent payment-related date
// + the journal that booked it + the payer's name and account/IBAN
// (when the enriched fields are populated; older caches won't have
// payer info — that comes from a fresh `chb odoo invoices sync`).
// Falls back to writeDate when Payments is empty but the state is
// paid / partial / in_payment.
func candidateLastActivity(payments []OdooInvoicePayment, writeDate, paymentState string) (date, journal, payer, account string) {
	for _, p := range payments {
		d := strings.TrimSpace(p.Date)
		if len(d) >= 10 {
			d = d[:10]
		}
		if d > date {
			date = d
			journal = p.Journal
			payer = p.PayerName
			account = p.PayerAccount
		}
	}
	if date != "" {
		return date, journal, payer, account
	}
	switch strings.ToLower(paymentState) {
	case "paid", "in_payment", "partial":
		d := strings.TrimSpace(writeDate)
		if len(d) >= 10 {
			return d[:10], "", "", ""
		}
	}
	return "", "", "", ""
}

// loadFirstLineItemIndex walks every monthly PUBLIC invoice + bill
// cache and builds an id → "first line item title" map. The private
// projection drops LineItems, but the public file keeps them — used
// to render an extra preview line under each candidate in -i mode.
func loadFirstLineItemIndex(dataDir string) map[int]string {
	out := map[int]string{}
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
			loadFirstLineItemsFromFile(filepath.Join(dataDir, y.Name(), m.Name(), odoosource.RelPath(odoosource.InvoicesFile)), true, out)
			loadFirstLineItemsFromFile(filepath.Join(dataDir, y.Name(), m.Name(), odoosource.RelPath(odoosource.BillsFile)), false, out)
		}
	}
	return out
}

func loadFirstLineItemsFromFile(path string, isInvoice bool, out map[int]string) {
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	var entries []OdooOutgoingInvoicePublic
	if isInvoice {
		var file OdooOutgoingInvoicesFile
		if json.Unmarshal(data, &file) != nil {
			return
		}
		entries = file.Invoices
	} else {
		var file OdooVendorBillsFile
		if json.Unmarshal(data, &file) != nil {
			return
		}
		entries = file.Bills
	}
	for _, e := range entries {
		if _, exists := out[e.ID]; exists {
			continue
		}
		for _, li := range e.LineItems {
			// Skip section / note rows that Odoo uses for layout, not
			// real line items. "product" / "" are the real ones.
			switch strings.ToLower(li.DisplayType) {
			case "line_section", "line_note":
				continue
			}
			if title := strings.TrimSpace(li.Title); title != "" {
				out[e.ID] = title
				break
			}
			if title := strings.TrimSpace(li.ProductName); title != "" {
				out[e.ID] = title
				break
			}
		}
	}
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

// candidateStatusLine returns the dedicated second-line status string
// for a non-open candidate. Prefers the actual payer's name + IBAN
// when the cache has them; falls back to the booking journal otherwise.
// Format:
//   "paid on DATE by NAME (IBAN) via JOURNAL (will unreconcile + reattach)"
// — with each segment dropped when its data is missing.
func candidateStatusLine(c reconcileCandidate) string {
	state := strings.ToLower(c.PaymentState)
	if state == "" || state == "not_paid" {
		return ""
	}
	var sb strings.Builder
	sb.WriteString(Fmt.Yellow)
	switch state {
	case "paid":
		sb.WriteString("paid")
	case "in_payment":
		sb.WriteString("in payment")
	case "partial":
		sb.WriteString("partially paid")
	default:
		sb.WriteString(state)
	}
	if c.LastPayment != "" {
		sb.WriteString(" on ")
		sb.WriteString(c.LastPayment)
	}
	switch {
	case c.LastPaymentPayer != "" && c.LastPaymentAccount != "":
		sb.WriteString(" by ")
		sb.WriteString(c.LastPaymentPayer)
		sb.WriteString(" (")
		sb.WriteString(c.LastPaymentAccount)
		sb.WriteString(")")
	case c.LastPaymentPayer != "":
		sb.WriteString(" by ")
		sb.WriteString(c.LastPaymentPayer)
	case c.LastPaymentAccount != "":
		sb.WriteString(" from ")
		sb.WriteString(c.LastPaymentAccount)
	}
	if c.LastPaymentBy != "" {
		sb.WriteString(" via ")
		sb.WriteString(c.LastPaymentBy)
	}
	if state == "paid" || state == "partial" {
		sb.WriteString(" (will unreconcile + reattach)")
	}
	sb.WriteString(Fmt.Reset)
	return sb.String()
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

// refMatchEntry is a pre-filtered + pre-lowercased candidate entry used
// by matchLineToCandidates strategy 1. Built once per match run via
// buildRefMatchIndex so the hot loop doesn't re-filter or re-lowercase
// the same allPosted list ~once per bank line.
type refMatchEntry struct {
	Kind        string
	NumberLower string
	Cand        reconcileCandidate
}

func buildRefMatchIndex(allPosted []reconcileCandidate) []refMatchEntry {
	out := make([]refMatchEntry, 0, len(allPosted))
	for _, c := range allPosted {
		if !isRefMatchable(c.Number) {
			continue
		}
		out = append(out, refMatchEntry{
			Kind:        c.Kind,
			NumberLower: strings.ToLower(c.Number),
			Cand:        c,
		})
	}
	return out
}

func matchLineToCandidates(ln OdooCacheLine, openCandidates []reconcileCandidate, refIndex []refMatchEntry, byPartner map[int][]reconcileCandidate, byAmount map[int64][]reconcileCandidate) []reconcileCandidate {
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
	//
	// Searches *all* posted candidates (open + paid + in_payment): if
	// the bank line's payment_ref names a specific invoice, we want to
	// flag it as a match even when the invoice was already (perhaps
	// wrongly) reconciled by something else. The apply phase falls
	// back to unreconcile+reattach for paid candidates, so the
	// operator's surgical override stays trustworthy.
	if ref != "" {
		var refHits []reconcileCandidate
		seen := map[int]bool{}
		for _, e := range refIndex {
			if e.Kind != wantKind {
				continue
			}
			if seen[e.Cand.ID] {
				continue
			}
			if strings.Contains(ref, e.NumberLower) {
				refHits = append(refHits, e.Cand)
				seen[e.Cand.ID] = true
			}
		}
		if len(refHits) > 0 {
			return refHits
		}
	}
	// Amount-based strategies (2/3/4) below stay open-only via
	// byPartner / byAmount — those are pre-indexed from openCandidates
	// by the caller, so paid invoices don't pollute amount fallbacks.
	_ = openCandidates

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

// loadLocalCandidatePartitions does ONE walk of the data dir and
// returns both the open-only and all-posted candidate slices the
// matcher needs. The old code called loadLocalOpenCandidates +
// loadLocalAllPostedCandidates back-to-back, doubling disk + parse
// work and re-walking the public side for firstLineByID twice — on a
// few years of monthly buckets that adds up to many extra seconds.
func loadLocalCandidatePartitions() ([]reconcileCandidate, []reconcileCandidate, error) {
	dataDir := DataDir()
	firstLineByID := loadFirstLineItemIndex(dataDir)
	var open, all []reconcileCandidate

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
					continue
				}
				date, journal, payer, account := candidateLastActivity(inv.Payments, inv.WriteDate, inv.PaymentState)
				c := reconcileCandidate{
					ID:                 inv.ID,
					Kind:               "invoice",
					Number:             firstNonEmpty(inv.Number, inv.Reference),
					Residual:           candidateResidual(inv.ResidualAmount, inv.TotalSignedAmount),
					SignedTotal:        inv.TotalSignedAmount,
					PartnerID:          inv.Partner.ID,
					PartnerName:        firstNonEmpty(inv.Partner.DisplayName, inv.Partner.Name),
					Date:               firstNonEmpty(inv.InvoiceDate, inv.Date),
					State:              inv.State,
					PaymentState:       inv.PaymentState,
					LastPayment:        date,
					LastPaymentBy:      journal,
					LastPaymentPayer:   payer,
					LastPaymentAccount: account,
					FirstLineItem:      firstLineByID[inv.ID],
				}
				all = append(all, c)
				if invoiceIsOpen(inv) {
					open = append(open, c)
				}
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
			date, journal, payer, account := candidateLastActivity(b.Payments, b.WriteDate, b.PaymentState)
			c := reconcileCandidate{
				ID:                 b.ID,
				Kind:               "bill",
				Number:             firstNonEmpty(b.Number, b.Reference),
				Residual:           candidateResidual(b.ResidualAmount, b.TotalSignedAmount),
				SignedTotal:        b.TotalSignedAmount,
				PartnerID:          b.Partner.ID,
				PartnerName:        firstNonEmpty(b.Partner.DisplayName, b.Partner.Name),
				Date:               firstNonEmpty(b.InvoiceDate, b.Date),
				State:              b.State,
				PaymentState:       b.PaymentState,
				LastPayment:        date,
				LastPaymentBy:      journal,
				LastPaymentPayer:   payer,
				LastPaymentAccount: account,
				FirstLineItem:      firstLineByID[b.ID],
			}
			all = append(all, c)
			if invoiceIsOpen(b) {
				open = append(open, c)
			}
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
	return open, all, nil
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
	// First line items live on the PUBLIC invoice/bill files only —
	// the private projection drops them. Walk public alongside so the
	// interactive prompt can show "what's actually on the invoice"
	// next to each candidate. Map keyed by account.move id.
	firstLineByID := loadFirstLineItemIndex(dataDir)
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
				date, journal, payer, account := candidateLastActivity(inv.Payments, inv.WriteDate, inv.PaymentState)
				c := reconcileCandidate{
					ID:                 inv.ID,
					Kind:               "invoice",
					Number:             firstNonEmpty(inv.Number, inv.Reference),
					Residual:           candidateResidual(inv.ResidualAmount, inv.TotalSignedAmount),
					SignedTotal:        inv.TotalSignedAmount,
					PartnerID:          inv.Partner.ID,
					PartnerName:        firstNonEmpty(inv.Partner.DisplayName, inv.Partner.Name),
					Date:               firstNonEmpty(inv.InvoiceDate, inv.Date),
					State:              inv.State,
					PaymentState:       inv.PaymentState,
					LastPayment:        date,
					LastPaymentBy:      journal,
					LastPaymentPayer:   payer,
					LastPaymentAccount: account,
					FirstLineItem:      firstLineByID[inv.ID],
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
			date, journal, payer, account := candidateLastActivity(b.Payments, b.WriteDate, b.PaymentState)
			c := reconcileCandidate{
				ID:                 b.ID,
				Kind:               "bill",
				Number:             firstNonEmpty(b.Number, b.Reference),
				Residual:           candidateResidual(b.ResidualAmount, b.TotalSignedAmount),
				SignedTotal:        b.TotalSignedAmount,
				PartnerID:          b.Partner.ID,
				PartnerName:        firstNonEmpty(b.Partner.DisplayName, b.Partner.Name),
				Date:               firstNonEmpty(b.InvoiceDate, b.Date),
				State:              b.State,
				PaymentState:       b.PaymentState,
				LastPayment:        date,
				LastPaymentBy:      journal,
				LastPaymentPayer:   payer,
				LastPaymentAccount: account,
				FirstLineItem:      firstLineByID[b.ID],
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

// loadLocalTxIndexForJournal joins the bank statement lines in the
// journal cache to the source-of-truth local transactions
// (loadAccountTransactionsForOdoo) via unique_import_id. Returns an
// index the interactive prompt uses to surface counterparty + IBAN /
// wallet address from the local cache, which is usually richer than
// the bank line on Odoo (Monerium-only memo, KBC counterparty name,
// Stripe customer name, etc.). Returns nil when no account is linked
// to the journal — the caller treats nil as "no extra info".
func loadLocalTxIndexForJournal(journalID int) map[string]TransactionEntry {
	acc := linkedAccountForJournal(journalID)
	if acc == nil {
		return nil
	}
	local := loadAccountTransactionsForOdoo(acc)
	if len(local) == 0 {
		return nil
	}
	out := make(map[string]TransactionEntry, len(local))
	for _, tx := range local {
		if id := buildUniqueImportID(acc, tx); id != "" {
			out[id] = tx
		}
	}
	return out
}

// localTxPreview returns two strings extracted from the local tx
// keyed by unique_import_id: a "counterparty + IBAN/address" row and
// the canonical tx URI (tx.ID — ethereum:100:tx:0x… / stripe:txn_…).
// Either may be empty when the local cache has no matching record or
// no enrichment for that field.
func localTxPreview(index map[string]TransactionEntry, importID string) (counterpartyLine, uriLine string) {
	if index == nil || importID == "" {
		return "", ""
	}
	tx, ok := index[importID]
	if !ok {
		return "", ""
	}
	// Counterparty: prefer the name (Monerium memo / Stripe customer /
	// KBC counterparty), append the IBAN or wallet address in dim.
	var cp string
	if tx.Counterparty != "" {
		cp = tx.Counterparty
	}
	if iban := stringMetadata(tx.Metadata, "iban"); iban != "" {
		if cp != "" {
			cp += "  " + Fmt.Dim + "(" + iban + ")" + Fmt.Reset
		} else {
			cp = iban
		}
	} else if addr := localTxAddress(tx); addr != "" {
		if cp != "" {
			cp += "  " + Fmt.Dim + "(" + addr + ")" + Fmt.Reset
		} else {
			cp = addr
		}
	}
	// URI row: the canonical tx identifier (ethereum:100:tx:0x… or
	// stripe:txn_…). Full form, no truncation — operators copy-paste
	// it into other chb commands (rules add, etc.).
	uri := tx.ID
	if uri == "" {
		uri = tx.ProviderID
	}
	return cp, uri
}

// localTxAddress pulls the most useful address-shaped identifier off
// the local tx: from/to wallet address for blockchain, or the raw
// CounterpartyId URI for everything else. Empty when nothing fits.
func localTxAddress(tx TransactionEntry) string {
	for _, key := range []string{"from", "to"} {
		if v, ok := tx.Metadata[key].(string); ok && v != "" && !strings.HasPrefix(v, "0x000000000000") {
			return v
		}
	}
	if strings.HasPrefix(tx.CounterpartyID, "ethereum:") || strings.HasPrefix(tx.CounterpartyID, "stripe:") {
		return tx.CounterpartyID
	}
	return ""
}
