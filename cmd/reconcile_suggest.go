package cmd

import (
	"math"
	"sort"
	"strings"
)

// SuggestDirection is the bank-line direction the suggester is looking
// for: incoming (positive bank amount, matches customer invoices) or
// outgoing (negative bank amount, matches vendor bills). It collapses
// both starting points — a moveKind on the invoice side and the sign of
// the bank line / tx on the journal side — onto the same axis so the
// inner algorithm has a single source of truth.
type SuggestDirection int

const (
	// SuggestIncoming targets positive-amount bank lines and customer
	// invoices (out_invoice).
	SuggestIncoming SuggestDirection = iota
	// SuggestOutgoing targets negative-amount bank lines and vendor
	// bills (in_invoice).
	SuggestOutgoing
)

// MoveKindKey returns "invoice" / "bill" matching the candidate Kind
// field used by reconcileCandidate. Keeps the direction <-> kind mapping
// in one place so the suggester and the picker can't drift.
func (d SuggestDirection) MoveKindKey() string {
	if d == SuggestOutgoing {
		return "bill"
	}
	return "invoice"
}

// Suggestion is one ranked candidate produced by SuggestForMove or
// SuggestForTx. Wraps the underlying record (bank line OR invoice/bill)
// plus scoring + status so the picker can render a uniform table no
// matter which direction the suggestion came from.
//
// Kind discriminates the payload:
//   - Kind == "bank-line": Line + JournalID + JournalName are populated;
//     suggestion was produced by SuggestForMove (the operator is
//     starting from an invoice/bill and picking a payment).
//   - Kind == "invoice" / "bill": Move is populated; suggestion was
//     produced by SuggestForTx (starting from a bank-side tx and
//     picking which invoice/bill it pays).
//
// The flat scalars (ID, Date, Amount, ...) are populated for both
// flavours so a picker can render a uniform table without unpacking the
// underlying record. Callers that need the full record (e.g. to call
// reconcileStatementLineWithMove or attachMoveToBankLine) still have
// Line / Move available.
type Suggestion struct {
	Kind string // "bank-line" / "invoice" / "bill"

	// Common scalar projection (populated for every Suggestion).
	ID           int     // statement line ID, or account.move ID
	Date         string  // YYYY-MM-DD
	Amount       float64 // absolute, EUR
	Currency     string
	Partner      string
	PartnerID    int
	Reference    string // payment_ref (bank-line) or move number (move)
	Description  string // payment_ref / first line item / move title
	PaymentState string // not_paid / in_payment / partial / paid (moves only)

	// Bank-line payload (Kind == "bank-line"). Zero-valued otherwise.
	Line        OdooCacheLine
	JournalID   int
	JournalName string

	// Move payload (Kind == "invoice" / "bill"). Zero-valued otherwise.
	Move reconcileCandidate

	// Scoring + status.
	PartnerMatch    bool // partner match against the query side
	DaysDelta       int  // absolute days between candidate and query date
	AlreadyAttached bool // bank line: IsReconciled; move: paymentState ∈ {paid,in_payment,partial}

	// Rich match metadata (populated by SuggestBankLinesForMove and the
	// reconcile search pool). MatchScore ranks the candidate; MemoConfirmed
	// is set when the bank-line memo names the invoice number (the strongest
	// signal). IBAN is the counterpart IBAN from the Monerium order cache,
	// when known (2026-03+). MatchReason is a human-readable "why".
	MatchScore    int
	MatchReason   string
	MemoConfirmed bool
	IBAN          string
}

// Direction returns the direction implied by the suggestion's payload
// so the picker doesn't have to thread the original argument through.
func (s Suggestion) Direction() SuggestDirection {
	if s.Kind == "bank-line" {
		if s.Line.Amount < 0 {
			return SuggestOutgoing
		}
		return SuggestIncoming
	}
	if s.Move.SignedTotal < 0 {
		return SuggestOutgoing
	}
	if s.Move.SignedTotal > 0 {
		return SuggestIncoming
	}
	if s.Move.Kind == "bill" {
		return SuggestOutgoing
	}
	return SuggestIncoming
}

// SuggestForMove returns ranked bank-line candidates for the given
// invoice/bill. Walks every linked-journal cache (allLinkedOdooJournalIDs)
// and applies the two-pass widening rule:
//
//   - Pass 1: amount + direction + UNRECONCILED only. If non-empty,
//     return that.
//   - Pass 2: same filter but including already-reconciled bank lines
//     (AlreadyAttached: true). Returned after pass 1 fails — the
//     interactive picker can then offer unreconcile + reattach via the
//     existing reconcileStatementLineWithMove fallback.
//
// Within each pass: partner-match first, then date proximity. The
// returned slice has un-reconciled candidates strictly before
// AlreadyAttached ones, so the picker can land the cursor on the first
// safe pick by default.
//
// Reads only local caches — no Odoo RPCs. Up-to-date results require a
// recent `chb pull`.
func SuggestForMove(row moveRow, kind moveKind) []Suggestion {
	amount := row.Move.TotalAmount
	if amount <= 0 {
		return nil
	}
	dir := SuggestIncoming
	if kind.isBill {
		dir = SuggestOutgoing
	}
	moveDate := row.Move.Date

	partnerIdx := loadLatestOdooPartnerIndex(DataDir())
	invoicePartnerID := partnerIDForMoveRow(row, partnerIdx)
	partnerTokens := partnerNameTokens(row.Partner)

	var open, attached []Suggestion
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
			if isOdooSyntheticLine(ln) || ln.Amount == 0 {
				continue
			}
			if !bankLineDirectionMatches(ln, dir) {
				continue
			}
			if math.Abs(math.Abs(ln.Amount)-amount) > 0.01 {
				continue
			}
			s := Suggestion{
				Kind:            "bank-line",
				ID:              ln.ID,
				Date:            ln.Date,
				Amount:          math.Abs(ln.Amount),
				Currency:        "EUR",
				PartnerID:       ln.PartnerID,
				Reference:       ln.PaymentRef,
				Description:     bankLineDescription(ln),
				Line:            ln,
				JournalID:       jid,
				JournalName:     journalName,
				PartnerMatch:    bankLineMatchesPartner(ln, invoicePartnerID, partnerTokens, partnerIdx),
				DaysDelta:       dateDeltaDaysAbs(ln.Date, moveDate),
				AlreadyAttached: ln.IsReconciled,
			}
			if partnerIdx != nil && ln.PartnerID > 0 {
				if p, ok := partnerIdx.byID[ln.PartnerID]; ok {
					s.Partner = p.Name
				}
			}
			if ln.IsReconciled {
				attached = append(attached, s)
			} else {
				open = append(open, s)
			}
		}
	}

	sortSuggestions(open)
	if len(open) > 0 {
		return open
	}
	sortSuggestions(attached)
	return attached
}

// SuggestForTx returns ranked invoice/bill candidates for the given
// bank-side tx. Inverse direction gate of SuggestForMove: incoming tx
// (positive SignedAmount) -> out_invoice candidates; outgoing tx
// (negative SignedAmount) -> in_invoice candidates.
//
// Two-pass widening:
//   - Pass 1: amount + direction + OPEN moves only. If non-empty,
//     return that.
//   - Pass 2: include posted moves regardless of payment_state. Paid /
//     in_payment / partial candidates get AlreadyAttached: true; the
//     picker badges them and Enter triggers unreconcile + reattach.
//
// Reads loadLocalCandidates(false) so the second pass surfaces paid
// invoices without an extra RPC.
func SuggestForTx(t incomeExpenseTx) []Suggestion {
	if t.Amount <= 0 {
		return nil
	}
	dir := SuggestIncoming
	if t.SignedAmount < 0 {
		dir = SuggestOutgoing
	}
	wantKind := dir.MoveKindKey()

	allCandidates, err := loadLocalCandidates(false)
	if err != nil || len(allCandidates) == 0 {
		return nil
	}

	tokens := partnerNameTokens(t.Counterparty)
	target := centsKey(t.Amount)

	var open, attached []Suggestion
	for _, c := range allCandidates {
		if c.Kind != wantKind {
			continue
		}
		if centsKey(c.Residual) != target && centsKey(c.SignedTotal) != target {
			continue
		}
		if !candidateSignMatches(c, t.SignedAmount) {
			continue
		}
		s := Suggestion{
			Kind:            c.Kind,
			ID:              c.ID,
			Date:            c.Date,
			Amount:          math.Abs(c.Residual),
			Currency:        "EUR",
			Partner:         c.PartnerName,
			PartnerID:       c.PartnerID,
			Reference:       c.Number,
			Description:     firstNonEmpty(c.FirstLineItem, c.Number),
			PaymentState:    c.PaymentState,
			Move:            c,
			PartnerMatch:    candidateMatchesTxPartner(c, t.Counterparty, tokens),
			DaysDelta:       dateDeltaDaysAbs(c.Date, t.Date),
			AlreadyAttached: !c.IsOpen(),
		}
		if s.Amount == 0 {
			s.Amount = math.Abs(c.SignedTotal)
		}
		if s.AlreadyAttached {
			attached = append(attached, s)
		} else {
			open = append(open, s)
		}
	}

	sortSuggestions(open)
	if len(open) > 0 {
		return open
	}
	sortSuggestions(attached)
	return attached
}

// bankLineDescription returns the most-readable text for a bank line —
// payment_ref when present, narration (truncated) otherwise.
func bankLineDescription(ln OdooCacheLine) string {
	if ln.PaymentRef != "" {
		return ln.PaymentRef
	}
	if ln.Narration != "" {
		s := strings.ReplaceAll(ln.Narration, "\n", " ")
		if len(s) > 80 {
			s = s[:77] + "…"
		}
		return s
	}
	return ""
}

// bankLineDirectionMatches reports whether the bank line's sign aligns
// with the suggester's direction: incoming wants Amount > 0, outgoing
// wants Amount < 0. Zero-amount lines are filtered out upstream.
func bankLineDirectionMatches(ln OdooCacheLine, dir SuggestDirection) bool {
	if dir == SuggestIncoming {
		return ln.Amount > 0
	}
	return ln.Amount < 0
}

// sortSuggestions applies the canonical two-tier ordering: partner-match
// first (true > false), then by absolute date proximity (smaller delta
// first). Stable so cache order survives ties — the picker reads
// deterministic.
func sortSuggestions(s []Suggestion) {
	sort.SliceStable(s, func(i, j int) bool {
		if s[i].PartnerMatch != s[j].PartnerMatch {
			return s[i].PartnerMatch
		}
		return s[i].DaysDelta < s[j].DaysDelta
	})
}

// FirstUnattachedIndex returns the index of the first suggestion in the
// slice whose AlreadyAttached flag is false, or 0 when none are open
// (or the slice is empty). Used by TUI pickers to land the cursor on
// the safest default — Enter on an open candidate is a plain reconcile,
// Enter on an attached one is an explicit override.
func FirstUnattachedIndex(s []Suggestion) int {
	for i, x := range s {
		if !x.AlreadyAttached {
			return i
		}
	}
	return 0
}

// SuggestionBadge returns the operator-visible status badge for the
// suggestion, suitable for rendering in a TUI cell. Empty when the
// candidate has no special status (open + no partner match). Combines
// the partner-match marker with the already-paid status line so the
// picker can show one column instead of juggling two.
func SuggestionBadge(s Suggestion) string {
	if s.AlreadyAttached {
		switch s.Kind {
		case "bank-line":
			// Bank line already attached to some other move — picking
			// will unreconcile the existing match and reattach here.
			return "already attached"
		default:
			line := candidateStatusLine(s.Move)
			if line == "" {
				// Fallback when the cache doesn't carry payment-state
				// detail; happens for older caches.
				return "already paid"
			}
			return strings.TrimSpace(stripANSIColors(line))
		}
	}
	if s.PartnerMatch {
		return "partner match"
	}
	return ""
}

// stripANSIColors removes the Fmt.Yellow / Fmt.Reset escapes from a
// candidateStatusLine so the resulting text fits cleanly in a TUI table
// cell that applies its own styling. Keeps the underlying string content
// (paid on DATE by NAME via JOURNAL ...) intact.
func stripANSIColors(s string) string {
	// candidateStatusLine wraps the whole string in Fmt.Yellow ... Fmt.Reset.
	// Strip those two only — keep any other escapes inside untouched so we
	// don't have to ship a full ANSI parser for what is in practice a
	// fixed-shape format.
	s = strings.ReplaceAll(s, Fmt.Yellow, "")
	s = strings.ReplaceAll(s, Fmt.Reset, "")
	return s
}
