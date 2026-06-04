package cmd

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	moneriumsource "github.com/CommonsHub/chb/providers/monerium"
)

// buildReconciledMoveIndex scans the local invoice + bill caches and maps each
// reconciled bank-line uniqueImportId to a human label for the move it is
// reconciled to ("CHB/2025/00163 — Partner"). Best-effort and offline: the
// public caches only carry the link for some moves, so a miss is common and the
// caller should fall back to the line's own memo/counterparty.
func buildReconciledMoveIndex() map[string]string {
	out := map[string]string{}
	dataDir := DataDir()
	for _, kind := range []moveKind{moveKindInvoice, moveKindBill} {
		_ = walkMoveMonths(dataDir, kind, func(year, month string) error {
			moves, err := loadMoves(dataDir, year, month, kind)
			if err != nil {
				return nil
			}
			partners := loadMovePartners(dataDir, year, month, kind)
			for _, mv := range moves {
				rt := mv.ReconciledTransaction
				if rt == nil || rt.ID == "" {
					continue
				}
				label := firstNonEmptyStr(mv.Title, fmt.Sprintf("#%d", mv.ID))
				if p := partners[mv.ID]; p != "" {
					label += " — " + p
				}
				out[rt.ID] = label
			}
			return nil
		})
	}
	return out
}

// Scoring weights for matching a bank line to an invoice/bill. Memo naming the
// invoice number is by far the strongest signal (it survives combined / partial
// transfers where the amount differs); exact amount is next; partner is a
// supporting signal used mostly for ranking. See SuggestBankLinesForMove.
const (
	scoreMemoFullRef = 100 // memo contains the full "YYYY/NNNNN"
	scoreMemoCounter = 55  // memo contains the bare NNNNN counter
	scoreExactAmount = 40
	scorePartner     = 20
)

// moneriumOrderIndex maps a lowercased on-chain tx hash to the Monerium order
// that settled it, so EURe bank lines (whose uniqueImportId embeds the tx hash)
// can be enriched with the counterpart IBAN + name + memo. Only orders from
// 2026-03 onward are cached locally; older lines simply get no IBAN.
type moneriumOrderIndex map[string]moneriumsource.Order

func loadMoneriumOrderIndex() moneriumOrderIndex {
	idx := moneriumOrderIndex{}
	patterns := []string{
		filepath.Join(DataDir(), "*", "*", "providers", moneriumsource.Source, "*.json"),
		filepath.Join(DataDir(), "latest", "providers", moneriumsource.Source, "*.json"),
	}
	for _, p := range patterns {
		files, _ := filepath.Glob(p)
		for _, f := range files {
			data, err := os.ReadFile(f)
			if err != nil {
				continue
			}
			var cache moneriumsource.CacheFile
			if json.Unmarshal(data, &cache) != nil {
				continue
			}
			for _, o := range cache.Orders {
				for _, h := range o.Meta.TxHashes {
					if h != "" {
						idx[strings.ToLower(h)] = o
					}
				}
			}
		}
	}
	return idx
}

func txHashFromImportID(uii string) string {
	for _, p := range strings.Split(uii, ":") {
		if strings.HasPrefix(p, "0x") && len(p) > 40 {
			return strings.ToLower(p)
		}
	}
	return ""
}

// enrich returns the counterpart IBAN / name / memo for a bank line, or empty
// strings when no Monerium order matches its tx hash.
func (idx moneriumOrderIndex) enrich(ln OdooCacheLine) (iban, name, memo string) {
	if idx == nil {
		return "", "", ""
	}
	o, ok := idx[txHashFromImportID(ln.UniqueImportID)]
	if !ok {
		return "", "", ""
	}
	name = o.Counterpart.Details.Name
	if name == "" {
		name = o.Counterpart.Details.CompanyName
	}
	return o.Counterpart.Identifier.IBAN, name, o.Memo
}

// invoiceRefTokens splits a move number like "CHB/2025/00204" into the full
// "2025/00204" tail (disambiguates across years) and the bare "00204" counter.
// Returns empty strings when the title isn't a clean number (e.g. a narration).
func invoiceRefTokens(title string) (fullRef, counter string) {
	title = strings.TrimSpace(title)
	segs := strings.Split(title, "/")
	if len(segs) < 2 {
		return "", ""
	}
	last := segs[len(segs)-1]
	if !isAllDigits(last) {
		return "", ""
	}
	return segs[len(segs)-2] + "/" + last, last
}

func isAllDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// scoredBankLine builds a scored Suggestion for one bank line against a move.
// memoHay is the lower-cased searchable text (paymentRef + narration + Monerium
// memo). Returns the populated Suggestion and its score; callers decide whether
// the score clears their floor.
func scoredBankLine(ln OdooCacheLine, jid int, journalName string, idx moneriumOrderIndex,
	amount float64, moveDate, fullRef, counter string,
	partnerID int, partnerTokens []string, partnerIdx *odooPartnerIndex) Suggestion {

	iban, mName, mMemo := idx.enrich(ln)
	memoHay := strings.ToLower(strings.Join([]string{ln.PaymentRef, ln.Narration, mMemo}, " "))

	var score int
	var reasons []string
	memoConfirmed := false
	switch {
	case fullRef != "" && strings.Contains(memoHay, strings.ToLower(fullRef)):
		score += scoreMemoFullRef
		memoConfirmed = true
		reasons = append(reasons, "memo names "+fullRef)
	case counter != "" && len(counter) >= 4 && strings.Contains(memoHay, counter):
		score += scoreMemoCounter
		reasons = append(reasons, "memo has "+counter)
	}
	exact := math.Abs(math.Abs(ln.Amount)-amount) < 0.01
	if exact {
		score += scoreExactAmount
		reasons = append(reasons, "exact amount")
	}
	partner := bankLineMatchesPartner(ln, partnerID, partnerTokens, partnerIdx)
	if !partner && mName != "" {
		low := strings.ToLower(mName)
		for _, t := range partnerTokens {
			if strings.Contains(low, t) {
				partner = true
				break
			}
		}
	}
	if partner {
		score += scorePartner
		reasons = append(reasons, "partner")
	}

	cpName := ""
	if partnerIdx != nil && ln.PartnerID > 0 {
		if p, ok := partnerIdx.byID[ln.PartnerID]; ok {
			cpName = p.Name
		}
	}
	if cpName == "" {
		cpName = mName
	}

	return Suggestion{
		Kind:            "bank-line",
		ID:              ln.ID,
		Date:            ln.Date,
		Amount:          math.Abs(ln.Amount),
		Currency:        "EUR",
		Partner:         cpName,
		PartnerID:       ln.PartnerID,
		Reference:       ln.PaymentRef,
		Description:     bankLineDescription(ln),
		Line:            ln,
		JournalID:       jid,
		JournalName:     journalName,
		PartnerMatch:    partner,
		DaysDelta:       dateDeltaDaysAbs(ln.Date, moveDate),
		AlreadyAttached: ln.IsReconciled,
		MatchScore:      score,
		MatchReason:     strings.Join(reasons, " · "),
		MemoConfirmed:   memoConfirmed,
		IBAN:            iban,
	}
}

// SuggestBankLinesForMove ranks bank-line candidates for an invoice/bill using
// the multi-signal score (memo > amount > partner) instead of an exact-amount
// gate. This surfaces combined/partial transfers whose memo names the invoice
// even when the amount differs. Reads only local caches.
//
// A candidate is kept when it clears a floor: memo-confirmed, exact amount, or
// a partner match within ~200 days. Sorted by score desc, then unreconciled
// before already-attached, then date proximity.
func SuggestBankLinesForMove(row moveRow, kind moveKind) []Suggestion {
	amount := row.Move.TotalAmount
	if amount <= 0 {
		return nil
	}
	dir := SuggestIncoming
	if kind.isBill {
		dir = SuggestOutgoing
	}
	moveDate := row.Move.Date
	fullRef, counter := invoiceRefTokens(row.Move.Title)

	partnerIdx := loadLatestOdooPartnerIndex(DataDir())
	partnerID := partnerIDForMoveRow(row, partnerIdx)
	partnerTokens := partnerNameTokens(row.Partner)
	mon := loadMoneriumOrderIndex()

	var out []Suggestion
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
			if isOdooSyntheticLine(ln) || ln.Amount == 0 || !bankLineDirectionMatches(ln, dir) {
				continue
			}
			s := scoredBankLine(ln, jid, journalName, mon, amount, moveDate, fullRef, counter, partnerID, partnerTokens, partnerIdx)
			keep := s.MemoConfirmed ||
				math.Abs(s.Amount-amount) < 0.01 ||
				(s.PartnerMatch && s.DaysDelta <= 200)
			if keep {
				out = append(out, s)
			}
		}
	}
	sortScoredSuggestions(out)
	// Cap the list: the highest-scored (memo / exact-amount) survive, so a
	// partner that tags hundreds of micro-transactions (e.g. the fridge) can't
	// flood the review. The search pool covers anything beyond the cap.
	const maxMoveCandidates = 25
	if len(out) > maxMoveCandidates {
		out = out[:maxMoveCandidates]
	}
	return out
}

// buildReconcileSearchPool returns every direction-matching bank line (enriched
// with IBAN / counterpart / memo), unreconciled first then by recency. This is
// the corpus the interactive search filters over when the operator wants to
// look beyond the auto-suggested candidates.
func buildReconcileSearchPool(kind moveKind) []Suggestion {
	dir := SuggestIncoming
	if kind.isBill {
		dir = SuggestOutgoing
	}
	partnerIdx := loadLatestOdooPartnerIndex(DataDir())
	mon := loadMoneriumOrderIndex()

	var out []Suggestion
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
			if isOdooSyntheticLine(ln) || ln.Amount == 0 || !bankLineDirectionMatches(ln, dir) {
				continue
			}
			// No move context here — score 0; just enrich for display/search.
			s := scoredBankLine(ln, jid, journalName, mon, -1, "", "", "", 0, nil, partnerIdx)
			out = append(out, s)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].AlreadyAttached != out[j].AlreadyAttached {
			return !out[i].AlreadyAttached // open first
		}
		return out[i].Date > out[j].Date // newest first
	})
	return out
}

// suggestionMatchesQuery reports whether a candidate matches a free-text search
// query: a leading numeric / comparison term filters by amount, otherwise the
// text is matched as a case-insensitive substring across counterpart name,
// IBAN, payment ref, narration, and journal.
func suggestionMatchesQuery(s Suggestion, query string) bool {
	q := strings.TrimSpace(strings.ToLower(query))
	if q == "" {
		return true
	}
	for _, term := range strings.Fields(q) {
		if !suggestionMatchesTerm(s, term) {
			return false
		}
	}
	return true
}

func suggestionMatchesTerm(s Suggestion, term string) bool {
	// Amount comparison terms: >100, <1000, >=50, =200, or a bare number.
	if op, val, ok := parseAmountTerm(term); ok {
		switch op {
		case ">":
			return s.Amount > val
		case "<":
			return s.Amount < val
		case ">=":
			return s.Amount >= val
		case "<=":
			return s.Amount <= val
		default: // "=" or bare number → match the amount as a substring too
			if math.Abs(s.Amount-val) < 0.01 {
				return true
			}
			// fall through to text match (e.g. "204" inside a ref)
		}
	}
	hay := strings.ToLower(strings.Join([]string{
		s.Partner, s.IBAN, s.Reference, s.Line.Narration, s.JournalName,
	}, " "))
	hay = strings.ReplaceAll(hay, " ", "")
	return strings.Contains(hay, strings.ReplaceAll(term, " ", ""))
}

func parseAmountTerm(term string) (op string, val float64, ok bool) {
	for _, o := range []string{">=", "<=", ">", "<", "="} {
		if strings.HasPrefix(term, o) {
			if v, err := parseFloatLoose(strings.TrimPrefix(term, o)); err == nil {
				return o, v, true
			}
			return "", 0, false
		}
	}
	if v, err := parseFloatLoose(term); err == nil {
		return "=", v, true
	}
	return "", 0, false
}

func parseFloatLoose(s string) (float64, error) {
	s = strings.TrimSpace(strings.ReplaceAll(s, ",", "."))
	return strconv.ParseFloat(s, 64)
}

// sortScoredSuggestions orders by score desc, then unreconciled before
// already-attached, then by date proximity (smaller delta first).
func sortScoredSuggestions(s []Suggestion) {
	sort.SliceStable(s, func(i, j int) bool {
		if s[i].MatchScore != s[j].MatchScore {
			return s[i].MatchScore > s[j].MatchScore
		}
		if s[i].AlreadyAttached != s[j].AlreadyAttached {
			return !s[i].AlreadyAttached
		}
		return s[i].DaysDelta < s[j].DaysDelta
	})
}

// matchConfidence maps a candidate's signals to a short label for the review UI.
func matchConfidence(s Suggestion) string {
	switch {
	case s.MemoConfirmed:
		return "high"
	case s.MatchScore >= scoreExactAmount:
		return "medium"
	case s.MatchScore > 0:
		return "low"
	default:
		return ""
	}
}
