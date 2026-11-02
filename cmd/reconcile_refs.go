package cmd

import (
	"fmt"
	"sort"
	"strings"
	"unicode"
)

// refTokens splits a reference or memo into lowercase runs of letters and
// runs of digits; everything else (spaces, slashes, underscores, dashes)
// separates. "2026_RCC_72", "2026/RCC/72" and "Factuur 2026 RCC 72" all
// carry [2026 rcc 72]; "2025Q2CHI0099" is [2025 q 2 chi 0099].
func refTokens(s string) []string {
	var out []string
	var cur []rune
	kind := 0 // 1 letters, 2 digits
	flush := func() {
		if len(cur) > 0 {
			out = append(out, string(cur))
			cur = cur[:0]
		}
	}
	for _, r := range strings.ToLower(s) {
		k := 0
		switch {
		case r >= '0' && r <= '9':
			k = 2
		case unicode.IsLetter(r):
			k = 1
		}
		if k == 0 || k != kind {
			flush()
		}
		kind = k
		if k != 0 {
			cur = append(cur, r)
		}
	}
	flush()
	return out
}

// seqIndex: the first index ≥ from where needle appears as a contiguous
// run of whole tokens in hay ("INV/2025/0001" is not in "INV/2025/00010"),
// or -1.
func seqIndex(hay, needle []string, from int) int {
	if len(needle) == 0 {
		return -1
	}
	for i := from; i+len(needle) <= len(hay); i++ {
		ok := true
		for j, t := range needle {
			if hay[i+j] != t {
				ok = false
				break
			}
		}
		if ok {
			return i
		}
	}
	return -1
}

// memoRef is a memo tokenised for reference lookups: its tokens, and for
// each token whether it continues the previous one within the same word
// (glued, or joined by - / _ .): "S00091-1" is [s 00091 1] with 00091
// and 1 joined, so the reference S00091 is not in it.
type memoRef struct {
	toks   []string
	joined []bool // joined[i]: toks[i] continues toks[i-1]
}

func newMemoRef(s string) memoRef {
	var m memoRef
	var cur []rune
	kind := 0
	gap := "" // separators since the last token
	start := true
	flush := func() {
		if len(cur) > 0 {
			m.toks = append(m.toks, string(cur))
			cur = cur[:0]
		}
	}
	for _, r := range strings.ToLower(s) {
		k := 0
		switch {
		case r >= '0' && r <= '9':
			k = 2
		case unicode.IsLetter(r):
			k = 1
		}
		if k == 0 {
			flush()
			gap += string(r)
			kind = 0
			continue
		}
		if k != kind {
			flush()
			m.joined = append(m.joined, !start && (gap == "" || gap == "-" || gap == "/" || gap == "_" || gap == "."))
			gap = ""
		}
		start = false
		kind = k
		cur = append(cur, r)
	}
	flush()
	return m
}

// names reports whether the memo names ref: its tokens appear in order and
// the run is not continued by another token of the same word ("S00091"
// is not named by "S00091-1"; "2026_RCC_72" is named by "Factuur 2026 RCC
// 72," and by "Invoice 2026.RCC.72").
func (m memoRef) names(ref []string) bool {
	for i := seqIndex(m.toks, ref, 0); i >= 0; i = seqIndex(m.toks, ref, i+1) {
		end := i + len(ref)
		if end < len(m.toks) && m.joined[end] {
			continue
		}
		return true
	}
	return false
}

// refMentioned reports whether memo names ref (an invoice/bill number, a
// vendor reference, a sale order), ignoring case and separators. Refs too
// short or purely numeric are never matched (see isRefMatchable).
func refMentioned(memo, ref string) bool {
	ref = strings.TrimSpace(ref)
	if !isRefMatchable(ref) {
		return false
	}
	return newMemoRef(memo).names(refTokens(ref))
}

// structuredComms returns the 12 digits of every Belgian structured
// communication (+++xxx/xxxx/xxxxx+++, with or without the +++ and
// slashes) whose mod-97 check holds.
func structuredComms(text string) []string {
	var out []string
	for _, m := range structuredCommunication.FindAllStringSubmatch(text, -1) {
		digits := m[1] + m[2] + m[3]
		var base int64
		for _, c := range digits[:10] {
			base = base*10 + int64(c-'0')
		}
		check := int64((digits[10]-'0')*10 + (digits[11] - '0'))
		want := base % 97
		if want == 0 {
			want = 97
		}
		if base > 0 && check == want {
			out = append(out, digits)
		}
	}
	return out
}

// structuredComm returns a document's payment_reference as 12 digits when
// it is a valid structured communication, else "".
func structuredComm(paymentReference string) string {
	if cs := structuredComms(paymentReference); len(cs) == 1 {
		return cs[0]
	}
	return ""
}

// documentRefs lists every reference a bank memo may use for a document:
// its number, the vendor reference (ref), a free-text payment reference
// and each sale order of its origin ("S00176").
//
// A vendor reference, free-text payment reference or origin counts only
// when it is distinctive (see distinctiveRef): "2025/Q1" or "commons hub"
// would name every quarterly transfer or every rent payment.
func documentRefs(number, ref, paymentReference, origin string) []string {
	var out []string
	add := func(s string) {
		s = strings.TrimSpace(s)
		if !isRefMatchable(s) {
			return
		}
		for _, o := range out {
			if strings.EqualFold(o, s) {
				return
			}
		}
		out = append(out, s)
	}
	add(number)
	if distinctiveRef(ref) {
		add(ref)
	}
	if structuredComm(paymentReference) == "" && distinctiveRef(paymentReference) {
		add(paymentReference)
	}
	for _, o := range strings.Split(origin, ",") {
		if distinctiveRef(o) {
			add(o)
		}
	}
	return out
}

// distinctiveRef: the reference carries a number that is not just a year
// or a quarter — a run of 5+ digits, or of 3+ digits that is not a year.
func distinctiveRef(s string) bool {
	for _, t := range refTokens(s) {
		if t[0] < '0' || t[0] > '9' {
			continue
		}
		if len(t) >= 5 {
			return true
		}
		if len(t) >= 3 && !(len(t) == 4 && (strings.HasPrefix(t, "19") || strings.HasPrefix(t, "20"))) {
			return true
		}
	}
	return false
}

// lineMemo is everything a bank line says about what it pays: the payment
// reference plus the narration's free-text fields.
func lineMemo(ln OdooCacheLine) string {
	parts := []string{ln.PaymentRef}
	for _, k := range []string{"memo", "description", "fullDescription", "stripeDescription"} {
		if v := metaString(ln.Metadata, k); v != "" {
			parts = append(parts, v)
		}
	}
	return strings.Join(parts, " ")
}

// namedDocument: a document a bank line's memo names while the two are not
// matched together.
type namedDocument struct {
	Doc       reconcileCandidate
	JournalID int
	Line      OdooCacheLine
	// Why: "open" — the line is unmatched and the document still open;
	// "paid" — the line is unmatched but the document was settled by
	// another payment (a double payment, or that other match is wrong);
	// "elsewhere" — the line is matched, yet the document its memo names
	// is still open (the line probably pays the wrong document).
	Why string
}

// findNamedDocuments lists what reconcile could not settle on its own.
func findNamedDocuments(all []reconcileCandidate, linesByJournal map[int][]OdooCacheLine) []namedDocument {
	refIdx := buildRefMatchIndex(all)
	var out []namedDocument
	jids := make([]int, 0, len(linesByJournal))
	for jid := range linesByJournal {
		jids = append(jids, jid)
	}
	sort.Ints(jids)
	for _, jid := range jids {
		for _, ln := range linesByJournal[jid] {
			if isOdooSyntheticLine(ln) || ln.Amount == 0 {
				continue
			}
			matched := bankLineMatchedToDocument(ln)
			for _, c := range refHits(lineMemo(ln), ln.Amount, refIdx) {
				why := ""
				switch {
				case c.AwaitsBankLine():
					// A registered payment waits for its bank line:
					// reconcile settles it.
				case matched && c.IsOpen():
					why = "elsewhere"
				case !matched && c.IsOpen():
					why = "open"
				case !matched && strings.EqualFold(c.PaymentState, "paid"):
					why = "paid"
				}
				if why != "" {
					out = append(out, namedDocument{Doc: c, JournalID: jid, Line: ln, Why: why})
				}
			}
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Line.Date > out[j].Line.Date })
	return out
}

// printNamedDocumentsReport closes a sync: bank lines whose memo names an
// invoice/bill they are not matched with, for the operator to settle
// (`chb odoo journals <id> reconcile -i`). Batch reconcile never breaks an
// existing match, so these stay until someone decides.
func printNamedDocumentsReport(journalIDs []int) {
	_, all, err := loadLocalCandidatePartitions()
	if err != nil || len(all) == 0 {
		return
	}
	lines := map[int][]OdooCacheLine{}
	for _, jid := range journalIDs {
		if ls, ok := loadLatestOdooJournalLinesCache(jid); ok {
			lines[jid] = ls
		}
	}
	found := findNamedDocuments(all, lines)
	if len(found) == 0 {
		return
	}
	label := map[string]string{
		"open":      "still open",
		"paid":      "paid by another payment",
		"elsewhere": "still open, line already matched (to another document, or partly)",
	}
	fmt.Printf("\n  %s⚠ %s whose memo names an invoice/bill they are not matched with:%s\n", Fmt.Yellow, Pluralize(len(found), "bank line", ""), Fmt.Reset)
	const maxRows = 25
	for i, f := range found {
		if i == maxRows {
			fmt.Printf("    %s… and %d more%s\n", Fmt.Dim, len(found)-maxRows, Fmt.Reset)
			break
		}
		fmt.Printf("    %s#%d line #%d %s %10s → %-18s %s%s\n", Fmt.Dim, f.JournalID, f.Line.ID, f.Line.Date,
			formatBalancePlain(f.Line.Amount, "EUR"), f.Doc.label(), label[f.Why], Fmt.Reset)
	}
	fmt.Printf("    %sSettle them with %schb odoo journals <id> reconcile -i%s\n", Fmt.Dim, Fmt.Cyan, Fmt.Reset)
}
