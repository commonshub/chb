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

// tokensContainSeq reports whether needle appears as a contiguous run of
// whole tokens in hay: "INV/2025/0001" is not found in "INV/2025/00010".
func tokensContainSeq(hay, needle []string) bool {
	if len(needle) == 0 || len(needle) > len(hay) {
		return false
	}
	for i := 0; i+len(needle) <= len(hay); i++ {
		ok := true
		for j, t := range needle {
			if hay[i+j] != t {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
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
	return tokensContainSeq(refTokens(memo), refTokens(ref))
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

// namedOpenDocument: an open invoice/bill a bank line names (memo
// reference or structured communication) while the line is still not
// matched to it.
type namedOpenDocument struct {
	Doc       reconcileCandidate
	JournalID int
	Line      OdooCacheLine
}

// findNamedOpenDocuments pairs open documents with the unmatched bank lines
// whose memo names them — what reconcile could not settle on its own
// (amounts differ, several documents named, a paid document named, …).
func findNamedOpenDocuments(open []reconcileCandidate, linesByJournal map[int][]OdooCacheLine) []namedOpenDocument {
	refIdx := buildRefMatchIndex(open)
	var out []namedOpenDocument
	jids := make([]int, 0, len(linesByJournal))
	for jid := range linesByJournal {
		jids = append(jids, jid)
	}
	sort.Ints(jids)
	for _, jid := range jids {
		for _, ln := range linesByJournal[jid] {
			if reconcileMatcherSkipsLine(ln) {
				continue
			}
			for _, c := range refHits(lineMemo(ln), ln.Amount, refIdx) {
				if c.IsOpen() {
					out = append(out, namedOpenDocument{Doc: c, JournalID: jid, Line: ln})
				}
			}
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Line.Date > out[j].Line.Date })
	return out
}

// printNamedOpenDocumentsReport closes a sync: open invoices/bills that a
// bank line names but that are still open, for the operator to settle
// (`chb odoo journals <id> reconcile -i`).
func printNamedOpenDocumentsReport(journalIDs []int) {
	open, _, err := loadLocalCandidatePartitions()
	if err != nil || len(open) == 0 {
		return
	}
	lines := map[int][]OdooCacheLine{}
	for _, jid := range journalIDs {
		if ls, ok := loadLatestOdooJournalLinesCache(jid); ok {
			lines[jid] = ls
		}
	}
	found := findNamedOpenDocuments(open, lines)
	if len(found) == 0 {
		return
	}
	fmt.Printf("\n  %s⚠ %s named on a bank line but still open:%s\n", Fmt.Yellow, Pluralize(len(found), "invoice/bill", "invoices/bills"), Fmt.Reset)
	const maxRows = 25
	for i, f := range found {
		if i == maxRows {
			fmt.Printf("    %s… and %d more%s\n", Fmt.Dim, len(found)-maxRows, Fmt.Reset)
			break
		}
		fmt.Printf("    %s %-18s %10s open  ← #%d line #%d %s %10s%s\n", Fmt.Dim, f.Doc.label(),
			formatBalancePlain(f.Doc.ResidualEUR, "EUR"), f.JournalID, f.Line.ID, f.Line.Date,
			formatBalancePlain(f.Line.Amount, "EUR"), Fmt.Reset)
	}
	fmt.Printf("    %sSettle them with %schb odoo journals <id> reconcile -i%s\n", Fmt.Dim, Fmt.Cyan, Fmt.Reset)
}
