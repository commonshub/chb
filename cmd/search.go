package cmd

import (
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// SearchItem is one record in the unified search index — a transaction, an
// invoice, or a bill — projected onto a common shape so `chb search` can match
// and render them uniformly.
type SearchItem struct {
	Kind          string  `json:"kind"` // "tx" | "invoice" | "bill"
	Date          string  `json:"date"`
	Amount        float64 `json:"amount"` // signed: + income, - expense
	Currency      string  `json:"currency"`
	Reference     string  `json:"reference"`               // move number / paymentRef
	Communication string  `json:"communication,omitempty"` // structured comm (+++…+++)
	Counterparty  string  `json:"counterparty,omitempty"`
	IBAN          string  `json:"iban,omitempty"`
	Memo          string  `json:"memo,omitempty"`
	Account       string  `json:"account,omitempty"` // account/journal, or move state
	ID            string  `json:"id"`

	hay string // precomputed lowercase haystack of every text field
}

// buildSearchIndex loads transactions, invoices and bills into one searchable
// slice. Read-only; reads local caches only.
func buildSearchIndex() []SearchItem {
	var items []SearchItem
	mon := loadMoneriumOrderIndex()

	for _, tx := range loadAllTransactions("") {
		date := ""
		if tx.Timestamp > 0 {
			date = time.Unix(tx.Timestamp, 0).In(BrusselsTZ()).Format("2006-01-02")
		}
		cp := txDisplayCounterparty(tx)
		memo := txDisplayDescription(tx)
		ref := ""
		if tx.Metadata != nil {
			if v, ok := tx.Metadata["paymentRef"].(string); ok {
				ref = v
			}
		}
		iban := ""
		if tx.TxHash != "" {
			if o, ok := mon[strings.ToLower(tx.TxHash)]; ok {
				iban = o.Counterpart.Identifier.IBAN
				if cp == "" {
					cp = o.Counterpart.Details.Name
				}
				if memo == "" {
					memo = o.Memo
				}
			}
		}
		it := SearchItem{
			Kind: "tx", Date: date, Amount: txAmount(tx), Currency: tx.Currency,
			Reference: ref, Communication: ref, Counterparty: cp, IBAN: iban,
			Memo: memo, Account: firstNonEmptyStr(tx.AccountName, tx.AccountSlug), ID: tx.ID,
		}
		it.hay = strings.ToLower(strings.Join([]string{txSearchableText(tx), iban, ref, cp, memo, it.Account}, " "))
		items = append(items, it)
	}

	addMoves := func(kind moveKind, k string) {
		rows, _ := loadMoveRows(kind, "", "")
		for _, r := range rows {
			m := r.Move
			it := SearchItem{
				Kind: k, Date: m.Date, Amount: moveSignedAmount(m, kind.isBill), Currency: firstNonEmptyStr(m.Currency, "EUR"),
				Reference: moveReference(m), Communication: r.Reference, Counterparty: r.Partner,
				IBAN: r.IBAN, Memo: moveFirstLineItem(m),
				Account: strings.TrimSpace(m.State + "/" + m.PaymentState),
				ID:      fmt.Sprintf("%s #%d", k, m.ID),
			}
			it.hay = strings.ToLower(strings.Join([]string{
				moveReference(m), r.Reference, r.Partner, r.IBAN,
				moveLineItemHaystack(m), m.Category, m.Collective,
			}, " "))
			items = append(items, it)
		}
	}
	addMoves(moveKindInvoice, "invoice")
	addMoves(moveKindBill, "bill")
	return items
}

// moveSignedAmount returns the income(+) / expense(-) signed total for a move.
// The base sign comes from the move KIND (invoice = income, bill = expense) so
// it's correct even when MoveType is missing from older caches; a credit note
// (refund) flips it. isMoveCreditNote falls back to the R-prefixed number when
// MoveType is empty.
func moveSignedAmount(m OdooOutgoingInvoicePublic, isBill bool) float64 {
	sign := 1.0
	if isBill {
		sign = -1
	}
	if isMoveCreditNote(m) {
		sign = -sign
	}
	return sign * math.Abs(m.TotalAmount)
}

// searchTerm is one parsed query token.
type searchTerm struct {
	text      string   // lowercase substring to match (empty for signed-amount terms)
	hasAmount bool     // token parsed as a number
	signed    bool     // an explicit +/- was given → amount direction matters
	sign      float64  // +1 / -1 when signed
	amount    float64  // magnitude, rounded to cents
	kinds     []string // a "kind:" filter (OR over tx/invoice/bill); empty otherwise
}

// canonicalSearchKind normalises a kind alias to "tx" | "invoice" | "bill".
func canonicalSearchKind(s string) string {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "tx", "txn", "transaction", "transactions":
		return "tx"
	case "inv", "invoice", "invoices":
		return "invoice"
	case "bill", "bills", "vendorbill", "vendorbills":
		return "bill"
	}
	return strings.ToLower(strings.TrimSpace(s))
}

var searchAmountRE = regexp.MustCompile(`^([+-]?)(\d+(?:[.,]\d+)?)$`)

func parseSearchTerms(q string) []searchTerm {
	var out []searchTerm
	for _, raw := range strings.Fields(q) {
		// kind:tx / kind:invoice,bill — restrict to record type(s).
		if low := strings.ToLower(raw); strings.HasPrefix(low, "kind:") {
			t := searchTerm{}
			for _, k := range strings.Split(low[len("kind:"):], ",") {
				if ck := canonicalSearchKind(k); ck != "" {
					t.kinds = append(t.kinds, ck)
				}
			}
			out = append(out, t)
			continue
		}
		t := searchTerm{text: strings.ToLower(raw)}
		if mm := searchAmountRE.FindStringSubmatch(raw); mm != nil {
			val, err := strconv.ParseFloat(strings.Replace(mm[2], ",", ".", 1), 64)
			if err == nil {
				t.hasAmount = true
				t.amount = math.Round(val*100) / 100
				switch mm[1] {
				case "+":
					t.signed, t.sign, t.text = true, 1, "" // sign given → amount-only
				case "-":
					t.signed, t.sign, t.text = true, -1, ""
				default:
					t.text = mm[2] // bare number may also substring-match (e.g. "204")
				}
			}
		}
		out = append(out, t)
	}
	return out
}

func centsEq(a, b float64) bool { return math.Abs(a-b) < 0.005 }

func (t searchTerm) matches(it SearchItem) bool {
	if len(t.kinds) > 0 {
		for _, k := range t.kinds {
			if k == it.Kind {
				return true
			}
		}
		return false
	}
	if t.hasAmount {
		mag := math.Round(math.Abs(it.Amount)*100) / 100
		if t.signed {
			if (t.sign > 0 && it.Amount > 0) || (t.sign < 0 && it.Amount < 0) {
				if centsEq(mag, t.amount) {
					return true
				}
			}
			return false // a signed amount term is amount-only
		}
		if centsEq(mag, t.amount) {
			return true
		}
		// unsigned number: fall back to text match (e.g. "00204" in a reference)
	}
	return t.text != "" && strings.Contains(it.hay, t.text)
}

func itemMatchesSearch(it SearchItem, terms []searchTerm) bool {
	for _, t := range terms {
		if !t.matches(it) {
			return false
		}
	}
	return true
}

// searchScore ranks a matching item for relevance: exact reference / amount
// matches float to the top, then newest first (handled by the date sort).
func searchScore(it SearchItem, terms []searchTerm) int {
	s := 0
	refLow := strings.ToLower(it.Reference)
	for _, t := range terms {
		if t.hasAmount {
			if centsEq(math.Round(math.Abs(it.Amount)*100)/100, t.amount) {
				s += 3
			}
		}
		if t.text != "" {
			if refLow == t.text {
				s += 4
			} else if strings.Contains(refLow, t.text) {
				s += 2
			}
		}
	}
	return s
}

func sortSearchResults(items []SearchItem, terms []searchTerm) {
	sort.SliceStable(items, func(i, j int) bool {
		si, sj := searchScore(items[i], terms), searchScore(items[j], terms)
		if si != sj {
			return si > sj
		}
		return items[i].Date > items[j].Date // newest first
	})
}

// Search is the `chb search [keywords] [-i] [-n N]` entry point.
func Search(args []string) error {
	if HasFlag(args, "--help", "-h", "help") {
		printSearchHelp()
		return nil
	}
	interactive := HasFlag(args, "-i", "--interactive")
	limit := GetNumber(args, []string{"-n", "--limit"}, 0)
	query := strings.Join(searchKeywords(args), " ")
	// --kind tx|invoice|bill (comma-separated) is sugar for a leading kind: term.
	if k := GetOption(args, "--kind"); strings.TrimSpace(k) != "" {
		query = strings.TrimSpace("kind:" + k + " " + query)
	}

	index := buildSearchIndex()

	if interactive {
		runSearchTUI(index, query)
		return nil
	}

	if strings.TrimSpace(query) == "" {
		printSearchHelp()
		return nil
	}

	terms := parseSearchTerms(query)
	var results []SearchItem
	for _, it := range index {
		if itemMatchesSearch(it, terms) {
			results = append(results, it)
		}
	}
	sortSearchResults(results, terms)
	if limit > 0 && len(results) > limit {
		results = results[:limit]
	}

	if JSONMode(args) {
		return EmitJSON(results)
	}
	printSearchResults(results, query, limit)
	return nil
}

// searchKeywords extracts the free-text/amount query tokens from args, skipping
// the command's own flags. Amount tokens like "-50.12" / "+100" start with a
// sign but are NOT flags, so only the known flags are stripped.
func searchKeywords(args []string) []string {
	valueFlags := map[string]bool{"-n": true, "--limit": true, "--kind": true}
	boolFlags := map[string]bool{
		"-i": true, "--interactive": true, "--json": true,
		"--text": true, "--pretty": true,
	}
	var out []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		if valueFlags[a] {
			i++ // skip the value
			continue
		}
		if boolFlags[a] {
			continue
		}
		out = append(out, a)
	}
	return out
}

// searchAmountCell renders the signed amount with an explicit +/- so the
// income/expense direction is visible (fmtAmountCurrency shows magnitude only).
func searchAmountCell(it SearchItem) string {
	sign := "+"
	if it.Amount < 0 {
		sign = "-"
	}
	cur := it.Currency
	if cur == "" {
		cur = "EUR"
	}
	return sign + fmtNumber(math.Abs(it.Amount)) + " " + cur
}

func searchKindLabel(k string) string {
	switch k {
	case "tx":
		return "tx"
	case "invoice":
		return "inv"
	case "bill":
		return "bill"
	}
	return k
}

func printSearchResults(results []SearchItem, query string, limit int) {
	f := Fmt
	if len(results) == 0 {
		fmt.Printf("\n%sNo matches for %q.%s\n\n", f.Dim, query, f.Reset)
		return
	}
	headers := []string{"Kind", "Date", "Amount", "Counterparty", "Reference", "Memo"}
	rightAlign := map[int]bool{2: true}
	caps := []int{4, 10, 13, 26, 20, 34}

	rows := make([][]string, 0, len(results))
	for _, it := range results {
		rows = append(rows, []string{
			searchKindLabel(it.Kind),
			it.Date,
			searchAmountCell(it),
			Truncate(firstNonEmptyStr(it.Counterparty, it.Account), caps[3]),
			Truncate(firstNonEmptyStr(it.Reference, it.Communication), caps[4]),
			Truncate(it.Memo, caps[5]),
		})
	}

	widths := make([]int, len(headers))
	for i, h := range headers {
		widths[i] = displayWidth(h)
	}
	for _, r := range rows {
		for i, c := range r {
			if w := displayWidth(c); w > widths[i] {
				widths[i] = w
			}
		}
	}
	for i := range widths {
		if widths[i] > caps[i] {
			widths[i] = caps[i]
		}
	}
	render := func(cells []string, dim bool) string {
		parts := make([]string, len(cells))
		for i, c := range cells {
			if rightAlign[i] {
				parts[i] = padLeft(c, widths[i])
			} else {
				parts[i] = padRight(c, widths[i])
			}
		}
		line := "  " + strings.Join(parts, "  ")
		if dim {
			return cpTUIDimStyle.Render(line)
		}
		return line
	}

	fmt.Printf("\n%s🔎 %d match(es) for %q%s\n\n", f.Bold, len(results), query, f.Reset)
	fmt.Println(render(headers, true))
	for _, r := range rows {
		fmt.Println(render(r, false))
	}
	if limit > 0 {
		fmt.Printf("\n%s(limited to %d — raise with -n or drop it for all)%s\n", f.Dim, limit, f.Reset)
	}
	fmt.Println()
}

func printSearchHelp() {
	f := Fmt
	fmt.Printf(`
%schb search%s — Spotlight search across transactions, invoices and bills

%sUSAGE%s
  %schb search%s <keywords…> [-i] [-n N]

%sMATCHES (case-insensitive, all terms must match)%s
  • reference / number (CHB…, MEM/2025/…, vendor ref)
  • structured communication (+++…+++)
  • counterparty name or IBAN
  • memo / description / line item
  • amount — bare number matches the magnitude either way (%s100%s = ±100);
    add a sign for direction: %s+100%s = income of 100, %s-50.12%s = expense of 50.12
  • kind — %skind:invoice%s / %skind:bill%s / %skind:tx%s (comma-separate for several,
    e.g. %skind:tx,invoice%s). Works in the query and in the TUI; or use %s--kind%s.

%sOPTIONS%s
  %s-i%s, %s--interactive%s   Open the spotlight TUI (live filter as you type)
  %s--kind%s <k>          Restrict to tx / invoice / bill (comma-separated)
  %s-n%s N, %s--limit%s N     Show at most N results (%s-n 1%s = top hit only)
  %s--json%s               Output JSON (auto when piped)
  %s--help, -h%s           Show this help

%sEXAMPLES%s
  %schb search citizen wallet%s          # text across all records
  %schb search +1815%s                   # income of exactly 1815
  %schb search "in progress" -50.12%s    # expense of 50.12 from that counterparty
  %schb search BE07 7340%s               # by (partial) IBAN
  %schb search CHB/2025/00204 -n 1%s     # the single best hit
  %schb search -i%s                      # open the spotlight
`,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset, f.Cyan, f.Reset, f.Cyan, f.Reset, // amount bullet
		f.Cyan, f.Reset, f.Cyan, f.Reset, f.Cyan, f.Reset, f.Cyan, f.Reset, f.Cyan, f.Reset, // kind bullet
		f.Bold, f.Reset, // OPTIONS
		f.Yellow, f.Reset, f.Yellow, f.Reset, // -i / --interactive
		f.Yellow, f.Reset, // --kind
		f.Yellow, f.Reset, f.Yellow, f.Reset, f.Cyan, f.Reset, // -n / --limit / -n 1
		f.Yellow, f.Reset, // --json
		f.Yellow, f.Reset, // --help
		f.Bold, f.Reset,
		f.Dim, f.Reset,
		f.Dim, f.Reset,
		f.Dim, f.Reset,
		f.Dim, f.Reset,
		f.Dim, f.Reset,
		f.Dim, f.Reset,
	)
}
