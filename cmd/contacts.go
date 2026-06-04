package cmd

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// Contacts is the `chb contacts [keyword]` entry point. With no/ambiguous match
// it lists matching contacts; with exactly one match it shows that contact's
// detail with all invoices, bills and transactions aggregated across its merge
// group (including pending, not-yet-pushed merges).
func Contacts(args []string) error {
	if HasFlag(args, "--help", "-h", "help") {
		printContactsHelp()
		return nil
	}
	terms := lowerTerms(searchKeywords(args))

	ctx := loadContactsContext(false)
	summaries := ctx.allSummaries()

	var matches []contactSummary
	for _, s := range summaries {
		if contactSummaryMatches(s, terms) {
			matches = append(matches, s)
		}
	}
	sort.Slice(matches, func(i, j int) bool {
		return strings.ToLower(matches[i].Partner.Name) < strings.ToLower(matches[j].Partner.Name)
	})

	if len(matches) == 0 {
		fmt.Printf("\n%sNo contacts match %q.%s\n\n", Fmt.Dim, strings.Join(terms, " "), Fmt.Reset)
		return nil
	}
	if len(matches) == 1 {
		ctx.ensureTxs()
		printContactDetail(ctx.view(matches[0].Partner.ID))
		return nil
	}
	if JSONMode(args) {
		return EmitJSON(matches)
	}
	printContactsList(matches, terms)
	return nil
}

func lowerTerms(words []string) []string {
	out := make([]string, 0, len(words))
	for _, w := range words {
		if w = strings.ToLower(strings.TrimSpace(w)); w != "" {
			out = append(out, w)
		}
	}
	return out
}

func contactSummaryMatches(s contactSummary, terms []string) bool {
	if len(terms) == 0 {
		return true
	}
	p := s.Partner
	hay := strings.ToLower(strings.Join(append([]string{
		p.Name, p.Email, p.VAT, p.Phone, p.Mobile,
	}, s.IBANs...), " "))
	hayNoSpace := strings.ReplaceAll(hay, " ", "")
	for _, t := range terms {
		tns := strings.ReplaceAll(t, " ", "")
		if !strings.Contains(hay, t) && !strings.Contains(hayNoSpace, tns) {
			return false
		}
	}
	return true
}

func printContactsList(matches []contactSummary, terms []string) {
	f := Fmt
	fmt.Printf("\n%s👤 %d contact(s) matching %q%s\n\n", f.Bold, len(matches), strings.Join(terms, " "), f.Reset)
	headers := []string{"ID", "Name", "Email", "Inv", "Bill", "IBAN"}
	caps := []int{7, 30, 30, 4, 4, 22}
	rightAlign := map[int]bool{3: true, 4: true}
	rows := make([][]string, 0, len(matches))
	for _, s := range matches {
		merged := ""
		if s.GroupSize > 1 {
			merged = fmt.Sprintf(" (+%d)", s.GroupSize-1)
		}
		rows = append(rows, []string{
			fmt.Sprintf("%d", s.Partner.ID),
			Truncate(s.Partner.Name+merged, caps[1]),
			Truncate(s.Partner.Email, caps[2]),
			fmt.Sprintf("%d", s.NumInvoice),
			fmt.Sprintf("%d", s.NumBill),
			Truncate(firstIBAN(s.IBANs), caps[5]),
		})
	}
	fmt.Println(renderPlainTable(headers, rows, caps, rightAlign))
	fmt.Printf("\n%sRefine the keyword to a single match to see full detail.%s\n\n", f.Dim, f.Reset)
}

func firstIBAN(ibans []string) string {
	if len(ibans) > 0 {
		return ibans[0]
	}
	return ""
}

func printContactDetail(v ContactView) {
	f := Fmt
	p := v.Partner
	fmt.Printf("\n%s👤 %s%s  %s(id %d)%s\n", f.Bold, firstNonEmptyStr(p.Name, "(unknown)"), f.Reset, f.Dim, p.ID, f.Reset)
	kv := func(k, val string) {
		if strings.TrimSpace(val) == "" {
			return
		}
		fmt.Printf("  %s%-12s%s %s\n", f.Dim, k+":", f.Reset, val)
	}
	kv("Email", strings.Join(v.Emails, ", "))
	kv("VAT", p.VAT)
	kv("Phone", firstNonEmptyStr(p.Phone, p.Mobile))
	kv("IBAN", strings.Join(v.IBANs, ", "))
	if len(v.GroupIDs) > 1 {
		var names []string
		for _, m := range v.GroupIDs {
			if m == p.ID {
				continue
			}
			names = append(names, fmt.Sprintf("#%d", m))
		}
		note := fmt.Sprintf("%d contacts merged (%s)", len(v.GroupIDs), strings.Join(names, ", "))
		if len(v.PendingMerges) > 0 {
			note += f.Yellow + "  [pending push]" + f.Reset
		}
		kv("Merged", note)
	}

	printMoveBlock("Invoices", v.Invoices)
	printMoveBlock("Bills", v.Bills)

	if len(v.Txs) > 0 {
		fmt.Printf("\n  %sTransactions (%d)%s\n", f.Bold, len(v.Txs), f.Reset)
		headers := []string{"Date", "Account", "Amount", "Counterparty", "Memo"}
		caps := []int{10, 14, 13, 22, 30}
		rightAlign := map[int]bool{2: true}
		rows := make([][]string, 0, len(v.Txs))
		shown := v.Txs
		if len(shown) > 30 {
			shown = shown[:30]
		}
		for _, tx := range shown {
			rows = append(rows, []string{
				txDateString(tx),
				Truncate(firstNonEmptyStr(tx.AccountName, tx.AccountSlug), caps[1]),
				signedTxAmountCell(tx),
				Truncate(txDisplayCounterparty(tx), caps[3]),
				Truncate(txDisplayDescription(tx), caps[4]),
			})
		}
		fmt.Println(renderPlainTable(headers, rows, caps, rightAlign))
		if len(v.Txs) > 30 {
			fmt.Printf("  %s… and %d more%s\n", f.Dim, len(v.Txs)-30, f.Reset)
		}
	}
	fmt.Println()
}

func printMoveBlock(title string, rows []moveRow) {
	if len(rows) == 0 {
		return
	}
	f := Fmt
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].Move.Date > rows[j].Move.Date })
	fmt.Printf("\n  %s%s (%d)%s  %s— total %s%s\n", f.Bold, title, len(rows), f.Reset, f.Dim, fmtEUR(moveRowsTotal(rows)), f.Reset)
	headers := []string{"Date", "Reference", "Amount", "State", "Description"}
	caps := []int{10, 20, 13, 16, 30}
	rightAlign := map[int]bool{2: true}
	cells := make([][]string, 0, len(rows))
	for _, r := range rows {
		cells = append(cells, []string{
			r.Move.Date,
			Truncate(moveReference(r.Move), caps[1]),
			fmtAmountCurrency(r.Move.TotalAmount, r.Move.Currency),
			Truncate(strings.TrimSpace(r.Move.State+"/"+r.Move.PaymentState), caps[3]),
			Truncate(moveFirstLineItem(r.Move), caps[4]),
		})
	}
	fmt.Println(renderPlainTable(headers, cells, caps, rightAlign))
}

// txDateString / signedTxAmountCell are small shims so contacts detail doesn't
// depend on the transactions-browser internals.
func txDateString(tx TransactionEntry) string {
	if tx.Timestamp <= 0 {
		return ""
	}
	return time.Unix(tx.Timestamp, 0).In(BrusselsTZ()).Format("2006-01-02")
}

func signedTxAmountCell(tx TransactionEntry) string {
	a := txAmount(tx)
	sign := "+"
	if a < 0 {
		sign = "-"
	}
	cur := tx.Currency
	if cur == "" {
		cur = "EUR"
	}
	return sign + fmtNumber(absFloat(a)) + " " + cur
}

// renderPlainTable renders a simple aligned table (header dim, body plain) with
// per-column caps and right-alignment. Shared by the contacts views.
func renderPlainTable(headers []string, rows [][]string, caps []int, rightAlign map[int]bool) string {
	widths := make([]int, len(headers))
	for i, h := range headers {
		widths[i] = displayWidth(h)
	}
	for _, r := range rows {
		for i, c := range r {
			if i < len(widths) {
				if w := displayWidth(c); w > widths[i] {
					widths[i] = w
				}
			}
		}
	}
	for i := range widths {
		if i < len(caps) && widths[i] > caps[i] {
			widths[i] = caps[i]
		}
	}
	line := func(cells []string, dim bool) string {
		parts := make([]string, len(cells))
		for i, c := range cells {
			if rightAlign[i] {
				parts[i] = padLeft(c, widths[i])
			} else {
				parts[i] = padRight(c, widths[i])
			}
		}
		s := "  " + strings.Join(parts, "  ")
		if dim {
			return cpTUIDimStyle.Render(s)
		}
		return s
	}
	var b strings.Builder
	b.WriteString(line(headers, true))
	for _, r := range rows {
		b.WriteString("\n")
		b.WriteString(line(r, false))
	}
	return b.String()
}

func printContactsHelp() {
	f := Fmt
	fmt.Printf(`
%schb contacts%s — Look up a contact and everything attached to it

%sUSAGE%s
  %schb contacts%s <keyword…>

Matches contacts by name, email, VAT or IBAN (case-insensitive). With one match
it shows the contact plus all invoices, bills and transactions aggregated across
its merge group — including merges recorded locally but not yet pushed. With
several matches it lists them; refine the keyword to drill in.

%sSEE ALSO%s
  %schb odoo contacts merge%s   Select duplicates and merge them (recorded locally)
  %schb search%s <keyword>      Spotlight across individual records
`,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
	)
}
