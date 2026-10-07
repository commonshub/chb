package cmd

// Categories from Odoo, the consolidated source of truth. A transaction
// that rules and processors left uncategorised takes its category from how
// its bank statement line is booked in Odoo:
//
//   - reconciled with an invoice or a bill (counterpart on a receivable or
//     payable): the category of that document's lines — the line's analytic
//     category, else its product (categories.json "products"), else its GL
//     account; the documents' URIs go to metadata.documents;
//   - not (yet) reconciled, but paid with the Belgian structured
//     communication of one of our invoices (+++000/0044/21681+++, the move
//     id and its mod-97 check): that invoice's category, the same way;
//   - booked straight to an account (580000 internal transfer, 451200 VAT,
//     455000 salaries, 61xx…): the category whose PCMN prefix matches the
//     account (categories.json "accounts", longest prefix wins).
//
// The opening-balance row a bank journal starts with is not a transaction:
// category opening_balance, type INTERNAL.
//
// Runs at generate time on the local mirror (journal lines,
// statement-matches.json, chart, bills and invoices) after the rules and
// after the Odoo mapping, so a category taken from Odoo is never pushed back
// to Odoo. metadata.categorySource = "odoo" marks it.

import (
	"crypto/sha256"
	"math"
	"encoding/hex"
	"fmt"
	"hash"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
)

var (
	odooTxCategorizerMu    sync.Mutex
	odooTxCategorizerCache = map[string]*odooTxCategorizer{}
)

// odooTxCategorizerFor builds the index once per data dir and process
// (generate --history runs it for every month).
func odooTxCategorizerFor(dataDir string) *odooTxCategorizer {
	odooTxCategorizerMu.Lock()
	defer odooTxCategorizerMu.Unlock()
	if o, ok := odooTxCategorizerCache[dataDir]; ok {
		return o
	}
	o := newOdooTxCategorizer(dataDir)
	odooTxCategorizerCache[dataDir] = o
	return o
}

type odooTxCategorizer struct {
	lineByID     map[int]OdooCacheLine
	lineByImport map[string]OdooCacheLine
	codeByID     map[int]string
	matches      map[int][]int
	moveAccounts map[int]map[string]float64 // matched move → amount per GL account
	docs         map[int]OdooOutgoingInvoice
	accounts     map[string]*AccountConfig
	prefixes     []categoryPrefix
	products     []categoryProduct
	slugs        map[string]bool
}

type categoryProduct struct{ glob, slug string }

// categoryProductGlobs: every category's product globs, lower-cased, in
// categories.json order (first match wins).
func categoryProductGlobs(cats []CategoryDef) []categoryProduct {
	var out []categoryProduct
	for _, c := range cats {
		for _, g := range c.Products {
			if g = strings.ToLower(strings.TrimSpace(g)); g != "" {
				out = append(out, categoryProduct{g, c.Slug})
			}
		}
	}
	return out
}

// invoiceLineCategory: the analytic category when it is a known category,
// else the product's (customer documents only: products are what we sell),
// else the GL account's.
//
// Membership is strict (Xavier, 2026-10-07): memberships carry no VAT,
// rentals 21%, so a line is membership only when it is a membership
// product (membershipProductIDs, or a membership-named product) without
// VAT. A membership-looking line with VAT is not membership: it falls back
// to "other-income". The hourly categories check flags the odd ones.
func invoiceLineCategory(li OdooInvoiceLineItem, prefixes []categoryPrefix, products []categoryProduct, slugs map[string]bool) string {
	if membershipProductIDs[li.ProductID] && !invoiceLineHasVAT(li) {
		return "membership"
	}
	c := invoiceLineTaggedCategory(li, products, slugs)
	if c == "" {
		c = categoryForAccountCode(prefixes, li.AccountCode)
	}
	if c == "membership" && invoiceLineHasVAT(li) {
		return "other-income"
	}
	return c
}

// membershipProductIDs: the Odoo membership products — €10/month (94),
// €100/year (111), €200/year for an organisation (104). No VAT, account
// 704200, MEM journal.
var membershipProductIDs = map[int]bool{94: true, 111: true, 104: true}

// membershipAmounts: the only membership prices (EUR, VAT-free).
var membershipAmounts = []float64{10, 100, 200}

func isMembershipAmount(v float64) bool {
	if v < 0 {
		v = -v
	}
	for _, a := range membershipAmounts {
		if math.Abs(v-a) < 0.005 {
			return true
		}
	}
	return false
}

func invoiceLineHasVAT(li OdooInvoiceLineItem) bool {
	for _, t := range li.Taxes {
		if t.Amount > 0 {
			return true
		}
	}
	return li.TotalAmount-li.SubtotalAmount > 0.005
}

// invoiceLineTaggedCategory: the line's analytic category when known, else
// its product's; "" when only the GL account could tell.
func invoiceLineTaggedCategory(li OdooInvoiceLineItem, products []categoryProduct, slugs map[string]bool) string {
	if li.Category != "" && slugs[li.Category] {
		return li.Category
	}
	name := strings.ToLower(strings.TrimSpace(li.ProductName))
	if name == "" {
		name = strings.ToLower(strings.TrimSpace(li.Title))
	}
	if name != "" {
		for _, p := range products {
			if globMatch(p.glob, name) {
				return p.slug
			}
		}
	}
	return ""
}

// structuredCommunication is a Belgian structured communication, with or
// without the +++/***  and slashes: 3 + 4 + 5 digits.
var structuredCommunication = regexp.MustCompile(`(?:\+\+\+|\*\*\*)?\s*\b(\d{3})\s*/?\s*(\d{4})\s*/?\s*(\d{5})\b\s*(?:\+\+\+|\*\*\*)?`)

// invoiceIDFromCommunication returns the account.move id Odoo encodes in
// an invoice's structured communication (first ten digits, last two their
// mod-97 check, 97 for 0), or 0.
func invoiceIDFromCommunication(text string) int {
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
			return int(base)
		}
	}
	return 0
}

type categoryPrefix struct{ prefix, slug string }

var openingBalancePattern = regexp.MustCompile(`(?i)^\s*(solde d'ouverture|opening balance|openingsbalans|beginsaldo)\s*$`)

// categoryPrefixes: every category's PCMN prefixes, longest first.
func categoryPrefixes(cats []CategoryDef) []categoryPrefix {
	var out []categoryPrefix
	for _, c := range cats {
		for _, a := range c.Accounts {
			if a = strings.TrimSpace(a); a != "" {
				out = append(out, categoryPrefix{a, c.Slug})
			}
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return len(out[i].prefix) > len(out[j].prefix) })
	return out
}

func categoryForAccountCode(prefixes []categoryPrefix, code string) string {
	for _, p := range prefixes {
		if strings.HasPrefix(code, p.prefix) {
			return p.slug
		}
	}
	return ""
}

func newOdooTxCategorizer(dataDir string) *odooTxCategorizer {
	o := &odooTxCategorizer{
		lineByID:     map[int]OdooCacheLine{},
		lineByImport: map[string]OdooCacheLine{},
		codeByID:     map[int]string{},
		docs:         map[int]OdooOutgoingInvoice{},
		accounts:     map[string]*AccountConfig{},
		slugs:        map[string]bool{},
	}
	cats := LoadCategories()
	o.prefixes = categoryPrefixes(cats)
	o.products = categoryProductGlobs(cats)
	for _, c := range cats {
		o.slugs[c.Slug] = true
	}
	lines := cachedJournalLines(dataDir)
	if len(lines) == 0 {
		return nil
	}
	for _, l := range lines {
		o.lineByID[l.ID] = l
		if l.UniqueImportID != "" {
			o.lineByImport[strings.ToLower(l.UniqueImportID)] = l
		}
	}
	if chart := loadChartRaw(dataDir); chart != nil {
		for _, a := range chart.Accounts {
			o.codeByID[a.ID] = a.Code
		}
	}
	o.matches = loadStatementMatches(dataDir)
	o.moveAccounts = loadStatementMatchedMoves(dataDir)
	for id, d := range loadAllCachedBills(dataDir) {
		o.docs[id] = d
	}
	for id, d := range loadAllCachedInvoices(dataDir) {
		o.docs[id] = d
	}
	configs := LoadAccountConfigs()
	for i := range configs {
		o.accounts[configs[i].Slug] = &configs[i]
	}
	return o
}

func metadataInt(m map[string]interface{}, key string) int {
	switch v := m[key].(type) {
	case float64:
		return int(v)
	case int:
		return v
	case int64:
		return int(v)
	}
	return 0
}

func (o *odooTxCategorizer) lineFor(tx TransactionEntry) (OdooCacheLine, bool) {
	if id := metadataInt(tx.Metadata, "odooLineId"); id > 0 {
		if l, ok := o.lineByID[id]; ok {
			return l, true
		}
	}
	if s, _ := tx.Metadata["odooImportId"].(string); s != "" {
		if l, ok := o.lineByImport[strings.ToLower(s)]; ok {
			return l, true
		}
	}
	if acc := o.accounts[tx.AccountSlug]; acc != nil {
		id := buildUniqueImportID(acc, tx)
		if l, ok := o.lineByImport[strings.ToLower(id)]; ok {
			return l, true
		}
		if c := CanonicalizeImportID(id); c != "" {
			if l, ok := o.lineByImport[strings.ToLower(c)]; ok {
				return l, true
			}
		}
	}
	return OdooCacheLine{}, false
}

// documentCategory: the category carrying the largest share of the
// documents' line amounts. A cached invoice/bill is read line by line
// (analytic, product, GL account); a matched move that is not one (a
// misc entry) by its GL accounts.
func (o *odooTxCategorizer) documentCategory(moveIDs []int) string {
	weight := map[string]float64{}
	for _, id := range moveIDs {
		counted := false
		doc := o.docs[id]
		products := o.products
		if !strings.HasPrefix(doc.MoveType, "out_") {
			products = nil
		}
		for _, li := range doc.LineItems {
			if li.DisplayType != "" && li.DisplayType != "product" {
				continue
			}
			amt := li.SubtotalAmount
			if amt < 0 {
				amt = -amt
			}
			if c := invoiceLineCategory(li, o.prefixes, products, o.slugs); c != "" {
				weight[c] += amt
				counted = true
			}
		}
		if counted {
			continue
		}
		for code, amt := range o.moveAccounts[id] {
			if c := categoryForAccountCode(o.prefixes, code); c != "" {
				weight[c] += amt
			}
		}
	}
	best, bestW := "", -1.0
	for c, w := range weight {
		if w > bestW || (w == bestW && c < best) {
			best, bestW = c, w
		}
	}
	return best
}

func (o *odooTxCategorizer) apply(tx *TransactionEntry) {
	desc, _ := tx.Metadata["description"].(string)
	if tx.Category == "" && openingBalancePattern.MatchString(desc) {
		tx.Category = "opening_balance"
		tx.Type = "INTERNAL"
		setMetadata(tx, "categorySource", "odoo")
		return
	}
	cat := ""
	if line, ok := o.lineFor(*tx); ok {
		code := o.codeByID[line.AccountID]
		moves := o.matches[line.ID]
		if len(moves) > 0 {
			var uris []interface{}
			for _, m := range moves {
				uris = append(uris, odooDocURI("account.move", m, o.docs[m].InvoiceURL))
			}
			setMetadata(tx, "documents", uris)
		}
		if tx.Category != "" {
			return
		}
		if isDocumentCounterpart(line.CounterpartType) || strings.HasPrefix(code, "40") || strings.HasPrefix(code, "44") {
			cat = o.documentCategory(moves)
		} else if code != "" && !strings.HasPrefix(code, "499") {
			cat = categoryForAccountCode(o.prefixes, code)
		}
	}
	if tx.Category != "" {
		return
	}
	if cat == "" && tx.IsIncoming() {
		cat = o.invoiceCategoryFromCommunication(tx)
	}
	if cat == "" {
		return
	}
	tx.Category = cat
	setMetadata(tx, "categorySource", "odoo")
	if cat == "internal_transfer" {
		tx.Type = "INTERNAL"
	}
	stampVAT(tx)
}

// invoiceCategoryFromCommunication: a payment carrying the structured
// communication of one of our posted customer invoices takes that
// invoice's category, and the invoice goes to metadata.documents.
func (o *odooTxCategorizer) invoiceCategoryFromCommunication(tx *TransactionEntry) string {
	var text []string
	for _, k := range []string{"description", "memo", "fullDescription"} {
		if s, _ := tx.Metadata[k].(string); s != "" {
			text = append(text, s)
		}
	}
	id := invoiceIDFromCommunication(strings.Join(text, " "))
	doc, ok := o.docs[id]
	if id == 0 || !ok || doc.MoveType != "out_invoice" || (doc.State != "" && doc.State != "posted") {
		return ""
	}
	cat := o.documentCategory([]int{id})
	if cat != "" {
		if _, has := tx.Metadata["documents"]; !has {
			setMetadata(tx, "documents", []interface{}{odooDocURI("account.move", id, doc.InvoiceURL)})
		}
	}
	return cat
}

func setMetadata(tx *TransactionEntry, key string, v interface{}) {
	if tx.Metadata == nil {
		tx.Metadata = map[string]interface{}{}
	}
	tx.Metadata[key] = v
}

var (
	odooBookingsHashMu    sync.Mutex
	odooBookingsHashCache = map[string]map[string]string{}
)

// monthOdooBookingsHash fingerprints what the Odoo categorisation of a
// month depends on and the month's providers/ do not hold: how its bank
// lines are booked (counterpart account, matched documents) and the
// category map. A reconciliation in Odoo changes it, so generate rebuilds
// that month. Computed once per data dir and process.
func monthOdooBookingsHash(dataDir, year, month string) string {
	odooBookingsHashMu.Lock()
	defer odooBookingsHashMu.Unlock()
	byMonth, ok := odooBookingsHashCache[dataDir]
	if !ok {
		matches := loadStatementMatches(dataDir)
		moveAccounts := loadStatementMatchedMoves(dataDir)
		cats, _ := os.ReadFile(settingsFilePath("categories.json"))
		lines := cachedJournalLines(dataDir)
		sort.Slice(lines, func(i, j int) bool { return lines[i].ID < lines[j].ID })
		hashes := map[string]hash.Hash{}
		for _, l := range lines {
			if len(l.Date) < 7 {
				continue
			}
			ym := l.Date[:4] + "/" + l.Date[5:7]
			h, ok := hashes[ym]
			if !ok {
				h = sha256.New()
				h.Write(cats)
				hashes[ym] = h
			}
			fmt.Fprintf(h, "%d|%d|%s|%v|", l.ID, l.AccountID, l.CounterpartType, matches[l.ID])
			for _, m := range matches[l.ID] {
				fmt.Fprintf(h, "%v", moveAccounts[m])
			}
			h.Write([]byte("\n"))
		}
		byMonth = map[string]string{}
		for ym, h := range hashes {
			byMonth[ym] = hex.EncodeToString(h.Sum(nil))
		}
		odooBookingsHashCache[dataDir] = byMonth
	}
	return byMonth[year+"/"+month]
}
