package cmd

// Categories from Odoo, the consolidated source of truth. A transaction
// that rules and processors left uncategorised takes its category from how
// its bank statement line is booked in Odoo:
//
//   - reconciled with an invoice or a bill (counterpart on a receivable or
//     payable): the category of that document's lines, by GL account; the
//     documents' URIs go to metadata.documents;
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
	docs         map[int]OdooOutgoingInvoice
	accounts     map[string]*AccountConfig
	prefixes     []categoryPrefix
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
		prefixes:     categoryPrefixes(LoadCategories()),
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
// documents' line amounts.
func (o *odooTxCategorizer) documentCategory(moveIDs []int) string {
	weight := map[string]float64{}
	for _, id := range moveIDs {
		for _, li := range o.docs[id].LineItems {
			amt := li.SubtotalAmount
			if amt < 0 {
				amt = -amt
			}
			if c := categoryForAccountCode(o.prefixes, li.AccountCode); c != "" {
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
	line, ok := o.lineFor(*tx)
	if !ok {
		return
	}
	code := o.codeByID[line.AccountID]
	moves := o.matches[line.ID]
	if len(moves) > 0 {
		var uris []interface{}
		for _, m := range moves {
			if d, ok := o.docs[m]; ok {
				uris = append(uris, odooDocURI("account.move", m, d.InvoiceURL))
			}
		}
		if len(uris) > 0 {
			setMetadata(tx, "documents", uris)
		}
	}
	if tx.Category != "" {
		return
	}
	cat := ""
	if isDocumentCounterpart(line.CounterpartType) || strings.HasPrefix(code, "40") || strings.HasPrefix(code, "44") {
		cat = o.documentCategory(moves)
	} else if code != "" && !strings.HasPrefix(code, "499") {
		cat = categoryForAccountCode(o.prefixes, code)
	}
	if cat == "" {
		return
	}
	tx.Category = cat
	setMetadata(tx, "categorySource", "odoo")
	if cat == "internal_transfer" {
		tx.Type = "INTERNAL"
	}
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
			fmt.Fprintf(h, "%d|%d|%s|%v\n", l.ID, l.AccountID, l.CounterpartType, matches[l.ID])
		}
		byMonth = map[string]string{}
		for ym, h := range hashes {
			byMonth[ym] = hex.EncodeToString(h.Sum(nil))
		}
		odooBookingsHashCache[dataDir] = byMonth
	}
	return byMonth[year+"/"+month]
}
