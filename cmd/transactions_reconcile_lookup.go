package cmd

import (
	"strings"
	"sync"
)

// txReconciliationLookup maps a transaction's unique_import_id to the
// Odoo cache line that represents it on its journal. Built once per
// process via newTxReconciliationLookup and reused for the duration of
// a `chb transactions` render so we don't re-read every journal cache
// for every row.
type txReconciliationLookup struct {
	byImportID map[string]OdooCacheLine
	accBySlug  map[string]AccountConfig
}

var (
	txReconciliationOnce  sync.Once
	txReconciliationCache *txReconciliationLookup
)

// getTxReconciliationLookup returns a cached lookup built from every
// linked Odoo journal's latest cache. Cheap on hit; first call loads
// each journal cache once.
func getTxReconciliationLookup() *txReconciliationLookup {
	txReconciliationOnce.Do(func() {
		txReconciliationCache = newTxReconciliationLookup()
	})
	return txReconciliationCache
}

func newTxReconciliationLookup() *txReconciliationLookup {
	out := &txReconciliationLookup{
		byImportID: map[string]OdooCacheLine{},
		accBySlug:  map[string]AccountConfig{},
	}
	configs := LoadAccountConfigs()
	seenJournals := map[int]bool{}
	for _, acc := range configs {
		if acc.Slug != "" {
			out.accBySlug[strings.ToLower(acc.Slug)] = acc
		}
		if acc.OdooJournalID <= 0 || seenJournals[acc.OdooJournalID] {
			continue
		}
		seenJournals[acc.OdooJournalID] = true
		lines, ok := loadLatestOdooJournalLinesCache(acc.OdooJournalID)
		if !ok {
			continue
		}
		for _, ln := range lines {
			if ln.UniqueImportID != "" {
				out.byImportID[strings.ToLower(ln.UniqueImportID)] = ln
			}
		}
	}
	return out
}

// LineFor returns the Odoo cache line for the given transaction, if any.
// Returns (line, true) on hit, (zero, false) when the tx hasn't been
// pushed yet or the journal cache isn't loaded.
func (l *txReconciliationLookup) LineFor(tx TransactionEntry) (OdooCacheLine, bool) {
	if l == nil {
		return OdooCacheLine{}, false
	}
	acc, ok := l.accBySlug[strings.ToLower(tx.AccountSlug)]
	if !ok {
		return OdooCacheLine{}, false
	}
	id := buildUniqueImportID(&acc, tx)
	if id == "" {
		return OdooCacheLine{}, false
	}
	ln, ok := l.byImportID[strings.ToLower(id)]
	return ln, ok
}

// ReconciledRef returns the reconciled-state cell for a transaction
// row: empty if the tx isn't reconciled, otherwise "✓" or "✓ ref" when
// an invoice/bill reference is extractable from the bank line's
// payment_ref / narration / unique_import_id.
func (l *txReconciliationLookup) ReconciledRef(tx TransactionEntry) string {
	refs := l.InvoiceRefs(tx)
	ln, ok := l.LineFor(tx)
	if !ok || !ln.IsReconciled {
		return ""
	}
	if len(refs) == 0 {
		return "✓"
	}
	return "✓ " + strings.Join(refs, ", ")
}

// InvoiceRefs returns the distinct invoice/bill references (e.g. INV/2024/123,
// BILL/2025/4) extractable from the reconciled Odoo bank line's payment_ref /
// narration / unique_import_id. Returns nil when the tx isn't reconciled or no
// reference matches the pattern.
func (l *txReconciliationLookup) InvoiceRefs(tx TransactionEntry) []string {
	ln, ok := l.LineFor(tx)
	if !ok || !ln.IsReconciled {
		return nil
	}
	text := ln.PaymentRef + " " + ln.Narration + " " + ln.UniqueImportID
	matches := odooInvoiceReferencePattern.FindAllString(strings.ToUpper(text), -1)
	if len(matches) == 0 {
		return nil
	}
	seen := map[string]bool{}
	uniq := make([]string, 0, len(matches))
	for _, m := range matches {
		if seen[m] {
			continue
		}
		seen[m] = true
		uniq = append(uniq, m)
	}
	return uniq
}
