package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	odoosource "github.com/CommonsHub/chb/providers/odoo"
)

// Odoo-pending files live at:
//
//	DATA_DIR/<YYYY>/<MM>/providers/odoo/pending/transactions.json
//
// One entry per TransactionEntry whose category/collective resolved to an
// OdooMapping at generate time. Push paths (Stripe / blockchain / KBC merge)
// read pending instead of consulting odoo_mapping.json directly — so editing
// the mapping always goes through `chb generate` to refresh the local files
// before the next push picks the new values up.
//
// Two reasons this lives in providers/<target>/pending/ rather than on
// TransactionEntry:
//
//  1. transactions.json stays target-agnostic. Wave / Xero / a different
//     accounting backend would write its own providers/<other>/pending/
//     without touching the canonical tx record.
//  2. Pending changes are inspectable as a flat artifact — `git diff
//     providers/odoo/pending/2026-05.json` shows exactly what the next push
//     would change.

// OdooPendingEntry is the resolved Odoo identifiers for one transaction.
// Either AccountCode or PartnerID may be empty when the mapping only sets
// one of them.
type OdooPendingEntry struct {
	TxURI       string `json:"txUri"`
	AccountCode string `json:"accountCode,omitempty"`
	PartnerID   int    `json:"partnerId,omitempty"`
	Category    string `json:"category,omitempty"`
	Collective  string `json:"collective,omitempty"`
}

// OdooPendingFile is the on-disk shape of providers/odoo/pending/transactions.json.
type OdooPendingFile struct {
	GeneratedAt string                      `json:"generatedAt"`
	Entries     map[string]OdooPendingEntry `json:"entries"`
}

func odooPendingRelPath() string {
	return odoosource.RelPath("pending", "transactions.json")
}

func odooPendingMonthPath(dataDir, year, month string) string {
	return filepath.Join(dataDir, year, month, odooPendingRelPath())
}

// LoadOdooPending reads the pending file for one month. Missing → empty map
// (not an error) so callers can treat absent state as "nothing to apply".
func LoadOdooPending(dataDir, year, month string) (map[string]OdooPendingEntry, error) {
	path := odooPendingMonthPath(dataDir, year, month)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]OdooPendingEntry{}, nil
		}
		return nil, err
	}
	var f OdooPendingFile
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("parse %s: %v", path, err)
	}
	if f.Entries == nil {
		return map[string]OdooPendingEntry{}, nil
	}
	return f.Entries, nil
}

// WriteOdooPending writes the pending file for one month, atomically
// replacing the previous content. Empty entries → file is removed so
// stale entries don't linger after a rule edit that drops a mapping.
func WriteOdooPending(dataDir, year, month string, entries map[string]OdooPendingEntry) error {
	path := odooPendingMonthPath(dataDir, year, month)
	if len(entries) == 0 {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	f := OdooPendingFile{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Entries:     entries,
	}
	data, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

// PopulateOdooPending enriches each transaction in txs with its pending
// AccountCode / PartnerID, looked up from the relevant providers/odoo/pending/
// files. Idempotent: skips entries that already have both fields set (e.g.
// freshly-resolved in-memory data that hasn't round-tripped through JSON).
//
// Used by push paths after they load transactions.json — those JSON files
// don't carry the Odoo-specific resolution anymore (it lives in pending).
func PopulateOdooPending(dataDir string, txs []TransactionEntry) {
	if len(txs) == 0 || dataDir == "" {
		return
	}
	cache := map[string]map[string]OdooPendingEntry{}
	loc := BrusselsTZ()
	for i := range txs {
		tx := &txs[i]
		if tx.AccountCode != "" && tx.PartnerID != 0 {
			continue
		}
		if tx.ID == "" || tx.Timestamp == 0 {
			continue
		}
		t := time.Unix(tx.Timestamp, 0).In(loc)
		year := t.Format("2006")
		month := t.Format("01")
		key := year + "-" + month
		entries, ok := cache[key]
		if !ok {
			entries, _ = LoadOdooPending(dataDir, year, month)
			cache[key] = entries
		}
		entry, ok := entries[tx.ID]
		if !ok {
			continue
		}
		if tx.AccountCode == "" {
			tx.AccountCode = entry.AccountCode
		}
		if tx.PartnerID == 0 {
			tx.PartnerID = entry.PartnerID
		}
	}
}
