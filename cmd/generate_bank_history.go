package cmd

// Bank history before the Odoo cutoff.
//
// KBC and the Wise balances are booked in Odoo from their odooSyncSince
// cutoff (2025-01-01) on. Before it, the books were kept in WinBooks and
// Odoo holds only an opening-balance entry, so the months before the cutoff
// come from the banks' own CSV exports (latest/providers/kbcbrussels/,
// latest/providers/wise/):
//
//   - KBC is odooSourceOfTruth: from the cutoff on its transactions come
//     from the Odoo journal (unchanged); before it, from the CSV.
//   - Wise is CSV-only: only rows before the cutoff are generated. From the
//     cutoff on the journals hold the accountant's import, and Wise is
//     archived, so nothing changes there.
//
// Rows before the cutoff are never pushed: every push and repair path
// windows the local transactions to >= odooSyncSince.

import (
	"fmt"
	"math"
	"strings"
	"time"

	kbcbrusselssource "github.com/CommonsHub/chb/providers/kbcbrussels"
	wisesource "github.com/CommonsHub/chb/providers/wise"
)

// monthBeforeCutoff reports whether a generate scope can hold rows before
// the account's cutoff. "latest" (full rebuild) always can.
func monthBeforeCutoff(acc AccountConfig, year, month string) (time.Time, bool) {
	cutoff, ok := acc.OdooSyncSinceTime()
	if !ok {
		return time.Time{}, false
	}
	if year == "latest" || month == "" {
		return cutoff, true
	}
	start, err := time.ParseInLocation("2006-01", year+"-"+month, BrusselsTZ())
	if err != nil {
		return time.Time{}, false
	}
	return cutoff, start.Before(cutoff)
}

// kbcPreCutoffCSVEntries returns the CSV rows of an Odoo-source-of-truth KBC
// account dated before its cutoff, for the month being generated.
func kbcPreCutoffCSVEntries(dataDir string, acc AccountConfig, year, month string) []TransactionEntry {
	cutoff, ok := monthBeforeCutoff(acc, year, month)
	if !ok || acc.IBAN == "" {
		return nil
	}
	var rows []kbcbrusselssource.Transaction
	var err error
	if year == "latest" || month == "" {
		rows, err = kbcbrusselssource.LoadTransactionsForIBAN(dataDir, acc.IBAN)
	} else {
		rows, err = kbcbrusselssource.LoadTransactionsForMonth(dataDir, acc.IBAN, year, month)
	}
	if err != nil {
		fmt.Printf("    %s⚠ kbcbrussels %s: %v%s\n", Fmt.Yellow, acc.IBAN, err, Fmt.Reset)
		return nil
	}
	var out []TransactionEntry
	for _, row := range rows {
		if row.Timestamp >= cutoff.Unix() {
			continue
		}
		out = append(out, kbcRowToTransactionEntry(acc, row))
	}
	return out
}

// wiseAccountsByBalance maps a Wise balance id to its account.
func wiseAccountsByBalance() map[string]AccountConfig {
	out := map[string]AccountConfig{}
	for _, acc := range LoadAccountConfigs() {
		if acc.Provider == "wise" && acc.WiseBalanceID != "" {
			out[acc.WiseBalanceID] = acc
		}
	}
	return out
}

// wiseEntriesForMonth returns the Wise rows of the month dated before each
// balance's cutoff (all rows when the account has no cutoff).
func wiseEntriesForMonth(dataDir string, accounts map[string]AccountConfig, year, month string) []TransactionEntry {
	if len(accounts) == 0 {
		return nil
	}
	rows, err := wisesource.LoadAllTransactions(dataDir)
	if err != nil {
		fmt.Printf("    %s⚠ wise: %v%s\n", Fmt.Yellow, err, Fmt.Reset)
		return nil
	}
	var out []TransactionEntry
	for _, row := range rows {
		acc, ok := accounts[row.BalanceID]
		if !ok {
			continue
		}
		if cutoff, has := acc.OdooSyncSinceTime(); has && row.Timestamp >= cutoff.Unix() {
			continue
		}
		if year != "latest" && month != "" {
			t := time.Unix(row.Timestamp, 0).In(BrusselsTZ())
			if t.Format("2006") != year || t.Format("01") != month {
				continue
			}
		}
		out = append(out, wiseRowToTransactionEntry(acc, row))
	}
	markWiseInternalTransfers(out)
	return out
}

// BuildWiseTxURI is wise:<balanceId>:tx:<transferId>[:<suffix>], the same
// key as the Odoo unique_import_id `chb wise sync` writes.
func BuildWiseTxURI(row wisesource.Transaction) string {
	id := fmt.Sprintf("wise:%s:tx:%s", row.BalanceID, row.TransferID)
	if row.ImportSuffix != "" {
		id += ":" + row.ImportSuffix
	}
	return id
}

func wiseRowToTransactionEntry(acc AccountConfig, row wisesource.Transaction) TransactionEntry {
	currency := strings.ToUpper(firstNonEmpty(row.Currency, acc.Currency, "EUR"))
	txType := "CREDIT"
	if row.Amount < 0 {
		txType = "DEBIT"
	}
	desc := strings.TrimSpace(firstNonEmpty(row.Reference, row.Description))
	metadata := map[string]interface{}{
		"description":     desc,
		"fullDescription": strings.TrimSpace(row.Description),
		"balance":         roundCents(row.Balance),
		"transferId":      row.TransferID,
	}
	if row.Reference != "" {
		metadata["reference"] = row.Reference
	}
	if row.Merchant != "" {
		metadata["merchant"] = row.Merchant
	}
	if row.TotalFees != 0 {
		metadata["fees"] = roundCents(row.TotalFees)
	}
	signed := roundCents(row.Amount)
	return TransactionEntry{
		ID:               BuildWiseTxURI(row),
		ProviderID:       row.TransferID,
		AccountID:        "wise:" + row.BalanceID,
		TxHash:           row.Hash,
		Provider:         wisesource.Source,
		Account:          row.BalanceID,
		AccountSlug:      acc.Slug,
		AccountName:      acc.Name,
		Currency:         currency,
		Value:            fmt.Sprintf("%.2f", signed),
		Amount:           signed,
		NetAmount:        signed,
		GrossAmount:      roundCents(math.Abs(row.Amount)),
		NormalizedAmount: signed,
		Type:             txType,
		Counterparty:     row.Counterparty(),
		Timestamp:        row.Timestamp,
		Metadata:         metadata,
	}
}

// markWiseInternalTransfers tags both legs of a move between two of our
// Wise balances (same transfer id, opposite amounts) as internal.
func markWiseInternalTransfers(entries []TransactionEntry) {
	byTransfer := map[string][]int{}
	for i, e := range entries {
		if id := e.ProviderID; id != "" {
			byTransfer[id] = append(byTransfer[id], i)
		}
	}
	for _, idx := range byTransfer {
		if len(idx) != 2 {
			continue
		}
		a, b := &entries[idx[0]], &entries[idx[1]]
		if a.AccountSlug == b.AccountSlug || math.Abs(a.Amount+b.Amount) >= 0.005 {
			continue
		}
		for _, e := range []*TransactionEntry{a, b} {
			e.Type = "INTERNAL"
			e.Category = "internal_transfer"
		}
	}
}

// bankHistoryMonths lists, per year, the months holding KBC or Wise CSV rows
// dated before the account's Odoo cutoff.
func bankHistoryMonths(dataDir string) map[string][]string {
	seen := map[string]map[string]bool{}
	add := func(ts int64) {
		t := time.Unix(ts, 0).In(BrusselsTZ())
		y, m := t.Format("2006"), t.Format("01")
		if seen[y] == nil {
			seen[y] = map[string]bool{}
		}
		seen[y][m] = true
	}
	for _, acc := range kbcAccountsByIBAN() {
		cutoff, ok := acc.OdooSyncSinceTime()
		if !ok || !acc.IsOdooSourceOfTruth() {
			continue
		}
		rows, _ := kbcbrusselssource.LoadTransactionsForIBAN(dataDir, acc.IBAN)
		for _, r := range rows {
			if r.Timestamp < cutoff.Unix() {
				add(r.Timestamp)
			}
		}
	}
	if accounts := wiseAccountsByBalance(); len(accounts) > 0 {
		rows, _ := wisesource.LoadAllTransactions(dataDir)
		for _, r := range rows {
			acc, ok := accounts[r.BalanceID]
			if !ok {
				continue
			}
			if cutoff, has := acc.OdooSyncSinceTime(); has && r.Timestamp >= cutoff.Unix() {
				continue
			}
			add(r.Timestamp)
		}
	}
	out := map[string][]string{}
	for y, ms := range seen {
		for m := range ms {
			out[y] = append(out[y], m)
		}
	}
	return out
}
