package cmd

import (
	"fmt"
	"math"
	"sort"
	"strings"

	kbcbrusselssource "github.com/CommonsHub/chb/providers/kbcbrussels"
)

// checkKBCBrusselsJournalAgainstCSV compares the Odoo journal's lines
// to the CSV that the operator dropped under latest/providers/kbcbrussels.
// The CSV is the source of truth (the upstream bank), so anything missing
// from Odoo flags a sync gap; anything missing from the CSV usually means
// the CSV export is older than the latest sync and needs refreshing.
func checkKBCBrusselsJournalAgainstCSV(creds *OdooCredentials, uid int, journalID int, acc *AccountConfig) error {
	iban := kbcbrusselssource.NormalizeIBAN(acc.IBAN)
	if iban == "" {
		return fmt.Errorf("account '%s' has no IBAN configured", acc.Slug)
	}

	rows, err := kbcbrusselssource.LoadTransactionsForIBAN(DataDir(), iban)
	if err != nil {
		return fmt.Errorf("load CSV: %v", err)
	}

	fmt.Printf("\n  %sCSV vs Odoo journal #%d (%s)%s\n", Fmt.Bold, journalID, iban, Fmt.Reset)
	if len(rows) == 0 {
		fmt.Printf("    %sNo CSV rows found for %s under %s%s\n",
			Fmt.Yellow, iban, kbcbrusselssource.LatestDir(DataDir()), Fmt.Reset)
		return nil
	}

	// Look up which Odoo lines already exist for any of these CSV rows.
	wantImportIDs := make([]string, 0, len(rows))
	importIDFor := make(map[string]kbcbrusselssource.Transaction, len(rows))
	for _, row := range rows {
		id := fmt.Sprintf("kbcbrussels:%s:%s", strings.ToLower(iban), strings.ToLower(row.Hash))
		wantImportIDs = append(wantImportIDs, id)
		importIDFor[id] = row
	}
	odooImportIDs, err := fetchOdooImportIDs(creds.URL, creds.DB, uid, creds.Password, journalID)
	if err != nil {
		return fmt.Errorf("fetch Odoo import ids: %v", err)
	}

	// CSV-side stats
	csvBalance := latestKBCBalance(rows)
	csvFirst := rows[0].Date
	csvLast := rows[len(rows)-1].Date

	// Odoo-side stats
	odooBalance, balErr := odooJournalCurrentBalance(creds, uid, journalID)
	odooLineCount := odooJournalLineCount(creds, uid, journalID)

	// Missing in Odoo (CSV row whose importID is absent from journal).
	missing := make([]kbcbrusselssource.Transaction, 0)
	for _, id := range wantImportIDs {
		if !odooImportIDs[id] {
			missing = append(missing, importIDFor[id])
		}
	}
	sort.SliceStable(missing, func(i, j int) bool {
		if missing[i].Timestamp == missing[j].Timestamp {
			return missing[i].Hash < missing[j].Hash
		}
		return missing[i].Timestamp < missing[j].Timestamp
	})

	currency := accCurrency(acc)
	fmt.Printf("    %sCSV:   %s between %s and %s, balance: %s%s\n",
		Fmt.Dim,
		Pluralize(len(rows), "row", ""),
		csvFirst, csvLast,
		formatAccountDataBalance(csvBalance, currency),
		Fmt.Reset)
	if balErr != nil {
		fmt.Printf("    %sOdoo:  %d lines, balance unavailable (%v)%s\n", Fmt.Dim, odooLineCount, balErr, Fmt.Reset)
	} else {
		fmt.Printf("    %sOdoo:  %d lines, balance: %s%s\n",
			Fmt.Dim, odooLineCount, formatAccountDataBalance(odooBalance, currency), Fmt.Reset)
	}

	if balErr == nil {
		diff := csvBalance - odooBalance
		if math.Abs(diff) < 0.01 {
			fmt.Printf("    %s✓ balances match%s\n", Fmt.Green, Fmt.Reset)
		} else {
			sign := ""
			if diff > 0 {
				sign = "+"
			}
			fmt.Printf("    %s⚠ off by %s%s%s\n",
				Fmt.Yellow, sign, formatAccountDataBalance(diff, currency), Fmt.Reset)
		}
	}

	if len(missing) == 0 {
		fmt.Printf("    %s✓ every CSV row is in Odoo%s\n", Fmt.Green, Fmt.Reset)
		return nil
	}
	fmt.Printf("    %s⚠ %s in CSV but not in Odoo:%s\n",
		Fmt.Yellow, Pluralize(len(missing), "row", ""), Fmt.Reset)
	preview := missing
	if len(preview) > 10 {
		preview = preview[:10]
	}
	for _, row := range preview {
		fmt.Printf("      %s%s  %s  %s%s\n",
			Fmt.Dim,
			row.Date,
			formatAccountDataBalance(row.Amount, currency),
			truncateRunes(row.Description, 60),
			Fmt.Reset)
	}
	if len(missing) > len(preview) {
		fmt.Printf("      %s… %d more%s\n", Fmt.Dim, len(missing)-len(preview), Fmt.Reset)
	}
	fmt.Printf("    %sTo import: chb generate then chb accounts %s odoo push%s\n",
		Fmt.Dim, acc.Slug, Fmt.Reset)
	return nil
}

// latestKBCBalance returns the running balance reported in the most
// recent CSV row (highest timestamp). The CSV's running balance is the
// bank's authoritative number — no recomputation needed.
func latestKBCBalance(rows []kbcbrusselssource.Transaction) float64 {
	if len(rows) == 0 {
		return 0
	}
	latest := rows[0]
	for _, r := range rows[1:] {
		if r.Timestamp > latest.Timestamp || (r.Timestamp == latest.Timestamp && r.Hash > latest.Hash) {
			latest = r
		}
	}
	return latest.Balance
}

func truncateRunes(s string, max int) string {
	r := []rune(s)
	if len(r) <= max {
		return strings.TrimSpace(string(r))
	}
	return strings.TrimSpace(string(r[:max])) + "…"
}
