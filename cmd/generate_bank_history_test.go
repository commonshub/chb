package cmd

import (
	"os"
	"path/filepath"
	"testing"
)

const wiseHistoryCSV = `"TransferWise ID",Date,Amount,Currency,Description,"Payment Reference","Running Balance","Exchange From","Exchange To","Exchange Rate","Payer Name","Payee Name","Payee Account Number",Merchant,"Card Last Four Digits","Card Holder Full Name",Attachment,Note,"Total fees","Exchange To Amount"
TRANSFER-2,10-01-2025,-10.00,EUR,"Sent money to X",,90.00,,,,,X,,,,,,,0.00,
TRANSFER-1,12-06-2024,-100.00,EUR,"Sent money to Vendor",INV-7,100.00,,,,,Vendor,,,,,,,0.00,
BALANCE-9,15-06-2024,-50.00,EUR,"Moved to jar",,50.00,,,,,,,,,,,,0.00,
`

const wiseJarCSV = `"TransferWise ID",Date,Amount,Currency,Description,"Payment Reference","Running Balance","Exchange From","Exchange To","Exchange Rate","Payer Name","Payee Name","Payee Account Number",Merchant,"Card Last Four Digits","Card Holder Full Name",Attachment,Note,"Total fees","Exchange To Amount"
BALANCE-9,15-06-2024,50.00,EUR,"Moved from main",,50.00,,,,,,,,,,,,0.00,
`

func TestWiseEntriesBeforeCutoffOnly(t *testing.T) {
	dataDir := t.TempDir()
	dir := filepath.Join(dataDir, "latest", "providers", "wise")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(dir, "statement_111_EUR_2024-01-01_2025-03-31.csv"), []byte(wiseHistoryCSV), 0o644)
	os.WriteFile(filepath.Join(dir, "statement_222_EUR_2024-01-01_2024-12-31.csv"), []byte(wiseJarCSV), 0o644)
	accounts := map[string]AccountConfig{
		"111": {Slug: "wise-main", Provider: "wise", WiseBalanceID: "111", OdooSyncSince: "2025-01-01"},
		"222": {Slug: "wise-jar", Provider: "wise", WiseBalanceID: "222", OdooSyncSince: "2025-01-01"},
	}

	june := wiseEntriesForMonth(dataDir, accounts, "2024", "06")
	if len(june) != 3 {
		t.Fatalf("June 2024: %d entries, want 3", len(june))
	}
	byID := map[string]TransactionEntry{}
	for _, e := range june {
		byID[e.ID] = e
	}
	pay := byID["wise:111:tx:TRANSFER-1"]
	if pay.Type != "DEBIT" || pay.Amount != -100 || pay.Counterparty != "Vendor" || pay.Provider != "wise" || pay.Metadata["description"] != "INV-7" {
		t.Errorf("payment = %+v", pay)
	}
	for _, id := range []string{"wise:111:tx:BALANCE-9", "wise:222:tx:BALANCE-9"} {
		if e := byID[id]; e.Type != "INTERNAL" || e.Category != "internal_transfer" {
			t.Errorf("%s: a move between two of our balances must be internal, got %s/%s", id, e.Type, e.Category)
		}
	}
	if jan := wiseEntriesForMonth(dataDir, accounts, "2025", "01"); len(jan) != 0 {
		t.Errorf("rows on or after the Odoo cutoff must not be generated: %+v", jan)
	}
}

func TestMonthBeforeCutoff(t *testing.T) {
	acc := AccountConfig{OdooSyncSince: "2025-01-01"}
	for _, c := range []struct {
		y, m string
		want bool
	}{{"2024", "12", true}, {"2023", "09", true}, {"2025", "01", false}, {"2026", "03", false}, {"latest", "", true}} {
		if _, got := monthBeforeCutoff(acc, c.y, c.m); got != c.want {
			t.Errorf("%s/%s: got %v want %v", c.y, c.m, got, c.want)
		}
	}
	if _, got := monthBeforeCutoff(AccountConfig{}, "2024", "01"); got {
		t.Error("no cutoff: the CSV leg must stay off (the account isn't Odoo-sourced from a date)")
	}
}
