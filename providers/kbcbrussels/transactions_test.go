package kbcbrussels

import (
	"os"
	"path/filepath"
	"testing"
)

// fixtureCSV is a hand-crafted excerpt of a KBC Brussels export. It uses
// CR-only line endings and ";" separators to mirror the real format.
const fixtureCSV = "Account number;Heading;Name;Currency;Statement number;Date;Description;Value date;Amount;Balance;credit;debit;counterparty's account number;Counterparty BIC;Counterparty name;Counterparty address;standard-format reference;Free-format reference\r" +
	"BE46734072238636;                                                  ;CITIZEN SPRING VZW;EUR;  02026032;15/05/2026;EUROPEAN DIRECT DEBIT 15-05 CREDITOR : AMAZON;15/05/2026;-2,99;402,31;              ;-2,99;IE30 CITI 9900 5132 9565 48;STTOIE22;                                                                       ;                                                                       ;                                   ;                                                                                                                                            \r" +
	"BE46734072238636;                                                  ;CITIZEN SPRING VZW;EUR;  02023001;28/09/2023;CREDIT TRANSFER FROM 28-09 BE54 ALL FOR CLIMATE;28/09/2023;1560,73;1599,63;1560,73;              ;BE54 8918 7411 4597;VDSPBE91;ALL FOR CLIMATE;                                                                       ;                                   ;SUMUP REGENS UNITE BERLIN 18/09\r" +
	"BE46734072238636;                                                  ;CITIZEN SPRING VZW;EUR;  02023001;28/09/2023;CREDIT TRANSFER FROM 28-09 BE54 ALL FOR CLIMATE;28/09/2023;38,90;38,90;38,90;              ;BE54 8918 7411 4597;VDSPBE91;ALL FOR CLIMATE;                                                                       ;                                   ;SUMUP REGENS UNITE BERLIN 15/09\r"

func writeFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	csvDir := filepath.Join(dir, "latest", "providers", "kbcbrussels")
	if err := os.MkdirAll(csvDir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(csvDir, "export_BE46734072238636_20260519_1254.csv")
	if err := os.WriteFile(path, []byte(fixtureCSV), 0600); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestLoadCSVParsesKBCFormat(t *testing.T) {
	dir := writeFixture(t)
	txs, err := LoadCSV(filepath.Join(dir, "latest", "providers", "kbcbrussels", "export_BE46734072238636_20260519_1254.csv"))
	if err != nil {
		t.Fatalf("LoadCSV: %v", err)
	}
	if len(txs) != 3 {
		t.Fatalf("len(txs) = %d, want 3", len(txs))
	}

	// First row: -2.99 EUR Amazon direct debit on 15/05/2026.
	tx := txs[0]
	if tx.AccountIBAN != "BE46734072238636" {
		t.Errorf("AccountIBAN = %q, want BE46734072238636", tx.AccountIBAN)
	}
	if tx.Amount != -2.99 {
		t.Errorf("Amount = %v, want -2.99", tx.Amount)
	}
	if tx.Balance != 402.31 {
		t.Errorf("Balance = %v, want 402.31", tx.Balance)
	}
	if tx.Date != "2026-05-15" {
		t.Errorf("Date = %q, want 2026-05-15", tx.Date)
	}
	if tx.CounterpartyIBAN != "IE30CITI99005132956548" {
		t.Errorf("CounterpartyIBAN = %q, want IE30CITI99005132956548", tx.CounterpartyIBAN)
	}
	if len(tx.Hash) != 16 {
		t.Errorf("Hash length = %d, want 16 hex chars", len(tx.Hash))
	}

	// Two rows on 28/09/2023 with identical date/amount-style fields but
	// different amounts → distinct hashes.
	if txs[1].Hash == txs[2].Hash {
		t.Errorf("rows with different amounts share a hash: %q", txs[1].Hash)
	}
}

func TestLoadAllTransactionsDedupAndSort(t *testing.T) {
	dir := writeFixture(t)
	all, err := LoadAllTransactions(dir)
	if err != nil {
		t.Fatalf("LoadAllTransactions: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("len(all) = %d, want 3", len(all))
	}
	// Sorted oldest first.
	if all[0].Date > all[len(all)-1].Date {
		t.Errorf("not sorted oldest-first: first=%q last=%q", all[0].Date, all[len(all)-1].Date)
	}
}

func TestLoadTransactionsForMonthFiltersByDate(t *testing.T) {
	dir := writeFixture(t)
	rows, err := LoadTransactionsForMonth(dir, "BE46734072238636", "2026", "05")
	if err != nil {
		t.Fatalf("LoadTransactionsForMonth: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("len = %d, want 1", len(rows))
	}
	if rows[0].Date != "2026-05-15" {
		t.Errorf("Date = %q, want 2026-05-15", rows[0].Date)
	}

	rows, err = LoadTransactionsForMonth(dir, "BE46734072238636", "2023", "09")
	if err != nil {
		t.Fatalf("LoadTransactionsForMonth: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("Sept 2023 len = %d, want 2", len(rows))
	}
}

func TestRowHashStableAcrossReads(t *testing.T) {
	dir := writeFixture(t)
	first, err := LoadAllTransactions(dir)
	if err != nil {
		t.Fatal(err)
	}
	second, err := LoadAllTransactions(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != len(second) {
		t.Fatalf("len mismatch: %d vs %d", len(first), len(second))
	}
	for i := range first {
		if first[i].Hash != second[i].Hash {
			t.Errorf("hash drift on row %d: %q vs %q", i, first[i].Hash, second[i].Hash)
		}
	}
}

func TestParseKBCNumberHandlesEuropeanFormat(t *testing.T) {
	cases := []struct {
		in   string
		want float64
		ok   bool
	}{
		{"1560,73", 1560.73, true},
		{"-2,99", -2.99, true},
		{"  -0,77  ", -0.77, true},
		{"", 0, false},
		{"NA", 0, false},
	}
	for _, c := range cases {
		got, ok := parseKBCNumber(c.in)
		if ok != c.ok {
			t.Errorf("parseKBCNumber(%q) ok = %v, want %v", c.in, ok, c.ok)
		}
		if ok && got != c.want {
			t.Errorf("parseKBCNumber(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

func TestNormalizeIBANStripsSpaces(t *testing.T) {
	if got := NormalizeIBAN("BE54 8918 7411 4597"); got != "BE54891874114597" {
		t.Errorf("NormalizeIBAN = %q, want BE54891874114597", got)
	}
}

func TestPreferredDescriptionPrioritizesFreeReference(t *testing.T) {
	cases := []struct {
		name string
		tx   Transaction
		want string
	}{
		{
			name: "free reference wins",
			tx: Transaction{
				FreeReference:     "Easter eggs",
				StandardReference: "STR/123",
				Description:       "EUROPEAN DIRECT DEBIT … REFERENCE : foo",
			},
			want: "Easter eggs",
		},
		{
			name: "falls back to standard reference",
			tx: Transaction{
				StandardReference: "***402/5106/52453***",
				Description:       "EUROPEAN DIRECT DEBIT ...",
			},
			want: "***402/5106/52453***",
		},
		{
			name: "extracts after REFERENCE marker",
			tx: Transaction{
				Description: "EUROPEAN DIRECT DEBIT  CREDITOR: AMAZON CREDITOR REF.: STR16NAUVM5I4NUL7HH7LRAN0YZKO4GBSS MANDATE REF.: RSZAY2LARREJDVJZ REFERENCE       : D01-0121592-2479037",
			},
			want: "D01-0121592-2479037",
		},
		{
			name: "case-insensitive REFERENCE matcher",
			tx: Transaction{
				Description: "credit transfer 28-09 reference: SUMUP BERLIN",
			},
			want: "SUMUP BERLIN",
		},
		{
			name: "falls back to full Description",
			tx: Transaction{
				Description: "PAYMENT VIA DEBIT MASTERCARD 13-05 12-05-2026 NOTION LABS",
			},
			want: "PAYMENT VIA DEBIT MASTERCARD 13-05 12-05-2026 NOTION LABS",
		},
		{
			name: "empty everything → empty",
			tx:   Transaction{},
			want: "",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := PreferredDescription(c.tx); got != c.want {
				t.Errorf("PreferredDescription() = %q, want %q", got, c.want)
			}
		})
	}
}
