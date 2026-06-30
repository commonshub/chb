package wise

import (
	"os"
	"path/filepath"
	"testing"
)

const sampleCSV = `"TransferWise ID",Date,Amount,Currency,Description,"Payment Reference","Running Balance","Exchange From","Exchange To","Exchange Rate","Payer Name","Payee Name","Payee Account Number",Merchant,"Card Last Four Digits","Card Holder Full Name",Attachment,Note,"Total fees","Exchange To Amount"
TRANSFER-1500837327,16-04-2025,-7260.00,EUR,"Sent money to TECHI",,1000.00,,,,,TECHI,BE123,,,,,,0.00,
TRANSFER-1467279035,24-03-2025,5000.00,EUR,"Received money",Top up,8260.00,,,,"EUR bank account",,,,,,,,0.00,
BALANCE_CASHBACK-abc,07-01-2025,6.40,EUR,"Balance cashback",,6.40,,,,,,,,,,,,0.00,
`

func writeTemp(t *testing.T, name, content string) string {
	t.Helper()
	dir := t.TempDir()
	// LoadAllTransactions expects latest/providers/wise/<file>
	full := filepath.Join(dir, "latest", "providers", Source)
	if err := os.MkdirAll(full, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(full, name), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestLoadCSVParsesRows(t *testing.T) {
	dir := writeTemp(t, "statement_104984308_EUR_2025-01-01_2025-04-30.csv", sampleCSV)
	txs, err := LoadForBalance(dir, "104984308")
	if err != nil {
		t.Fatal(err)
	}
	if len(txs) != 3 {
		t.Fatalf("got %d txs, want 3", len(txs))
	}
	// Sorted oldest-first: cashback (Jan) → transfer (Mar) → TECHI (Apr).
	first := txs[0]
	if first.Date != "2025-01-07" || first.Amount != 6.40 {
		t.Errorf("first tx = %s %.2f, want 2025-01-07 6.40", first.Date, first.Amount)
	}
	techi := txs[2]
	if techi.TransferID != "TRANSFER-1500837327" || techi.Amount != -7260.00 {
		t.Errorf("techi tx = %s %.2f, want TRANSFER-1500837327 -7260.00", techi.TransferID, techi.Amount)
	}
	if techi.Counterparty() != "TECHI" {
		t.Errorf("counterparty = %q, want TECHI", techi.Counterparty())
	}
	if techi.BalanceID != "104984308" {
		t.Errorf("balanceID = %q, want 104984308", techi.BalanceID)
	}
	if got, want := techi.ImportID(), "wise:104984308:TRANSFER-1500837327"; got != want {
		t.Errorf("importID = %q, want %q", got, want)
	}
}

func TestCreditCounterpartyUsesPayer(t *testing.T) {
	dir := writeTemp(t, "statement_99_EUR_x.csv", sampleCSV)
	txs, _ := LoadForBalance(dir, "104984308") // filename id wins over content
	_ = txs
	dir2 := writeTemp(t, "statement_104984308_EUR_y.csv", sampleCSV)
	all, _ := LoadForBalance(dir2, "104984308")
	for _, tx := range all {
		if tx.Amount == 5000.00 {
			if tx.Counterparty() != "EUR bank account" {
				t.Errorf("credit counterparty = %q, want payer 'EUR bank account'", tx.Counterparty())
			}
		}
	}
}

func TestDedupAcrossOverlappingExports(t *testing.T) {
	dir := t.TempDir()
	full := filepath.Join(dir, "latest", "providers", Source)
	os.MkdirAll(full, 0o755)
	// Same transaction id in two overlapping quarterly files for one balance.
	os.WriteFile(filepath.Join(full, "statement_55_EUR_q1.csv"), []byte(sampleCSV), 0o644)
	os.WriteFile(filepath.Join(full, "statement_55_EUR_q2.csv"), []byte(sampleCSV), 0o644)
	txs, err := LoadForBalance(dir, "55")
	if err != nil {
		t.Fatal(err)
	}
	if len(txs) != 3 {
		t.Fatalf("got %d txs after dedup, want 3", len(txs))
	}
}
