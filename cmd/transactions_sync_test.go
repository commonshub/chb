package cmd

import (
	"os"
	"path/filepath"
	"testing"

	etherscansource "github.com/CommonsHub/chb/providers/etherscan"
	stripesource "github.com/CommonsHub/chb/providers/stripe"
)

func TestCountNewTokenTransfersUsesExistingCacheKeys(t *testing.T) {
	existingTx := etherscansource.TokenTransfer{
		Hash: "0x1", From: "0xaaa", To: "0xbbb", Value: "100", TimeStamp: "1770000000", TokenDecimal: "18", TokenSymbol: "EURe",
	}
	newTx := etherscansource.TokenTransfer{
		Hash: "0x2", From: "0xaaa", To: "0xbbb", Value: "100", TimeStamp: "1770000010", TokenDecimal: "18", TokenSymbol: "EURe",
	}
	existing := map[string]bool{tokenTransferKey(existingTx): true}

	if got := countNewTokenTransfers(existing, []etherscansource.TokenTransfer{existingTx, newTx}); got != 1 {
		t.Fatalf("countNewTokenTransfers() = %d, want 1", got)
	}
}

func TestShouldRunBlockchainEnrichmentOnlyForChangedDefaultSync(t *testing.T) {
	changed := map[string]bool{"savings": true}

	if !shouldRunBlockchainEnrichment("savings", false, changed) {
		t.Fatal("changed account should run enrichment")
	}
	if shouldRunBlockchainEnrichment("checking", false, changed) {
		t.Fatal("unchanged default account should not run enrichment")
	}
	if !shouldRunBlockchainEnrichment("checking", true, changed) {
		t.Fatal("explicit refresh should run enrichment")
	}
}

func TestStripeMonthProviderFilesCachedRequiresCurrentFormatCompanions(t *testing.T) {
	dataDir := t.TempDir()
	writeTransactionSyncTestFile(t, stripesource.Path(dataDir, "2025", "01", stripesource.BalanceTransactionsFile), `{"transactions":[]}`)

	if stripeMonthProviderFilesCached(dataDir, "2025", "01") {
		t.Fatal("month with only balance-transactions.json should not be considered complete")
	}

	writeTransactionSyncTestFile(t, stripesource.Path(dataDir, "2025", "01", stripesource.ChargesFile), `{"charges":{},"refundToCharge":{}}`)
	writeTransactionSyncTestFile(t, stripesource.Path(dataDir, "2025", "01", stripesource.CustomersFile), `{"customers":{}}`)

	if !stripeMonthProviderFilesCached(dataDir, "2025", "01") {
		t.Fatal("month with balance-transactions, charges, and customers should be complete")
	}
}

func TestAllStripeMonthsCachedRequiresEveryMonthComplete(t *testing.T) {
	dataDir := t.TempDir()
	for _, ym := range []struct{ year, month string }{{"2025", "01"}, {"2025", "02"}} {
		writeTransactionSyncTestFile(t, stripesource.Path(dataDir, ym.year, ym.month, stripesource.BalanceTransactionsFile), `{"transactions":[]}`)
		writeTransactionSyncTestFile(t, stripesource.Path(dataDir, ym.year, ym.month, stripesource.ChargesFile), `{"charges":{},"refundToCharge":{}}`)
		writeTransactionSyncTestFile(t, stripesource.Path(dataDir, ym.year, ym.month, stripesource.CustomersFile), `{"customers":{}}`)
	}
	if !allStripeMonthsCached(dataDir, "2025-01", "2025-02") {
		t.Fatal("complete months should be cached")
	}
	if err := os.Remove(stripesource.Path(dataDir, "2025", "02", stripesource.CustomersFile)); err != nil {
		t.Fatal(err)
	}
	if allStripeMonthsCached(dataDir, "2025-01", "2025-02") {
		t.Fatal("range should be incomplete when one month lacks customers.json")
	}
}

func writeTransactionSyncTestFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}
