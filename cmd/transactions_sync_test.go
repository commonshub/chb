package cmd

import (
	"testing"

	etherscansource "github.com/CommonsHub/chb/sources/etherscan"
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
