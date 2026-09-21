package cmd

import (
	"encoding/json"
	"testing"
)

// TestImportIDRoundTrip simulates the full load → buildUniqueImportID
// path that the orphan finder relies on. It catches the scenario where
// a transactions.json file (with TxHash stripped by the public projection)
// is loaded and then asked to produce its unique_import_id.
func TestImportIDRoundTrip(t *testing.T) {
	acc := &AccountConfig{
		Slug:     "savings",
		Provider: "etherscan",
		Chain:    "gnosis",
		ChainID:  100,
		Address:  "0x6fDF0AaE33E313d9C98D2Aa19Bcd8EF777912CBf",
	}

	// Shape of one entry in stewards/transactions.json after the
	// public-projection step in generate.go strips TxHash.
	publicJSON := `{
		"id": "ethereum:100:tx:0xabc123def456",
		"provider": "etherscan",
		"accountSlug": "savings",
		"amount": 100.0,
		"type": "CREDIT",
		"timestamp": 1715000000
	}`

	var tx TransactionEntry
	if err := json.Unmarshal([]byte(publicJSON), &tx); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if tx.TxHash != "0xabc123def456" {
		t.Errorf("TxHash backfill failed: TxHash=%q, want 0xabc123def456", tx.TxHash)
	}

	got := buildUniqueImportID(acc, tx)
	want := "gnosis:0x6fdf0aae33e313d9c98d2aa19bcd8ef777912cbf:0xabc123def456:0"
	if got != want {
		t.Errorf("buildUniqueImportID(loaded tx) = %q, want %q", got, want)
	}

	// And the inverse: a broken Odoo unique_import_id canonicalizes to
	// the same value, so the orphan finder will mark it as Repairable.
	brokenID := "gnosis:0x6fdf0aae33e313d9c98d2aa19bcd8ef777912cbf:ethereum:100:tx:0xabc123def456:0"
	canonical := CanonicalizeImportID(brokenID)
	if canonical != want {
		t.Errorf("CanonicalizeImportID(broken) = %q, want %q", canonical, want)
	}
}
