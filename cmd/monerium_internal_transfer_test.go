package cmd

import "testing"

// TestMoneriumRedeemToOwnAccountIsInternal locks in that a Monerium redeem/mint
// whose counterpart IBAN belongs to an account we control is reclassified as an
// INTERNAL transfer. The €1000 EURe redeem on 2026-06-23 went to our own KBC
// account (BE46 7340 7223 8636) — it is money moving between two accounts we
// own, so push must route it to 580000 and never match it against a bill.
func TestMoneriumRedeemToOwnAccountIsInternal(t *testing.T) {
	const ownIBAN = "BE46734072238636" // our KBC account, normalized

	newProc := func() *moneriumProcessor {
		return &moneriumProcessor{
			ownIBANs: map[string]bool{ownIBAN: true},
			ordersByTxHash: map[string]moneriumOrderInfo{
				"0xabc": {IBAN: ownIBAN, Kind: "redeem", Memo: "Top up"},
				"0xdef": {IBAN: "DE89370400440532013000", Kind: "redeem"},
			},
		}
	}
	ctx := &ProcessorContext{}

	t.Run("redeem into our own KBC account → INTERNAL", func(t *testing.T) {
		tx := &TransactionEntry{Provider: "etherscan", TxHash: "0xABC", Type: "BURN", Amount: -1000}
		if err := newProc().ProcessTransaction(ctx, tx); err != nil {
			t.Fatalf("ProcessTransaction: %v", err)
		}
		if tx.Type != "INTERNAL" {
			t.Fatalf("Type = %q, want INTERNAL", tx.Type)
		}
		if got := tx.Metadata["direction"]; got != "BURN" {
			t.Fatalf("original direction = %v, want BURN", got)
		}
	})

	t.Run("redeem to a third-party IBAN stays a normal payment", func(t *testing.T) {
		tx := &TransactionEntry{Provider: "etherscan", TxHash: "0xDEF", Type: "BURN", Amount: -200}
		if err := newProc().ProcessTransaction(ctx, tx); err != nil {
			t.Fatalf("ProcessTransaction: %v", err)
		}
		if tx.Type != "BURN" {
			t.Fatalf("Type = %q, want BURN (unchanged)", tx.Type)
		}
	})
}
