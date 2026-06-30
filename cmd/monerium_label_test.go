package cmd

import "testing"

// TestMoneriumBurnLabelRefresh covers the fix for stale "burn EURe" / "mint EURe"
// payment_refs: a Monerium redeem/issue line uploaded before its order (memo +
// IBAN) existed locally got the bare type-fallback label. Once the memo is
// enriched, buildMoneriumLineSyncUpdate must treat that fallback as
// safe-to-replace and refresh it — but must NOT touch a human-written label.
func TestMoneriumBurnLabelRefresh(t *testing.T) {
	acc := &AccountConfig{Provider: "etherscan", Slug: "eure", AccountID: "gnosis:0xeure"}
	const memo = "CHB-S/2026/05/0001 - 260404"

	// Enriched Monerium burn: carries the order memo as its description.
	tx := TransactionEntry{
		Provider: "etherscan",
		Type:     "BURN",
		Currency: "EURe",
		Metadata: map[string]interface{}{
			"moneriumKind": "redeem",
			"description":  memo,
		},
	}

	t.Run("bare burn fallback is refreshed to the memo", func(t *testing.T) {
		row := map[string]interface{}{"payment_ref": "burn EURe"}
		update := buildMoneriumLineSyncUpdate(acc, tx, row)
		if update == nil || update["payment_ref"] != memo {
			t.Fatalf("expected payment_ref refreshed to %q, got %+v", memo, update)
		}
	})

	t.Run("mint fallback is refreshed too", func(t *testing.T) {
		mintTx := tx
		mintTx.Type = "MINT"
		row := map[string]interface{}{"payment_ref": "mint EURe"}
		update := buildMoneriumLineSyncUpdate(acc, mintTx, row)
		if update == nil || update["payment_ref"] != memo {
			t.Fatalf("expected mint fallback refreshed to %q, got %+v", memo, update)
		}
	})

	t.Run("human-written label is left untouched", func(t *testing.T) {
		row := map[string]interface{}{"payment_ref": "Rent refund — see email"}
		if update := buildMoneriumLineSyncUpdate(acc, tx, row); update != nil {
			t.Fatalf("expected no update for a human label, got %+v", update)
		}
	})

	t.Run("already-correct memo is a no-op", func(t *testing.T) {
		row := map[string]interface{}{"payment_ref": memo}
		if update := buildMoneriumLineSyncUpdate(acc, tx, row); update != nil {
			t.Fatalf("expected no update when already correct, got %+v", update)
		}
	})
}
