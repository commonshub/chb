package cmd

// These tests cover the guard that stops `chb odoo sync` / the account push
// from creating bare "mint EURe" statement lines in Odoo.
//
// Every EURe mint/burn corresponds to a Monerium order whose memo carries the
// human description (often the invoice ref the reconciler needs). When the
// order fetch was skipped (no credentials) or failed, the tx reaches the push
// without that metadata — and used to be pushed anyway, creating a line with
// no partner and nothing to match, which then needed a manual
// `fix --metadata` pass against the live journal.

import "testing"

func eureTestAccount() *AccountConfig {
	acc := &AccountConfig{
		Slug:     "savings",
		Provider: "etherscan",
		Chain:    "gnosis",
	}
	acc.Token = &struct {
		Address  string `json:"address"`
		Name     string `json:"name"`
		Symbol   string `json:"symbol"`
		Decimals int    `json:"decimals"`
	}{
		Address:  "0x420CA0f9B9b604cE0fd9C18EF134C705e5Fa3430",
		Name:     "EURe",
		Symbol:   "EURe",
		Decimals: 18,
	}
	return acc
}

func TestTxAwaitingMoneriumMetadata(t *testing.T) {
	acc := eureTestAccount()

	t.Run("bare mint is held", func(t *testing.T) {
		tx := TransactionEntry{Type: "MINT"}
		if !txAwaitingMoneriumMetadata(acc, tx) {
			t.Fatal("a mint with no Monerium metadata must be held back")
		}
	})

	t.Run("bare burn is held", func(t *testing.T) {
		tx := TransactionEntry{Type: "BURN"}
		if !txAwaitingMoneriumMetadata(acc, tx) {
			t.Fatal("a burn with no Monerium metadata must be held back")
		}
	})

	t.Run("mint with memo is pushed", func(t *testing.T) {
		tx := TransactionEntry{
			Type:     "MINT",
			Metadata: map[string]interface{}{"memo": "MEM/2026/00070"},
		}
		if txAwaitingMoneriumMetadata(acc, tx) {
			t.Fatal("an enriched mint (memo present) must not be held")
		}
	})

	t.Run("mint with only a rule/annotation description is held", func(t *testing.T) {
		tx := TransactionEntry{
			Type:     "MINT",
			Metadata: map[string]interface{}{"description": "Rent May 2026"},
		}
		if !txAwaitingMoneriumMetadata(acc, tx) {
			t.Fatal("a description without the Monerium order (no memo/kind/state/tag) must still be held")
		}
	})

	t.Run("mint tagged source:monerium is pushed", func(t *testing.T) {
		tx := TransactionEntry{Type: "MINT", Tags: [][]string{{"source", "monerium"}}}
		if txAwaitingMoneriumMetadata(acc, tx) {
			t.Fatal("a mint enriched from its Monerium order (empty memo) must not be held")
		}
	})

	t.Run("mint with moneriumKind only is pushed", func(t *testing.T) {
		// An order can exist with an empty memo; moneriumKind proves the
		// enrichment ran, so there is nothing more to wait for.
		tx := TransactionEntry{
			Type:     "MINT",
			Metadata: map[string]interface{}{"moneriumKind": "issue"},
		}
		if txAwaitingMoneriumMetadata(acc, tx) {
			t.Fatal("a mint whose enrichment ran (moneriumKind) must not be held")
		}
	})

	t.Run("ordinary transfers are never held", func(t *testing.T) {
		for _, typ := range []string{"CREDIT", "DEBIT", "INTERNAL", ""} {
			tx := TransactionEntry{Type: typ}
			if txAwaitingMoneriumMetadata(acc, tx) {
				t.Fatalf("type %q must not be held: only mint/burn wait for Monerium", typ)
			}
		}
	})

	t.Run("non-EURe token accounts are never held", func(t *testing.T) {
		cht := eureTestAccount()
		cht.Token.Symbol = "CHT"
		tx := TransactionEntry{Type: "MINT"}
		if txAwaitingMoneriumMetadata(cht, tx) {
			t.Fatal("mints of non-EURe tokens have no Monerium orders to wait for")
		}
	})

	t.Run("non-etherscan providers are never held", func(t *testing.T) {
		stripe := eureTestAccount()
		stripe.Provider = "stripe"
		tx := TransactionEntry{Type: "MINT"}
		if txAwaitingMoneriumMetadata(stripe, tx) {
			t.Fatal("the guard only applies to on-chain accounts")
		}
	})

	t.Run("case-insensitive type match", func(t *testing.T) {
		tx := TransactionEntry{Type: "mint"}
		if !txAwaitingMoneriumMetadata(eureTestAccount(), tx) {
			t.Fatal("type matching must be case-insensitive")
		}
	})
}
