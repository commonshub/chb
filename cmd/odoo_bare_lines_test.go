package cmd

import (
	"encoding/json"
	"testing"
)

func TestIsBareTypeFallbackRef(t *testing.T) {
	mint := TransactionEntry{Type: "MINT", Currency: "EURe"}
	// A redeem to our own IBAN is INTERNAL once enriched; its line was
	// pushed as "burn EURe".
	internal := TransactionEntry{Type: "INTERNAL", Currency: "EURe", Metadata: map[string]interface{}{"direction": "BURN"}}
	for _, c := range []struct {
		ref  string
		tx   TransactionEntry
		want bool
	}{
		{"mint EURe", mint, true},
		{" Mint eure ", mint, true},
		{"burn EURe", internal, true},
		{"mint EURe", internal, true},
		{"mint EURb", mint, false},
		{"MEM/2026/00057", mint, false},
		{"Internal transfer: STRIPE", internal, false},
		{"", mint, false},
		{"mint", TransactionEntry{Type: "MINT"}, false},
	} {
		if got := isBareTypeFallbackRef(c.ref, c.tx); got != c.want {
			t.Errorf("isBareTypeFallbackRef(%q, %s) = %v, want %v", c.ref, c.tx.Type, got, c.want)
		}
	}
	// What the push writes without metadata is the bare ref; with a memo it is not.
	if ref := buildOdooPaymentRef(mint); !isBareTypeFallbackRef(ref, mint) {
		t.Errorf("unenriched mint ref %q must be bare", ref)
	}
	enriched := TransactionEntry{Type: "MINT", Currency: "EURe", Metadata: map[string]interface{}{"description": "MEM/2026/00057"}}
	if ref := buildOdooPaymentRef(enriched); isBareTypeFallbackRef(ref, enriched) {
		t.Errorf("enriched mint ref %q must not be bare", ref)
	}
}

func TestBareMoneriumLinesOnlyEUReAccounts(t *testing.T) {
	lines := []OdooCacheLine{{ID: 1, PaymentRef: "mint EURb"}, {ID: 2, PaymentRef: "burn EURb"}}
	eurb := &AccountConfig{Provider: "etherscan"}
	if err := json.Unmarshal([]byte(`{"provider":"etherscan","token":{"symbol":"EURb"}}`), eurb); err != nil {
		t.Fatal(err)
	}
	if h, w := bareMoneriumLines(eurb, lines); len(h)+len(w) != 0 {
		t.Errorf("EURb has no Monerium orders: %v %v", h, w)
	}
	if h, w := bareMoneriumLines(&AccountConfig{Provider: "kbcbrussels"}, lines); len(h)+len(w) != 0 {
		t.Errorf("not an EURe account: %v %v", h, w)
	}
}
