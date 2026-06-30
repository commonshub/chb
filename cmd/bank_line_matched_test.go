package cmd

import "testing"

// A bank line "already attached" must mean matched to a real invoice/bill (its
// counterpart is on a receivable/payable account) — NOT merely categorized to a
// GL income/expense account, which is the normal input to reconciliation.
func TestBankLineMatchedToDocument(t *testing.T) {
	cases := []struct {
		name       string
		reconciled bool
		cpType     string
		want       bool
	}{
		{"unreconciled (still in suspense)", false, "", false},
		{"categorized to income GL", true, "income", false},
		{"categorized to other income GL", true, "income_other", false},
		{"categorized to expense GL", true, "expense", false},
		{"categorized to expense direct cost", true, "expense_direct_cost", false},
		{"matched to a customer invoice (A/R)", true, "asset_receivable", true},
		{"matched to a vendor bill (A/P)", true, "liability_payable", true},
		{"reconciled, unknown cp type (old cache) → conservative", true, "", true},
		// reconciled flag false overrides any counterpart type.
		{"not reconciled but A/R type", false, "asset_receivable", false},
	}
	for _, c := range cases {
		ln := OdooCacheLine{IsReconciled: c.reconciled, CounterpartType: c.cpType}
		if got := bankLineMatchedToDocument(ln); got != c.want {
			t.Errorf("%s: bankLineMatchedToDocument = %v, want %v", c.name, got, c.want)
		}
	}
}
