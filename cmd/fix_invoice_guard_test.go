package cmd

import "testing"

// TestStripeCounterpartIsInvoiceMatched locks in the precise condition that means
// a statement line is reconciled against an invoice/bill — and therefore must
// never have its account rewritten by `fix`: an A/R or A/P counterpart that is
// itself reconciled. Income/expense counterparts (mere categorization) and
// UNreconciled A/R/A/P (over-categorized by a catch-all rule) are fair game.
func TestStripeCounterpartIsInvoiceMatched(t *testing.T) {
	cases := []struct {
		name string
		cp   counterpartMoveLineInfo
		want bool
	}{
		{"reconciled receivable = invoice match", counterpartMoveLineInfo{AccountType: "asset_receivable", Reconciled: true}, true},
		{"reconciled payable = bill match", counterpartMoveLineInfo{AccountType: "liability_payable", Reconciled: true}, true},
		{"unreconciled receivable = over-categorized", counterpartMoveLineInfo{AccountType: "asset_receivable", Reconciled: false}, false},
		{"unreconciled payable = over-categorized", counterpartMoveLineInfo{AccountType: "liability_payable", Reconciled: false}, false},
		{"income = categorized", counterpartMoveLineInfo{AccountType: "income", Reconciled: false}, false},
		{"expense = categorized", counterpartMoveLineInfo{AccountType: "expense", Reconciled: false}, false},
		{"reconciled income is still not an invoice match", counterpartMoveLineInfo{AccountType: "income", Reconciled: true}, false},
		{"unknown / not fetched", counterpartMoveLineInfo{}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := stripeCounterpartIsInvoiceMatched(c.cp); got != c.want {
				t.Errorf("stripeCounterpartIsInvoiceMatched(%+v) = %v, want %v", c.cp, got, c.want)
			}
		})
	}
}
