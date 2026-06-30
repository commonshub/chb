package cmd

import "testing"

// TestReconcileMatcherConsidersCategorizedLines locks in the Part-3 behaviour: a
// bank line merely categorized to an income/expense account (is_reconciled=true)
// is still offered to the reconcile matcher, while a line truly matched to an
// invoice/bill (A/R or A/P counterpart) is skipped. Categorization is a fallback;
// attaching the payment to an invoice/bill always wins.
func TestReconcileMatcherConsidersCategorizedLines(t *testing.T) {
	cases := []struct {
		name string
		ln   OdooCacheLine
		skip bool
	}{
		{
			name: "categorized to income → considered",
			ln:   OdooCacheLine{ID: 1, Amount: 100, IsReconciled: true, CounterpartType: "income"},
			skip: false,
		},
		{
			name: "categorized to expense → considered",
			ln:   OdooCacheLine{ID: 2, Amount: -50, IsReconciled: true, CounterpartType: "expense"},
			skip: false,
		},
		{
			name: "matched to a customer invoice (A/R) → skipped",
			ln:   OdooCacheLine{ID: 3, Amount: 100, IsReconciled: true, CounterpartType: "asset_receivable"},
			skip: true,
		},
		{
			name: "matched to a vendor bill (A/P) → skipped",
			ln:   OdooCacheLine{ID: 4, Amount: -100, IsReconciled: true, CounterpartType: "liability_payable"},
			skip: true,
		},
		{
			name: "unreconciled line → considered (existing behaviour)",
			ln:   OdooCacheLine{ID: 5, Amount: 100, IsReconciled: false},
			skip: false,
		},
		{
			name: "reconciled but unknown counterpart type (old cache) → skipped conservatively",
			ln:   OdooCacheLine{ID: 6, Amount: 100, IsReconciled: true, CounterpartType: ""},
			skip: true,
		},
		{
			name: "zero amount → skipped",
			ln:   OdooCacheLine{ID: 7, Amount: 0, IsReconciled: false},
			skip: true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := reconcileMatcherSkipsLine(c.ln); got != c.skip {
				t.Errorf("reconcileMatcherSkipsLine(%+v) = %v, want %v", c.ln, got, c.skip)
			}
		})
	}
}
