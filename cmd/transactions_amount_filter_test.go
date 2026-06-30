package cmd

import "testing"

// `--amount ">710" --amount "<=800"` must keep only txs whose ABSOLUTE amount is
// > 710 AND <= 800 (operators apply to magnitude; negatives count by magnitude).
func TestAmountFiltersAreANDedOnMagnitude(t *testing.T) {
	f := TxFilter{Amounts: []AmountFilter{
		{Op: ">", Value: 710},
		{Op: "<=", Value: 800},
	}}

	cases := []struct {
		amount float64
		want   bool
	}{
		{710.00, false},  // boundary: not > 710
		{710.01, true},   // just inside lower bound
		{750.00, true},   // squarely inside
		{800.00, true},   // boundary: <= 800 passes
		{800.01, false},  // just past upper bound
		{500.00, false},  // below range
		{-750.00, true},  // negative magnitude inside range
		{-800.01, false}, // negative magnitude past upper bound
	}
	for _, c := range cases {
		got := f.matches(TransactionEntry{Amount: c.amount})
		if got != c.want {
			t.Errorf("amount %.2f: got %v, want %v", c.amount, got, c.want)
		}
	}

	// A single constraint still works.
	one := TxFilter{Amounts: []AmountFilter{{Op: ">=", Value: 100}}}
	if !one.matches(TransactionEntry{Amount: -100}) {
		t.Errorf("single >=100 should match |-100|")
	}
	if one.matches(TransactionEntry{Amount: 99.99}) {
		t.Errorf("single >=100 should reject 99.99")
	}

	// No amount filter → no amount constraint.
	if !(TxFilter{}).matches(TransactionEntry{Amount: 12345}) {
		t.Errorf("empty filter should match any amount")
	}
}
