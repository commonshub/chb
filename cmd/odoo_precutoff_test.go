package cmd

import "testing"

// preCutoffBalance is the balance a starting-balance entry must carry when the
// pre-cutoff lines are removed: the signed, cent-rounded sum of those lines.
func TestPreCutoffBalance(t *testing.T) {
	if got := preCutoffBalance(nil); got != 0 {
		t.Fatalf("empty = %.2f, want 0", got)
	}
	lines := []preCutoffLine{
		{Amount: 9000.00},
		{Amount: 326.90},
		{Amount: -100.00},
	}
	if got, want := preCutoffBalance(lines), 9226.90; got != want {
		t.Fatalf("preCutoffBalance() = %.2f, want %.2f", got, want)
	}
}
