package cmd

import (
	"math"
	"testing"
)

// line is a tiny helper to build an account.move.line map the way
// odooSearchReadAllMaps returns it (balance is what unmatchedAccountLines reads).
func line(move string, balance float64) map[string]interface{} {
	return map[string]interface{}{
		"move_id": []interface{}{1, move},
		"balance": balance,
	}
}

func sumBalances(rows []map[string]interface{}) float64 {
	var s float64
	for _, r := range rows {
		s += odooFloat(r["balance"])
	}
	return s
}

func TestUnmatchedAccountLines(t *testing.T) {
	tests := []struct {
		name       string
		rows       []map[string]interface{}
		wantLeft   int
		wantPairs  int
		wantNetTol float64 // expected net of survivors
	}{
		{
			name:      "all paired nets to empty",
			rows:      []map[string]interface{}{line("a", 100), line("b", -100), line("c", 50), line("d", -50)},
			wantLeft:  0,
			wantPairs: 2,
		},
		{
			name:       "surplus on positive side survives",
			rows:       []map[string]interface{}{line("a", 100), line("b", 100), line("c", -100)},
			wantLeft:   1,
			wantPairs:  1,
			wantNetTol: 100,
		},
		{
			name:       "orphan single leg",
			rows:       []map[string]interface{}{line("a", -56678.06)},
			wantLeft:   1,
			wantPairs:  0,
			wantNetTol: -56678.06,
		},
		{
			name:       "mixed magnitudes pair independently",
			rows:       []map[string]interface{}{line("a", 10000), line("b", -10000), line("c", 10000), line("d", 5000)},
			wantLeft:   2, // one extra +10000 and the unmatched +5000
			wantPairs:  1,
			wantNetTol: 15000,
		},
		{
			name:      "zero legs are ignored",
			rows:      []map[string]interface{}{line("a", 0), line("b", 0)},
			wantLeft:  0,
			wantPairs: 0,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, pairs := unmatchedAccountLines(tc.rows)
			if len(got) != tc.wantLeft {
				t.Errorf("survivors = %d, want %d", len(got), tc.wantLeft)
			}
			if pairs != tc.wantPairs {
				t.Errorf("pairs = %d, want %d", pairs, tc.wantPairs)
			}
			if got := sumBalances(got); math.Abs(got-tc.wantNetTol) > 0.005 {
				t.Errorf("survivor net = %.2f, want %.2f", got, tc.wantNetTol)
			}
		})
	}
}

// The key invariant: survivors always net to the same total as the full set,
// because every removed pair nets to zero. This must hold for any input.
func TestUnmatchedPreservesNet(t *testing.T) {
	rows := []map[string]interface{}{
		line("a", 63773.12), line("b", -56678.06), line("c", 10000), line("d", -10000),
		line("e", 5000), line("f", -5000), line("g", 5000), line("h", 45203.34),
		line("i", -7274.95), line("j", -19500),
	}
	full := sumBalances(rows)
	got, _ := unmatchedAccountLines(rows)
	if s := sumBalances(got); math.Abs(s-full) > 0.005 {
		t.Errorf("survivor net %.2f != full net %.2f", s, full)
	}
}
