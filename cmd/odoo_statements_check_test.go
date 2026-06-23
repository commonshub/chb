package cmd

import "testing"

func TestClassifyStatementBalance(t *testing.T) {
	cases := []struct {
		name                             string
		idx                              int
		start, endReal, odooEnd, lineSum float64
		wantKind                         string
		wantSuggested                    float64
	}{
		{
			// Journal 47's first statement: opening 115,525.44 carried as a line.
			// chb running = 0 + 55,644.26 = end_real (matches); Odoo drops the
			// opening line → odooEnd = -59,881.18 ≠ end_real.
			name: "opening carried as line on first statement",
			idx:  0, start: 0, endReal: 55644.26, odooEnd: -59881.18, lineSum: 55644.26,
			wantKind: "opening_in_line", wantSuggested: 115525.44,
		},
		{
			// After the opening_in_line repair: opening now in balance_start,
			// Odoo's running ties out. chb must NOT re-flag it (the 'second run
			// still says error' bug — chb's own start+Σlines double-counts the
			// opening that is both balance_start and still a line).
			name: "valid after opening moved to balance_start",
			idx:  0, start: 115525.44, endReal: 55644.26, odooEnd: 55644.26, lineSum: 55644.26,
			wantKind: "",
		},
		{
			name: "valid statement",
			idx:  3, start: 55644.26, endReal: 51401.23, odooEnd: 51401.23, lineSum: -4243.03,
			wantKind: "",
		},
		{
			// Both chb and Odoo disagree with the declared end -> genuine defect.
			name: "balance mismatch when both disagree",
			idx:  0, start: 0, endReal: 100, odooEnd: 80, lineSum: 80,
			wantKind: "balance_mismatch",
		},
		{
			// Odoo disagrees on a NON-first statement (no opening line there).
			name: "odoo running mismatch on later statement",
			idx:  5, start: 10, endReal: 20, odooEnd: 17, lineSum: 10,
			wantKind: "odoo_running_mismatch",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			kind, suggested := classifyStatementBalance(c.idx, c.start, c.endReal, c.odooEnd, c.lineSum)
			if kind != c.wantKind {
				t.Fatalf("kind = %q, want %q", kind, c.wantKind)
			}
			if c.wantKind == "opening_in_line" && suggested != c.wantSuggested {
				t.Fatalf("suggested = %.2f, want %.2f", suggested, c.wantSuggested)
			}
		})
	}
}
