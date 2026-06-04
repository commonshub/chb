package cmd

import "testing"

func TestInvoiceRefTokens(t *testing.T) {
	cases := map[string][2]string{
		"CHB/2025/00204":       {"2025/00204", "00204"},
		"MEM/2025/00071":       {"2025/00071", "00071"},
		"Reversal of: RCHB/x":  {"", ""},
		"S00172":               {"", ""},
		"+++000/0014/46512+++": {"", ""}, // structured comm, not a clean move number
	}
	for in, want := range cases {
		f, c := invoiceRefTokens(in)
		if f != want[0] || c != want[1] {
			t.Errorf("invoiceRefTokens(%q) = (%q,%q), want (%q,%q)", in, f, c, want[0], want[1])
		}
	}
}

func TestScoredBankLineMemoBeatsAmount(t *testing.T) {
	// Combined transfer: memo names the invoice but the amount differs.
	memoLine := OdooCacheLine{ID: 1, Amount: 2861.65, PaymentRef: "Invoice CHB/2025/00163 & Invoice CHB/2025/00204"}
	s := scoredBankLine(memoLine, 47, "savings", nil, 1464.10, "2025-12-04", "2025/00204", "00204", 0, nil, nil)
	if !s.MemoConfirmed {
		t.Fatalf("expected MemoConfirmed for memo naming the invoice; reason=%q score=%d", s.MatchReason, s.MatchScore)
	}
	if s.MatchScore < scoreMemoFullRef {
		t.Errorf("memo full-ref score = %d, want >= %d", s.MatchScore, scoreMemoFullRef)
	}

	// Exact amount, unrelated memo: scores on amount only, not memo-confirmed.
	amtLine := OdooCacheLine{ID: 2, Amount: 1464.10, PaymentRef: "some unrelated transfer"}
	a := scoredBankLine(amtLine, 47, "savings", nil, 1464.10, "2025-12-04", "2025/00204", "00204", 0, nil, nil)
	if a.MemoConfirmed {
		t.Error("exact-amount line should not be MemoConfirmed")
	}
	if a.MatchScore != scoreExactAmount {
		t.Errorf("exact-amount score = %d, want %d", a.MatchScore, scoreExactAmount)
	}
	if s.MatchScore <= a.MatchScore {
		t.Errorf("memo (%d) should outrank exact-amount (%d)", s.MatchScore, a.MatchScore)
	}
}

func TestParseAmountTerm(t *testing.T) {
	cases := []struct {
		in  string
		op  string
		val float64
		ok  bool
	}{
		{">1000", ">", 1000, true},
		{">=50.5", ">=", 50.5, true},
		{"<1000", "<", 1000, true},
		{"1464", "=", 1464, true},
		{"1.464,10", "", 0, false}, // ambiguous formatting → not a clean float
		{"progress", "", 0, false},
	}
	for _, c := range cases {
		op, val, ok := parseAmountTerm(c.in)
		if ok != c.ok || (ok && (op != c.op || val != c.val)) {
			t.Errorf("parseAmountTerm(%q) = (%q,%v,%v), want (%q,%v,%v)", c.in, op, val, ok, c.op, c.val, c.ok)
		}
	}
}

func TestSuggestionMatchesQuery(t *testing.T) {
	s := Suggestion{Amount: 1464.10, Partner: "Associazione In Progress", IBAN: "BE07 7340 6544 1966", Reference: "Catering 00204"}
	yes := []string{"progress", ">1000", "<2000", "be07", "00204", "1464.10", ""}
	no := []string{"9999", ">5000", "nonexistent"}
	for _, q := range yes {
		if !suggestionMatchesQuery(s, q) {
			t.Errorf("query %q should match", q)
		}
	}
	for _, q := range no {
		if suggestionMatchesQuery(s, q) {
			t.Errorf("query %q should NOT match", q)
		}
	}
}
