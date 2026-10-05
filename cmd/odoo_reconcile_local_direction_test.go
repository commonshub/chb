package cmd

import "testing"

func TestCandidateFitsDirection(t *testing.T) {
	cases := []struct {
		c    reconcileCandidate
		amt  float64
		want bool
	}{
		{reconcileCandidate{Kind: "bill", MoveType: "in_invoice"}, -10, true},
		{reconcileCandidate{Kind: "bill", MoveType: "in_refund"}, -0.41, false}, // the €0.41 Stripe fee vs KBC credit note
		{reconcileCandidate{Kind: "bill", MoveType: "in_refund"}, 0.41, true},
		{reconcileCandidate{Kind: "invoice", MoveType: "out_invoice"}, 50, true},
		{reconcileCandidate{Kind: "invoice", MoveType: "out_invoice"}, -50, false},
		{reconcileCandidate{Kind: "invoice", MoveType: "out_refund"}, -50, true},
		{reconcileCandidate{Kind: "bill", SignedTotal: 0.41}, -0.41, false}, // no move type: credit note by sign
		{reconcileCandidate{Kind: "bill", SignedTotal: -100}, -100, true},
	}
	for i, c := range cases {
		if got := candidateFitsDirection(c.c, c.amt); got != c.want {
			t.Errorf("case %d: %+v amount %v → %v, want %v", i, c.c, c.amt, got, c.want)
		}
	}
}

func TestAmountOnlyMatchIsFlagged(t *testing.T) {
	fee := OdooCacheLine{ID: 40669, Amount: -0.41, PaymentRef: "Stripe fee"}
	kbcNote := reconcileCandidate{ID: 1, Kind: "bill", MoveType: "in_refund", Residual: 0.41, PartnerID: 9, PartnerName: "KBC Bank NV", State: "posted"}
	bill := reconcileCandidate{ID: 2, Kind: "bill", MoveType: "in_invoice", Residual: 0.41, PartnerID: 9, PartnerName: "KBC Bank NV", State: "posted"}
	byAmount := map[int64][]reconcileCandidate{41: {kbcNote}}
	if hits, _ := matchLineToCandidates(fee, nil, nil, nil, byAmount); len(hits) != 0 {
		t.Fatalf("a credit note must not match an outgoing fee: %v", hits)
	}
	byAmount = map[int64][]reconcileCandidate{41: {bill}}
	hits, amountOnly := matchLineToCandidates(fee, nil, nil, nil, byAmount)
	if len(hits) != 1 || !amountOnly {
		t.Fatalf("amount-only hit must be flagged: %v %v", hits, amountOnly)
	}
	set := reconcileMatchSet{Matches: []reconcileLineMatch{{Line: fee, Hits: hits, AmountOnly: true}}, DemotedToDuplicate: map[int]bool{}}
	if w := set.unambiguousWinners(); len(w) != 0 {
		t.Fatalf("an amount-only match must never be applied by a batch run: %v", w)
	}
}
