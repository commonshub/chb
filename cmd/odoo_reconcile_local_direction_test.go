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
	hits, note := matchLineToCandidates(fee, nil, nil, nil, byAmount)
	if len(hits) != 1 || note == "" {
		t.Fatalf("amount-only hit must be flagged: %v %q", hits, note)
	}
	set := reconcileMatchSet{Matches: []reconcileLineMatch{{Line: fee, Hits: hits, NeedsReview: true, ReviewNote: note}}, DemotedToDuplicate: map[int]bool{}}
	if w := set.unambiguousWinners(); len(w) != 0 {
		t.Fatalf("an amount-only match must never be applied by a batch run: %v", w)
	}
}

func TestReferenceMatchNeedsExactAmount(t *testing.T) {
	bill := reconcileCandidate{ID: 7, Kind: "bill", MoveType: "in_invoice", Number: "CHB-S/2026/06/0015", Residual: 253.99, State: "posted"}
	idx := []refMatchEntry{{Kind: "bill", NumberLower: "chb-s/2026/06/0015", Cand: bill}}
	short := OdooCacheLine{ID: 39438, Amount: -244.90, PaymentRef: "CHB-S/2026/06/0015 - 2026-17905"}
	hits, note := matchLineToCandidates(short, nil, idx, nil, nil)
	if len(hits) != 1 || note == "" {
		t.Fatalf("a reference match with a different amount must need review: %v %q", hits, note)
	}
	exact := OdooCacheLine{ID: 1, Amount: -253.99, PaymentRef: "CHB-S/2026/06/0015"}
	if hits, note := matchLineToCandidates(exact, nil, idx, nil, nil); len(hits) != 1 || note != "" {
		t.Fatalf("an exact reference match applies: %v %q", hits, note)
	}
	paid := bill
	paid.Residual = 0
	idx[0].Cand = paid
	if _, note := matchLineToCandidates(exact, nil, idx, nil, nil); note == "" {
		t.Fatal("a reference to an already-paid document must need review")
	}
}
