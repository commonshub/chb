package cmd

import "testing"

func TestSearchKeywords(t *testing.T) {
	got := searchKeywords([]string{"citizen", "wallet", "-i", "-n", "5", "-50.12", "+100", "CHB/2025/00204"})
	want := []string{"citizen", "wallet", "-50.12", "+100", "CHB/2025/00204"}
	if len(got) != len(want) {
		t.Fatalf("searchKeywords = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("searchKeywords[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestParseSearchTermsAndMatch(t *testing.T) {
	income := SearchItem{Amount: 100, hay: "acme invoice chb/2025/00204"}
	expense := SearchItem{Amount: -50.12, hay: "vendor bill"}

	// "+100" → income of 100 only
	pos := parseSearchTerms("+100")
	if !pos[0].matches(income) {
		t.Error("+100 should match income of 100")
	}
	if pos[0].matches(SearchItem{Amount: -100, hay: ""}) {
		t.Error("+100 must NOT match an expense of 100")
	}

	// "-50.12" → expense of 50.12 only
	neg := parseSearchTerms("-50.12")
	if !neg[0].matches(expense) {
		t.Error("-50.12 should match expense of 50.12")
	}
	if neg[0].matches(SearchItem{Amount: 50.12, hay: ""}) {
		t.Error("-50.12 must NOT match income of 50.12")
	}

	// bare "100" → magnitude either way
	bare := parseSearchTerms("100")
	if !bare[0].matches(income) || !bare[0].matches(SearchItem{Amount: -100, hay: ""}) {
		t.Error("bare 100 should match either direction")
	}

	// bare number also text-matches (e.g. inside a reference)
	num := parseSearchTerms("00204")
	if !num[0].matches(income) {
		t.Error("00204 should text-match the reference in hay")
	}

	// plain text, case-insensitive (hay is pre-lowercased)
	txt := parseSearchTerms("ACME")
	if !txt[0].matches(income) {
		t.Error("ACME should match 'acme' in hay (case-insensitive)")
	}

	// AND semantics across terms
	terms := parseSearchTerms("acme +100")
	if !itemMatchesSearch(income, terms) {
		t.Error("'acme +100' should match the income item")
	}
	if itemMatchesSearch(expense, terms) {
		t.Error("'acme +100' should NOT match the expense item")
	}
}

func TestSearchKindFilter(t *testing.T) {
	inv := SearchItem{Kind: "invoice", Amount: 100, hay: "acme"}
	bill := SearchItem{Kind: "bill", Amount: -100, hay: "acme"}
	tx := SearchItem{Kind: "tx", Amount: 100, hay: "acme"}

	// single kind (alias "inv" → invoice)
	terms := parseSearchTerms("kind:inv acme")
	if !itemMatchesSearch(inv, terms) || itemMatchesSearch(bill, terms) || itemMatchesSearch(tx, terms) {
		t.Error("kind:inv should match only invoices")
	}
	// comma list → OR
	terms = parseSearchTerms("kind:tx,bill")
	if itemMatchesSearch(inv, terms) || !itemMatchesSearch(bill, terms) || !itemMatchesSearch(tx, terms) {
		t.Error("kind:tx,bill should match tx and bill but not invoice")
	}
	// kind: combines (AND) with other terms
	terms = parseSearchTerms("kind:invoice +100")
	if !itemMatchesSearch(inv, terms) {
		t.Error("kind:invoice +100 should match the income invoice")
	}
	if itemMatchesSearch(SearchItem{Kind: "tx", Amount: 100, hay: ""}, terms) {
		t.Error("kind:invoice should exclude a tx even at the same amount")
	}
}

func TestMoveSignedAmount(t *testing.T) {
	inv := OdooOutgoingInvoicePublic{MoveType: "out_invoice", TotalAmount: 100}
	if moveSignedAmount(inv, false) != 100 {
		t.Error("invoice should be +100")
	}
	bill := OdooOutgoingInvoicePublic{MoveType: "in_invoice", TotalAmount: 3000}
	if moveSignedAmount(bill, true) != -3000 {
		t.Error("bill should be -3000")
	}
	// MoveType empty (old cache) → kind still decides the base sign.
	billNoType := OdooOutgoingInvoicePublic{Title: "X20240101", TotalAmount: 3000}
	if moveSignedAmount(billNoType, true) != -3000 {
		t.Error("bill with empty MoveType should still be -3000")
	}
	// Credit note flips: customer refund (out_refund) is negative.
	cn := OdooOutgoingInvoicePublic{MoveType: "out_refund", TotalAmount: 100}
	if moveSignedAmount(cn, false) != -100 {
		t.Error("out_refund should be -100")
	}
}
