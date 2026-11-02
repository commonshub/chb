package cmd

import (
	"reflect"
	"testing"
)

// Regression fixtures: real prod memos the matcher used to miss or get
// wrong (Odoo cleanup of Nov 2026).

func invoiceCand(id int, number, payRef, origin string, partner int, residual float64) reconcileCandidate {
	return reconcileCandidate{
		ID: id, Kind: "invoice", MoveType: "out_invoice", Number: number, State: "posted", PaymentState: "not_paid",
		PartnerID: partner, ResidualEUR: residual, Residual: residual,
		Refs: documentRefs(number, "", payRef, origin), StructComm: structuredComm(payRef),
	}
}

func matchOne(t *testing.T, ln OdooCacheLine, cands ...reconcileCandidate) ([]reconcileCandidate, string) {
	t.Helper()
	byPartner := map[int][]reconcileCandidate{}
	byAmount := map[int64][]reconcileCandidate{}
	for _, c := range cands {
		byPartner[c.PartnerID] = append(byPartner[c.PartnerID], c)
		byAmount[centsKey(c.ResidualEUR)] = append(byAmount[centsKey(c.ResidualEUR)], c)
	}
	return matchLineToCandidates(ln, cands, buildRefMatchIndex(cands), byPartner, byAmount)
}

func wantSingle(t *testing.T, hits []reconcileCandidate, note string, id int) {
	t.Helper()
	if len(hits) != 1 || hits[0].ID != id || note != "" {
		t.Fatalf("want an applicable match to #%d, got %+v note %q", id, hits, note)
	}
}

func TestRefTokens(t *testing.T) {
	for in, want := range map[string][]string{
		"2026_RCC_72":         {"2026", "rcc", "72"},
		"Factuur 2026 RCC 72": {"factuur", "2026", "rcc", "72"},
		"2025Q2CHI0099":       {"2025", "q", "2", "chi", "0099"},
		"MEM/2026/00016":      {"mem", "2026", "00016"},
	} {
		if got := refTokens(in); !reflect.DeepEqual(got, want) {
			t.Errorf("%q: %v", in, got)
		}
	}
	if refMentioned("INV/2025/00010", "INV/2025/0001") {
		t.Error("a longer counter must not match a shorter number")
	}
}

func TestStructuredComms(t *testing.T) {
	for in, want := range map[string]string{
		"+++000/0005/16019+++": "000000516019",
		"000004632152":         "000004632152", // bare 12 digits, as banks send it
		"+++000/0046/32153+++": "",             // check digits wrong (53 ≠ 46321 mod 97)
		"0000046321520":        "",             // 13 digits: not a communication
	} {
		got := structuredComm(in)
		if got != want {
			t.Errorf("%q: got %q want %q", in, got, want)
		}
	}
}

func TestMatchSeparatorsIgnored(t *testing.T) {
	// SL 41015 "Factuur 2026 RCC 72" sat on 499000: the invoice is 2026_RCC_72.
	inv := invoiceCand(72, "2026_RCC_72", "", "", 5, 1105.23)
	hits, note := matchOne(t, OdooCacheLine{ID: 41015, Amount: 1105.23, PaymentRef: "Factuur 2026 RCC 72"}, inv)
	wantSingle(t, hits, note, 72)
}

func TestMatchStructuredCommunicationBeatsPartner(t *testing.T) {
	// SL 40727: Changing World pays De Waele Vincent's MEM/2026/00076.
	inv := invoiceCand(76, "MEM/2026/00076", "+++000/0005/16019+++", "", 900, 100)
	other := invoiceCand(77, "MEM/2026/00077", "", "", 901, 100) // Changing World's own €100 invoice
	hits, note := matchOne(t, OdooCacheLine{ID: 40727, Amount: 100, PartnerID: 901, PaymentRef: "+++000/0005/16019+++"}, inv, other)
	wantSingle(t, hits, note, 76)

	// SL 40975: Monerium, bare digits, €4,218.64 = 2026_RCC_87.
	rcc := invoiceCand(87, "2026_RCC_87", "+++000/0046/32152+++", "", 300, 4218.64)
	hits, note = matchOne(t, OdooCacheLine{ID: 40975, Amount: 4218.64, PartnerID: 301, PaymentRef: "000004632152"}, rcc)
	wantSingle(t, hits, note, 87)
}

func TestMatchSaleOrderOrigin(t *testing.T) {
	// Joy Tandt's €100, memo S00176, is MEM/2026/00067 — not Dom Greet's
	// MEM/2026/00070 of the same amount.
	joy := invoiceCand(67, "MEM/2026/00067", "", "S00176", 10, 100)
	dom := invoiceCand(70, "MEM/2026/00070", "", "S00180", 11, 100)
	hits, note := matchOne(t, OdooCacheLine{ID: 1, Amount: 100, PaymentRef: "S00176"}, joy, dom)
	wantSingle(t, hits, note, 67)
}

func TestMatchMemoPicksAmongSamePartnerSiblings(t *testing.T) {
	// inFlights: four open €X invoices; the memo names 2025Q1CHI0029.
	var cands []reconcileCandidate
	for i, n := range []string{"2025Q1CHI0029", "2025Q1CHI0034", "2025Q2CHI0065", "2025Q2CHI0114"} {
		cands = append(cands, invoiceCand(i+1, n, "", "", 42, 363))
	}
	hits, note := matchOne(t, OdooCacheLine{ID: 2, Amount: 363, PartnerID: 42, PaymentRef: "inFlights 2025Q1CHI0029"}, cands...)
	wantSingle(t, hits, note, 1)

	// No memo: several same-partner, same-amount documents → review, not the oldest.
	hits, note = matchOne(t, OdooCacheLine{ID: 3, Amount: 363, PartnerID: 42, PaymentRef: "payment"}, cands...)
	set := reconcileMatchSet{Matches: []reconcileLineMatch{{Hits: hits, NeedsReview: note != ""}}, DemotedToDuplicate: map[int]bool{}}
	if len(hits) != 4 || len(set.unambiguousWinners()) != 0 {
		t.Fatalf("several same-partner documents must not be applied: %d hits, note %q", len(hits), note)
	}
}

func TestMatchAmountAloneNeverAnotherPartner(t *testing.T) {
	// The same amount on another partner's invoice, a name token in the memo,
	// but the bank line belongs to someone else: review only.
	dom := invoiceCand(70, "MEM/2026/00070", "", "", 11, 100)
	dom.PartnerName = "Greet Dom"
	_, note := matchOne(t, OdooCacheLine{ID: 4, Amount: 100, PartnerID: 10, PaymentRef: "membership greet"}, dom)
	if note == "" {
		t.Fatal("a name-only match against another partner must need review")
	}
}

func TestMatchForeignCurrencyBill(t *testing.T) {
	// Postmark bills in USD; KBC card pays EUR: compare the EUR residual
	// within the FX tolerance, never automatically.
	bill := reconcileCandidate{ID: 128, Kind: "bill", MoveType: "in_invoice", Number: "BILL/2026/0128", State: "posted",
		PartnerID: 50, ResidualEUR: 13.20, Residual: 15, Foreign: true, Refs: []string{"BILL/2026/0128"}}
	hits, note := matchOne(t, OdooCacheLine{ID: 5, Amount: -13.41, PartnerID: 50, PaymentRef: "POSTMARK"}, bill)
	if len(hits) != 1 || note == "" {
		t.Fatalf("FX match must be found and reviewed: %+v %q", hits, note)
	}
	if hits, _ := matchOne(t, OdooCacheLine{ID: 6, Amount: -20, PartnerID: 50, PaymentRef: "POSTMARK"}, bill); len(hits) != 0 {
		t.Fatalf("beyond the FX tolerance: no match, got %+v", hits)
	}
}

func TestOutstandingReceiptsLineIsOffered(t *testing.T) {
	// SL 15392 (Octopus Heart, "MEM/2026/00016"): booked to 552002
	// Outstanding Receipts but never matched to the invoice.
	ln := OdooCacheLine{ID: 15392, Amount: 200, IsReconciled: true, CounterpartType: "asset_current", PaymentRef: "MEM/2026/00016"}
	if reconcileMatcherSkipsLine(ln) || bankLineMatchedToDocument(ln) {
		t.Fatal("a line parked on Outstanding Receipts is not matched to a document")
	}
	ln.CounterpartReconciled = true
	if !reconcileMatcherSkipsLine(ln) {
		t.Fatal("an Outstanding Receipts line reconciled with a payment is matched")
	}
	inv := invoiceCand(16, "MEM/2026/00016", "", "", 60, 200)
	hits, note := matchOne(t, OdooCacheLine{ID: 15392, Amount: 200, PaymentRef: "MEM/2026/00016"}, inv)
	wantSingle(t, hits, note, 16)
}

func TestOpenDocumentGuardUsesSharedMatcher(t *testing.T) {
	// 12 Oct Monerium payments carried their invoice's structured
	// communication but were booked straight to 700003: the guard must hold
	// them, whoever paid.
	rcc := invoiceCand(87, "2026_RCC_87", "+++000/0046/32152+++", "", 300, 4218.64)
	idx := newLocalDocIndex([]reconcileCandidate{rcc}, []reconcileCandidate{rcc})
	if has, doc := idx.openDocumentFor(OdooCacheLine{Amount: 4218.64, PartnerID: 301, PaymentRef: "000004632152"}); !has || doc != "2026_RCC_87" {
		t.Fatalf("structured communication must hold the line: %v %q", has, doc)
	}
	// Same amount, unrelated memo and partner: no hold.
	if has, _ := idx.openDocumentFor(OdooCacheLine{Amount: 4218.64, PartnerID: 999, PaymentRef: "coffee"}); has {
		t.Fatal("an amount alone must not hold a line")
	}
	// Foreign-currency bill of the line's partner (USD Postmark vs EUR card).
	bill := reconcileCandidate{ID: 128, Kind: "bill", MoveType: "in_invoice", Number: "BILL/2026/0128", State: "posted",
		PartnerID: 50, ResidualEUR: 13.20, Residual: 15, Foreign: true, Refs: []string{"BILL/2026/0128"}}
	idx = newLocalDocIndex([]reconcileCandidate{bill}, []reconcileCandidate{bill})
	if has, _ := idx.openDocumentFor(OdooCacheLine{Amount: -13.41, PartnerID: 50, PaymentRef: "POSTMARK"}); !has {
		t.Fatal("a foreign-currency bill within the FX tolerance must hold the card line")
	}
}

func TestNamedDocumentsReport(t *testing.T) {
	inv := invoiceCand(16, "MEM/2026/00016", "", "", 60, 200)
	// RadicalxChange's €200 paid Wikipolicy's MEM/2026/00074, but its memo is
	// 2026_RCC_28's structured communication, and that invoice is still open.
	rcc28 := invoiceCand(28, "2026_RCC_28", "+++000/0043/46610+++", "", 70, 194.81)
	// CHB/2026/00253 was paid twice: the second payment names a paid invoice.
	paid := invoiceCand(253, "CHB/2026/00253", "", "", 80, 744.15)
	paid.PaymentState = "paid"
	lines := map[int][]OdooCacheLine{
		45: {
			{ID: 15392, Date: "2026-03-02", Amount: 200, IsReconciled: true, CounterpartType: "asset_current", PaymentRef: "MEM/2026/00016"},
			{ID: 2, Date: "2026-03-03", Amount: 200, IsReconciled: true, CounterpartType: "asset_receivable", PaymentRef: "MEM/2026/00016 again"},
			{ID: 40422, Date: "2026-09-20", Amount: 200, IsReconciled: true, CounterpartType: "asset_receivable", PaymentRef: "+++000/0043/46610+++"},
			{ID: 22136, Date: "2026-05-13", Amount: 744.15, IsReconciled: true, CounterpartType: "income", PaymentRef: "CHB/2026/00253"},
			{ID: 3, Date: "2026-03-04", Amount: 50, PaymentRef: "unrelated"},
		},
	}
	got := map[int]string{}
	for _, f := range findNamedDocuments([]reconcileCandidate{inv, rcc28, paid}, lines) {
		got[f.Line.ID] = f.Why
	}
	want := map[int]string{15392: "open", 2: "elsewhere", 40422: "elsewhere", 22136: "paid"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("report %v, want %v", got, want)
	}
	// And the matcher never makes a paid document a batch winner.
	_, note := matchOne(t, lines[45][3], paid)
	if note == "" {
		t.Fatal("a memo naming a paid document must need review")
	}
}
