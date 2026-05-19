package cmd

import (
	"testing"

	kbcbrusselssource "github.com/CommonsHub/chb/providers/kbcbrussels"
)

func TestBuildKBCMergePlanMatchesByStrictKey(t *testing.T) {
	csv := map[mergeKey][]kbcMergeCSVRow{
		{date: "2025-05-06", cpIBAN: "be07734065441966", amountCents: -28500}: {
			{Row: kbcbrusselssource.Transaction{Hash: "csv1", Date: "2025-05-06", Amount: -285, CounterpartyIBAN: "BE07734065441966"}, ImportID: "kbcbrussels:be46:csv1"},
		},
	}
	odoo := map[mergeKey][]kbcMergeOdooLine{
		{date: "2025-05-06", cpIBAN: "be07734065441966", amountCents: -28500}: {
			{ID: 100, Date: "2025-05-06", Amount: -285, AmountCents: -28500, CounterpartyIBAN: "BE07734065441966"},
		},
	}
	plan := buildKBCMergePlan(csv, odoo)
	if len(plan.Pairs) != 1 {
		t.Fatalf("Pairs = %d, want 1", len(plan.Pairs))
	}
	if plan.Pairs[0].Odoo.ID != 100 {
		t.Errorf("paired Odoo id = %d, want 100", plan.Pairs[0].Odoo.ID)
	}
	if len(plan.ToAdd) != 0 || len(plan.ToDelete) != 0 {
		t.Errorf("expected clean match, got ToAdd=%d, ToDelete=%d", len(plan.ToAdd), len(plan.ToDelete))
	}
}

func TestBuildKBCMergePlanLooseMatchWhenOdooHasNoIBAN(t *testing.T) {
	// CSV has the IBAN; Odoo line was imported without `account_number`.
	// Pass 1 (strict) won't pair them; Pass 2 (loose, ignore IBAN) should.
	csv := map[mergeKey][]kbcMergeCSVRow{
		{date: "2025-05-07", cpIBAN: "be12345678901234", amountCents: -10000}: {
			{Row: kbcbrusselssource.Transaction{Hash: "csv2", Date: "2025-05-07", Amount: -100, CounterpartyIBAN: "BE12345678901234"}, ImportID: "kbcbrussels:be46:csv2"},
		},
	}
	odoo := map[mergeKey][]kbcMergeOdooLine{
		{date: "2025-05-07", cpIBAN: "", amountCents: -10000}: {
			{ID: 200, Date: "2025-05-07", Amount: -100, AmountCents: -10000},
		},
	}
	plan := buildKBCMergePlan(csv, odoo)
	if len(plan.Pairs) != 1 {
		t.Fatalf("Pairs = %d, want 1 (loose match)", len(plan.Pairs))
	}
	if plan.Pairs[0].Odoo.ID != 200 {
		t.Errorf("paired Odoo id = %d, want 200", plan.Pairs[0].Odoo.ID)
	}
}

func TestBuildKBCMergePlanFuzzyDateMatch(t *testing.T) {
	// KBC sometimes books an evening tx with a next-day booking date.
	// CSV: 2025-05-06, Odoo: 2025-05-07 — same IBAN, same amount.
	csv := map[mergeKey][]kbcMergeCSVRow{
		{date: "2025-05-06", cpIBAN: "be86377084284650", amountCents: -1863}: {
			{Row: kbcbrusselssource.Transaction{Hash: "csv3", Date: "2025-05-06", Amount: -18.63, CounterpartyIBAN: "BE86377084284650"}, ImportID: "kbcbrussels:be46:csv3"},
		},
	}
	odoo := map[mergeKey][]kbcMergeOdooLine{
		{date: "2025-05-07", cpIBAN: "be86377084284650", amountCents: -1863}: {
			{ID: 300, Date: "2025-05-07", Amount: -18.63, AmountCents: -1863, CounterpartyIBAN: "BE86377084284650"},
		},
	}
	plan := buildKBCMergePlan(csv, odoo)
	if len(plan.Pairs) != 1 {
		t.Fatalf("Pairs = %d, want 1 (fuzzy date match)", len(plan.Pairs))
	}
	if plan.Pairs[0].Odoo.ID != 300 {
		t.Errorf("paired Odoo id = %d, want 300", plan.Pairs[0].Odoo.ID)
	}
}

func TestBuildKBCMergePlanUnmatchedSplitsCorrectly(t *testing.T) {
	csv := map[mergeKey][]kbcMergeCSVRow{
		{date: "2025-05-06", cpIBAN: "be07734065441966", amountCents: -28500}: {
			{Row: kbcbrusselssource.Transaction{Hash: "csvOnly", Date: "2025-05-06", Amount: -285, CounterpartyIBAN: "BE07734065441966"}},
		},
	}
	odoo := map[mergeKey][]kbcMergeOdooLine{
		{date: "2025-01-30", cpIBAN: "", amountCents: -50000}: {
			{ID: 400, Date: "2025-01-30", Amount: -500, AmountCents: -50000},
		},
	}
	plan := buildKBCMergePlan(csv, odoo)
	if len(plan.Pairs) != 0 {
		t.Errorf("Pairs = %d, want 0", len(plan.Pairs))
	}
	if len(plan.ToAdd) != 1 || plan.ToAdd[0].Row.Hash != "csvOnly" {
		t.Errorf("ToAdd = %#v, want one entry 'csvOnly'", plan.ToAdd)
	}
	if len(plan.ToDelete) != 1 || plan.ToDelete[0].ID != 400 {
		t.Errorf("ToDelete = %#v, want one Odoo line #400", plan.ToDelete)
	}
}

func TestBuildKBCMergePlanWiderFuzzyCatchesCardPostingDelay(t *testing.T) {
	// Debit-card payments often post 2-3 days after the transaction.
	// CSV booking date 2025-07-12, Odoo posting date 2025-07-14 → Pass 4
	// (±3 days, IBAN-agnostic) should pair them.
	csv := map[mergeKey][]kbcMergeCSVRow{
		{date: "2025-07-12", cpIBAN: "", amountCents: -1109}: {
			{Row: kbcbrusselssource.Transaction{Hash: "notion1", Date: "2025-07-12", Amount: -11.09}},
		},
	}
	odoo := map[mergeKey][]kbcMergeOdooLine{
		{date: "2025-07-14", cpIBAN: "", amountCents: -1109}: {
			{ID: 7629, Date: "2025-07-14", Amount: -11.09, AmountCents: -1109, IsReconciled: true},
		},
	}
	plan := buildKBCMergePlan(csv, odoo)
	if len(plan.Pairs) != 1 {
		t.Fatalf("Pairs = %d, want 1 (±3 day fuzzy)", len(plan.Pairs))
	}
	if len(plan.ToDelete) != 0 {
		t.Errorf("ToDelete = %d, want 0 (the reconciled card line should be paired, not orphaned)", len(plan.ToDelete))
	}
}

func TestBuildKBCMergePlanPrefersReconciledOnDuplicate(t *testing.T) {
	// One CSV row, two Odoo lines with same key (real Odoo-side
	// duplicate: maybe the same tx was imported twice). The reconciled
	// one should be paired; the unreconciled one becomes an orphan.
	csv := map[mergeKey][]kbcMergeCSVRow{
		{date: "2025-04-29", cpIBAN: "be48967056780227", amountCents: 46665}: {
			{Row: kbcbrusselssource.Transaction{Hash: "wise1", Date: "2025-04-29", Amount: 466.65, CounterpartyIBAN: "BE48967056780227"}},
		},
	}
	odoo := map[mergeKey][]kbcMergeOdooLine{
		{date: "2025-04-30", cpIBAN: "be48967056780227", amountCents: 46665}: {
			{ID: 999, Date: "2025-04-30", Amount: 466.65, AmountCents: 46665, CounterpartyIBAN: "BE48967056780227", IsReconciled: false},
			{ID: 7589, Date: "2025-04-30", Amount: 466.65, AmountCents: 46665, CounterpartyIBAN: "BE48967056780227", IsReconciled: true},
		},
	}
	plan := buildKBCMergePlan(csv, odoo)
	if len(plan.Pairs) != 1 {
		t.Fatalf("Pairs = %d, want 1", len(plan.Pairs))
	}
	if plan.Pairs[0].Odoo.ID != 7589 {
		t.Errorf("paired Odoo = #%d, want #7589 (reconciled)", plan.Pairs[0].Odoo.ID)
	}
	if len(plan.ToDelete) != 1 || plan.ToDelete[0].ID != 999 {
		t.Errorf("ToDelete = %#v, want one entry #999 (the unreconciled duplicate)", plan.ToDelete)
	}
}

func TestDatesWithinOneDay(t *testing.T) {
	cases := []struct {
		a, b string
		want bool
	}{
		{"2025-05-06", "2025-05-07", true},
		{"2025-05-07", "2025-05-06", true},
		{"2025-05-06", "2025-05-06", true},
		{"2025-05-06", "2025-05-08", false},
		{"2025-05-06", "", false},
		{"bad", "2025-05-06", false},
	}
	for _, c := range cases {
		got := datesWithinOneDay(c.a, c.b)
		if got != c.want {
			t.Errorf("datesWithinOneDay(%q, %q) = %v, want %v", c.a, c.b, got, c.want)
		}
	}
}
