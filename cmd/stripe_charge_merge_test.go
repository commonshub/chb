package cmd

import (
	"testing"

	stripesource "github.com/CommonsHub/chb/providers/stripe"
)

// TestMergeFetchedChargesIntoMonthPreservesExisting guards the destructive-
// overwrite bug: an incremental pull (which fetches only NEW balance
// transactions) must NOT drop charges fetched on an earlier run.
func TestMergeFetchedChargesIntoMonthPreservesExisting(t *testing.T) {
	dir := t.TempDir()
	// Seed the month with two previously-fetched charges + a refund mapping.
	seed := map[string]*stripesource.Charge{
		"ch_old1": {ID: "ch_old1", ProductName: "Donation"},
		"ch_old2": {ID: "ch_old2", PaymentLink: "plink_x"},
	}
	if err := stripesource.SaveChargeData(dir, "2026", "06", seed, map[string]string{"re_1": "ch_old1"}); err != nil {
		t.Fatal(err)
	}
	// An incremental pull brings just one new charge.
	fetched := map[string]*stripesource.Charge{"ch_new": {ID: "ch_new", BillingName: "Emily Carey"}}
	merged, refunds, added := mergeFetchedChargesIntoMonth(dir, "2026", "06", fetched, map[string]string{"re_2": "ch_new"})

	if len(merged) != 3 {
		t.Fatalf("merged charges = %d, want 3 (2 existing + 1 new) — existing were dropped", len(merged))
	}
	for _, id := range []string{"ch_old1", "ch_old2", "ch_new"} {
		if merged[id] == nil {
			t.Errorf("merged is missing %s", id)
		}
	}
	if added != 1 {
		t.Errorf("added = %d, want 1", added)
	}
	if refunds["re_1"] != "ch_old1" || refunds["re_2"] != "ch_new" {
		t.Errorf("refund map not merged: %v", refunds)
	}
}
