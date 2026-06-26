package cmd

import (
	"testing"

	stripesource "github.com/CommonsHub/chb/providers/stripe"
)

// TestExpectedStripePerChargeFees covers the per-charge fee model: every charge
// with an implicit fee gets one fee line keyed by "<chargeImportID>:fee" at
// -fee, and fee-less / non-charge BTs get none.
func TestExpectedStripePerChargeFees(t *testing.T) {
	acc := &AccountConfig{Provider: "stripe", AccountID: "acct_X"}
	bts := []stripesource.Transaction{
		{ID: "txn_1", Type: "charge", Amount: 1000, Fee: 59, Net: 941, Created: 100},
		{ID: "txn_2", Type: "payment", Amount: 2000, Fee: 100, Net: 1900, Created: 200},
		{ID: "txn_3", Type: "charge", Amount: 500, Fee: 0, Net: 500, Created: 300}, // no fee
		{ID: "po_1", Type: "payout", Amount: -2000, Net: -2000, Created: 400},      // not a charge
	}
	got := expectedStripePerChargeFees(acc, bts)

	if len(got) != 2 {
		t.Fatalf("expected 2 fee lines (txn_1, txn_2), got %d: %v", len(got), got)
	}
	if v := got["stripe:acct_x:txn_1:fee"]; v != -0.59 {
		t.Fatalf("txn_1 fee = %.2f, want -0.59", v)
	}
	if v := got["stripe:acct_x:txn_2:fee"]; v != -1.00 {
		t.Fatalf("txn_2 fee = %.2f, want -1.00", v)
	}
	if _, ok := got["stripe:acct_x:txn_3:fee"]; ok {
		t.Fatal("fee-less charge must not produce a fee line")
	}
}

// Fees before the odooSyncSince cutoff are excluded.
func TestExpectedStripePerChargeFeesHonoursCutoff(t *testing.T) {
	acc := &AccountConfig{Provider: "stripe", AccountID: "acct_X", OdooSyncSince: "2025-01-01"}
	bts := []stripesource.Transaction{
		{ID: "old", Type: "charge", Amount: 1000, Fee: 999, Net: 1, Created: 1700000000},   // 2023-11
		{ID: "txn_1", Type: "charge", Amount: 1000, Fee: 25, Net: 975, Created: 1740000000}, // 2025-02
	}
	got := expectedStripePerChargeFees(acc, bts)
	if len(got) != 1 {
		t.Fatalf("pre-cutoff fee must be excluded, got %v", got)
	}
	if v := got["stripe:acct_x:txn_1:fee"]; v != -0.25 {
		t.Fatalf("txn_1 fee = %.2f, want -0.25", v)
	}
}

// stripeBTFeeImportID is the charge import id plus ":fee".
func TestStripeBTFeeImportID(t *testing.T) {
	acc := &AccountConfig{Provider: "stripe", AccountID: "acct_ABC"}
	bt := stripesource.Transaction{ID: "txn_123"}
	if got, want := stripeBTFeeImportID(acc, bt), "stripe:acct_abc:txn_123:fee"; got != want {
		t.Fatalf("stripeBTFeeImportID() = %q, want %q", got, want)
	}
}
