package cmd

import "testing"

// The cache-skip optimisation must NOT treat a payment_state flip as "unchanged"
// — Odoo doesn't bump write_date when a payment is reconciled, so without this a
// now-paid bill is never rewritten and reconcile keeps showing it as open.
func TestPublicMoveStatusUnchanged(t *testing.T) {
	base := []OdooOutgoingInvoicePublic{
		{ID: 1, State: "posted", PaymentState: "not_paid"},
		{ID: 2, State: "posted", PaymentState: "paid"},
	}

	same := []OdooOutgoingInvoicePublic{
		{ID: 1, State: "posted", PaymentState: "not_paid"},
		{ID: 2, State: "posted", PaymentState: "paid"},
	}
	if !publicMoveStatusUnchanged(base, same) {
		t.Errorf("identical statuses should be unchanged")
	}

	// payment_state flip (not_paid → paid) must register as changed.
	paid := []OdooOutgoingInvoicePublic{
		{ID: 1, State: "posted", PaymentState: "paid"},
		{ID: 2, State: "posted", PaymentState: "paid"},
	}
	if publicMoveStatusUnchanged(base, paid) {
		t.Errorf("payment_state flip must be detected as changed")
	}

	// state flip (posted → cancel) must register as changed.
	cancelled := []OdooOutgoingInvoicePublic{
		{ID: 1, State: "cancel", PaymentState: "not_paid"},
		{ID: 2, State: "posted", PaymentState: "paid"},
	}
	if publicMoveStatusUnchanged(base, cancelled) {
		t.Errorf("state flip must be detected as changed")
	}

	// different counts → changed.
	if publicMoveStatusUnchanged(base, base[:1]) {
		t.Errorf("differing length must be changed")
	}
}
