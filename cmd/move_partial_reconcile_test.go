package cmd

import "testing"

func TestMoveIsOpenIncludesPartial(t *testing.T) {
	rt := &OdooReconciledTransaction{ID: "stripe:x"}
	cases := []struct {
		name string
		m    OdooOutgoingInvoicePublic
		want bool
	}{
		{"not_paid → open", OdooOutgoingInvoicePublic{State: "posted", PaymentState: "not_paid"}, true},
		{"paid → closed", OdooOutgoingInvoicePublic{State: "posted", PaymentState: "paid"}, false},
		{"reversed → closed", OdooOutgoingInvoicePublic{State: "posted", PaymentState: "reversed"}, false},
		{"draft → closed", OdooOutgoingInvoicePublic{State: "draft", PaymentState: "not_paid"}, false},
		// partial must stay open so the remaining payment(s) can be linked —
		// even though it already carries a reconciled payment.
		{"partial → open", OdooOutgoingInvoicePublic{State: "posted", PaymentState: "partial"}, true},
		{"partial w/ reconciledTx → still open", OdooOutgoingInvoicePublic{State: "posted", PaymentState: "partial", ReconciledTransaction: rt}, true},
		// not_paid that chb already fully attached → hidden by the shortcut.
		{"not_paid w/ reconciledTx → closed", OdooOutgoingInvoicePublic{State: "posted", PaymentState: "not_paid", ReconciledTransaction: rt}, false},
	}
	for _, c := range cases {
		if got := moveIsOpen(c.m); got != c.want {
			t.Errorf("%s: moveIsOpen = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestMoveReconcileAmount(t *testing.T) {
	// partial: residual is the remaining balance → match that, not the total.
	partial := OdooOutgoingInvoicePublic{TotalAmount: 500, AmountResidual: 300}
	if got := moveReconcileAmount(partial); got != 300 {
		t.Errorf("partial: got %.2f, want 300 (residual)", got)
	}
	// not_paid: residual == total → either is fine.
	notPaid := OdooOutgoingInvoicePublic{TotalAmount: 500, AmountResidual: 500}
	if got := moveReconcileAmount(notPaid); got != 500 {
		t.Errorf("not_paid: got %.2f, want 500", got)
	}
	// old cache without residual → fall back to total.
	legacy := OdooOutgoingInvoicePublic{TotalAmount: 500}
	if got := moveReconcileAmount(legacy); got != 500 {
		t.Errorf("legacy: got %.2f, want 500 (total fallback)", got)
	}
}
