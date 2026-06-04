package cmd

import "testing"

func TestMoveIsOpen(t *testing.T) {
	cases := []struct {
		name string
		m    OdooOutgoingInvoicePublic
		want bool
	}{
		{"not_paid posted → open", OdooOutgoingInvoicePublic{State: "posted", PaymentState: "not_paid"}, true},
		{"in_payment posted → open", OdooOutgoingInvoicePublic{State: "posted", PaymentState: "in_payment"}, true},
		{"partial posted → open", OdooOutgoingInvoicePublic{State: "posted", PaymentState: "partial"}, true},
		{"blank payment state → open", OdooOutgoingInvoicePublic{State: "posted"}, true},
		{"paid → closed", OdooOutgoingInvoicePublic{State: "posted", PaymentState: "paid"}, false},
		{"reversed → closed (settled by credit note)", OdooOutgoingInvoicePublic{State: "posted", PaymentState: "reversed"}, false},
		{"reversed uppercase → closed", OdooOutgoingInvoicePublic{State: "posted", PaymentState: "Reversed"}, false},
		{"cancelled → closed", OdooOutgoingInvoicePublic{State: "cancel", PaymentState: "not_paid"}, false},
		{"draft → closed", OdooOutgoingInvoicePublic{State: "draft", PaymentState: "not_paid"}, false},
		{"already attached tx → closed", OdooOutgoingInvoicePublic{State: "posted", PaymentState: "not_paid", ReconciledTransaction: &OdooReconciledTransaction{ID: "tx1"}}, false},
	}
	for _, c := range cases {
		if got := moveIsOpen(c.m); got != c.want {
			t.Errorf("%s: moveIsOpen = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestIsMoveCreditNote(t *testing.T) {
	cases := []struct {
		name string
		m    OdooOutgoingInvoicePublic
		want bool
	}{
		{"out_refund", OdooOutgoingInvoicePublic{MoveType: "out_refund"}, true},
		{"in_refund", OdooOutgoingInvoicePublic{MoveType: "in_refund"}, true},
		{"out_invoice", OdooOutgoingInvoicePublic{MoveType: "out_invoice"}, false},
		// Fallback (no moveType): R-prefixed number is a credit note...
		{"fallback RCHB number", OdooOutgoingInvoicePublic{Title: "RCHB/2025/00011"}, true},
		{"fallback RMEM number", OdooOutgoingInvoicePublic{Title: "RMEM/2025/00004"}, true},
		// ...but a "Reversal of:" narration on a regular invoice is NOT.
		{"fallback reversal narration", OdooOutgoingInvoicePublic{Title: "Reversal of: RCHB/2025/00014, oops"}, false},
		{"fallback CHB number", OdooOutgoingInvoicePublic{Title: "CHB/2025/00203"}, false},
		// moveType wins over the title heuristic.
		{"moveType beats R-title", OdooOutgoingInvoicePublic{MoveType: "out_invoice", Title: "RCHB/2025/00011"}, false},
	}
	for _, c := range cases {
		if got := isMoveCreditNote(c.m); got != c.want {
			t.Errorf("%s: isMoveCreditNote = %v, want %v", c.name, got, c.want)
		}
	}
}
