package cmd

import (
	"testing"

	stripesource "github.com/CommonsHub/chb/sources/stripe"
)

func TestStripeStatementLineAmountUsesGrossForCustomerTransactions(t *testing.T) {
	tests := []struct {
		name string
		bt   stripesource.Transaction
		want float64
	}{
		{
			name: "charge",
			bt:   stripesource.Transaction{Type: "charge", Amount: 2500, Fee: 100, Net: 2400},
			want: 25,
		},
		{
			name: "payment",
			bt:   stripesource.Transaction{Type: "payment", Amount: 4250, Fee: 150, Net: 4100},
			want: 42.5,
		},
		{
			name: "refund",
			bt:   stripesource.Transaction{Type: "refund", Amount: -1000, Fee: -40, Net: -960},
			want: -10,
		},
		{
			name: "payment refund",
			bt:   stripesource.Transaction{Type: "payment_refund", Amount: -1600, Fee: -60, Net: -1540},
			want: -16,
		},
		{
			name: "payout",
			bt:   stripesource.Transaction{Type: "payout", Amount: -5000, Fee: 0, Net: -5000},
			want: -50,
		},
		{
			name: "stripe fee",
			bt:   stripesource.Transaction{Type: "stripe_fee", Amount: -300, Fee: 0, Net: -300},
			want: -3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := stripeStatementLineAmount(tt.bt); got != tt.want {
				t.Fatalf("stripeStatementLineAmount() = %.2f, want %.2f", got, tt.want)
			}
		})
	}
}

func TestUpdateBTStatsUsesGrossCustomerAmounts(t *testing.T) {
	stats := &syncStats{}

	updateBTStats(stats, stripesource.Transaction{Type: "charge", Amount: 2500, Fee: 100, Net: 2400}, 25)
	updateBTStats(stats, stripesource.Transaction{Type: "refund", Amount: -1000, Fee: -40, Net: -960}, -10)

	if stats.Charges != 1 {
		t.Fatalf("Charges = %d, want 1", stats.Charges)
	}
	if stats.ChargesGross != 25 {
		t.Fatalf("ChargesGross = %.2f, want 25.00", stats.ChargesGross)
	}
	if stats.ChargeFees != 0.6 {
		t.Fatalf("ChargeFees = %.2f, want 0.60", stats.ChargeFees)
	}
	if stats.Refunds != 1 {
		t.Fatalf("Refunds = %d, want 1", stats.Refunds)
	}
	if stats.RefundsTotal != -10 {
		t.Fatalf("RefundsTotal = %.2f, want -10.00", stats.RefundsTotal)
	}
}

func TestUpdateBTStatsNetsPayoutCancellations(t *testing.T) {
	stats := &syncStats{}

	updateBTStats(stats, stripesource.Transaction{Type: "payout", Amount: -6000, Net: -6000}, -60)
	updateBTStats(stats, stripesource.Transaction{Type: "payout_cancel", Amount: 1000, Net: 1000}, 10)

	if stats.PayoutsTotal != -50 {
		t.Fatalf("PayoutsTotal = %.2f, want -50.00", stats.PayoutsTotal)
	}
}

func TestStripeFeeAdjustmentCentsTracksCustomerTransactionFees(t *testing.T) {
	tests := []struct {
		name string
		bt   stripesource.Transaction
		want int64
		ok   bool
	}{
		{
			name: "charge fee",
			bt:   stripesource.Transaction{Type: "charge", Amount: 2500, Fee: 100, Net: 2400},
			want: 100,
			ok:   true,
		},
		{
			name: "refund returned fee",
			bt:   stripesource.Transaction{Type: "refund", Amount: -1000, Fee: -40, Net: -960},
			want: -40,
			ok:   true,
		},
		{
			name: "payout no fee line",
			bt:   stripesource.Transaction{Type: "payout", Amount: -2400, Fee: 0, Net: -2400},
			ok:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := stripeFeeAdjustmentCents(tt.bt)
			if ok != tt.ok {
				t.Fatalf("ok = %v, want %v", ok, tt.ok)
			}
			if got != tt.want {
				t.Fatalf("fee cents = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestOpenStatementFeeImportIDIsStableAcrossRuns(t *testing.T) {
	// Regression: the rolling "Stripe fees for open statement" line must
	// have an importID that does not change between sync runs, so that
	// successive runs update the same line instead of accumulating
	// duplicates. Tying the key to the open statement's Odoo ID is the
	// invariant we rely on.
	const accountID = "acct_1ABC"
	const stmtID = 48
	first := openStatementFeeImportID(accountID, stmtID)
	second := openStatementFeeImportID(accountID, stmtID)
	if first != second {
		t.Fatalf("importID is non-deterministic: %q vs %q", first, second)
	}
	if got, want := first, "stripe:acct_1abc:open:48:fees"; got != want {
		t.Fatalf("importID = %q, want %q", got, want)
	}

	// Different statements (e.g. after a payout closes one and opens
	// another) must produce different IDs.
	if openStatementFeeImportID(accountID, stmtID) == openStatementFeeImportID(accountID, stmtID+1) {
		t.Fatalf("importID must differ across open statements")
	}
}

func TestParseStripeAccountIDFromOpenFeeImportID(t *testing.T) {
	tests := []struct {
		name     string
		importID string
		want     string
	}{
		{"canonical", "stripe:acct_1abc:open:48:fees", "acct_1abc"},
		{"legacy bt range", "stripe:acct_1abc:open:bt_aaa:bt_zzz:fees", "acct_1abc"},
		{"non-stripe", "manual:acct_1abc:open:48:fees", ""},
		{"closed payout", "stripe:acct_1abc:po_123:fees", ""},
		{"too short", "stripe:acct_1abc:open", ""},
		{"empty", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseStripeAccountIDFromOpenFeeImportID(tt.importID); got != tt.want {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestStripeGrossCustomerRowsPlusAggregateFeeLineEqualNet(t *testing.T) {
	var feeCents int64
	var grossTotal float64
	var netTotal float64

	for _, bt := range []stripesource.Transaction{
		{Type: "charge", Amount: 2500, Fee: 100, Net: 2400},
		{Type: "refund", Amount: -1000, Fee: -40, Net: -960},
	} {
		grossTotal += stripeStatementLineAmount(bt)
		netTotal += centsToEuros(bt.Net)
		if cents, ok := stripeFeeAdjustmentCents(bt); ok {
			feeCents += cents
		}
	}

	total := grossTotal + stripeAggregateFeeLineAmount(feeCents)
	if total != netTotal {
		t.Fatalf("gross+aggregate fee = %.2f, want net %.2f", total, netTotal)
	}
	if got := stripeAggregateFeeLineAmount(feeCents); got != -0.6 {
		t.Fatalf("aggregate fee line = %.2f, want -0.60", got)
	}
}
