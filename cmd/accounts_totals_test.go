package cmd

import (
	"math"
	"testing"

	stripesource "github.com/CommonsHub/chb/sources/stripe"
)

func TestAccountTotalsFromStripeTransactions(t *testing.T) {
	totals := accountTotalsFromStripeTransactions([]stripesource.Transaction{
		{ID: "charge", Type: "charge", Amount: 10000, Fee: 300, Net: 9700, Currency: "eur"},
		{ID: "refund", Type: "refund", Amount: -2500, Fee: -50, Net: -2450, Currency: "eur"},
		{ID: "fee", Type: "stripe_fee", Amount: -125, Net: -125, Currency: "eur"},
		{ID: "credit", Type: "adjustment", Amount: 25, Net: 25, Currency: "eur"},
		{ID: "payout", Type: "payout", Amount: -6000, Net: -6000, Currency: "eur"},
		{ID: "cancel", Type: "payout_cancel", Amount: 1000, Net: 1000, Currency: "eur"},
	}, "EUR")

	assertFloat(t, "GrossIncome", totals.GrossIncome, 100)
	assertFloat(t, "TotalPaidOut", totals.TotalPaidOut, 25)
	assertFloat(t, "TxFees", totals.TxFees, 2.5)
	assertFloat(t, "OtherFees", totals.OtherFees, 1)
	assertFloat(t, "InternalTransfers", totals.InternalTransfers, 50)
	assertFloat(t, "CurrentBalance", totals.CurrentBalance, 21.5)
}

func assertFloat(t *testing.T, name string, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 0.001 {
		t.Fatalf("%s = %.2f, want %.2f", name, got, want)
	}
}
