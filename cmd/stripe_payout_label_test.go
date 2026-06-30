package cmd

import (
	"strings"
	"testing"

	stripesource "github.com/CommonsHub/chb/providers/stripe"
)

// TestBtPaymentRefPayoutPrefersDescription locks in that a payout line is
// labelled with its description (the operator's memo) rather than the bare
// "Manual payout <date>" — and that the epoch-date label only appears as a last
// resort when there's no description AND no usable timestamp.
func TestBtPaymentRefPayoutPrefersDescription(t *testing.T) {
	t.Run("description wins", func(t *testing.T) {
		bt := stripesource.Transaction{Type: "payout", Description: "2024-12 Memberships", Created: 1764542899}
		if got := btPaymentRef(bt); got != "2024-12 Memberships" {
			t.Fatalf("btPaymentRef = %q, want %q", got, "2024-12 Memberships")
		}
	})

	t.Run("no description, valid created → dated manual-payout label (not 1970)", func(t *testing.T) {
		bt := stripesource.Transaction{Type: "payout", Created: 1764542899} // 2025-11
		got := btPaymentRef(bt)
		if !strings.HasPrefix(got, "Manual payout 2025-") {
			t.Fatalf("btPaymentRef = %q, want a 2025 Manual payout label", got)
		}
		if strings.Contains(got, "1970") {
			t.Fatalf("btPaymentRef = %q, should not be the epoch date", got)
		}
	})

	t.Run("automatic payout without description uses Auto label", func(t *testing.T) {
		bt := stripesource.Transaction{Type: "payout", PayoutAutomatic: true, PayoutArrivalDate: 1764542899}
		if got := btPaymentRef(bt); !strings.HasPrefix(got, "Auto payout 2025-") {
			t.Fatalf("btPaymentRef = %q, want an Auto payout label", got)
		}
	})
}

// TestPayoutStatementLabelsLeadsWithDescription locks in that a closed Stripe
// statement's name leads with the payout memo when present.
func TestPayoutStatementLabelsLeadsWithDescription(t *testing.T) {
	bt := stripesource.Transaction{Type: "payout", Description: "Donations 2025-11", Created: 1764542899, Net: -50000, Currency: "eur"}
	name, _ := payoutStatementLabels(bt)
	if !strings.HasPrefix(name, "Donations 2025-11 — ") {
		t.Fatalf("statement name = %q, want it to lead with the description", name)
	}
	if !strings.Contains(name, "Stripe payout") {
		t.Fatalf("statement name = %q, want it to retain the Stripe payout detail", name)
	}
}
