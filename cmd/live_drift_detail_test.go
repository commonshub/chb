package cmd

import (
	"strings"
	"testing"
)

func TestLiveDriftDetailTellsMirrorLagFromJournalDrift(t *testing.T) {
	acc := &AccountConfig{Slug: "stripe", OdooJournalID: 48}

	// Odoo == mirror, live ahead: that is new activity, not drift.
	got := liveDriftDetail(acc, 1475.47, 1475.47, 1603.42, "EUR", "Stripe")
	if strings.Contains(got, "⚠") || !strings.Contains(got, "behind live") || !strings.Contains(got, "127.95") {
		t.Errorf("mirror lag must read as informational, got: %q", got)
	}

	// Mirror == live, Odoo differs: the journal really drifted.
	got = liveDriftDetail(acc, 1474.82, 1603.42, 1603.42, "EUR", "Stripe")
	if !strings.Contains(got, "journal drift") || !strings.Contains(got, "-128.60") || !strings.Contains(got, "journals 48 fix") {
		t.Errorf("real drift must be flagged with the fix hint, got: %q", got)
	}

	// Both differ: report both numbers.
	got = liveDriftDetail(acc, 10, 20, 30, "EUR", "Stripe")
	if !strings.Contains(got, "journal drift") || !strings.Contains(got, "behind live") {
		t.Errorf("mixed case must report both gaps, got: %q", got)
	}
}
