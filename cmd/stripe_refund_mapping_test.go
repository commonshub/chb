package cmd

import (
	"encoding/json"
	"testing"
)

// loadDefaultOdooMappings reads the embedded cmd/defaults/odoo_mapping.json —
// the chart-of-accounts mapping shipped to every install — so these tests guard
// the file that actually ships, independent of the developer's ~/.chb settings.
func loadDefaultOdooMappings(t *testing.T) []OdooMapping {
	t.Helper()
	data, err := defaultSettingsFS.ReadFile("defaults/odoo_mapping.json")
	if err != nil {
		t.Fatalf("read embedded default mapping: %v", err)
	}
	var mappings []OdooMapping
	if err := json.Unmarshal(data, &mappings); err != nil {
		t.Fatalf("parse embedded default mapping: %v", err)
	}
	return mappings
}

// TestIncomeRefundsBookOnIncomeAccount locks in the fix for Stripe ticket/
// membership refunds landing on the 499000 suspense account. A refund is an
// OUTGOING (DEBIT) line that carries the original charge's income category
// (e.g. "ticket"). The income mappings used to be gated to direction:"in", so an
// outgoing refund matched no rule and fell through to the journal's default
// suspense account. They are now directionless, so a refund books as
// contra-revenue (negative) on the same income account it reverses.
func TestIncomeRefundsBookOnIncomeAccount(t *testing.T) {
	mappings := loadDefaultOdooMappings(t)
	cases := []struct {
		category string
		wantCode string
	}{
		{"ticket", "700150"},
		{"event_tickets", "700150"},
		{"membership", "730000"},
		{"donation", "740040"},
		{"sponsoring", "700110"},
		{"coworking", "700003"},
	}
	for _, c := range cases {
		t.Run(c.category+"_refund_out", func(t *testing.T) {
			// A refund is outgoing; the income category survives from the
			// original charge.
			tx := TransactionEntry{Type: "DEBIT", Category: c.category}
			m := LookupOdooMapping(mappings, tx)
			if m == nil {
				t.Fatalf("outgoing %q refund matched no mapping → would land on 499000 suspense", c.category)
			}
			if m.Set.AccountCode != c.wantCode {
				t.Fatalf("outgoing %q refund → %s, want %s", c.category, m.Set.AccountCode, c.wantCode)
			}
		})
		t.Run(c.category+"_sale_in", func(t *testing.T) {
			// The normal incoming sale must still resolve to the same account.
			tx := TransactionEntry{Type: "CREDIT", Category: c.category}
			m := LookupOdooMapping(mappings, tx)
			if m == nil || m.Set.AccountCode != c.wantCode {
				t.Fatalf("incoming %q → %v, want %s", c.category, m, c.wantCode)
			}
		})
	}
}

// TestCateringDrinksKeepDirectionalSplit guards that catering/drinks were NOT
// made directionless: they carry a genuine in (sale → 700002/700003) vs out
// (purchase → 604200) split that an unconditional rule would collapse.
func TestCateringDrinksKeepDirectionalSplit(t *testing.T) {
	mappings := loadDefaultOdooMappings(t)
	for _, cat := range []string{"catering", "drinks"} {
		in := LookupOdooMapping(mappings, TransactionEntry{Type: "CREDIT", Category: cat})
		out := LookupOdooMapping(mappings, TransactionEntry{Type: "DEBIT", Category: cat})
		if in == nil || out == nil {
			t.Fatalf("%s: expected both in and out mappings, got in=%v out=%v", cat, in, out)
		}
		if in.Set.AccountCode == out.Set.AccountCode {
			t.Fatalf("%s: in and out collapsed to the same account %s — purchase/sale split lost", cat, in.Set.AccountCode)
		}
	}
}
