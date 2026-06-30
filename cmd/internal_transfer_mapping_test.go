package cmd

import "testing"

// TestLookupOdooMappingInternalTransfer locks in that an internal transfer
// resolves to the internal-transfer account even when generate left its category
// blank (Type INTERNAL only) — so the push stamps the account and `fix` can move
// a mis-accounted internal transfer to it.
func TestLookupOdooMappingInternalTransfer(t *testing.T) {
	mappings := []OdooMapping{
		{Match: OdooMappingMatch{Category: "internal_transfer"}, Set: OdooMappingResult{AccountCode: "580000"}},
		{Match: OdooMappingMatch{Category: "donation"}, Set: OdooMappingResult{AccountCode: "740040"}},
	}

	t.Run("INTERNAL with no category → 580000", func(t *testing.T) {
		for _, dir := range []string{"INTERNAL"} {
			tx := TransactionEntry{Type: dir}
			m := LookupOdooMapping(mappings, tx)
			if m == nil || m.Set.AccountCode != "580000" {
				t.Fatalf("INTERNAL tx resolved to %v, want 580000", m)
			}
		}
	})

	t.Run("applyOdooMapping stamps AccountCode on an INTERNAL tx", func(t *testing.T) {
		tx := TransactionEntry{Type: "INTERNAL"}
		applyOdooMapping(mappings, &tx)
		if tx.AccountCode != "580000" {
			t.Fatalf("AccountCode = %q, want 580000", tx.AccountCode)
		}
	})

	t.Run("non-internal uncategorized tx still resolves to nothing", func(t *testing.T) {
		if m := LookupOdooMapping(mappings, TransactionEntry{Type: "CREDIT"}); m != nil {
			t.Fatalf("uncategorized CREDIT resolved to %v, want nil", m)
		}
	})

	t.Run("explicit category on an INTERNAL tx is honoured", func(t *testing.T) {
		tx := TransactionEntry{Type: "INTERNAL", Category: "donation"}
		m := LookupOdooMapping(mappings, tx)
		if m == nil || m.Set.AccountCode != "740040" {
			t.Fatalf("INTERNAL+donation resolved to %v, want 740040", m)
		}
	})
}
