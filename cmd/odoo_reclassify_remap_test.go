package cmd

import "testing"

func TestIsSuspenseAccountCode(t *testing.T) {
	for _, c := range []struct {
		code string
		want bool
	}{
		{"499000", true}, // comptes d'attente / suspense
		{"", true},       // no account = uncategorised
		{"  499000 ", true},
		{"700003", false}, // a real income account (manual/assigned)
		{"612300", false},
		{"550003", false},
	} {
		if got := isSuspenseAccountCode(c.code); got != c.want {
			t.Errorf("isSuspenseAccountCode(%q) = %v, want %v", c.code, got, c.want)
		}
	}
}

// A KBC cp-order drink line (Odoo-only narration) resolves to 700003 via the
// rules — the categorization that the reclassification step runs on the Odoo
// narration when the local mirror has no category.
func TestCpOrderNarrationResolvesToDrinks(t *testing.T) {
	cz := &Categorizer{
		categories: map[string]CategoryDef{},
		rules: []Rule{
			{Match: RuleMatch{Description: "*CP-ORDER-*"}, Assign: RuleAssign{Category: "drinks", Collective: "commonshub"}},
		},
	}
	mappings := []OdooMapping{
		{Match: OdooMappingMatch{Category: "drinks", Direction: "in"}, Set: OdooMappingResult{AccountCode: "700003"}},
	}
	// incoming drink-order payment (amount > 0 → CREDIT/in).
	got := proposeAccountFromOdooNarration(cz, mappings, "kbcbrussels",
		"VEREECKE ALAIN Credit transfer BE14 7350 7124 6383 cp-order-44641", 4.0)
	if got != "700003" {
		t.Errorf("cp-order narration → %q, want 700003", got)
	}
	// outgoing (amount < 0) must NOT match the direction:in drinks rule.
	if got := proposeAccountFromOdooNarration(cz, mappings, "kbcbrussels", "refund cp-order-1", -4.0); got != "" {
		t.Errorf("outgoing cp-order → %q, want empty (no in-rule match)", got)
	}
}
