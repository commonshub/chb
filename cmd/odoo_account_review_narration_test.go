package cmd

import "testing"

// Vendor bank narrations on an Odoo-source-of-truth journal (KBC) must resolve
// to a proposed account straight from the pulled journal data (payment_ref),
// without any CSV/local-mirror lookup.
func TestProposeAccountFromOdooNarration(t *testing.T) {
	cz := &Categorizer{
		categories: map[string]CategoryDef{},
		rules: []Rule{
			{Match: RuleMatch{Description: "*hetzner*"}, Assign: RuleAssign{Category: "webservice"}},
			// Proximus / Electrabel are gated to the KBC provider — the synthetic
			// narration tx must carry that provider or these never match.
			{Match: RuleMatch{Description: "*proximus*", Provider: "kbcbrussels"}, Assign: RuleAssign{Category: "internet"}},
			{Match: RuleMatch{Description: "*electrabel*", Provider: "kbcbrussels"}, Assign: RuleAssign{Category: "utilities"}},
		},
	}
	mappings := []OdooMapping{
		{Match: OdooMappingMatch{Category: "webservice", Direction: "out"}, Set: OdooMappingResult{AccountCode: "616040"}},
		{Match: OdooMappingMatch{Category: "internet", Direction: "out"}, Set: OdooMappingResult{AccountCode: "616030"}},
		{Match: OdooMappingMatch{Category: "utilities", Direction: "out"}, Set: OdooMappingResult{AccountCode: "612011"}},
	}

	cases := []struct {
		provider string
		ref      string
		amount   float64
		want     string
	}{
		{"kbcbrussels", "EUROPEAN DIRECT DEBIT CREDITOR : HETZNER ONLINE GMBH REFERENCE : CUSTOMER NO.", -54.45, "616040"},
		{"kbcbrussels", "PROXIMUS Direct debit 2015296442 P000679520 007604074920", -54.45, "616030"},
		{"kbcbrussels", "EUROPEAN DIRECT DEBIT CREDITOR : ELECTRABEL", -120.00, "612011"},
		// Same Proximus narration but provider unknown (journal not linked) → the
		// provider-gated rule won't fire. This is the exact 2026-06-26 bug.
		{"", "PROXIMUS Direct debit 2015296442 P000679520 007604074920", -54.45, ""},
		{"kbcbrussels", "", -10, ""},                     // no narration → no proposal
		{"kbcbrussels", "SOME UNKNOWN MERCHANT", -10, ""}, // unmatched → no proposal
		{"kbcbrussels", "HETZNER refund", 54.45, ""},      // incoming → dir=out rule doesn't apply
	}
	for _, c := range cases {
		got := proposeAccountFromOdooNarration(cz, mappings, c.provider, c.ref, c.amount)
		if got != c.want {
			t.Errorf("provider=%q ref=%q amount=%.2f: got %q, want %q", c.provider, c.ref, c.amount, got, c.want)
		}
	}
}
