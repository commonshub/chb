package cmd

import "testing"

func TestNormalizeAnalyticName(t *testing.T) {
	cases := map[string]string{
		"Block 26":         "block26",
		"Block26":          "block26",
		"Accounting costs": "accountingcost",
		"Accounting":       "accounting",
		"Grants":           "grant",
		"Grant":            "grant",
		"Open Letter":      "openletter",
		"Weaving Wolves":   "weavingwolve",
		"Stripe Fees":      "stripefee",
	}
	for in, want := range cases {
		if got := normalizeAnalyticName(in); got != want {
			t.Errorf("normalizeAnalyticName(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestSimilarAnalyticAccount(t *testing.T) {
	existing := []analyticExistingAccount{
		{ID: 78, PlanID: 3, Name: "Block 26"},
		{ID: 17, PlanID: 3, Name: "Open Letter"},
		{ID: 15, PlanID: 8, Name: "Accounting costs"},
		{ID: 113, PlanID: 13, Name: "Grants"},
	}

	if sim, ok := similarAnalyticAccount(analyticAccountSpec{Name: "Block26", PlanID: 3}, existing); !ok || sim.ID != 78 {
		t.Fatalf("Block26 should match Block 26 (#78), got %+v ok=%v", sim, ok)
	}
	if sim, ok := similarAnalyticAccount(analyticAccountSpec{Name: "Openletter", PlanID: 3}, existing); !ok || sim.ID != 17 {
		t.Fatalf("Openletter should match Open Letter (#17), got %+v ok=%v", sim, ok)
	}
	if sim, ok := similarAnalyticAccount(analyticAccountSpec{Name: "Grant", PlanID: 13}, existing); !ok || sim.ID != 113 {
		t.Fatalf("Grant should match Grants (#113), got %+v ok=%v", sim, ok)
	}
	// Same normalized name on a DIFFERENT plan must not match (Donation on
	// costs vs Donations on income is an intentional directional split).
	if _, ok := similarAnalyticAccount(analyticAccountSpec{Name: "Grants", PlanID: 8}, existing); ok {
		t.Fatal("cross-plan names must not be flagged as near-duplicates")
	}
	// "Accounting" vs "Accounting costs" differ by a whole word, not just
	// case/spacing/plural — out of scope for the strict matcher.
	if _, ok := similarAnalyticAccount(analyticAccountSpec{Name: "Accounting", PlanID: 8}, existing); ok {
		t.Fatal("Accounting should not strictly match Accounting costs")
	}
}

// ensureOdooAnalyticAccounts with createMissing=false must reuse existing
// accounts and silently drop the rest without any RPC (creds are nil-safe
// here because the create branch is never reached).
func TestEnsureOdooAnalyticAccountsSkipsCreatesWhenDeclined(t *testing.T) {
	existing := map[string]int{
		analyticAccountKey(3, "Block26"): 78,
	}
	specs := []analyticAccountSpec{
		{Slug: "block26", Name: "Block26", PlanID: 3},
		{Slug: "newcollective", Name: "Newcollective", PlanID: 3},
	}
	out, err := ensureOdooAnalyticAccounts(nil, 0, specs, existing, nil, false)
	if err != nil {
		t.Fatalf("ensureOdooAnalyticAccounts: %v", err)
	}
	if len(out) != 1 || out[0].AccountID != 78 || out[0].Slug != "block26" {
		t.Fatalf("expected only the existing account to be returned, got %+v", out)
	}
}

// A live binding must reuse the bound account by id — before any name-match
// and without creating — and surface the account's REAL Odoo name (not the
// slug-derived one), even when the slug's pretty-name differs.
func TestEnsureOdooAnalyticAccountsReusesByBinding(t *testing.T) {
	// No name-match entry: "Open Letter" would not match slug-derived "Openletter".
	existing := map[string]int{}
	resolved := map[string]analyticExistingAccount{
		analyticLinkKey("collective", "openletter"): {ID: 17, PlanID: 3, Name: "Open Letter"},
	}
	specs := []analyticAccountSpec{
		{Slug: "openletter", Name: "Openletter", PlanID: 3, Kind: "collective"},
	}
	// createMissing=false would normally skip an un-found spec; the binding
	// must still resolve it. nil creds are safe because no create runs.
	out, err := ensureOdooAnalyticAccounts(nil, 0, specs, existing, resolved, false)
	if err != nil {
		t.Fatalf("ensureOdooAnalyticAccounts: %v", err)
	}
	if len(out) != 1 || out[0].AccountID != 17 {
		t.Fatalf("expected binding to reuse #17, got %+v", out)
	}
	if out[0].Name != "Open Letter" {
		t.Fatalf("cache should carry the real Odoo name, got %q", out[0].Name)
	}
}
