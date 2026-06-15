package cmd

import "testing"

func TestAnalyticGroupKey(t *testing.T) {
	// "Open Letter" and "Openletter" on the same plan collapse to one group.
	if analyticGroupKey(3, "Open Letter") != analyticGroupKey(3, "Openletter") {
		t.Fatal("Open Letter / Openletter should share a group key on plan 3")
	}
	// Same normalized name on different plans must NOT collide.
	if analyticGroupKey(8, "Donation") == analyticGroupKey(13, "Donations") {
		t.Fatal("cross-plan names must not share a group key")
	}
}

func TestNameMatchesAccount(t *testing.T) {
	accounts := []analyticExistingAccount{{ID: 17, PlanID: 3, Name: "Open Letter"}}
	// slug openletter -> pretty "Openletter" -> NOT a case-insensitive match
	// of "Open Letter" (space differs) -> sync would recreate -> bind needed.
	if nameMatchesAccount(analyticAccountSpec{Name: "Openletter", PlanID: 3}, accounts) {
		t.Fatal("Openletter must NOT name-match Open Letter")
	}
	// Acronyms only differ by case, which analyticAccountKey already folds, so
	// they DO name-match and need no binding.
	osv := []analyticExistingAccount{{ID: 140, PlanID: 3, Name: "OSV"}}
	if !nameMatchesAccount(analyticAccountSpec{Name: "Osv", PlanID: 3}, osv) {
		t.Fatal("Osv must name-match OSV (case-folded)")
	}
}

// buildAnalyticFixGroup must keep the account with entries, delete the empty
// twin, and bind the slug whose pretty-name doesn't match the survivor.
func TestBuildAnalyticFixGroup_DeletesEmptyTwinAndBinds(t *testing.T) {
	accounts := []analyticExistingAccount{
		{ID: 131, PlanID: 3, Name: "Openletter"}, // chb-created twin, empty
		{ID: 17, PlanID: 3, Name: "Open Letter"}, // manual, has entries
	}
	specs := []analyticAccountSpec{{Slug: "openletter", Name: "Openletter", PlanID: 3, Kind: "collective"}}
	counts := map[int]int{17: 12, 131: 0}
	entryCount := func(id int) int { return counts[id] }

	grp := buildAnalyticFixGroup(accounts, specs, map[int]string{3: "collective"}, true, entryCount)

	if grp.Survivor.ID != 17 {
		t.Fatalf("survivor = #%d, want #17 (the one with entries)", grp.Survivor.ID)
	}
	var deleted []int
	for _, r := range grp.Rows {
		if r.Action == "delete" {
			deleted = append(deleted, r.Account.ID)
		}
	}
	if len(deleted) != 1 || deleted[0] != 131 {
		t.Fatalf("deleted = %v, want [131]", deleted)
	}
	if len(grp.Bindings) != 1 || grp.Bindings[0].Slug != "openletter" || grp.Bindings[0].Kind != "collective" {
		t.Fatalf("bindings = %+v, want collective:openletter", grp.Bindings)
	}
	// Proposed canonical name prefers the human-spaced variant.
	if grp.NewName != "Open Letter" {
		t.Fatalf("NewName = %q, want \"Open Letter\"", grp.NewName)
	}
}

// When both duplicates carry entries, neither is deleted and the group is
// flagged for manual merge — but the survivor is still bound.
func TestBuildAnalyticFixGroup_BothHaveEntries(t *testing.T) {
	accounts := []analyticExistingAccount{
		{ID: 112, PlanID: 13, Name: "Grant"},
		{ID: 113, PlanID: 13, Name: "Grants"},
	}
	specs := []analyticAccountSpec{
		{Slug: "grant", Name: "Grant", PlanID: 13, Kind: "category"},
		{Slug: "grants", Name: "Grants", PlanID: 13, Kind: "category"},
	}
	counts := map[int]int{112: 3, 113: 7}
	grp := buildAnalyticFixGroup(accounts, specs, map[int]string{13: "income"}, true, func(id int) int { return counts[id] })

	if grp.Survivor.ID != 113 {
		t.Fatalf("survivor = #%d, want #113 (more entries)", grp.Survivor.ID)
	}
	for _, r := range grp.Rows {
		if r.Action == "delete" {
			t.Fatalf("nothing should be deleted when both have entries, got delete on #%d", r.Account.ID)
		}
	}
	if grp.Warn == "" || !grp.manualMerge() {
		t.Fatal("expected a manual-merge warning when both carry entries")
	}
	// Every slug in the cluster is bound to the survivor by id (robust to rename).
	if len(grp.Bindings) != 2 {
		t.Fatalf("bindings = %+v, want both grant and grants", grp.Bindings)
	}
	// Singular form is proposed as the canonical name.
	if grp.NewName != "Grant" {
		t.Fatalf("NewName = %q, want \"Grant\"", grp.NewName)
	}
}

func TestProposeCanonicalAnalyticName(t *testing.T) {
	acc := func(names ...string) []analyticExistingAccount {
		var out []analyticExistingAccount
		for i, n := range names {
			out = append(out, analyticExistingAccount{ID: i + 1, PlanID: 3, Name: n})
		}
		return out
	}
	cases := []struct {
		names []string
		want  string
	}{
		{[]string{"Grant", "Grants"}, "Grant"},                 // drop plural twin
		{[]string{"Grants", "Grant"}, "Grant"},                 // order-independent
		{[]string{"Rentals", "Rental"}, "Rental"},              // drop plural twin
		{[]string{"Open Letter", "Openletter"}, "Open Letter"}, // prefer spaced
		{[]string{"Stripe Fee", "Stripe Fees"}, "Stripe Fee"},  // plural twin w/ space
	}
	for _, c := range cases {
		got := proposeCanonicalAnalyticName(acc(c.names...), nil)
		if got != c.want {
			t.Errorf("proposeCanonicalAnalyticName(%v) = %q, want %q", c.names, got, c.want)
		}
	}
}

func TestAnalyticMergeHeader(t *testing.T) {
	grp := analyticFixGroup{
		Survivor: analyticExistingAccount{ID: 113, Name: "Grants"},
		Rows: []analyticFixAccountRow{
			{Account: analyticExistingAccount{ID: 113, Name: "Grants"}, Action: "keep"},
			{Account: analyticExistingAccount{ID: 112, Name: "Grant"}, Action: "delete"},
		},
	}
	got := analyticMergeHeader(grp)
	want := "Merging Grants and Grant (removing #112, keeping #113)"
	if got != want {
		t.Fatalf("analyticMergeHeader = %q, want %q", got, want)
	}
}

func TestResolveAnalyticBindings_DropsStale(t *testing.T) {
	byID := map[int]analyticExistingAccount{
		17: {ID: 17, PlanID: 3, Name: "Open Letter"},
	}
	links := OdooAnalyticLinks{
		"collective:openletter": 17,  // live
		"collective:ghost":      999, // account no longer exists
	}
	resolved := resolveAnalyticBindings(links, byID)
	if got, ok := resolved["collective:openletter"]; !ok || got.ID != 17 {
		t.Fatalf("live binding lost: %+v", resolved)
	}
	if _, ok := resolved["collective:ghost"]; ok {
		t.Fatal("stale binding to a deleted account must be dropped")
	}
}
