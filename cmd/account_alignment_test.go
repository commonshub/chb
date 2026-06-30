package cmd

import "testing"

func TestAccountAlignment(t *testing.T) {
	// Three-way agree → aligned.
	a := accountAlignment{currency: "EUR", local: 100, journal: 100, gl: 100, hasLocal: true, hasJournal: true, hasGL: true}
	if !a.linked() || !a.comparable() || !a.aligned() {
		t.Fatalf("expected linked, comparable, aligned; got %+v", a)
	}

	// Within a cent → still aligned.
	a2 := accountAlignment{currency: "EUR", local: 100.004, journal: 100, hasLocal: true, hasJournal: true}
	if !a2.aligned() {
		t.Errorf("sub-cent gap should be aligned")
	}

	// local vs journal diverge → misaligned, note names the widest pair.
	a3 := accountAlignment{currency: "EUR", local: 402.31, journal: 1035.39, hasLocal: true, hasJournal: true}
	if a3.aligned() {
		t.Errorf("633 EUR gap should be misaligned")
	}
	if got := a3.diffNote(); got != "local vs journal off by 633.08 EUR" {
		t.Errorf("diffNote = %q", got)
	}

	// Three legs, GL is the outlier → note reports the widest pair (journal vs GL).
	a4 := accountAlignment{currency: "EUR", local: 100, journal: 100, gl: 250, hasLocal: true, hasJournal: true, hasGL: true}
	if got := a4.diffNote(); got != "journal vs GL off by 150.00 EUR" && got != "local vs GL off by 150.00 EUR" {
		t.Errorf("diffNote = %q, want a 150 EUR GL gap", got)
	}

	// Only one leg → nothing to compare → trivially aligned, not comparable.
	a5 := accountAlignment{currency: "EUR", local: 100, hasLocal: true}
	if a5.comparable() {
		t.Errorf("single leg should not be comparable")
	}
	if !a5.aligned() {
		t.Errorf("single leg should be trivially aligned")
	}
	if a5.linked() {
		t.Errorf("local-only is not linked to a journal/GL")
	}

	// misalignedAccounts filters + sorts worst-first.
	aligns := map[string]accountAlignment{
		"ok":    a,
		"small": {slug: "small", currency: "EUR", local: 10, journal: 28.5, hasLocal: true, hasJournal: true},
		"big":   {slug: "big", currency: "EUR", local: 0, journal: 2000, hasLocal: true, hasJournal: true},
		"solo":  a5,
	}
	bad := misalignedAccounts(aligns)
	if len(bad) != 2 {
		t.Fatalf("want 2 misaligned, got %d", len(bad))
	}
	if bad[0].slug != "big" || bad[1].slug != "small" {
		t.Errorf("want worst-first [big, small], got [%s, %s]", bad[0].slug, bad[1].slug)
	}
}
