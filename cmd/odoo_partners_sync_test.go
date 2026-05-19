package cmd

import "testing"

func TestResolveOdooPartnerFromLocalIndexMatchesEmailThenName(t *testing.T) {
	idx := &odooPartnerIndex{
		byEmail: map[string][]OdooPartner{
			"jane@example.com": {{ID: 42, Name: "Jane Donor", Email: "jane@example.com", Active: true}},
		},
		byName: map[string][]OdooPartner{
			"jane donor": {{ID: 42, Name: "Jane Donor", Email: "jane@example.com", Active: true}},
			"sam donor":  {{ID: 77, Name: "Sam Donor", Active: true}},
		},
	}
	stats := &syncStats{}
	cache := map[string]int{}

	if got := resolveOdooPartnerFromLocalIndex(idx, "Jane Donor", "jane@example.com", cache, stats); got != 42 {
		t.Fatalf("email match = %d, want 42", got)
	}
	if got := resolveOdooPartnerFromLocalIndex(idx, "Sam   Donor", "", cache, stats); got != 77 {
		t.Fatalf("name match = %d, want 77", got)
	}
	if stats.PartnersMatched != 2 {
		t.Fatalf("PartnersMatched = %d, want 2", stats.PartnersMatched)
	}
}

func TestResolveOdooPartnerFromLocalIndexCountsWouldCreate(t *testing.T) {
	idx := &odooPartnerIndex{
		byEmail: map[string][]OdooPartner{},
		byName:  map[string][]OdooPartner{},
	}
	stats := &syncStats{}

	if got := resolveOdooPartnerFromLocalIndex(idx, "New Donor", "new@example.com", map[string]int{}, stats); got != 0 {
		t.Fatalf("missing partner id = %d, want 0", got)
	}
	if stats.PartnersCreated != 1 {
		t.Fatalf("PartnersCreated = %d, want 1", stats.PartnersCreated)
	}
}

func TestResolveOdooPartnerFromLocalIndexCountsAmbiguous(t *testing.T) {
	idx := &odooPartnerIndex{
		byEmail: map[string][]OdooPartner{},
		byName: map[string][]OdooPartner{
			"same name": {
				{ID: 1, Name: "Same Name", Active: true},
				{ID: 2, Name: "Same Name", Active: true},
			},
		},
	}
	stats := &syncStats{}

	if got := resolveOdooPartnerFromLocalIndex(idx, "Same Name", "", map[string]int{}, stats); got != 0 {
		t.Fatalf("ambiguous partner id = %d, want 0", got)
	}
	if stats.PartnersSkipped != 1 {
		t.Fatalf("PartnersSkipped = %d, want 1", stats.PartnersSkipped)
	}
}

func TestLookupOdooPartnerFromLocalIndexDoesNotCountMissingAsCreate(t *testing.T) {
	idx := &odooPartnerIndex{
		byEmail: map[string][]OdooPartner{},
		byName: map[string][]OdooPartner{
			"known donor": {{ID: 9, Name: "Known Donor", Active: true}},
		},
	}
	if id, ambiguous := lookupOdooPartnerFromLocalIndex(idx, "Known Donor", ""); id != 9 || ambiguous {
		t.Fatalf("known lookup = (%d, %v), want (9, false)", id, ambiguous)
	}
	if id, ambiguous := lookupOdooPartnerFromLocalIndex(idx, "New Donor", ""); id != 0 || ambiguous {
		t.Fatalf("missing lookup = (%d, %v), want (0, false)", id, ambiguous)
	}
}
