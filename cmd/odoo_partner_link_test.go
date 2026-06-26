package cmd

import "testing"

func TestPlanPartnerLinks(t *testing.T) {
	// Existing partners: IBAN BE111 → #10; name "ACME" (any case) → #20.
	bankResolver := func(acc string) int {
		if normalizeBankAccountNumber(acc) == "BE111" {
			return 10
		}
		return 0
	}
	nameResolver := func(name string) int {
		switch name {
		case "ACME", "acme", "Acme":
			return 20
		}
		return 0
	}

	lines := []noPartnerLine{
		{ID: 1, AccountNumber: "BE 1 11"},          // bank match → #10
		{ID: 2, Name: "acme"},                      // name match (case-insensitive) → #20
		{ID: 3, Name: "Acme", AccountNumber: "X1"}, // bank miss, name match → #20
		{ID: 4, Name: "Bob's Bakery", AccountNumber: "BE222"}, // new partner (bank+name miss)
		{ID: 5, Name: "Bob's Bakery", AccountNumber: "BE333"}, // same new partner, 2nd account
		{ID: 6, AccountNumber: "BE444"},            // new partner, named after account
		{ID: 7},                                    // no name, no account → unmatchable
	}

	plan := planPartnerLinks(lines, bankResolver, nameResolver)

	if plan.NoPartner != 7 {
		t.Fatalf("NoPartner = %d, want 7", plan.NoPartner)
	}
	// Existing: lines 1,2,3 → partners {10,20}.
	if len(plan.ToExisting) != 3 {
		t.Fatalf("ToExisting = %d, want 3", len(plan.ToExisting))
	}
	if plan.ToExisting[1].PartnerID != 10 || plan.ToExisting[2].PartnerID != 20 || plan.ToExisting[3].PartnerID != 20 {
		t.Fatalf("existing mapping wrong: %v", plan.ToExisting)
	}
	// Line 1 matched by bank → nothing to attach; line 3 matched by name with a
	// free account "X1" → attach it; line 2 had no account.
	if plan.ToExisting[1].AttachAccount != "" {
		t.Fatalf("bank-matched line must not attach: %q", plan.ToExisting[1].AttachAccount)
	}
	if plan.ToExisting[3].AttachAccount != "X1" {
		t.Fatalf("name-matched line should attach X1, got %q", plan.ToExisting[3].AttachAccount)
	}
	if plan.ToExisting[2].AttachAccount != "" {
		t.Fatalf("line 2 has no account to attach, got %q", plan.ToExisting[2].AttachAccount)
	}
	if got := plan.existingPartnerCount(); got != 2 {
		t.Fatalf("existingPartnerCount = %d, want 2", got)
	}
	// New: "Bob's Bakery" (lines 4,5 with two accounts) + the account-named one (line 6).
	if len(plan.NewGroups) != 2 {
		t.Fatalf("NewGroups = %d, want 2: %v", len(plan.NewGroups), plan.NewGroups)
	}
	if got := plan.newLineCount(); got != 3 {
		t.Fatalf("newLineCount = %d, want 3", got)
	}
	bob := plan.NewGroups["name:bob's bakery"]
	if bob == nil {
		t.Fatalf("missing Bob's Bakery group: %v", plan.NewGroups)
	}
	if len(bob.LineIDs) != 2 {
		t.Fatalf("Bob lines = %v, want [4 5]", bob.LineIDs)
	}
	if !bob.Accounts["BE222"] || !bob.Accounts["BE333"] || len(bob.Accounts) != 2 {
		t.Fatalf("Bob accounts = %v, want BE222+BE333", bob.Accounts)
	}
	// Line 7 has no signal.
	if len(plan.Unmatchable) != 1 || plan.Unmatchable[0] != 7 {
		t.Fatalf("Unmatchable = %v, want [7]", plan.Unmatchable)
	}
}

func TestPlanPartnerLinksAllResolved(t *testing.T) {
	always := func(string) int { return 99 }
	plan := planPartnerLinks([]noPartnerLine{{ID: 1, Name: "X"}}, always, always)
	if !plan.hasWork() || len(plan.NewGroups) != 0 {
		t.Fatalf("expected only existing links, got %+v", plan)
	}
}

func TestStripeCustomerFromNarrationStripsHTML(t *testing.T) {
	// Odoo stores narration as HTML — the parser must strip the <p> wrapper.
	narr := `<p>{"customerName":"George Binette","stripeMetadata":{"stripeCustomerId":"cus_ABC123"}}</p>`
	name, cid := stripeCustomerFromNarration(narr)
	if name != "George Binette" {
		t.Fatalf("name = %q, want George Binette", name)
	}
	if cid != "cus_ABC123" {
		t.Fatalf("customerID = %q, want cus_ABC123", cid)
	}
	// Plain JSON (no wrapper) still works.
	if n, _ := stripeCustomerFromNarration(`{"customerName":"X"}`); n != "X" {
		t.Fatalf("plain JSON name = %q, want X", n)
	}
	// Garbage → empty, no panic.
	if n, c := stripeCustomerFromNarration("<p>not json</p>"); n != "" || c != "" {
		t.Fatalf("garbage = %q/%q, want empty", n, c)
	}
}
