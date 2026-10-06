package cmd

import (
	"testing"

	odoosource "github.com/CommonsHub/chb/providers/odoo"
)

func TestOdooMemberStatus(t *testing.T) {
	unpaid := func(due string) []odoosource.MembershipInvoice {
		return []odoosource.MembershipInvoice{{ID: 1, Date: due, DueDate: due, State: "posted", PaymentState: "not_paid", Amount: 10, Residual: 10}}
	}
	cases := []struct {
		name                    string
		sub                     providerSubscription
		ref                     string
		status, since, graceEnd string
	}{
		{"paid", providerSubscription{OdooState: "3_progress", Invoices: []odoosource.MembershipInvoice{{DueDate: "2026-09-01", State: "posted", PaymentState: "paid", Residual: 0}}}, "2026-10-06", "active", "", ""},
		{"unpaid, not yet due", providerSubscription{OdooState: "3_progress", Invoices: unpaid("2026-10-10")}, "2026-10-06", "active", "", ""},
		{"unpaid 5 days", providerSubscription{OdooState: "3_progress", Invoices: unpaid("2026-10-01")}, "2026-10-06", "grace", "2026-10-01", "2026-10-16"},
		{"unpaid 20 days", providerSubscription{OdooState: "3_progress", Invoices: unpaid("2026-09-16")}, "2026-10-06", "lapsed", "2026-10-01", ""},
		{"paused", providerSubscription{OdooState: "4_paused", CurrentPeriodEnd: "2026-10-01"}, "2026-10-06", "grace", "2026-10-01", "2026-10-16"},
		{"churned", providerSubscription{OdooState: "6_churn", EndDate: "2026-09-20"}, "2026-10-06", "lapsed", "2026-09-20", ""},
	}
	for _, c := range cases {
		s, since, ge := odooMemberStatus(c.sub, c.ref)
		if s != c.status || since != c.since || ge != c.graceEnd {
			t.Errorf("%s: got %s/%s/%s, want %s/%s/%s", c.name, s, since, ge, c.status, c.since, c.graceEnd)
		}
	}
}

func TestMergeOdooFirstAndMismatches(t *testing.T) {
	snaps := []providerSnapshot{
		{Provider: "stripe", Subscriptions: []providerSubscription{
			{ID: "sub_a", Source: "stripe", EmailHash: "a", FirstName: "Ann", Status: "active", Plan: "monthly", CreatedAt: "2025-01-01"},
			{ID: "sub_b", Source: "stripe", EmailHash: "b", FirstName: "Bob", Status: "active", Plan: "monthly", CreatedAt: "2025-01-02"},
		}},
		{Provider: "odoo", Subscriptions: []providerSubscription{
			{ID: "odoo-1", Source: "odoo", EmailHash: "a", FirstName: "Ann", OdooState: "6_churn", EndDate: "2026-10-02", Plan: "monthly", CreatedAt: "2025-01-01"},
			{ID: "odoo-2", Source: "odoo", EmailHash: "c", FirstName: "XL", LastName: "Collective SRL", IsOrganization: true, OdooState: "3_progress", Plan: "yearly", CreatedAt: "2024-01-01"},
			{ID: "odoo-4", Source: "odoo", EmailHash: "e", FirstName: "Vincent", LastName: "Doe", IsOrganization: true, OdooState: "3_progress", Plan: "yearly", CreatedAt: "2024-02-01"}, // an individual on the non-profit plan
			{ID: "odoo-5", Source: "odoo", EmailHash: "f", FirstName: "Psyche", IsOrganization: true, IsCompany: true, OdooState: "3_progress", Plan: "yearly", CreatedAt: "2024-03-01"},
			{ID: "odoo-3", Source: "odoo", EmailHash: "d", FirstName: "Old", OdooState: "6_churn", EndDate: "2026-08-01", CreatedAt: "2023-01-01"},
		}},
	}
	members, mism := mergeProviderSnapshotsAt(snaps, 2026, 10)
	byID := map[string]Member{}
	for _, m := range members {
		byID[m.ID] = m
	}
	if m, ok := byID["odoo-1"]; !ok || m.Status != "lapsed" {
		t.Errorf("Ann must come from Odoo (lapsed): %+v", members)
	}
	if _, ok := byID["sub_a"]; ok {
		t.Error("Ann must not be listed twice")
	}
	if _, ok := byID["odoo-3"]; ok {
		t.Error("a member who lapsed before the month is not listed")
	}
	if byID["odoo-2"].OrganizationName != "XL Collective SRL" {
		t.Errorf("organisation name: %q", byID["odoo-2"].OrganizationName)
	}
	kinds := map[string]bool{}
	for _, x := range mism {
		kinds[x.Kind+":"+x.EmailHash] = true
	}
	if !kinds["status_differs:a"] || !kinds["no_odoo_subscription:b"] || len(mism) != 2 {
		t.Errorf("mismatches: %+v", mism)
	}

	f := MembersOutputFile{Members: members, Mismatches: mism}
	pub := membersFileForAudience(f, AudiencePublic)
	if byID["odoo-4"].OrganizationName != "" {
		t.Error("an individual on the non-profit plan must not get an organisation name")
	}
	names := map[string]bool{}
	for _, m := range pub.Members {
		names[m.OrganizationName] = true
	}
	if len(pub.Members) != 2 || !names["XL Collective SRL"] || !names["Psyche"] || pub.Mismatches != nil {
		t.Errorf("public: organisations only, no mismatches: %+v", pub)
	}
	mem := membersFileForAudience(f, AudienceMembers)
	for _, m := range mem.Members {
		if m.Status == "lapsed" || m.Accounts.EmailHash != "" || m.OdooPartnerID != 0 {
			t.Errorf("members tier leaks: %+v", m)
		}
	}
	if mem.Mismatches != nil {
		t.Error("members tier must not carry mismatches")
	}
}

func TestStripeCancelledButPaidThrough(t *testing.T) {
	snaps := []providerSnapshot{{Provider: "stripe", Subscriptions: []providerSubscription{
		{ID: "sub_r", Source: "stripe", EmailHash: "r", FirstName: "Rebeka", Status: "canceled", CurrentPeriodEnd: "2099-10-10", CreatedAt: "2025-01-01"},
	}}}
	members, _ := mergeProviderSnapshotsAt(snaps, 2026, 10)
	if len(members) != 1 || members[0].Status != "active" || members[0].StatusReason != "" {
		t.Fatalf("a cancelled subscription still paid through is active: %+v", members)
	}
}
