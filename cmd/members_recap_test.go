package cmd

import (
	"reflect"
	"testing"
	"time"
)

func TestDueRecapPeriods(t *testing.T) {
	at := func(s string) time.Time {
		d, _ := time.ParseInLocation("2006-01-02", s, BrusselsTZ())
		return d
	}
	ids := func(ps []recapPeriod) []string {
		var out []string
		for _, p := range ps {
			out = append(out, p.ID)
		}
		return out
	}
	cases := map[string][]string{
		"2026-11-01": {"2026-10"},
		"2026-11-03": {"2026-10"},
		"2026-11-04": nil,
		"2026-10-27": nil,
		"2026-10-01": {"2026-09", "2026-Q3"},
		"2027-01-02": {"2026-12", "2026-Q4", "2026"},
		"2026-04-01": {"2026-03", "2026-Q1"},
	}
	for day, want := range cases {
		if got := ids(dueRecapPeriods(at(day))); !reflect.DeepEqual(got, want) {
			t.Errorf("%s: got %v, want %v", day, got, want)
		}
	}
}

func TestParseRecapPeriod(t *testing.T) {
	for s, kind := range map[string]string{"2026-10": "monthly", "2026-Q4": "quarterly", "2026": "yearly"} {
		if p, err := parseRecapPeriod(s); err != nil || p.Kind != kind {
			t.Errorf("%s: %v %v", s, p, err)
		}
	}
	for _, s := range []string{"2026-13", "2026-Q5", "26", "2026-10-01"} {
		if _, err := parseRecapPeriod(s); err == nil {
			t.Errorf("%s: want error", s)
		}
	}
}

func TestRecapRecipientsAndPlan(t *testing.T) {
	str := func(s string) *string { return &s }
	members := []Member{
		{FirstName: "A", Status: "active", Accounts: MemberAccounts{Discord: str("111111111111111111")}},
		{FirstName: "B", Status: "grace", OdooPartnerID: 7},
		{FirstName: "C", Status: "active", StripeCustomerID: "cus_c"},
		{FirstName: "D", Status: "active", Accounts: MemberAccounts{Discord: str("someusername")}}, // a username, not an id
		{FirstName: "E", Status: "lapsed", Accounts: MemberAccounts{Discord: str("555555555555555555")}},
		{FirstName: "F", Status: "active", Accounts: MemberAccounts{Discord: str("666666666666666666")}},
	}
	links := map[string]string{"odoo:7": "222222222222222222", "stripe:cus_c": "333333333333333333"}
	rs, missing := recapRecipients(members, links, recapWebsiteLinks{})
	if len(rs) != 4 || missing != 1 {
		t.Fatalf("recipients %d missing %d", len(rs), missing)
	}

	subs := recapSubscribers{
		Default: RecapPrefs{Monthly: true},
		Members: map[string]RecapPrefs{
			"222222222222222222": {Monthly: true, Quarterly: true, Yearly: true},
			"333333333333333333": {},                // opted out
			"666666666666666666": {Quarterly: true}, // quarterly only
		},
	}
	periods := []recapPeriod{{"2026-09", "monthly"}, {"2026-Q3", "quarterly"}}
	ledger := map[string]RecapLedgerEntry{"111111111111111111:2026-09": {}}
	todo, optedOut, already := planRecaps(rs, periods, subs, ledger)
	var keys []string
	for _, s := range todo {
		keys = append(keys, s.key())
	}
	want := []string{"222222222222222222:2026-09", "222222222222222222:2026-Q3", "666666666666666666:2026-Q3"}
	if !reflect.DeepEqual(keys, want) || optedOut != 1 || already != 1 {
		t.Errorf("todo %v optedOut %d already %d", keys, optedOut, already)
	}
}

func TestRecapHashLinks(t *testing.T) {
	type link = websiteDiscordLink
	hl := hashDiscordLinks([]link{
		{"111111111111111111", "h1", "login", nil}, {"111111111111111111", "h1", "email", nil}, // login + application: same id
		{"222222222222222222", "h2", "", nil}, {"333333333333333333", "h2", "name", nil}, // ambiguous
		{"444444444444444444", "h4", "name", nil},
	})
	if !reflect.DeepEqual(hl, map[string]string{"h1": "111111111111111111", "h4": "444444444444444444"}) {
		t.Fatalf("hash links %v", hl)
	}
	members := []Member{
		{FirstName: "A", Status: "active", Accounts: MemberAccounts{EmailHash: "h1"}},
		{FirstName: "B", Status: "active", Accounts: MemberAccounts{EmailHash: "h2"}},                   // ambiguous: skipped
		{FirstName: "C", Status: "active", OdooPartnerID: 9, Accounts: MemberAccounts{EmailHash: "h4"}}, // hand map wins
	}
	rs, missing := recapRecipients(members, map[string]string{"odoo:9": "999999999999999999"}, recapWebsiteLinks{Hash: hl})
	if missing != 1 || len(rs) != 2 || rs[0].DiscordID != "111111111111111111" || rs[1].DiscordID != "999999999999999999" {
		t.Errorf("recipients %+v missing %d", rs, missing)
	}
}

func TestRecapCheckoutLinksMapBySubscription(t *testing.T) {
	paid := func(sub string) *websiteDiscordLinkPaid { return &websiteDiscordLinkPaid{Subscription: sub} }
	web := splitRecapWebsiteLinks([]websiteDiscordLink{
		// Paid from the dashboard, but typed another member's email at Stripe.
		{DiscordID: "111111111111111111", EmailHash: "hB", Source: "checkout", Paid: paid("sub_1RVSiqFAhaWeDyowXYZ")},
		// A checkout without a subscription falls back to the email hash.
		{DiscordID: "333333333333333333", EmailHash: "hC", Source: "checkout"},
	})
	members := []Member{
		{ID: "sub_1RVSiqFAha...", FirstName: "A", Status: "active", Accounts: MemberAccounts{EmailHash: "hA"}},
		{ID: "sub_1QoXXXFAha...", FirstName: "B", Status: "active", Accounts: MemberAccounts{EmailHash: "hB"}},
		{ID: "sub_1QoYYYFAha...", FirstName: "C", Status: "active", Accounts: MemberAccounts{EmailHash: "hC"}},
	}
	rs, missing := recapRecipients(members, nil, web)
	got := map[string]string{}
	for _, r := range rs {
		got[r.FirstName] = r.DiscordID
	}
	want := map[string]string{"A": "111111111111111111", "C": "333333333333333333"}
	if !reflect.DeepEqual(got, want) || missing != 1 {
		t.Fatalf("recipients %v missing %d (B, whose email was typed by someone else, must get no recap)", got, missing)
	}
}
