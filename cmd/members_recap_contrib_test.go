package cmd

import (
	"reflect"
	"testing"
)

func TestRecapPeriodRange(t *testing.T) {
	for id, want := range map[string][2]string{
		"2026-11": {"2026-11-01", "2026-12-01"},
		"2026-Q4": {"2026-10-01", "2027-01-01"},
		"2026":    {"2026-01-01", "2027-01-01"},
	} {
		p, _ := parseRecapPeriod(id)
		if s, e := recapPeriodRange(p); s != want[0] || e != want[1] {
			t.Errorf("%s: %s–%s", id, s, e)
		}
	}
	q, _ := parseRecapPeriod("2026-Q4")
	if got := recapPeriodMonths([]recapPeriod{q}); !reflect.DeepEqual(got, []string{"2026-10", "2026-11", "2026-12"}) {
		t.Errorf("months %v", got)
	}
}

func TestRecapContribution(t *testing.T) {
	ix := &recapContribIndex{
		partners: map[int]recapContribPartner{
			10: {Name: "Ann", Commercial: 10},
			11: {Name: "Ann (contact)", Commercial: 50},
			50: {Name: "Acme SRL", IsCompany: true, Commercial: 50},
			60: {Name: "Bot Org", IsCompany: true, Commercial: 60},
			20: {Name: "Bob", Commercial: 20},
		},
		partnersByHash: map[string][]int{"hA": {11}},
		payments: []recapInvoicePayment{
			{Partner: 10, Commercial: 10, Date: "2026-11-05", Amount: 50, Description: "Invoice 2026_RCC_104: Satoshi room"}, // own invoice
			{Partner: 10, Commercial: 10, Date: "2026-11-06", Amount: 30, Stripe: true},                                      // counted via Stripe
			{Partner: 10, Commercial: 10, Date: "2026-10-31", Amount: 99},                                                    // outside the period
			{Partner: 10, Commercial: 10, Date: "2026-11-20", Amount: -5},                                                    // credit note
			{Partner: 11, Commercial: 50, Date: "2026-11-10", Amount: 200},                                                   // as Acme's contact
			{Partner: 50, Commercial: 50, Date: "2026-11-12", Amount: 100, Stripe: true},                                     // Acme
			{Partner: 60, Commercial: 60, Date: "2026-11-12", Amount: 70},
		},
		stripeByHash: map[string][]recapStripeCharge{"hA": {
			{Date: "2026-11-02", At: "2026-11-02T10:00:00+01:00", Amount: 10, Kind: "charge", Category: "membership"},
			{Date: "2026-11-03", At: "2026-11-03T10:00:00+01:00", Amount: -10, Kind: "refund"},
			{Date: "2026-11-30", At: "2026-11-30T10:00:00+01:00", Amount: 30, Kind: "charge", Category: "donation", Product: "Donation to the Commons Hub fridge"},
		}},
		invoiceOrgs: map[string][]int{"111111111111111111": {60}, "222222222222222222": {60, 50}},
	}
	p, _ := parseRecapPeriod("2026-11")
	ann := recapRecipient{OdooPartnerID: 10, EmailHash: "hA", FirstName: "Ann", Interval: "month"}
	c := ix.contribution(ann, p)
	if c.Personal != 75 || c.OrgName != "Acme SRL" || c.Org != 300 {
		t.Errorf("Ann %+v", c)
	}
	wantEuro := []recapEuroTransaction{
		{"2026-11-02T10:00:00+01:00", -10, "Membership (monthly)"},
		{"2026-11-03T10:00:00+01:00", 10, "Refund"},
		{"2026-11-05T12:00:00+01:00", -50, "Invoice 2026_RCC_104: Satoshi room"},
		{"2026-11-20T12:00:00+01:00", 5, ""},
		{"2026-11-30T10:00:00+01:00", -30, "Donation to the Commons Hub fridge"},
	}
	if !reflect.DeepEqual(c.Euro, wantEuro) {
		t.Errorf("euro %+v", c.Euro)
	}
	// No company in Odoo: the bot's invoice-orgs names one (exactly one).
	bob := recapRecipient{DiscordID: "111111111111111111", OdooPartnerID: 20}
	if c := ix.contribution(bob, p); c.Personal != 0 || c.OrgName != "Bot Org" || c.Org != 70 || len(c.Euro) != 0 {
		t.Errorf("Bob %+v", c)
	}
	two := recapRecipient{DiscordID: "222222222222222222"}
	if c := ix.contribution(two, p); c.OrgName != "" {
		t.Errorf("two orgs must not be guessed: %+v", c)
	}
	req := newRecapRequest(recapRecipient{EmailHash: "hA", FirstName: "Ann"}, "2026-11", &recapContribution{Personal: 12.5, OrgName: "Acme SRL"})
	if req.EmailHash != "hA" || req.Name != "Ann" || req.DiscordID != "" || *req.Contribution != 12.5 || req.OrgName != "Acme SRL" || *req.OrgContribution != 0 {
		t.Errorf("request %+v", req)
	}
	req = newRecapRequest(recapRecipient{DiscordID: "111111111111111111", EmailHash: "hA"}, "2026-11", &recapContribution{})
	if req.DiscordID == "" || req.EmailHash != "" || req.Contribution == nil || *req.Contribution != 0 || req.OrgContribution != nil {
		t.Errorf("request %+v", req)
	}
}
