package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func sp(s string) *string { return &s }

func TestMembersFileForAudience(t *testing.T) {
	full := MembersOutputFile{
		Summary: MembersSummary{TotalMembers: 2, ActiveMembers: 2},
		Members: []Member{{
			ID: "sub_1", FirstName: "Jane", Plan: "monthly", Status: "active",
			Accounts:        MemberAccounts{EmailHash: "abc", Discord: sp("42")},
			SubscriptionURL: "https://dashboard.stripe.com/subscriptions/sub_1",
			LatestPayment:   &MemberPayment{Date: "2026-09-01", Status: "paid", URL: "https://dashboard.stripe.com/payments/pi_1"},
		}},
	}
	pub := membersFileForAudience(full, AudiencePublic)
	if len(pub.Members) != 0 || pub.Summary.TotalMembers != 2 {
		t.Errorf("public keeps the summary only: %+v", pub)
	}
	mem := membersFileForAudience(full, AudienceMembers)
	m := mem.Members[0]
	if m.FirstName != "Jane" || m.Plan != "monthly" || m.Accounts.Discord == nil {
		t.Errorf("members keeps who and which plan: %+v", m)
	}
	if m.Accounts.EmailHash != "" || m.SubscriptionURL != "" || m.LatestPayment.URL != "" {
		t.Errorf("members must not carry email hash or Stripe urls: %+v", m)
	}
	if full.Members[0].LatestPayment.URL == "" || full.Members[0].Accounts.EmailHash == "" {
		t.Error("projection mutated the full value")
	}
	if membersFileForAudience(full, AudienceStewards).Members[0].SubscriptionURL == "" {
		t.Error("stewards keeps everything")
	}
}

func TestContributorProjectionsDropWallet(t *testing.T) {
	addr := sp("0xabc")
	month := MonthlyContributorsFile{Contributors: []ContributorEntry{{ID: "1", Address: addr, Profile: ContributorProfile{Username: "jane"}}}}
	for _, a := range []Audience{AudiencePublic, AudienceMembers} {
		if got := contributorsFileForAudience(month, a); got.Contributors[0].Address != nil || got.Contributors[0].Profile.Username != "jane" {
			t.Errorf("%s: wallet must go, identity stays: %+v", a, got.Contributors[0])
		}
	}
	if contributorsFileForAudience(month, AudienceStewards).Contributors[0].Address == nil {
		t.Error("stewards keeps the wallet")
	}
	year := YearlyUsersFile{Contributors: []YearlyUsersEntry{{ID: "1", Address: addr}}}
	if yearlyUsersFileForAudience(year, AudienceMembers).Contributors[0].Address != nil {
		t.Error("yearly: wallet must go below stewards")
	}
	top := TopContributorsFile{Contributors: []TopContributor{{ID: "1", WalletAddress: addr}}}
	if topContributorsFileForAudience(top, AudiencePublic).Contributors[0].WalletAddress != nil {
		t.Error("top: wallet must go below stewards")
	}
	if month.Contributors[0].Address == nil {
		t.Error("projection mutated the source")
	}
}

func TestImagesFileForAudience(t *testing.T) {
	f := ImagesFile{Images: []ImageEntry{{ID: "1", Message: "with Jane at jane@example.com", Author: ImageAuthor{Username: "bob"}}}}
	pub := imagesFileForAudience(f, AudiencePublic)
	if pub.Images[0].Message != "" || pub.Images[0].Author.Username != "bob" {
		t.Errorf("public keeps the author, drops the text: %+v", pub.Images[0])
	}
	if imagesFileForAudience(f, AudienceMembers).Images[0].Message == "" {
		t.Error("members keeps the text")
	}
}

func TestCounterpartiesFileForAudience(t *testing.T) {
	f := CounterpartiesFile{Counterparties: map[string]CounterpartyEntry{
		"iban:be46":  {Name: "🏦 KBC", Slug: "kbc"},
		"iban:be68":  {Name: "Jane Doe"},
		"stripe:cus": {Name: "Acme SRL"},
	}}
	pub := counterpartiesFileForAudience(f, AudiencePublic)
	if len(pub.Counterparties) != 1 || pub.Counterparties["iban:be46"].Slug != "kbc" {
		t.Errorf("public lists our own accounts only: %+v", pub.Counterparties)
	}
	if len(counterpartiesFileForAudience(f, AudienceMembers).Counterparties) != 3 {
		t.Error("members sees every counterparty")
	}
}

func TestEventProjections(t *testing.T) {
	n := 12
	rev := 240.0
	ev := FullEvent{
		ID: "e1", Name: "Repair Café", Guests: json.RawMessage(`[{"name":"Jane"}]`), LumaData: json.RawMessage(`{"raw":1}`),
		TicketSales: &EventTicketSales{TxCount: 3},
		Metadata:    EventMetadata{Host: sp("Ana"), Attendance: &n, TicketRevenue: &rev, Note: sp("vip list")},
	}
	pub := eventForAudience(ev, AudiencePublic)
	if pub.Guests != nil || pub.LumaData != nil || pub.TicketSales != nil || pub.Metadata.Attendance != nil || pub.Metadata.TicketRevenue != nil || pub.Metadata.Note != nil {
		t.Errorf("public must drop guests, raw payload, sales and income: %+v", pub)
	}
	if pub.Metadata.Host == nil || pub.Name != "Repair Café" {
		t.Error("public keeps the event as published (name, host)")
	}
	mem := eventForAudience(ev, AudienceMembers)
	if mem.Guests != nil || mem.LumaData != nil {
		t.Error("members must not carry attendee lists or the raw Luma payload")
	}
	if mem.TicketSales == nil || mem.Metadata.Attendance == nil {
		t.Error("members keeps sales and attendance")
	}
	if eventForAudience(ev, AudienceStewards).Guests == nil {
		t.Error("stewards keeps everything")
	}

	csv := "Event ID,Date,Event Name,Host,Attendance,Tickets Sold,Ticket Revenue,Fridge Income,Rental Income,Location,URL,Note\n" +
		"e1,2026-09-01,\"Repair, Café\",Ana,12,3,240,10,0,Room 1,https://x,vip\n"
	pubCSV := eventsCSVForAudience(csv, AudiencePublic)
	if strings.Contains(pubCSV, "Attendance") || strings.Contains(pubCSV, "vip") || !strings.Contains(pubCSV, "\"Repair, Café\"") || !strings.Contains(pubCSV, "Ana") {
		t.Errorf("public csv drops attendance/revenue/income/note, keeps quoted fields and host:\n%s", pubCSV)
	}
	if eventsCSVForAudience(csv, AudienceMembers) != csv {
		t.Error("members csv is the full sheet")
	}
}

func TestInboundSpreadsForAudience(t *testing.T) {
	f := InboundSpreadsFile{Inbound: []InboundSpread{{TxID: "t", Counterparty: "Jane", Amount: "10"}}}
	if inboundSpreadsFileForAudience(f, AudiencePublic).Inbound[0].Counterparty != "" {
		t.Error("public spreads must not name the counterparty")
	}
	if inboundSpreadsFileForAudience(f, AudienceMembers).Inbound[0].Counterparty != "Jane" {
		t.Error("members spreads keep the counterparty")
	}
}

func TestWriteTiersLegacyAndProfilesNeverPublic(t *testing.T) {
	dataDir := t.TempDir()
	t.Setenv("DATA_DIR", dataDir)

	data := []byte(`{"id":"1","introductions":[{"content":"hi"}]}`)
	writeTiers(dataDir, "latest", "", filepath.Join("profiles", "jane.json"), tierPayload{Stewards: data, Members: data, Public: nil, Legacy: data})
	if _, err := os.Stat(filepath.Join(dataDir, "latest", "public", "profiles", "jane.json")); err == nil {
		t.Error("profiles must never reach public/")
	}
	for _, p := range []string{"latest/stewards/profiles/jane.json", "latest/members/profiles/jane.json", "latest/generated/profiles/jane.json"} {
		if _, err := os.Stat(filepath.Join(dataDir, p)); err != nil {
			t.Errorf("%s missing", p)
		}
	}

	t.Setenv("CHB_LEGACY_GENERATED", "0")
	writeTiersSame(dataDir, "2026", "09", "summary.json", []byte(`{"ok":true}`))
	if _, err := os.Stat(filepath.Join(dataDir, "2026", "09", "generated", "summary.json")); err == nil {
		t.Error("CHB_LEGACY_GENERATED=0 must stop writing generated/")
	}
	if _, err := os.Stat(filepath.Join(dataDir, "2026", "09", "public", "summary.json")); err != nil {
		t.Error("public summary missing")
	}
}

func TestMigrateGeneratedToStewards(t *testing.T) {
	dataDir := t.TempDir()
	t.Setenv("DATA_DIR", dataDir)
	write := func(rel, content string) {
		p := filepath.Join(dataDir, rel)
		os.MkdirAll(filepath.Dir(p), 0o755)
		os.WriteFile(p, []byte(content), 0o644)
	}
	write("2026/07/generated/transactions.json", `{"transactions":[]}`)
	write("2026/07/generated/private/enrichment.json", `{"enrichments":{"x":{"iban":"BE68539007547034"}}}`)
	write("2026/07/generated/events/images/e1.jpg", "jpg")
	write("latest/generated/summary.json", `{}`)
	write("2026/08/generated/transactions.json", `{"old":true}`)
	write("2026/08/stewards/transactions.json", `{"new":true}`) // already regenerated: must not be overwritten

	migrateGeneratedToStewards(dataDir)

	for _, p := range []string{"2026/07/stewards/transactions.json", "2026/07/stewards/private/enrichment.json", "2026/07/stewards/events/images/e1.jpg", "latest/stewards/summary.json"} {
		if _, err := os.Stat(filepath.Join(dataDir, p)); err != nil {
			t.Errorf("%s not seeded", p)
		}
	}
	if _, err := os.Stat(filepath.Join(dataDir, "2026", "07", "generated", "private")); !os.IsNotExist(err) {
		t.Error("generated/private/ must be removed — it was the leak")
	}
	if _, err := os.Stat(filepath.Join(dataDir, "2026", "07", "generated", "transactions.json")); err != nil {
		t.Error("the rest of generated/ stays for legacy consumers")
	}
	if b, _ := os.ReadFile(filepath.Join(dataDir, "2026", "08", "stewards", "transactions.json")); string(b) != `{"new":true}` {
		t.Error("an existing stewards file must not be overwritten by the legacy copy")
	}
	// Idempotent.
	migrateGeneratedToStewards(dataDir)

	// The seeded enrichment is still honoured by the PII loader.
	if tf := LoadTransactionsWithPII(dataDir, "2026", "07"); tf == nil {
		t.Error("LoadTransactionsWithPII must read the seeded stewards tree")
	}
}
