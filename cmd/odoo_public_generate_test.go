package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	odoosource "github.com/CommonsHub/chb/providers/odoo"
)

func seedInvoices(t *testing.T, dataDir, year, month string, invs []OdooOutgoingInvoice) {
	t.Helper()
	pub := OdooOutgoingInvoicesFile{Year: year, Month: month, Source: "odoo", Count: len(invs), Invoices: buildPublicInvoices(invs)}
	priv := OdooOutgoingInvoicesPrivateFile{Year: year, Month: month, Source: "odoo", Count: len(invs), Invoices: buildPrivateInvoices(invs)}
	for path, v := range map[string]interface{}{
		odoosource.Path(dataDir, year, month, odoosource.InvoicesFile):        pub,
		odoosource.PrivatePath(dataDir, year, month, odoosource.InvoicesFile): priv,
	} {
		data, _ := json.Marshal(v)
		os.MkdirAll(filepath.Dir(path), 0o700)
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

func readTier(t *testing.T, dataDir, year, month string, a Audience, rel string, v interface{}) string {
	t.Helper()
	raw, err := os.ReadFile(audiencePath(dataDir, year, month, a, rel))
	if err != nil {
		t.Fatalf("%s/%s/%s %s missing", year, month, a, rel)
	}
	if v != nil {
		if err := json.Unmarshal(raw, v); err != nil {
			t.Fatal(err)
		}
	}
	return string(raw)
}

func TestAccountingFilesPerTier(t *testing.T) {
	tmp := t.TempDir()
	dataDir := filepath.Join(tmp, "data")
	t.Setenv("DATA_DIR", dataDir)
	t.Setenv("APP_DATA_DIR", filepath.Join(tmp, "app"))
	os.MkdirAll(filepath.Join(tmp, "app", "settings"), 0o755)
	os.WriteFile(filepath.Join(tmp, "app", "settings", "rooms.json"),
		[]byte(`{"rooms":[{"slug":"ostrom","name":"Ostrom Room","pricePerHour":100},{"slug":"satoshi","name":"Satoshi Room"}]}`), 0o644)

	bills := []OdooOutgoingInvoice{
		{ // Odoo says person, the name says company: published by name.
			ID: 201, Number: "CHB-S/2026/08/0001", MoveType: "in_invoice", State: "posted", PaymentState: "paid",
			InvoiceDate: "2026-08-03", TotalAmount: 106, UntaxedAmount: 100, VATAmount: 6, Currency: "EUR",
			Partner: OdooInvoicePartner{ID: 31, Name: "DelivCo SRL (Big Bag Delivery)", Email: "orders@delivco.example"},
			LineItems: []OdooInvoiceLineItem{
				{ID: 1, Title: "Club-Mate 24x50cl", ProductName: "Club-Mate", DisplayType: "product", Quantity: 2, UnitPrice: 25, SubtotalAmount: 50, TotalAmount: 53, AccountCode: "604200", AccountName: "ACHATS NOURRITURE"},
				{ID: 2, Title: "Delivery", DisplayType: "product", Quantity: 1, SubtotalAmount: 50, TotalAmount: 53, AccountCode: "604200", AccountName: "ACHATS NOURRITURE"},
			},
		},
		{ // A person paid back for train tickets: anonymous in public.
			ID: 202, Number: "CHB-S/2026/08/0002", MoveType: "in_invoice", State: "posted", PaymentState: "not_paid",
			InvoiceDate: "2026-08-05", TotalAmount: 40, UntaxedAmount: 40, ResidualAmount: 40, Currency: "EUR",
			Partner:   OdooInvoicePartner{ID: 32, Name: "Andrea Doe / numéro national 84.05.18-647.30", Email: "andrea@example.com"},
			LineItems: []OdooInvoiceLineItem{{ID: 3, Title: "Train Brussels-Ghent for Andrea", ProductName: "Travel", DisplayType: "product", SubtotalAmount: 40, TotalAmount: 40, AccountCode: "613000", AccountName: "RETRIBUTIONS ANDREA"}},
		},
		{ // Payroll from the social secretariat: amounts stay, text goes.
			ID: 203, Number: "CHB-S/2026/08/0003", MoveType: "in_invoice", State: "posted", PaymentState: "paid",
			InvoiceDate: "2026-08-28", TotalAmount: 3000, UntaxedAmount: 3000, Currency: "EUR",
			Partner:   OdooInvoicePartner{ID: 33, Name: "Partena", IsCompany: true, VAT: "BE0409536968"},
			LineItems: []OdooInvoiceLineItem{{ID: 4, Title: "Salaire août Jane Smith", DisplayType: "product", SubtotalAmount: 3000, TotalAmount: 3000, AccountCode: "620200", AccountName: "REMUNERATIONS"}},
		},
		{ // Another individual, same category: merged with 202 in public.
			ID: 204, Number: "CHB-S/2026/08/0004", MoveType: "in_invoice", State: "posted", PaymentState: "paid",
			InvoiceDate: "2026-08-09", TotalAmount: 10, UntaxedAmount: 10, Currency: "EUR",
			Partner:   OdooInvoicePartner{ID: 34, Name: "Bob Roe"},
			LineItems: []OdooInvoiceLineItem{{ID: 5, Title: "Bus", ProductName: "Travel", DisplayType: "product", SubtotalAmount: 10, TotalAmount: 10, AccountCode: "613000", AccountName: "RETRIBUTIONS ANDREA"}},
		},
	}
	seedBills(t, dataDir, "2026", "08", bills)

	invoices := []OdooOutgoingInvoice{
		{
			ID: 301, Number: "CHB/2026/00200", MoveType: "out_invoice", State: "posted", PaymentState: "paid",
			InvoiceDate: "2026-08-10", TotalAmount: 242, UntaxedAmount: 200, Currency: "EUR",
			Partner:   OdooInvoicePartner{ID: 41, Name: "Open Org ASBL", IsCompany: true, VAT: "BE0123456749", Email: "board@openorg.example"},
			LineItems: []OdooInvoiceLineItem{{ID: 6, Title: "Ostrom Room half day for the board retreat", ProductName: "Ostrom Room", DisplayType: "product", Quantity: 4, SubtotalAmount: 200, TotalAmount: 242, AccountCode: "700100"}},
		},
		{
			ID: 302, Number: "CHB/2026/00201", MoveType: "out_invoice", State: "posted", PaymentState: "not_paid",
			InvoiceDate: "2026-08-12", TotalAmount: 60.5, UntaxedAmount: 50, ResidualAmount: 60.5, Currency: "EUR",
			Partner:   OdooInvoicePartner{ID: 42, Name: "Carol Poe", Email: "carol@example.com", Phone: "+32470111111"},
			LineItems: []OdooInvoiceLineItem{{ID: 7, Title: "Satoshi room for Carol's birthday", ProductName: "Satoshi room", DisplayType: "product", Quantity: 1, SubtotalAmount: 50, TotalAmount: 60.5, AccountCode: "700100"}},
		},
		{
			ID: 303, Number: "MEM/2026/00030", MoveType: "out_invoice", State: "posted", PaymentState: "paid",
			InvoiceDate: "2026-08-01", TotalAmount: 100, UntaxedAmount: 100, Currency: "EUR",
			Partner:   OdooInvoicePartner{ID: 42, Name: "Carol Poe"},
			LineItems: []OdooInvoiceLineItem{{ID: 8, Title: "Yearly membership", ProductName: "Membership", DisplayType: "product", SubtotalAmount: 100, TotalAmount: 100, ProductID: 111, AccountCode: "704200"}},
		},
	}
	seedInvoices(t, dataDir, "2026", "08", invoices)

	n, err := generateAccountingFiles(dataDir)
	if err != nil || n != 1 {
		t.Fatalf("months=%d err=%v", n, err)
	}

	// expenses.json
	var pubExp, memExp ExpensesFile
	pubRaw := readTier(t, dataDir, "2026", "08", AudiencePublic, "expenses.json", &pubExp)
	memRaw := readTier(t, dataDir, "2026", "08", AudienceMembers, "expenses.json", &memExp)
	stwRaw := readTier(t, dataDir, "2026", "08", AudienceStewards, "expenses.json", nil)
	for _, want := range []string{"DelivCo SRL (Big Bag Delivery)", "Club-Mate 24x50cl", "Partena", `"product": "Travel"`, "Services and other goods"} {
		if !strings.Contains(pubRaw, want) {
			t.Errorf("public expenses should show %q", want)
		}
	}
	for _, leak := range []string{"Andrea", "Bob Roe", "Train Brussels", "Jane Smith", "RETRIBUTIONS", "ACHATS", "84.05.18", "@", "odooId"} {
		if strings.Contains(pubRaw, leak) {
			t.Errorf("public expenses leak %q", leak)
		}
	}
	if !strings.Contains(memRaw, "Andrea Doe") || !strings.Contains(memRaw, "Train Brussels") || strings.Contains(memRaw, "84.05.18") || strings.Contains(memRaw, "Jane Smith") || strings.Contains(memRaw, "@") {
		t.Error("members: names and texts yes, national numbers, payroll text and emails no")
	}
	if !strings.Contains(stwRaw, "Jane Smith") || !strings.Contains(stwRaw, "andrea@example.com") {
		t.Error("stewards keep everything")
	}
	if pubExp.Totals.Total != 3156 || pubExp.Totals.Due != 40 || pubExp.Totals.Count != 4 {
		t.Errorf("expense totals = %+v", pubExp.Totals)
	}

	// vendors.json: individuals merged per category in public.
	var pubV VendorsFile
	readTier(t, dataDir, "2026", "08", AudiencePublic, "vendors.json", &pubV)
	var merged *VendorRow
	for i, r := range pubV.Vendors {
		if r.Vendor.Type == "individual" {
			if merged != nil {
				t.Error("two anonymous rows for one category")
			}
			merged = &pubV.Vendors[i]
		}
	}
	if merged == nil || merged.Individuals != 2 || merged.Total != 50 || merged.Vendor.Name != "" || merged.Vendor.ID != "" {
		t.Errorf("merged individuals row = %+v", merged)
	}
	if pubV.Totals.Count != 4 || pubV.Vendors[0].Vendor.Name != "Partena" {
		t.Errorf("vendors = %+v", pubV)
	}
	var memV VendorsFile
	readTier(t, dataDir, "2026", "08", AudienceMembers, "vendors.json", &memV)
	if len(memV.Vendors) != 4 {
		t.Errorf("members list every vendor: %d rows", len(memV.Vendors))
	}

	// customers.json
	var pubC, stwC CustomersFile
	pubCRaw := readTier(t, dataDir, "2026", "08", AudiencePublic, "customers.json", &pubC)
	readTier(t, dataDir, "2026", "08", AudienceStewards, "customers.json", &stwC)
	if !strings.Contains(pubCRaw, "Open Org ASBL") || strings.Contains(pubCRaw, "Carol") || strings.Contains(pubCRaw, "birthday") {
		t.Errorf("public customers: organisations named, individuals typed only: %s", pubCRaw)
	}
	// Individuals are merged per income type: Carol's membership and her
	// rental are two anonymous rows, flagged as coming from a member.
	var indTotal, indReceived float64
	rows := 0
	for _, r := range pubC.Customers {
		if r.Customer.Type == "individual" {
			rows++
			indTotal += r.Total
			indReceived += r.Received
			if !r.Customer.Member || r.Customer.ID != "" || len(r.Products) != 0 {
				t.Errorf("anonymous row = %+v", r)
			}
		}
	}
	if rows != 2 || indTotal != 160.5 || indReceived != 100 {
		t.Errorf("individual rows=%d total=%v received=%v", rows, indTotal, indReceived)
	}
	if pubC.Totals.Total != 402.5 || pubC.Totals.Paid != 342 {
		t.Errorf("customer totals = %+v", pubC.Totals)
	}
	foundContact := false
	for _, r := range stwC.Customers {
		if r.Customer.Contact != nil && r.Customer.Contact.Phone == "+32470111111" && len(r.InvoiceRefs) == 2 {
			foundContact = true
		}
	}
	if !foundContact {
		t.Error("stewards see full customer details and their invoices")
	}

	// bookings.json: rentals from 700100 with the room recognised.
	var pubB BookingsFile
	pubBRaw := readTier(t, dataDir, "2026", "08", AudiencePublic, "bookings.json", &pubB)
	if len(pubB.Rentals) != 2 || strings.Contains(pubBRaw, "Carol") || strings.Contains(pubBRaw, "board retreat") {
		t.Errorf("public rentals = %s", pubBRaw)
	}
	rev := map[string]float64{}
	for _, r := range pubB.Rooms {
		rev[r.Room] = r.RentalRevenue
	}
	if rev["ostrom"] != 200 || rev["satoshi"] != 50 {
		t.Errorf("room revenue = %v", rev)
	}

	// Year aggregates.
	var yearV VendorsFile
	readTier(t, dataDir, "2026", "", AudiencePublic, "vendors.json", &yearV)
	if yearV.Scope != "year" || yearV.Period != "2026" || yearV.Totals.Total != 3156 {
		t.Errorf("year vendors = %+v", yearV)
	}
	var yearB BookingsFile
	readTier(t, dataDir, "2026", "", AudiencePublic, "bookings.json", &yearB)
	if len(yearB.Months) != 1 || yearB.Months[0].Month != "2026-08" {
		t.Errorf("year bookings months = %+v", yearB.Months)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "latest", "public", "vendors.json")); err == nil {
		t.Error("accounting files are never mirrored to latest/")
	}
}

func TestMatchPublicEvent(t *testing.T) {
	evs := []FullEvent{{Name: "Commons Assembly #12", StartAt: "2026-08-27T19:00:00+02:00"}}
	start, _ := time.Parse(time.RFC3339, "2026-08-27T18:30:00+02:00")
	if _, ok := matchPublicEvent(evs, start, "Commons assembly #12 (setup incl.)"); !ok {
		t.Error("same day, title contained: a match")
	}
	if _, ok := matchPublicEvent(evs, start, "Private party"); ok {
		t.Error("different title and time: no match")
	}
	if _, ok := matchPublicEvent(evs, start, "Comm"); ok {
		t.Error("very short titles never match on text")
	}
}

func TestSoleTraderCustomerIsAnonymousInPublic(t *testing.T) {
	p := Party{ID: "p-1", Type: "sole_trader", Name: "Thierry Example", VAT: "BE0123456749"}
	if got := customerForAudience(p, AudiencePublic); got.Name != "" || got.VAT != "" || got.ID != "" {
		t.Errorf("public customer = %+v", got)
	}
	if got := customerForAudience(p, AudienceMembers); got.Name == "" {
		t.Error("members see the name")
	}
	if got := partyForAudience(p, AudiencePublic); got.Name == "" {
		t.Error("a sole-trader vendor stays named in public")
	}
}

func TestPartyTypeLegalForms(t *testing.T) {
	cases := map[string]string{
		"DelivCo SRL (Big Bag Delivery)": "organisation",
		"Nicolas Example SRL":            "organisation",
		"STICHTING LODEWIJK DE RAET":     "organisation",
		"BG DIGITAL MARKETING SDN. BHD.": "organisation",
		"Open Org ASBL":                  "organisation",
		"Promo Direct s.r.o.":            "organisation",
		"Johan Van As":                   "individual",
		"Sarah Sanders":                  "individual",
	}
	for name, want := range cases {
		if got := partyType(OdooInvoicePartner{Name: name}); got != want {
			t.Errorf("%q → %s, want %s", name, got, want)
		}
	}
}

func TestBookingsFromRoomCalendars(t *testing.T) {
	tmp := t.TempDir()
	dataDir := filepath.Join(tmp, "data")
	t.Setenv("DATA_DIR", dataDir)
	t.Setenv("APP_DATA_DIR", filepath.Join(tmp, "app"))
	writeFile(t, filepath.Join(tmp, "app", "settings", "rooms.json"), `{"rooms":[{"slug":"ostrom","name":"Ostrom Room"}]}`)
	ics := "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:a1\r\nDTSTART:20260826T160000Z\r\nDTEND:20260826T200000Z\r\nSUMMARY:Private party of Dana Loe\r\nEND:VEVENT\r\n" +
		"BEGIN:VEVENT\r\nUID:a2\r\nDTSTART:20260827T170000Z\r\nDTEND:20260827T190000Z\r\nSUMMARY:Commons Assembly\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	writeFile(t, filepath.Join(dataDir, "2026", "08", "providers", "ics", "ostrom.ics"), ics)
	writeFile(t, filepath.Join(dataDir, "2026", "08", "public", "events.json"),
		`{"month":"2026-08","events":[{"id":"e1","name":"Commons Assembly","startAt":"2026-08-27T19:00:00+02:00","url":"https://lu.ma/x","source":"luma"}]}`)
	if _, err := generateAccountingFiles(dataDir); err != nil {
		t.Fatal(err)
	}
	var pub, mem BookingsFile
	raw := readTier(t, dataDir, "2026", "08", AudiencePublic, "bookings.json", &pub)
	readTier(t, dataDir, "2026", "08", AudienceMembers, "bookings.json", &mem)
	if len(pub.Bookings) != 2 || strings.Contains(raw, "Dana") {
		t.Fatalf("public bookings = %s", raw)
	}
	if pub.Bookings[0].Hours != 4 || pub.Bookings[0].Public || pub.Bookings[0].Title != "" || pub.Bookings[0].Room != "ostrom" {
		t.Errorf("private booking = %+v", pub.Bookings[0])
	}
	if !pub.Bookings[1].Public || pub.Bookings[1].Title != "Commons Assembly" || pub.Bookings[1].EventURL == "" {
		t.Errorf("public event booking = %+v", pub.Bookings[1])
	}
	if mem.Bookings[0].Title != "Private party of Dana Loe" {
		t.Error("members see booking titles")
	}
	if pub.Rooms[0].Bookings != 2 || pub.Rooms[0].Hours != 6 || pub.Rooms[0].PublicBookings != 1 {
		t.Errorf("room summary = %+v", pub.Rooms)
	}
}

func TestNaturalPersonExpenseDoesNotLinkToEvent(t *testing.T) {
	for _, vendor := range []Party{
		{ID: "p-1", Type: "sole_trader", Name: "Sam Lens", VAT: "BE0712345678"},
		{ID: "p-2", Type: "individual", Name: "Andrea Doe"},
	} {
		e := Expense{ID: "b-1", Kind: "bill", Date: "2026-09-21", Vendor: vendor, VendorRef: "2026-014", Event: "luma:evt-ocd2026",
			Description: "Photos of the Open Commons Day",
			Lines:       []ExpenseLine{{Description: "Photos of the Open Commons Day", Product: "Photography", Total: 363}}}
		pub := expenseForAudience(e, AudiencePublic)
		raw, _ := json.Marshal(pub)
		for _, leak := range []string{"Open Commons Day", "evt-ocd2026", "2026-014"} {
			if strings.Contains(string(raw), leak) {
				t.Errorf("%s: public expense leaks %q: %s", vendor.Type, leak, raw)
			}
		}
		if pub.Description != "Photography" || pub.Lines[0].Product != "Photography" {
			t.Errorf("%s: public keeps the product: %s", vendor.Type, raw)
		}
		if (vendor.Type == "sole_trader") != (pub.Vendor.Name == "Sam Lens") {
			t.Errorf("%s: vendor = %+v", vendor.Type, pub.Vendor)
		}
		if mem := expenseForAudience(e, AudienceMembers); mem.Event == "" || mem.Lines[0].Description == "" {
			t.Errorf("%s: members keep the event and the text", vendor.Type)
		}
	}
	org := Expense{Vendor: Party{Type: "organisation", Name: "DelivCo SRL"}, Event: "luma:evt-ocd2026",
		Lines: []ExpenseLine{{Description: "Club-Mate for the Open Commons Day"}}}
	if pub := expenseForAudience(org, AudiencePublic); pub.Event == "" || pub.Lines[0].Description == "" {
		t.Error("organisations keep the event and the text")
	}
}

// Bills addressed to a contact inside a company belong to the company:
// "XL Collective SRL, Leen Schelfhout" is XL Collective SRL, whose VAT the
// contact merely inherits.
func TestDocumentPartyIsTheContactsCompany(t *testing.T) {
	contact := OdooInvoicePartner{ID: 1261, Name: "Leen Schelfhout", DisplayName: "XL Collective SRL, Leen Schelfhout", VAT: "BE0720836593", CompanyType: "person", Email: "leen@example.com"}

	// Cache pulled before v3.16.1: only the display name tells.
	p := documentParty(OdooOutgoingInvoice{Partner: contact})
	if p.Type != "organisation" || p.Name != "XL Collective SRL" || p.VAT != "BE0720836593" || p.Contact.Person != "Leen Schelfhout" {
		t.Errorf("fallback party = %+v %+v", p, p.Contact)
	}
	if got := partyForAudience(p, AudiencePublic); strings.Contains(got.Name, "Leen") || got.Contact != nil {
		t.Errorf("public = %+v", got)
	}

	// Stored commercial partner: its id, so bills to the company itself and
	// to its contacts land on one vendor row.
	company := OdooInvoicePartner{ID: 900, Name: "XL Collective SRL", VAT: "BE0720836593", IsCompany: true}
	p2 := documentParty(OdooOutgoingInvoice{Partner: contact, CommercialPartner: &company})
	p3 := documentParty(OdooOutgoingInvoice{Partner: company})
	if p2.ID != p3.ID || p2.Name != "XL Collective SRL" || p2.Contact.Person != "Leen Schelfhout" {
		t.Errorf("commercial party = %+v vs %+v", p2, p3)
	}

	// A person on their own stays a person.
	solo := documentParty(OdooOutgoingInvoice{Partner: OdooInvoicePartner{ID: 5, Name: "Sam Roe", DisplayName: "Sam Roe"}})
	if solo.Type != "individual" || solo.Name != "Sam Roe" {
		t.Errorf("solo = %+v", solo)
	}
	b := billFromInvoice(OdooOutgoingInvoice{ID: 9, State: "posted", Partner: contact})
	if b.Vendor.Name != "XL Collective SRL" || b.Vendor.Type != "business" {
		t.Errorf("pending-bills vendor = %+v", b.Vendor)
	}
}
