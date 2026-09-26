package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	odoosource "github.com/CommonsHub/chb/providers/odoo"
)

func seedBills(t *testing.T, dataDir, year, month string, bills []OdooOutgoingInvoice) {
	t.Helper()
	pub := OdooVendorBillsFile{Year: year, Month: month, Source: "odoo", Count: len(bills), Bills: buildPublicInvoices(bills)}
	priv := OdooVendorBillsPrivateFile{Year: year, Month: month, Source: "odoo", Count: len(bills), Bills: buildPrivateInvoices(bills)}
	for path, v := range map[string]interface{}{
		odoosource.Path(dataDir, year, month, odoosource.BillsFile):        pub,
		odoosource.PrivatePath(dataDir, year, month, odoosource.BillsFile): priv,
	} {
		data, _ := json.Marshal(v)
		os.MkdirAll(filepath.Dir(path), 0o700)
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

func testBills() []OdooOutgoingInvoice {
	return []OdooOutgoingInvoice{
		{ // open bill from a company
			ID: 101, Number: "CHB-S/2026/04/0001", Ref: "F-778", MoveType: "in_invoice", State: "posted", PaymentState: "not_paid",
			InvoiceDate: "2026-04-10", DueDate: "2026-05-10", TotalAmount: 121, UntaxedAmount: 100, VATAmount: 21, ResidualAmount: 121,
			Currency: "EUR", Partner: OdooInvoicePartner{ID: 7, Name: "Electrabel SA", Email: "billing@electrabel.example", VAT: "BE0403170701", IsCompany: true},
			PartnerBank: &OdooInvoiceBankAccount{}, LineItems: []OdooInvoiceLineItem{{ID: 1, Title: "Electricity April", DisplayType: "product", SubtotalAmount: 100, TotalAmount: 121}},
			Attachments: []OdooDocumentAttachment{{ID: 9, Name: "invoice.pdf"}}, InvoiceURL: "https://odoo.example/web#id=101",
		},
		{ // open reimbursement to a private person
			ID: 102, Number: "CHB-S/2026/04/0002", Ref: "note-jane", MoveType: "in_invoice", State: "posted", PaymentState: "partial",
			InvoiceDate: "2026-04-12", TotalAmount: 80, ResidualAmount: 30, Currency: "EUR",
			Partner:   OdooInvoicePartner{ID: 8, Name: "Jane Doe", Email: "jane@example.com", Phone: "+32470000000"},
			LineItems: []OdooInvoiceLineItem{{ID: 2, Title: "Train tickets for Jane", DisplayType: "product", TotalAmount: 80}},
		},
		{ // paid bill
			ID: 103, Number: "CHB-S/2026/04/0003", MoveType: "in_invoice", State: "posted", PaymentState: "paid",
			InvoiceDate: "2026-04-15", TotalAmount: 50, Currency: "EUR", Partner: OdooInvoicePartner{ID: 9, Name: "Proximus", IsCompany: true},
		},
		{ // open vendor credit note: listed, not pending, not summed
			ID: 104, Number: "RCHB-S/2026/04/0001", MoveType: "in_refund", State: "posted", PaymentState: "not_paid",
			InvoiceDate: "2026-04-20", TotalAmount: 10, ResidualAmount: 10, Currency: "EUR", Partner: OdooInvoicePartner{ID: 9, Name: "Proximus", IsCompany: true},
		},
		{ // draft: not a bill yet
			ID: 105, Number: "/", MoveType: "in_invoice", State: "draft", PaymentState: "not_paid",
			InvoiceDate: "2026-04-21", TotalAmount: 999, ResidualAmount: 999, Currency: "EUR", Partner: OdooInvoicePartner{ID: 9, Name: "Proximus", IsCompany: true},
		},
	}
}

func readBillsFile(t *testing.T, path string) (BillsFile, string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%s missing", path)
	}
	var f BillsFile
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	return f, string(raw)
}

func TestGenerateBillsTiers(t *testing.T) {
	dataDir := t.TempDir()
	t.Setenv("DATA_DIR", dataDir)
	seedBills(t, dataDir, "2026", "04", testBills())

	months, pending := generateBills(dataDir, "")
	if months != 1 || pending != 2 {
		t.Fatalf("months=%d pending=%d, want 1 and 2", months, pending)
	}

	// Month file: posted bills only (no draft), paid and unpaid.
	month, _ := readBillsFile(t, filepath.Join(dataDir, "2026", "04", "public", billsFile))
	if len(month.Bills) != 4 || month.Scope != "month" || month.Month != "2026-04" {
		t.Fatalf("month file = %+v", month)
	}
	if month.Totals.Count != 3 || month.Totals.Total != 251 || month.Totals.AmountDue != 151 {
		t.Errorf("month totals = %+v (credit notes are not summed)", month.Totals)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "latest", "public", billsFile)); err == nil {
		t.Error("month bills.json must not be mirrored to latest/")
	}

	pub, pubRaw := readBillsFile(t, filepath.Join(dataDir, "latest", "public", pendingBillsFile))
	mem, memRaw := readBillsFile(t, filepath.Join(dataDir, "latest", "members", pendingBillsFile))
	stw, stwRaw := readBillsFile(t, filepath.Join(dataDir, "latest", "stewards", pendingBillsFile))
	if len(pub.Bills) != 2 || pub.Totals.AmountDue != 151 || pub.Scope != "pending" {
		t.Fatalf("public pending = %+v", pub)
	}
	for _, leak := range []string{"jane@example.com", "Jane Doe", "Train tickets for Jane", "note-jane", "+32470000000", "billing@electrabel", "odoo.example", "odooId"} {
		if strings.Contains(pubRaw, leak) {
			t.Errorf("public pending leaks %q", leak)
		}
	}
	for _, leak := range []string{"jane@example.com", "billing@electrabel", "+32470000000", "odoo.example", "odooId"} {
		if strings.Contains(memRaw, leak) {
			t.Errorf("members pending leaks %q", leak)
		}
	}
	if !strings.Contains(memRaw, "Jane Doe") || !strings.Contains(memRaw, "Train tickets for Jane") {
		t.Error("members see who is owed and for what")
	}
	if !strings.Contains(pubRaw, "Electrabel SA") || !strings.Contains(pubRaw, "Electricity April") {
		t.Error("public sees business vendors and what the bill is for")
	}
	if !strings.Contains(stwRaw, "jane@example.com") || stw.Bills[0].Stewards == nil {
		t.Error("stewards keep the full record")
	}
	var person Bill
	for _, b := range pub.Bills {
		if b.Vendor.Type == "individual" {
			person = b
		}
	}
	if person.AmountDue != 30 || person.Status != "partially_paid" || person.Vendor.Name != "" {
		t.Errorf("public individual bill = %+v", person)
	}
	if pub.Bills[0].ID != mem.Bills[0].ID || !strings.HasPrefix(pub.Bills[0].ID, "b-") {
		t.Error("a bill keeps one stable public id across tiers")
	}
	for _, tier := range []string{"public", "members", "stewards"} {
		st, _ := os.Stat(filepath.Join(dataDir, "latest", tier, pendingBillsFile))
		want := map[string]os.FileMode{"public": 0o644, "members": 0o640, "stewards": 0o600}[tier]
		if st.Mode().Perm() != want {
			t.Errorf("%s pending mode = %o, want %o", tier, st.Mode().Perm(), want)
		}
	}
}

func TestBillsTotalsPerCurrency(t *testing.T) {
	bills := []Bill{
		{Type: "bill", Status: "pending", Currency: "EUR", Total: 10, AmountDue: 10},
		{Type: "bill", Status: "pending", Currency: "USD", Total: 15, AmountDue: 15},
	}
	if tot := billsTotals(bills); tot.Count != 1 || tot.AmountDue != 10 {
		t.Errorf("EUR totals = %+v, must not mix in USD", tot)
	}
	if by := otherCurrencyTotals(bills); by["USD"].AmountDue != 15 {
		t.Errorf("by currency = %+v", by)
	}
	if otherCurrencyTotals(bills[:1]) != nil {
		t.Error("no per-currency block when everything is EUR")
	}
}

func TestBillVendorNamedAfterMailbox(t *testing.T) {
	b := billFromInvoice(OdooOutgoingInvoice{ID: 1, State: "posted", Partner: OdooInvoicePartner{Name: "billing@citizenspring.earth"}})
	if b.Vendor.Name != "citizenspring.earth" {
		t.Errorf("vendor name = %q, want the mailbox domain", b.Vendor.Name)
	}
}

func TestLoadAllCachedBillsAcrossMonths(t *testing.T) {
	dataDir := t.TempDir()
	t.Setenv("DATA_DIR", dataDir)
	b := testBills()
	seedBills(t, dataDir, "2025", "04", b[:1])
	seedBills(t, dataDir, "2026", "04", b[1:3])
	all := loadAllCachedBills(dataDir)
	if len(all) != 3 || all[101].PaymentState != "not_paid" || all[102].Partner.Name != "Jane Doe" {
		t.Errorf("loaded %d bills: %+v", len(all), all)
	}
	if !billIsOpen("posted", "in_payment") || billIsOpen("draft", "not_paid") || billIsOpen("posted", "paid") {
		t.Error("billIsOpen")
	}
}
