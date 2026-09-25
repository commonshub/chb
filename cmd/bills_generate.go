package cmd

// Vendor bills → bills.json (per month) and pending-bills.json (latest/).
//
// Reads the Odoo bill caches written by `chb bills pull`
// (YYYY/MM/providers/odoo/<db>/{bills.json,private/bills.json}) and writes,
// per audience tier:
//
//	YYYY/MM/<tier>/bills.json        every posted bill dated that month (paid or not)
//	latest/<tier>/pending-bills.json every bill still to pay, whatever its month
//
// The pending list exists so the website can publish what the Hub still
// owes and invite people to cover it. See docs/bills.md.
//
// Tiers:
//
//	stewards  everything chb knows (vendor contact details, bank account,
//	          Odoo links, attachments).
//	members   vendor names (business or person), vendor invoice number,
//	          line descriptions and amounts. No contact details, no bank
//	          account, no Odoo links.
//	public    as members, except that a vendor who is a private individual
//	          stays anonymous: no name, no invoice number, no line
//	          descriptions — only the amounts and the category.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	odoosource "github.com/CommonsHub/chb/providers/odoo"
)

const (
	billsFile        = "bills.json"
	pendingBillsFile = "pending-bills.json"
)

// Bill is one vendor bill or vendor credit note, as published.
type Bill struct {
	ID           string     `json:"id"`     // stable public id, "b-" + 10 hex
	Number       string     `json:"number"` // our accounting number, e.g. CHB-S/2026/04/0012
	Type         string     `json:"type"`   // "bill" | "credit_note"
	Status       string     `json:"status"` // "pending" | "partially_paid" | "paid" | "reversed"
	Date         string     `json:"date"`
	DueDate      string     `json:"dueDate,omitempty"`
	Vendor       BillVendor `json:"vendor"`
	VendorRef    string     `json:"vendorRef,omitempty"` // the vendor's own invoice number
	Description  string     `json:"description,omitempty"`
	Lines        []BillLine `json:"lines,omitempty"`
	Category     string     `json:"category,omitempty"`
	Collective   string     `json:"collective,omitempty"`
	Event        string     `json:"event,omitempty"`
	Currency     string     `json:"currency"`
	Untaxed      float64    `json:"untaxedAmount"`
	VAT          float64    `json:"vatAmount"`
	Total        float64    `json:"totalAmount"`
	AmountDue    float64    `json:"amountDue"`
	HasDocument  bool       `json:"hasDocument"`
	PaymentState string     `json:"paymentState"` // Odoo's raw value, for reference

	// Stewards only.
	Stewards *BillStewards `json:"stewards,omitempty"`
}

type BillVendor struct {
	Type string `json:"type"` // "business" | "individual"
	Name string `json:"name,omitempty"`
	VAT  string `json:"vat,omitempty"` // business VAT number (public in the company register)
}

type BillLine struct {
	Description string  `json:"description"`
	Quantity    float64 `json:"quantity,omitempty"`
	Untaxed     float64 `json:"untaxedAmount"`
	Total       float64 `json:"totalAmount"`
	VATRate     string  `json:"vatRate,omitempty"` // "21%"
}

type BillStewards struct {
	OdooID      int                      `json:"odooId"`
	OdooURL     string                   `json:"odooUrl,omitempty"`
	Partner     OdooInvoicePartner       `json:"partner"`
	PartnerBank *OdooInvoiceBankAccount  `json:"partnerBank,omitempty"`
	Payments    []OdooInvoicePayment     `json:"payments,omitempty"`
	Attachments []OdooDocumentAttachment `json:"attachments,omitempty"`
	Reference   string                   `json:"paymentReference,omitempty"`
}

type BillsTotals struct {
	Count     int     `json:"count"`
	Total     float64 `json:"totalAmount"`
	AmountDue float64 `json:"amountDue"`
}

// BillsFile is YYYY/MM/<tier>/bills.json and latest/<tier>/pending-bills.json.
type BillsFile struct {
	GeneratedAt string      `json:"generatedAt"`
	Source      string      `json:"source"`          // "odoo"
	Scope       string      `json:"scope"`           // "month" | "pending"
	Month       string      `json:"month,omitempty"` // "2026-04" for a month file
	Currency    string      `json:"currency"`
	Totals      BillsTotals `json:"totals"` // EUR bills only; credit notes are listed but not summed
	// TotalsByCurrency has the same sums per currency, when bills in other
	// currencies exist (a few SaaS vendors bill in USD).
	TotalsByCurrency map[string]BillsTotals `json:"totalsByCurrency,omitempty"`
	Bills            []Bill                 `json:"bills"`
}

func billPublicID(odooID int) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s:account.move:%d", odoosource.PathNamespace(), odooID)))
	return "b-" + hex.EncodeToString(sum[:])[:10]
}

func billStatus(inv OdooOutgoingInvoice) string {
	switch inv.PaymentState {
	case "paid", "in_payment":
		return "paid"
	case "partial":
		return "partially_paid"
	case "reversed":
		return "reversed"
	}
	return "pending"
}

// billVendorIsBusiness: a company, or anyone registered for VAT (a sole
// trader's business identity is public in the company register).
func billVendorIsBusiness(p OdooInvoicePartner) bool {
	return p.IsCompany || p.CompanyType == "company" || strings.TrimSpace(p.VAT) != ""
}

func billFromInvoice(inv OdooOutgoingInvoice) Bill {
	b := Bill{
		ID:           billPublicID(inv.ID),
		Number:       inv.Number,
		Type:         "bill",
		Status:       billStatus(inv),
		Date:         firstNonEmpty(inv.InvoiceDate, inv.Date),
		DueDate:      inv.DueDate,
		VendorRef:    firstNonEmpty(inv.Ref, inv.Title),
		Category:     inv.Category,
		Collective:   inv.Collective,
		Event:        inv.Event,
		Currency:     firstNonEmpty(inv.Currency, "EUR"),
		Untaxed:      inv.UntaxedAmount,
		VAT:          inv.VATAmount,
		Total:        inv.TotalAmount,
		AmountDue:    inv.ResidualAmount,
		HasDocument:  len(inv.Attachments) > 0,
		PaymentState: inv.PaymentState,
	}
	if inv.MoveType == "in_refund" {
		b.Type = "credit_note"
	}
	if b.Status == "paid" || b.Status == "reversed" {
		b.AmountDue = 0
	}
	name := strings.TrimSpace(firstNonEmpty(inv.Partner.Name, inv.Partner.DisplayName, inv.PartnerDisplayName))
	if at := strings.LastIndex(name, "@"); at >= 0 {
		// A partner named after a mailbox ("billing@example.org"): the
		// domain says who it is, the mailbox is contact data.
		name = strings.TrimSpace(name[at+1:])
	}
	b.Vendor = BillVendor{Type: "individual", Name: name}
	if billVendorIsBusiness(inv.Partner) {
		b.Vendor.Type = "business"
		b.Vendor.VAT = strings.TrimSpace(inv.Partner.VAT)
	}
	var descs []string
	for _, li := range inv.LineItems {
		if li.DisplayType != "" && li.DisplayType != "product" {
			continue
		}
		d := strings.TrimSpace(firstNonEmpty(li.Title, li.ProductName))
		line := BillLine{Description: d, Quantity: li.Quantity, Untaxed: li.SubtotalAmount, Total: li.TotalAmount}
		if len(li.Taxes) == 1 && li.Taxes[0].AmountType == "percent" {
			line.VATRate = fmt.Sprintf("%g%%", li.Taxes[0].Amount)
		}
		b.Lines = append(b.Lines, line)
		if d != "" && !containsString(descs, d) {
			descs = append(descs, d)
		}
		if b.Category == "" && li.Category != "" {
			b.Category = li.Category
		}
	}
	b.Description = strings.Join(descs, ", ")
	b.Stewards = &BillStewards{
		OdooID: inv.ID, OdooURL: inv.InvoiceURL, Partner: inv.Partner, PartnerBank: inv.PartnerBank,
		Payments: inv.Payments, Attachments: inv.Attachments, Reference: inv.Reference,
	}
	return b
}

// billForAudience projects a stewards bill down to a tier.
func billForAudience(b Bill, a Audience) Bill {
	if a == AudienceStewards {
		return b
	}
	b.Stewards = nil
	if a == AudiencePublic && b.Vendor.Type == "individual" {
		b.Vendor = BillVendor{Type: "individual"}
		b.VendorRef = ""
		b.Description = ""
		lines := make([]BillLine, len(b.Lines))
		for i, l := range b.Lines {
			l.Description = ""
			lines[i] = l
		}
		b.Lines = lines
	}
	return b
}

func billsFileForAudience(f BillsFile, a Audience) BillsFile {
	out := f
	out.Bills = make([]Bill, len(f.Bills))
	for i, b := range f.Bills {
		out.Bills[i] = billForAudience(b, a)
	}
	return out
}

// billIsPending: a posted vendor bill with an amount still to pay.
func billIsPending(b Bill) bool {
	return b.Type == "bill" && (b.Status == "pending" || b.Status == "partially_paid") && b.AmountDue > 0
}

// billsTotals sums the EUR bills (see billsTotalsByCurrency for the rest).
func billsTotals(bills []Bill) BillsTotals {
	return billsTotalsByCurrency(bills)["EUR"]
}

// billsTotalsByCurrency sums bills per currency; nil when all are EUR.
func billsTotalsByCurrency(bills []Bill) map[string]BillsTotals {
	out := map[string]BillsTotals{}
	for _, b := range bills {
		if b.Type != "bill" || b.Status == "reversed" {
			continue
		}
		t := out[b.Currency]
		t.Count++
		t.Total = roundCents(t.Total + b.Total)
		t.AmountDue = roundCents(t.AmountDue + b.AmountDue)
		out[b.Currency] = t
	}
	return out
}

func otherCurrencyTotals(bills []Bill) map[string]BillsTotals {
	by := billsTotalsByCurrency(bills)
	for c := range by {
		if c != "EUR" {
			return by
		}
	}
	return nil
}

func sortBills(bills []Bill) {
	sort.Slice(bills, func(i, j int) bool {
		if bills[i].Date != bills[j].Date {
			return bills[i].Date > bills[j].Date
		}
		return bills[i].Number > bills[j].Number
	})
}

// writeBillsTiers writes one BillsFile to its path in every tier. Month
// files are not mirrored to latest/: latest/ carries the pending list.
func writeBillsTiers(dataDir, year, month, rel string, full BillsFile) {
	for _, a := range []Audience{AudienceStewards, AudienceMembers, AudiencePublic} {
		data, err := json.MarshalIndent(billsFileForAudience(full, a), "", "  ")
		if err != nil {
			continue
		}
		cleaned, err := enforceAudiencePolicy(a, rel, data)
		if err != nil {
			Warnf("  %s⚠ %s/%s: %v%s", Fmt.Yellow, a, rel, err, Fmt.Reset)
			continue
		}
		target := audiencePath(dataDir, year, month, a, rel)
		if err := os.MkdirAll(filepath.Dir(target), a.DirMode()); err != nil {
			continue
		}
		if err := os.WriteFile(target, cleaned, a.FileMode()); err != nil {
			continue
		}
		if base, ok := dataBaseForPath(target); ok {
			_ = applyDataPathPolicy(base, target, false)
		}
	}
}

// generateBills writes the month files for every month with a bill cache
// (or only `only`, "YYYY-MM") and always rebuilds the pending list.
// Returns (months written, pending bills).
func generateBills(dataDir, only string) (int, int) {
	now := time.Now().UTC().Format(time.RFC3339)
	all := loadAllCachedBills(dataDir)
	byMonth := map[string][]Bill{}
	var pending []Bill
	for _, inv := range all {
		if inv.State != "posted" {
			continue // drafts are not bills yet; cancelled ones never were
		}
		b := billFromInvoice(inv)
		if len(b.Date) < 7 {
			continue
		}
		byMonth[b.Date[:7]] = append(byMonth[b.Date[:7]], b)
		if billIsPending(b) {
			pending = append(pending, b)
		}
	}
	months := 0
	for ym, bills := range byMonth {
		if only != "" && ym != only {
			continue
		}
		sortBills(bills)
		writeBillsTiers(dataDir, ym[:4], ym[5:], billsFile, BillsFile{
			GeneratedAt: now, Source: "odoo", Scope: "month", Month: ym, Currency: "EUR",
			Totals: billsTotals(bills), TotalsByCurrency: otherCurrencyTotals(bills), Bills: bills,
		})
		months++
	}
	if len(all) > 0 {
		sortBills(pending)
		writeBillsTiers(dataDir, "latest", "", pendingBillsFile, BillsFile{
			GeneratedAt: now, Source: "odoo", Scope: "pending", Currency: "EUR",
			Totals: billsTotals(pending), TotalsByCurrency: otherCurrencyTotals(pending), Bills: nonNilBills(pending),
		})
	}
	return months, len(pending)
}

func nonNilBills(b []Bill) []Bill {
	if b == nil {
		return []Bill{}
	}
	return b
}

// BillsPending is `chb bills pending [--json] [--public|--members]`: the
// list the website publishes, from the local bill cache.
func BillsPending(args []string) error {
	if HasFlag(args, "--help", "-h") {
		fmt.Print(`
chb bills pending — vendor bills still to pay (from the local Odoo cache)

USAGE
  chb bills pending [--public | --members] [--json]

Shows what latest/<tier>/pending-bills.json contains (default: stewards
view). Refresh the cache with 'chb bills pull'; 'chb generate' writes the
files. A bill Odoo still calls unpaid may have been paid already but not
reconciled with its bank line.
`)
		return nil
	}
	a := AudienceStewards
	if HasFlag(args, "--public") {
		a = AudiencePublic
	} else if HasFlag(args, "--members") {
		a = AudienceMembers
	}
	var pending []Bill
	for _, inv := range loadAllCachedBills(DataDir()) {
		if inv.State != "posted" {
			continue
		}
		if b := billFromInvoice(inv); billIsPending(b) {
			pending = append(pending, billForAudience(b, a))
		}
	}
	sortBills(pending)
	if HasFlag(args, "--json") {
		out, _ := json.MarshalIndent(BillsFile{Source: "odoo", Scope: "pending", Currency: "EUR", Totals: billsTotals(pending), Bills: nonNilBills(pending)}, "", "  ")
		fmt.Println(string(out))
		return nil
	}
	if len(pending) == 0 {
		fmt.Printf("\n  No pending bills in the local cache.\n  %sRefresh with: chb bills pull%s\n\n", Fmt.Dim, Fmt.Reset)
		return nil
	}
	fmt.Println()
	fmt.Printf("  %-10s %-10s %-30s %-34s %12s\n", "DATE", "DUE", "VENDOR", "DESCRIPTION", "DUE AMOUNT")
	for _, b := range pending {
		vendor := b.Vendor.Name
		if vendor == "" {
			vendor = "(private individual)"
		}
		amount := fmtEUR(b.AmountDue)
		if b.Currency != "EUR" {
			amount = fmt.Sprintf("%.2f %s", b.AmountDue, b.Currency)
		}
		fmt.Printf("  %-10s %-10s %-30s %-34s %12s\n", b.Date, b.DueDate, truncateRunes(vendor, 30), truncateRunes(b.Description, 34), amount)
	}
	t := billsTotals(pending)
	fmt.Printf("\n  %s, %s still to pay (%s view)\n", Pluralize(t.Count, "EUR bill", ""), fmtEUR(t.AmountDue), a)
	for c, ot := range otherCurrencyTotals(pending) {
		if c != "EUR" {
			fmt.Printf("  + %s, %.2f %s\n", Pluralize(ot.Count, c+" bill", ""), ot.AmountDue, c)
		}
	}
	fmt.Printf("  %sOdoo counts a bill as unpaid until it is reconciled with its payment.%s\n\n", Fmt.Dim, Fmt.Reset)
	return nil
}
