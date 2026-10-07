package cmd

// Accounting transparency files, per audience tier, per month and per year:
//
//	expenses.json   every vendor bill, vendor credit note and expense claim,
//	                line by line (what we actually ordered)
//	vendors.json    who we paid: per vendor, category and totals
//	customers.json  who paid us: per customer, income types and totals
//	bookings.json   when the rooms were booked (room calendars) and what
//	                room rentals brought in (invoice lines on 700100)
//
// Written under YYYY/MM/<tier>/ and YYYY/<tier>/ (the year aggregate).
// Never mirrored to latest/.
//
// GDPR model (see docs/accounting-data.md):
//
//	stewards  everything, contact details included.
//	members   names of everyone (organisations and individuals), no contact
//	          details, no bank accounts, no Odoo links.
//	public    organisations and VAT-registered sole traders are named;
//	          private individuals appear only by type ("individual",
//	          "member") and are merged into one row per category, so no
//	          person can be followed from row to row. Free-text descriptions
//	          written about or by individuals are dropped (product names
//	          stay), payroll lines lose their text, account names are
//	          replaced by their class.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	nostrsource "github.com/CommonsHub/chb/providers/nostr"
	odoosource "github.com/CommonsHub/chb/providers/odoo"
)

// ── Parties ──────────────────────────────────────────────────────────────

// Party is a vendor or a customer as published.
type Party struct {
	ID      string        `json:"id,omitempty"`   // stable public id; absent for individuals below members
	Type    string        `json:"type"`           // organisation | sole_trader | individual
	Name    string        `json:"name,omitempty"` // absent for individuals in public
	VAT     string        `json:"vat,omitempty"`
	Member  bool          `json:"member,omitempty"`  // individual holding a membership
	Contact *PartyContact `json:"contact,omitempty"` // stewards only
}

type PartyContact struct {
	PartnerID int    `json:"partnerId,omitempty"`
	Person    string `json:"person,omitempty"` // the contact the document was addressed to, inside the company
	Email     string `json:"email,omitempty"`
	Phone     string `json:"phone,omitempty"`
	Street    string `json:"street,omitempty"`
	ZIP       string `json:"zip,omitempty"`
	City      string `json:"city,omitempty"`
	Country   string `json:"country,omitempty"`
	Website   string `json:"website,omitempty"`
}

func partyID(partnerID int, name string) string {
	key := fmt.Sprintf("%s:res.partner:%d", odoosource.PathNamespace(), partnerID)
	if partnerID == 0 {
		key = "name:" + strings.ToLower(name)
	}
	sum := sha256.Sum256([]byte(key))
	return "p-" + hex.EncodeToString(sum[:])[:10]
}

func partyType(p OdooInvoicePartner) string {
	switch {
	case p.IsCompany || p.CompanyType == "company":
		return "organisation"
	case nameHasLegalForm(firstNonEmpty(p.Name, p.DisplayName)):
		// Odoo says "person", the name says otherwise ("DelivCo SRL (Big
		// Bag Delivery)"): a registered legal entity is an organisation.
		return "organisation"
	case strings.TrimSpace(p.VAT) != "":
		return "sole_trader"
	}
	return "individual"
}

// legalFormPattern matches the legal-form abbreviations of registered
// entities (Belgian, Dutch, French, German, English) as whole words.
var legalFormPattern = regexp.MustCompile(`(?i)(^|[\s(,])(srl|sprl|scrl|sa|nv|bv|bvba|cvba|vof|asbl|vzw|aisbl|ivzw|stichting|fondation|foundation|vereniging|gmbh|bhd|sdn\.? bhd\.?|ltd|limited|llc|inc|plc|sas|sarl|eurl|sasu|s\.r\.l\.?|s\.r\.o\.?|sro|b\.v\.?|n\.v\.?|s\.a\.?)($|[\s),.])`)

// nameHasLegalForm reports whether a partner name carries a legal form.
func nameHasLegalForm(name string) bool {
	return legalFormPattern.MatchString(strings.TrimSpace(name))
}

// documentParty is who a bill or invoice is really with: the company when
// it was addressed to one of its contacts ("XL Collective SRL, Leen
// Schelfhout" is XL Collective SRL, not Leen). Uses the stored commercial
// partner, else Odoo's "Company, Contact" display name for caches pulled
// before v3.16.1.
func documentParty(inv OdooOutgoingInvoice) Party {
	if cp := inv.CommercialPartner; cp != nil && cp.ID != 0 && cp.ID != inv.Partner.ID {
		party := partyFromPartner(*cp, "")
		party.Contact.Person = inv.Partner.Name
		return party
	}
	p := inv.Partner
	display := strings.TrimSpace(firstNonEmpty(p.DisplayName, inv.PartnerDisplayName))
	if name := strings.TrimSpace(p.Name); name != "" && !p.IsCompany && strings.HasSuffix(display, ", "+name) {
		if company := strings.TrimSpace(strings.TrimSuffix(display, ", "+name)); company != "" {
			parent := p
			parent.ID, parent.Name, parent.DisplayName = 0, company, company
			parent.IsCompany, parent.CompanyType = true, "company"
			party := partyFromPartner(parent, "")
			party.Contact.Person = name
			return party
		}
	}
	return partyFromPartner(p, inv.PartnerDisplayName)
}

func partyFromPartner(p OdooInvoicePartner, fallbackName string) Party {
	name := strings.TrimSpace(firstNonEmpty(p.Name, p.DisplayName, fallbackName))
	if at := strings.LastIndex(name, "@"); at >= 0 {
		name = strings.TrimSpace(name[at+1:]) // a mailbox as a name: keep the domain
	}
	return Party{
		ID: partyID(p.ID, name), Type: partyType(p), Name: name, VAT: strings.TrimSpace(p.VAT),
		Contact: &PartyContact{
			PartnerID: p.ID, Email: p.Email, Phone: firstNonEmpty(p.Phone, p.Mobile),
			Street: strings.TrimSpace(strings.Join([]string{p.Street, p.Street2}, " ")), ZIP: p.ZIP,
			City: p.City, Country: p.Country, Website: p.Website,
		},
	}
}

func partyForAudience(p Party, a Audience) Party {
	if a == AudienceStewards {
		return p
	}
	p.Contact = nil
	if a == AudiencePublic && p.Type == "individual" {
		return Party{Type: "individual", Member: p.Member}
	}
	return p
}

// customerForAudience: customers are named in public only when they are
// organisations. A sole trader buying from us is a person paying for a
// room or a membership, so public sees the type only.
func customerForAudience(p Party, a Audience) Party {
	if a == AudiencePublic && p.Type == "sole_trader" {
		return Party{Type: "individual", Member: p.Member}
	}
	return partyForAudience(p, a)
}

// customerIsAnonymous: the customer is merged into an anonymous row in a.
func customerIsAnonymous(p Party, a Audience) bool {
	return a == AudiencePublic && p.Type != "organisation"
}

// ── Accounts ─────────────────────────────────────────────────────────────

// AccountRef is a general-ledger account. Public files carry the code and
// its class only: some account names name a person.
type AccountRef struct {
	Code  string `json:"code"`
	Class string `json:"class"`
	Name  string `json:"name,omitempty"`
}

// accountClassLabel names the Belgian chart-of-accounts class of a code.
func accountClassLabel(code string) string {
	if len(code) < 2 {
		return ""
	}
	switch code[:2] {
	case "60":
		return "Purchases of goods"
	case "61":
		return "Services and other goods"
	case "62":
		return "Remuneration and social charges"
	case "63":
		return "Depreciation and provisions"
	case "64":
		return "Other operating charges"
	case "65":
		return "Financial charges"
	case "66":
		return "Exceptional charges"
	case "67":
		return "Taxes"
	case "70":
		return "Turnover"
	case "74":
		return "Other operating income"
	case "75":
		return "Financial income"
	}
	return "Balance sheet"
}

// isPayrollAccount: remuneration (62…) and the social-security and
// withholding-tax debts that come with it (453/454/455).
func isPayrollAccount(code string) bool {
	return strings.HasPrefix(code, "62") || strings.HasPrefix(code, "453") ||
		strings.HasPrefix(code, "454") || strings.HasPrefix(code, "455")
}

func accountRef(code, name string) *AccountRef {
	if code == "" {
		return nil
	}
	return &AccountRef{Code: code, Class: accountClassLabel(code), Name: name}
}

// incomeTypeByCategory: the incomeType of a line whose analytic tag or
// product names its category (categories.json "products").
var incomeTypeByCategory = map[string]string{
	"rental":     "room_rental",
	"rentals":    "room_rental",
	"coworking":  "coworking",
	"catering":   "catering",
	"fridge":     "drinks",
	"drinks":     "drinks",
	"membership": "membership",
	"ticket":     "tickets_events",
	"sponsoring": "sponsorship",
	"donation":   "donation",
}

// lineIncomeTyper classifies customer invoice lines: analytic tag, then
// product, then income account. The account alone is not enough: 700000
// (membership dues) also carries rooms, catering and coworking products.
type lineIncomeTyper struct {
	products []categoryProduct
	slugs    map[string]bool
}

func newLineIncomeTyper() lineIncomeTyper {
	cats := LoadCategories()
	t := lineIncomeTyper{products: categoryProductGlobs(cats), slugs: map[string]bool{}}
	for _, c := range cats {
		t.slugs[c.Slug] = true
	}
	return t
}

func (t lineIncomeTyper) of(li OdooInvoiceLineItem) string {
	if li.DisplayType != "" && li.DisplayType != "product" {
		return incomeType(li.AccountCode) // notes and sections carry no money
	}
	if membershipProductIDs[li.ProductID] && !invoiceLineHasVAT(li) {
		return "membership"
	}
	typ := incomeTypeByCategory[invoiceLineTaggedCategory(li, t.products, t.slugs)]
	if typ == "" {
		typ = incomeType(li.AccountCode)
	}
	if typ == "membership" && !strictMembershipLine(li) {
		if invoiceLineHasVAT(li) {
			return "sales_services" // memberships carry no VAT
		}
		return "other_income" // e.g. a €1000 corporate membership
	}
	return typ
}

// incomeType classifies a customer invoice line by its income account.
func incomeType(code string) string {
	switch code {
	case "704200":
		return "membership"
	case "700100":
		return "room_rental"
	case "700150":
		return "tickets_events"
	case "700110", "749001", "616460":
		return "sponsorship"
	case "740040":
		return "donation"
	case "700200":
		return "reinvoiced_costs"
	}
	switch {
	case strings.HasPrefix(code, "70"):
		return "sales_services"
	case strings.HasPrefix(code, "74"):
		return "other_income"
	}
	return "other"
}

// ── Money ────────────────────────────────────────────────────────────────

// eurFactor converts a document's amounts to EUR (company currency): 1 for
// EUR documents, |amount_total_signed| / total otherwise.
func eurFactor(inv OdooOutgoingInvoice) float64 {
	if inv.Currency == "" || inv.Currency == "EUR" || inv.TotalAmount == 0 || inv.TotalSignedAmount == 0 {
		return 1
	}
	return math.Abs(inv.TotalSignedAmount) / inv.TotalAmount
}

func docStatus(inv OdooOutgoingInvoice) string {
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

// odooDocURI is the global identifier of an Odoo record,
// odoo:<host>:<db>:<model>:<id> (OdooURI). The same string is used in
// public files, on Nostr and on the website. Host and database come from
// ODOO_URL / ODOO_DATABASE; a document's own link is the fallback host.
func odooDocURI(model string, id int, docURL string) string {
	if id == 0 {
		return ""
	}
	rawURL := os.Getenv("ODOO_URL")
	if rawURL == "" {
		rawURL = docURL
	}
	host := OdooHost(rawURL)
	db := strings.TrimSpace(os.Getenv("ODOO_DATABASE"))
	if db == "" && os.Getenv("ODOO_URL") != "" {
		db = odooDBFromURL(os.Getenv("ODOO_URL"))
	}
	if db == "" {
		db = odoosource.PathNamespace()
	}
	if db == "" {
		db = odooDBFromURL("https://" + host)
	}
	return OdooURI(host, db, model, id)
}

// docID is the v3.16 local id ("b-…"). Deprecated: use the uri.
func docID(prefix string, odooID int) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s:account.move:%d", odoosource.PathNamespace(), odooID)))
	return prefix + hex.EncodeToString(sum[:])[:10]
}

// ── expenses.json ────────────────────────────────────────────────────────

type ExpenseLine struct {
	Description string      `json:"description,omitempty"`
	Product     string      `json:"product,omitempty"`
	Quantity    float64     `json:"quantity,omitempty"`
	UnitPrice   float64     `json:"unitPrice,omitempty"`
	Untaxed     float64     `json:"untaxedAmount"`
	Total       float64     `json:"totalAmount"`
	VATRate     string      `json:"vatRate,omitempty"`
	Account     *AccountRef `json:"account,omitempty"`
	Category    string      `json:"category,omitempty"`
	Payroll     bool        `json:"payroll,omitempty"`
}

type Expense struct {
	URI         string        `json:"uri"`    // odoo:<host>:<db>:account.move:<id>, or hr.expense for a claim not posted yet
	ID          string        `json:"id"`     // deprecated alias ("b-…"/"x-…"), removed in the next release: use uri
	Number      string        `json:"number"` // our accounting number
	Kind        string        `json:"kind"`   // bill | credit_note | expense
	Status      string        `json:"status"` // pending | partially_paid | paid | reversed | submitted
	Date        string        `json:"date"`
	DueDate     string        `json:"dueDate,omitempty"`
	Vendor      Party         `json:"vendor"`
	VendorRef   string        `json:"vendorRef,omitempty"`
	Description string        `json:"description,omitempty"`
	Category    string        `json:"category,omitempty"`
	Collective  string        `json:"collective,omitempty"`
	Event       string        `json:"event,omitempty"`
	Currency    string        `json:"currency"`
	Untaxed     float64       `json:"untaxedAmount"`
	VAT         float64       `json:"vatAmount"`
	Total       float64       `json:"totalAmount"`
	TotalEUR    float64       `json:"totalAmountEUR"`
	AmountDue   float64       `json:"amountDue"`
	HasDocument bool          `json:"hasDocument"`
	Payroll     bool          `json:"payroll,omitempty"`
	Note        string        `json:"note,omitempty"` // text of a trusted Nostr annotation on this uri
	Lines       []ExpenseLine `json:"lines"`

	Stewards *DocStewards `json:"stewards,omitempty"`
}

type DocStewards struct {
	OdooID           int                      `json:"odooId,omitempty"`
	OdooURL          string                   `json:"odooUrl,omitempty"`
	PaymentReference string                   `json:"paymentReference,omitempty"`
	PartnerBank      *OdooInvoiceBankAccount  `json:"partnerBank,omitempty"`
	Payments         []OdooInvoicePayment     `json:"payments,omitempty"`
	Attachments      []OdooDocumentAttachment `json:"attachments,omitempty"`
	Employee         string                   `json:"employee,omitempty"`
	ExpenseState     string                   `json:"expenseState,omitempty"`
}

type MoneyTotals struct {
	Count   int     `json:"count"`
	Untaxed float64 `json:"untaxedAmount"`
	Total   float64 `json:"totalAmount"`
	Paid    float64 `json:"paidAmount"`
	Due     float64 `json:"amountDue"`
}

type CategoryTotal struct {
	Category string  `json:"category"`
	Count    int     `json:"count"`
	Total    float64 `json:"totalAmount"`
}

type ExpensesFile struct {
	GeneratedAt string          `json:"generatedAt"`
	Scope       string          `json:"scope"`  // month | year
	Period      string          `json:"period"` // "2026-08" or "2026"
	Currency    string          `json:"currency"`
	Totals      MoneyTotals     `json:"totals"` // EUR; credit notes subtract
	ByCategory  []CategoryTotal `json:"byCategory"`
	Expenses    []Expense       `json:"expenses"`
}

var vatRateFromTax = regexp.MustCompile(`(\d+(?:[.,]\d+)?)\s*%`)

func lineVATRate(l OdooInvoiceLineItem) string {
	if len(l.Taxes) == 1 && l.Taxes[0].AmountType == "percent" {
		return fmt.Sprintf("%g%%", l.Taxes[0].Amount)
	}
	if len(l.Taxes) == 1 {
		if m := vatRateFromTax.FindStringSubmatch(l.Taxes[0].Name); m != nil {
			return strings.ReplaceAll(m[1], ",", ".") + "%"
		}
	}
	return ""
}

func expenseFromBill(inv OdooOutgoingInvoice, claim *OdooExpense) Expense {
	f := eurFactor(inv)
	e := Expense{
		URI: odooDocURI("account.move", inv.ID, inv.InvoiceURL),
		ID:  docID("b-", inv.ID), Number: inv.Number, Kind: "bill", Status: docStatus(inv),
		Date: firstNonEmpty(inv.InvoiceDate, inv.Date), DueDate: inv.DueDate,
		Vendor: documentParty(inv), VendorRef: firstNonEmpty(inv.Ref, inv.Title),
		Category: inv.Category, Collective: inv.Collective, Event: inv.Event,
		Currency: firstNonEmpty(inv.Currency, "EUR"), Untaxed: inv.UntaxedAmount, VAT: inv.VATAmount,
		Total: inv.TotalAmount, TotalEUR: round2(inv.TotalAmount * f), AmountDue: inv.ResidualAmount,
		HasDocument: len(inv.Attachments) > 0,
		Stewards: &DocStewards{OdooID: inv.ID, OdooURL: inv.InvoiceURL, PaymentReference: inv.Reference,
			PartnerBank: inv.PartnerBank, Payments: inv.Payments, Attachments: inv.Attachments},
	}
	if inv.MoveType == "in_refund" {
		e.Kind = "credit_note"
	}
	if claim != nil {
		e.Kind = "expense"
		e.Stewards.Employee = claim.Employee
		e.Stewards.ExpenseState = claim.State
	}
	if e.Status == "paid" || e.Status == "reversed" {
		e.AmountDue = 0
	}
	var descs []string
	payroll := false
	for _, li := range inv.LineItems {
		if li.DisplayType != "" && li.DisplayType != "product" {
			continue
		}
		if li.SubtotalAmount == 0 && li.TotalAmount == 0 {
			continue // a note line
		}
		l := ExpenseLine{
			Description: strings.TrimSpace(li.Title), Product: strings.TrimSpace(li.ProductName),
			Quantity: li.Quantity, UnitPrice: li.UnitPrice, Untaxed: li.SubtotalAmount, Total: li.TotalAmount,
			VATRate: lineVATRate(li), Account: accountRef(li.AccountCode, li.AccountName), Category: li.Category,
			Payroll: isPayrollAccount(li.AccountCode),
		}
		payroll = payroll || l.Payroll
		e.Lines = append(e.Lines, l)
		if d := firstNonEmpty(l.Description, l.Product); d != "" && !containsString(descs, d) && !l.Payroll {
			descs = append(descs, d)
		}
		if e.Category == "" && li.Category != "" {
			e.Category = li.Category
		}
	}
	e.Payroll = payroll
	e.Description = strings.Join(descs, ", ")
	if e.Category == "" {
		e.Category = mainAccountClass(e.Lines)
	}
	return e
}

// expenseFromClaim turns an expense claim that has no posted bill yet into
// an Expense (status "submitted").
func expenseFromClaim(c OdooExpense) Expense {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s:hr.expense:%d", odoosource.PathNamespace(), c.ID)))
	e := Expense{
		URI: odooDocURI("hr.expense", c.ID, ""),
		ID:  "x-" + hex.EncodeToString(sum[:])[:10], Number: "", Kind: "expense", Status: "submitted",
		Date: c.Date, Vendor: Party{ID: partyID(0, c.Employee), Type: "individual", Name: c.Employee},
		Currency: firstNonEmpty(c.Currency, "EUR"), Total: c.Total, TotalEUR: round2(c.Total), AmountDue: c.Total,
		Untaxed: round2(c.Total - c.TaxAmount), VAT: c.TaxAmount,
		Lines: []ExpenseLine{{Description: c.Name, Product: c.Product, Quantity: c.Quantity, UnitPrice: c.UnitPrice,
			Untaxed: round2(c.Total - c.TaxAmount), Total: c.Total, Account: accountRef(c.AccountCode, c.AccountName)}},
		Stewards: &DocStewards{Employee: c.Employee, ExpenseState: c.State},
	}
	e.Description = firstNonEmpty(c.Name, c.Product)
	e.Category = mainAccountClass(e.Lines)
	return e
}

func mainAccountClass(lines []ExpenseLine) string {
	best, amount := "", 0.0
	by := map[string]float64{}
	for _, l := range lines {
		if l.Account != nil {
			by[l.Account.Class] += math.Abs(l.Untaxed)
		}
	}
	keys := make([]string, 0, len(by))
	for k := range by {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if by[k] > amount {
			best, amount = k, by[k]
		}
	}
	return best
}

func expenseForAudience(e Expense, a Audience) Expense {
	if a == AudienceStewards {
		return e
	}
	e.Stewards = nil
	individual := e.Vendor.Type == "individual"
	// A sole trader is a natural person: public keeps their business name
	// and what they sold, but never their free text or the event the bill
	// is tagged with, which would place a person at a date and a place.
	person := individual || e.Vendor.Type == "sole_trader"
	e.Vendor = partyForAudience(e.Vendor, a)
	lines := make([]ExpenseLine, len(e.Lines))
	for i, l := range e.Lines {
		if l.Payroll {
			// Remuneration details are about identifiable staff: amounts
			// stay (they are in the accounts anyway), the text goes.
			l.Description, l.Product = "", ""
		}
		if a == AudiencePublic {
			if l.Account != nil {
				ref := *l.Account
				ref.Name = ""
				l.Account = &ref
			}
			if person {
				l.Description = "" // free text about a person; the product name stays
			}
		}
		lines[i] = l
	}
	e.Lines = lines
	if a == AudiencePublic && person {
		e.Event = ""
		e.Note = ""
		e.VendorRef = ""
		var products []string
		for _, l := range lines {
			if l.Product != "" && !containsString(products, l.Product) {
				products = append(products, l.Product)
			}
		}
		e.Description = strings.Join(products, ", ")
	}
	if e.Payroll && a != AudienceStewards {
		e.Description = "Payroll"
	}
	return e
}

func expenseTotals(list []Expense) (MoneyTotals, []CategoryTotal) {
	var t MoneyTotals
	by := map[string]*CategoryTotal{}
	for _, e := range list {
		if e.Status == "reversed" {
			continue
		}
		sign := 1.0
		if e.Kind == "credit_note" {
			sign = -1
		}
		f := 1.0
		if e.Total != 0 {
			f = e.TotalEUR / e.Total
		}
		t.Count++
		t.Untaxed = round2(t.Untaxed + sign*e.Untaxed*f)
		t.Total = round2(t.Total + sign*e.TotalEUR)
		t.Due = round2(t.Due + sign*e.AmountDue*f)
		cat := firstNonEmpty(e.Category, "uncategorised")
		if by[cat] == nil {
			by[cat] = &CategoryTotal{Category: cat}
		}
		by[cat].Count++
		by[cat].Total = round2(by[cat].Total + sign*e.TotalEUR)
	}
	t.Paid = round2(t.Total - t.Due)
	var cats []CategoryTotal
	for _, c := range by {
		cats = append(cats, *c)
	}
	sort.Slice(cats, func(i, j int) bool {
		if cats[i].Total != cats[j].Total {
			return cats[i].Total > cats[j].Total
		}
		return cats[i].Category < cats[j].Category
	})
	return t, cats
}

// ── vendors.json ─────────────────────────────────────────────────────────

type VendorRow struct {
	Vendor      Party    `json:"vendor"`
	Category    string   `json:"category,omitempty"` // where most of the money went
	Categories  []string `json:"categories,omitempty"`
	Documents   int      `json:"documents"`
	Individuals int      `json:"individuals,omitempty"` // public: individuals merged into this row
	Untaxed     float64  `json:"untaxedAmount"`
	Total       float64  `json:"totalAmount"`
	Paid        float64  `json:"paidAmount"`
	Due         float64  `json:"amountDue"`
}

type VendorsFile struct {
	GeneratedAt string      `json:"generatedAt"`
	Scope       string      `json:"scope"`
	Period      string      `json:"period"`
	Currency    string      `json:"currency"`
	Totals      MoneyTotals `json:"totals"` // count = vendors
	Vendors     []VendorRow `json:"vendors"`
}

// vendorsFrom aggregates expenses per vendor, already projected for a.
// In public, individuals are merged per category.
func vendorsFrom(expenses []Expense, a Audience) []VendorRow {
	type acc struct {
		row  VendorRow
		cats map[string]float64
		ids  map[string]bool
	}
	rows := map[string]*acc{}
	var order []string
	for _, full := range expenses {
		if full.Status == "reversed" {
			continue
		}
		e := expenseForAudience(full, a)
		key := full.Vendor.ID
		if a == AudiencePublic && full.Vendor.Type == "individual" {
			key = "individuals:" + firstNonEmpty(full.Category, "uncategorised")
		}
		r := rows[key]
		if r == nil {
			r = &acc{row: VendorRow{Vendor: e.Vendor}, cats: map[string]float64{}, ids: map[string]bool{}}
			rows[key] = r
			order = append(order, key)
		}
		sign := 1.0
		if full.Kind == "credit_note" {
			sign = -1
		}
		f := 1.0
		if full.Total != 0 {
			f = full.TotalEUR / full.Total
		}
		r.row.Documents++
		r.row.Untaxed = round2(r.row.Untaxed + sign*full.Untaxed*f)
		r.row.Total = round2(r.row.Total + sign*full.TotalEUR)
		r.row.Due = round2(r.row.Due + sign*full.AmountDue*f)
		r.cats[firstNonEmpty(full.Category, "uncategorised")] += sign * full.TotalEUR
		r.ids[full.Vendor.ID] = true
	}
	var out []VendorRow
	for _, k := range order {
		r := rows[k]
		r.row.Paid = round2(r.row.Total - r.row.Due)
		r.row.Categories, r.row.Category = sortedCategories(r.cats)
		if strings.HasPrefix(k, "individuals:") {
			r.row.Individuals = len(r.ids)
		}
		out = append(out, r.row)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Total != out[j].Total {
			return out[i].Total > out[j].Total
		}
		return out[i].Vendor.Name+out[i].Category < out[j].Vendor.Name+out[j].Category
	})
	return out
}

func sortedCategories(by map[string]float64) ([]string, string) {
	keys := make([]string, 0, len(by))
	for k := range by {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if by[keys[i]] != by[keys[j]] {
			return by[keys[i]] > by[keys[j]]
		}
		return keys[i] < keys[j]
	})
	main := ""
	if len(keys) > 0 {
		main = keys[0]
	}
	return keys, main
}

func rowTotals(n int, untaxed, total, due []float64) MoneyTotals {
	t := MoneyTotals{Count: n}
	for i := range total {
		t.Untaxed = round2(t.Untaxed + untaxed[i])
		t.Total = round2(t.Total + total[i])
		t.Due = round2(t.Due + due[i])
	}
	t.Paid = round2(t.Total - t.Due)
	return t
}

// ── customers.json ───────────────────────────────────────────────────────

type CustomerInvoiceRef struct {
	Number  string  `json:"number"`
	Date    string  `json:"date"`
	Total   float64 `json:"totalAmount"`
	Due     float64 `json:"amountDue"`
	OdooURL string  `json:"odooUrl,omitempty"`
}

type CustomerRow struct {
	Customer     Party                `json:"customer"`
	IncomeType   string               `json:"incomeType,omitempty"` // where most of the money came from
	IncomeTypes  []string             `json:"incomeTypes,omitempty"`
	Products     []string             `json:"products,omitempty"`
	Invoices     []string             `json:"invoices"` // URIs of the invoices and credit notes in this row
	InvoiceCount int                  `json:"invoiceCount"`
	Individuals  int                  `json:"individuals,omitempty"` // public: individuals merged into this row
	Untaxed      float64              `json:"untaxedAmount"`
	Total        float64              `json:"totalAmount"`
	Received     float64              `json:"receivedAmount"`
	Due          float64              `json:"amountDue"`
	InvoiceRefs  []CustomerInvoiceRef `json:"invoiceList,omitempty"` // stewards
}

type CustomersFile struct {
	GeneratedAt string          `json:"generatedAt"`
	Scope       string          `json:"scope"`
	Period      string          `json:"period"`
	Currency    string          `json:"currency"`
	Totals      MoneyTotals     `json:"totals"` // count = customers; paidAmount = received
	ByIncome    []CategoryTotal `json:"byIncomeType"`
	Customers   []CustomerRow   `json:"customers"`
}

// customerDoc is one posted customer invoice or credit note, normalised.
type customerDoc struct {
	inv   OdooOutgoingInvoice
	party Party
	types map[string]float64 // income type → untaxed EUR
	sign  float64
	f     float64
	date  string
}

func customersFrom(docs []customerDoc, a Audience) ([]CustomerRow, []CategoryTotal) {
	type acc struct {
		row   CustomerRow
		types map[string]float64
		prods map[string]bool
		ids   map[string]bool
	}
	rows := map[string]*acc{}
	var order []string
	byIncome := map[string]*CategoryTotal{}
	for _, d := range docs {
		main := ""
		if ks, m := sortedCategories(d.types); len(ks) > 0 {
			main = m
		}
		key := d.party.ID
		anonymous := customerIsAnonymous(d.party, a)
		if anonymous {
			key = "individuals:" + firstNonEmpty(main, "other")
		}
		r := rows[key]
		if r == nil {
			r = &acc{row: CustomerRow{Customer: customerForAudience(d.party, a)}, types: map[string]float64{}, prods: map[string]bool{}, ids: map[string]bool{}}
			rows[key] = r
			order = append(order, key)
		}
		total := d.sign * d.inv.TotalAmount * d.f
		due := d.sign * d.inv.ResidualAmount * d.f
		if docStatus(d.inv) == "paid" {
			due = 0
		}
		r.row.InvoiceCount++
		r.row.Invoices = append(r.row.Invoices, odooDocURI("account.move", d.inv.ID, d.inv.InvoiceURL))
		r.row.Untaxed = round2(r.row.Untaxed + d.sign*d.inv.UntaxedAmount*d.f)
		r.row.Total = round2(r.row.Total + total)
		r.row.Due = round2(r.row.Due + due)
		for t, v := range d.types {
			r.types[t] += d.sign * v
			if byIncome[t] == nil {
				byIncome[t] = &CategoryTotal{Category: t}
			}
			byIncome[t].Count++
			byIncome[t].Total = round2(byIncome[t].Total + d.sign*v)
		}
		if !anonymous {
			for _, li := range d.inv.LineItems {
				if p := strings.TrimSpace(li.ProductName); p != "" && li.SubtotalAmount != 0 {
					r.prods[p] = true
				}
			}
		}
		r.ids[d.party.ID] = true
		if a == AudienceStewards {
			r.row.InvoiceRefs = append(r.row.InvoiceRefs, CustomerInvoiceRef{
				Number: d.inv.Number, Date: d.date, Total: round2(total), Due: round2(due), OdooURL: d.inv.InvoiceURL})
		}
	}
	var out []CustomerRow
	for _, k := range order {
		r := rows[k]
		r.row.Received = round2(r.row.Total - r.row.Due)
		r.row.IncomeTypes, r.row.IncomeType = sortedCategories(r.types)
		for p := range r.prods {
			r.row.Products = append(r.row.Products, p)
		}
		sort.Strings(r.row.Products)
		if strings.HasPrefix(k, "individuals:") {
			r.row.Individuals = len(r.ids)
		}
		out = append(out, r.row)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Total != out[j].Total {
			return out[i].Total > out[j].Total
		}
		return out[i].Customer.Name+out[i].IncomeType < out[j].Customer.Name+out[j].IncomeType
	})
	var inc []CategoryTotal
	for _, c := range byIncome {
		inc = append(inc, *c)
	}
	sort.Slice(inc, func(i, j int) bool {
		if inc[i].Total != inc[j].Total {
			return inc[i].Total > inc[j].Total
		}
		return inc[i].Category < inc[j].Category
	})
	return out, inc
}

// ── bookings.json ────────────────────────────────────────────────────────

type BookingRow struct {
	Room     string  `json:"room"` // room slug
	RoomName string  `json:"roomName"`
	Start    string  `json:"start"`
	End      string  `json:"end"`
	Hours    float64 `json:"hours"`
	Public   bool    `json:"public"` // matches an event published on the public calendar
	Title    string  `json:"title,omitempty"`
	EventURL string  `json:"eventUrl,omitempty"`
	// Payment: "tokens" | "euros" | null (unknown), from the calendar
	// event description (cmd/booking_payment.go).
	Payment *string `json:"payment"`
}

type RentalRow struct {
	URI           string  `json:"uri"`  // the invoice: odoo:<host>:<db>:account.move:<id>
	Date          string  `json:"date"` // invoice date
	Room          string  `json:"room,omitempty"`
	Product       string  `json:"product,omitempty"`
	Description   string  `json:"description,omitempty"` // members/stewards
	Quantity      float64 `json:"quantity,omitempty"`
	Untaxed       float64 `json:"untaxedAmount"`
	Total         float64 `json:"totalAmount"`
	Customer      Party   `json:"customer"`
	InvoiceNumber string  `json:"invoiceNumber,omitempty"` // members/stewards
	Event         string  `json:"event,omitempty"`         // from a trusted annotation on the invoice
	Note          string  `json:"note,omitempty"`          // its text (not public for individual customers)
}

type RoomSummary struct {
	Room           string  `json:"room"`
	RoomName       string  `json:"roomName"`
	Bookings       int     `json:"bookings"`
	Hours          float64 `json:"hours"`
	PublicBookings int     `json:"publicBookings"`
	RentalLines    int     `json:"rentalLines"`
	RentalRevenue  float64 `json:"rentalRevenue"` // untaxed EUR
}

type BookingsMonth struct {
	Month string        `json:"month"`
	Rooms []RoomSummary `json:"rooms"`
}

type BookingsFile struct {
	GeneratedAt string          `json:"generatedAt"`
	Scope       string          `json:"scope"`
	Period      string          `json:"period"`
	Currency    string          `json:"currency"`
	Rooms       []RoomSummary   `json:"rooms"`            // per room over the period; room "" = rental not tied to a room
	Months      []BookingsMonth `json:"months,omitempty"` // year file: per month
	Bookings    []BookingRow    `json:"bookings"`
	Rentals     []RentalRow     `json:"rentals"`
}

// roomForRentalLine guesses the room from a rental line's product and text.
func roomForRentalLine(rooms []RoomInfo, li OdooInvoiceLineItem) string {
	text := strings.ToLower(li.ProductName + " " + li.Title)
	for _, r := range rooms {
		words := []string{strings.ToLower(r.Slug), strings.ToLower(r.Name)}
		switch r.Slug {
		case "coworking":
			words = append(words, "cowork")
		case "mushroom":
			words = append(words, "mush room", "mush")
		case "phonebooth":
			words = append(words, "phone booth")
		}
		for _, w := range words {
			if w != "" && strings.Contains(text, w) {
				return r.Slug
			}
		}
	}
	return ""
}

func bookingForAudience(b BookingRow, a Audience) BookingRow {
	if a == AudiencePublic && !b.Public {
		b.Title = ""
	}
	return b
}

func rentalForAudience(r RentalRow, a Audience) RentalRow {
	anonymous := customerIsAnonymous(r.Customer, a)
	r.Customer = customerForAudience(r.Customer, a)
	if a == AudiencePublic {
		r.Description = ""
		r.InvoiceNumber = ""
		if anonymous {
			r.Note, r.Event = "", "" // would place a person at a date
		}
	}
	return r
}

func summariseRooms(rooms []RoomInfo, bookings []BookingRow, rentals []RentalRow) []RoomSummary {
	by := map[string]*RoomSummary{}
	names := map[string]string{}
	for _, r := range rooms {
		names[r.Slug] = r.Name
	}
	get := func(slug string) *RoomSummary {
		if by[slug] == nil {
			by[slug] = &RoomSummary{Room: slug, RoomName: names[slug]}
		}
		return by[slug]
	}
	for _, b := range bookings {
		s := get(b.Room)
		s.Bookings++
		s.Hours = round2(s.Hours + b.Hours)
		if b.Public {
			s.PublicBookings++
		}
	}
	for _, r := range rentals {
		s := get(r.Room)
		s.RentalLines++
		s.RentalRevenue = round2(s.RentalRevenue + r.Untaxed)
	}
	var out []RoomSummary
	for _, s := range by {
		out = append(out, *s)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].RentalRevenue != out[j].RentalRevenue {
			return out[i].RentalRevenue > out[j].RentalRevenue
		}
		return out[i].Room < out[j].Room
	})
	return out
}

// ── Annotations ──────────────────────────────────────────────────────────

// loadOdooAnnotations reads every month's odoo-annotations.json (written by
// `chb nostr pull`), trusted authors only, keyed by odoo: URI.
func loadOdooAnnotations(dataDir string) map[string]*TxAnnotation {
	out := map[string]*TxAnnotation{}
	trusted := nostrTrustedPubkeys()
	classifiers := nostrCategoryAuthors()
	for _, ym := range dataMonthRange(dataDir) {
		data, err := os.ReadFile(nostrsource.Path(dataDir, ym[:4], ym[5:], nostrsource.OdooAnnotationsFile))
		if err != nil {
			continue
		}
		var cache NostrAnnotationCache
		if json.Unmarshal(data, &cache) != nil {
			continue
		}
		for uri, a := range cache.Annotations {
			if !annotationTrusted(a, trusted) {
				continue
			}
			if cur, ok := out[uri]; !ok || a.CreatedAt > cur.CreatedAt {
				out[uri] = restrictAnnotation(a, classifiers)
			}
		}
	}
	return out
}

// applyAnnotation overrides category, collective and event (a trusted
// annotation outranks Odoo) and returns the annotation's text.
func applyAnnotation(a *TxAnnotation, category, collective, event *string) string {
	if a == nil {
		return ""
	}
	if a.Category != "" {
		*category = a.Category
	}
	if a.Collective != "" {
		*collective = a.Collective
	}
	if a.Event != "" {
		*event = a.Event
	}
	return strings.TrimSpace(a.Description)
}

// ── Generation ───────────────────────────────────────────────────────────

func loadAllCachedInvoices(dataDir string) map[int]OdooOutgoingInvoice {
	out := map[int]OdooOutgoingInvoice{}
	for _, p := range globOdooMonthFiles(dataDir, odoosource.InvoicesFile) {
		rel, err := filepath.Rel(dataDir, p)
		if err != nil {
			continue
		}
		parts := strings.Split(filepath.ToSlash(rel), "/")
		for _, inv := range loadCachedInvoiceMonth(dataDir, parts[0], parts[1]) {
			out[inv.ID] = inv
		}
	}
	return out
}

// publicEventsByDay indexes the month's public events by Brussels day.
func publicEventsByDay(dataDir, year, month string) map[string][]FullEvent {
	out := map[string][]FullEvent{}
	data, err := os.ReadFile(audiencePath(dataDir, year, month, AudiencePublic, "events.json"))
	if err != nil {
		return out
	}
	var f FullEventsFile
	if json.Unmarshal(data, &f) != nil {
		return out
	}
	for _, ev := range f.Events {
		if t, err := time.Parse(time.RFC3339, ev.StartAt); err == nil {
			day := t.In(BrusselsTZ()).Format("2006-01-02")
			out[day] = append(out[day], ev)
		}
	}
	return out
}

var titleNonAlnum = regexp.MustCompile(`[^a-z0-9]+`)

func normTitle(s string) string {
	return strings.Trim(titleNonAlnum.ReplaceAllString(strings.ToLower(s), " "), " ")
}

// matchPublicEvent finds the public event a room booking hosts: same day,
// and either the same start minute or titles where one contains the other
// (at least 5 characters). Room calendars and Luma rarely agree to the
// minute.
func matchPublicEvent(events []FullEvent, start time.Time, title string) (FullEvent, bool) {
	nt := normTitle(title)
	for _, ev := range events {
		t, err := time.Parse(time.RFC3339, ev.StartAt)
		if err != nil {
			continue
		}
		ne := normTitle(ev.Name)
		if len(nt) >= 5 && len(ne) >= 5 && (strings.Contains(nt, ne) || strings.Contains(ne, nt)) {
			return ev, true
		}
		if math.Abs(t.Sub(start).Minutes()) <= 1 {
			return ev, true
		}
	}
	return FullEvent{}, false
}

type accountingPeriod struct {
	expenses  []Expense
	customers []customerDoc
	bookings  []BookingRow
	rentals   []RentalRow
}

// generateAccountingFiles writes expenses/vendors/customers/bookings for
// every month with data, and the year aggregates. Returns months written.
func generateAccountingFiles(dataDir string) (int, error) {
	now := time.Now().UTC().Format(time.RFC3339)
	rooms, _ := LoadRooms()
	bills := loadAllCachedBills(dataDir)
	invoices := loadAllCachedInvoices(dataDir)
	claims := loadAllOdooExpenses(dataDir)

	annotations := loadOdooAnnotations(dataDir)

	claimByMove := map[int]*OdooExpense{}
	for id := range claims {
		c := claims[id]
		if c.MoveID != 0 {
			claimByMove[c.MoveID] = &c
		}
	}

	periods := map[string]*accountingPeriod{}
	get := func(ym string) *accountingPeriod {
		if periods[ym] == nil {
			periods[ym] = &accountingPeriod{}
		}
		return periods[ym]
	}

	for _, inv := range bills {
		if inv.State != "posted" {
			continue
		}
		e := expenseFromBill(inv, claimByMove[inv.ID])
		e.Note = applyAnnotation(annotations[e.URI], &e.Category, &e.Collective, &e.Event)
		if c := claimByMove[inv.ID]; c != nil && e.Note == "" {
			// An annotation on the claim (hr.expense) counts for its bill too.
			e.Note = applyAnnotation(annotations[odooDocURI("hr.expense", c.ID, "")], &e.Category, &e.Collective, &e.Event)
		}
		if len(e.Date) >= 7 {
			get(e.Date[:7]).expenses = append(get(e.Date[:7]).expenses, e)
		}
	}
	for _, c := range claims {
		if c.MoveID != 0 && bills[c.MoveID].ID != 0 {
			continue // posted: already listed as its bill
		}
		switch c.State {
		case "draft", "refused", "cancel":
			continue
		}
		e := expenseFromClaim(c)
		e.Note = applyAnnotation(annotations[e.URI], &e.Category, &e.Collective, &e.Event)
		if len(e.Date) >= 7 {
			get(e.Date[:7]).expenses = append(get(e.Date[:7]).expenses, e)
		}
	}

	// Who is a member: any customer invoice with a membership line.
	typer := newLineIncomeTyper()
	members := map[string]bool{}
	for _, inv := range invoices {
		for _, li := range inv.LineItems {
			if typer.of(li) == "membership" {
				members[documentParty(inv).ID] = true
			}
		}
	}
	for _, inv := range invoices {
		if inv.State != "posted" {
			continue
		}
		date := firstNonEmpty(inv.InvoiceDate, inv.Date)
		if len(date) < 7 {
			continue
		}
		d := customerDoc{inv: inv, party: documentParty(inv),
			types: map[string]float64{}, sign: 1, f: eurFactor(inv), date: date}
		if inv.MoveType == "out_refund" {
			d.sign = -1
		}
		d.party.Member = d.party.Type != "organisation" && members[d.party.ID]
		for _, li := range inv.LineItems {
			if li.SubtotalAmount == 0 {
				continue
			}
			t := typer.of(li)
			d.types[t] += li.SubtotalAmount * d.f
			if t == "room_rental" && docStatus(inv) != "reversed" {
				rental := RentalRow{
					URI:  odooDocURI("account.move", inv.ID, inv.InvoiceURL),
					Date: date, Room: roomForRentalLine(rooms, li), Product: strings.TrimSpace(li.ProductName),
					Description: strings.TrimSpace(li.Title), Quantity: li.Quantity,
					Untaxed: round2(d.sign * li.SubtotalAmount * d.f), Total: round2(d.sign * li.TotalAmount * d.f),
					Customer: d.party, InvoiceNumber: inv.Number,
				}
				var cat, col string
				rental.Note = applyAnnotation(annotations[rental.URI], &cat, &col, &rental.Event)
				get(date[:7]).rentals = append(get(date[:7]).rentals, rental)
			}
		}
		if docStatus(inv) == "reversed" {
			continue
		}
		get(date[:7]).customers = append(get(date[:7]).customers, d)
	}

	if all, err := loadAllBookings(); err == nil {
		slugByName := map[string]string{}
		for _, r := range rooms {
			slugByName[r.Name] = r.Slug
		}
		seen := map[string]bool{}
		eventsByMonth := map[string]map[string][]FullEvent{}
		for _, b := range all {
			start := b.Start.In(BrusselsTZ())
			key := b.UID + "|" + start.Format(time.RFC3339) + "|" + b.Room
			if seen[key] {
				continue // the same event archived in more than one month
			}
			seen[key] = true
			ym := start.Format("2006-01")
			if eventsByMonth[ym] == nil {
				eventsByMonth[ym] = publicEventsByDay(dataDir, ym[:4], ym[5:])
			}
			row := BookingRow{Room: firstNonEmpty(slugByName[b.Room], b.Room), RoomName: b.Room,
				Start: start.Format(time.RFC3339), End: b.End.In(BrusselsTZ()).Format(time.RFC3339),
				Hours: round2(b.End.Sub(b.Start).Hours()), Title: strings.TrimSpace(b.Title)}
			if ev, ok := matchPublicEvent(eventsByMonth[ym][start.Format("2006-01-02")], b.Start, b.Title); ok {
				row.Public, row.Title, row.EventURL = true, ev.Name, ev.URL
			}
			if p := bookingPayment(b.Description); p != "" {
				row.Payment = &p
			}
			get(ym).bookings = append(get(ym).bookings, row)
		}
	}

	months := make([]string, 0, len(periods))
	for ym := range periods {
		months = append(months, ym)
	}
	sort.Strings(months)
	byYear := map[string]*accountingPeriod{}
	var yearMonths = map[string][]string{}
	for _, ym := range months {
		p := periods[ym]
		writeAccountingPeriod(dataDir, ym[:4], ym[5:], "month", ym, p, rooms, nil, now)
		y := ym[:4]
		if byYear[y] == nil {
			byYear[y] = &accountingPeriod{}
		}
		byYear[y].expenses = append(byYear[y].expenses, p.expenses...)
		byYear[y].customers = append(byYear[y].customers, p.customers...)
		byYear[y].bookings = append(byYear[y].bookings, p.bookings...)
		byYear[y].rentals = append(byYear[y].rentals, p.rentals...)
		yearMonths[y] = append(yearMonths[y], ym)
	}
	for y, p := range byYear {
		var perMonth []BookingsMonth
		for _, ym := range yearMonths[y] {
			pm := periods[ym]
			if len(pm.bookings) == 0 && len(pm.rentals) == 0 {
				continue
			}
			perMonth = append(perMonth, BookingsMonth{Month: ym, Rooms: summariseRooms(rooms, pm.bookings, pm.rentals)})
		}
		writeAccountingPeriod(dataDir, y, "", "year", y, p, rooms, perMonth, now)
	}
	return len(months), nil
}

func writeAccountingPeriod(dataDir, year, month, scope, period string, p *accountingPeriod, rooms []RoomInfo, perMonth []BookingsMonth, now string) {
	sort.Slice(p.expenses, func(i, j int) bool {
		if p.expenses[i].Date != p.expenses[j].Date {
			return p.expenses[i].Date > p.expenses[j].Date
		}
		return p.expenses[i].ID < p.expenses[j].ID
	})
	sort.Slice(p.customers, func(i, j int) bool {
		if p.customers[i].date != p.customers[j].date {
			return p.customers[i].date > p.customers[j].date
		}
		return p.customers[i].inv.ID < p.customers[j].inv.ID
	})
	sort.Slice(p.bookings, func(i, j int) bool {
		if p.bookings[i].Start != p.bookings[j].Start {
			return p.bookings[i].Start < p.bookings[j].Start
		}
		return p.bookings[i].Room < p.bookings[j].Room
	})
	sort.Slice(p.rentals, func(i, j int) bool {
		if p.rentals[i].Date != p.rentals[j].Date {
			return p.rentals[i].Date < p.rentals[j].Date
		}
		return p.rentals[i].InvoiceNumber+p.rentals[i].Product < p.rentals[j].InvoiceNumber+p.rentals[j].Product
	})

	write := func(rel string, build func(a Audience) interface{}) {
		writeTiersNoMirror(dataDir, year, month, rel, build)
	}
	if len(p.expenses) > 0 {
		write("expenses.json", func(a Audience) interface{} {
			list := make([]Expense, len(p.expenses))
			for i, e := range p.expenses {
				list[i] = expenseForAudience(e, a)
			}
			t, cats := expenseTotals(p.expenses)
			return ExpensesFile{GeneratedAt: now, Scope: scope, Period: period, Currency: "EUR", Totals: t, ByCategory: cats, Expenses: list}
		})
		write("vendors.json", func(a Audience) interface{} {
			rows := vendorsFrom(p.expenses, a)
			vendors := map[string]bool{}
			var un, tot, due []float64
			for _, r := range rows {
				un, tot, due = append(un, r.Untaxed), append(tot, r.Total), append(due, r.Due)
			}
			for _, e := range p.expenses {
				if e.Status != "reversed" {
					vendors[e.Vendor.ID] = true
				}
			}
			return VendorsFile{GeneratedAt: now, Scope: scope, Period: period, Currency: "EUR",
				Totals: rowTotals(len(vendors), un, tot, due), Vendors: rows}
		})
	}
	if len(p.customers) > 0 {
		write("customers.json", func(a Audience) interface{} {
			rows, inc := customersFrom(p.customers, a)
			people := map[string]bool{}
			for _, d := range p.customers {
				people[d.party.ID] = true
			}
			var un, tot, due []float64
			for _, r := range rows {
				un, tot, due = append(un, r.Untaxed), append(tot, r.Total), append(due, r.Due)
			}
			return CustomersFile{GeneratedAt: now, Scope: scope, Period: period, Currency: "EUR",
				Totals: rowTotals(len(people), un, tot, due), ByIncome: inc, Customers: rows}
		})
	}
	if len(p.bookings) > 0 || len(p.rentals) > 0 {
		write("bookings.json", func(a Audience) interface{} {
			bs := make([]BookingRow, len(p.bookings))
			for i, b := range p.bookings {
				bs[i] = bookingForAudience(b, a)
			}
			rs := make([]RentalRow, len(p.rentals))
			for i, r := range p.rentals {
				rs[i] = rentalForAudience(r, a)
			}
			return BookingsFile{GeneratedAt: now, Scope: scope, Period: period, Currency: "EUR",
				Rooms: summariseRooms(rooms, p.bookings, p.rentals), Months: perMonth, Bookings: bs, Rentals: rs}
		})
	}
}

// writeTiersNoMirror writes one artifact to every tier of a month (or a
// year, month ""), projected per tier by build, without the latest/ mirror.
func writeTiersNoMirror(dataDir, year, month, rel string, build func(a Audience) interface{}) {
	for _, a := range Audiences {
		data, err := json.MarshalIndent(build(a), "", "  ")
		if err != nil {
			continue
		}
		cleaned, err := enforceAudiencePolicy(a, rel, data)
		if err != nil {
			Warnf("  %s⚠ %s/%s %s: %v%s", Fmt.Yellow, a, rel, firstNonEmpty(year+"-"+month, year), err, Fmt.Reset)
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
