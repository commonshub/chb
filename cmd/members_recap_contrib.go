package cmd

// What a member, and the organisation they belong to, paid the hub over a
// recap period. Passed to /api/recap as contribution= (always) and
// orgName= / orgContribution= (when the member has an organisation).
//
// Personal, tax-inclusive, as received:
//   - Stripe charges (memberships, donations, tickets) whose payer email
//     hashes to the member's accounts.emailHash, minus refunds;
//   - payments received in the period on Odoo customer invoices of the
//     member's own partner (the member's odooPartnerId, or a partner whose
//     email hashes to theirs), minus credit notes. Payments booked on a
//     Stripe journal are skipped: the Stripe charge already counts them.
//     Invoices addressed to the member as a contact of a company count for
//     the company, not for the member.
//
// Organisation: the company the member's Odoo contact belongs to
// (commercial partner). Without one, the bot's invoice-orgs.json
// ({"<discord id>": [<odoo partner id>, …]}, relayed by the website at
// GET /api/members/invoice-orgs), when it names exactly one partner. Never
// guessed from names. Its amount is the
// payments received in the period on all of its customer invoices.

import (
	"encoding/json"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	odoosource "github.com/CommonsHub/chb/providers/odoo"
)

type recapContribPartner struct {
	Name       string
	IsCompany  bool
	Commercial int
}

type recapInvoicePayment struct {
	Partner     int // invoice partner
	Commercial  int // the company it belongs to (= Partner when none)
	Date        string
	Amount      float64 // signed: credit notes negative
	Stripe      bool    // booked on a Stripe journal
	Description string  // "Invoice 2026_RCC_104: Satoshi room"
}

type recapStripeCharge struct {
	Date     string
	At       string // RFC3339, Brussels
	Amount   float64
	Kind     string // charge | refund
	Category string
	Product  string
}

// recapEuroTransaction is one entry of /api/recap's euroTransactions, from
// the member's side: a payment to the hub is negative, a refund positive.
type recapEuroTransaction struct {
	At          string  `json:"at"`
	Amount      float64 `json:"amount"`
	Description string  `json:"description"`
}

const recapMaxEuroTransactions = 100

type recapContribIndex struct {
	partners       map[int]recapContribPartner
	partnersByHash map[string][]int
	payments       []recapInvoicePayment
	stripeByHash   map[string][]recapStripeCharge
	invoiceOrgs    map[string][]int // discord id → partner ids
}

// recapPeriodRange: [start, end) dates (YYYY-MM-DD) of the period.
func recapPeriodRange(p recapPeriod) (string, string) {
	var start time.Time
	var end time.Time
	switch p.Kind {
	case "monthly":
		start, _ = time.Parse("2006-01", p.ID)
		end = start.AddDate(0, 1, 0)
	case "quarterly":
		y, _ := time.Parse("2006", p.ID[:4])
		q := int(p.ID[6] - '0')
		start = y.AddDate(0, 3*(q-1), 0)
		end = start.AddDate(0, 3, 0)
	default:
		start, _ = time.Parse("2006", p.ID)
		end = start.AddDate(1, 0, 0)
	}
	return start.Format("2006-01-02"), end.Format("2006-01-02")
}

func buildRecapContribIndex(dataDir, salt string, periods []recapPeriod) *recapContribIndex {
	ix := &recapContribIndex{
		partners:       map[int]recapContribPartner{},
		partnersByHash: map[string][]int{},
		stripeByHash:   map[string][]recapStripeCharge{},
		invoiceOrgs:    map[string][]int{},
	}
	var pf OdooPartnersFile
	if data, err := os.ReadFile(odoosource.Path(dataDir, "latest", "", odoosource.PartnersFile)); err == nil {
		_ = json.Unmarshal(data, &pf)
	}
	for _, p := range pf.Partners {
		ix.partners[p.ID] = recapContribPartner{Name: p.Name, IsCompany: p.IsCompany, Commercial: p.CommercialPartnerID}
		if em := strings.TrimSpace(p.Email); em != "" && salt != "" && p.Active {
			h := hashEmail(em, salt)
			ix.partnersByHash[h] = append(ix.partnersByHash[h], p.ID)
		}
	}
	for _, inv := range loadAllCachedInvoices(dataDir) {
		if inv.State != "posted" || (inv.MoveType != "out_invoice" && inv.MoveType != "out_refund") {
			continue
		}
		commercial := inv.Partner.ID
		if inv.CommercialPartner != nil && inv.CommercialPartner.ID > 0 {
			commercial = inv.CommercialPartner.ID
		}
		sign := 1.0
		if inv.MoveType == "out_refund" {
			sign = -1
		}
		desc := recapInvoiceDescription(inv)
		for _, pay := range inv.Payments {
			ix.payments = append(ix.payments, recapInvoicePayment{
				Partner: inv.Partner.ID, Commercial: commercial, Date: pay.Date,
				Amount:      sign * math.Abs(pay.Amount),
				Stripe:      strings.Contains(strings.ToLower(pay.Journal), "stripe"),
				Description: desc,
			})
		}
	}
	if salt != "" {
		for _, ym := range recapPeriodMonths(periods) {
			f := loadTransactionsFile(dataDir, ym[:4], ym[5:], false)
			if f == nil {
				continue
			}
			for _, tx := range f.Transactions {
				if tx.Provider != "stripe" || tx.Metadata == nil {
					continue
				}
				kind, _ := tx.Metadata["kind"].(string)
				if kind != "charge" && kind != "refund" {
					continue
				}
				em, _ := tx.Metadata["email"].(string)
				if strings.TrimSpace(em) == "" {
					continue
				}
				h := hashEmail(em, salt)
				at := time.Unix(tx.Timestamp, 0).In(BrusselsTZ())
				cat, _ := tx.Metadata["category"].(string)
				prod, _ := tx.Metadata["product"].(string)
				ix.stripeByHash[h] = append(ix.stripeByHash[h], recapStripeCharge{
					Date: at.Format("2006-01-02"), At: at.Format(time.RFC3339),
					Amount: tx.Amount, Kind: kind, Category: cat, Product: strings.TrimSpace(prod),
				})
			}
		}
	}
	return ix
}

// fetchRecapInvoiceOrgs: the bot's Discord id → Odoo partner ids, relayed
// by the website. "test-" keys are the website's test user, whose ids come
// from the test copy of Odoo: never used.
func fetchRecapInvoiceOrgs(secret string) (map[string][]int, error) {
	var resp struct {
		Orgs map[string][]int `json:"orgs"`
	}
	if err := recapGet(secret, "/api/members/invoice-orgs", &resp); err != nil {
		return nil, err
	}
	out := map[string][]int{}
	for k, v := range resp.Orgs {
		if strings.HasPrefix(k, "test-") || !discordIDPattern.MatchString(k) {
			continue
		}
		out[k] = v
	}
	return out, nil
}

// recapInvoiceDescription: "Invoice <number>: <what>", "Credit note <number>".
func recapInvoiceDescription(inv OdooOutgoingInvoice) string {
	label := "Invoice"
	if inv.MoveType == "out_refund" {
		label = "Credit note"
	}
	if inv.Number != "" {
		label += " " + inv.Number
	}
	what := strings.TrimSpace(inv.Title)
	if what == "" {
		for _, l := range inv.LineItems {
			if l.DisplayType == "" || l.DisplayType == "product" {
				if what = strings.TrimSpace(firstNonEmpty(l.Title, l.ProductName)); what != "" {
					break
				}
			}
		}
	}
	if i := strings.IndexByte(what, '\n'); i >= 0 {
		what = strings.TrimSpace(what[:i])
	}
	if what != "" {
		label += ": " + what
	}
	return label
}

// recapStripeDescription: "Membership (monthly)", "Donation to …", "Refund".
func recapStripeDescription(c recapStripeCharge, interval string) string {
	if c.Kind == "refund" || c.Amount < 0 {
		return "Refund"
	}
	switch c.Category {
	case "membership":
		switch interval {
		case "month":
			return "Membership (monthly)"
		case "year":
			return "Membership (yearly)"
		}
		return "Membership"
	case "donation":
		if c.Product != "" {
			return c.Product
		}
		return "Donation"
	}
	if c.Product != "" {
		return c.Product
	}
	if c.Category != "" {
		return strings.ToUpper(c.Category[:1]) + strings.ReplaceAll(c.Category[1:], "_", " ")
	}
	return "Payment"
}

// recapDateAt: a YYYY-MM-DD payment date as noon Brussels, RFC3339.
func recapDateAt(d string) string {
	t, err := time.ParseInLocation("2006-01-02", d, BrusselsTZ())
	if err != nil {
		return d
	}
	return t.Add(12 * time.Hour).Format(time.RFC3339)
}

// recapPeriodMonths: every YYYY-MM covered by the periods.
func recapPeriodMonths(periods []recapPeriod) []string {
	seen := map[string]bool{}
	var out []string
	for _, p := range periods {
		start, end := recapPeriodRange(p)
		t, _ := time.Parse("2006-01-02", start)
		for t.Format("2006-01-02") < end {
			if ym := t.Format("2006-01"); !seen[ym] {
				seen[ym] = true
				out = append(out, ym)
			}
			t = t.AddDate(0, 1, 0)
		}
	}
	return out
}

type recapContribution struct {
	Personal float64
	OrgName  string // empty: no organisation
	Org      float64
	// Euro: the member's own payments and refunds, oldest first (at most
	// recapMaxEuroTransactions, the latest kept).
	Euro []recapEuroTransaction
}

func (ix *recapContribIndex) contribution(r recapRecipient, p recapPeriod) recapContribution {
	var c recapContribution
	start, end := recapPeriodRange(p)
	in := func(d string) bool { return d >= start && d < end }

	if r.EmailHash != "" {
		for _, s := range ix.stripeByHash[r.EmailHash] {
			if in(s.Date) {
				c.Personal += s.Amount
				c.Euro = append(c.Euro, recapEuroTransaction{At: s.At, Amount: roundCents(-s.Amount), Description: recapStripeDescription(s, r.Interval)})
			}
		}
	}
	own := map[int]bool{}
	if r.OdooPartnerID > 0 {
		own[r.OdooPartnerID] = true
	}
	if r.EmailHash != "" {
		for _, id := range ix.partnersByHash[r.EmailHash] {
			own[id] = true
		}
	}
	orgs := map[int]bool{}
	for id := range own {
		if cp := ix.partners[id].Commercial; cp > 0 && cp != id && ix.partners[cp].IsCompany {
			orgs[cp] = true
		}
	}
	for _, pay := range ix.payments {
		if own[pay.Partner] && (pay.Commercial == pay.Partner || pay.Commercial == 0) && !pay.Stripe && in(pay.Date) {
			c.Personal += pay.Amount
			c.Euro = append(c.Euro, recapEuroTransaction{At: recapDateAt(pay.Date), Amount: roundCents(-pay.Amount), Description: pay.Description})
		}
	}
	org := 0
	if len(orgs) == 1 {
		for id := range orgs {
			org = id
		}
	} else if len(orgs) == 0 && r.DiscordID != "" {
		if ids := ix.invoiceOrgs[r.DiscordID]; len(ids) == 1 {
			org = ids[0]
		}
	}
	if org > 0 && ix.partners[org].Name != "" && !own[org] {
		c.OrgName = ix.partners[org].Name
		for _, pay := range ix.payments {
			if pay.Commercial == org && in(pay.Date) {
				c.Org += pay.Amount
			}
		}
	}
	sort.SliceStable(c.Euro, func(i, j int) bool { return c.Euro[i].At < c.Euro[j].At })
	if len(c.Euro) > recapMaxEuroTransactions {
		c.Euro = c.Euro[len(c.Euro)-recapMaxEuroTransactions:]
	}
	for i := range c.Euro {
		if c.Euro[i].Amount == 0 {
			c.Euro[i].Amount = 0 // no -0
		}
	}
	c.Personal = roundCents(c.Personal)
	c.Org = roundCents(c.Org)
	if c.Personal == 0 {
		c.Personal = 0 // no -0
	}
	if c.Org == 0 {
		c.Org = 0
	}
	return c
}

func formatRecapEuros(v float64) string {
	return strconv.FormatFloat(roundCents(v), 'f', -1, 64)
}
