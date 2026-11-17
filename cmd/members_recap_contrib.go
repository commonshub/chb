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
// ({"<discord id>": [<odoo partner id>, …]}; CHB_INVOICE_ORGS_FILE, default
// settings/invoice-orgs.json), when it names exactly one partner. Never guessed from names. Its amount is the
// payments received in the period on all of its customer invoices.

import (
	"encoding/json"
	"math"
	"os"
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
	Partner    int // invoice partner
	Commercial int // the company it belongs to (= Partner when none)
	Date       string
	Amount     float64 // signed: credit notes negative
	Stripe     bool    // booked on a Stripe journal
}

type recapStripeCharge struct {
	Date   string
	Amount float64
}

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
		for _, pay := range inv.Payments {
			ix.payments = append(ix.payments, recapInvoicePayment{
				Partner: inv.Partner.ID, Commercial: commercial, Date: pay.Date,
				Amount: sign * math.Abs(pay.Amount),
				Stripe: strings.Contains(strings.ToLower(pay.Journal), "stripe"),
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
				ix.stripeByHash[h] = append(ix.stripeByHash[h], recapStripeCharge{
					Date:   time.Unix(tx.Timestamp, 0).In(BrusselsTZ()).Format("2006-01-02"),
					Amount: tx.Amount,
				})
			}
		}
	}
	path := strings.TrimSpace(os.Getenv("CHB_INVOICE_ORGS_FILE"))
	if path == "" {
		path = settingsFilePath("invoice-orgs.json")
	}
	if data, err := os.ReadFile(path); err == nil {
		_ = json.Unmarshal(data, &ix.invoiceOrgs)
	}
	return ix
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
}

func (ix *recapContribIndex) contribution(r recapRecipient, p recapPeriod) recapContribution {
	var c recapContribution
	start, end := recapPeriodRange(p)
	in := func(d string) bool { return d >= start && d < end }

	if r.EmailHash != "" {
		for _, s := range ix.stripeByHash[r.EmailHash] {
			if in(s.Date) {
				c.Personal += s.Amount
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

// params: contribution= always, orgName= / orgContribution= with an organisation.
func (c recapContribution) params(q map[string][]string) {
	q["contribution"] = []string{formatRecapEuros(c.Personal)}
	if c.OrgName != "" {
		q["orgName"] = []string{c.OrgName}
		q["orgContribution"] = []string{formatRecapEuros(c.Org)}
	}
}

func formatRecapEuros(v float64) string {
	return strconv.FormatFloat(roundCents(v), 'f', -1, 64)
}
