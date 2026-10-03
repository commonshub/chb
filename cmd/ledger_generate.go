package cmd

// Chart of accounts and ledger balances, per audience tier:
//
//	latest/<tier>/accounts-chart.json   every account: code, labels, class, group, type
//	YYYY/<tier>/ledger-balances.json    trial balance at account level for calendar year YYYY
//
// Privacy below stewards:
//   - an account whose label names a natural person (a current account
//     "C/C <name>", fees "RETRIBUTIONS <name>") gets a neutral label in
//     public, and its balances are merged into one row per group (two-digit
//     PCMN group); members see the real label;
//   - payroll (62…) is published as the single 62 total in public and
//     members; stewards see every account.
// settings.json `accounting.privateAccounts` / `publicAccounts` (codes)
// override the detection.

import (
	"encoding/json"

	odoosource "github.com/CommonsHub/chb/providers/odoo"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	accountsChartFile  = "accounts-chart.json"
	ledgerBalancesFile = "ledger-balances.json"
)

type ChartEntry struct {
	Code       string            `json:"code"`
	Label      string            `json:"label"`
	Labels     map[string]string `json:"labels,omitempty"`
	Class      string            `json:"class"`               // PCMN class 1–7
	Group      string            `json:"group"`               // two-digit group, e.g. "61"
	OdooGroup  string            `json:"odooGroup,omitempty"` // Odoo's group prefix, e.g. "6130"
	GroupName  string            `json:"groupName,omitempty"`
	Type       string            `json:"type"`
	Reconcile  bool              `json:"reconcile"`
	Deprecated bool              `json:"deprecated"`
	Used       bool              `json:"used"`                 // appears in a published ledger
	Individual bool              `json:"individual,omitempty"` // label names a person (label neutral in public)
}

type AccountsChartFile struct {
	GeneratedAt string       `json:"generatedAt"`
	Languages   []string     `json:"languages"`
	Accounts    []ChartEntry `json:"accounts"`
}

type LedgerOut struct {
	Code    string `json:"code"`
	Label   string `json:"label"`
	Class   string `json:"class"`
	Group   string `json:"group"`
	Merged  int    `json:"merged,omitempty"` // accounts aggregated into this row
	Opening euros  `json:"opening"`
	Debit   euros  `json:"debit"`
	Credit  euros  `json:"credit"`
	Closing euros  `json:"closing"`
}

type LedgerBalancesFile struct {
	GeneratedAt string      `json:"generatedAt"`
	Year        string      `json:"year"`
	PeriodStart string      `json:"periodStart"`
	PeriodEnd   string      `json:"periodEnd"`
	FiscalStart string      `json:"fiscalStart"`
	Currency    string      `json:"currency"`
	Totals      LedgerOut   `json:"totals"` // debit = credit when the books balance
	Accounts    []LedgerOut `json:"accounts"`
}

var personAccountPrefix = regexp.MustCompile(`(?i)^(c/c|compte courant|current account|retributions?|r[ée]mun[ée]ration)\s+(.+)$`)

// roleWords are what follows "C/C" or "RETRIBUTIONS" when the account is
// about a group, a tax or the capital, not a person.
var roleWords = regexp.MustCompile(`(?i)\b(g[ée]rants|tva|vat|directors|directeurs|administrateurs|bestuurders|staff|personnel|employ[ée]s|ouvriers|associ[ée]s|partners|[ée]quitable|sabam|à r[ée]cup[ée]rer|a recuperer|payable|receivable|current account|capital|du capital)\b`)

// singleRoleWords: a role one person holds ("C/C director"). The balance is
// that person's, so the account is treated like one named after them.
var singleRoleWords = regexp.MustCompile(`(?i)\b(g[ée]rant|director|directeur|directrice|administrat(eur|rice)|pr[ée]sident(e)?|tr[ée]sori(er|[eè]re)|secr[ée]taire|zaakvoerder|bestuurder|voorzitter|penningmeester|secretaris|ceo|founder|fondat(eur|rice))\b`)

type accountPrivacy struct {
	private, public map[string]bool
	orgNames        []string // normalised names of company partners in Odoo
}

var nonAlnumSpace = regexp.MustCompile(`[^a-z0-9]+`)

func normName(s string) string {
	return strings.TrimSpace(nonAlnumSpace.ReplaceAllString(strings.ToLower(s), " "))
}

// loadOrgPartnerNames reads the company partners from the Odoo partner
// snapshot (latest/providers/odoo/<db>/partners.json).
func loadOrgPartnerNames() []string {
	data, err := os.ReadFile(odoosource.Path(DataDir(), "latest", "", odoosource.PartnersFile))
	if err != nil {
		return nil
	}
	var f struct {
		Partners []struct {
			Name      string `json:"name"`
			IsCompany bool   `json:"isCompany"`
		} `json:"partners"`
	}
	if json.Unmarshal(data, &f) != nil {
		return nil
	}
	var out []string
	for _, p := range f.Partners {
		if p.IsCompany || nameHasLegalForm(p.Name) {
			if n := normName(p.Name); n != "" {
				out = append(out, n)
			}
		}
	}
	return out
}

func loadAccountPrivacy() accountPrivacy {
	p := accountPrivacy{private: map[string]bool{}, public: map[string]bool{}, orgNames: loadOrgPartnerNames()}
	data, err := os.ReadFile(settingsFilePath("settings.json"))
	if err != nil {
		return p
	}
	var s struct {
		Accounting struct {
			PrivateAccounts []string `json:"privateAccounts"`
			PublicAccounts  []string `json:"publicAccounts"`
		} `json:"accounting"`
	}
	if json.Unmarshal(data, &s) == nil {
		for _, c := range s.Accounting.PrivateAccounts {
			p.private[c] = true
		}
		for _, c := range s.Accounting.PublicAccounts {
			p.public[c] = true
		}
	}
	return p
}

// isIndividualAccount: the label names a natural person.
func (p accountPrivacy) isIndividualAccount(code, name string) bool {
	if p.private[code] {
		return true
	}
	if p.public[code] {
		return false
	}
	m := personAccountPrefix.FindStringSubmatch(strings.TrimSpace(name))
	if m == nil {
		return false
	}
	rest := strings.TrimSpace(m[2])
	if singleRoleWords.MatchString(rest) && !roleWords.MatchString(rest) {
		return true
	}
	if rest == "" || roleWords.MatchString(rest) || nameHasLegalForm(rest) {
		return false
	}
	// "C/C ALL FOR CLIMATE": the current account of a company partner
	// ("All for Climate DAO") is not a person's. Anything unknown stays
	// private: a missed organisation only loses its name in public.
	nr := normName(rest)
	for _, org := range p.orgNames {
		if org == nr || strings.HasPrefix(org, nr+" ") {
			return false
		}
	}
	return true
}

func isPayrollCode(code string) bool { return strings.HasPrefix(code, "62") }

func groupOf(code string) string {
	if len(code) >= 2 {
		return code[:2]
	}
	return code
}

func classOf(code string) string {
	if code == "" {
		return ""
	}
	return code[:1]
}

func loadChartRaw(dataDir string) *ChartFile {
	data, err := os.ReadFile(chartRawPath(dataDir))
	if err != nil {
		return nil
	}
	var c ChartFile
	if json.Unmarshal(data, &c) != nil {
		return nil
	}
	return &c
}

func loadLedgerRaw(dataDir, year string) *LedgerFile {
	data, err := os.ReadFile(ledgerRawPath(dataDir, year))
	if err != nil {
		return nil
	}
	var l LedgerFile
	if json.Unmarshal(data, &l) != nil {
		return nil
	}
	return &l
}

const neutralIndividualLabel = "Account of an individual"

func neutralGroupLabel(group string) string {
	switch {
	case strings.HasPrefix(group, "4"):
		return "Current accounts of individuals"
	case strings.HasPrefix(group, "6"):
		return "Charges: individuals"
	case strings.HasPrefix(group, "7"):
		return "Income: individuals"
	}
	return "Accounts of individuals"
}

// generateChartAndLedger writes the chart (latest/) and every year's ledger.
// Returns (accounts in the chart, years written).
func generateChartAndLedger(dataDir string) (int, int) {
	chart := loadChartRaw(dataDir)
	if chart == nil {
		return 0, 0
	}
	now := time.Now().UTC().Format(time.RFC3339)
	priv := loadAccountPrivacy()
	used := map[string]bool{}
	years := []string{}
	ledgers, _ := filepath.Glob(filepath.Join(dataDir, "[0-9][0-9][0-9][0-9]", "12", "providers", "odoo", "*", ledgerBalancesFile))
	for _, p := range ledgers {
		rel, _ := filepath.Rel(dataDir, p)
		y := strings.Split(filepath.ToSlash(rel), "/")[0]
		if l := loadLedgerRaw(dataDir, y); l != nil {
			years = append(years, y)
			for _, r := range l.Accounts {
				used[r.Code] = true
			}
		}
	}
	sort.Strings(years)

	writeTiersNoMirror(dataDir, "latest", "", accountsChartFile, func(a Audience) interface{} {
		out := AccountsChartFile{GeneratedAt: now, Languages: chart.Languages}
		for _, c := range chart.Accounts {
			ind := priv.isIndividualAccount(c.Code, c.Name)
			e := ChartEntry{Code: c.Code, Label: c.Name, Labels: c.Labels, Class: classOf(c.Code), Group: groupOf(c.Code),
				OdooGroup: c.GroupCode, GroupName: c.GroupName, Type: c.Type, Reconcile: c.Reconcile,
				Deprecated: !c.Active, Used: used[c.Code], Individual: ind}
			if ind && a == AudiencePublic {
				e.Label, e.Labels = neutralIndividualLabel, nil
			}
			if a != AudienceStewards {
				// Bank accounts are often named after their IBAN.
				e.Label = maskBankDetails(e.Label)
				if e.Labels != nil {
					masked := map[string]string{}
					for k, v := range e.Labels {
						masked[k] = maskBankDetails(v)
					}
					e.Labels = masked
				}
			}
			out.Accounts = append(out.Accounts, e)
		}
		if out.Accounts == nil {
			out.Accounts = []ChartEntry{}
		}
		return out
	})

	written := 0
	for _, y := range years {
		l := loadLedgerRaw(dataDir, y)
		writeTiersNoMirror(dataDir, y, "", ledgerBalancesFile, func(a Audience) interface{} {
			return ledgerForAudience(l, a, priv, now)
		})
		written++
	}
	return len(chart.Accounts), written
}

func ledgerForAudience(l *LedgerFile, a Audience, priv accountPrivacy, now string) LedgerBalancesFile {
	out := LedgerBalancesFile{GeneratedAt: now, Year: l.Year, PeriodStart: l.PeriodStart, PeriodEnd: l.PeriodEnd,
		FiscalStart: l.FiscalStart, Currency: "EUR", Accounts: []LedgerOut{}}
	merged := map[string]*LedgerOut{}
	var order []string
	add := func(key string, row LedgerOut, merge bool) {
		if !merge {
			out.Accounts = append(out.Accounts, row)
			return
		}
		m := merged[key]
		if m == nil {
			// A fresh row: code and label of the group, amounts from zero.
			m = &LedgerOut{Code: row.Code, Label: row.Label, Class: row.Class, Group: row.Group}
			merged[key] = m
			order = append(order, key)
		}
		m.Merged++
		m.Opening += row.Opening
		m.Debit += row.Debit
		m.Credit += row.Credit
		m.Closing += row.Closing
	}
	for _, r := range l.Accounts {
		label := r.Name
		if a != AudienceStewards {
			label = maskBankDetails(label) // bank accounts are often named after their IBAN
		}
		row := LedgerOut{Code: r.Code, Label: label, Class: classOf(r.Code), Group: groupOf(r.Code),
			Opening: eur(r.Opening), Debit: eur(r.Debit), Credit: eur(r.Credit), Closing: eur(r.Closing)}
		out.Totals.Opening += row.Opening
		out.Totals.Debit += row.Debit
		out.Totals.Credit += row.Credit
		out.Totals.Closing += row.Closing
		switch {
		case a == AudienceStewards:
			add("", row, false)
		case isPayrollCode(r.Code):
			row.Code, row.Label, row.Group = "62", "Remuneration, social security and pensions", "62"
			add("payroll", row, true)
		case a == AudiencePublic && priv.isIndividualAccount(r.Code, r.Name):
			g := groupOf(r.Code)
			row.Code, row.Label = g, neutralGroupLabel(g)
			add("individual:"+g, row, true)
		default:
			add("", row, false)
		}
	}
	for _, k := range order {
		out.Accounts = append(out.Accounts, *merged[k])
	}
	sort.SliceStable(out.Accounts, func(i, j int) bool { return out.Accounts[i].Code < out.Accounts[j].Code })
	out.Totals.Label = "Total"
	return out
}
