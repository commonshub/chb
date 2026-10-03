package cmd

// Annual accounts → annual-accounts.json, per audience tier:
//
//	YYYY/<tier>/annual-accounts.json   the fiscal year(s) ending in YYYY
//	latest/<tier>/annual-accounts.json every fiscal year
//	YYYY/public/annual-accounts/<file> the filed abbreviated statements
//
// public and members: filed fiscal years only, and only the abbreviated
// balance sheet and profit and loss (class-level aggregates by NBB code, no
// personal data). stewards: every fiscal year, draft or filed, and every
// document (trial balance, internal balance sheet) with its archive path.
// Consistency checks are computed here and published with the figures.

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	aa "github.com/CommonsHub/chb/providers/annualaccounts"
)

const annualAccountsFile = "annual-accounts.json"

type AnnualKeyFigures struct {
	TotalAssets         euros `json:"totalAssets"`         // 20/58
	FixedAssets         euros `json:"fixedAssets"`         // 21/28
	CurrentAssets       euros `json:"currentAssets"`       // 29/58
	Receivables         euros `json:"receivables"`         // 40/41
	Cash                euros `json:"cash"`                // 54/58
	Equity              euros `json:"equity"`              // 10/15
	AccumulatedResult   euros `json:"accumulatedResult"`   // 14
	AmountsPayable      euros `json:"amountsPayable"`      // 17/49
	Turnover            euros `json:"turnover"`            // 70
	GiftsAndSubsidies   euros `json:"giftsAndSubsidies"`   // 73
	GoodsAndServices    euros `json:"goodsAndServices"`    // 60/61
	Remuneration        euros `json:"remuneration"`        // 62
	GrossMargin         euros `json:"grossMargin"`         // 9900
	OperatingResult     euros `json:"operatingResult"`     // 9901
	ResultOfThePeriod   euros `json:"resultOfThePeriod"`   // 9904
	BroughtForward      euros `json:"broughtForward"`      // 14P
	CarriedForward      euros `json:"carriedForward"`      // (14)
	OtherAppropriations euros `json:"otherAppropriations"` // Odoo's balancing line under 14
}

type AnnualCheck struct {
	Level   string `json:"level"` // error | warning | info
	Code    string `json:"code"`
	Message string `json:"message"`
	Amount  *euros `json:"amount,omitempty"`
}

type AnnualDocumentOut struct {
	Kind   string `json:"kind"`
	File   string `json:"file"`
	Path   string `json:"path"` // relative to the data root
	SHA256 string `json:"sha256"`
	Bytes  int64  `json:"bytes"`
}

type AnnualPeriod struct {
	Start   string `json:"start"`
	End     string `json:"end"`
	Months  int    `json:"months"`
	Assumed bool   `json:"startAssumed,omitempty"`
}

type AnnualFiscalYear struct {
	Label      string              `json:"label"`
	Year       string              `json:"year"` // year the period ends: where the files live
	Period     AnnualPeriod        `json:"period"`
	Status     string              `json:"status"` // filed | draft (draft: stewards only)
	FiledAt    string              `json:"filedAt,omitempty"`
	NBB        *AnnualNBB          `json:"nbb,omitempty"`
	Schema     string              `json:"schema"`
	Currency   string              `json:"currency"`
	KeyFigures AnnualKeyFigures    `json:"keyFigures"`
	Figures    map[string]euros    `json:"figures"` // every NBB code read, as printed ("20/58", "9904", "(14)", "14P")
	Labels     map[string]string   `json:"labels"`
	Documents  []AnnualDocumentOut `json:"documents"`
	Checks     []AnnualCheck       `json:"checks"`
}

type AnnualNBB struct {
	Reference string `json:"reference,omitempty"`
	URL       string `json:"url,omitempty"`
}

type AnnualEntity struct {
	Name             string `json:"name"`
	EnterpriseNumber string `json:"enterpriseNumber"`
	FormerName       string `json:"formerName,omitempty"`
}

// AnnualAccountsFile is YYYY/<tier>/annual-accounts.json and the latest/ index.
type AnnualAccountsFile struct {
	GeneratedAt string             `json:"generatedAt"`
	Scope       string             `json:"scope"`          // year | all
	Year        string             `json:"year,omitempty"` // year files
	Entity      AnnualEntity       `json:"entity"`
	FiscalYears []AnnualFiscalYear `json:"fiscalYears"`
}

// annualFigures reads the figures of a filing: a figures file wins,
// otherwise the codes printed in the abbreviated statements.
func annualFigures(dataDir string, f *AnnualFiling) (map[string]float64, map[string]string) {
	figs, labels := map[string]float64{}, map[string]string{}
	dir := annualFilingDir(dataDir, f.PeriodEnd)
	for _, d := range f.Documents {
		if d.Kind != "figures" || !strings.EqualFold(filepath.Ext(d.File), ".csv") {
			continue
		}
		if list, err := aa.ReadFiguresCSV(filepath.Join(dir, d.File)); err == nil {
			for _, fg := range list {
				figs[fg.Code] = fg.Amount
			}
		}
	}
	for _, d := range f.Documents {
		if !aa.PublicKind(d.Kind) {
			continue
		}
		lines, err := aa.ExtractLines(filepath.Join(dir, d.File))
		if err != nil {
			continue
		}
		for _, fg := range aa.ParseFigures(lines) {
			if _, set := figs[fg.Code]; !set {
				figs[fg.Code] = fg.Amount
			}
			if fg.Label != "" {
				labels[fg.Code] = fg.Label
			}
		}
	}
	return figs, labels
}

func eur(v float64) euros { return euros(math.Round(v * 100)) }

func monthsBetween(start, end string) int {
	s, err1 := time.Parse("2006-01-02", start)
	e, err2 := time.Parse("2006-01-02", end)
	if err1 != nil || err2 != nil {
		return 0
	}
	return (e.Year()-s.Year())*12 + int(e.Month()-s.Month()) + 1
}

// buildAnnualFiscalYear assembles one fiscal year, with its checks
// (the previous filing, if any, for the opening-balance check).
func buildAnnualFiscalYear(dataDir string, f *AnnualFiling, all []*AnnualFiling) AnnualFiscalYear {
	figs, labels := annualFigures(dataDir, f)
	g := func(code string) float64 { return figs[code] }
	has := func(code string) bool { _, ok := figs[code]; return ok }
	fy := AnnualFiscalYear{
		Label: f.Label, Year: f.PeriodEnd[:4],
		Period: AnnualPeriod{Start: f.PeriodStart, End: f.PeriodEnd, Months: monthsBetween(f.PeriodStart, f.PeriodEnd), Assumed: f.PeriodAssumed},
		Status: f.Status, FiledAt: f.FiledAt, Schema: firstNonEmpty(f.Schema, "abbreviated-association"), Currency: "EUR",
		Figures: map[string]euros{}, Labels: labels,
	}
	if f.NBBReference != "" || f.NBBURL != "" {
		fy.NBB = &AnnualNBB{Reference: f.NBBReference, URL: f.NBBURL}
	}
	for k, v := range figs {
		fy.Figures[k] = eur(v)
	}
	fy.KeyFigures = AnnualKeyFigures{
		TotalAssets: eur(g("20/58")), FixedAssets: eur(g("21/28")), CurrentAssets: eur(g("29/58")),
		Receivables: eur(g("40/41")), Cash: eur(g("54/58")), Equity: eur(g("10/15")),
		AccumulatedResult: eur(g("14")), AmountsPayable: eur(g("17/49")), Turnover: eur(g("70")),
		GiftsAndSubsidies: eur(g("73")), GoodsAndServices: eur(g("60/61")), Remuneration: eur(g("62")),
		GrossMargin: eur(g("9900")), OperatingResult: eur(g("9901")), ResultOfThePeriod: eur(g("9904")),
		BroughtForward: eur(g("14P")), CarriedForward: eur(g("(14)")), OtherAppropriations: eur(g("14.otherAppropriations")),
	}

	add := func(level, code, msg string, amount *float64) {
		c := AnnualCheck{Level: level, Code: code, Message: msg}
		if amount != nil {
			e := eur(*amount)
			c.Amount = &e
		}
		fy.Checks = append(fy.Checks, c)
	}
	differs := func(a, b float64) bool { return math.Abs(a-b) > 0.005 }
	amt := func(v float64) *float64 { return &v }

	if len(figs) == 0 {
		add("error", "no-figures", "no figures could be read: import the abbreviated statements (PDF) or a figures.csv", nil)
	}
	if f.PeriodAssumed {
		add("warning", "period-assumed", "the period start is assumed to be 1 January; set it with --period", nil)
	}
	if has("20/58") && has("10/49") && differs(g("20/58"), g("10/49")) {
		add("error", "unbalanced", fmt.Sprintf("total assets (20/58) %.2f ≠ total liabilities (10/49) %.2f", g("20/58"), g("10/49")), amt(g("20/58")-g("10/49")))
	}
	if has("10/15") && has("17/49") && has("10/49") && differs(g("10/15")+g("16")+g("17/49"), g("10/49")) {
		add("error", "liabilities-sum", "equity (10/15) + provisions (16) + amounts payable (17/49) ≠ total liabilities (10/49)", amt(g("10/15")+g("16")+g("17/49")-g("10/49")))
	}
	if has("14.profitOfTheYear") && has("14") {
		sum := g("14.profitOfTheYear") + g("14.otherAppropriations") + g("14.previousYears")
		if differs(sum, g("14")) {
			add("error", "accumulated-result-sum", "the lines under accumulated profits (14) do not add up", amt(sum-g("14")))
		}
	}
	if v := g("14.otherAppropriations"); v != 0 {
		add("warning", "balancing-appropriation", fmt.Sprintf("the balance sheet carries an \"other appropriations of the year\" of %.2f under accumulated profits (14): a balancing entry, not an appropriation decided by the general assembly", v), amt(v))
	}
	if has("(14)") && has("14") && differs(g("(14)"), g("14")) {
		add("warning", "carried-forward-mismatch", fmt.Sprintf("the appropriation account carries forward %.2f ((14)) but the balance sheet shows %.2f under accumulated profits (14)", g("(14)"), g("14")), amt(g("14")-g("(14)")))
	}
	if has("9900") {
		listed := g("70") + g("71") + g("72") + g("73") + g("74") + g("76A") - g("60/61")
		if differs(listed, g("9900")) {
			add("warning", "gross-margin-unexplained", fmt.Sprintf("gross margin (9900) %.2f, but its listed components (70, 71, 72, 73, 74, 76A minus 60/61) give %.2f: %.2f is not itemised (often other operating income, 74)", g("9900"), listed, g("9900")-listed), amt(g("9900")-listed))
		}
	}
	if has("9901") && has("9900") {
		calc := g("9900") - g("62") - g("630") - g("631/4") - g("635/9") - g("640/8") + g("649") - g("66A")
		if differs(calc, g("9901")) {
			add("error", "operating-result", "operating result (9901) does not follow from the gross margin and operating charges", amt(g("9901")-calc))
		}
	}
	if has("9903") && has("9901") {
		if calc := g("9901") + g("75/76B") - g("65/66B"); differs(calc, g("9903")) {
			add("error", "result-before-taxes", "result before taxes (9903) ≠ 9901 + 75/76B − 65/66B", amt(g("9903")-calc))
		}
	}
	if has("9904") && has("9903") {
		if calc := g("9903") + g("780") - g("680") - g("67/77"); differs(calc, g("9904")) {
			add("error", "result-of-period", "result of the period (9904) ≠ 9903 + 780 − 680 − 67/77", amt(g("9904")-calc))
		}
	}
	for _, code := range []string{"17", "42/48", "43", "44", "45", "46", "48", "492/3", "499"} {
		if v := g(code); v < -0.005 {
			add("warning", "negative-liability", fmt.Sprintf("%s (%s) has a debit balance of %.2f: a liability should not be negative", firstNonEmpty(labels[code], "liability"), code, v), amt(v))
		}
	}
	// Opening balance: last year's carried-forward result must be this
	// year's brought-forward result.
	var prev *AnnualFiling
	for _, o := range all {
		if o.PeriodEnd < f.PeriodStart && (prev == nil || o.PeriodEnd > prev.PeriodEnd) {
			prev = o
		}
	}
	if prev == nil {
		add("info", "no-previous-year", "the previous fiscal year is not imported: the opening balance is not checked", nil)
	} else {
		pf, _ := annualFigures(dataDir, prev)
		closing, ok := pf["(14)"]
		if !ok {
			closing = pf["14"]
		}
		if has("14P") && differs(closing, g("14P")) {
			add("warning", "opening-balance-mismatch", fmt.Sprintf("fiscal year %s closed with %.2f carried forward, but %s brings forward %.2f (14P)", prev.Label, closing, f.Label, g("14P")), amt(g("14P")-closing))
		}
	}
	if fy.Checks == nil {
		fy.Checks = []AnnualCheck{}
	}
	return fy
}

// annualDocumentsFor lists the documents an audience may see, with their
// path: the public copy for the abbreviated statements, the archive for
// the rest (stewards only).
func annualDocumentsFor(f *AnnualFiling, a Audience) []AnnualDocumentOut {
	out := []AnnualDocumentOut{}
	for _, d := range f.Documents {
		public := aa.PublicKind(d.Kind) && f.Status == "filed"
		if a != AudienceStewards && !public {
			continue
		}
		path := filepath.ToSlash(filepath.Join(f.PeriodEnd[:4], f.PeriodEnd[5:7], aa.RelPath(f.PeriodEnd, d.File)))
		if public {
			path = filepath.ToSlash(filepath.Join(f.PeriodEnd[:4], AudiencePublic.Dir(), "annual-accounts", d.File))
		}
		out = append(out, AnnualDocumentOut{Kind: d.Kind, File: d.File, Path: path, SHA256: d.SHA256, Bytes: d.Bytes})
	}
	return out
}

// generateAnnualAccounts writes every tier's files. Returns the number of
// fiscal years.
func generateAnnualAccounts(dataDir string) (int, error) {
	filings := loadAnnualFilings(dataDir)
	if len(filings) == 0 {
		return 0, nil
	}
	now := time.Now().UTC().Format(time.RFC3339)
	entity := AnnualEntity{Name: "Commons Hub Brussels ASBL", EnterpriseNumber: "0804.505.132", FormerName: "Citizen Spring ASBL"}
	for i := len(filings) - 1; i >= 0; i-- {
		if filings[i].EntityName != "" {
			entity.Name = filings[i].EntityName
			entity.EnterpriseNumber = firstNonEmpty(filings[i].EnterpriseNumber, entity.EnterpriseNumber)
			break
		}
	}
	built := make([]AnnualFiscalYear, len(filings))
	for i, f := range filings {
		built[i] = buildAnnualFiscalYear(dataDir, f, filings)
	}

	// Public copies of the filed statements; stale ones removed.
	keep := map[string]bool{}
	for _, f := range filings {
		for _, d := range annualDocumentsFor(f, AudiencePublic) {
			src := filepath.Join(annualFilingDir(dataDir, f.PeriodEnd), d.File)
			dst := filepath.Join(dataDir, filepath.FromSlash(d.Path))
			keep[dst] = true
			if !fileExists(dst) {
				if err := copyPublicAsset(src, dst); err != nil {
					return 0, err
				}
			}
		}
	}
	pubDirs, _ := filepath.Glob(filepath.Join(dataDir, "[0-9][0-9][0-9][0-9]", AudiencePublic.Dir(), "annual-accounts"))
	for _, dir := range pubDirs {
		entries, _ := os.ReadDir(dir)
		for _, e := range entries {
			if p := filepath.Join(dir, e.Name()); !keep[p] {
				os.Remove(p)
			}
		}
	}

	byYear := map[string][]int{}
	for i, f := range filings {
		byYear[f.PeriodEnd[:4]] = append(byYear[f.PeriodEnd[:4]], i)
	}
	project := func(idx []int, a Audience) []AnnualFiscalYear {
		out := []AnnualFiscalYear{}
		for _, i := range idx {
			if a != AudienceStewards && filings[i].Status != "filed" {
				continue
			}
			fy := built[i]
			fy.Documents = annualDocumentsFor(filings[i], a)
			out = append(out, fy)
		}
		return out
	}
	years := make([]string, 0, len(byYear))
	for y := range byYear {
		years = append(years, y)
	}
	sort.Strings(years)
	for _, y := range years {
		idx := byYear[y]
		writeTiersNoMirror(dataDir, y, "", annualAccountsFile, func(a Audience) interface{} {
			return AnnualAccountsFile{GeneratedAt: now, Scope: "year", Year: y, Entity: entity, FiscalYears: project(idx, a)}
		})
	}
	all := make([]int, len(filings))
	for i := range filings {
		all[i] = i
	}
	writeTiersNoMirror(dataDir, "latest", "", annualAccountsFile, func(a Audience) interface{} {
		return AnnualAccountsFile{GeneratedAt: now, Scope: "all", Entity: entity, FiscalYears: project(all, a)}
	})
	return len(filings), nil
}
