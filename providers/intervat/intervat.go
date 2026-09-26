// Package intervat reads the periodic VAT declarations that Belgium's
// Intervat service (SPF Finances / FOD Financiën, via MyMinfin) exports as
// XML — one file per filed declaration, namespace
// http://www.minfin.fgov.be/VATConsignment.
//
// There is no API: the operator downloads the XML files and runs
// `chb vat import`, which archives them unchanged under the period's month
// (YYYY/MM/providers/intervat/<statementId>.xml, MM = last month of the
// quarter). `chb generate` turns them into the public vat.json.
package intervat

import (
	"encoding/xml"
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// Source is the provider directory name under providers/.
const Source = "intervat"

// Namespace is the XML namespace of a VAT declaration.
const Namespace = "http://www.minfin.fgov.be/VATConsignment"

type SourceProvider struct{}

func (SourceProvider) Name() string { return Source }

// RelPath returns the path inside a month for this provider.
func RelPath(elems ...string) string {
	return filepath.Join(append([]string{"providers", Source}, elems...)...)
}

// Declaration is one filed VAT return, as exported by Intervat.
type Declaration struct {
	XMLName             xml.Name  `xml:"VATDeclarationType"`
	SequenceNumber      string    `xml:"SequenceNumber,attr"`
	DeclarantReference  string    `xml:"DeclarantReference,attr"`
	ReplacedDeclaration string    `xml:"ReplacedVATDeclaration"`
	Declarant           Declarant `xml:"Declarant"`
	Period              struct {
		Month   int `xml:"Month"`
		Quarter int `xml:"Quarter"`
		Year    int `xml:"Year"`
	} `xml:"Period"`
	Amounts []struct {
		Grid  string `xml:"GridNumber,attr"`
		Value string `xml:",chardata"`
	} `xml:"Data>Amount"`
	ClientListingNihil string `xml:"ClientListingNihil"`
	Ask                struct {
		Restitution string `xml:"Restitution,attr"`
		Payment     string `xml:"Payment,attr"`
	} `xml:"Ask"`
}

// Declarant is the filer block. Email and phone are whoever filed it —
// often the accountant's own contact details — and never leave the raw
// archive.
type Declarant struct {
	VATNumber   string `xml:"VATNumber"`
	Name        string `xml:"Name"`
	Street      string `xml:"Street"`
	PostCode    string `xml:"PostCode"`
	City        string `xml:"City"`
	CountryCode string `xml:"CountryCode"`
	Email       string `xml:"EmailAddress"`
	Phone       string `xml:"Phone"`
}

// Parse decodes one declaration and checks it is one.
func Parse(data []byte) (*Declaration, error) {
	var d Declaration
	if err := xml.Unmarshal(data, &d); err != nil {
		return nil, fmt.Errorf("not a VAT declaration: %w", err)
	}
	if d.XMLName.Local != "VATDeclarationType" || d.XMLName.Space != Namespace {
		return nil, fmt.Errorf("not a VAT declaration (root %s)", d.XMLName.Local)
	}
	if d.Period.Year == 0 || (d.Period.Quarter == 0 && d.Period.Month == 0) {
		return nil, fmt.Errorf("VAT declaration has no period")
	}
	if d.Period.Quarter < 0 || d.Period.Quarter > 4 || d.Period.Month < 0 || d.Period.Month > 12 {
		return nil, fmt.Errorf("VAT declaration has an invalid period")
	}
	if _, err := d.Grids(); err != nil {
		return nil, err
	}
	return &d, nil
}

// PeriodKey is "2026-Q2" for a quarterly return, "2026-05" for a monthly one.
func (d *Declaration) PeriodKey() string {
	if d.Period.Month > 0 {
		return fmt.Sprintf("%04d-%02d", d.Period.Year, d.Period.Month)
	}
	return fmt.Sprintf("%04d-Q%d", d.Period.Year, d.Period.Quarter)
}

// FirstMonth and LastMonth are the calendar months the period covers.
func (d *Declaration) FirstMonth() int {
	if d.Period.Month > 0 {
		return d.Period.Month
	}
	return (d.Period.Quarter-1)*3 + 1
}

func (d *Declaration) LastMonth() int {
	if d.Period.Month > 0 {
		return d.Period.Month
	}
	return d.Period.Quarter * 3
}

// ArchiveMonth is where the raw file is archived: the period's last month.
func (d *Declaration) ArchiveMonth() (year, month string) {
	return fmt.Sprintf("%04d", d.Period.Year), fmt.Sprintf("%02d", d.LastMonth())
}

// Grids returns the amounts keyed by two-digit grid number ("00", "54", …),
// in euro cents to keep sums exact.
func (d *Declaration) Grids() (map[string]int64, error) {
	out := make(map[string]int64, len(d.Amounts))
	for _, a := range d.Amounts {
		g := GridKey(a.Grid)
		cents, err := parseCents(a.Value)
		if err != nil {
			return nil, fmt.Errorf("grid %s: %w", g, err)
		}
		out[g] += cents
	}
	return out, nil
}

// GridKey normalises a grid number to two digits ("0" → "00").
func GridKey(g string) string {
	g = strings.TrimSpace(g)
	if len(g) == 1 {
		return "0" + g
	}
	return g
}

func parseCents(s string) (int64, error) {
	s = strings.TrimSpace(s)
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")
	whole, frac, _ := strings.Cut(s, ".")
	if len(frac) > 2 {
		return 0, fmt.Errorf("amount %q has more than 2 decimals", s)
	}
	for len(frac) < 2 {
		frac += "0"
	}
	w, err := strconv.ParseInt(whole, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("amount %q: %w", s, err)
	}
	f, err := strconv.ParseInt(frac, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("amount %q: %w", s, err)
	}
	c := w*100 + f
	if neg {
		c = -c
	}
	return c, nil
}

var statementIDPattern = regexp.MustCompile(`(?i)STATEMENT_\d+_(\d+)\.xml$`)

// StatementIDFromFilename extracts the Intervat statement id from an export
// name like TVA_25092026_12_00_15_STATEMENT_1_69272513.xml, or from an
// archived <id>.xml. Returns "" when the name carries none.
func StatementIDFromFilename(name string) string {
	base := filepath.Base(name)
	if m := statementIDPattern.FindStringSubmatch(base); m != nil {
		return m[1]
	}
	stem := strings.TrimSuffix(base, filepath.Ext(base))
	if stem != "" && strings.Trim(stem, "0123456789") == "" {
		return stem
	}
	return ""
}

// Totals are the Belgian return's own control sums.
type Totals struct {
	OutputVAT  int64 // grid XX = 54+55+56+57+61+63
	InputVAT   int64 // grid YY = 59+62+64
	Due        int64 // grid 71
	Credit     int64 // grid 72
	Consistent bool  // 71/72 match XX−YY
}

// ComputeTotals derives the control sums and checks grid 71/72 against them.
func ComputeTotals(g map[string]int64) Totals {
	t := Totals{
		OutputVAT: g["54"] + g["55"] + g["56"] + g["57"] + g["61"] + g["63"],
		InputVAT:  g["59"] + g["62"] + g["64"],
		Due:       g["71"],
		Credit:    g["72"],
	}
	diff := t.OutputVAT - t.InputVAT
	switch {
	case diff >= 0:
		t.Consistent = t.Due == diff && t.Credit == 0
	default:
		t.Consistent = t.Credit == -diff && t.Due == 0
	}
	return t
}

// SortedGrids returns grid keys in the order the official form lists them.
func SortedGrids(g map[string]int64) []string {
	keys := make([]string, 0, len(g))
	for k := range g {
		keys = append(keys, k)
	}
	pos := map[string]int{}
	for i, k := range GridOrder {
		pos[k] = i
	}
	sort.Slice(keys, func(i, j int) bool {
		pi, oki := pos[keys[i]]
		pj, okj := pos[keys[j]]
		if oki && okj {
			return pi < pj
		}
		if oki != okj {
			return oki
		}
		return keys[i] < keys[j]
	})
	return keys
}

// GridOrder is the order of the grids on the official form.
var GridOrder = []string{
	"00", "01", "02", "03", "44", "45", "46", "47", "48", "49",
	"81", "82", "83", "84", "85", "86", "87", "88",
	"54", "55", "56", "57", "61", "63", "59", "62", "64",
	"71", "72", "91",
}

// GridLabels describes each grid of the Belgian periodic VAT return.
var GridLabels = map[string]string{
	"00": "Operations under a special regime (taxable base)",
	"01": "Operations at 6% (taxable base)",
	"02": "Operations at 12% (taxable base)",
	"03": "Operations at 21% (taxable base)",
	"44": "Services to customers in other EU countries",
	"45": "Operations where the customer owes the VAT",
	"46": "Exempt intra-EU supplies of goods",
	"47": "Other exempt operations and operations abroad",
	"48": "Credit notes issued on grids 44 and 46",
	"49": "Credit notes issued on other sales",
	"81": "Purchases of goods, raw materials and consumables",
	"82": "Purchases of services and miscellaneous goods",
	"83": "Purchases of investment goods",
	"84": "Credit notes received on grids 86 and 88",
	"85": "Other credit notes received",
	"86": "Intra-EU acquisitions of goods",
	"87": "Other incoming operations where we owe the VAT",
	"88": "Services received from other EU countries",
	"54": "VAT due on grids 01, 02 and 03",
	"55": "VAT due on grids 86 and 88",
	"56": "VAT due on grid 87",
	"57": "VAT due on imports",
	"61": "Regularisations in favour of the State",
	"62": "Regularisations in our favour",
	"63": "VAT to repay on credit notes received",
	"64": "VAT to recover on credit notes issued",
	"59": "Deductible VAT",
	"71": "Amount due to the State",
	"72": "Amount due by the State",
	"91": "Actual VAT due for 1–20 December (monthly filers)",
}
