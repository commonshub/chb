package cmd

// VAT declarations → vat.json.
//
// The raw Intervat XML archives (YYYY/MM/providers/intervat/<id>.xml, see
// cmd/vat_sync.go) become one public file per year, YYYY/vat.json, and one
// for all periods, latest/vat.json. A VAT return is the organisation's own
// filing with the State — amounts per grid, no counterparties — so it is
// public and, like hashes.json, identical for every audience: it lives once
// outside the tiers. The filer's email and phone stay in the raw archive.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/CommonsHub/chb/providers/intervat"
)

const vatFile = "vat.json"

// euros is an amount in cents that marshals as a two-decimal JSON number.
type euros int64

func (e euros) MarshalJSON() ([]byte, error) {
	v := int64(e)
	sign := ""
	if v < 0 {
		sign, v = "-", -v
	}
	return []byte(fmt.Sprintf("%s%d.%02d", sign, v/100, v%100)), nil
}

func (e *euros) UnmarshalJSON(b []byte) error {
	var f float64
	if err := json.Unmarshal(b, &f); err != nil {
		return err
	}
	if f < 0 {
		*e = euros(f*100 - 0.5)
	} else {
		*e = euros(f*100 + 0.5)
	}
	return nil
}

func (e euros) Float() float64 { return float64(e) / 100 }

// VATFile is YYYY/vat.json and latest/vat.json.
type VATFile struct {
	GeneratedAt string            `json:"generatedAt"`
	Source      string            `json:"source"` // "intervat"
	VATNumber   string            `json:"vatNumber"`
	Currency    string            `json:"currency"`
	GridLabels  map[string]string `json:"gridLabels"`
	Periods     []VATPeriod       `json:"periods"` // oldest first
}

// VATPeriod is one declaration period with its effective (latest) filing.
type VATPeriod struct {
	Period      string           `json:"period"` // "2026-Q2" or "2026-05"
	Year        int              `json:"year"`
	Quarter     int              `json:"quarter,omitempty"`
	Month       int              `json:"month,omitempty"`
	From        string           `json:"from"` // first day, YYYY-MM-DD
	To          string           `json:"to"`   // last day
	StatementID string           `json:"statementId"`
	Amendments  int              `json:"amendments"` // filings superseded by a later one
	Grids       map[string]euros `json:"grids"`
	Totals      VATTotals        `json:"totals"`
	Filings     []VATFiling      `json:"filings"` // every filing, oldest first
}

type VATFiling struct {
	StatementID    string           `json:"statementId"`
	SequenceNumber string           `json:"sequenceNumber,omitempty"`
	Replaces       string           `json:"replaces,omitempty"` // Intervat reference of the declaration it corrects
	DeclarantName  string           `json:"declarantName"`
	Superseded     bool             `json:"superseded"`
	Grids          map[string]euros `json:"grids"`
	Totals         VATTotals        `json:"totals"`
}

// VATTotals are the return's control sums. Net > 0 was paid to the State,
// Net < 0 is owed by it.
type VATTotals struct {
	OutputVAT  euros `json:"outputVat"` // 54+55+56+57+61+63
	InputVAT   euros `json:"inputVat"`  // 59+62+64
	Due        euros `json:"due"`       // grid 71
	Credit     euros `json:"credit"`    // grid 72
	Net        euros `json:"net"`       // 71 − 72
	Consistent bool  `json:"consistent"`
}

type vatArchive struct {
	path string
	id   string
	decl *intervat.Declaration
}

// loadVATArchives reads every archived declaration under DATA_DIR.
func loadVATArchives(dataDir string) ([]vatArchive, error) {
	paths, _ := filepath.Glob(filepath.Join(dataDir, "[0-9][0-9][0-9][0-9]", "[0-9][0-9]", "providers", intervat.Source, "*.xml"))
	var out []vatArchive
	for _, p := range paths {
		data, err := os.ReadFile(p)
		if err != nil {
			return nil, err
		}
		d, err := intervat.Parse(data)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", p, err)
		}
		out = append(out, vatArchive{path: p, id: strings.TrimSuffix(filepath.Base(p), ".xml"), decl: d})
	}
	return out, nil
}

// filingBefore orders filings of one period: numeric statement ids
// ascending; a filing that replaces another goes after one that doesn't.
func filingBefore(a, b vatArchive) bool {
	ra, rb := a.decl.ReplacedDeclaration != "", b.decl.ReplacedDeclaration != ""
	na, errA := strconv.ParseInt(a.id, 10, 64)
	nb, errB := strconv.ParseInt(b.id, 10, 64)
	if errA == nil && errB == nil && na != nb {
		return na < nb
	}
	if ra != rb {
		return !ra
	}
	return a.id < b.id
}

func vatTotals(g map[string]int64) VATTotals {
	t := intervat.ComputeTotals(g)
	return VATTotals{
		OutputVAT: euros(t.OutputVAT), InputVAT: euros(t.InputVAT),
		Due: euros(t.Due), Credit: euros(t.Credit), Net: euros(t.Due - t.Credit),
		Consistent: t.Consistent,
	}
}

func toEuros(g map[string]int64) map[string]euros {
	out := make(map[string]euros, len(g))
	for k, v := range g {
		out[k] = euros(v)
	}
	return out
}

// buildVATPeriods groups archives by period, oldest period first.
func buildVATPeriods(archives []vatArchive) []VATPeriod {
	byPeriod := map[string][]vatArchive{}
	for _, a := range archives {
		byPeriod[a.decl.PeriodKey()] = append(byPeriod[a.decl.PeriodKey()], a)
	}
	var periods []VATPeriod
	for key, list := range byPeriod {
		sort.Slice(list, func(i, j int) bool { return filingBefore(list[i], list[j]) })
		eff := list[len(list)-1].decl
		first := time.Date(eff.Period.Year, time.Month(eff.FirstMonth()), 1, 0, 0, 0, 0, time.UTC)
		last := time.Date(eff.Period.Year, time.Month(eff.LastMonth())+1, 0, 0, 0, 0, 0, time.UTC)
		p := VATPeriod{
			Period: key, Year: eff.Period.Year, Quarter: eff.Period.Quarter, Month: eff.Period.Month,
			From: first.Format("2006-01-02"), To: last.Format("2006-01-02"),
			StatementID: list[len(list)-1].id,
			Amendments:  len(list) - 1,
		}
		for i, a := range list {
			g, _ := a.decl.Grids()
			f := VATFiling{
				StatementID:    a.id,
				SequenceNumber: a.decl.SequenceNumber,
				Replaces:       a.decl.ReplacedDeclaration,
				DeclarantName:  strings.TrimSpace(a.decl.Declarant.Name),
				Superseded:     i < len(list)-1,
				Grids:          toEuros(g),
				Totals:         vatTotals(g),
			}
			p.Filings = append(p.Filings, f)
			if i == len(list)-1 {
				p.Grids, p.Totals = f.Grids, f.Totals
			}
		}
		periods = append(periods, p)
	}
	sort.Slice(periods, func(i, j int) bool { return periods[i].From < periods[j].From })
	return periods
}

func vatNumberOf(archives []vatArchive) string {
	var best vatArchive
	for _, a := range archives {
		if best.decl == nil || filingBefore(best, a) {
			best = a
		}
	}
	if best.decl == nil {
		return ""
	}
	n := strings.TrimSpace(best.decl.Declarant.VATNumber)
	cc := strings.TrimSpace(best.decl.Declarant.CountryCode)
	if cc == "" {
		cc = "BE"
	}
	if n != "" && !strings.HasPrefix(strings.ToUpper(n), cc) {
		n = cc + n
	}
	return n
}

// generateVAT writes YYYY/vat.json for every year with a declaration and
// latest/vat.json with all of them. Returns the number of periods.
func generateVAT(dataDir string) (int, error) {
	archives, err := loadVATArchives(dataDir)
	if err != nil {
		return 0, err
	}
	if len(archives) == 0 {
		return 0, nil
	}
	periods := buildVATPeriods(archives)
	base := VATFile{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Source:      intervat.Source,
		VATNumber:   vatNumberOf(archives),
		Currency:    "EUR",
		GridLabels:  intervat.GridLabels,
	}
	write := func(path string, ps []VATPeriod) error {
		f := base
		f.Periods = ps
		data, err := json.MarshalIndent(f, "", "  ")
		if err != nil {
			return err
		}
		// Public by definition: hold it to the public tier's policy even
		// though it lives outside the tiers.
		data, err = enforceAudiencePolicy(AudiencePublic, vatFile, data)
		if err != nil {
			return err
		}
		return writeDataFile(path, data)
	}
	byYear := map[int][]VATPeriod{}
	for _, p := range periods {
		byYear[p.Year] = append(byYear[p.Year], p)
	}
	for y, ps := range byYear {
		if err := write(filepath.Join(dataDir, fmt.Sprintf("%04d", y), vatFile), ps); err != nil {
			return 0, err
		}
	}
	if err := write(filepath.Join(dataDir, "latest", vatFile), periods); err != nil {
		return 0, err
	}
	return len(periods), nil
}
