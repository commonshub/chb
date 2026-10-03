package cmd

// `chb annual-accounts` — import the association's annual accounts.
//
// Like the VAT declarations there is no API (yet): the operator imports
// the documents of a fiscal year (the abbreviated balance sheet and profit
// and loss as filed with the National Bank, optionally the trial balance or
// an internal balance sheet, and/or a figures.csv / NBB structured export).
// They are archived unchanged in
//
//	YYYY/MM/providers/annual-accounts/<period end>/   (YYYY/MM = end of the fiscal period)
//
// next to a filing.json (period, status draft|filed, NBB reference).
// cmd/annual_accounts_generate.go publishes them. A fiscal year is draft
// until marked filed: drafts and every document that is not an abbreviated
// statement stay in the stewards tier.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	aa "github.com/CommonsHub/chb/providers/annualaccounts"
)

// AnnualFiling is providers/annual-accounts/<period end>/filing.json.
type AnnualFiling struct {
	Label            string           `json:"label"` // as the NBB names the fiscal year; defaults to the end year
	PeriodStart      string           `json:"periodStart"`
	PeriodEnd        string           `json:"periodEnd"`
	PeriodAssumed    bool             `json:"periodAssumed,omitempty"` // start assumed to be 1 January
	Status           string           `json:"status"`                  // draft | filed
	FiledAt          string           `json:"filedAt,omitempty"`       // empty when filed but the date is unknown
	FiguresSource    string           `json:"figuresSource,omitempty"` // "" = the statements; "internal-balance" = class totals read from the internal balance sheet
	NBBReference     string           `json:"nbbReference,omitempty"`
	NBBURL           string           `json:"nbbUrl,omitempty"`
	Schema           string           `json:"schema"` // abbreviated-association
	EntityName       string           `json:"entityName,omitempty"`
	EnterpriseNumber string           `json:"enterpriseNumber,omitempty"` // 0804.505.132
	Documents        []AnnualDocument `json:"documents"`
}

type AnnualDocument struct {
	File   string `json:"file"`
	Kind   string `json:"kind"` // balance-sheet | profit-and-loss | trial-balance | internal | figures | other
	SHA256 string `json:"sha256"`
	Bytes  int64  `json:"bytes"`
}

func annualAccountsInboxDir(dataDir string) string {
	return filepath.Join(dataDir, "latest", "providers", aa.Source)
}

func annualFilingDir(dataDir, periodEnd string) string {
	return filepath.Join(dataDir, periodEnd[:4], periodEnd[5:7], aa.RelPath(periodEnd))
}

// AnnualAccountsCommand dispatches `chb annual-accounts …`.
func AnnualAccountsCommand(args []string) error {
	if len(args) == 0 || args[0] == "list" {
		return annualAccountsList(args)
	}
	switch args[0] {
	case "import":
		return annualAccountsImport(args[1:])
	case "set":
		return annualAccountsSet(args[1:])
	}
	printAnnualAccountsHelp()
	if HasFlag(args, "--help", "-h") {
		return nil
	}
	return fmt.Errorf("unknown subcommand %q", args[0])
}

func parsePeriodFlag(v string) (string, string, error) {
	start, end, ok := strings.Cut(v, ":")
	if !ok || !isISODate(start) || !isISODate(end) || start > end {
		return "", "", fmt.Errorf("--period %q: expected YYYY-MM-DD:YYYY-MM-DD", v)
	}
	return start, end, nil
}

func isISODate(s string) bool {
	_, err := time.Parse("2006-01-02", s)
	return err == nil
}

// collectAnnualFiles lists the files to import (dirs expanded, dot files
// and macOS ._ files skipped).
func collectAnnualFiles(inputs []string) ([]string, error) {
	var files []string
	for _, in := range inputs {
		st, err := os.Stat(in)
		if err != nil {
			return nil, err
		}
		if !st.IsDir() {
			files = append(files, in)
			continue
		}
		entries, _ := os.ReadDir(in)
		for _, e := range entries {
			if e.IsDir() || strings.HasPrefix(e.Name(), ".") {
				continue
			}
			files = append(files, filepath.Join(in, e.Name()))
		}
	}
	sort.Strings(files)
	return files, nil
}

// uploadPrefix strips the "1a2b3c4d-" prefix chat uploads add to names.
var uploadPrefix = regexp.MustCompile(`^[0-9a-f]{8}-`)

// importAnnualFiles archives files as one fiscal year. Returns the filing.
func importAnnualFiles(dataDir string, files []string, period, label string, internal map[string]bool, dryRun bool) (*AnnualFiling, error) {
	type doc struct {
		src, name, kind string
		data            []byte
		lines           []string
	}
	var docs []doc
	periodEnd := ""
	entity, number := "", ""
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			return nil, err
		}
		name := uploadPrefix.ReplaceAllString(filepath.Base(f), "")
		d := doc{src: f, name: name, data: data}
		switch strings.ToLower(filepath.Ext(name)) {
		case ".pdf":
			d.lines, _ = aa.ExtractLines(f)
			d.kind = aa.DetectKind(name, d.lines)
			if pe := aa.PeriodEnd(d.lines); pe != "" && periodEnd == "" {
				periodEnd = pe
			}
			if entity == "" && len(d.lines) > 0 && d.kind != aa.KindOther {
				entity, number = annualEntity(d.lines)
			}
		case ".csv", ".xbrl", ".xml", ".json":
			d.kind = "figures"
		default:
			d.kind = aa.KindOther
		}
		if internal[filepath.Base(f)] || internal[name] {
			d.kind = aa.KindInternal
		}
		docs = append(docs, d)
	}
	start, end, assumed := "", periodEnd, false
	if period != "" {
		var err error
		if start, end, err = parsePeriodFlag(period); err != nil {
			return nil, err
		}
	}
	if end == "" {
		return nil, fmt.Errorf("could not read the end of the fiscal period from a balance sheet: pass --period YYYY-MM-DD:YYYY-MM-DD")
	}
	if start == "" {
		start, assumed = end[:4]+"-01-01", true
	}
	dir := annualFilingDir(dataDir, end)
	filing := &AnnualFiling{Status: "draft", Schema: "abbreviated-association"}
	if data, err := os.ReadFile(filepath.Join(dir, "filing.json")); err == nil {
		_ = json.Unmarshal(data, filing)
	}
	// Re-importing a document keeps a period start set earlier with
	// --period; only a brand-new fiscal year gets the 1 January guess.
	if assumed && filing.PeriodStart != "" && !filing.PeriodAssumed && filing.PeriodEnd == end {
		start, assumed = filing.PeriodStart, false
	}
	filing.PeriodStart, filing.PeriodEnd, filing.PeriodAssumed = start, end, assumed
	if label != "" {
		filing.Label = label
	} else if filing.Label == "" {
		filing.Label = end[:4]
	}
	if entity != "" {
		filing.EntityName, filing.EnterpriseNumber = entity, number
	}
	byName := map[string]int{}
	for i, d := range filing.Documents {
		byName[d.File] = i
	}
	for _, d := range docs {
		sum := sha256.Sum256(d.data)
		ad := AnnualDocument{File: d.name, Kind: d.kind, SHA256: hex.EncodeToString(sum[:]), Bytes: int64(len(d.data))}
		if i, ok := byName[d.name]; ok {
			filing.Documents[i] = ad
		} else {
			byName[d.name] = len(filing.Documents)
			filing.Documents = append(filing.Documents, ad)
		}
		if !dryRun {
			if err := writeDataFile(filepath.Join(dir, d.name), d.data); err != nil {
				return nil, err
			}
		}
	}
	if !dryRun {
		if err := saveAnnualFiling(dataDir, filing); err != nil {
			return nil, err
		}
	}
	return filing, nil
}

var vatLine = regexp.MustCompile(`VAT:\s*BE\s*0?([0-9]{3})\.?([0-9]{3})\.?([0-9]{3})`)

// annualEntity reads the association's name (first line) and enterprise
// number (the VAT line) from a statement header.
func annualEntity(lines []string) (string, string) {
	name := ""
	for _, l := range lines[:min(len(lines), 6)] {
		if strings.Contains(strings.ToUpper(l), "ASBL") || strings.Contains(strings.ToUpper(l), "VZW") {
			name = strings.TrimSpace(strings.Split(l, " | ")[0])
			break
		}
	}
	for _, l := range lines[:min(len(lines), 12)] {
		if m := vatLine.FindStringSubmatch(l); m != nil {
			return name, "0" + m[1] + "." + m[2] + "." + m[3]
		}
	}
	return name, ""
}

func saveAnnualFiling(dataDir string, f *AnnualFiling) error {
	data, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return err
	}
	return writeDataFile(filepath.Join(annualFilingDir(dataDir, f.PeriodEnd), "filing.json"), data)
}

// loadAnnualFilings reads every filing.json, oldest period first.
func loadAnnualFilings(dataDir string) []*AnnualFiling {
	paths, _ := filepath.Glob(filepath.Join(dataDir, "[0-9][0-9][0-9][0-9]", "[0-9][0-9]", aa.RelPath("*", "filing.json")))
	var out []*AnnualFiling
	for _, p := range paths {
		data, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		var f AnnualFiling
		if json.Unmarshal(data, &f) == nil && f.PeriodEnd != "" {
			out = append(out, &f)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PeriodEnd < out[j].PeriodEnd })
	return out
}

func annualAccountsImport(args []string) error {
	if HasFlag(args, "--help", "-h") || len(args) == 0 {
		printAnnualAccountsHelp()
		return nil
	}
	// Flags taking a value; --filed takes one only when it is a date.
	valueFlags := map[string]bool{"--period": true, "--nbb-ref": true, "--nbb-url": true, "--label": true, "--internal": true, "--figures-source": true}
	var inputs []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		if strings.HasPrefix(a, "--") {
			if strings.Contains(a, "=") || i+1 >= len(args) {
				continue
			}
			if valueFlags[a] || (a == "--filed" && isISODate(args[i+1])) {
				i++
			}
			continue
		}
		inputs = append(inputs, a)
	}
	files, err := collectAnnualFiles(inputs)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return fmt.Errorf("no files to import")
	}
	internal := map[string]bool{}
	for _, f := range GetOptions(args, "--internal") {
		internal[filepath.Base(f)] = true
	}
	dryRun := HasFlag(args, "--dry-run")
	dataDir := DataDir()
	filing, err := importAnnualFiles(dataDir, files, GetOption(args, "--period"), GetOption(args, "--label"), internal, dryRun)
	if err != nil {
		return err
	}
	if HasFlag(args, "--filed") || GetOption(args, "--filed") != "" || HasFlag(args, "--nbb-ref") || HasFlag(args, "--nbb-url") || GetOption(args, "--figures-source") != "" {
		if err := applyAnnualSetFlags(filing, args); err != nil {
			return err
		}
		if !dryRun {
			if err := saveAnnualFiling(dataDir, filing); err != nil {
				return err
			}
		}
	}
	printAnnualFiling(filing)
	if dryRun {
		fmt.Printf("  %s(dry-run — nothing written)%s\n\n", Fmt.Dim, Fmt.Reset)
		return nil
	}
	n, err := generateAnnualAccounts(dataDir)
	if err != nil {
		return err
	}
	fmt.Printf("  ✓ annual-accounts.json: %s\n\n", Pluralize(n, "fiscal year", ""))
	return nil
}

func applyAnnualSetFlags(f *AnnualFiling, args []string) error {
	if HasFlag(args, "--filed") || GetOption(args, "--filed") != "" {
		v := GetOption(args, "--filed")
		if isISODate(v) {
			f.Status, f.FiledAt = "filed", v
		} else {
			f.Status, f.FiledAt = "filed", "" // filed, date unknown
		}
	}
	if v := GetOption(args, "--figures-source"); v != "" {
		switch v {
		case "statements":
			f.FiguresSource = ""
		case "internal-balance":
			f.FiguresSource = v
		default:
			return fmt.Errorf("--figures-source %q: expected statements or internal-balance", v)
		}
	}
	if HasFlag(args, "--draft") {
		f.Status, f.FiledAt = "draft", ""
	}
	if v := GetOption(args, "--period"); v != "" {
		start, end, err := parsePeriodFlag(v)
		if err != nil {
			return err
		}
		if end != f.PeriodEnd {
			return fmt.Errorf("--period ends %s but this fiscal year ends %s; re-import to move it", end, f.PeriodEnd)
		}
		f.PeriodStart, f.PeriodAssumed = start, false
	}
	if v := GetOption(args, "--nbb-ref"); v != "" {
		f.NBBReference = v
	}
	if v := GetOption(args, "--nbb-url"); v != "" {
		f.NBBURL = v
	}
	if v := GetOption(args, "--label"); v != "" {
		f.Label = v
	}
	return nil
}

// annualAccountsSet is `chb annual-accounts set <period end | end year> …`.
func annualAccountsSet(args []string) error {
	if len(args) == 0 || strings.HasPrefix(args[0], "-") {
		printAnnualAccountsHelp()
		return fmt.Errorf("which fiscal year? pass its end date (YYYY-MM-DD) or end year")
	}
	dataDir := DataDir()
	var target *AnnualFiling
	for _, f := range loadAnnualFilings(dataDir) {
		if f.PeriodEnd == args[0] || f.PeriodEnd[:4] == args[0] || f.Label == args[0] {
			target = f
		}
	}
	if target == nil {
		return fmt.Errorf("no imported fiscal year matches %q (see `chb annual-accounts`)", args[0])
	}
	if err := applyAnnualSetFlags(target, args[1:]); err != nil {
		return err
	}
	if err := saveAnnualFiling(dataDir, target); err != nil {
		return err
	}
	printAnnualFiling(target)
	n, err := generateAnnualAccounts(dataDir)
	if err != nil {
		return err
	}
	fmt.Printf("  ✓ annual-accounts.json: %s\n\n", Pluralize(n, "fiscal year", ""))
	return nil
}

func annualAccountsList(args []string) error {
	if HasFlag(args, "--help", "-h") {
		printAnnualAccountsHelp()
		return nil
	}
	dataDir := DataDir()
	filings := loadAnnualFilings(dataDir)
	if len(filings) == 0 {
		fmt.Printf("\n  No annual accounts imported.\n  %sImport them: chb annual-accounts import <files…>%s\n\n", Fmt.Dim, Fmt.Reset)
		return nil
	}
	fmt.Println()
	fmt.Printf("  %-6s %-23s %-8s %-11s %14s %14s  %s\n", "FY", "PERIOD", "STATUS", "FILED", "TOTAL ASSETS", "RESULT", "CHECKS")
	for _, f := range filings {
		fy := buildAnnualFiscalYear(dataDir, f, filings)
		warn := 0
		for _, c := range fy.Checks {
			if c.Level != "info" {
				warn++
			}
		}
		fmt.Printf("  %-6s %s → %s %-8s %-11s %14s %14s  %d\n", f.Label, f.PeriodStart, f.PeriodEnd, f.Status, firstNonEmpty(f.FiledAt, "-"),
			fmtEUR(fy.KeyFigures.TotalAssets.Float()), fmtEURSigned(fy.KeyFigures.ResultOfThePeriod.Float()), warn)
	}
	fmt.Println()
	return nil
}

func printAnnualFiling(f *AnnualFiling) {
	fmt.Printf("\n  %sFiscal year %s%s  %s → %s  (%s", Fmt.Bold, f.Label, Fmt.Reset, f.PeriodStart, f.PeriodEnd, f.Status)
	if f.FiledAt != "" {
		fmt.Printf(", filed %s", f.FiledAt)
	}
	fmt.Println(")")
	if f.PeriodAssumed {
		fmt.Printf("  %s⚠ period start assumed to be 1 January: confirm with --period START:END%s\n", Fmt.Yellow, Fmt.Reset)
	}
	for _, d := range f.Documents {
		vis := "stewards only"
		if aa.PublicKind(d.Kind) {
			vis = "public once filed"
		}
		fmt.Printf("    %-16s %-60s %s\n", d.Kind, d.File, vis)
	}
	fmt.Println()
}

// pullAnnualAccountsInbox is the provider pull step: documents dropped in
// latest/providers/annual-accounts/ are imported as a draft fiscal year
// (period end read from the balance sheet). Nothing becomes public until
// `chb annual-accounts set <year> --filed <date>`.
func pullAnnualAccountsInbox(args []string) (string, error) {
	dataDir := DataDir()
	register := ""
	if n, err := refreshNBBRegister(dataDir, HasFlag(args, "--force")); err != nil {
		Warnf("⚠ NBB register: %v", err)
	} else if n >= 0 {
		register = fmt.Sprintf("; NBB register: %s", Pluralize(n, "deposit", ""))
	}
	files, _ := collectAnnualFiles([]string{annualAccountsInboxDir(dataDir)})
	var keep []string
	for _, f := range files {
		switch filepath.Base(f) {
		case "README.md", filepath.Base(nbbRegisterPath(dataDir)):
			continue // not a document: the register snapshot lives here too
		}
		keep = append(keep, f)
	}
	if len(keep) == 0 {
		if register != "" {
			_, _ = generateAnnualAccounts(dataDir)
		}
		return "drop folder empty" + register, nil
	}
	filing, err := importAnnualFiles(dataDir, keep, "", "", nil, false)
	if err != nil {
		return "", fmt.Errorf("drop folder left as is: %w", err)
	}
	for _, f := range keep {
		os.Remove(f)
	}
	if _, err := generateAnnualAccounts(dataDir); err != nil {
		return "", err
	}
	return fmt.Sprintf("fiscal year %s imported as %s (%s)%s", filing.Label, filing.Status, Pluralize(len(keep), "document", ""), register), nil
}

func printAnnualAccountsHelp() {
	fmt.Print(`
chb annual-accounts — the association's annual accounts (NBB filing)

USAGE
  chb annual-accounts                         List fiscal years
  chb annual-accounts import <files|dirs> [--period YYYY-MM-DD:YYYY-MM-DD]
        [--filed YYYY-MM-DD] [--nbb-ref REF] [--nbb-url URL] [--label 2023]
        [--internal <file> …] [--dry-run]
  chb annual-accounts set <end year|end date|label> [--filed [YYYY-MM-DD] | --draft]
        [--period …] [--nbb-ref REF] [--nbb-url URL] [--label …]
        [--figures-source statements|internal-balance]

One import = one fiscal year: the abbreviated balance sheet and profit and
loss (PDF, as filed with the National Bank), optionally the trial balance,
an internal balance sheet, or a figures.csv ("code;amount" per line, e.g.
from the NBB structured export). Documents are archived unchanged under the
month the fiscal period ends. The period end is read from the balance
sheet; pass --period when the fiscal year is not a calendar year (e.g.
--period 2023-07-01:2024-12-31 --label 2023).

A fiscal year stays a draft (stewards only) until marked filed (--filed
without a date when the filing date is unknown). --figures-source
internal-balance notes that the figures (figures.csv) were read from the
accountant's internal balance sheet because the filed statement is not
available. 'chb pull' checks the NBB register once a day and fills in the
deposit date and reference when the filing appears there. Only the
abbreviated balance sheet and profit and loss are ever published; trial
balances, internal balance sheets (--internal forces it) and anything
unrecognised stay in the stewards tier.

Files dropped in $DATA_DIR/latest/providers/annual-accounts/ are imported
as a draft by 'chb pull'.
`)
}
