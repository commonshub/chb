package cmd

// `chb vat import` — archive Intervat VAT declaration exports.
//
// Like the KBC statements there is no API: the operator downloads the XML
// exports from MyMinfin/Intervat and imports them. Files are archived
// byte-for-byte as YYYY/MM/providers/intervat/<statementId>.xml, MM being
// the last month of the declared period, then vat.json is regenerated
// (cmd/vat_generate.go).

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/CommonsHub/chb/providers/intervat"
)

// vatInboxDir is where exports can be dropped for `chb vat import` with no
// arguments (the same drop-folder convention as KBC).
func vatInboxDir(dataDir string) string {
	return filepath.Join(dataDir, "latest", "providers", intervat.Source)
}

// VATCommand is `chb vat [import …]`.
func VATCommand(args []string) error {
	if HasFlag(args, "--help", "-h") {
		printVATHelp()
		return nil
	}
	if len(args) > 0 && args[0] == "import" {
		return VATImport(args[1:])
	}
	if len(args) > 0 && args[0] != "list" && !strings.HasPrefix(args[0], "-") {
		printVATHelp()
		return fmt.Errorf("unknown subcommand %q", args[0])
	}
	return VATList(args)
}

type vatImportResult struct {
	src, dest, id string
	decl          *intervat.Declaration
	status        string
	err           error
}

// VATImport is `chb vat import [file|dir …] [--dry-run] [--force]`.
func VATImport(args []string) error {
	dryRun := HasFlag(args, "--dry-run", "-n")
	force := HasFlag(args, "--force")
	dataDir := DataDir()

	var inputs []string
	for _, a := range args {
		if !strings.HasPrefix(a, "-") {
			inputs = append(inputs, a)
		}
	}
	fromInbox := len(inputs) == 0
	if fromInbox {
		inputs = []string{vatInboxDir(dataDir)}
	}
	files, err := collectVATFiles(inputs)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		fmt.Printf("\n  No VAT declaration XML found in %s\n", strings.Join(inputs, ", "))
		fmt.Printf("  %sDownload the declarations from MyMinfin/Intervat, then run: chb vat import <files…>%s\n\n", Fmt.Dim, Fmt.Reset)
		return nil
	}

	var results []vatImportResult
	failed := 0
	for _, src := range files {
		r := importVATFile(dataDir, src, dryRun, force)
		if r.err != nil {
			failed++
		} else if fromInbox && !dryRun && (r.status == "imported" || r.status == "already imported" || r.status == "replaced") {
			os.Remove(src)
		}
		results = append(results, r)
	}

	fmt.Println()
	fmt.Printf("  %-9s %-10s %-12s %14s  %s\n", "PERIOD", "STATEMENT", "KIND", "DUE (71/72)", "STATUS")
	for _, r := range results {
		if r.err != nil {
			fmt.Printf("  %s✗ %s: %v%s\n", Fmt.Red, filepath.Base(r.src), r.err, Fmt.Reset)
			continue
		}
		g, _ := r.decl.Grids()
		t := intervat.ComputeTotals(g)
		kind := "original"
		if r.decl.ReplacedDeclaration != "" {
			kind = "correction"
		}
		status := r.status
		if !t.Consistent {
			status += Fmt.Yellow + " ⚠ 71/72 ≠ output − input VAT" + Fmt.Reset
		}
		fmt.Printf("  %-9s %-10s %-12s %14s  %s\n", r.decl.PeriodKey(), r.id, kind, fmtEURSigned(float64(t.Due-t.Credit)/100), status)
	}
	fmt.Println()
	if dryRun {
		fmt.Printf("  %sDry run — nothing written. Re-run without --dry-run to archive.%s\n\n", Fmt.Dim, Fmt.Reset)
	} else {
		n, err := generateVAT(dataDir)
		if err != nil {
			return err
		}
		fmt.Printf("  ✓ vat.json: %s (latest/vat.json + YYYY/vat.json)\n\n", Pluralize(n, "period", ""))
	}
	if failed > 0 {
		return fmt.Errorf("%d file(s) could not be imported", failed)
	}
	return nil
}

func collectVATFiles(inputs []string) ([]string, error) {
	var files []string
	for _, in := range inputs {
		st, err := os.Stat(in)
		if err != nil {
			if os.IsNotExist(err) && len(inputs) == 1 {
				return nil, nil
			}
			return nil, err
		}
		if !st.IsDir() {
			files = append(files, in)
			continue
		}
		entries, err := os.ReadDir(in)
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			name := e.Name()
			if e.IsDir() || strings.HasPrefix(name, ".") || !strings.EqualFold(filepath.Ext(name), ".xml") {
				continue
			}
			files = append(files, filepath.Join(in, name))
		}
	}
	sort.Strings(files)
	return files, nil
}

func importVATFile(dataDir, src string, dryRun, force bool) vatImportResult {
	r := vatImportResult{src: src}
	data, err := os.ReadFile(src)
	if err != nil {
		r.err = err
		return r
	}
	d, err := intervat.Parse(data)
	if err != nil {
		r.err = err
		return r
	}
	r.decl = d
	r.id = intervat.StatementIDFromFilename(src)
	if r.id == "" {
		sum := sha256.Sum256(data)
		r.id = "sha-" + hex.EncodeToString(sum[:])[:12]
	}
	year, month := d.ArchiveMonth()
	r.dest = filepath.Join(dataDir, year, month, intervat.RelPath(r.id+".xml"))

	if existing, err := os.ReadFile(r.dest); err == nil {
		if bytes.Equal(existing, data) {
			r.status = "already imported"
			return r
		}
		if !force {
			r.err = fmt.Errorf("statement %s is already archived with different content (--force to replace)", r.id)
			return r
		}
		r.status = "replaced"
	} else {
		r.status = "imported"
	}
	if dryRun {
		r.status = "would be " + r.status
		return r
	}
	if err := writeDataFile(r.dest, data); err != nil {
		r.err = err
	}
	return r
}

// VATList is `chb vat [--json]`: one row per declared period.
func VATList(args []string) error {
	dataDir := DataDir()
	archives, err := loadVATArchives(dataDir)
	if err != nil {
		return err
	}
	periods := buildVATPeriods(archives)
	if HasFlag(args, "--json") {
		out := VATFile{Source: intervat.Source, VATNumber: vatNumberOf(archives), Currency: "EUR", GridLabels: intervat.GridLabels, Periods: periods}
		b, _ := json.MarshalIndent(out, "", "  ")
		fmt.Println(string(b))
		return nil
	}
	if len(periods) == 0 {
		fmt.Printf("\n  No VAT declarations archived yet.\n  %sImport the Intervat XML exports: chb vat import <files…>%s\n\n", Fmt.Dim, Fmt.Reset)
		return nil
	}
	fmt.Println()
	fmt.Printf("  %-9s %-10s %14s %14s %14s  %s\n", "PERIOD", "STATEMENT", "OUTPUT VAT", "INPUT VAT", "NET (71−72)", "FILINGS")
	for _, p := range periods {
		note := Pluralize(len(p.Filings), "filing", "")
		if p.Amendments > 0 {
			note += fmt.Sprintf(" (%d superseded)", p.Amendments)
		}
		if !p.Totals.Consistent {
			note += Fmt.Yellow + " ⚠ 71/72 inconsistent" + Fmt.Reset
		}
		fmt.Printf("  %-9s %-10s %14s %14s %14s  %s\n", p.Period, p.StatementID,
			fmtEUR(p.Totals.OutputVAT.Float()), fmtEUR(p.Totals.InputVAT.Float()), fmtEURSigned(p.Totals.Net.Float()), note)
	}
	fmt.Printf("\n  %sPublished as latest/vat.json and YYYY/vat.json by chb generate.%s\n\n", Fmt.Dim, Fmt.Reset)
	return nil
}

func printVATHelp() {
	fmt.Printf(`
chb vat — Belgian periodic VAT declarations (Intervat exports)

USAGE
  chb vat [--json]                          List declared periods
  chb vat import [file|dir …] [--dry-run] [--force]

Download the declarations from MyMinfin → Intervat as XML (one file per
filed declaration, e.g. TVA_25092026_12_00_15_STATEMENT_1_69272513.xml)
and import them. With no argument, import reads the drop folder
$DATA_DIR/latest/providers/intervat/ and empties it; 'chb pull' (the
hourly job) does the same, so dropping the files there is enough.

Each file is archived unchanged as YYYY/MM/providers/intervat/<statement>.xml
(MM = last month of the period). A correction for a period supersedes the
earlier filing. vat.json is regenerated right away (and by chb generate):
latest/vat.json has every period, YYYY/vat.json that year's. Both are
public; the filer's email and phone stay in the raw archive.
`)
}

// pullVATInbox is the intervat provider's pull step (part of `chb pull`,
// hence of the hourly cron): it archives whatever sits in the drop folder,
// quietly. There is no remote to fetch from.
func pullVATInbox(args []string) (string, error) {
	dataDir := DataDir()
	files, err := collectVATFiles([]string{vatInboxDir(dataDir)})
	if err != nil {
		return "", err
	}
	if len(files) == 0 {
		return "drop folder empty", nil
	}
	imported, failed := 0, 0
	var firstErr error
	for _, src := range files {
		r := importVATFile(dataDir, src, false, false)
		if r.err != nil {
			failed++
			if firstErr == nil {
				firstErr = fmt.Errorf("%s: %w", filepath.Base(src), r.err)
			}
			Warnf("⚠ VAT import %s: %v", filepath.Base(src), r.err)
			continue
		}
		if r.status == "imported" {
			imported++
		}
		os.Remove(src)
	}
	if imported > 0 {
		if _, err := generateVAT(dataDir); err != nil {
			return "", err
		}
	}
	summary := Pluralize(imported, "new declaration", "")
	if failed > 0 {
		summary += fmt.Sprintf(", %d refused", failed)
	}
	return summary, firstErr
}
