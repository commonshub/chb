package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeAnnual(t *testing.T, dir, name, content string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func readAnnual(t *testing.T, dataDir, year string, a Audience) AnnualAccountsFile {
	t.Helper()
	var f AnnualAccountsFile
	raw, err := os.ReadFile(audiencePath(dataDir, year, "", a, annualAccountsFile))
	if err != nil || json.Unmarshal(raw, &f) != nil {
		t.Fatalf("%s/%s annual-accounts.json: %v", year, a, err)
	}
	return f
}

func TestAnnualAccountsVisibilityAndChecks(t *testing.T) {
	tmp := t.TempDir()
	dataDir := filepath.Join(tmp, "data")
	t.Setenv("DATA_DIR", dataDir)
	in := t.TempDir()
	// Previous fiscal year (18 months), carried forward 100.
	prevFiles := []string{
		writeAnnual(t, in, "figures-prev.csv", "code;amount\n20/58;500\n10/49;500\n(14);100\n14;100\n"),
	}
	if _, err := importAnnualFiles(dataDir, prevFiles, "2029-07-01:2030-12-31", "2029", nil, false); err != nil {
		t.Fatal(err)
	}
	// This year: unbalanced on purpose, a balancing appropriation, a
	// negative trade debt, and 14P ≠ last year's (14).
	in2 := t.TempDir()
	files := []string{
		writeAnnual(t, in2, "figures.csv", "code;amount\n20/58;1000\n10/49;990\n14;600\n14.profitOfTheYear;-100\n14.otherAppropriations;650\n14.previousYears;50\n(14);-50\n14P;50\n44;-20,50\n9900;300\n70;100\n"),
		writeAnnual(t, in2, "balance_sheet_abbr_assoc_31122031_x.pdf", "%PDF fake"),
		writeAnnual(t, in2, "trial_balance_2031_x.pdf", "%PDF fake"),
		writeAnnual(t, in2, "ASBL_-_Bilan_interne_-_2031.pdf", "%PDF names and salaries"),
	}
	if _, err := importAnnualFiles(dataDir, files, "2031-01-01:2031-12-31", "", nil, false); err != nil {
		t.Fatal(err)
	}
	if _, err := generateAnnualAccounts(dataDir); err != nil {
		t.Fatal(err)
	}

	// Drafts: nothing public, everything for stewards.
	if pub := readAnnual(t, dataDir, "2031", AudiencePublic); len(pub.FiscalYears) != 0 {
		t.Fatalf("a draft must not be public: %+v", pub.FiscalYears)
	}
	stw := readAnnual(t, dataDir, "2031", AudienceStewards)
	if len(stw.FiscalYears) != 1 || len(stw.FiscalYears[0].Documents) != 4 {
		t.Fatalf("stewards = %+v", stw.FiscalYears)
	}

	// Filed: only the abbreviated statement is public, copied to public/.
	for _, f := range loadAnnualFilings(dataDir) {
		f.Status, f.FiledAt = "filed", "2032-06-30"
		if err := saveAnnualFiling(dataDir, f); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := generateAnnualAccounts(dataDir); err != nil {
		t.Fatal(err)
	}
	pub := readAnnual(t, dataDir, "2031", AudiencePublic)
	if len(pub.FiscalYears) != 1 {
		t.Fatalf("filed year missing from public: %+v", pub)
	}
	fy := pub.FiscalYears[0]
	if len(fy.Documents) != 1 || fy.Documents[0].Kind != "balance-sheet" || fy.Documents[0].Path != "2031/public/annual-accounts/balance_sheet_abbr_assoc_31122031_x.pdf" {
		t.Errorf("public documents = %+v", fy.Documents)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "2031", "public", "annual-accounts", "ASBL_-_Bilan_interne_-_2031.pdf")); err == nil {
		t.Error("an internal balance sheet reached public/")
	}
	raw, _ := os.ReadFile(audiencePath(dataDir, "2031", "", AudiencePublic, annualAccountsFile))
	if strings.Contains(string(raw), "interne") || strings.Contains(string(raw), "trial_balance") {
		t.Error("public annual-accounts.json mentions a stewards-only document")
	}
	if fy.KeyFigures.TotalAssets != 100000 || fy.KeyFigures.CarriedForward != -5000 || fy.Period.Months != 12 {
		t.Errorf("key figures = %+v period = %+v", fy.KeyFigures, fy.Period)
	}
	codes := map[string]bool{}
	for _, c := range fy.Checks {
		codes[c.Code] = true
	}
	for _, want := range []string{"unbalanced", "balancing-appropriation", "carried-forward-mismatch", "negative-liability", "opening-balance-mismatch", "gross-margin-unexplained"} {
		if !codes[want] {
			t.Errorf("missing check %q in %+v", want, fy.Checks)
		}
	}
	// The 18-month previous year lives under the year it ends.
	prev := readAnnual(t, dataDir, "2030", AudiencePublic)
	if len(prev.FiscalYears) != 1 || prev.FiscalYears[0].Label != "2029" || prev.FiscalYears[0].Period.Months != 18 {
		t.Errorf("previous year = %+v", prev.FiscalYears)
	}
	if idx := readAnnual(t, dataDir, "latest", AudiencePublic); len(idx.FiscalYears) != 2 || idx.Scope != "all" {
		t.Errorf("latest index = %+v", idx)
	}

	// Back to draft: the public copy is withdrawn.
	for _, f := range loadAnnualFilings(dataDir) {
		if f.PeriodEnd == "2031-12-31" {
			f.Status, f.FiledAt = "draft", ""
			_ = saveAnnualFiling(dataDir, f)
		}
	}
	_, _ = generateAnnualAccounts(dataDir)
	if _, err := os.Stat(filepath.Join(dataDir, "2031", "public", "annual-accounts", "balance_sheet_abbr_assoc_31122031_x.pdf")); err == nil {
		t.Error("withdrawn statement still in public/")
	}
}

func TestAnnualFiledWithoutDateAndNBBRegister(t *testing.T) {
	tmp := t.TempDir()
	dataDir := filepath.Join(tmp, "data")
	t.Setenv("DATA_DIR", dataDir)
	in := t.TempDir()
	files := []string{writeAnnual(t, in, "figures.csv", "20/58;10\n10/49;10\n")}
	f, err := importAnnualFiles(dataDir, files, "2031-01-01:2031-12-31", "", nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := applyAnnualSetFlags(f, []string{"--filed", "--figures-source", "internal-balance"}); err != nil {
		t.Fatal(err)
	}
	if f.Status != "filed" || f.FiledAt != "" || f.FiguresSource != "internal-balance" {
		t.Fatalf("filing = %+v", f)
	}
	_ = saveAnnualFiling(dataDir, f)

	// Register checked: nothing deposited yet.
	writeFile(t, nbbRegisterPath(dataDir), `{"checkedAt":"2031-10-03T10:00:00Z","enterpriseNumber":"0804.505.132","deposits":[]}`)
	_, _ = generateAnnualAccounts(dataDir)
	raw, _ := os.ReadFile(audiencePath(dataDir, "2031", "", AudiencePublic, annualAccountsFile))
	if !strings.Contains(string(raw), `"filedAt": null`) {
		t.Errorf("an unknown filing date must be null: %s", raw)
	}
	fy := readAnnual(t, dataDir, "2031", AudiencePublic).FiscalYears[0]
	codes := map[string]bool{}
	for _, c := range fy.Checks {
		codes[c.Code] = true
	}
	if !codes["not-on-nbb-register"] || !codes["figures-from-internal-balance"] {
		t.Errorf("checks = %+v", fy.Checks)
	}
	if fy.NBB.URL != "https://consult.cbso.nbb.be/consult-enterprise/0804505132" || fy.NBB.OnRegister {
		t.Errorf("nbb = %+v", fy.NBB)
	}

	// The deposit appears on the register: date and reference come from it.
	writeFile(t, nbbRegisterPath(dataDir), `{"checkedAt":"2032-01-10T10:00:00Z","enterpriseNumber":"0804.505.132","deposits":[{"id":"x","reference":"2032-00012345","periodEndDate":"2031-12-31T00:00:00Z","depositDate":"2032-01-05T09:00:00Z"}]}`)
	_, _ = generateAnnualAccounts(dataDir)
	fy = readAnnual(t, dataDir, "2031", AudiencePublic).FiscalYears[0]
	if !fy.NBB.OnRegister || fy.NBB.Reference != "2032-00012345" || fy.FiledAt == nil || *fy.FiledAt != "2032-01-05" {
		t.Errorf("after deposit: nbb = %+v filedAt = %v", fy.NBB, fy.FiledAt)
	}
	for _, c := range fy.Checks {
		if c.Code == "not-on-nbb-register" {
			t.Error("still flagged as not on the register")
		}
	}
}

func TestAnnualDropFolderIgnoresRegister(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	t.Setenv("DATA_DIR", dataDir)
	writeFile(t, nbbRegisterPath(dataDir), `{"checkedAt":"`+time.Now().UTC().Format(time.RFC3339)+`","enterpriseNumber":"0804.505.132","deposits":[]}`)
	s, err := pullAnnualAccountsInbox(nil)
	if err != nil || !strings.HasPrefix(s, "drop folder empty") {
		t.Errorf("summary %q err %v: the register snapshot is not a document", s, err)
	}
}
