package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func vatXML(quarter, year, replaced, grids string) string {
	r := ""
	if replaced != "" {
		r = "<ReplacedVATDeclaration>" + replaced + "</ReplacedVATDeclaration>"
	}
	return `<VATDeclarationType SequenceNumber="1" xmlns="http://www.minfin.fgov.be/VATConsignment" xmlns:ns2="http://www.minfin.fgov.be/InputCommon">` + r +
		`<Declarant><ns2:VATNumber>0804505132</ns2:VATNumber><ns2:Name>CHB</ns2:Name><ns2:CountryCode>BE</ns2:CountryCode><ns2:EmailAddress>filer@example.com</ns2:EmailAddress><ns2:Phone>0470000000</ns2:Phone></Declarant>` +
		`<Period><Quarter>` + quarter + `</Quarter><Year>` + year + `</Year></Period><Data>` + grids + `</Data><Ask Restitution="NO"/></VATDeclarationType>`
}

func TestVATImportAndGenerate(t *testing.T) {
	dataDir := t.TempDir()
	t.Setenv("DATA_DIR", dataDir)
	in := t.TempDir()
	original := vatXML("2", "2026", "", `<Amount GridNumber="54">100.00</Amount><Amount GridNumber="59">40.00</Amount><Amount GridNumber="71">60.00</Amount>`)
	correction := vatXML("2", "2026", "1-0804505132-202632", `<Amount GridNumber="54">100.00</Amount><Amount GridNumber="59">50.00</Amount><Amount GridNumber="71">50.00</Amount>`)
	q4 := vatXML("4", "2025", "", `<Amount GridNumber="54">10.00</Amount><Amount GridNumber="59">30.00</Amount><Amount GridNumber="72">20.00</Amount>`)
	write := func(name, body string) {
		if err := os.WriteFile(filepath.Join(in, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("TVA_25092026_12_00_14_STATEMENT_1_69270977.xml", original)
	write("TVA_25092026_12_00_15_STATEMENT_1_69272513.xml", correction)
	write("TVA_25092026_12_00_11_STATEMENT_1_64898909.xml", q4)

	if err := VATImport([]string{in}); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"2026/06/providers/intervat/69270977.xml", "2026/06/providers/intervat/69272513.xml", "2025/12/providers/intervat/64898909.xml"} {
		got, err := os.ReadFile(filepath.Join(dataDir, p))
		if err != nil {
			t.Fatalf("%s not archived", p)
		}
		if !strings.Contains(string(got), "filer@example.com") {
			t.Errorf("%s: the raw archive must be byte-for-byte", p)
		}
	}

	raw, err := os.ReadFile(filepath.Join(dataDir, "latest", vatFile))
	if err != nil {
		t.Fatal("latest/vat.json missing")
	}
	for _, leak := range []string{"filer@example.com", "0470000000", "@"} {
		if strings.Contains(string(raw), leak) {
			t.Errorf("vat.json leaks %q", leak)
		}
	}
	st, _ := os.Stat(filepath.Join(dataDir, "latest", vatFile))
	if st.Mode().Perm() != 0o644 {
		t.Errorf("vat.json mode = %o, want 644", st.Mode().Perm())
	}
	var f VATFile
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	if f.VATNumber != "BE0804505132" || len(f.Periods) != 2 || f.Periods[0].Period != "2025-Q4" {
		t.Fatalf("vat.json = %+v", f)
	}
	q2 := f.Periods[1]
	if q2.StatementID != "69272513" || q2.Amendments != 1 || q2.Totals.Due != 5000 || !q2.Totals.Consistent {
		t.Errorf("Q2 = %+v", q2)
	}
	if len(q2.Filings) != 2 || !q2.Filings[0].Superseded || q2.Filings[1].Superseded || q2.Filings[1].Replaces == "" {
		t.Errorf("Q2 filings = %+v", q2.Filings)
	}
	if q4p := f.Periods[0]; q4p.Totals.Net != -2000 || q4p.Totals.Credit != 2000 || q4p.To != "2025-12-31" {
		t.Errorf("Q4 = %+v", q4p)
	}
	var y2025 VATFile
	b, _ := os.ReadFile(filepath.Join(dataDir, "2025", vatFile))
	if json.Unmarshal(b, &y2025) != nil || len(y2025.Periods) != 1 {
		t.Errorf("2025/vat.json = %s", b)
	}
	if !strings.Contains(string(raw), `"due": 50.00`) {
		t.Error("amounts must be written with two decimals")
	}

	// Re-import is a no-op; a different file under the same statement id is refused.
	if err := VATImport([]string{in}); err != nil {
		t.Errorf("re-import: %v", err)
	}
	write("TVA_25092026_12_00_15_STATEMENT_1_69272513.xml", original)
	if err := VATImport([]string{filepath.Join(in, "TVA_25092026_12_00_15_STATEMENT_1_69272513.xml")}); err == nil {
		t.Error("changed content under an archived statement id must be refused without --force")
	}
}

func TestVATImportDropFolderIsEmptied(t *testing.T) {
	dataDir := t.TempDir()
	t.Setenv("DATA_DIR", dataDir)
	inbox := vatInboxDir(dataDir)
	os.MkdirAll(inbox, 0o755)
	p := filepath.Join(inbox, "TVA_1_STATEMENT_1_1.xml")
	os.WriteFile(p, []byte(vatXML("1", "2026", "", `<Amount GridNumber="71">0.00</Amount>`)), 0o644)
	if err := VATImport(nil); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(p); err == nil {
		t.Error("imported file still in the drop folder")
	}
	if _, err := os.Stat(filepath.Join(dataDir, "2026", "03", "providers", "intervat", "1.xml")); err != nil {
		t.Error("drop-folder file not archived under 2026/03")
	}
}

func TestPullVATInboxIsPartOfPull(t *testing.T) {
	dataDir := t.TempDir()
	t.Setenv("DATA_DIR", dataDir)
	if _, ok := providerSpec("intervat"); !ok {
		t.Fatal("intervat must be a provider so chb pull (the hourly cron) imports the drop folder")
	}
	if s, err := pullVATInbox(nil); err != nil || s != "drop folder empty" {
		t.Errorf("empty inbox: %q %v", s, err)
	}
	inbox := vatInboxDir(dataDir)
	os.MkdirAll(inbox, 0o755)
	os.WriteFile(filepath.Join(inbox, "TVA_1_STATEMENT_1_42.xml"), []byte(vatXML("3", "2024", "", `<Amount GridNumber="72">5.00</Amount><Amount GridNumber="59">5.00</Amount>`)), 0o644)
	os.WriteFile(filepath.Join(inbox, "junk.xml"), []byte(`<foo/>`), 0o644)
	s, err := pullVATInbox(nil)
	if err == nil || !strings.Contains(s, "1 new declaration") || !strings.Contains(s, "1 refused") {
		t.Errorf("summary %q err %v", s, err)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "2024", "09", "providers", "intervat", "42.xml")); err != nil {
		t.Error("declaration not archived")
	}
	if _, err := os.Stat(filepath.Join(inbox, "junk.xml")); err != nil {
		t.Error("a refused file stays in the drop folder for a human to look at")
	}
	if _, err := os.Stat(filepath.Join(dataDir, "latest", vatFile)); err != nil {
		t.Error("vat.json not regenerated")
	}
}
