package intervat

import "testing"

const q2 = `<VATDeclarationType SequenceNumber="1" xmlns="http://www.minfin.fgov.be/VATConsignment" xmlns:ns2="http://www.minfin.fgov.be/InputCommon"><ReplacedVATDeclaration>1-0804505132-202632</ReplacedVATDeclaration><Declarant><ns2:VATNumber>0804505132</ns2:VATNumber><ns2:Name>CHB</ns2:Name><ns2:CountryCode>BE</ns2:CountryCode><ns2:EmailAddress>filer@example.com</ns2:EmailAddress></Declarant><Period><Quarter>2</Quarter><Year>2026</Year></Period><Data><Amount GridNumber="0">2030.00</Amount><Amount GridNumber="54">10974.81</Amount><Amount GridNumber="55">482.01</Amount><Amount GridNumber="57">13.01</Amount><Amount GridNumber="61">3546.99</Amount><Amount GridNumber="63">10.82</Amount><Amount GridNumber="59">2698.04</Amount><Amount GridNumber="64">550.87</Amount><Amount GridNumber="71">11778.73</Amount></Data><ClientListingNihil>NO</ClientListingNihil><Ask Restitution="NO"/></VATDeclarationType>`

func TestParseQuarterlyDeclaration(t *testing.T) {
	d, err := Parse([]byte(q2))
	if err != nil {
		t.Fatal(err)
	}
	if d.PeriodKey() != "2026-Q2" || d.FirstMonth() != 4 || d.LastMonth() != 6 {
		t.Errorf("period = %s %d–%d", d.PeriodKey(), d.FirstMonth(), d.LastMonth())
	}
	if y, m := d.ArchiveMonth(); y != "2026" || m != "06" {
		t.Errorf("archive month = %s/%s", y, m)
	}
	if d.ReplacedDeclaration != "1-0804505132-202632" || d.Declarant.Email != "filer@example.com" {
		t.Errorf("declaration = %+v", d)
	}
	g, _ := d.Grids()
	if g["00"] != 203000 || g["71"] != 1177873 {
		t.Errorf("grids = %v", g)
	}
	tot := ComputeTotals(g)
	if tot.OutputVAT != 1502764 || tot.InputVAT != 324891 || !tot.Consistent {
		t.Errorf("totals = %+v", tot)
	}
	g["71"]++
	if ComputeTotals(g).Consistent {
		t.Error("a grid 71 that does not match output − input VAT must be flagged")
	}
}

func TestParseRejectsOtherXML(t *testing.T) {
	for _, s := range []string{`<foo/>`, `not xml`, `<VATDeclarationType xmlns="http://www.minfin.fgov.be/VATConsignment"/>`} {
		if _, err := Parse([]byte(s)); err == nil {
			t.Errorf("Parse(%q) accepted", s)
		}
	}
}

func TestStatementIDFromFilename(t *testing.T) {
	cases := map[string]string{
		"TVA_25092026_12_00_15_STATEMENT_1_69272513.xml": "69272513",
		"/x/y/69272513.xml":  "69272513",
		"my-declaration.xml": "",
	}
	for in, want := range cases {
		if got := StatementIDFromFilename(in); got != want {
			t.Errorf("%s → %q, want %q", in, got, want)
		}
	}
}

func TestCreditPeriodAndMonthlyPeriod(t *testing.T) {
	x := `<VATDeclarationType xmlns="http://www.minfin.fgov.be/VATConsignment"><Period><Month>5</Month><Year>2026</Year></Period><Data><Amount GridNumber="54">10.00</Amount><Amount GridNumber="59">25.50</Amount><Amount GridNumber="72">15.50</Amount></Data></VATDeclarationType>`
	d, err := Parse([]byte(x))
	if err != nil {
		t.Fatal(err)
	}
	if d.PeriodKey() != "2026-05" {
		t.Errorf("period = %s", d.PeriodKey())
	}
	g, _ := d.Grids()
	if tot := ComputeTotals(g); !tot.Consistent || tot.Credit != 1550 {
		t.Errorf("totals = %+v", tot)
	}
}
