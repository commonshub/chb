package cmd

import (
	"strings"
	"testing"
)

func TestIndividualAccountDetection(t *testing.T) {
	p := accountPrivacy{private: map[string]bool{"499999": true}, public: map[string]bool{"489350": true}, orgNames: []string{"all for climate dao"}}
	cases := map[string]bool{
		"489302|C/C DAMMAN Xavier":        true,
		"489309|C/C KEVIN SUNDAR":         true,
		"613000|RETRIBUTIONS LEEN":        true,
		"489301|C/C ALL FOR CLIMATE":      false, // a company partner
		"416200|C/C BRUSSELS PAY ASBL":    false, // legal form
		"416000|C/C GERANT":               true,  // a role one person holds
		"489100|Current account director": true,
		"489110|C/C ADMINISTRATEURS":      false, // a group
		"489120|C/C Voorzitter":           true,
		"411200|C/C TVA A RECUPERER":      false,
		"694000|REMUNERATION DU CAPITAL":  false,
		"620200|REMUNERATIONS-EMPLOYES":   false,
		"499999|ATTENTE":                  true,  // settings override
		"489350|C/C JANE DOE":             false, // settings override
		"604200|ACHATS NOURRITURE":        false,
	}
	for in, want := range cases {
		code, name, _ := strings.Cut(in, "|")
		if got := p.isIndividualAccount(code, name); got != want {
			t.Errorf("%s → %v, want %v", in, got, want)
		}
	}
}

func TestLedgerForAudience(t *testing.T) {
	ownIBANsForTest = map[string]bool{}
	defer func() { ownIBANsForTest = nil }()
	l := &LedgerFile{Year: "2031", PeriodStart: "2031-01-01", PeriodEnd: "2031-12-31", FiscalStart: "2031-01-01", Accounts: []LedgerRow{
		{Code: "489302", Name: "C/C DOE Jane", Opening: -100, Credit: 50, Closing: -150},
		{Code: "489309", Name: "C/C ROE Bob", Opening: -10, Debit: 10, Closing: 0},
		{Code: "550013", Name: "MONERIUM BE56 0016 9232 9088", Debit: 200, Closing: 200},
		{Code: "604200", Name: "ACHATS NOURRITURE", Debit: 30, Closing: 30},
		{Code: "620200", Name: "REMUNERATIONS-EMPLOYES", Debit: 1000, Closing: 1000},
		{Code: "621000", Name: "COTISAT.PATRO.D'ASS.SOC", Debit: 300, Closing: 300},
	}}
	p := accountPrivacy{private: map[string]bool{}, public: map[string]bool{}}
	pub := ledgerForAudience(l, AudiencePublic, p, "now")
	mem := ledgerForAudience(l, AudienceMembers, p, "now")
	stw := ledgerForAudience(l, AudienceStewards, p, "now")

	row := func(f LedgerBalancesFile, code string) *LedgerOut {
		for i := range f.Accounts {
			if f.Accounts[i].Code == code {
				return &f.Accounts[i]
			}
		}
		return nil
	}
	if r := row(pub, "48"); r == nil || r.Merged != 2 || r.Closing != -15000 || strings.Contains(r.Label, "DOE") {
		t.Errorf("public individuals row = %+v", r)
	}
	if row(pub, "489302") != nil || row(mem, "489302") == nil || row(mem, "489302").Label != "C/C DOE Jane" {
		t.Error("individual accounts: merged and neutral in public, named in members")
	}
	if r := row(pub, "62"); r == nil || r.Merged != 2 || r.Debit != 130000 {
		t.Errorf("public payroll = %+v", r)
	}
	if row(mem, "620200") != nil || row(mem, "62") == nil {
		t.Error("members see payroll as the 62 total only")
	}
	if row(stw, "620200") == nil || row(stw, "489302") == nil {
		t.Error("stewards see every account")
	}
	if r := row(pub, "550013"); r == nil || strings.Contains(r.Label, "BE56") {
		t.Errorf("bank account label must be masked below stewards: %+v", r)
	}
	if pub.Totals.Debit != stw.Totals.Debit || pub.Totals.Credit != stw.Totals.Credit {
		t.Error("totals are the same in every tier")
	}
}
