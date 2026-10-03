package annualaccounts

import "testing"

func TestParseFigures(t *testing.T) {
	lines := []string{
		"Commons Hub Brussels ASBL",
		"Balance Sheet (Abbr",
		"As of | 31/12/2030",
		"20/58 - TOTAL ASSETS | 1,000.00",
		"14 - Accumulated Profits (Losses) (+)/(-) | 600.00",
		"Profit (Loss) of the Year | -100.00",
		"Other Appropriations of the Year | 650.00",
		"Profits (Losses) from Previous Years | 50.00",
		"44 - Trade Debts | -20.50",
		"-90.00",
		"9903 - Profit (Loss) for the Period Before Taxes (+)/(-)",
		"(14) - Profit (Loss) to Be Carried Forward (+)/(-) | -50.00",
		"14P - Profit (Loss) of the Preceding Period Brought Forward (+)/(-) | 50.00",
		"ASSETS",
	}
	got := map[string]float64{}
	for _, f := range ParseFigures(lines) {
		got[f.Code] = f.Amount
	}
	want := map[string]float64{
		"20/58": 1000, "14": 600, "14.profitOfTheYear": -100, "14.otherAppropriations": 650,
		"14.previousYears": 50, "44": -20.5, "9903": -90, "(14)": -50, "14P": 50,
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %v, want %v", k, got[k], v)
		}
	}
	if PeriodEnd(lines) != "2030-12-31" {
		t.Errorf("period end = %q", PeriodEnd(lines))
	}
}

func TestDetectKind(t *testing.T) {
	cases := map[string]string{
		"balance_sheet_abbr_assoc_31122025_x.pdf": KindBalanceSheet,
		"profit_and_loss_abbr_assoc_2025_x.pdf":   KindProfitAndLoss,
		"trial_balance_2025_x.pdf":                KindTrialBalance,
		"ASBL_-_Bilan_interne_-_2024.pdf":         KindInternal,
		"notes.pdf":                               KindOther,
	}
	for name, want := range cases {
		if got := DetectKind(name, nil); got != want {
			t.Errorf("%s → %s, want %s", name, got, want)
		}
	}
	if PublicKind(KindTrialBalance) || PublicKind(KindInternal) || !PublicKind(KindBalanceSheet) {
		t.Error("only the abbreviated statements are publishable")
	}
}
