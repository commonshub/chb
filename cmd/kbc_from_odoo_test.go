package cmd

import "testing"

func TestOdooLineToKBCTransaction(t *testing.T) {
	acc := AccountConfig{Slug: "kbc", Name: "KBC", IBAN: "BE46734072238636", Currency: "EUR", OdooSourceOfTruth: true, OdooJournalID: 28}

	// incoming cp-order drink line.
	in := OdooCacheLine{ID: 27348, Date: "2026-06-26", Amount: 4, PaymentRef: "VEREECKE ALAIN Credit transfer BE14 7350 7124 6383 cp-order-44641"}
	tx := odooLineToKBCTransaction(acc, in)
	if tx.Provider != "kbcbrussels" {
		t.Errorf("provider = %q, want kbcbrussels", tx.Provider)
	}
	if tx.Type != "CREDIT" || tx.Amount != 4 {
		t.Errorf("type/amount = %s/%.2f, want CREDIT/4", tx.Type, tx.Amount)
	}
	if tx.AccountSlug != "kbc" || tx.Account != acc.IBAN {
		t.Errorf("account wiring wrong: slug=%q account=%q", tx.AccountSlug, tx.Account)
	}
	if got := stringMetadata(tx.Metadata, "description"); got != in.PaymentRef {
		t.Errorf("description = %q, want the payment_ref (so rules can categorise cp-order)", got)
	}
	if tx.Timestamp == 0 {
		t.Errorf("timestamp not parsed from date")
	}
	if tx.TxHash != "27348" {
		t.Errorf("txHash = %q, want the stable Odoo line id 27348", tx.TxHash)
	}

	// outgoing line → DEBIT.
	out := OdooCacheLine{ID: 27363, Date: "2026-05-26", Amount: -54.45, PaymentRef: "PROXIMUS Direct debit"}
	if d := odooLineToKBCTransaction(acc, out); d.Type != "DEBIT" || d.Amount != -54.45 {
		t.Errorf("outgoing: type/amount = %s/%.2f, want DEBIT/-54.45", d.Type, d.Amount)
	}
}

func TestKBCEntryInYearMonth(t *testing.T) {
	acc := AccountConfig{Slug: "kbc", IBAN: "BE46", Currency: "EUR"}
	tx := odooLineToKBCTransaction(acc, OdooCacheLine{ID: 1, Date: "2026-06-26", Amount: 10, PaymentRef: "x"})
	if !kbcEntryInYearMonth(tx, "2026", "06") {
		t.Errorf("June tx should match 2026/06")
	}
	if kbcEntryInYearMonth(tx, "2026", "07") {
		t.Errorf("June tx must not match 2026/07")
	}
	if !kbcEntryInYearMonth(tx, "2026", "") {
		t.Errorf("empty month should match any month in the year")
	}
	if kbcEntryInYearMonth(tx, "2025", "06") {
		t.Errorf("wrong year must not match")
	}
}
