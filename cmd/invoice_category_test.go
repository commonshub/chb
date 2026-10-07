package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestInvoiceIDFromCommunication(t *testing.T) {
	for in, want := range map[string]int{
		"000004421681":                   44216,
		"+++000/0044/21681+++":           44216,
		"***000/0044/22186***":           44221,
		"payment 000/0044/22186 thanks":  44221,
		"000004421682":                   0, // bad check digits
		"MEM 2025 0051":                  0,
		"CP-D524F27A-0FD8-471D-941E-209": 0,
	} {
		if got := invoiceIDFromCommunication(in); got != want {
			t.Errorf("%q → %d, want %d", in, got, want)
		}
	}
}

func TestInvoiceLinesCategorisedByProduct(t *testing.T) {
	data, err := os.ReadFile("defaults/categories.json")
	if err != nil {
		t.Fatal(err)
	}
	var cats []CategoryDef
	if err := json.Unmarshal(data, &cats); err != nil {
		t.Fatal(err)
	}
	o := &odooTxCategorizer{
		prefixes: categoryPrefixes(cats), products: categoryProductGlobs(cats), slugs: map[string]bool{},
		lineByID: map[int]OdooCacheLine{}, lineByImport: map[string]OdooCacheLine{},
		docs: map[int]OdooOutgoingInvoice{
			// Satoshi room booked on 700000 (membership dues): the product wins.
			44216: {ID: 44216, MoveType: "out_invoice", State: "posted", LineItems: []OdooInvoiceLineItem{
				{ProductName: "Satoshi room", DisplayType: "product", SubtotalAmount: 87.5, AccountCode: "700000"},
				{Title: "Rental Satoshi 29th September", DisplayType: "line_note"},
			}},
			// Analytic tag beats product and account.
			2: {ID: 2, MoveType: "out_invoice", LineItems: []OdooInvoiceLineItem{
				{ProductName: "Shifter", SubtotalAmount: 10, AccountCode: "700000", Category: "coworking"},
			}},
			// Unknown analytic category ("individual") is ignored.
			3: {ID: 3, MoveType: "out_invoice", LineItems: []OdooInvoiceLineItem{
				{ProductName: "Yearly individual membership", SubtotalAmount: 10, AccountCode: "704200", Category: "individual"},
			}},
			// Bills: products don't apply, the GL account does.
			4: {ID: 4, MoveType: "in_invoice", LineItems: []OdooInvoiceLineItem{
				{ProductName: "Coffee beans", SubtotalAmount: 10, AccountCode: "604300"},
			}},
			// Mixed: the larger share wins.
			5: {ID: 5, MoveType: "out_invoice", LineItems: []OdooInvoiceLineItem{
				{ProductName: "Mush Room", SubtotalAmount: 87.5, AccountCode: "700000"},
				{ProductName: "Tea, coffee, water and snacks", SubtotalAmount: 52.5, AccountCode: "700000"},
			}},
		},
	}
	for _, c := range cats {
		o.slugs[c.Slug] = true
	}
	for id, want := range map[int]string{44216: "rental", 2: "coworking", 3: "membership", 4: "coffee", 5: "rental"} {
		if got := o.documentCategory([]int{id}); got != want {
			t.Errorf("doc %d → %q, want %q", id, got, want)
		}
	}

	// A Monerium mint with the invoice's structured communication.
	tx := TransactionEntry{ID: "m", Currency: "EURe", Type: "MINT", Amount: 105.88, GrossAmount: 105.88,
		Metadata: map[string]interface{}{"description": "000004421681", "vatAmount": 18.38}}
	o.apply(&tx)
	if tx.Category != "rental" || tx.Metadata["categorySource"] != "odoo" {
		t.Errorf("mint: %q %v", tx.Category, tx.Metadata)
	}
	if docs, _ := tx.Metadata["documents"].([]interface{}); len(docs) != 1 {
		t.Errorf("documents: %v", tx.Metadata["documents"])
	}
	// Outgoing never matches a customer invoice.
	out := TransactionEntry{ID: "o", Currency: "EURe", Type: "BURN", Amount: -105.88, GrossAmount: -105.88,
		Metadata: map[string]interface{}{"description": "000004421681"}}
	o.apply(&out)
	if out.Category != "" {
		t.Errorf("outgoing got %q", out.Category)
	}
}

func TestCoffeeRuleSmallIncomingOnly(t *testing.T) {
	data, err := readDefaultSettingsFile("rules.json")
	if err != nil {
		t.Fatal(err)
	}
	var rules []Rule
	if err := json.Unmarshal(data, &rules); err != nil {
		t.Fatal(err)
	}
	find := func(tx TransactionEntry) string {
		cat := ""
		for i := range rules {
			if rules[i].MatchesTransaction(tx) && rules[i].Assign.Category != "" {
				cat = rules[i].Assign.Category
				if rules[i].Match.Description == "*coffee*" {
					return cat
				}
			}
		}
		return ""
	}
	small := TransactionEntry{Provider: "kbcbrussels", AccountSlug: "kbc", Currency: "EUR", Type: "CREDIT", Amount: 2.5, GrossAmount: 2.5,
		Metadata: map[string]interface{}{"fullDescription": "Meunier-Patin Instant credit transfer Coffee 14.39"}}
	if got := find(small); got != "drinks" {
		t.Errorf("small coffee payment → %q", got)
	}
	big := small
	big.Amount, big.GrossAmount = 746, 746
	big.Metadata = map[string]interface{}{"fullDescription": "Jura E4 coffee machine"}
	if got := find(big); got != "" {
		t.Errorf("coffee machine refund must not be drinks: %q", got)
	}
}

func TestUncategorisedHubTransactions(t *testing.T) {
	dir := t.TempDir()
	f := TransactionsFile{Transactions: []TransactionEntry{
		{ID: "a", Currency: "EUR", Amount: 2.5, Timestamp: 1, Collective: "commonshub"},
		{ID: "b", Currency: "EUR", Amount: 4.5, Collective: "commonshub", Category: "drinks"},
		{ID: "c", Currency: "EURe", Amount: 5, Type: "INTERNAL", Collective: "commonshub"},
		{ID: "d", Currency: "CHT", Amount: 5, Collective: "commonshub"},
		{ID: "e", Currency: "EURe", Amount: 5, Collective: "idg"},
		{ID: "f", Currency: "EURe", Amount: 1, Collective: "commonshub", Metadata: map[string]interface{}{"excluded": "test"}},
	}}
	p := audiencePath(dir, "2026", "10", AudienceStewards, "transactions.json")
	_ = os.MkdirAll(filepath.Dir(p), 0o700)
	data, _ := json.Marshal(f)
	_ = os.WriteFile(p, data, 0o600)
	got := uncategorisedHubTransactions(dir, "2026", "10")
	if len(got) != 1 || got[0].ID != "a" {
		t.Errorf("got %+v", got)
	}
	if s := checkUncategorisedHubTransactions(dir, time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)); !strings.HasPrefix(s, "1 uncategorised") {
		t.Errorf("summary %q", s)
	}
}
