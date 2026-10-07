package cmd

import (
	"encoding/json"
	"os"
	"testing"
)

func TestCategoryTaxonomyPrefixes(t *testing.T) {
	data, err := os.ReadFile("defaults/categories.json")
	if err != nil {
		t.Fatal(err)
	}
	var cats []CategoryDef
	if err := json.Unmarshal(data, &cats); err != nil {
		t.Fatal(err)
	}
	seen := map[string]string{}
	for _, c := range cats {
		if c.Direction != "income" && c.Direction != "expense" && c.Direction != "both" {
			t.Errorf("%s: direction %q", c.Slug, c.Direction)
		}
		for _, a := range c.Accounts {
			if prev, ok := seen[a]; ok {
				t.Errorf("prefix %s maps to %s and %s", a, prev, c.Slug)
			}
			seen[a] = c.Slug
		}
	}
	p := categoryPrefixes(cats)
	for code, want := range map[string]string{
		"580000": "internal_transfer", "451200": "vat", "455000": "HR", "620200": "HR",
		"613105": "consulting", "613200": "accounting", "610100": "rent", "604200": "catering",
		"700100": "rental", "740041": "subsidy", "740040": "donation", "700000": "other-income", "704200": "membership",
		"616040": "webservice", "657020": "stripe_fee", "611010": "supplies", "611000": "maintenance",
		"644000": "other-expense", "749000": "other-income", "499000": "", "440000": "", "489302": "",
	} {
		if got := categoryForAccountCode(p, code); got != want {
			t.Errorf("%s → %q, want %q", code, got, want)
		}
	}
}

func TestOdooTxCategorizer(t *testing.T) {
	cats := []CategoryDef{
		{Slug: "internal_transfer", Accounts: []string{"58"}},
		{Slug: "vat", Accounts: []string{"451"}},
		{Slug: "consulting", Accounts: []string{"613105"}},
		{Slug: "catering", Accounts: []string{"604200"}},
		{Slug: "subsidy", Accounts: []string{"740041"}},
	}
	o := &odooTxCategorizer{
		lineByID: map[int]OdooCacheLine{
			1: {ID: 1, MoveID: 11, AccountID: 580, CounterpartType: "asset_current"},
			2: {ID: 2, MoveID: 12, AccountID: 451, CounterpartType: "liability_current"},
			3: {ID: 3, MoveID: 13, AccountID: 440, CounterpartType: "liability_payable"},
			4: {ID: 4, MoveID: 14, AccountID: 499, CounterpartType: "asset_current"},
			5: {ID: 5, MoveID: 15, AccountID: 440, CounterpartType: "liability_payable"},
		},
		lineByImport: map[string]OdooCacheLine{},
		codeByID:     map[int]string{580: "580000", 451: "451200", 440: "440000", 499: "499000"},
		matches:      map[int][]int{3: {900, 901}, 5: {902}},
		// 902 is not a cached bill: its booking lines come from the matches file.
		moveAccounts: map[int]map[string]float64{902: {"740041": 6036.71}},
		docs: map[int]OdooOutgoingInvoice{
			900: {ID: 900, LineItems: []OdooInvoiceLineItem{{AccountCode: "613105", SubtotalAmount: 1000}, {AccountCode: "604200", SubtotalAmount: 50}}},
			901: {ID: 901, LineItems: []OdooInvoiceLineItem{{AccountCode: "604200", SubtotalAmount: 200}}},
		},
		accounts: map[string]*AccountConfig{},
		prefixes: categoryPrefixes(cats),
	}
	tx := func(line int, cat string) TransactionEntry {
		return TransactionEntry{Category: cat, Type: "DEBIT", Metadata: map[string]interface{}{"odooLineId": float64(line)}}
	}
	cases := []struct {
		tx       TransactionEntry
		cat, typ string
		docs     int
	}{
		{tx(1, ""), "internal_transfer", "INTERNAL", 0},
		{tx(2, ""), "vat", "DEBIT", 0},
		{tx(3, ""), "consulting", "DEBIT", 2},
		{tx(3, "rent"), "rent", "DEBIT", 2}, // rules win; documents still linked
		{tx(4, ""), "", "DEBIT", 0},         // suspense: no category
		{tx(5, ""), "subsidy", "DEBIT", 1},  // matched entry, not a cached bill
		{tx(99, ""), "", "DEBIT", 0},        // not in Odoo
		{TransactionEntry{Type: "CREDIT", Metadata: map[string]interface{}{"description": "Solde d'ouverture"}}, "opening_balance", "INTERNAL", 0},
	}
	for i, c := range cases {
		x := c.tx
		o.apply(&x)
		docs, _ := x.Metadata["documents"].([]interface{})
		if x.Category != c.cat || x.Type != c.typ || len(docs) != c.docs {
			t.Errorf("case %d: got %q/%s/%d docs, want %q/%s/%d", i, x.Category, x.Type, len(docs), c.cat, c.typ, c.docs)
		}
		if c.cat != "" && c.tx.Category == "" && x.Metadata["categorySource"] != "odoo" {
			t.Errorf("case %d: categorySource not set", i)
		}
	}
}

func TestOdooAccountCodeFromField(t *testing.T) {
	if got := odooAccountCodeFromField([]interface{}{float64(7), "613105 HONORAIRES DIVERS"}); got != "613105" {
		t.Fatalf("got %q", got)
	}
	if got := odooAccountCodeFromField(false); got != "" {
		t.Fatalf("got %q", got)
	}
}

func TestDefaultReferenceRules(t *testing.T) {
	data, err := os.ReadFile("defaults/rules.json")
	if err != nil {
		t.Fatal(err)
	}
	var rules []Rule
	if err := json.Unmarshal(data, &rules); err != nil {
		t.Fatal(err)
	}
	c := &Categorizer{rules: rules}
	for _, tc := range []struct {
		tx   TransactionEntry
		want string
	}{
		{TransactionEntry{Type: "BURN", Amount: -8152.21, Counterparty: "FPS Finance - VAT Revenue Service Brussels", Metadata: map[string]interface{}{"memo": "+++080/4505/13233+++"}}, "vat"},
		{TransactionEntry{Type: "BURN", Amount: -12603.84, Counterparty: "XL Collective SRL", Metadata: map[string]interface{}{"memo": "Taxe communale 2025 CHB"}}, "taxes"},
		{TransactionEntry{Type: "BURN", Amount: -907.5, Metadata: map[string]interface{}{"memo": "Refund Summerschool September 2025 8700186"}}, "refund"},
		{TransactionEntry{Type: "BURN", Amount: -10989.84, Metadata: map[string]interface{}{"description": "Internal transfer from EOA to multisig"}}, "internal_transfer"},
		{TransactionEntry{Type: "DEBIT", Amount: -1, Counterparty: "e-invoice.be", Metadata: map[string]interface{}{"description": "***080/4505/13233***"}}, ""},
	} {
		tx := tc.tx
		c.Apply(&tx)
		if tx.Category != tc.want {
			t.Errorf("%v: got %q, want %q", tc.tx.Metadata, tx.Category, tc.want)
		}
	}
}
