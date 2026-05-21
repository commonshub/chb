package cmd

import (
	"encoding/json"
	"testing"
)

func TestParseOdooCreatedIDs(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want []int
	}{
		{name: "single", raw: `42`, want: []int{42}},
		{name: "array", raw: `[4, 8]`, want: []int{4, 8}},
		{name: "empty", raw: `false`, want: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseOdooCreatedIDs(json.RawMessage(tt.raw))
			if len(got) != len(tt.want) {
				t.Fatalf("len=%d want %d (%v)", len(got), len(tt.want), got)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("got %v want %v", got, tt.want)
				}
			}
		})
	}
}

func TestTransactionBankAccountNumber(t *testing.T) {
	tx := TransactionEntry{Metadata: map[string]interface{}{"counterparty_iban": " BE12 3456 7890 1234 "}}
	if got := transactionBankAccountNumber(tx); got != "BE12 3456 7890 1234" {
		t.Fatalf("got %q", got)
	}
	if got := normalizeBankAccountNumber("be12 3456-7890.1234"); got != "BE12345678901234" {
		t.Fatalf("normalized %q", got)
	}
}

func TestCryptoBankAccountNumber(t *testing.T) {
	address := "0x6fDF0AaE33E313d9C98D2Aa19Bcd8EF777912CBf"
	if !isEVMAddress(address) {
		t.Fatalf("expected address to be valid")
	}
	got := cryptoBankAccountNumber("Gnosis", address)
	want := "gnosis:0x6fdf0aae33e313d9c98d2aa19bcd8ef777912cbf"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
	if normalized := normalizeBankAccountNumber(got); normalized != "GNOSIS0X6FDF0AAE33E313D9C98D2AA19BCD8EF777912CBF" {
		t.Fatalf("normalized %q", normalized)
	}
}

func TestCryptoCounterpartyNameForMinter(t *testing.T) {
	tx := TransactionEntry{
		Currency:     "EURe",
		Counterparty: "0x0000000000000000000000000000000000000000",
	}
	if got := cryptoCounterpartyName(tx, "gnosis", normalizeEVMAddress(tx.Counterparty)); got != "gnosis/EURe Minter" {
		t.Fatalf("got %q", got)
	}
}

func TestSignedOdooAmountForInternalTransactionDirection(t *testing.T) {
	acc := &AccountConfig{Slug: "checking"}
	debit := TransactionEntry{
		Type:             "INTERNAL",
		NormalizedAmount: 12.5,
		Metadata:         map[string]interface{}{"direction": "DEBIT"},
	}
	if got := signedOdooAmountForTransaction(acc, debit); got != -12.5 {
		t.Fatalf("debit got %v", got)
	}
	credit := TransactionEntry{
		Type:             "INTERNAL",
		NormalizedAmount: 12.5,
		Metadata:         map[string]interface{}{"direction": "CREDIT"},
	}
	if got := signedOdooAmountForTransaction(acc, credit); got != 12.5 {
		t.Fatalf("credit got %v", got)
	}
}

func TestOdooStatementLineCounterpartyFallbacks(t *testing.T) {
	tests := []struct {
		name string
		line odooStatementLineForReconcile
		want string
	}{
		{
			name: "partner name",
			line: odooStatementLineForReconcile{PartnerName: "Alice"},
			want: "Alice",
		},
		{
			name: "payment ref",
			line: odooStatementLineForReconcile{PaymentRef: "Stripe charge"},
			want: "Stripe charge",
		},
		{
			name: "import id",
			line: odooStatementLineForReconcile{UniqueImportID: "ethereum:tx:1"},
			want: "ethereum:tx:1",
		},
		{
			name: "line id",
			line: odooStatementLineForReconcile{ID: 42},
			want: "line #42",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := odooStatementLineCounterparty(tt.line); got != tt.want {
				t.Fatalf("got %q want %q", got, tt.want)
			}
		})
	}
}

func TestExtractOdooInvoiceReferencesFromStatementLine(t *testing.T) {
	line := odooStatementLineForReconcile{
		PaymentRef: "Stripe payment for MEM/2026/00032",
		Narration:  `{"invoice":"mem/2026/00032","other":"INV/2026/001"}`,
	}
	got := extractOdooInvoiceReferencesFromStatementLine(line)
	want := []string{"MEM/2026/00032", "INV/2026/001"}
	if len(got) != len(want) {
		t.Fatalf("refs = %#v, want %#v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("refs = %#v, want %#v", got, want)
		}
	}
}
