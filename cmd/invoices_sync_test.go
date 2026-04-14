package cmd

import (
	"encoding/json"
	"testing"
)

func TestExtractTxHash(t *testing.T) {
	hash := "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
	cases := []string{
		hash,
		"gnosis:wallet:" + hash + ":0",
		"payment ref " + hash,
	}
	for _, input := range cases {
		if got := extractTxHash(input); got != hash {
			t.Fatalf("extractTxHash(%q) = %q, want %q", input, got, hash)
		}
	}
}

func TestInferTxProvider(t *testing.T) {
	if got := inferTxProvider("ch_123"); got != "stripe" {
		t.Fatalf("inferTxProvider(stripe ref) = %q", got)
	}
	if got := inferTxProvider("gnosis:wallet:0xabc:0"); got != "gnosis" {
		t.Fatalf("inferTxProvider(gnosis ref) = %q", got)
	}
}

func TestBuildPublicInvoicesIncludesStatuses(t *testing.T) {
	invoices := buildPublicInvoices([]OdooOutgoingInvoice{
		{
			ID:           42,
			Title:        "Invoice 42",
			State:        "posted",
			PaymentState: "paid",
			InvoiceDate:  "2026-04-01",
			TotalAmount:  120,
		},
	})

	if len(invoices) != 1 {
		t.Fatalf("expected 1 invoice, got %d", len(invoices))
	}
	if invoices[0].State != "posted" {
		t.Fatalf("expected state to be preserved, got %q", invoices[0].State)
	}
	if invoices[0].PaymentState != "paid" {
		t.Fatalf("expected payment state to be preserved, got %q", invoices[0].PaymentState)
	}
}

func TestLoadCachedInvoiceMonthReadsStatusesFromPublicFile(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	publicFile := OdooOutgoingInvoicesFile{
		SchemaVersion: odooDocumentsSchemaVersion,
		Year:          "2026",
		Month:         "04",
		Source:        "odoo",
		Count:         1,
		FetchedAt:     "2026-04-14T10:00:00Z",
		Invoices: []OdooOutgoingInvoicePublic{
			{
				ID:           42,
				Title:        "Invoice 42",
				State:        "posted",
				PaymentState: "paid",
				Date:         "2026-04-01",
			},
		},
	}

	data, err := json.Marshal(publicFile)
	if err != nil {
		t.Fatalf("marshal public file: %v", err)
	}
	if err := writeTestFile(DataDir()+"/2026/04/finance/odoo", "invoices.json", data); err != nil {
		t.Fatalf("write invoices.json: %v", err)
	}

	loaded := loadCachedInvoiceMonth(DataDir(), "2026", "04")
	if len(loaded) != 1 {
		t.Fatalf("expected 1 loaded invoice, got %d", len(loaded))
	}
	if loaded[0].State != "posted" {
		t.Fatalf("expected state from public cache, got %q", loaded[0].State)
	}
	if loaded[0].PaymentState != "paid" {
		t.Fatalf("expected payment state from public cache, got %q", loaded[0].PaymentState)
	}
}
