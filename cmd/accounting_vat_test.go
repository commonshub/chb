package cmd

import (
	"math"
	"testing"
)

func TestStampVATOnIncomingDefaultsTo21Percent(t *testing.T) {
	tx := TransactionEntry{
		Type:        "CREDIT",
		Currency:    "EUR",
		GrossAmount: 1210,
		Category:    "ticket",
	}
	stampVAT(&tx)
	got := floatMetadata(tx.Metadata, "vatAmount")
	if math.Abs(got-210) > 0.01 {
		t.Errorf("vatAmount: got %v, want 210", got)
	}
}

func TestStampVATSkipsExemptCategories(t *testing.T) {
	for _, cat := range []string{"rent", "donation", "membership"} {
		tx := TransactionEntry{
			Type:        "CREDIT",
			Currency:    "EUR",
			GrossAmount: 1000,
			Category:    cat,
		}
		stampVAT(&tx)
		if v := floatMetadata(tx.Metadata, "vatAmount"); v != 0 {
			t.Errorf("category %q: expected no vatAmount, got %v", cat, v)
		}
	}
}

func TestStampVATSkipsOutgoing(t *testing.T) {
	tx := TransactionEntry{
		Type:        "DEBIT",
		Currency:    "EUR",
		GrossAmount: 1000,
		Category:    "supplies",
	}
	stampVAT(&tx)
	if v := floatMetadata(tx.Metadata, "vatAmount"); v != 0 {
		t.Errorf("outgoing tx should not get vatAmount, got %v", v)
	}
}

func TestStampVATClearsStaleWhenCategoryBecomesExempt(t *testing.T) {
	tx := TransactionEntry{
		Type:        "CREDIT",
		Currency:    "EUR",
		GrossAmount: 1210,
		Category:    "ticket",
		Metadata:    map[string]interface{}{"vatAmount": 210.0},
	}
	tx.Category = "rent"
	stampVAT(&tx)
	if v := floatMetadata(tx.Metadata, "vatAmount"); v != 0 {
		t.Errorf("stale vatAmount should be cleared, got %v", v)
	}
}

func TestStampVATAcceptsEURe(t *testing.T) {
	tx := TransactionEntry{
		Type:        "CREDIT",
		Currency:    "EURe",
		GrossAmount: 121,
		Category:    "ticket",
	}
	stampVAT(&tx)
	got := floatMetadata(tx.Metadata, "vatAmount")
	if math.Abs(got-21) > 0.01 {
		t.Errorf("EURe vatAmount: got %v, want 21", got)
	}
}

func TestStampVATSkipsNonEUR(t *testing.T) {
	tx := TransactionEntry{
		Type:        "CREDIT",
		Currency:    "CHT",
		GrossAmount: 100,
		Category:    "ticket",
	}
	stampVAT(&tx)
	if v := floatMetadata(tx.Metadata, "vatAmount"); v != 0 {
		t.Errorf("non-EUR currency should not get vatAmount, got %v", v)
	}
}
