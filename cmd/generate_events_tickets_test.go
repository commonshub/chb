package cmd

import "testing"

func TestSummariseEventTxGrossAndRefunds(t *testing.T) {
	txs := []TransactionEntry{
		{ID: "a", Type: "CREDIT", NormalizedAmount: 10, Currency: "EUR", Timestamp: 100, Provider: "stripe"},
		{ID: "b", Type: "CREDIT", NormalizedAmount: 15, Currency: "EUR", Timestamp: 200, Provider: "stripe"},
		{ID: "c", Type: "DEBIT", NormalizedAmount: -5, Currency: "EUR", Timestamp: 300, Provider: "stripe"},
		{ID: "d", Type: "CREDIT", NormalizedAmount: 20, Currency: "USD", Timestamp: 400, Provider: "stripe"},
	}
	s := summariseEventTx(txs)
	if s == nil {
		t.Fatal("summary should not be nil")
	}
	if s.TxCount != 4 {
		t.Errorf("TxCount = %d; want 4", s.TxCount)
	}
	if s.RefundCount != 1 {
		t.Errorf("RefundCount = %d; want 1", s.RefundCount)
	}
	if s.Gross["EUR"] != 25 {
		t.Errorf("Gross EUR = %v; want 25", s.Gross["EUR"])
	}
	if s.Net["EUR"] != 20 {
		t.Errorf("Net EUR = %v; want 20 (25 - 5 refund)", s.Net["EUR"])
	}
	if s.Gross["USD"] != 20 || s.Net["USD"] != 20 {
		t.Errorf("USD gross/net = %v/%v; want 20/20", s.Gross["USD"], s.Net["USD"])
	}
	if len(s.Transactions) != 4 {
		t.Errorf("Transactions len = %d; want 4", len(s.Transactions))
	}
}

func TestSummariseEventTxEmpty(t *testing.T) {
	if s := summariseEventTx(nil); s != nil {
		t.Errorf("expected nil for empty input, got %+v", s)
	}
}

func TestEventTicketSalesEqual(t *testing.T) {
	a := summariseEventTx([]TransactionEntry{{ID: "a", Type: "CREDIT", NormalizedAmount: 10, Currency: "EUR", Timestamp: 1}})
	b := summariseEventTx([]TransactionEntry{{ID: "a", Type: "CREDIT", NormalizedAmount: 10, Currency: "EUR", Timestamp: 1}})
	if !eventTicketSalesEqual(a, b) {
		t.Error("identical summaries should compare equal")
	}
	c := summariseEventTx([]TransactionEntry{{ID: "a", Type: "CREDIT", NormalizedAmount: 11, Currency: "EUR", Timestamp: 1}})
	if eventTicketSalesEqual(a, c) {
		t.Error("different amounts should not compare equal")
	}
	if !eventTicketSalesEqual(nil, nil) {
		t.Error("nil/nil should compare equal")
	}
	if eventTicketSalesEqual(a, nil) {
		t.Error("non-nil vs nil should not compare equal")
	}
}
