package etherscan

import (
	"math/big"
	"testing"
)

func TestAssignLogIndexesAndMerge(t *testing.T) {
	fridge := "0x0492b5c6bd2564826ead6eeec9e1a9756603825e"
	zero := "0x0000000000000000000000000000000000000000"
	// 0xa062…: three mints 9.83, 3.93, 9.83 (logs 2, 6, 10).
	txs := []TokenTransfer{
		{Hash: "0xa062", From: zero, To: fridge, Value: "9830000", TimeStamp: "1"},
		{Hash: "0xa062", From: zero, To: fridge, Value: "3930000", TimeStamp: "1"},
		{Hash: "0xa062", From: zero, To: fridge, Value: "9830000", TimeStamp: "1"},
		{Hash: "0xsingle", From: zero, To: fridge, Value: "1", TimeStamp: "2"},
	}
	receipt := []ReceiptTransfer{
		{LogIndex: 2, From: zero, To: fridge, Value: big.NewInt(9830000)},
		{LogIndex: 6, From: zero, To: fridge, Value: big.NewInt(3930000)},
		{LogIndex: 10, From: zero, To: fridge, Value: big.NewInt(9830000)},
	}
	calls := 0
	old := FetchReceiptTransfers
	FetchReceiptTransfers = func(rpc, h string) ([]ReceiptTransfer, error) { calls++; return receipt, nil }
	defer func() { FetchReceiptTransfers = old }()
	n, err := EnrichLogIndexes(txs, "rpc")
	if err != nil || n != 3 || calls != 1 {
		t.Fatalf("enrich: n=%d calls=%d err=%v (single-transfer txs need no receipt)", n, calls, err)
	}
	got := []int{*txs[0].LogIndex, *txs[1].LogIndex, *txs[2].LogIndex}
	if got[0] != 2 || got[1] != 6 || got[2] != 10 || txs[3].LogIndex != nil {
		t.Fatalf("log indexes %v", got)
	}
	// An incremental fetch (no log indexes) must not duplicate or drop the
	// two identical 9.83 mints.
	fetched := []TokenTransfer{txs[0], txs[1], txs[2]}
	for i := range fetched {
		fetched[i].LogIndex = nil
	}
	merged := MergeTokenTransfers(txs, fetched)
	if len(merged) != 4 {
		t.Fatalf("merge kept %d transfers, want 4", len(merged))
	}
	withIdx := 0
	for _, m := range merged {
		if m.LogIndex != nil {
			withIdx++
		}
	}
	if withIdx != 3 {
		t.Errorf("merge must keep the enriched copies: %d with index", withIdx)
	}
}
