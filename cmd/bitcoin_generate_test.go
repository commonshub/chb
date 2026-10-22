package cmd

import (
	"encoding/json"
	"testing"

	bitcoinsource "github.com/CommonsHub/chb/providers/bitcoin"
	coinossource "github.com/CommonsHub/chb/providers/coinos"
)

func TestBitcoinTxToEntry(t *testing.T) {
	var tx bitcoinsource.Tx
	json.Unmarshal([]byte(`{"txid":"ABC","fee":200,
		"vin":[{"prevout":{"scriptpubkey_address":"bc1qours","value":3000}}],
		"vout":[{"scriptpubkey_address":"bc1qdest","value":1000},{"scriptpubkey_address":"bc1qours","value":1800}],
		"status":{"confirmed":true,"block_height":5,"block_time":1760000000}}`), &tx)
	acc := AccountConfig{Slug: "bitcoin", Name: "BTC", Provider: "bitcoin", Address: "bc1qours"}
	e, ok := bitcoinTxToEntry(acc, tx)
	if !ok {
		t.Fatal("expected entry")
	}
	if e.ID != "bitcoin:tx:abc" || e.AccountID != "bitcoin:address:bc1qours" || e.CounterpartyID != "bitcoin:address:bc1qdest" {
		t.Fatalf("ids: %+v", e)
	}
	if e.Type != "DEBIT" || e.Amount != -0.000012 || e.Value != "-0.00001200" || e.Fee != 0.000002 || e.Currency != "BTC" {
		t.Fatalf("amounts: %+v", e)
	}
}

func TestCoinosPaymentToEntryAndInternal(t *testing.T) {
	acc := AccountConfig{Slug: "coinos", Name: "coinos", Provider: "coinos", AccountID: "commonshub"}
	out := coinosPaymentToEntry(acc, coinossource.Payment{ID: "p1", Amount: -1000, Fee: 100, OurFee: 10, Hash: "abc", Type: "bitcoin", Rate: 100000, Currency: "EUR", Created: 1760000000000})
	if out.Type != "DEBIT" || out.Amount != -0.0000111 || out.Timestamp != 1760000000 || out.AccountID != "coinos:user:commonshub" {
		t.Fatalf("entry: %+v", out)
	}
	if out.Metadata["fiatAmount"] != -1.11 {
		t.Fatalf("fiat: %v", out.Metadata["fiatAmount"])
	}
	in := TransactionEntry{Provider: "bitcoin", TxHash: "abc", Type: "CREDIT"}
	txs := []TransactionEntry{out, in}
	markBitcoinInternalTransfers(txs)
	if txs[0].Type != "INTERNAL" || txs[1].Type != "INTERNAL" || txs[1].Metadata["direction"] != "CREDIT" {
		t.Fatalf("internal: %+v", txs)
	}
}
