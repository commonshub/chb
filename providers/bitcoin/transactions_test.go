package bitcoin

import (
	"encoding/json"
	"testing"
)

const addr = "bc1qours"

func parseTx(t *testing.T, s string) Tx {
	t.Helper()
	var tx Tx
	if err := json.Unmarshal([]byte(s), &tx); err != nil {
		t.Fatal(err)
	}
	return tx
}

func TestFlowForReceive(t *testing.T) {
	tx := parseTx(t, `{"txid":"a","fee":150,
		"vin":[{"prevout":{"scriptpubkey_address":"bc1psender","value":10000}}],
		"vout":[{"scriptpubkey_address":"bc1psender","value":6850},{"value":0},{"scriptpubkey_address":"bc1qours","value":3000}],
		"status":{"confirmed":true,"block_time":1760000000}}`)
	f := tx.FlowFor(addr)
	if f.Net() != 3000 || f.FundedInputs || len(f.Inputs) != 1 || f.Inputs[0] != "bc1psender" {
		t.Fatalf("unexpected flow %+v", f)
	}
}

func TestFlowForSpendIncludesFee(t *testing.T) {
	tx := parseTx(t, `{"txid":"b","fee":200,
		"vin":[{"prevout":{"scriptpubkey_address":"bc1qours","value":3000}}],
		"vout":[{"scriptpubkey_address":"bc1qdest","value":1000},{"scriptpubkey_address":"bc1qours","value":1800}],
		"status":{"confirmed":true,"block_time":1760000000}}`)
	f := tx.FlowFor(addr)
	if f.Net() != -1200 || !f.FundedInputs || len(f.Outputs) != 1 || f.Outputs[0] != "bc1qdest" {
		t.Fatalf("unexpected flow %+v", f)
	}
}
