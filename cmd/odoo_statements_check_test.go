package cmd

import (
	"encoding/json"
	"testing"
)

func TestOdooFloatUnmarshalFalseAndNull(t *testing.T) {
	var row struct {
		Amount odooJSONFloat `json:"amount"`
	}
	for _, input := range []string{`{"amount":false}`, `{"amount":null}`} {
		if err := json.Unmarshal([]byte(input), &row); err != nil {
			t.Fatalf("unmarshal %s: %v", input, err)
		}
		if got := row.Amount.Float64(); got != 0 {
			t.Fatalf("%s decoded to %v, want 0", input, got)
		}
	}
}

func TestOdooFloatUnmarshalNumber(t *testing.T) {
	var row struct {
		Amount odooJSONFloat `json:"amount"`
	}
	if err := json.Unmarshal([]byte(`{"amount":123.45}`), &row); err != nil {
		t.Fatalf("unmarshal number: %v", err)
	}
	if got := row.Amount.Float64(); got != 123.45 {
		t.Fatalf("decoded to %v, want 123.45", got)
	}
}
