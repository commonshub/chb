package cmd

import "testing"

func bridgeTx(id, account, typ string, amount float64, ts int64, desc string) TransactionEntry {
	tx := TransactionEntry{
		ID: id, Provider: "etherscan", AccountSlug: account, Currency: "EURe",
		Type: typ, Amount: amount, GrossAmount: amount, Timestamp: ts,
		Metadata: map[string]interface{}{},
	}
	if amount < 0 {
		tx.GrossAmount = -amount
	}
	if desc != "" {
		tx.Metadata["description"] = desc
	}
	return tx
}

func TestPairBridgeTransfersTagsBothLegs(t *testing.T) {
	txs := []TransactionEntry{
		bridgeTx("burn", "eoa-polygon", "BURN", -2500, 1000, "Financial contribution Alain Boribon"),
		bridgeTx("mint", "savings-gnosis", "MINT", 2500, 1008, "Financial contribution Alain Boribon"),
	}
	txs[1].Category = "loan" // a rule read the memo as income

	if n := pairBridgeTransfers(txs, nil); n != 1 {
		t.Fatalf("pairs = %d, want 1", n)
	}
	for _, tx := range txs {
		if tx.Category != "internal_transfer" || tx.Type != "INTERNAL" {
			t.Errorf("%s: category %q type %q, want internal_transfer INTERNAL", tx.ID, tx.Category, tx.Type)
		}
		if _, ok := tx.Metadata["vatAmount"]; ok {
			t.Errorf("%s: vatAmount left on an internal transfer", tx.ID)
		}
	}
	if txs[0].Metadata["bridgePair"] != "mint" || txs[1].Metadata["bridgePair"] != "burn" {
		t.Errorf("bridgePair links = %v / %v", txs[0].Metadata["bridgePair"], txs[1].Metadata["bridgePair"])
	}
}

func TestPairBridgeTransfersRejects(t *testing.T) {
	const day = 24 * 3600
	cases := []struct {
		name string
		burn TransactionEntry
		mint TransactionEntry
	}{
		{"same account", bridgeTx("b", "a", "BURN", -10, 0, "memo x"), bridgeTx("m", "a", "MINT", 10, 1, "memo x")},
		{"other amount", bridgeTx("b", "a", "BURN", -10, 0, "memo x"), bridgeTx("m", "b", "MINT", 10.01, 1, "memo x")},
		{"too late", bridgeTx("b", "a", "BURN", -10, 0, "memo x"), bridgeTx("m", "b", "MINT", 10, 3*day, "memo x")},
		{"other memo", bridgeTx("b", "a", "BURN", -10, 0, "memo x"), bridgeTx("m", "b", "MINT", 10, 1, "memo y")},
		{"no memo", bridgeTx("b", "a", "BURN", -10, 0, ""), bridgeTx("m", "b", "MINT", 10, 1, "")},
		{"plain transfers", bridgeTx("b", "a", "DEBIT", -10, 0, "memo x"), bridgeTx("m", "b", "CREDIT", 10, 1, "memo x")},
	}
	for _, c := range cases {
		txs := []TransactionEntry{c.burn, c.mint}
		if n := pairBridgeTransfers(txs, nil); n != 0 {
			t.Errorf("%s: paired %d, want 0", c.name, n)
		}
	}
}

func TestPairBridgeTransfersSkipsAnnotated(t *testing.T) {
	txs := []TransactionEntry{
		bridgeTx("burn", "a", "BURN", -10, 0, "memo x"),
		bridgeTx("mint", "b", "MINT", 10, 1, "memo x"),
	}
	txs[1].Category = "donation"
	if n := pairBridgeTransfers(txs, map[int]bool{1: true}); n != 0 {
		t.Fatalf("paired %d, want 0: the annotated leg must stay as annotated", n)
	}
	if txs[1].Category != "donation" {
		t.Errorf("annotated category changed to %q", txs[1].Category)
	}
}

func TestPairBridgeTransfersUsesEachMintOnce(t *testing.T) {
	txs := []TransactionEntry{
		bridgeTx("b1", "a", "BURN", -10, 0, "memo x"),
		bridgeTx("b2", "a", "BURN", -10, 5, "memo x"),
		bridgeTx("m1", "b", "MINT", 10, 10, "memo x"),
	}
	if n := pairBridgeTransfers(txs, nil); n != 1 {
		t.Fatalf("paired %d, want 1", n)
	}
	if txs[0].Category != "internal_transfer" || txs[1].Category != "" {
		t.Errorf("want the earliest burn paired: b1=%q b2=%q", txs[0].Category, txs[1].Category)
	}
}

func TestRuleMatchType(t *testing.T) {
	r := Rule{Match: RuleMatch{Type: "mint", Direction: "in"}, Assign: RuleAssign{Category: "loan"}}
	if !r.MatchesTransaction(TransactionEntry{Type: "MINT", Amount: 5}) {
		t.Error("type mint should match a MINT (case-insensitive)")
	}
	if r.MatchesTransaction(TransactionEntry{Type: "CREDIT", Amount: 5}) {
		t.Error("type mint must not match a CREDIT")
	}
}
