package cmd

import "testing"

func TestBuildOdooLineSyncUpdateUsesEnrichedCounterpartyAndNarration(t *testing.T) {
	chain := "gnosis"
	acc := &AccountConfig{Provider: "etherscan"}
	tx := TransactionEntry{
		Provider:     "etherscan",
		TxHash:       "0xabc",
		Chain:        &chain,
		Currency:     "EURe",
		Counterparty: "FONDATION MYCELIUM",
		Metadata: map[string]interface{}{
			"iban":          "BE41523081095210",
			"moneriumKind":  "issue",
			"moneriumState": "processed",
		},
	}
	row := map[string]interface{}{
		"payment_ref":      "0x0000000000000000000000000000000000000000",
		"narration":        "",
		"partner_id":       false,
		"partner_bank_id":  false,
		"unique_import_id": "gnosis:wallet:0xabc:0",
	}

	update := buildOdooLineSyncUpdate(acc, tx, row, 12, 34)

	if update["payment_ref"] != "FONDATION MYCELIUM" {
		t.Fatalf("payment_ref update = %#v, want enriched counterparty", update["payment_ref"])
	}
	if update["partner_id"] != 34 {
		t.Fatalf("partner_id update = %#v, want 34", update["partner_id"])
	}
	if update["partner_bank_id"] != 12 {
		t.Fatalf("partner_bank_id update = %#v, want 12", update["partner_bank_id"])
	}
	if update["narration"] == "" {
		t.Fatal("expected narration update")
	}
}

func TestBuildOdooLineSyncUpdateLeavesMatchingLineAlone(t *testing.T) {
	acc := &AccountConfig{Provider: "etherscan"}
	tx := TransactionEntry{Provider: "etherscan", Counterparty: "FONDATION MYCELIUM"}
	row := map[string]interface{}{
		"payment_ref":     "FONDATION MYCELIUM",
		"narration":       buildOdooNarration(acc, tx),
		"partner_id":      []interface{}{float64(34), "FONDATION MYCELIUM"},
		"partner_bank_id": []interface{}{float64(12), "BE41523081095210"},
	}

	update := buildOdooLineSyncUpdate(acc, tx, row, 12, 34)

	if len(update) != 0 {
		t.Fatalf("update = %#v, want no changes", update)
	}
}

func TestBuildMoneriumLineSyncUpdateOnlyRepairsZeroAddressPlaceholders(t *testing.T) {
	acc := &AccountConfig{Provider: "etherscan"}
	tx := TransactionEntry{
		Provider:     "etherscan",
		Counterparty: "FONDATION MYCELIUM",
		Tags:         [][]string{{"source", "monerium"}},
		Metadata:     map[string]interface{}{"moneriumKind": "issue"},
	}
	row := map[string]interface{}{
		"payment_ref": "EURe from 0x0000...0000",
		"narration":   "",
	}

	update := buildMoneriumLineSyncUpdate(acc, tx, row)

	if update["payment_ref"] != "FONDATION MYCELIUM" {
		t.Fatalf("payment_ref update = %#v, want enriched counterparty", update["payment_ref"])
	}
	if update["narration"] == "" {
		t.Fatal("expected narration update")
	}

	row["payment_ref"] = "FONDATION MYCELIUM"
	if update := buildMoneriumLineSyncUpdate(acc, tx, row); len(update) != 0 {
		t.Fatalf("matching row update = %#v, want no changes", update)
	}

	tx.Tags = nil
	tx.Metadata = nil
	row["payment_ref"] = "EURe from 0x0000...0000"
	if update := buildMoneriumLineSyncUpdate(acc, tx, row); len(update) != 0 {
		t.Fatalf("non-monerium update = %#v, want no changes", update)
	}
}

// TestBuildMoneriumLineSyncUpdateRefreshesCounterpartyAsPaymentRef pins
// the bug fixed in the v3.4.6 patch: Monerium lines uploaded before the
// memo enrichment ran end up with payment_ref equal to the counterparty
// name (e.g. "INFLIGHTS BV"). The refresh pass must replace that with
// the enriched description (e.g. "MEM/2026/00036") so the reconcile
// matcher's reference strategy can fire.
func TestBuildMoneriumLineSyncUpdateRefreshesCounterpartyAsPaymentRef(t *testing.T) {
	acc := &AccountConfig{Provider: "etherscan"}
	tx := TransactionEntry{
		Provider:     "etherscan",
		Counterparty: "INFLIGHTS BV",
		Tags:         [][]string{{"source", "monerium"}},
		Metadata: map[string]interface{}{
			"moneriumKind": "issue",
			"description":  "MEM/2026/00036",
		},
	}
	row := map[string]interface{}{
		"payment_ref": "INFLIGHTS BV", // legacy upload before memo enrichment
		"narration":   "",
	}

	update := buildMoneriumLineSyncUpdate(acc, tx, row)
	if update["payment_ref"] != "MEM/2026/00036" {
		t.Fatalf("payment_ref update = %#v, want %q", update["payment_ref"], "MEM/2026/00036")
	}

	// Whitespace + case differences between counterparty and current
	// payment_ref shouldn't block the refresh either.
	row["payment_ref"] = "  inflights bv "
	update = buildMoneriumLineSyncUpdate(acc, tx, row)
	if update["payment_ref"] != "MEM/2026/00036" {
		t.Fatalf("case-insensitive refresh = %#v, want %q", update["payment_ref"], "MEM/2026/00036")
	}

	// But a payment_ref that's neither zero-address, empty, nor the
	// counterparty (i.e. a manually-set value) MUST be left alone —
	// otherwise the refresh pass would clobber operator edits.
	row["payment_ref"] = "manual: reimbursement Q3"
	if update := buildMoneriumLineSyncUpdate(acc, tx, row); len(update) != 0 {
		t.Fatalf("manual payment_ref update = %#v, want no changes", update)
	}

	// A payment_ref that already matches the target value is a no-op.
	row["payment_ref"] = "MEM/2026/00036"
	if update := buildMoneriumLineSyncUpdate(acc, tx, row); len(update) != 0 {
		t.Fatalf("already-matching update = %#v, want no changes", update)
	}
}

func TestOdooStatementLineMetadataWriteContextSkipsMoveSynchronization(t *testing.T) {
	ctx := odooStatementLineMetadataWriteContext()
	raw, ok := ctx["context"].(map[string]interface{})
	if !ok {
		t.Fatalf("context = %#v, want map", ctx["context"])
	}
	if raw["skip_account_move_synchronization"] != true {
		t.Fatalf("skip_account_move_synchronization = %#v, want true", raw["skip_account_move_synchronization"])
	}
	if raw["check_move_validity"] != false {
		t.Fatalf("check_move_validity = %#v, want false", raw["check_move_validity"])
	}
}
