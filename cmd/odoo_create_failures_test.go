package cmd

import (
	"errors"
	"testing"
)

func TestClassifyStatementLineCreateFailureDuplicateReference(t *testing.T) {
	line := map[string]interface{}{"unique_import_id": "stripe:acct:txn_123"}
	failure := classifyStatementLineCreateFailure(line, errors.New("odoo error: This unique transaction reference can be imported only once"))

	if failure.ImportID != "stripe:acct:txn_123" {
		t.Fatalf("ImportID = %q", failure.ImportID)
	}
	if failure.Reason != "reference already exists in Odoo" {
		t.Fatalf("Reason = %q", failure.Reason)
	}
}

func TestHandleStatementLineCrossJournalConflictsIgnoresNonConflictFailures(t *testing.T) {
	// Failures without a populated ConflictJournalID (e.g. permission
	// errors, validation failures) must not trigger the interactive
	// conflict-resolution flow.
	err := handleStatementLineCrossJournalConflicts(nil, 0, 48, []statementLineCreateFailure{
		{ImportID: "a", Reason: "missing required Odoo field"},
		{ImportID: "b", Reason: "Odoo permission error"},
	})
	if err != nil {
		t.Fatalf("expected nil error for non-conflict failures, got: %v", err)
	}
}

func TestHandleStatementLineCrossJournalConflictsIgnoresSameJournalConflicts(t *testing.T) {
	// A "conflict" within the same journal we're syncing into is not a
	// cross-journal collision — those get handled by the duplicate-update
	// path in syncStripeChronological.
	err := handleStatementLineCrossJournalConflicts(nil, 0, 48, []statementLineCreateFailure{
		{ImportID: "a", Reason: "reference already exists in journal #48", ConflictJournalID: 48},
	})
	if err != nil {
		t.Fatalf("expected nil error for same-journal conflict, got: %v", err)
	}
}

func TestSyncStatsRecordCreateFailures(t *testing.T) {
	stats := &syncStats{}
	stats.recordCreateFailures([]statementLineCreateFailure{
		{ImportID: "a", Reason: "reference already exists in journal #30"},
		{ImportID: "b", Reason: "reference already exists in journal #30"},
		{ImportID: "c", Reason: "missing required Odoo field"},
	})

	if stats.LinesFailed != 3 {
		t.Fatalf("LinesFailed = %d, want 3", stats.LinesFailed)
	}
	if got := stats.CreateFailures["reference already exists in journal #30"]; got != 2 {
		t.Fatalf("duplicate reason count = %d, want 2", got)
	}
	if got := stats.CreateFailures["missing required Odoo field"]; got != 1 {
		t.Fatalf("missing field reason count = %d, want 1", got)
	}
	if len(stats.CreateDetails) != 3 {
		t.Fatalf("CreateDetails len = %d, want 3", len(stats.CreateDetails))
	}
}
