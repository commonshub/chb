package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// TestStripePayoutSync is an integration test that syncs two payouts to an Odoo test database,
// then force-resyncs one without touching the other.
//
// Prerequisites:
//   - ODOO_URL, ODOO_LOGIN, ODOO_PASSWORD set (must be a "test" database)
//   - STRIPE_SECRET_KEY set
//
// Run with: go test -v -run TestStripePayoutSync ./cmd/
func TestStripePayoutSync(t *testing.T) {
	t.Skip("obsolete: per-payout sync removed in favour of chronological BT iteration")

	// Ensure env vars are loaded
	LoadEnvFromConfig()

	creds, err := ResolveOdooCredentials()
	if err != nil {
		t.Skipf("Odoo credentials not configured: %v", err)
	}
	if !strings.Contains(creds.DB, "test") {
		t.Fatalf("Safety check: database name must contain 'test', got %q", creds.DB)
	}

	stripeKey := os.Getenv("STRIPE_SECRET_KEY")
	if stripeKey == "" {
		t.Skip("STRIPE_SECRET_KEY not set")
	}

	uid, err := odooAuth(creds.URL, creds.DB, creds.Login, creds.Password)
	if err != nil || uid == 0 {
		t.Fatalf("Odoo auth failed: %v", err)
	}

	payout1 := "po_1OUgT1FAhaWeDyowVcD9E9Hn"
	payout2 := "po_1OZPK4FAhaWeDyowoYavi9Fh"
	journalName := "test_stripe"

	// ── Setup: find or create test journal ──
	journalID := findOrCreateTestJournal(t, creds, uid, journalName)
	t.Logf("Using journal '%s' (ID: %d) on db '%s'", journalName, journalID, creds.DB)

	// Clean the journal
	t.Log("Cleaning journal...")
	deleteAllJournalStatements(t, creds, uid, journalID)
	assertStatementCount(t, creds, uid, journalID, 0, "after cleanup")
	assertLineCount(t, creds, uid, journalID, 0, "after cleanup")

	// Build a temporary AccountConfig for testing
	acc := &AccountConfig{
		Name:            "Test Stripe",
		Slug:            "test-stripe",
		Provider:        "stripe",
		AccountID:       "test_acct",
		OdooJournalID:   journalID,
		OdooJournalName: journalName,
	}

	// Clean up any orphan lines with our test import IDs from previous runs
	cleanupTestImportIDs(t, creds, uid, "stripe:test_acct:")

	// ── Step 1: Sync payout 1 ──
	t.Logf("Syncing payout 1: %s", payout1)
	_, err = syncStripeToOdoo(acc, creds, uid, 0, false, false, false, payout1, time.Time{})
	if err != nil {
		t.Fatalf("Failed to sync payout 1: %v", err)
	}

	stmt1Count := assertStatementCount(t, creds, uid, journalID, 1, "after payout 1")
	lines1 := countStatementLines(t, creds, uid, journalID)
	t.Logf("After payout 1: %d statement(s), %d line(s)", stmt1Count, lines1)
	if lines1 == 0 {
		t.Fatal("Expected lines after syncing payout 1")
	}

	// Verify statement name is the payout ID
	stmtRefs := getStatementRefs(t, creds, uid, journalID)
	if !stmtRefs[payout1] {
		t.Fatalf("Expected statement named %s, got %v", payout1, stmtRefs)
	}

	// ── Step 2: Sync payout 2 ──
	t.Logf("Syncing payout 2: %s", payout2)
	_, err = syncStripeToOdoo(acc, creds, uid, 0, false, false, false, payout2, time.Time{})
	if err != nil {
		t.Fatalf("Failed to sync payout 2: %v", err)
	}

	stmt2Count := assertStatementCount(t, creds, uid, journalID, 2, "after payout 2")
	lines2 := countStatementLines(t, creds, uid, journalID)
	t.Logf("After payout 2: %d statement(s), %d line(s)", stmt2Count, lines2)
	if lines2 <= lines1 {
		t.Fatalf("Expected more lines after payout 2: had %d, now %d", lines1, lines2)
	}

	// Verify both statements exist
	stmtRefs = getStatementRefs(t, creds, uid, journalID)
	if !stmtRefs[payout1] || !stmtRefs[payout2] {
		t.Fatalf("Expected both payout statements, got %v", stmtRefs)
	}

	// Record line counts per statement for later comparison
	linesForPayout1 := countLinesForStatement(t, creds, uid, journalID, payout1)
	linesForPayout2 := countLinesForStatement(t, creds, uid, journalID, payout2)
	t.Logf("Payout 1 lines: %d, Payout 2 lines: %d", linesForPayout1, linesForPayout2)

	// ── Step 3: Force re-sync payout 1 only ──
	t.Logf("Force re-syncing payout 1: %s", payout1)
	_, err = syncStripeToOdoo(acc, creds, uid, 0, false, true, false, payout1, time.Time{})
	if err != nil {
		t.Fatalf("Failed to force re-sync payout 1: %v", err)
	}

	// Verify still 2 statements
	assertStatementCount(t, creds, uid, journalID, 2, "after force re-sync payout 1")

	// Verify payout 2 is untouched
	linesForPayout2After := countLinesForStatement(t, creds, uid, journalID, payout2)
	if linesForPayout2After != linesForPayout2 {
		t.Fatalf("Payout 2 was modified! Had %d lines, now %d", linesForPayout2, linesForPayout2After)
	}

	// Verify payout 1 was re-created with same line count
	linesForPayout1After := countLinesForStatement(t, creds, uid, journalID, payout1)
	if linesForPayout1After != linesForPayout1 {
		t.Logf("Warning: payout 1 line count changed: %d → %d (may be expected if data changed)", linesForPayout1, linesForPayout1After)
	}

	totalLines := countStatementLines(t, creds, uid, journalID)
	t.Logf("Final state: 2 statements, %d total lines (payout1: %d, payout2: %d)",
		totalLines, linesForPayout1After, linesForPayout2After)

	t.Log("✓ Test passed — journal left intact for manual inspection in Odoo")
}

// ── Test helpers ──

func findOrCreateTestJournal(t *testing.T, creds *OdooCredentials, uid int, name string) int {
	t.Helper()

	// Search for existing
	result, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.journal", "search_read",
		[]interface{}{[]interface{}{
			[]interface{}{"name", "=", name},
			[]interface{}{"type", "=", "bank"},
		}},
		map[string]interface{}{"fields": []string{"id"}, "limit": 1})
	if err == nil {
		var journals []struct {
			ID int `json:"id"`
		}
		json.Unmarshal(result, &journals)
		if len(journals) > 0 {
			return journals[0].ID
		}
	}

	// Create new
	createResult, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.journal", "create",
		[]interface{}{[]interface{}{map[string]interface{}{
			"name": name,
			"type": "bank",
		}}}, nil)
	if err != nil {
		t.Fatalf("Failed to create test journal: %v", err)
	}
	var ids []int
	json.Unmarshal(createResult, &ids)
	if len(ids) == 0 {
		t.Fatal("Journal creation returned no ID")
	}
	return ids[0]
}

func deleteAllJournalStatements(t *testing.T, creds *OdooCredentials, uid int, journalID int) {
	t.Helper()

	// Find ALL statement lines for this journal (including orphans without statements)
	for attempt := 0; attempt < 3; attempt++ {
		linesData, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
			"account.bank.statement.line", "search_read",
			[]interface{}{[]interface{}{
				[]interface{}{"journal_id", "=", journalID},
			}},
			map[string]interface{}{"fields": []string{"id", "move_id"}, "limit": 0})
		if err != nil {
			return
		}

		var lines []struct {
			ID     int         `json:"id"`
			MoveID interface{} `json:"move_id"`
		}
		json.Unmarshal(linesData, &lines)
		if len(lines) == 0 {
			break
		}

		var moveIDs []interface{}
		for _, l := range lines {
			if mid := odooFieldID(l.MoveID); mid > 0 {
				moveIDs = append(moveIDs, mid)
			}
		}
		if len(moveIDs) > 0 {
			// Unreconcile
			reconLines, _ := odooExec(creds.URL, creds.DB, uid, creds.Password,
				"account.move.line", "search",
				[]interface{}{[]interface{}{
					[]interface{}{"move_id", "in", moveIDs},
					[]interface{}{"reconciled", "=", true},
				}}, nil)
			var reconIDs []int
			json.Unmarshal(reconLines, &reconIDs)
			if len(reconIDs) > 0 {
				rIface := make([]interface{}, len(reconIDs))
				for i, id := range reconIDs {
					rIface[i] = id
				}
				odooExec(creds.URL, creds.DB, uid, creds.Password,
					"account.move.line", "remove_move_reconcile",
					[]interface{}{rIface}, nil)
			}
			// Draft + delete moves (cascades to statement lines)
			odooExec(creds.URL, creds.DB, uid, creds.Password,
				"account.move", "button_draft", []interface{}{moveIDs}, nil)
			odooExec(creds.URL, creds.DB, uid, creds.Password,
				"account.move", "unlink", []interface{}{moveIDs}, nil)
		}
	}

	// Delete statements
	stmtIDs, _ := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement", "search",
		[]interface{}{[]interface{}{
			[]interface{}{"journal_id", "=", journalID},
		}}, nil)
	var sids []int
	json.Unmarshal(stmtIDs, &sids)
	if len(sids) > 0 {
		sIface := make([]interface{}, len(sids))
		for i, id := range sids {
			sIface[i] = id
		}
		odooExec(creds.URL, creds.DB, uid, creds.Password,
			"account.bank.statement", "unlink", []interface{}{sIface}, nil)
	}
}

func assertStatementCount(t *testing.T, creds *OdooCredentials, uid int, journalID int, expected int, context string) int {
	t.Helper()
	result, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement", "search_count",
		[]interface{}{[]interface{}{
			[]interface{}{"journal_id", "=", journalID},
		}}, nil)
	if err != nil {
		t.Fatalf("Failed to count statements %s: %v", context, err)
	}
	var count int
	json.Unmarshal(result, &count)
	if count != expected {
		t.Fatalf("Expected %d statement(s) %s, got %d", expected, context, count)
	}
	return count
}

func assertLineCount(t *testing.T, creds *OdooCredentials, uid int, journalID int, expected int, context string) {
	t.Helper()
	count := countStatementLines(t, creds, uid, journalID)
	if count != expected {
		t.Fatalf("Expected %d line(s) %s, got %d", expected, context, count)
	}
}

func countStatementLines(t *testing.T, creds *OdooCredentials, uid int, journalID int) int {
	t.Helper()
	result, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement.line", "search_count",
		[]interface{}{[]interface{}{
			[]interface{}{"journal_id", "=", journalID},
		}}, nil)
	if err != nil {
		t.Fatalf("Failed to count lines: %v", err)
	}
	var count int
	json.Unmarshal(result, &count)
	return count
}

func getStatementRefs(t *testing.T, creds *OdooCredentials, uid int, journalID int) map[string]bool {
	t.Helper()
	result, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement", "search_read",
		[]interface{}{[]interface{}{
			[]interface{}{"journal_id", "=", journalID},
		}},
		map[string]interface{}{"fields": []string{"name", "reference"}, "limit": 0})
	if err != nil {
		t.Fatalf("Failed to get statement refs: %v", err)
	}
	var stmts []struct {
		Name      string      `json:"name"`
		Reference interface{} `json:"reference"`
	}
	json.Unmarshal(result, &stmts)
	refs := map[string]bool{}
	for _, s := range stmts {
		refs[s.Name] = true
		if ref, ok := s.Reference.(string); ok && ref != "" {
			refs[ref] = true
		}
	}
	return refs
}

func countLinesForStatement(t *testing.T, creds *OdooCredentials, uid int, journalID int, payoutID string) int {
	t.Helper()
	// Find statement by reference (payout ID) or name
	result, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement", "search_read",
		[]interface{}{[]interface{}{
			[]interface{}{"journal_id", "=", journalID},
			"|",
			[]interface{}{"reference", "=", payoutID},
			[]interface{}{"name", "=", payoutID},
		}},
		map[string]interface{}{"fields": []string{"id", "line_ids"}, "limit": 1})
	if err != nil {
		t.Fatalf("Failed to find statement %s: %v", payoutID, err)
	}
	var stmts []struct {
		ID      int   `json:"id"`
		LineIDs []int `json:"line_ids"`
	}
	json.Unmarshal(result, &stmts)
	if len(stmts) == 0 {
		t.Fatalf("Statement for payout %s not found", payoutID)
	}
	return len(stmts[0].LineIDs)
}

// cleanupTestImportIDs removes any statement lines with import IDs matching a prefix (global, any journal).
func cleanupTestImportIDs(t *testing.T, creds *OdooCredentials, uid int, prefix string) {
	t.Helper()
	linesData, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement.line", "search_read",
		[]interface{}{[]interface{}{
			[]interface{}{"unique_import_id", "ilike", prefix},
		}},
		map[string]interface{}{"fields": []string{"id", "move_id"}, "limit": 0})
	if err != nil {
		return
	}
	var lines []struct {
		ID     int         `json:"id"`
		MoveID interface{} `json:"move_id"`
	}
	json.Unmarshal(linesData, &lines)
	if len(lines) == 0 {
		return
	}
	t.Logf("Cleaning up %d orphan lines with prefix %s", len(lines), prefix)
	var moveIDs []interface{}
	for _, l := range lines {
		if mid := odooFieldID(l.MoveID); mid > 0 {
			moveIDs = append(moveIDs, mid)
		}
	}
	if len(moveIDs) > 0 {
		odooExec(creds.URL, creds.DB, uid, creds.Password,
			"account.move", "button_draft", []interface{}{moveIDs}, nil)
		odooExec(creds.URL, creds.DB, uid, creds.Password,
			"account.move", "unlink", []interface{}{moveIDs}, nil)
	}
}

// TestStripePayoutSyncDryRun verifies dry run doesn't create anything.
func TestStripePayoutSyncDryRun(t *testing.T) {
	t.Skip("obsolete: per-payout sync removed in favour of chronological BT iteration")
	LoadEnvFromConfig()

	creds, err := ResolveOdooCredentials()
	if err != nil {
		t.Skipf("Odoo credentials not configured: %v", err)
	}
	if !strings.Contains(creds.DB, "test") {
		t.Fatalf("Safety check: database name must contain 'test', got %q", creds.DB)
	}

	stripeKey := os.Getenv("STRIPE_SECRET_KEY")
	if stripeKey == "" {
		t.Skip("STRIPE_SECRET_KEY not set")
	}
	_ = stripeKey
	_ = fmt.Sprintf("") // avoid unused import

	uid, err := odooAuth(creds.URL, creds.DB, creds.Login, creds.Password)
	if err != nil || uid == 0 {
		t.Fatalf("Odoo auth failed: %v", err)
	}

	journalID := findOrCreateTestJournal(t, creds, uid, "test_stripe")
	deleteAllJournalStatements(t, creds, uid, journalID)

	acc := &AccountConfig{
		Name:            "Test Stripe",
		Slug:            "test-stripe",
		Provider:        "stripe",
		AccountID:       "test_acct",
		OdooJournalID:   journalID,
		OdooJournalName: "test_stripe",
	}

	// Dry run should not create anything
	_, err = syncStripeToOdoo(acc, creds, uid, 0, true, false, false, "po_1OUgT1FAhaWeDyowVcD9E9Hn", time.Time{})
	if err != nil {
		t.Fatalf("Dry run failed: %v", err)
	}

	assertStatementCount(t, creds, uid, journalID, 0, "after dry run")
	assertLineCount(t, creds, uid, journalID, 0, "after dry run")
	t.Log("✓ Dry run created nothing")
}
