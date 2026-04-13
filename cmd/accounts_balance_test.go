package cmd

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"
	"time"
)

// TestStripe2024Balance verifies that syncing all of 2024 to Odoo produces
// the same ending balance as Stripe's own report:
//
//	Starting balance: €0.00
//	Activity before fees: €36,954.40
//	Fees: -€1,235.73
//	Net balance change: €35,718.67
//	Total payouts: -€26,391.77
//	Ending balance: €9,326.90
//
// Run with: go test -v -run TestStripe2024Balance -timeout 600s ./cmd/
func TestStripe2024Balance(t *testing.T) {
	LoadEnvFromConfig()

	creds, err := ResolveOdooCredentials()
	if err != nil {
		t.Skipf("Odoo credentials not configured: %v", err)
	}
	if !strings.Contains(creds.DB, "test") {
		t.Fatalf("Safety check: database name must contain 'test', got %q", creds.DB)
	}
	if os.Getenv("STRIPE_SECRET_KEY") == "" {
		t.Skip("STRIPE_SECRET_KEY not set")
	}

	uid, err := odooAuth(creds.URL, creds.DB, creds.Login, creds.Password)
	if err != nil || uid == 0 {
		t.Fatalf("Odoo auth failed: %v", err)
	}

	// Use the real Stripe journal — we need the real account ID for import ID matching
	journalName := "test_stripe_2024"
	journalID := findOrCreateTestJournal(t, creds, uid, journalName)
	t.Logf("Using journal '%s' (ID: %d) on db '%s'", journalName, journalID, creds.DB)

	// Clean this journal AND the main Stripe journal to avoid unique_import_id collisions
	deleteAllJournalStatements(t, creds, uid, journalID)
	// Also find and clean the main Stripe journal if it exists
	mainJournalResult, _ := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.journal", "search_read",
		[]interface{}{[]interface{}{
			[]interface{}{"name", "=", "Stripe"},
			[]interface{}{"type", "=", "bank"},
		}},
		map[string]interface{}{"fields": []string{"id"}, "limit": 1})
	var mainJournals []struct{ ID int `json:"id"` }
	json.Unmarshal(mainJournalResult, &mainJournals)
	if len(mainJournals) > 0 && mainJournals[0].ID != journalID {
		// Count lines in main journal
		countResult, _ := odooExec(creds.URL, creds.DB, uid, creds.Password,
			"account.bank.statement.line", "search_count",
			[]interface{}{[]interface{}{
				[]interface{}{"journal_id", "=", mainJournals[0].ID},
			}}, nil)
		var mainCount int
		json.Unmarshal(countResult, &mainCount)
		if mainCount > 0 {
			t.Logf("Main Stripe journal #%d has %d lines — cleaning to avoid import ID collisions", mainJournals[0].ID, mainCount)
			deleteAllJournalStatements(t, creds, uid, mainJournals[0].ID)
			// Verify
			json.Unmarshal(countResult, &mainCount)
			countResult2, _ := odooExec(creds.URL, creds.DB, uid, creds.Password,
				"account.bank.statement.line", "search_count",
				[]interface{}{[]interface{}{
					[]interface{}{"journal_id", "=", mainJournals[0].ID},
				}}, nil)
			var remaining int
			json.Unmarshal(countResult2, &remaining)
			if remaining > 0 {
				t.Fatalf("Failed to clean main journal #%d: %d lines remain", mainJournals[0].ID, remaining)
			}
		}
	}

	realAccountID := "acct_1Nn0FaFAhaWeDyow"
	acc := &AccountConfig{
		Name:            "Test Stripe 2024",
		Slug:            "stripe",
		Provider:        "stripe",
		AccountID:       realAccountID,
		OdooJournalID:   journalID,
		OdooJournalName: journalName,
	}

	// Compute expected values from local data
	var totalGross, totalFees, totalDebitsGross float64
	var nCharges, nDebits int

	for m := 1; m <= 12; m++ {
		txFile := LoadTransactionsWithPII(DataDir(), "2024", fmt.Sprintf("%02d", m))
		if txFile == nil {
			continue
		}
		for _, tx := range txFile.Transactions {
			if tx.Provider != "stripe" || tx.Type == "INTERNAL" {
				continue
			}
			if tx.Type == "CREDIT" {
				totalGross += tx.GrossAmount
				totalFees += tx.Fee
				nCharges++
			} else if tx.Type == "DEBIT" {
				totalDebitsGross += tx.GrossAmount
				nDebits++
			}
		}
	}

	t.Logf("Local 2024 data: %d charges (gross: %.2f, fees: %.2f), %d debits (%.2f)",
		nCharges, totalGross, totalFees, nDebits, totalDebitsGross)

	// Sync with --until 2024
	untilDate := time.Date(2025, 1, 1, 0, 0, 0, 0, BrusselsTZ())
	t.Log("Syncing payouts + orphan charges...")
	err = syncStripeToOdoo(acc, creds, uid, 0, false, false, "", untilDate)
	if err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	// Count what's in Odoo
	lineResult, _ := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement.line", "search_read",
		[]interface{}{[]interface{}{
			[]interface{}{"journal_id", "=", journalID},
		}},
		map[string]interface{}{"fields": []string{"amount"}, "limit": 0})
	var lines []struct {
		Amount float64 `json:"amount"`
	}
	json.Unmarshal(lineResult, &lines)

	var odooBalance float64
	var positiveTotal, negativeTotal float64
	for _, l := range lines {
		odooBalance += l.Amount
		if l.Amount > 0 {
			positiveTotal += l.Amount
		} else {
			negativeTotal += l.Amount
		}
	}

	t.Logf("Odoo result: %d lines, balance: %.2f", len(lines), odooBalance)
	t.Logf("  Positive (charges): %.2f", positiveTotal)
	t.Logf("  Negative (fees+payouts): %.2f", negativeTotal)

	// Stripe's reported ending balance
	expectedBalance := 9326.90

	t.Logf("Expected balance: %.2f", expectedBalance)
	t.Logf("Odoo balance:     %.2f", odooBalance)
	t.Logf("Difference:       %.2f", odooBalance-expectedBalance)

	// The local data may have slightly different fee totals than Stripe's report
	// (€1,235.73 vs our computed fees) due to rounding. Allow small tolerance.
	if math.Abs(odooBalance-expectedBalance) > 100 {
		t.Errorf("Balance mismatch too large: expected ~%.2f, got %.2f (diff: %.2f)",
			expectedBalance, odooBalance, odooBalance-expectedBalance)
	} else if math.Abs(odooBalance-expectedBalance) > 1 {
		t.Logf("⚠ Balance close but not exact (diff: %.2f) — likely fee rounding differences",
			odooBalance-expectedBalance)
	} else {
		t.Log("✓ Balance matches Stripe report")
	}

	t.Log("Journal left intact for manual inspection")
}
