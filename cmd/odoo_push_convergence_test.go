package cmd

import (
	"strings"
	"testing"
)

// After a push the Odoo journal balance must equal the local balance; when
// it doesn't, the summary has to say so explicitly and point at the two
// remediation verbs (`odoo journals <id> fix`, `accounts <slug> push
// --force`) instead of leaving the user to eyeball the two snapshot rows.
func TestLocalJournalBalanceMismatchHint(t *testing.T) {
	acc := &AccountConfig{Slug: "stripe", Provider: "stripe", OdooJournalID: 48, Currency: "EUR"}
	local := accountOdooSyncSnapshot{Balance: 812.03, Currency: "EUR"}

	t.Run("balanced stays silent", func(t *testing.T) {
		journal := accountOdooSyncSnapshot{Balance: 812.03, Currency: "EUR"}
		if hint := localJournalBalanceMismatchHint(acc, local, journal); hint != "" {
			t.Fatalf("expected no hint for matching balances, got %q", hint)
		}
	})

	t.Run("sub-cent drift stays silent", func(t *testing.T) {
		journal := accountOdooSyncSnapshot{Balance: 812.034, Currency: "EUR"}
		if hint := localJournalBalanceMismatchHint(acc, local, journal); hint != "" {
			t.Fatalf("expected no hint for sub-cent drift, got %q", hint)
		}
	})

	t.Run("mismatch names both remediations", func(t *testing.T) {
		journal := accountOdooSyncSnapshot{Balance: -2801.78, Currency: "EUR"}
		hint := localJournalBalanceMismatchHint(acc, local, journal)
		if hint == "" {
			t.Fatal("expected a hint for mismatched balances")
		}
		for _, want := range []string{"chb odoo journals 48 fix", "chb accounts stripe push --force", "≠"} {
			if !strings.Contains(hint, want) {
				t.Errorf("hint missing %q:\n%s", want, hint)
			}
		}
	})

	t.Run("no linked journal stays silent", func(t *testing.T) {
		unlinked := &AccountConfig{Slug: "stripe", Provider: "stripe"}
		journal := accountOdooSyncSnapshot{Balance: 0, Currency: "EUR"}
		if hint := localJournalBalanceMismatchHint(unlinked, local, journal); hint != "" {
			t.Fatalf("expected no hint without a linked journal, got %q", hint)
		}
	})
}

// The fix-time balance diagnostic must (a) offer the manual-line delete for
// the part of the gap that manual lines explain, and (b) escalate to a
// `push --force` rebuild for whatever residual they don't — that residual is
// chb-owned divergence (missing history, drifted amounts) no delete fixes.
func TestJournalBalanceDiagnosticHints(t *testing.T) {
	acc := &AccountConfig{Slug: "stripe", Provider: "stripe", OdooJournalID: 48}

	t.Run("manual lines fully explain the gap", func(t *testing.T) {
		// journal = local + manual lines → deleting them converges.
		d := journalBalanceDiagnostic{
			localBalance:   812.03,
			journalBalance: 6537.41,
			manualCount:    2,
			manualSum:      5725.38,
			balanceDelta:   5725.38,
			residual:       0,
			hasGap:         true,
		}
		hints := journalBalanceDiagnosticHints(d, acc)
		if len(hints) != 1 {
			t.Fatalf("expected only the manual-line hint, got %d: %v", len(hints), hints)
		}
		if !strings.Contains(hints[0], "delete step below") {
			t.Errorf("manual-line hint should mention the delete step, got %q", hints[0])
		}
	})

	t.Run("residual gap escalates to push --force", func(t *testing.T) {
		// Journal #48 as found in the wild: +5725.38 of manual lines AND a
		// -9339.19 chb-owned backlog (2024 history never pushed).
		d := journalBalanceDiagnostic{
			localBalance:   812.03,
			journalBalance: -2801.78,
			manualCount:    2,
			manualSum:      5725.38,
			balanceDelta:   -3613.81,
			residual:       -9339.19,
			hasGap:         true,
		}
		hints := journalBalanceDiagnosticHints(d, acc)
		if len(hints) != 2 {
			t.Fatalf("expected manual + residual hints, got %d: %v", len(hints), hints)
		}
		if !strings.Contains(hints[1], "chb accounts stripe push --force") {
			t.Errorf("residual hint should suggest push --force, got %q", hints[1])
		}
	})

	t.Run("gap with no manual lines is all residual", func(t *testing.T) {
		d := journalBalanceDiagnostic{
			localBalance:   812.03,
			journalBalance: -8527.16,
			balanceDelta:   -9339.19,
			residual:       -9339.19,
			hasGap:         true,
		}
		hints := journalBalanceDiagnosticHints(d, acc)
		if len(hints) != 1 || !strings.Contains(hints[0], "push --force") {
			t.Fatalf("expected a single push --force hint, got %v", hints)
		}
	})

	t.Run("no gap, no hints", func(t *testing.T) {
		d := journalBalanceDiagnostic{localBalance: 812.03, journalBalance: 812.03}
		if hints := journalBalanceDiagnosticHints(d, acc); len(hints) != 0 {
			t.Fatalf("expected no hints, got %v", hints)
		}
	})
}

// computeJournalBalanceDiagnostic feeds the hints above; its residual must
// be the gap that would remain after deleting every manual line.
func TestComputeJournalBalanceDiagnosticResidual(t *testing.T) {
	t.Setenv("DATA_DIR", t.TempDir()) // empty local archive → local balance 0

	res := &odooOrphanFindResult{
		Account:           &AccountConfig{Slug: "checking", Provider: "etherscan", Chain: "gnosis", OdooJournalID: 44},
		OdooImportedCount: 3,
		OdooImportedSum:   100.00,
		Unowned: []odooOrphanLine{
			{ID: 1, Date: "2025-01-01", Amount: 40.00, PaymentRef: "Solde de départ"},
		},
		UnownedSum: 40.00,
	}
	d, ok := computeJournalBalanceDiagnostic(res)
	if !ok {
		t.Fatal("expected a diagnostic")
	}
	if !d.hasGap {
		t.Fatal("expected a gap (journal 140 vs local 0)")
	}
	if d.balanceDelta != 140.00 {
		t.Errorf("balanceDelta = %v, want 140.00", d.balanceDelta)
	}
	// Deleting the 40.00 manual line still leaves 100.00 of chb-owned lines
	// local knows nothing about.
	if d.residual != 100.00 {
		t.Errorf("residual = %v, want 100.00", d.residual)
	}
}

// odooSyncSince marks a journal as starting at a cutoff date with a manual
// opening entry. The parser anchors the date in Brussels time and rejects
// malformed values.
func TestOdooSyncSinceTime(t *testing.T) {
	acc := &AccountConfig{Slug: "x", OdooSyncSince: "2025-01-01"}
	cutoff, ok := acc.OdooSyncSinceTime()
	if !ok {
		t.Fatal("expected ok for valid date")
	}
	if got := cutoff.Format("2006-01-02 15:04 -0700"); got != "2025-01-01 00:00 +0100" {
		t.Errorf("cutoff = %s, want Brussels midnight", got)
	}
	if _, ok := (&AccountConfig{}).OdooSyncSinceTime(); ok {
		t.Error("unset cutoff must return ok=false")
	}
	if _, ok := (&AccountConfig{OdooSyncSince: "01/01/2025"}).OdooSyncSinceTime(); ok {
		t.Error("malformed cutoff must return ok=false")
	}
	var nilAcc *AccountConfig
	if _, ok := nilAcc.OdooSyncSinceTime(); ok {
		t.Error("nil account must return ok=false")
	}
}

// On a cutoff journal the manual opening entry is validated against the
// locally-computed balance, never offered for deletion; whatever the manual
// lines don't explain stays a push --force matter.
func TestJournalBalanceDiagnosticHintsCutoff(t *testing.T) {
	acc := &AccountConfig{Slug: "202605-savings-hacked", Provider: "etherscan", OdooJournalID: 47, OdooSyncSince: "2025-01-01"}

	t.Run("correct opening is validated and protected", func(t *testing.T) {
		// Journal 47 target state: opening 115,525.44 + windowed activity
		// -115,525.44 → journal total 0.00, no gap anywhere.
		d := journalBalanceDiagnostic{
			localBalance:    -115525.44,
			journalBalance:  0,
			manualCount:     1,
			manualSum:       115525.44,
			hasCutoff:       true,
			cutoff:          "2025-01-01",
			expectedOpening: 115525.44,
			openingDelta:    0,
			balanceDelta:    0,
			residual:        0,
		}
		hints := journalBalanceDiagnosticHints(d, acc)
		if len(hints) != 1 {
			t.Fatalf("expected one validation hint, got %v", hints)
		}
		if !strings.Contains(hints[0], "matches") || !strings.Contains(hints[0], "never deletes") {
			t.Errorf("expected protected-validation hint, got %q", hints[0])
		}
		if strings.Contains(hints[0], "delete step below") {
			t.Errorf("cutoff opening must not be offered for deletion: %q", hints[0])
		}
	})

	t.Run("wrong opening reports the exact adjustment", func(t *testing.T) {
		// Journal 48's old state: accountant booked 8,630.03 but the
		// computed opening is 9,326.90.
		d := journalBalanceDiagnostic{
			manualCount:     2,
			manualSum:       5725.38,
			hasCutoff:       true,
			cutoff:          "2025-01-01",
			expectedOpening: 9326.90,
			openingDelta:    -3601.52,
			balanceDelta:    -3601.52,
			hasGap:          true,
			residual:        0,
		}
		hints := journalBalanceDiagnosticHints(d, acc)
		if len(hints) != 1 {
			t.Fatalf("expected one adjustment hint, got %v", hints)
		}
		for _, want := range []string{"adjust the opening entry", "+9,326.90", "never deletes"} {
			if !strings.Contains(hints[0], want) {
				t.Errorf("hint missing %q: %q", want, hints[0])
			}
		}
	})

	t.Run("missing opening suggests force rebuild", func(t *testing.T) {
		d := journalBalanceDiagnostic{
			hasCutoff:       true,
			cutoff:          "2025-01-01",
			expectedOpening: 9326.90,
			openingDelta:    -9326.90,
			balanceDelta:    -9326.90,
			hasGap:          true,
		}
		hints := journalBalanceDiagnosticHints(d, acc)
		if len(hints) != 1 || !strings.Contains(hints[0], "push --force") {
			t.Fatalf("expected force-rebuild hint, got %v", hints)
		}
	})

	t.Run("owned divergence still escalates under cutoff", func(t *testing.T) {
		d := journalBalanceDiagnostic{
			manualCount:     1,
			manualSum:       115525.44,
			hasCutoff:       true,
			cutoff:          "2025-01-01",
			expectedOpening: 115525.44,
			balanceDelta:    115525.44, // pre-cutoff owned lines still present
			residual:        115525.44,
			hasGap:          true,
		}
		hints := journalBalanceDiagnosticHints(d, acc)
		if len(hints) != 2 {
			t.Fatalf("expected validation + residual hints, got %v", hints)
		}
		if !strings.Contains(hints[1], "push --force") {
			t.Errorf("residual hint should suggest push --force, got %q", hints[1])
		}
	})
}

// planStartingBalanceConvergence computes the minimal change-set that
// converges a journal onto the opening-entry model, from cached lines only.
func TestPlanStartingBalanceConvergence(t *testing.T) {
	t.Setenv("DATA_DIR", t.TempDir()) // empty local archive → computed opening 0
	acc := &AccountConfig{Slug: "x", Provider: "etherscan", Chain: "gnosis", OdooJournalID: 47}
	cutoff, ok := ParseSinceDate("20250101")
	if !ok {
		t.Fatal("ParseSinceDate must accept YYYYMMDD")
	}
	if got := cutoff.Format("2006-01-02"); got != "2025-01-01" {
		t.Fatalf("cutoff = %s, want 2025-01-01", got)
	}

	t.Run("pre-cutoff owned lines are deleted, correct opening kept", func(t *testing.T) {
		// Local archive is empty so the computed opening is 0 — make the
		// manual line 0 too so it counts as correct.
		lines := []OdooCacheLine{
			{ID: 1, Date: "2024-06-03", Amount: 100, UniqueImportID: "gnosis:a:0x1:0"},
			{ID: 2, Date: "2024-12-31", Amount: -40, UniqueImportID: "gnosis:a:0x2:0"},
			{ID: 3, Date: "2025-01-01", Amount: 0, PaymentRef: "Solde de départ"},
			{ID: 4, Date: "2025-02-01", Amount: 7, UniqueImportID: "gnosis:a:0x3:0"},
		}
		plan := planStartingBalanceConvergence(acc, cutoff, lines)
		if len(plan.DeleteLines) != 2 || plan.DeleteSum != 60 {
			t.Fatalf("expected 2 deletions summing 60, got %d / %v", len(plan.DeleteLines), plan.DeleteSum)
		}
		if plan.OpeningAction != "ok" {
			t.Errorf("OpeningAction = %q, want ok", plan.OpeningAction)
		}
		if !plan.hasChanges() {
			t.Error("plan with deletions must report changes")
		}
	})

	t.Run("wrong opening is corrected, post-cutoff lines untouched", func(t *testing.T) {
		lines := []OdooCacheLine{
			{ID: 3, Date: "2025-01-01", Amount: 8630.03, PaymentRef: "Solde de départ"},
			{ID: 4, Date: "2025-02-01", Amount: 7, UniqueImportID: "stripe:acct:txn_1"},
		}
		plan := planStartingBalanceConvergence(acc, cutoff, lines)
		if len(plan.DeleteLines) != 0 {
			t.Fatalf("no pre-cutoff lines, got %d deletions", len(plan.DeleteLines))
		}
		if plan.OpeningAction != "update" || plan.Opening == nil || plan.Opening.ID != 3 {
			t.Fatalf("expected update of line 3, got %q %+v", plan.OpeningAction, plan.Opening)
		}
	})

	t.Run("no manual line and zero opening needs nothing", func(t *testing.T) {
		plan := planStartingBalanceConvergence(acc, cutoff, []OdooCacheLine{
			{ID: 4, Date: "2025-02-01", Amount: 7, UniqueImportID: "stripe:acct:txn_1"},
		})
		if plan.OpeningAction != "ok" || plan.hasChanges() {
			t.Fatalf("expected no-op plan, got %q hasChanges=%v", plan.OpeningAction, plan.hasChanges())
		}
	})

	t.Run("multiple manual lines are ambiguous and untouched", func(t *testing.T) {
		plan := planStartingBalanceConvergence(acc, cutoff, []OdooCacheLine{
			{ID: 3, Date: "2025-01-01", Amount: 8630.03, PaymentRef: "Solde de départ"},
			{ID: 5, Date: "2025-12-31", Amount: -2904.65, PaymentRef: "Solde compte"},
		})
		if plan.OpeningAction != "ambiguous" || len(plan.ExtraManual) != 2 {
			t.Fatalf("expected ambiguous with 2 manual lines, got %q / %d", plan.OpeningAction, len(plan.ExtraManual))
		}
		if plan.hasChanges() {
			t.Error("ambiguous plan must not claim changes")
		}
	})
}
