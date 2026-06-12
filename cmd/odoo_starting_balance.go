package cmd

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"time"
)

// startingBalancePlan is the minimal change-set that converges a journal
// onto the "manual opening entry + CHB lines since the cutoff" model. It is
// computed entirely from the local journal-lines cache — zero Odoo RPCs —
// so the plan can be previewed (and confirmed) before any write happens.
// Everything the plan does NOT mention is already in the desired state or
// is handled by the windowed push that follows it.
type startingBalancePlan struct {
	Cutoff          time.Time
	CutoffDate      string  // YYYY-MM-DD
	ExpectedOpening float64 // locally-computed balance before the cutoff

	DeleteLines []OdooCacheLine // CHB-owned lines dated before the cutoff
	DeleteSum   float64

	Opening       *OdooCacheLine  // the journal's manual opening entry, when present
	OpeningAction string          // "create" | "update" | "ok" | "ambiguous"
	ExtraManual   []OdooCacheLine // manual lines beyond the first — never touched
}

func (p startingBalancePlan) hasChanges() bool {
	return len(p.DeleteLines) > 0 || p.OpeningAction == "create" || p.OpeningAction == "update"
}

// planStartingBalanceConvergence partitions the cached journal lines:
// CHB-owned lines dated before the cutoff double-count the opening entry
// and must go; the (single) manual line is the opening entry and is
// validated against the locally-computed balance — created when absent,
// amount-corrected when wrong, left alone when right. Multiple manual
// lines are ambiguous: we can't tell which one is the opening, so the
// plan refuses to guess and defers to a human (or `journals <id> fix`).
func planStartingBalanceConvergence(acc *AccountConfig, cutoff time.Time, lines []OdooCacheLine) startingBalancePlan {
	plan := startingBalancePlan{
		Cutoff:          cutoff,
		CutoffDate:      cutoff.Format("2006-01-02"),
		ExpectedOpening: accountLocalBalanceBefore(acc, cutoff),
	}
	var manual []OdooCacheLine
	for i := range lines {
		l := lines[i]
		if l.UniqueImportID == "" {
			manual = append(manual, l)
			continue
		}
		if l.Date != "" && l.Date < plan.CutoffDate {
			plan.DeleteLines = append(plan.DeleteLines, l)
			plan.DeleteSum += l.Amount
		}
	}
	plan.DeleteSum = roundCents(plan.DeleteSum)
	switch len(manual) {
	case 0:
		if plan.ExpectedOpening == 0 {
			plan.OpeningAction = "ok" // a zero opening needs no line
		} else {
			plan.OpeningAction = "create"
		}
	case 1:
		plan.Opening = &manual[0]
		if math.Abs(manual[0].Amount-plan.ExpectedOpening) <= 0.005 {
			plan.OpeningAction = "ok"
		} else {
			plan.OpeningAction = "update"
		}
	default:
		plan.OpeningAction = "ambiguous"
		plan.ExtraManual = manual
	}
	return plan
}

func printStartingBalancePlan(plan startingBalancePlan, acc *AccountConfig) {
	cur := accCurrency(acc)
	fmt.Printf("\n  %sStarting-balance convergence (cutoff %s)%s\n", Fmt.Bold, plan.CutoffDate, Fmt.Reset)
	fmt.Printf("    %sComputed opening (local history before cutoff): %s%s\n",
		Fmt.Dim, formatBalance(plan.ExpectedOpening, cur), Fmt.Reset)

	if len(plan.DeleteLines) > 0 {
		byMonth := map[string]struct {
			n   int
			sum float64
		}{}
		for _, l := range plan.DeleteLines {
			key := l.Date
			if len(key) >= 7 {
				key = key[:7]
			}
			agg := byMonth[key]
			agg.n++
			agg.sum += l.Amount
			byMonth[key] = agg
		}
		months := make([]string, 0, len(byMonth))
		for m := range byMonth {
			months = append(months, m)
		}
		sort.Strings(months)
		fmt.Printf("    %s to delete (pre-cutoff, double-count the opening):%s\n",
			Pluralize(len(plan.DeleteLines), "CHB-owned line", ""), Fmt.Reset)
		for _, m := range months {
			fmt.Printf("      %s  %18s  %12s\n", m, Pluralize(byMonth[m].n, "line", ""), fmtEURSigned(byMonth[m].sum))
		}
		fmt.Printf("      %stotal %s%s\n", Fmt.Dim, fmtEURSigned(plan.DeleteSum), Fmt.Reset)
	}

	switch plan.OpeningAction {
	case "create":
		fmt.Printf("    Opening entry: %screate%s 'Solde de départ %s' %s\n",
			Fmt.Yellow, Fmt.Reset, plan.CutoffDate, fmtEURSigned(plan.ExpectedOpening))
	case "update":
		fmt.Printf("    Opening entry: %supdate%s #%d %q  %s → %s\n",
			Fmt.Yellow, Fmt.Reset, plan.Opening.ID, plan.Opening.PaymentRef,
			fmtEURSigned(plan.Opening.Amount), fmtEURSigned(plan.ExpectedOpening))
	case "ok":
		if plan.Opening != nil {
			fmt.Printf("    Opening entry: %s✓ #%d %q matches computed balance%s\n",
				Fmt.Green, plan.Opening.ID, plan.Opening.PaymentRef, Fmt.Reset)
		}
	case "ambiguous":
		Warnf("    %s⚠ %s with no unique_import_id — cannot tell which is the opening; review with `chb odoo journals <id> fix`:%s",
			Fmt.Yellow, Pluralize(len(plan.ExtraManual), "manual line", ""), Fmt.Reset)
		for _, l := range plan.ExtraManual {
			fmt.Printf("      #%d  %s  %12s  %s\n", l.ID, l.Date, fmtEURSigned(l.Amount), l.PaymentRef)
		}
	}
	fmt.Println()
}

// applyStartingBalanceConvergence previews the plan, asks for confirmation
// (one prompt covering the deletions and the opening correction), executes
// it, and refreshes the journal-lines cache so the windowed push that
// follows plans against Odoo's true state. Returns whether anything was
// written. Dry-run prints the plan and writes nothing.
func applyStartingBalanceConvergence(creds *OdooCredentials, uid int, acc *AccountConfig, plan startingBalancePlan, assumeYes, dryRun bool) (bool, error) {
	printStartingBalancePlan(plan, acc)
	if !plan.hasChanges() {
		return false, nil
	}
	if dryRun {
		fmt.Printf("  %s(dry-run) no changes applied%s\n\n", Fmt.Dim, Fmt.Reset)
		return false, nil
	}
	if !assumeYes {
		fmt.Printf("  %sApply? Deletes the listed lines and corrects the opening entry. This cannot be undone.%s [y/N] ", Fmt.Bold, Fmt.Reset)
		reader := bufio.NewReader(os.Stdin)
		resp, _ := reader.ReadString('\n')
		resp = strings.TrimSpace(resp)
		if resp != "y" && resp != "Y" && resp != "yes" {
			return false, fmt.Errorf("starting-balance convergence cancelled")
		}
	}

	if len(plan.DeleteLines) > 0 {
		ids := make([]int, len(plan.DeleteLines))
		for i, l := range plan.DeleteLines {
			ids[i] = l.ID
		}
		if err := deleteStatementLines(creds, uid, ids); err != nil {
			return false, fmt.Errorf("delete pre-cutoff lines: %v", err)
		}
		fmt.Printf("  %s✓ Deleted %s%s\n", Fmt.Green, Pluralize(len(ids), "pre-cutoff line", ""), Fmt.Reset)
	}

	switch plan.OpeningAction {
	case "create":
		if err := createOpeningBalanceLine(creds, uid, acc, plan.Cutoff); err != nil {
			return true, err
		}
	case "update":
		vals := map[string]interface{}{"amount": plan.ExpectedOpening}
		if err := updateStatementLineFieldsForMetadata(creds, uid, plan.Opening.ID, plan.Opening.MoveID, vals); err != nil {
			return true, fmt.Errorf("update opening entry #%d: %v", plan.Opening.ID, err)
		}
		fmt.Printf("  %s✓ Opening entry #%d corrected to %s%s\n",
			Fmt.Green, plan.Opening.ID, fmtEURSigned(plan.ExpectedOpening), Fmt.Reset)
	}

	// The deletions/opening write changed the journal's count + balance;
	// refresh the cache so the push's freshness guard and planning see
	// Odoo as it now stands.
	if _, err := writeOdooJournalLinesCacheFullRefetch(creds, uid, acc.OdooJournalID, nil); err != nil {
		Warnf("  %s⚠ Could not refresh journal cache after convergence: %v%s", Fmt.Yellow, err, Fmt.Reset)
	}
	return true, nil
}
