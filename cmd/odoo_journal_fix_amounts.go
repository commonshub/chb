package cmd

import (
	"bufio"
	"errors"
	"fmt"
	"math"
	"os"
	"strings"

	stripesource "github.com/CommonsHub/chb/providers/stripe"
)

// odooAmountFix is one statement line whose amount in Odoo disagrees with the
// locally-computed, correctly-signed amount for the same transaction (matched
// by unique_import_id).
type odooAmountFix struct {
	LineID       int
	MoveID       int
	Date         string
	ImportID     string
	OldAmount    float64
	NewAmount    float64
	IsReconciled bool
}

// expectedOdooMainLineAmount mirrors what the push writes on the MAIN
// statement line for a transaction. Stripe charge-like transactions
// (charge/payment/refund — see stripeStatementLineAmount) are pushed with
// the signed GROSS amount; their fees are carried by the per-payout
// aggregate ":fees" line, so expecting the net here would double-count
// every fee once "fixed". Everything else carries the signed net amount.
func expectedOdooMainLineAmount(acc *AccountConfig, tx TransactionEntry) float64 {
	if acc != nil && acc.Provider == "stripe" {
		kind, _ := tx.Metadata["kind"].(string)
		switch strings.ToLower(kind) {
		case "charge", "payment", "refund", "payment_refund":
			amt := tx.GrossAmount
			if amt == 0 {
				amt = tx.Amount
			}
			if tx.IsOutgoing() {
				return -math.Abs(amt)
			}
			return math.Abs(amt)
		}
	}
	return signedOdooAmountForTransaction(acc, tx)
}

// detectOdooJournalAmountFixes returns the journal's statement lines whose
// amount in Odoo differs from the locally-computed signed amount for the same
// transaction (matched by unique_import_id). Read-only. The canonical case:
// internal transfers pushed before the internalTransactionDirection sign fix
// went out as +amount (incoming) when they were outflows, so a drained wallet's
// journal reads far above its true (~0) balance.
func detectOdooJournalAmountFixes(creds *OdooCredentials, uid, journalID int, acc *AccountConfig) ([]odooAmountFix, error) {
	if acc == nil {
		return nil, nil
	}
	// Correct, signed amount per local transaction, keyed by unique_import_id.
	want := map[string]float64{}
	if acc.Provider == "stripe" {
		// Mirror the writer exactly: the push computes line amounts from the
		// raw balance transactions via stripeStatementLineAmount, so the
		// checker must too. The generated transaction view force-signs
		// fee-kind entries negative (e.g. positive "free credit"
		// adjustments become DEBITs), which would flag correct lines as
		// wrong and "repair" the journal away from the true balance.
		bts, err := stripesource.LoadTransactionsSince(DataDir(), acc.AccountID, 0)
		if err != nil {
			return nil, fmt.Errorf("load Stripe provider transactions: %v", err)
		}
		for _, bt := range bts {
			if id := stripeBTImportID(acc, bt); id != "" {
				want[id] = roundCents(stripeStatementLineAmount(bt))
			}
		}
	} else {
		for _, tx := range loadAccountTransactionsForOdoo(acc) {
			if id := buildUniqueImportID(acc, tx); id != "" {
				want[id] = roundCents(expectedOdooMainLineAmount(acc, tx))
			}
		}
	}
	if len(want) == 0 {
		return nil, nil
	}
	rows, err := odooSearchReadAllMaps(creds, uid, "account.bank.statement.line",
		[]interface{}{[]interface{}{"journal_id", "=", journalID}},
		[]string{"id", "date", "amount", "unique_import_id", "move_id", "is_reconciled"},
		"date asc, id asc")
	if err != nil {
		return nil, err
	}
	var fixes []odooAmountFix
	for _, row := range rows {
		id := odooString(row["unique_import_id"])
		if id == "" {
			continue
		}
		newAmt, ok := want[id]
		if !ok {
			continue
		}
		oldAmt := roundCents(odooFloat(row["amount"]))
		if oldAmt == newAmt {
			continue
		}
		fixes = append(fixes, odooAmountFix{
			LineID:       odooInt(row["id"]),
			MoveID:       odooFieldID(row["move_id"]),
			Date:         odooString(row["date"]),
			ImportID:     id,
			OldAmount:    oldAmt,
			NewAmount:    newAmt,
			IsReconciled: odooBool(row["is_reconciled"]),
		})
	}
	return fixes, nil
}

// odooFeeRepairs groups the Stripe aggregate-fee-line problems found in a
// journal: lines whose amount disagrees with the locally-recomputed aggregate
// fee, and stale fee lines that no longer correspond to any expected fee.
type odooFeeRepairs struct {
	Corrections []odooAmountFix // wrong amount → rewrite in place
	Spurious    []odooAmountFix // no matching expected fee → delete
}

func (r odooFeeRepairs) empty() bool { return len(r.Corrections) == 0 && len(r.Spurious) == 0 }

// detectOdooJournalFeeFixes recomputes the aggregate Stripe ":fees" lines from
// local balance transactions and compares them to what's in the journal. Only
// meaningful for Stripe journals — other providers have no aggregate fee lines.
//
// The push folds each charge's implicit Stripe fee into one aggregate fee line
// per statement: a "stripe:<acct>:<payout>:fees" line when an automatic payout
// closes the statement, plus a single rolling "stripe:<acct>:open:<stmt>:fees"
// line for the still-open statement. That rolling line is updated ADDITIVELY
// across sync runs, which is exactly how it drifts: a re-push can double-add,
// leaving the open-statement fee line far larger than the fees it represents
// (the canonical symptom — one open-fee line carrying thousands of euros of
// phantom fees).
//
// This mirrors the push's partition (stripeImplicitChargeFeeCents accumulated
// between stripePayoutClosesStatement boundaries) to derive the correct amount
// for every fee line, then classifies:
//   - closed-payout or open-statement fee line, wrong amount → correction
//   - closed-payout fee line whose payout isn't expected, or an extra/obsolete
//     open-statement fee line → spurious (delete)
//
// Missing fee lines are left to `push`/`sync`, which create them.
// expectedStripePerChargeFees returns the correct amount (EUR) for every
// per-charge Stripe fee line, keyed by lowercased import id
// (stripeBTFeeImportID — "stripe:<acct>:<txn>:fee"). One entry per charge that
// carries an implicit processing fee. Pure; windowed by the account's cutoff so
// it lines up with what the push books. Exported for unit testing.
func expectedStripePerChargeFees(acc *AccountConfig, bts []stripesource.Transaction) map[string]float64 {
	out := map[string]float64{}
	if acc == nil {
		return out
	}
	cutoffUnix := int64(0)
	hasCutoff := false
	if cutoff, ok := acc.OdooSyncSinceTime(); ok {
		cutoffUnix, hasCutoff = cutoff.Unix(), true
	}
	for _, bt := range bts {
		if hasCutoff && bt.Created < cutoffUnix {
			continue
		}
		cents, ok := stripeImplicitChargeFeeCents(bt)
		if !ok || cents == 0 {
			continue
		}
		out[strings.ToLower(stripeBTFeeImportID(acc, bt))] = roundCents(stripeAggregateFeeLineAmount(cents))
	}
	return out
}

// detectOdooJournalFeeFixes reconciles the journal's Stripe fee lines against
// the per-charge model. Deprecated aggregate lines — per-payout "…:fees" and
// the rolling "…:open:<stmt>:fees" — are all spurious now (one-time migration
// deletes them). Per-charge "…:fee" lines are corrected if their amount drifted
// and deleted if they no longer correspond to a local charge. Missing per-charge
// fee lines are created by `push`/`sync`, not here.
func detectOdooJournalFeeFixes(creds *OdooCredentials, uid, journalID int, acc *AccountConfig) (odooFeeRepairs, error) {
	var out odooFeeRepairs
	if acc == nil || acc.Provider != "stripe" {
		return out, nil
	}
	bts, err := stripesource.LoadTransactionsSince(DataDir(), acc.AccountID, 0)
	if err != nil {
		return out, fmt.Errorf("load Stripe provider transactions: %v", err)
	}
	expected := expectedStripePerChargeFees(acc, bts)

	rows, err := odooSearchReadAllMaps(creds, uid, "account.bank.statement.line",
		[]interface{}{[]interface{}{"journal_id", "=", journalID}},
		[]string{"id", "date", "amount", "unique_import_id", "move_id", "is_reconciled"},
		"date asc, id asc")
	if err != nil {
		return out, err
	}
	for _, row := range rows {
		id := odooString(row["unique_import_id"])
		lid := strings.ToLower(id)
		isAggregate := strings.HasSuffix(lid, ":fees") // per-payout or :open: rolling
		isPerCharge := strings.HasSuffix(lid, ":fee")
		if id == "" || (!isAggregate && !isPerCharge) {
			continue
		}
		fix := odooAmountFix{
			LineID:       odooInt(row["id"]),
			MoveID:       odooFieldID(row["move_id"]),
			Date:         odooString(row["date"]),
			ImportID:     id,
			OldAmount:    roundCents(odooFloat(row["amount"])),
			IsReconciled: odooBool(row["is_reconciled"]),
		}
		if isAggregate {
			// Deprecated aggregate fee line — always delete.
			out.Spurious = append(out.Spurious, fix)
			continue
		}
		want, ok := expected[lid]
		if !ok {
			out.Spurious = append(out.Spurious, fix)
			continue
		}
		if math.Abs(fix.OldAmount-want) > 0.005 {
			fix.NewAmount = want
			out.Corrections = append(out.Corrections, fix)
		}
	}
	return out, nil
}

// printOdooJournalFeeFixes previews the fee corrections and stale deletions and
// the net journal-balance change they represent.
func printOdooJournalFeeFixes(r odooFeeRepairs) {
	var delta float64
	if len(r.Corrections) > 0 {
		fmt.Printf("\n  %s%s with the wrong aggregate-fee amount:%s\n", Fmt.Yellow, Pluralize(len(r.Corrections), "fee line", ""), Fmt.Reset)
		for _, f := range r.Corrections {
			note := ""
			if f.IsReconciled {
				note = Fmt.Dim + "  (reconciled → will unreconcile)" + Fmt.Reset
			}
			fmt.Printf("    %s%s%s  line #%d  %s → %s%s\n",
				Fmt.Dim, f.Date, Fmt.Reset, f.LineID,
				fmtEURSigned(f.OldAmount), fmtEURSigned(f.NewAmount), note)
			delta += f.NewAmount - f.OldAmount
		}
	}
	if len(r.Spurious) > 0 {
		fmt.Printf("\n  %s%s with no matching expected fee (stale → delete):%s\n", Fmt.Yellow, Pluralize(len(r.Spurious), "fee line", ""), Fmt.Reset)
		for _, f := range r.Spurious {
			fmt.Printf("    %s%s%s  line #%d  %s%s\n",
				Fmt.Dim, f.Date, Fmt.Reset, f.LineID, fmtEURSigned(f.OldAmount), Fmt.Reset)
			delta += -f.OldAmount
		}
	}
	fmt.Printf("\n  %sNet journal balance change: %s%s\n", Fmt.Dim, fmtEURSigned(delta), Fmt.Reset)
}

// countReconciledAmountFixes reports how many of the fixes sit on a reconciled
// line (and so must be unreconciled before the amount can be rewritten).
func countReconciledAmountFixes(fixes []odooAmountFix) int {
	n := 0
	for _, f := range fixes {
		if f.IsReconciled {
			n++
		}
	}
	return n
}

// printOdooJournalAmountFixes previews the wrong lines and the net journal
// balance change they represent.
func printOdooJournalAmountFixes(fixes []odooAmountFix) {
	fmt.Printf("\n  %s%s with the wrong amount:%s\n", Fmt.Yellow, Pluralize(len(fixes), "line", ""), Fmt.Reset)
	var delta float64
	for _, f := range fixes {
		note := ""
		if f.IsReconciled {
			note = Fmt.Dim + "  (reconciled → will unreconcile)" + Fmt.Reset
		}
		fmt.Printf("    %s%s%s  line #%d  %s → %s%s\n",
			Fmt.Dim, f.Date, Fmt.Reset, f.LineID,
			fmtEURSigned(f.OldAmount), fmtEURSigned(f.NewAmount), note)
		delta += f.NewAmount - f.OldAmount
	}
	fmt.Printf("\n  %sNet journal balance change: %s%s\n", Fmt.Dim, fmtEURSigned(delta), Fmt.Reset)
}

// applyOdooJournalAmountFixes writes the corrected amounts in place. Each line
// is repaired with the standard draft → write → repost dance (writing amount on
// a posted move otherwise fails with "vous ne pouvez pas supprimer une écriture
// comptable validée"); reconciled lines are unreconciled first. On success it
// refreshes the local journal cache. Returns ok / failed counts.
func applyOdooJournalAmountFixes(creds *OdooCredentials, uid, journalID int, fixes []odooAmountFix) (okCount, failed int) {
	// Move states up front for the draft/post decision.
	moveStates := map[int]string{}
	moveIDs := make([]interface{}, 0, len(fixes))
	seenMove := map[int]bool{}
	for _, f := range fixes {
		if f.MoveID > 0 && !seenMove[f.MoveID] {
			seenMove[f.MoveID] = true
			moveIDs = append(moveIDs, f.MoveID)
		}
	}
	if len(moveIDs) > 0 {
		if mrows, merr := odooSearchReadAllMaps(creds, uid, "account.move",
			[]interface{}{[]interface{}{"id", "in", moveIDs}}, []string{"id", "state"}, ""); merr == nil {
			for _, r := range mrows {
				if mid := odooInt(r["id"]); mid > 0 {
					moveStates[mid] = odooString(r["state"])
				}
			}
		} else {
			Warnf("    %s⚠ Could not read move states: %v%s", Fmt.Yellow, merr, Fmt.Reset)
		}
	}

	for _, f := range fixes {
		if f.MoveID == 0 {
			Warnf("    %s⚠ line #%d: missing move_id; skipped%s", Fmt.Yellow, f.LineID, Fmt.Reset)
			failed++
			continue
		}
		// A reconciled line can't be drafted/rewritten — break the
		// reconciliation first; the operator re-reconciles afterwards.
		// "No reconciled move lines" is benign: Odoo can flag a line
		// is_reconciled without any reconcile entries on its move (e.g.
		// matched straight against suspense) — the line is writable as-is.
		if f.IsReconciled {
			if err := unreconcileStatementLineMove(creds, uid, odooStatementLineForReconcile{MoveID: f.MoveID}); err != nil && !errors.Is(err, errNoReconciledMoveLines) {
				Warnf("    %s⚠ line #%d unreconcile: %v%s", Fmt.Yellow, f.LineID, err, Fmt.Reset)
				failed++
				continue
			}
		}
		posted := moveStates[f.MoveID] == "posted"
		if posted {
			if _, e := odooExec(creds.URL, creds.DB, uid, creds.Password,
				"account.move", "button_draft", []interface{}{[]interface{}{f.MoveID}}, nil); e != nil {
				Warnf("    %s⚠ line #%d draft: %v%s", Fmt.Yellow, f.LineID, e, Fmt.Reset)
				failed++
				continue
			}
		}
		_, werr := odooExec(creds.URL, creds.DB, uid, creds.Password,
			"account.bank.statement.line", "write",
			[]interface{}{[]interface{}{f.LineID}, map[string]interface{}{"amount": f.NewAmount}},
			map[string]interface{}{"context": map[string]interface{}{"check_move_validity": false}})
		// Re-post even if the write failed, so a partial run never leaves the
		// move stuck in draft.
		if posted {
			if _, e := odooExec(creds.URL, creds.DB, uid, creds.Password,
				"account.move", "action_post", []interface{}{[]interface{}{f.MoveID}}, nil); e != nil {
				Warnf("    %s⚠ line #%d repost: %v%s", Fmt.Yellow, f.LineID, e, Fmt.Reset)
				failed++
				continue
			}
		}
		if werr != nil {
			Warnf("    %s⚠ line #%d write: %v%s", Fmt.Yellow, f.LineID, werr, Fmt.Reset)
			failed++
			continue
		}
		okCount++
	}

	if okCount > 0 {
		if _, e := writeOdooJournalLinesCache(creds, uid, journalID); e != nil {
			Warnf("    %s⚠ journal cache refresh: %v%s", Fmt.Yellow, e, Fmt.Reset)
		}
	}
	return okCount, failed
}

// repairOdooJournalLineAmounts is the focused entry point: detect → preview →
// confirm → apply, for the amount-repair step alone. `chb odoo journals <id>
// fix` runs the same step as part of its broader repair; this shortcut runs
// only it (no orphan/duplicate/metadata scan) when that's all you need.
func repairOdooJournalLineAmounts(creds *OdooCredentials, uid, journalID int, assumeYes, dryRun bool) error {
	if !dryRun {
		if err := RequireOdooWriteCapability(); err != nil {
			return err
		}
	}
	acc := linkedAccountForJournal(journalID)
	if acc == nil {
		return fmt.Errorf("no account linked to Odoo journal #%d. Run: chb accounts <slug> link", journalID)
	}
	printOdooTargetLine(creds)
	fmt.Printf("\n  %sChecking line amounts for journal #%d (%s)…%s\n", Fmt.Bold, journalID, acc.Slug, Fmt.Reset)

	fixes, err := detectOdooJournalAmountFixes(creds, uid, journalID, acc)
	if err != nil {
		return err
	}
	if len(fixes) == 0 {
		fmt.Printf("  %s✓ All matched line amounts already correct — nothing to fix.%s\n\n", Fmt.Green, Fmt.Reset)
		return nil
	}

	printOdooJournalAmountFixes(fixes)
	if dryRun {
		fmt.Printf("  %s(dry-run — no writes)%s\n\n", Fmt.Dim, Fmt.Reset)
		return nil
	}
	if !assumeYes && isInteractiveTTY() {
		fmt.Printf("\n  %sFix %s in Odoo journal #%d? [y/N] %s", Fmt.Bold, Pluralize(len(fixes), "line", ""), journalID, Fmt.Reset)
		reader := bufio.NewReader(os.Stdin)
		resp, _ := reader.ReadString('\n')
		resp = strings.TrimSpace(strings.ToLower(resp))
		if resp != "y" && resp != "yes" {
			fmt.Printf("  Aborted.\n\n")
			return nil
		}
	}

	reconciledN := countReconciledAmountFixes(fixes)
	okCount, failed := applyOdooJournalAmountFixes(creds, uid, journalID, fixes)
	printOdooJournalAmountFixResult(journalID, len(fixes), okCount, failed, reconciledN)
	return nil
}

// printOdooJournalAmountFixResult renders the shared ✓/⚠ summary line after an
// amount-repair pass (used by both `fix` and the focused shortcut).
func printOdooJournalAmountFixResult(journalID, planned, okCount, failed, reconciledN int) {
	mark := Fmt.Green + "✓" + Fmt.Reset
	if failed > 0 {
		mark = Fmt.Yellow + "⚠" + Fmt.Reset
	}
	fmt.Printf("  %s Fixed %d/%d line amount%s", mark, okCount, planned, plural(planned))
	if failed > 0 {
		fmt.Printf(" (%s%d failed%s)", Fmt.Red, failed, Fmt.Reset)
	}
	fmt.Println()
	if reconciledN > 0 {
		fmt.Printf("  %s↳ %s unreconciled to allow the change — run `chb odoo journals %d reconcile` to re-match.%s\n",
			Fmt.Dim, Pluralize(reconciledN, "line", ""), journalID, Fmt.Reset)
	}
	fmt.Println()
}
