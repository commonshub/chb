package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	stripesource "github.com/CommonsHub/chb/sources/stripe"
)

// stripe_odoo_sync implements the chronological BT-iteration sync model.
//
// Model:
//   - The journal always has exactly one "open" statement (reference="open").
//   - Every Stripe balance_transaction becomes a line attached to the
//     currently-open statement, in chronological order.
//   - When a BT is an automatic payout, the open statement is closed:
//     balance_end_real = balance_start + Σ(lines), name/reference set to
//     the payout's descriptor and ID. A new open statement is created,
//     starting at the closing balance.
//   - Manual payouts (automatic=false) are just another line; no close.
//
// Invariants enforced at creation time:
//   - every closed statement: balance_start + Σ(lines) == balance_end_real
//   - every chain: statement_n.balance_start == statement_{n-1}.balance_end_real
//
// As a result, `chb odoo journals <id> fix` should rarely have work to do.

// syncStripeChronological is the implementation. AccountOdooPush routes here
// for Stripe accounts.
func syncStripeChronological(
	acc *AccountConfig,
	creds *OdooCredentials,
	uid int,
	dryRun bool,
	force bool,
	payoutFilter string,
	untilDate time.Time,
) (string, error) {
	if payoutFilter != "" {
		return "", fmt.Errorf("--payout is not supported in the chronological sync model; use --force to reset and resync everything")
	}

	if force && !dryRun {
		if err := emptyOdooJournal(creds, uid, acc.OdooJournalID, acc.OdooJournalName); err != nil {
			return "", err
		}
	}

	// Resume cursor: latest synced BT created time (0 = from the beginning).
	resumeCursor, err := fetchStripeResumeCursor(creds, uid, acc)
	if err != nil {
		return "", fmt.Errorf("determine resume cursor: %v", err)
	}
	if resumeCursor > 0 {
		odooLog("  %sResuming from %s%s\n", Fmt.Dim,
			time.Unix(resumeCursor, 0).Format("2006-01-02 15:04"), Fmt.Reset)
	} else {
		odooLog("  %sNo prior sync — starting from account history%s\n", Fmt.Dim, Fmt.Reset)
	}

	odooLog("  %sLoading archived Stripe source transactions...%s\n", Fmt.Dim, Fmt.Reset)
	bts, err := stripesource.LoadTransactionsSince(DataDir(), acc.AccountID, resumeCursor)
	if err != nil {
		return "", fmt.Errorf("load Stripe source transactions: %v", err)
	}
	odooLog("  %s%d new balance transactions%s\n\n", Fmt.Dim, len(bts), Fmt.Reset)
	if len(bts) == 0 {
		odooLog("  %s✓ Already in sync%s\n\n", Fmt.Green, Fmt.Reset)
		return "already in sync", nil
	}
	if force {
		odooLog("  %sReset rebuild: importing chronologically, skipping per-line reconciliation, using in-memory balances unless Odoo create mismatches occur.%s\n\n",
			Fmt.Dim, Fmt.Reset)
	}

	// Find (or create) the currently-open statement. runningBalance is
	// seeded from the actual line sum so that resumes over a partially-
	// filled open statement produce the correct closing balance.
	openStmtID, _, runningBalance, err := findOrCreateOpenStatement(creds, uid, acc.OdooJournalID, dryRun)
	if err != nil {
		return "", err
	}

	// De-dup against anything already in Odoo (belt & suspenders for
	// partial previous runs).
	existingIDs, _ := fetchOdooImportIDs(creds.URL, creds.DB, uid, creds.Password, acc.OdooJournalID)

	stats := &syncStats{}
	partnerCache := map[string]int{}
	var batch []map[string]interface{}
	feeCents := int64(0)
	feeBTs := 0
	feeStartDate := ""
	feeEndDate := ""
	feeFirstBTID := ""
	feeLastBTID := ""
	processedBTs := 0
	skippedBTs := 0
	payoutsSeen := 0
	createMismatch := false

	resetFeeAccumulator := func() {
		feeCents = 0
		feeBTs = 0
		feeStartDate = ""
		feeEndDate = ""
		feeFirstBTID = ""
		feeLastBTID = ""
	}
	appendAggregateFeeLine := func(paymentRef, importID, date string) {
		if feeCents == 0 {
			resetFeeAccumulator()
			return
		}
		amount := stripeAggregateFeeLineAmount(feeCents)
		runningBalance += amount
		if feeBTs > 0 && feeStartDate != "" && feeEndDate != "" && feeStartDate != feeEndDate {
			paymentRef = fmt.Sprintf("%s (%s to %s)", paymentRef, feeStartDate, feeEndDate)
		}
		batch = append(batch, map[string]interface{}{
			"statement_id":     openStmtID,
			"journal_id":       acc.OdooJournalID,
			"date":             date,
			"payment_ref":      paymentRef,
			"amount":           amount,
			"unique_import_id": importID,
		})
		resetFeeAccumulator()
	}

	flush := func(reason string) {
		if dryRun || len(batch) == 0 {
			batch = nil
			return
		}
		batchLen := len(batch)
		start := time.Now()
		odooLog("    %screating %d statement line(s) in Odoo (%s)...%s\n", Fmt.Dim, batchLen, reason, Fmt.Reset)
		createdIDs, _ := batchCreateStatementLinesWithIDs(creds, uid, batch)
		odooLog("    %screated %d/%d line(s) in %s%s\n", Fmt.Dim, len(createdIDs), batchLen, time.Since(start).Round(time.Second), Fmt.Reset)
		stats.LinesCreated += len(createdIDs)
		stats.LinesSkipped += batchLen - len(createdIDs)
		if len(createdIDs) != batchLen {
			createMismatch = true
		}
		if force {
			// Reset rebuilds are dominated by Odoo writes. Per-line reconciliation is
			// better handled with `chb odoo journals <id> reconcile` after the import.
			odooLog("    %sreset rebuild: skipping per-line reconciliation%s\n", Fmt.Dim, Fmt.Reset)
		} else {
			reconcileStart := time.Now()
			odooLog("    %sreconciling %d new line(s)...%s\n", Fmt.Dim, len(createdIDs), Fmt.Reset)
			reconcileCreatedStatementLines(creds, uid, createdIDs, false, stats)
			odooLog("    %sreconcile pass done in %s%s\n", Fmt.Dim, time.Since(reconcileStart).Round(time.Second), Fmt.Reset)
		}
		batch = nil
	}

	for i, bt := range bts {
		if !untilDate.IsZero() && time.Unix(bt.Created, 0).After(untilDate) {
			break
		}
		processedBTs++
		if processedBTs == 1 || processedBTs%100 == 0 {
			odooLog("  %spreparing Stripe BT %d/%d (%s)%s\n",
				Fmt.Dim, processedBTs, len(bts), time.Unix(bt.Created, 0).In(BrusselsTZ()).Format("2006-01-02"), Fmt.Reset)
		}
		importID := fmt.Sprintf("stripe:%s:%s:0", strings.ToLower(acc.AccountID), strings.ToLower(bt.ID))
		if existingIDs[importID] {
			stats.LinesSkipped++
			skippedBTs++
			continue
		}

		amount := stripeStatementLineAmount(bt)
		runningBalance += amount
		date := time.Unix(bt.Created, 0).In(BrusselsTZ()).Format("2006-01-02")

		line := map[string]interface{}{
			"statement_id":     openStmtID,
			"journal_id":       acc.OdooJournalID,
			"date":             date,
			"payment_ref":      btPaymentRef(bt),
			"amount":           amount,
			"unique_import_id": importID,
		}
		if bt.CustomerName != "" {
			if pid := resolveOdooPartner(creds, uid, bt.CustomerName, bt.CustomerEmail, partnerCache, stats); pid > 0 {
				line["partner_id"] = pid
			}
		}
		batch = append(batch, line)

		updateBTStats(stats, bt, amount)

		if cents, ok := stripeFeeAdjustmentCents(bt); ok {
			feeCents += cents
			feeBTs++
			if feeStartDate == "" {
				feeStartDate = date
			}
			feeEndDate = date
			if feeFirstBTID == "" {
				feeFirstBTID = bt.ID
			}
			feeLastBTID = bt.ID
		}

		// Close the open statement on automatic payout.
		if bt.Type == "payout" && bt.PayoutAutomatic {
			payoutsSeen++
			name, ref := payoutStatementLabels(bt)
			odooLog("  %sPayout %d: %s  (%d/%d BTs)%s\n", Fmt.Dim, payoutsSeen, name, i+1, len(bts), Fmt.Reset)
			feeKey := bt.PayoutID
			if feeKey == "" {
				feeKey = bt.ID
			}
			appendAggregateFeeLine(
				fmt.Sprintf("Stripe fees for payout %s", feeKey),
				fmt.Sprintf("stripe:%s:%s:fees", strings.ToLower(acc.AccountID), strings.ToLower(feeKey)),
				date,
			)
			flush("before payout close")
			closingBalance := runningBalance
			if !dryRun {
				if !force || createMismatch {
					// Re-derive from the authoritative Odoo line sum on incremental
					// syncs. During a reset rebuild, the in-memory sum is authoritative
					// for the lines we just created and avoids two Odoo reads per payout.
					// If any line failed to create, fall back to Odoo's authoritative sum.
					if authoritative, err := statementEndBalance(creds, uid, openStmtID); err == nil {
						closingBalance = authoritative
						runningBalance = authoritative
					}
				}
				closeStart := time.Now()
				odooLog("    %sclosing Odoo statement #%d...%s\n", Fmt.Dim, openStmtID, Fmt.Reset)
				if err := closeOpenStatement(creds, uid, openStmtID, name, ref, closingBalance); err != nil {
					fmt.Printf("    %s✗ Failed to close statement %d: %v%s\n", Fmt.Red, openStmtID, err, Fmt.Reset)
				}
				odooLog("    %sclosed statement in %s%s\n", Fmt.Dim, time.Since(closeStart).Round(time.Second), Fmt.Reset)
			}
			odooLog("  %s✓ Closed %s  (end balance %s)%s\n",
				Fmt.Green, name, fmtEURSigned(closingBalance), Fmt.Reset)
			stats.Statements++
			// Open a new statement for subsequent BTs, chaining from the
			// closing balance.
			if !dryRun {
				openStart := time.Now()
				odooLog("    %sopening next Odoo statement...%s\n", Fmt.Dim, Fmt.Reset)
				newID, err := createOpenStatement(creds, uid, acc.OdooJournalID, closingBalance)
				if err != nil {
					return "", fmt.Errorf("open new statement: %v", err)
				}
				odooLog("    %sopened statement #%d in %s%s\n", Fmt.Dim, newID, time.Since(openStart).Round(time.Second), Fmt.Reset)
				openStmtID = newID
			}
		}
	}

	if feeCents != 0 {
		importID := fmt.Sprintf("stripe:%s:open:%s:%s:fees",
			strings.ToLower(acc.AccountID),
			strings.ToLower(feeFirstBTID),
			strings.ToLower(feeLastBTID),
		)
		date := feeEndDate
		if date == "" {
			date = time.Now().In(BrusselsTZ()).Format("2006-01-02")
		}
		appendAggregateFeeLine("Stripe fees for open statement", importID, date)
	}
	flush("final open statement")

	// Persist the trailing open statement's running balance from the
	// authoritative Odoo line sum so the invariant holds until the next
	// auto-payout closes it.
	if !dryRun {
		end := runningBalance
		if !force || createMismatch {
			if authoritative, err := statementEndBalance(creds, uid, openStmtID); err == nil {
				end = authoritative
			}
		}
		odooLog("  %supdating open statement balance...%s\n", Fmt.Dim, Fmt.Reset)
		if err := setStatementBalanceEndReal(creds, uid, openStmtID, end); err != nil {
			Warnf("  %s⚠ Failed to update open statement balance: %v%s", Fmt.Yellow, err, Fmt.Reset)
		}
	}
	if skippedBTs > 0 || processedBTs > 0 {
		odooLog("  %sprocessed %d Stripe BT(s), skipped %d duplicate(s), closed %d statement(s)%s\n",
			Fmt.Dim, processedBTs, skippedBTs, stats.Statements, Fmt.Reset)
	}

	stats.PayoutsTotal = 0 // recomputed from lines already
	if !quietOdooContext() {
		stats.print()
	}
	warnInvalidStatements(creds, uid, acc.OdooJournalID)
	summary := fmt.Sprintf("%d new transactions uploaded", stats.LinesCreated)
	if stats.Statements > 0 {
		summary = fmt.Sprintf("%d new transactions uploaded, %d statements closed", stats.LinesCreated, stats.Statements)
	}
	return summary, nil
}

// btPaymentRef returns a short human-readable description for a BT line.
func btPaymentRef(bt stripesource.Transaction) string {
	switch bt.Type {
	case "charge", "payment":
		if bt.CustomerName != "" {
			return bt.CustomerName
		}
		if bt.Description != "" {
			return bt.Description
		}
		return "Stripe charge"
	case "refund", "payment_refund":
		if bt.Description != "" {
			return bt.Description
		}
		return "Refund"
	case "payout":
		if bt.PayoutAutomatic {
			return fmt.Sprintf("Auto payout %s", time.Unix(bt.PayoutArrivalDate, 0).Format("2006-01-02"))
		}
		return fmt.Sprintf("Manual payout %s", time.Unix(bt.PayoutArrivalDate, 0).Format("2006-01-02"))
	case "stripe_fee":
		if bt.Description != "" {
			return bt.Description
		}
		return "Stripe fee"
	case "adjustment":
		if bt.Description != "" {
			return bt.Description
		}
		return "Adjustment"
	}
	if bt.Description != "" {
		return bt.Description
	}
	return bt.Type
}

// payoutStatementLabels returns (name, reference) for the closed statement
// representing this automatic payout.
func payoutStatementLabels(bt stripesource.Transaction) (string, string) {
	arrival := time.Unix(bt.PayoutArrivalDate, 0).In(BrusselsTZ()).Format("2006-01-02")
	amount := float64(-bt.Net) / 100.0 // payout BT net is negative
	currency := strings.ToUpper(bt.Currency)
	var name string
	if bt.PayoutBankLast4 != "" {
		name = fmt.Sprintf("%s Stripe → ****%s (%.2f %s)", arrival, bt.PayoutBankLast4, amount, currency)
	} else {
		name = fmt.Sprintf("%s Stripe payout (%.2f %s)", arrival, amount, currency)
	}
	return name, bt.PayoutID
}

// stripeStatementLineAmount returns the amount to write on the Odoo statement
// line. Customer-facing transactions use the gross amount paid/refunded; Stripe
// fees are represented by separate rows, so folding the fee into each charge
// would understate customer revenue and double count fees in the journal view.
func stripeStatementLineAmount(bt stripesource.Transaction) float64 {
	switch bt.Type {
	case "charge", "payment", "refund", "payment_refund":
		return centsToEuros(bt.Amount)
	default:
		return centsToEuros(bt.Net)
	}
}

func stripeFeeAdjustmentCents(bt stripesource.Transaction) (int64, bool) {
	if bt.Fee == 0 {
		return 0, false
	}
	switch bt.Type {
	case "charge", "payment", "refund", "payment_refund":
		return bt.Fee, true
	default:
		return 0, false
	}
}

func stripeAggregateFeeLineAmount(feeCents int64) float64 {
	return -centsToEuros(feeCents)
}

// updateBTStats tallies stats per BT type.
func updateBTStats(s *syncStats, bt stripesource.Transaction, amount float64) {
	switch bt.Type {
	case "charge", "payment":
		s.Charges++
		s.ChargesGross += centsToEuros(bt.Amount)
		s.ChargeFees += centsToEuros(bt.Fee)
	case "refund", "payment_refund":
		s.Refunds++
		s.RefundsTotal += centsToEuros(bt.Amount)
		s.ChargeFees += centsToEuros(bt.Fee)
	case "payout":
		s.PayoutsTotal += amount
	case "stripe_fee", "adjustment":
		s.StripeFees += -amount
	}
}

// ── Odoo statement helpers ──────────────────────────────────────────────────

// findOrCreateOpenStatement returns the ID, balance_start, and current
// running balance (balance_start + Σ existing lines) of the currently-open
// statement. If none exists, one is created with balance_start equal to the
// most recent closed statement's balance_end_real (or 0 if none).
func findOrCreateOpenStatement(creds *OdooCredentials, uid int, journalID int, dryRun bool) (int, float64, float64, error) {
	// Look for a statement marked open (reference="open").
	result, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement", "search_read",
		[]interface{}{[]interface{}{
			[]interface{}{"journal_id", "=", journalID},
			[]interface{}{"reference", "=", "open"},
		}},
		map[string]interface{}{"fields": []string{"id", "balance_start"}, "limit": 1, "order": "id desc"})
	if err == nil {
		var rows []struct {
			ID           int     `json:"id"`
			BalanceStart float64 `json:"balance_start"`
		}
		_ = json.Unmarshal(result, &rows)
		if len(rows) > 0 {
			sum, _ := statementLineSum(creds, uid, rows[0].ID)
			return rows[0].ID, rows[0].BalanceStart, rows[0].BalanceStart + sum, nil
		}
	}

	// None. Find the most recent closed statement's end balance as our start.
	var start float64
	lastResult, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement", "search_read",
		[]interface{}{[]interface{}{
			[]interface{}{"journal_id", "=", journalID},
			[]interface{}{"reference", "!=", "open"},
		}},
		map[string]interface{}{"fields": []string{"balance_end_real"}, "limit": 1, "order": "date desc, id desc"})
	if err == nil {
		var last []struct {
			BalanceEndReal float64 `json:"balance_end_real"`
		}
		_ = json.Unmarshal(lastResult, &last)
		if len(last) > 0 {
			start = last[0].BalanceEndReal
		}
	}

	if dryRun {
		return 0, start, start, nil
	}
	id, err := createOpenStatement(creds, uid, journalID, start)
	if err != nil {
		return 0, 0, 0, err
	}
	return id, start, start, nil
}

// statementEndBalance returns balance_start + Σ(line.amount) for stmtID —
// i.e. the value balance_end_real should hold to satisfy the invariant.
func statementEndBalance(creds *OdooCredentials, uid int, stmtID int) (float64, error) {
	result, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement", "read",
		[]interface{}{[]interface{}{stmtID}, []string{"balance_start"}}, nil)
	if err != nil {
		return 0, err
	}
	var rows []struct {
		BalanceStart float64 `json:"balance_start"`
	}
	if err := json.Unmarshal(result, &rows); err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, fmt.Errorf("statement %d not found", stmtID)
	}
	sum, err := statementLineSum(creds, uid, stmtID)
	if err != nil {
		return 0, err
	}
	return rows[0].BalanceStart + sum, nil
}

// statementLineSum returns Σ(amount) of the lines attached to stmtID.
func statementLineSum(creds *OdooCredentials, uid int, stmtID int) (float64, error) {
	result, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement.line", "read_group",
		[]interface{}{
			[]interface{}{[]interface{}{"statement_id", "=", stmtID}},
			[]string{"amount:sum"},
			[]string{},
		},
		map[string]interface{}{"lazy": false})
	if err != nil {
		return 0, err
	}
	var groups []struct {
		Amount float64 `json:"amount"`
	}
	if err := json.Unmarshal(result, &groups); err != nil {
		return 0, err
	}
	if len(groups) == 0 {
		return 0, nil
	}
	return groups[0].Amount, nil
}

func createOpenStatement(creds *OdooCredentials, uid int, journalID int, balanceStart float64) (int, error) {
	result, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement", "create",
		[]interface{}{[]interface{}{map[string]interface{}{
			"journal_id":       journalID,
			"name":             "Open",
			"reference":        "open",
			"balance_start":    balanceStart,
			"balance_end_real": balanceStart,
		}}}, nil)
	if err != nil {
		return 0, err
	}
	var ids []int
	if err := json.Unmarshal(result, &ids); err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, fmt.Errorf("create returned no id")
	}
	return ids[0], nil
}

func closeOpenStatement(creds *OdooCredentials, uid int, stmtID int, name, ref string, runningBalance float64) error {
	_, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement", "write",
		[]interface{}{[]interface{}{stmtID}, map[string]interface{}{
			"name":             name,
			"reference":        ref,
			"balance_end_real": runningBalance,
		}}, nil)
	return err
}

func setStatementBalanceEndReal(creds *OdooCredentials, uid int, stmtID int, value float64) error {
	_, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement", "write",
		[]interface{}{[]interface{}{stmtID}, map[string]interface{}{
			"balance_end_real": value,
		}}, nil)
	return err
}

// fetchStripeResumeCursor returns the Unix timestamp of the most-recently-
// synced BT for the account, as derived from existing unique_import_id
// values. Returns 0 if nothing is synced yet.
//
// We can't recover a precise timestamp from the import_id alone, so we use
// the max line `date` as a lower bound and subtract a 2-day safety buffer;
// de-dup via existingIDs catches the overlap.
func fetchStripeResumeCursor(creds *OdooCredentials, uid int, acc *AccountConfig) (int64, error) {
	result, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement.line", "search_read",
		[]interface{}{[]interface{}{
			[]interface{}{"journal_id", "=", acc.OdooJournalID},
			[]interface{}{"unique_import_id", "=like", "stripe:%"},
		}},
		map[string]interface{}{"fields": []string{"date"}, "limit": 1, "order": "date desc, id desc"})
	if err != nil {
		return 0, err
	}
	var rows []struct {
		Date odooStr `json:"date"`
	}
	if err := json.Unmarshal(result, &rows); err != nil {
		return 0, err
	}
	if len(rows) == 0 || rows[0].Date == "" {
		return 0, nil
	}
	t, err := time.Parse("2006-01-02", string(rows[0].Date))
	if err != nil {
		return 0, nil
	}
	// 2-day safety buffer to catch BTs whose date was yesterday but
	// created later in the day than our last-synced line.
	return t.AddDate(0, 0, -2).Unix(), nil
}

// AccountStripePending prints a breakdown of balance transactions that have
// accumulated since the most recent payout — what will flow into the next
// payout. Useful for sanity-checking Odoo's trailing balance against
// Stripe's live state (and vs. the dashboard's upcoming-payout forecast).
func AccountStripePending(slug string) error {
	configs := LoadAccountConfigs()
	var acc *AccountConfig
	for i := range configs {
		if strings.EqualFold(configs[i].Slug, slug) {
			acc = &configs[i]
			break
		}
	}
	if acc == nil {
		return fmt.Errorf("account '%s' not found", slug)
	}
	if acc.Provider != "stripe" {
		return fmt.Errorf("account '%s' is not a Stripe account", slug)
	}
	// Walk archived balance_transactions newest-first to find the last payout,
	// then collect every BT created after it.
	fmt.Printf("\n  %sLoading archived Stripe source transactions...%s\n", Fmt.Dim, Fmt.Reset)
	all, err := stripesource.LoadTransactions(DataDir(), acc.AccountID)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("no Stripe source data found; run `chb transactions sync --source stripe --reset` first")
		}
		return err
	}
	var lastPayout *stripesource.Transaction
	var since []stripesource.Transaction
	for i := len(all) - 1; i >= 0; i-- {
		if all[i].Type == "payout" {
			lastPayout = &all[i]
			if i+1 < len(all) {
				since = append(since, all[i+1:]...)
			}
			break
		}
	}
	if lastPayout == nil {
		since = all
	}

	// Accumulate buckets. In Stripe semantics:
	//   charge BT:    amount=+gross,  fee=+per-charge-fee,  net=amount-fee
	//   refund BT:    amount=-gross,  fee=-fee-returned,    net=amount-fee
	//   stripe_fee:   amount=-fee,    fee=0,                net=amount
	//   adjustment:   amount=±x,      fee=0,                net=amount
	// So Σ(BT.net) = Σ(amount) - Σ(fee)  across all BT types.
	var chargesGross, refundsGross int64
	var chargeFees, refundFees, stripeFeeAmt, adjustmentAmt int64
	chargesN, refundsN := 0, 0
	for _, bt := range since {
		switch bt.Type {
		case "charge", "payment":
			chargesN++
			chargesGross += bt.Amount
			chargeFees += bt.Fee
		case "refund", "payment_refund":
			refundsN++
			refundsGross += bt.Amount // already negative
			refundFees += bt.Fee      // signed: negative when Stripe returns the fee
		case "stripe_fee":
			stripeFeeAmt += bt.Amount // negative (outflow from balance)
		case "adjustment":
			adjustmentAmt += bt.Amount
		}
	}

	// Total fees paid to Stripe (positive number = total deduction from balance).
	//   - chargeFees: positive, fees taken from charges
	//   - refundFees: signed, negative when Stripe returned fees on a refund
	//   - stripeFeeAmt: negative, standalone Stripe billing fees — negate to add.
	totalFees := chargeFees + refundFees - stripeFeeAmt
	netSincePayout := chargesGross + refundsGross + adjustmentAmt - totalFees

	// Print
	fmt.Printf("\n  %s%s%s\n", Fmt.Bold, acc.Name, Fmt.Reset)
	if lastPayout != nil {
		fmt.Printf("  %sLast payout: %s (%s)  %s%s\n", Fmt.Dim,
			lastPayout.ID,
			time.Unix(lastPayout.Created, 0).In(BrusselsTZ()).Format("2006-01-02 15:04"),
			fmtEURSigned(centsToEuros(lastPayout.Net)),
			Fmt.Reset)
	} else {
		fmt.Printf("  %sNo prior payout found — all BTs are pending%s\n", Fmt.Dim, Fmt.Reset)
	}
	fmt.Println()

	fmt.Printf("  %sSince last payout (%d BTs):%s\n", Fmt.Bold, len(since), Fmt.Reset)
	fmt.Printf("    Charges      %4d   %s  %sgross paid by customers%s\n",
		chargesN, fmtEURSigned(centsToEuros(chargesGross)), Fmt.Dim, Fmt.Reset)
	fmt.Printf("    Refunds      %4d   %s  %sgross returned to customers%s\n",
		refundsN, fmtEURSigned(centsToEuros(refundsGross)), Fmt.Dim, Fmt.Reset)
	if adjustmentAmt != 0 {
		fmt.Printf("    Adjustments         %s\n", fmtEURSigned(centsToEuros(adjustmentAmt)))
	}
	fmt.Printf("    Fees                %s  %s(charge %s + refund %s + Stripe %s)%s\n",
		fmtEURSigned(centsToEuros(-totalFees)),
		Fmt.Dim,
		fmtEURSigned(centsToEuros(-chargeFees)),
		fmtEURSigned(centsToEuros(-refundFees)),
		fmtEURSigned(centsToEuros(stripeFeeAmt)),
		Fmt.Reset)
	fmt.Printf("    ─────────────────────────\n")
	fmt.Printf("    Net since payout    %s\n", fmtEURSigned(centsToEuros(netSincePayout)))

	// Compare to Odoo's trailing open statement
	creds, err := ResolveOdooCredentials()
	if err == nil {
		uid, _ := odooAuth(creds.URL, creds.DB, creds.Login, creds.Password)
		if uid > 0 && acc.OdooJournalID > 0 {
			_, start, running, err := findOrCreateOpenStatement(creds, uid, acc.OdooJournalID, true)
			if err == nil {
				fmt.Printf("\n  %sOdoo open statement%s\n", Fmt.Bold, Fmt.Reset)
				fmt.Printf("    balance_start:      %-15s\n", fmtEUR(start))
				fmt.Printf("    lines sum:          %-15s\n", fmtEURSigned(running-start))
				fmt.Printf("    current balance:    %-15s\n", fmtEUR(running))
			}
		}
	}
	fmt.Println()
	return nil
}
