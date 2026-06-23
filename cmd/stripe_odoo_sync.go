package cmd

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	stripesource "github.com/CommonsHub/chb/providers/stripe"
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
// stripeSummaryCursorMatched is the sentinel summary value returned
// when the local-first cursor short-circuit kicked in. AccountOdooPush
// matches on it (via strings.Contains) to skip the post-push reconcile.
const stripeSummaryCursorMatched = "already in sync (cursor)"

// destStillMatchesCursor verifies that Odoo's current journal
// aggregate matches the destination state we recorded after the last
// successful push. Returns true when they match. False on confirmed
// drift OR on a cursor missing the dest snapshot (old chb format) —
// in the latter case we deliberately force one full pass so the
// drift-detection invariant is populated. Without that, an existing
// stale cursor would keep short-circuiting forever.
//
// One read_group RPC, same call the post-push stamper uses.
func destStillMatchesCursor(creds *OdooCredentials, uid int, journalID int, cur SyncCursor) bool {
	if cur.DestCount == 0 && cur.DestBalanceCents == 0 {
		// Old cursor format with no destination snapshot. Force a
		// full pass so the next post-push stamp populates the new
		// fields; from then on the short-circuit can be trusted.
		return false
	}
	liveCount, liveBalance, err := odooJournalAggregate(creds, uid, journalID)
	if err != nil {
		// Aggregation failure is transient; treat as "matches" so we
		// don't force a full push on a single flaky RPC.
		return true
	}
	liveCents := int64(math.Round(liveBalance * 100))
	return liveCount == cur.DestCount && liveCents == cur.DestBalanceCents
}

func syncStripeChronological(
	acc *AccountConfig,
	creds *OdooCredentials,
	uid int,
	dryRun bool,
	force bool,
	skipReconciliation bool,
	payoutFilter string,
	sinceDate time.Time,
	untilDate time.Time,
	previewLimit int,
	stages odooSyncStages,
	useHistory bool,
) (string, error) {
	if payoutFilter != "" {
		return "", fmt.Errorf("--payout is not supported in the chronological sync model; use --force to reset and resync everything")
	}

	if force && !dryRun {
		if err := emptyOdooJournal(creds, uid, acc.OdooJournalID, true); err != nil {
			return "", err
		}
	}

	progressVisible := stripeOdooProgressVisible()
	progressStatus := newStatusLine()
	defer progressStatus.Clear()

	// Load every local BT and dedup against the full Odoo import_id set
	// further down. The previous max(date)-2d resume cursor missed BTs
	// that Stripe added retroactively beyond the 2-day buffer (refunds,
	// disputes, late captures), leaving them silently absent from Odoo.
	if progressVisible {
		progressStatus.Update("Stripe transactions: loading local archive...")
	}
	bts, err := stripesource.LoadTransactionsSince(DataDir(), acc.AccountID, 0)
	if err != nil {
		return "", fmt.Errorf("load Stripe provider transactions: %v", err)
	}
	// odooSyncSince journals hold a manual opening entry at the cutoff;
	// window the BT universe up front (before the cursor snapshot and the
	// dedup passes) so pre-cutoff lines are never (re-)created and the
	// saved cursor counts stay consistent with what push considers.
	if cutoff, ok := acc.OdooSyncSinceTime(); ok {
		var windowed []stripesource.Transaction
		for _, bt := range bts {
			if bt.Created >= cutoff.Unix() {
				windowed = append(windowed, bt)
			}
		}
		bts = windowed
	}
	archivedBTs := len(bts)
	bts = filterStripeBTsByDateWindow(bts, sinceDate, untilDate)
	sort.SliceStable(bts, func(i, j int) bool {
		if bts[i].Created == bts[j].Created {
			return strings.ToLower(bts[i].ID) < strings.ToLower(bts[j].ID)
		}
		return bts[i].Created < bts[j].Created
	})
	// Snapshot the full local BT set BEFORE any cursor filtering so we
	// can stamp the post-push cursor correctly: cursor.Count = total
	// local BTs, cursor.LastImportID = latest local BT. Without this,
	// after filterStripeBTsAfterOdooCursor narrows bts, we'd save a
	// cursor reflecting only the just-pushed slice — not what's locally
	// available — and the next sync's short-circuit would always miss.
	totalLocalBTs := len(bts)
	var latestLocalImportID string
	var latestLocalCreated int64
	if totalLocalBTs > 0 {
		latestLocalImportID = stripeBTImportID(acc, bts[totalLocalBTs-1])
		latestLocalCreated = bts[totalLocalBTs-1].Created
	}

	// Local-first cursor short-circuit: if our saved cursor's last
	// import id + count matches what we have locally AND Odoo's
	// destination aggregate still matches what we left there after
	// the last push, there's nothing new to push — exit before any
	// per-BT processing. Catches the common case where Stripe
	// returned no new BTs since last sync. --history / --force /
	// explicit date windows bypass. Caller (AccountOdooPush)
	// recognises the "(cursor)" summary suffix and skips the
	// follow-up auto-reconcile pass.
	//
	// The destination-aggregate check (DestCount / DestBalanceCents)
	// is what stops the cursor from masking external edits to Odoo:
	// without it, a line deleted in Odoo between two syncs lets the
	// cursor wrongly conclude "already in sync" and the dedup pass
	// that would have re-created the line never runs.
	if !useHistory && !force && sinceDate.IsZero() && untilDate.IsZero() && !dryRun && totalLocalBTs > 0 {
		cur := LoadSyncCursor(SyncCursorKeyForStripeAccount(acc.AccountID))
		if cur.LastImportID != "" && cur.Count == totalLocalBTs && cur.LastImportID == latestLocalImportID {
			if destStillMatchesCursor(creds, uid, acc.OdooJournalID, cur) {
				odooLog("  %sStripe: already in sync (cursor — %d BTs)%s\n",
					Fmt.Dim, totalLocalBTs, Fmt.Reset)
				return stripeSummaryCursorMatched, nil
			}
			odooLog("  %sCursor matches local but Odoo journal #%d drifted — running full push%s\n",
				Fmt.Dim, acc.OdooJournalID, Fmt.Reset)
		}
		// The cursor can't vouch for the journal (first run, new local BTs,
		// destination drift, or the last push had create failures and was
		// deliberately not stamped). The watermark filter below would hide
		// any gap BEFORE the latest Odoo line — exactly where failed
		// creates land — so escalate to the full duplicate check. The
		// cursor is re-stamped only by a fully clean push, which restores
		// the cheap short-circuit.
		odooLog("  %sCursor can't confirm journal #%d is complete — using full duplicate check%s\n",
			Fmt.Dim, acc.OdooJournalID, Fmt.Reset)
		useHistory = true
	}

	if !useHistory && sinceDate.IsZero() && untilDate.IsZero() && !(dryRun && force) {
		cursor, cursorErr := fetchLatestStripeOdooImportCursor(creds, uid, acc.OdooJournalID, acc.AccountID)
		if cursorErr != nil {
			Warnf("  %s⚠ Could not read latest Odoo line, using full duplicate check: %v%s", Fmt.Yellow, cursorErr, Fmt.Reset)
			useHistory = true
		} else if cursor.Found {
			filtered, matched := filterStripeBTsAfterOdooCursor(acc, bts, cursor)
			if matched {
				bts = filtered
			} else {
				Warnf("  %s⚠ Latest Odoo import not found locally (%s), using full duplicate check%s",
					Fmt.Yellow, cursor.UniqueImportID, Fmt.Reset)
				useHistory = true
			}
		}
	}
	if progressVisible {
		progressStatus.Update("Stripe transactions: loading local metadata...")
	}
	chargeIndex := loadArchivedStripeCharges(DataDir())
	eventHints := loadLocalStripeEventHints(DataDir())
	progressStatus.Clear()
	windowLabel := Pluralize(len(bts), "selected balance transaction", "")
	if archivedBTs == len(bts) {
		windowLabel = "all " + Pluralize(archivedBTs, "local balance transaction", "")
	}
	modeLabels := []string{}
	if force {
		modeLabels = append(modeLabels, "reset rebuild")
	}
	if skipReconciliation || (stages.Explicit && !stages.Reconcile) {
		modeLabels = append(modeLabels, "reconcile later")
	}
	modeSuffix := ""
	if len(modeLabels) > 0 {
		modeSuffix = " (" + strings.Join(modeLabels, ", ") + ")"
	}
	odooLog("  %sStripe transactions: %s%s%s\n", Fmt.Dim, windowLabel, modeSuffix, Fmt.Reset)
	if len(bts) == 0 {
		odooLog("  %sNo local Stripe balance transactions in selected window.%s\n\n", Fmt.Dim, Fmt.Reset)
		return "no local transactions in selected window", nil
	}
	inlinePartners := stages.Partners && !stages.Explicit
	inlineAccounts := stages.Accounts && !stages.Explicit
	inlineReconcile := stages.Reconcile && !stages.Explicit
	compactStatementLogs := true

	// Find (or create) the currently-open statement. runningBalance is
	// seeded from the actual line sum so that resumes over a partially-
	// filled open statement produce the correct closing balance.
	openStmtID, _, runningBalance, err := findOrCreateOpenStatement(creds, uid, acc.OdooJournalID, dryRun)
	if err != nil {
		return "", err
	}
	if dryRun && force {
		// A reset dry-run previews the rebuild against an empty journal.
		openStmtID = 0
		runningBalance = 0
	}

	// De-dup against anything already in Odoo. Prefer the local journal-lines
	// cache: the push entry just freshness-verified it (verifyOdooJournalCacheFresh)
	// and a --startingBalance convergence refreshes it, and it already holds
	// every line's unique_import_id + id + payment_ref + narration. Reading it
	// replaces a full-journal search_read over the wire (the dominant cost
	// before the first "preparing balance transaction" line on a large journal)
	// with one cheap aggregate check + a local file read. We fall back to the
	// live fetch when no cache exists or it no longer matches Odoo's
	// count+balance. --force skips the cache (the journal is being wiped).
	existingIDs := map[string]bool{}
	var existingRows map[string]map[string]interface{}
	usedCache := false
	if !force {
		if progressVisible {
			progressStatus.Update("Stripe transactions: reading local journal cache...")
		}
		if ids, rows, ok := stripeExistingFromJournalCache(creds, uid, acc.OdooJournalID); ok {
			existingIDs = ids
			existingRows = rows
			usedCache = true
		}
	}
	if !usedCache && !(dryRun && force) {
		if useHistory {
			existingIDs, _ = fetchOdooImportIDs(creds.URL, creds.DB, uid, creds.Password, acc.OdooJournalID)
		} else {
			existingIDs, _ = fetchOdooImportIDsForStripeBTs(creds, uid, acc.OdooJournalID, acc, bts)
		}
	}
	if existingRows == nil {
		var existingImportIDs []string
		for _, bt := range bts {
			importID := stripeBTImportID(acc, bt)
			if existingIDs[importID] {
				existingImportIDs = append(existingImportIDs, importID)
			}
		}
		existingRows, _ = fetchOdooStatementLinesByImportID(creds, uid, existingImportIDs)
	}

	stats := &syncStats{}
	partnerCache := map[string]int{}
	partnerCollectiveTagWarnings := map[string]bool{}
	localPartnerIndex := loadLatestOdooPartnerIndex(DataDir())
	categorizer := NewCategorizer(nil)
	odooMappings, _ := LoadOdooMappings()
	var batch []map[string]interface{}
	var batchAccountCodes []string
	feeCents := int64(0)
	feeBTs := 0
	feeStartDate := ""
	feeEndDate := ""
	processedBTs := 0
	skippedBTs := 0
	existingUpdates := 0
	dryRunCreates := 0
	payoutsSeen := 0
	createMismatch := false
	var dryRunPlan []odooSyncPlanRow

	addDryRunPlan := func(action, date, paymentRef, partner, account string, amount float64, importID string) {
		if !dryRun || quietOdooContext() {
			return
		}
		if previewLimit > 0 && len(dryRunPlan) >= previewLimit {
			return
		}
		dryRunPlan = append(dryRunPlan, odooSyncPlanRow{
			Action:      action,
			Date:        date,
			Description: paymentRef,
			Partner:     partner,
			Account:     account,
			Amount:      amount,
			Currency:    accCurrency(acc),
			Ref:         importID,
		})
	}

	// Fees seen on already-pushed (duplicate) transactions are tracked
	// separately: they're already represented in Odoo (their payout's fee
	// line, or the rolling open-statement fee line), so they must never feed
	// the additive open-statement update below. They're only consulted when
	// rebuilding a payout fee line that is missing entirely.
	dupFeeCents := int64(0)
	dupFeeBTs := 0
	dupFeeStartDate := ""
	dupFeeEndDate := ""

	resetFeeAccumulator := func() {
		feeCents = 0
		feeBTs = 0
		feeStartDate = ""
		feeEndDate = ""
		dupFeeCents = 0
		dupFeeBTs = 0
		dupFeeStartDate = ""
		dupFeeEndDate = ""
	}
	appendAggregateFeeLine := func(paymentRef, importID, date string) {
		if feeCents == 0 {
			resetFeeAccumulator()
			return
		}
		amount := stripeAggregateFeeLineAmount(feeCents)
		narration := buildStripeAggregateFeeNarration(acc, importID, feeCents, feeBTs, feeStartDate, feeEndDate)
		runningBalance += amount
		if feeBTs > 0 && feeStartDate != "" && feeEndDate != "" && feeStartDate != feeEndDate {
			paymentRef = fmt.Sprintf("%s (%s to %s)", paymentRef, feeStartDate, feeEndDate)
		}
		accountCode := ""
		if inlineAccounts {
			accountCode = stripeFeeOdooAccountCode(odooMappings)
		}
		if dryRun {
			addDryRunPlan("create", date, paymentRef, "", accountCode, amount, importID)
		}
		batch = append(batch, map[string]interface{}{
			"statement_id":     openStmtID,
			"journal_id":       acc.OdooJournalID,
			"date":             date,
			"payment_ref":      paymentRef,
			"amount":           amount,
			"unique_import_id": importID,
			"narration":        narration,
		})
		batchAccountCodes = append(batchAccountCodes, accountCode)
		if dryRun {
			stats.LinesCreated++
		}
		resetFeeAccumulator()
	}

	flush := func(reason string) error {
		if dryRun || len(batch) == 0 {
			batch = nil
			batchAccountCodes = nil
			return nil
		}
		batchLen := len(batch)
		start := time.Now()
		if progressVisible {
			progressStatus.Update("Stripe: creating %s in Odoo (%s)...", Pluralize(batchLen, "statement line", ""), reason)
		}
		if !compactStatementLogs {
			odooLog("    %screating %s in Odoo (%s)...%s\n", Fmt.Dim, Pluralize(batchLen, "statement line", ""), reason, Fmt.Reset)
		}
		createResult, _ := batchCreateStatementLinesWithProgressReport(creds, uid, batch, reason)
		createdIDs := createResult.IDs
		if !compactStatementLogs {
			odooLog("    %screated %d/%s in %s%s\n", Fmt.Dim, len(createdIDs), Pluralize(batchLen, "line", ""), time.Since(start).Round(time.Second), Fmt.Reset)
		}
		stats.LinesCreated += len(createdIDs)
		stats.recordCreateFailures(createResult.Failures)
		stats.LinesSkipped += batchLen - len(createdIDs) - len(createResult.Failures)
		if len(createdIDs) != batchLen {
			createMismatch = true
		}
		if inlineAccounts {
			applyStripeBatchAccountCodes(creds, uid, createdIDs, batchAccountCodes, progressStatus, progressVisible)
		}
		if force || skipReconciliation || !inlineReconcile {
			// Reset rebuilds are dominated by Odoo writes. Per-line reconciliation is
			// better handled with `chb odoo journals <id> reconcile` after the import.
			if !compactStatementLogs {
				odooLog("    %sskipping per-line reconciliation%s\n", Fmt.Dim, Fmt.Reset)
			}
		} else {
			reconcileStart := time.Now()
			if progressVisible {
				progressStatus.Update("Stripe: reconciling %s...", Pluralize(len(createdIDs), "new line", ""))
			}
			odooLog("    %sreconciling %s...%s\n", Fmt.Dim, Pluralize(len(createdIDs), "new line", ""), Fmt.Reset)
			reconcileCreatedStatementLines(creds, uid, createdIDs, false, stats)
			odooLog("    %sreconcile pass done in %s%s\n", Fmt.Dim, time.Since(reconcileStart).Round(time.Second), Fmt.Reset)
		}
		batch = nil
		batchAccountCodes = nil
		// Cross-journal collisions are usually a sign of historic data
		// living in a deprecated journal; let the user decide how to free
		// the references before continuing.
		progressStatus.Clear()
		if err := handleStatementLineCrossJournalConflicts(creds, uid, acc.OdooJournalID, createResult.Failures); err != nil {
			return err
		}
		return nil
	}

	// Reset rebuilds of a cutoff journal (odooSyncSince) re-create the
	// opening entry the wipe removed: one manual-style line (no
	// unique_import_id) dated at the cutoff, carrying the locally-computed
	// pre-cutoff balance. It joins the first batch so it lands in the
	// first statement and anchors the running-balance chain.
	if force {
		if cutoff, ok := acc.OdooSyncSinceTime(); ok {
			if opening := accountLocalBalanceBefore(acc, cutoff); opening != 0 {
				date := cutoff.Format("2006-01-02")
				ref := fmt.Sprintf("Solde de départ %s", date)
				runningBalance += opening
				if dryRun {
					addDryRunPlan("create", date, ref, "", "", opening, "(opening balance)")
					stats.LinesCreated++
				} else {
					batch = append(batch, map[string]interface{}{
						"statement_id": openStmtID,
						"journal_id":   acc.OdooJournalID,
						"date":         date,
						"payment_ref":  ref,
						"amount":       opening,
						"narration": fmt.Sprintf(
							"Opening balance computed by CHB from the full local Stripe history: sum of every balance transaction before %s.", date),
					})
					batchAccountCodes = append(batchAccountCodes, "")
				}
			}
		}
	}

	for i, bt := range bts {
		processedBTs++
		if processedBTs == 1 || processedBTs%100 == 0 {
			if progressVisible {
				progressStatus.Update("Stripe: preparing balance transaction %d/%d (%s)",
					processedBTs, len(bts), time.Unix(bt.Created, 0).In(BrusselsTZ()).Format("2006-01-02"))
			}
		}
		importID := stripeBTImportID(acc, bt)
		bt = enrichStripeBTFromCharge(bt, chargeIndex)
		bt = enrichStripeBTFromLocalEvent(bt, eventHints)
		bt.CustomerName = normalizeStripePartnerName(bt.CustomerName, bt.CustomerEmail)
		if existingIDs[importID] {
			date := time.Unix(bt.Created, 0).In(BrusselsTZ()).Format("2006-01-02")
			amount := stripeStatementLineAmount(bt)
			ruleTx := stripeRuleTransaction(acc, bt, amount)
			categorizer.Apply(&ruleTx)
			paymentRef := stripeOdooPaymentRef(bt, ruleTx)
			accountCode := ""
			if inlineAccounts {
				accountCode = stripeOdooAccountCode(bt, ruleTx, odooMappings)
			}
			update := map[string]interface{}{}
			if row := existingRows[importID]; row != nil {
				paymentRefChanged := paymentRef != "" && odooString(row["payment_ref"]) != paymentRef
				if paymentRefChanged {
					update["payment_ref"] = paymentRef
					if narr := buildStripeOdooNarration(acc, bt, ruleTx, importID, amount); narr != "" && odooString(row["narration"]) != narr {
						update["narration"] = narr
					}
				}
			}
			if len(update) > 0 {
				if dryRun {
					addDryRunPlan("update", date, paymentRef, bt.CustomerName, accountCode, amount, importID)
					if len(dryRunPlan) > 0 && dryRunPlan[len(dryRunPlan)-1].Ref == importID {
						dryRunPlan[len(dryRunPlan)-1].Reason = strings.Join(sortedMapKeys(update), ", ")
					}
				} else if row := existingRows[importID]; row != nil {
					if lineID := odooInt(row["id"]); lineID > 0 {
						// Use the metadata-aware wrapper so posted moves get
						// drafted → written → reposted. The bare write fails
						// with Odoo's UserError ("can't modify a validated
						// accounting entry") whenever the bank-statement
						// line's account.move is in 'posted' state, which
						// happens for every reconciled Stripe line.
						moveID := odooFieldID(row["move_id"])
						if err := updateStatementLineFieldsForMetadata(creds, uid, lineID, moveID, update); err != nil {
							Warnf("  %s⚠ Failed to update Stripe line %s: %v%s", Fmt.Yellow, importID, err, Fmt.Reset)
						} else {
							existingUpdates++
						}
					}
				}
			} else if inlineAccounts && dryRun && stripeBTIsFee(bt) && accountCode != "" {
				addDryRunPlan("update", date, paymentRef, bt.CustomerName, accountCode, amount, importID)
				if len(dryRunPlan) > 0 && dryRunPlan[len(dryRunPlan)-1].Ref == importID {
					dryRunPlan[len(dryRunPlan)-1].Reason = "account"
				}
			} else if inlineAccounts && !dryRun && stripeBTIsFee(bt) && accountCode != "" {
				if row := existingRows[importID]; row != nil {
					if lineID := odooInt(row["id"]); lineID > 0 {
						if err := applyOdooMappingAccount(creds, uid, []int{lineID}, accountCode); err != nil {
							Warnf("  %s⚠ Failed to set fee account on %s: %v%s", Fmt.Yellow, importID, err, Fmt.Reset)
						} else {
							existingUpdates++
						}
					}
				}
			} else if dryRun {
				addDryRunPlan("skip", date, paymentRef, bt.CustomerName, accountCode, amount, importID)
			}
			// Keep the payout fee aggregation running across duplicates so a
			// missing aggregate ":fees" line can be rebuilt. A payout's fee
			// line may be absent even though all its transactions were pushed
			// (e.g. its creation was blocked by a since-resolved cross-journal
			// import-id conflict) — and it can only be reconstructed while
			// walking the full transaction stream. Only on --history runs:
			// that's when existingIDs holds the journal's complete id set.
			if useHistory {
				if cents, ok := stripeImplicitChargeFeeCents(bt); ok {
					dupFeeCents += cents
					dupFeeBTs++
					if dupFeeStartDate == "" {
						dupFeeStartDate = date
					}
					dupFeeEndDate = date
				}
				if stripePayoutClosesStatement(bt) {
					feeKey := bt.PayoutID
					if feeKey == "" {
						feeKey = bt.ID
					}
					feeImportID := fmt.Sprintf("stripe:%s:%s:fees", strings.ToLower(acc.AccountID), strings.ToLower(feeKey))
					// The whole period's fees: new transactions accumulate in
					// feeCents (normal path), already-pushed ones in dupFeeCents.
					totalCents := feeCents + dupFeeCents
					if !existingIDs[feeImportID] && totalCents != 0 {
						// Create the missing fee line without a statement_id —
						// it belongs to a long-closed period, not the open
						// statement; `chb odoo journals <id> fix` attaches
						// loose lines to the right statement by date.
						start, end := feeStartDate, feeEndDate
						if start == "" || (dupFeeStartDate != "" && dupFeeStartDate < start) {
							start = dupFeeStartDate
						}
						if end == "" || (dupFeeEndDate != "" && dupFeeEndDate > end) {
							end = dupFeeEndDate
						}
						amount := stripeAggregateFeeLineAmount(totalCents)
						narration := buildStripeAggregateFeeNarration(acc, feeImportID, totalCents, feeBTs+dupFeeBTs, start, end)
						ref := fmt.Sprintf("Stripe fees for payout %s", feeKey)
						if start != "" && end != "" && start != end {
							ref = fmt.Sprintf("%s (%s to %s)", ref, start, end)
						}
						accountCode := ""
						if inlineAccounts {
							accountCode = stripeFeeOdooAccountCode(odooMappings)
						}
						if dryRun {
							addDryRunPlan("create", date, ref, "", accountCode, amount, feeImportID)
							stats.LinesCreated++
						} else {
							batch = append(batch, map[string]interface{}{
								"journal_id":       acc.OdooJournalID,
								"date":             date,
								"payment_ref":      ref,
								"amount":           amount,
								"unique_import_id": feeImportID,
								"narration":        narration,
							})
							batchAccountCodes = append(batchAccountCodes, accountCode)
						}
					}
					resetFeeAccumulator()
				}
			}
			stats.LinesSkipped++
			skippedBTs++
			continue
		}

		amount := stripeStatementLineAmount(bt)
		date := time.Unix(bt.Created, 0).In(BrusselsTZ()).Format("2006-01-02")
		runningBalance += amount
		ruleTx := stripeRuleTransaction(acc, bt, amount)
		categorizer.Apply(&ruleTx)
		paymentRef := stripeOdooPaymentRef(bt, ruleTx)
		accountCode := ""
		if inlineAccounts {
			accountCode = stripeOdooAccountCode(bt, ruleTx, odooMappings)
		}
		if dryRun {
			dryRunCreates++
			stats.LinesCreated++
			addDryRunPlan("create", date, paymentRef, bt.CustomerName, accountCode, amount, importID)
		}

		line := map[string]interface{}{
			"statement_id":     openStmtID,
			"journal_id":       acc.OdooJournalID,
			"date":             date,
			"payment_ref":      paymentRef,
			"amount":           amount,
			"unique_import_id": importID,
			"narration":        buildStripeOdooNarration(acc, bt, ruleTx, importID, amount),
		}
		if inlinePartners && dryRun {
			resolveOdooPartnerFromLocalIndex(localPartnerIndex, bt.CustomerName, bt.CustomerEmail, partnerCache, stats)
		} else if inlinePartners && bt.CustomerName != "" {
			if pid, ambiguous := lookupOdooPartnerFromLocalIndex(localPartnerIndex, bt.CustomerName, bt.CustomerEmail); pid > 0 {
				line["partner_id"] = pid
				stats.PartnersMatched++
				if ambiguous {
					stats.recordPartnerMergeSuggestion(bt.CustomerName, bt.CustomerEmail, pid, localPartnerIndexCandidateIDs(localPartnerIndex, bt.CustomerName, bt.CustomerEmail))
				}
				if err := ensureOdooPartnerCollectiveTag(creds, uid, pid, ruleTx.Collective); err != nil {
					warnKey := fmt.Sprintf("%d:%s", pid, normalizeTransactionTagSlug(ruleTx.Collective))
					if !partnerCollectiveTagWarnings[warnKey] {
						partnerCollectiveTagWarnings[warnKey] = true
						Warnf("  %s⚠ Could not tag Odoo partner #%d with collective %s: %v%s", Fmt.Yellow, pid, ruleTx.Collective, err, Fmt.Reset)
					}
				}
			} else if pid := resolveOdooPartner(creds, uid, bt.CustomerName, bt.CustomerEmail, stringMetadata(bt.Metadata, "stripeCustomerId"), ruleTx.Collective, true, partnerCache, stats); pid > 0 {
				line["partner_id"] = pid
			}
		}
		batch = append(batch, line)
		batchAccountCodes = append(batchAccountCodes, accountCode)

		updateBTStats(stats, bt, amount)

		if cents, ok := stripeImplicitChargeFeeCents(bt); ok {
			feeCents += cents
			feeBTs++
			if feeStartDate == "" {
				feeStartDate = date
			}
			feeEndDate = date
		}

		// Close the open statement on automatic payout. Older local Stripe
		// archives may lack the expanded payout object, so fall back to treating
		// payout balance transactions as statement boundaries when that metadata
		// is absent.
		if stripePayoutClosesStatement(bt) {
			payoutsSeen++
			name, ref := payoutStatementLabels(bt)
			if !compactStatementLogs {
				progressStatus.Clear()
				odooLog("  %sPayout %d: %s  (%d/%d Stripe balance transactions)%s\n", Fmt.Dim, payoutsSeen, name, i+1, len(bts), Fmt.Reset)
			}
			feeKey := bt.PayoutID
			if feeKey == "" {
				feeKey = bt.ID
			}
			appendAggregateFeeLine(
				fmt.Sprintf("Stripe fees for payout %s", feeKey),
				fmt.Sprintf("stripe:%s:%s:fees", strings.ToLower(acc.AccountID), strings.ToLower(feeKey)),
				date,
			)
			if err := flush("before payout close"); err != nil {
				return "", err
			}
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
				if progressVisible {
					progressStatus.Update("Stripe: closing statement %s...", name)
				}
				if !compactStatementLogs {
					odooLog("    %sclosing Odoo statement #%d...%s\n", Fmt.Dim, openStmtID, Fmt.Reset)
				}
				if err := closeOpenStatement(creds, uid, openStmtID, name, ref, closingBalance); err != nil {
					fmt.Printf("    %s✗ Failed to close statement %d: %v%s\n", Fmt.Red, openStmtID, err, Fmt.Reset)
				}
				if !compactStatementLogs {
					odooLog("    %sclosed statement in %s%s\n", Fmt.Dim, time.Since(closeStart).Round(time.Second), Fmt.Reset)
				}
			}
			if !compactStatementLogs {
				odooLog("  %s✓ Closed %s  (end balance %s)%s\n",
					Fmt.Green, name, fmtEURSigned(closingBalance), Fmt.Reset)
			}
			stats.Statements++
			// Open a new statement for subsequent BTs, chaining from the
			// closing balance.
			if !dryRun {
				openStart := time.Now()
				if progressVisible {
					progressStatus.Update("Stripe: opening next Odoo statement...")
				}
				if !compactStatementLogs {
					odooLog("    %sopening next Odoo statement...%s\n", Fmt.Dim, Fmt.Reset)
				}
				newID, err := createOpenStatement(creds, uid, acc.OdooJournalID, closingBalance)
				if err != nil {
					return "", fmt.Errorf("open new statement: %v", err)
				}
				if !compactStatementLogs {
					odooLog("    %sopened statement #%d in %s%s\n", Fmt.Dim, newID, time.Since(openStart).Round(time.Second), Fmt.Reset)
				}
				openStmtID = newID
			}
		}
	}
	progressStatus.Clear()
	if dryRun && !quietOdooContext() {
		if len(dryRunPlan) == 0 {
			odooLog("  %sNo Stripe balance transactions in selected window.%s\n\n", Fmt.Dim, Fmt.Reset)
		} else {
			printOdooDryRunPlanRows(dryRunPlan, accCurrency(acc))
		}
	}

	if feeCents != 0 {
		// Stable importID per open statement: the same statement persists
		// across sync runs until an automatic payout closes it, so we want
		// a single rolling fee line that we update in place rather than a
		// new line per run. See openStatementFeeImportID.
		importID := openStatementFeeImportID(acc.AccountID, openStmtID)
		date := feeEndDate
		if date == "" {
			date = time.Now().In(BrusselsTZ()).Format("2006-01-02")
		}
		paymentRef := "Stripe fees for open statement"
		addAmount := stripeAggregateFeeLineAmount(feeCents)

		if dryRun {
			runningBalance += addAmount
			resetFeeAccumulator()
		} else if existingID, existingAmount, existingMoveID, err := fetchOdooLineByImportID(creds, uid, acc.OdooJournalID, importID); err == nil && existingID > 0 {
			newAmount := existingAmount + addAmount
			runningBalance += addAmount
			// Same posted-move dance as the per-tx update path: the bank
			// statement line's account.move locks once posted, so we draft
			// → write → repost via updateStatementLineFieldsForMetadata.
			if werr := updateStatementLineFieldsForMetadata(creds, uid, existingID, existingMoveID, stripeOpenStatementFeeLineUpdateVals(
				acc, importID, newAmount, feeCents, feeBTs, feeStartDate, feeEndDate,
			)); werr != nil {
				Warnf("  %s⚠ Failed to update open-statement fee line #%d: %v%s", Fmt.Yellow, existingID, werr, Fmt.Reset)
			} else if !compactStatementLogs {
				odooLog("    %supdated open-statement fee line #%d  %s → %s%s\n",
					Fmt.Dim, existingID, fmtEURSigned(existingAmount), fmtEURSigned(newAmount), Fmt.Reset)
			}
			resetFeeAccumulator()
		} else {
			appendAggregateFeeLine(paymentRef, importID, date)
		}
	}
	if err := flush("final open statement"); err != nil {
		return "", err
	}

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
		if progressVisible {
			progressStatus.Update("Stripe: updating open statement balance...")
		}
		if err := setStatementBalanceEndReal(creds, uid, openStmtID, end); err != nil {
			Warnf("  %s⚠ Failed to update open statement balance: %v%s", Fmt.Yellow, err, Fmt.Reset)
		}
	}
	progressStatus.Clear()
	if skippedBTs > 0 || processedBTs > 0 {
		updatePart := ""
		if existingUpdates > 0 {
			updatePart = ", updated " + Pluralize(existingUpdates, "existing line", "")
		}
		createdPart := ""
		if stats.LinesCreated > 0 {
			createdPart = ", created " + Pluralize(stats.LinesCreated, "line", "")
		}
		odooLog("  %sStripe transactions: processed %s%s, skipped %s%s, closed %s%s\n",
			Fmt.Dim, Pluralize(processedBTs, "balance transaction", ""), createdPart, Pluralize(skippedBTs, "duplicate", ""), updatePart, Pluralize(stats.Statements, "statement", ""), Fmt.Reset)
	}

	if !quietOdooContext() {
		stats.printStripeCompact()
	}
	warnInvalidStatements(creds, uid, acc.OdooJournalID)
	var summary string
	switch {
	case dryRun && dryRunCreates > 0:
		summary = fmt.Sprintf("dry-run: %d tx would be uploaded", dryRunCreates)
	case stats.LinesFailed > 0:
		summary = fmt.Sprintf("%d new, %d failed", stats.LinesCreated, stats.LinesFailed)
	case existingUpdates > 0:
		summary = fmt.Sprintf("already in sync, %d existing lines updated", existingUpdates)
	case stats.LinesCreated == 0 && stats.Statements == 0:
		summary = "already in sync"
	case stats.Statements > 0:
		summary = fmt.Sprintf("%d new, %d statements closed", stats.LinesCreated, stats.Statements)
	default:
		summary = fmt.Sprintf("%d new", stats.LinesCreated)
	}
	// Stamp the cursor so the next sync's local-first check can
	// short-circuit. Only when the push fully succeeded, and only
	// when we were processing the full local set (no --since/--until
	// window). Uses the pre-filter snapshot so the cursor reflects
	// what's locally available, not just what we pushed this round.
	// Also records Odoo's post-push aggregate so the next short-
	// circuit can detect destination-side drift (lines deleted /
	// edited in Odoo between syncs).
	// Only a run that actually vouched for the FULL local set may stamp:
	// a full-dedup pass (useHistory) or a reset rebuild (force). A
	// watermark-filtered run only verified the window past the latest Odoo
	// line — stamping from it would baseline any pre-watermark gap (e.g.
	// lines whose create failed in an earlier push) as "expected state",
	// and the short-circuit would hide it forever.
	if !dryRun && stats.LinesFailed == 0 && (useHistory || force) && totalLocalBTs > 0 && sinceDate.IsZero() && untilDate.IsZero() {
		destCount, destBalance, derr := odooJournalAggregate(creds, uid, acc.OdooJournalID)
		cursor := SyncCursor{
			Key:           SyncCursorKeyForStripeAccount(acc.AccountID),
			LastImportID:  latestLocalImportID,
			LastTimestamp: latestLocalCreated,
			Count:         totalLocalBTs,
		}
		if derr == nil {
			cursor.DestCount = destCount
			cursor.DestBalanceCents = int64(math.Round(destBalance * 100))
		}
		_ = SaveSyncCursor(cursor)
	}
	return summary, nil
}

func stripeOdooProgressVisible() bool {
	return !quietOdooContext() || journalRowLayoutActive != nil
}

func filterStripeBTsByDateWindow(bts []stripesource.Transaction, sinceDate, untilDate time.Time) []stripesource.Transaction {
	if sinceDate.IsZero() && untilDate.IsZero() {
		return bts
	}
	out := make([]stripesource.Transaction, 0, len(bts))
	for _, bt := range bts {
		created := time.Unix(bt.Created, 0)
		if !sinceDate.IsZero() && created.Before(sinceDate) {
			continue
		}
		if !untilDate.IsZero() && created.After(untilDate) {
			break
		}
		out = append(out, bt)
	}
	return out
}

func filterStripeBTsAfterOdooCursor(acc *AccountConfig, bts []stripesource.Transaction, cursor odooImportCursor) ([]stripesource.Transaction, bool) {
	if !cursor.Found || cursor.UniqueImportID == "" {
		return bts, false
	}
	lastIdx := -1
	for i, bt := range bts {
		if stripeBTImportID(acc, bt) == cursor.UniqueImportID {
			lastIdx = i
		}
		if stripeFeeImportIDMatchesCursor(acc, bt, cursor.UniqueImportID) {
			lastIdx = i
		}
	}
	if lastIdx == -1 {
		return bts, false
	}
	// Walk back to the first BT on the same Brussels-day as the cursor BT.
	// A previous chunk may have had interleaved successes and failures
	// (e.g. a partial cross-journal collision, a transient Odoo error);
	// Odoo's id-desc cursor then advances past the successes and would
	// silently skip the failures on resume. Re-processing the whole day
	// with the existingIDs dedup catches them without double-importing.
	cursorDay := time.Unix(bts[lastIdx].Created, 0).In(BrusselsTZ()).Format("2006-01-02")
	start := lastIdx
	for start > 0 {
		prevDay := time.Unix(bts[start-1].Created, 0).In(BrusselsTZ()).Format("2006-01-02")
		if prevDay != cursorDay {
			break
		}
		start--
	}
	if start >= len(bts) {
		return []stripesource.Transaction{}, true
	}
	return append([]stripesource.Transaction(nil), bts[start:]...), true
}

func fetchOdooImportIDsForStripeBTs(creds *OdooCredentials, uid int, journalID int, acc *AccountConfig, bts []stripesource.Transaction) (map[string]bool, error) {
	result := map[string]bool{}
	values := make([]string, 0, len(bts))
	seen := map[string]bool{}
	add := func(id string) {
		if id == "" || seen[id] {
			return
		}
		seen[id] = true
		values = append(values, id)
	}
	for _, bt := range bts {
		add(stripeBTImportID(acc, bt))
		if stripePayoutClosesStatement(bt) {
			feeKey := bt.PayoutID
			if feeKey == "" {
				feeKey = bt.ID
			}
			add(fmt.Sprintf("stripe:%s:%s:fees", strings.ToLower(acc.AccountID), strings.ToLower(feeKey)))
		}
	}
	if len(values) == 0 {
		return result, nil
	}
	for start := 0; start < len(values); start += 80 {
		end := start + 80
		if end > len(values) {
			end = len(values)
		}
		chunk := make([]interface{}, 0, end-start)
		for _, id := range values[start:end] {
			chunk = append(chunk, id)
		}
		rows, err := odooSearchReadAllMaps(creds, uid, "account.bank.statement.line",
			[]interface{}{
				[]interface{}{"journal_id", "=", journalID},
				[]interface{}{"unique_import_id", "in", chunk},
			},
			[]string{"unique_import_id"},
			"id asc",
		)
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			if id := odooString(row["unique_import_id"]); id != "" {
				result[id] = true
			}
		}
	}
	return result, nil
}

// stripeExistingFromJournalCache builds the dedup set (unique_import_id →
// present) and the existing-row lookup (import_id → {id, payment_ref,
// narration}) from the local journal-lines cache, so the Stripe push can
// skip the full-journal scan over the wire. The cache is the local mirror
// of the journal's Odoo lines; it carries exactly the three fields the
// dedup/update path reads. Returns ok=false when there's no usable cache
// or it no longer matches Odoo's count+balance (one cheap aggregate RPC via
// journalCacheMatchesOdoo), so the caller falls back to a live fetch.
func stripeExistingFromJournalCache(creds *OdooCredentials, uid, journalID int) (map[string]bool, map[string]map[string]interface{}, bool) {
	lines, ok := loadLatestOdooJournalLinesCache(journalID)
	if !ok || len(lines) == 0 {
		return nil, nil, false
	}
	if !journalCacheMatchesOdoo(creds, uid, journalID, lines) {
		return nil, nil, false
	}
	ids, rows := buildStripeExistingFromCacheLines(lines)
	return ids, rows, true
}

// buildStripeExistingFromCacheLines is the pure mapping from cached journal
// lines to the (dedup set, existing-row lookup) the Stripe push consumes.
// Lines without a unique_import_id (e.g. the manual opening entry) are
// skipped — they never collide with a Stripe BT. The row map mirrors the
// three Odoo fields the update path reads (id, payment_ref, narration), with
// the same Go types odooInt/odooString expect.
func buildStripeExistingFromCacheLines(lines []OdooCacheLine) (map[string]bool, map[string]map[string]interface{}) {
	ids := make(map[string]bool, len(lines))
	rows := make(map[string]map[string]interface{}, len(lines))
	for _, l := range lines {
		if l.UniqueImportID == "" {
			continue
		}
		ids[l.UniqueImportID] = true
		rows[l.UniqueImportID] = map[string]interface{}{
			"id":          l.ID,
			"payment_ref": l.PaymentRef,
			"narration":   l.Narration,
		}
	}
	return ids, rows
}

func stripeBTImportID(acc *AccountConfig, bt stripesource.Transaction) string {
	if acc == nil || bt.ID == "" {
		return ""
	}
	return fmt.Sprintf("stripe:%s:%s", strings.ToLower(acc.AccountID), strings.ToLower(bt.ID))
}

func stripeFeeImportIDMatchesCursor(acc *AccountConfig, bt stripesource.Transaction, importID string) bool {
	if !stripePayoutClosesStatement(bt) {
		return false
	}
	feeKey := bt.PayoutID
	if feeKey == "" {
		feeKey = bt.ID
	}
	return fmt.Sprintf("stripe:%s:%s:fees", strings.ToLower(acc.AccountID), strings.ToLower(feeKey)) == importID
}

func stripeOdooLocalSnapshot(acc *AccountConfig) (accountOdooSyncSnapshot, bool) {
	return stripeOdooLocalSnapshotSince(acc, time.Time{})
}

// stripeOdooLocalSnapshotSince windows the snapshot to BTs at/after the
// cutoff: the line set a cutoff journal (odooSyncSince) is expected to hold
// on top of its manual opening entry. The fee accumulator only sees windowed
// BTs — pre-cutoff fees are part of the opening balance, not a fee line.
func stripeOdooLocalSnapshotSince(acc *AccountConfig, cutoff time.Time) (accountOdooSyncSnapshot, bool) {
	snap := accountOdooSyncSnapshot{
		Label:    "Local Stripe files",
		Currency: accCurrency(acc),
	}
	if acc == nil || acc.AccountID == "" {
		return snap, false
	}
	bts, err := stripesource.LoadTransactionsSince(DataDir(), acc.AccountID, 0)
	if err != nil {
		return snap, false
	}
	if !cutoff.IsZero() {
		var kept []stripesource.Transaction
		for _, bt := range bts {
			if bt.Created >= cutoff.Unix() {
				kept = append(kept, bt)
			}
		}
		bts = kept
	}
	if len(bts) == 0 {
		return snap, true
	}
	feeCents := int64(0)
	addFeeLine := func() {
		if feeCents == 0 {
			return
		}
		snap.TxCount++
		snap.Balance += stripeAggregateFeeLineAmount(feeCents)
		feeCents = 0
	}
	for _, bt := range bts {
		if bt.Created > 0 {
			t := time.Unix(bt.Created, 0)
			if snap.FirstTxAt.IsZero() || t.Before(snap.FirstTxAt) {
				snap.FirstTxAt = t
			}
			if t.After(snap.LastTxAt) {
				snap.LastTxAt = t
			}
		}
		snap.TxCount++
		snap.Balance += stripeStatementLineAmount(bt)
		if cents, ok := stripeImplicitChargeFeeCents(bt); ok {
			feeCents += cents
		}
		if stripePayoutClosesStatement(bt) {
			addFeeLine()
		}
	}
	addFeeLine()
	snap.Balance = roundCents(snap.Balance)
	return snap, true
}

func stripeOdooAccountCode(bt stripesource.Transaction, tx TransactionEntry, odooMappings []OdooMapping) string {
	// Prefer the resolved value from `chb generate`; fall back to a
	// live mapping lookup only when transactions.json predates the
	// AccountCode field (older fixtures).
	if tx.AccountCode != "" {
		return tx.AccountCode
	}
	if matched := LookupOdooMapping(odooMappings, tx); matched != nil {
		return matched.Set.AccountCode
	}
	return ""
}

func stripeFeeOdooAccountCode(odooMappings []OdooMapping) string {
	tx := TransactionEntry{
		Provider: "stripe",
		Type:     "DEBIT",
		Category: "stripe_fee",
	}
	if matched := LookupOdooMapping(odooMappings, tx); matched != nil {
		return matched.Set.AccountCode
	}
	return ""
}

func stripeBTIsFee(bt stripesource.Transaction) bool {
	switch strings.ToLower(bt.Type) {
	case "stripe_fee":
		return true
	}
	text := strings.ToLower(strings.TrimSpace(bt.Description))
	return strings.Contains(text, "stripe fee") ||
		strings.Contains(text, "billing - usage fee") ||
		strings.Contains(text, "automatic taxes")
}

type stripeOdooStageLine struct {
	ID             int
	MoveID         int
	PartnerID      int
	AccountID      int
	CounterpartID  int
	Date           string
	PaymentRef     string
	UniqueImportID string
	Amount         float64
	Metadata       map[string]interface{}
}

type stripeOdooPartnerPlanTarget struct {
	Name       string
	Email      string
	CustomerID string
	Collective string
	LineIDs    []int
}

func stripeOdooPartnerPlanKey(name, email string) string {
	if key := normalizePartnerEmail(email); key != "" {
		return "email:" + key
	}
	return "name:" + normalizePartnerName(name)
}

func stripeOdooStageLineIsAggregateFee(line stripeOdooStageLine) bool {
	if strings.EqualFold(metaString(line.Metadata, "type"), "aggregate_fee") {
		return true
	}
	return strings.HasSuffix(strings.ToLower(strings.TrimSpace(line.UniqueImportID)), ":fees")
}

func fetchStripeOdooStageLines(creds *OdooCredentials, uid int, acc *AccountConfig, since, until time.Time) ([]stripeOdooStageLine, error) {
	if cached, ok := loadLatestOdooJournalLinesCache(acc.OdooJournalID); ok {
		if !quietOdooContext() {
			fmt.Printf("  %sUsing local Odoo journal cache: %s%s\n", Fmt.Dim, odooJournalLinesCachePath(acc.OdooJournalID), Fmt.Reset)
		}
		return filterStripeOdooCacheLines(cached, since, until), nil
	}
	if !quietOdooContext() {
		fmt.Printf("  %sNo local Odoo journal cache found for #%d; fetching lines once from Odoo.%s\n", Fmt.Dim, acc.OdooJournalID, Fmt.Reset)
	}
	cacheLines, err := fetchOdooJournalLinesForCache(creds, uid, acc.OdooJournalID)
	if err != nil {
		return nil, fmt.Errorf("fetch Stripe journal lines: %v", err)
	}
	if _, writeErr := writeOdooJournalLinesCacheFile(acc.OdooJournalID, cacheLines); writeErr != nil {
		Warnf("  %s⚠ Odoo journal cache: %v%s", Fmt.Yellow, writeErr, Fmt.Reset)
	}
	return filterStripeOdooCacheLines(cacheLines, since, until), nil
}

func filterStripeOdooCacheLines(rows []OdooCacheLine, since, until time.Time) []stripeOdooStageLine {
	lines := make([]stripeOdooStageLine, 0, len(rows))
	for _, row := range rows {
		date := row.Date
		if !since.IsZero() || !until.IsZero() {
			lineDate, dateErr := time.Parse("2006-01-02", date)
			if dateErr == nil {
				if !since.IsZero() && lineDate.Before(since) {
					continue
				}
				if !until.IsZero() && !lineDate.Before(until) {
					continue
				}
			}
		}
		meta := row.Metadata
		if meta == nil {
			meta = parseOdooLineNarration(row.Narration)
		}
		importID := firstNonEmpty(row.UniqueImportID, metaString(meta, "uniqueImportId"))
		if !strings.EqualFold(metaString(meta, "provider"), "stripe") && !strings.HasPrefix(strings.ToLower(importID), "stripe:") {
			continue
		}
		lines = append(lines, stripeOdooStageLine{
			ID:             row.ID,
			MoveID:         row.MoveID,
			PartnerID:      row.PartnerID,
			AccountID:      row.AccountID,
			CounterpartID:  row.CounterpartID,
			Date:           date,
			PaymentRef:     row.PaymentRef,
			UniqueImportID: importID,
			Amount:         row.Amount,
			Metadata:       meta,
		})
	}
	return lines
}

func fetchStripeOdooStageLinesFromOdoo(creds *OdooCredentials, uid int, acc *AccountConfig, since, until time.Time) ([]stripeOdooStageLine, error) {
	rows, err := odooSearchReadAllMaps(creds, uid, "account.bank.statement.line",
		[]interface{}{[]interface{}{"journal_id", "=", acc.OdooJournalID}},
		[]string{"id", "partner_id", "move_id", "unique_import_id", "date", "payment_ref", "amount", "narration"},
		"date asc, id asc")
	if err != nil {
		return nil, fmt.Errorf("fetch Stripe journal lines: %v", err)
	}
	lines := make([]stripeOdooStageLine, 0, len(rows))
	for _, row := range rows {
		date := odooString(row["date"])
		if !since.IsZero() || !until.IsZero() {
			lineDate, dateErr := time.Parse("2006-01-02", date)
			if dateErr == nil {
				if !since.IsZero() && lineDate.Before(since) {
					continue
				}
				if !until.IsZero() && !lineDate.Before(until) {
					continue
				}
			}
		}
		meta := stripeOdooLineMetadata(row)
		importID := firstNonEmpty(odooString(row["unique_import_id"]), metaString(meta, "uniqueImportId"))
		if !strings.EqualFold(metaString(meta, "provider"), "stripe") && !strings.HasPrefix(strings.ToLower(importID), "stripe:") {
			continue
		}
		lines = append(lines, stripeOdooStageLine{
			ID:             odooInt(row["id"]),
			MoveID:         odooFieldID(row["move_id"]),
			PartnerID:      odooFieldID(row["partner_id"]),
			AccountID:      0,
			CounterpartID:  0,
			Date:           date,
			PaymentRef:     odooString(row["payment_ref"]),
			UniqueImportID: importID,
			Amount:         odooFloat(row["amount"]),
			Metadata:       meta,
		})
	}
	return lines, nil
}

func stripeOdooLineMetadata(row map[string]interface{}) map[string]interface{} {
	return parseOdooLineNarration(strings.TrimSpace(odooString(row["narration"])))
}

func latestStripePartnerStageSince(creds *OdooCredentials, uid int, journalID int) (time.Time, bool, error) {
	rows, err := odooSearchReadAllMaps(creds, uid, "account.bank.statement.line",
		[]interface{}{
			[]interface{}{"journal_id", "=", journalID},
			[]interface{}{"partner_id", "!=", false},
		},
		[]string{"date", "narration", "unique_import_id"},
		"date desc, id desc")
	if err != nil {
		return time.Time{}, false, err
	}
	for _, row := range rows {
		meta := stripeOdooLineMetadata(row)
		importID := firstNonEmpty(odooString(row["unique_import_id"]), metaString(meta, "uniqueImportId"))
		if !strings.EqualFold(metaString(meta, "provider"), "stripe") && !strings.HasPrefix(strings.ToLower(importID), "stripe:") {
			continue
		}
		date := odooString(row["date"])
		t, parseErr := time.ParseInLocation("2006-01-02", date, BrusselsTZ())
		if parseErr == nil {
			return t, true, nil
		}
	}
	return time.Time{}, false, nil
}

func latestStripePartnerStageSinceFromLocalCache(journalID int) (time.Time, bool, error) {
	rows, ok := loadLatestOdooJournalLinesCache(journalID)
	if !ok {
		return time.Time{}, false, nil
	}
	var latest time.Time
	for _, row := range rows {
		if row.PartnerID <= 0 {
			continue
		}
		meta := row.Metadata
		if meta == nil {
			meta = parseOdooLineNarration(row.Narration)
		}
		importID := firstNonEmpty(row.UniqueImportID, metaString(meta, "uniqueImportId"))
		if !strings.EqualFold(metaString(meta, "provider"), "stripe") && !strings.HasPrefix(strings.ToLower(importID), "stripe:") {
			continue
		}
		t, err := time.ParseInLocation("2006-01-02", row.Date, BrusselsTZ())
		if err != nil {
			continue
		}
		if latest.IsZero() || t.After(latest) {
			latest = t
		}
	}
	if latest.IsZero() {
		return time.Time{}, false, nil
	}
	return latest, true, nil
}

func runQuietOdooStep(fn func() (int, error)) (int, error) {
	wasQuiet := quietOdooContext()
	setQuietOdooContext(true)
	defer setQuietOdooContext(wasQuiet)
	return fn()
}

func fetchStripeOdooStageLinesQuiet(creds *OdooCredentials, uid int, acc *AccountConfig, since, until time.Time) ([]stripeOdooStageLine, error) {
	wasQuiet := quietOdooContext()
	setQuietOdooContext(true)
	defer setQuietOdooContext(wasQuiet)
	return fetchStripeOdooStageLines(creds, uid, acc, since, until)
}

func printStripeOdooPartnerStageHeader(creds *OdooCredentials, acc *AccountConfig, since, until time.Time) {
	file, ok := loadLatestOdooPartnersFile(DataDir())
	partnerLine := "unknown"
	if ok {
		partnerLine = fmt.Sprintf("%d", file.Count)
		if fetched, ageStyle := formatOdooPartnerCacheTimestamp(file.FetchedAt); fetched != "" {
			lastSync := "last sync: " + fetched
			if ageStyle != "" {
				lastSync = ageStyle + lastSync + Fmt.Reset
			}
			partnerLine += " (" + lastSync + ")"
		}
	}
	journalName := OdooJournalName(acc.OdooJournalID)
	if journalName == "" {
		journalName = fmt.Sprintf("journal #%d", acc.OdooJournalID)
	}
	sinceLine := "full history"
	if !since.IsZero() {
		sinceLine = since.Format("2006-01-02")
	}
	if !until.IsZero() {
		sinceLine += " until " + until.AddDate(0, 0, -1).Format("2006-01-02")
	}

	fmt.Println()
	fmt.Printf("  %s %s (db: %s)\n", padRight("Odoo DB:", 10), creds.URL, creds.DB)
	fmt.Printf("  %s %s\n", padRight("Partners:", 10), partnerLine)
	fmt.Printf("  %s %s (#%d)\n", padRight("Journal:", 10), journalName, acc.OdooJournalID)
	fmt.Printf("  %s %s\n\n", padRight("Since:", 10), sinceLine)
}

func printStripeOdooAccountStageHeader(creds *OdooCredentials, acc *AccountConfig, since, until time.Time) {
	journalName := OdooJournalName(acc.OdooJournalID)
	if journalName == "" {
		journalName = fmt.Sprintf("journal #%d", acc.OdooJournalID)
	}
	sinceLine := "full history"
	if !since.IsZero() {
		sinceLine = since.Format("2006-01-02")
	}
	if !until.IsZero() {
		sinceLine += " until " + until.AddDate(0, 0, -1).Format("2006-01-02")
	}

	fmt.Println()
	fmt.Printf("  %s %s (db: %s)\n", padRight("Odoo DB:", 10), creds.URL, creds.DB)
	fmt.Printf("  %s %s (#%d)\n", padRight("Journal:", 10), journalName, acc.OdooJournalID)
	fmt.Printf("  %s %s\n\n", padRight("Since:", 10), sinceLine)
}

func formatOdooPartnerCacheTimestamp(s string) (string, string) {
	t, err := time.Parse(time.RFC3339, strings.TrimSpace(s))
	if err != nil {
		return strings.TrimSpace(s), Fmt.Yellow
	}
	age := time.Since(t)
	style := ""
	switch {
	case age > 24*time.Hour:
		style = Fmt.Red
	case age > time.Hour:
		style = Fmt.Yellow
	}
	return t.In(BrusselsTZ()).Format("2006-01-02 15:04"), style
}

func syncStripeOdooPartnersStage(creds *OdooCredentials, uid int, acc *AccountConfig, since, until time.Time, dryRun bool, previewLimit int, explain bool) (reviewed, updated int, err error) {
	status := newStatusLine()
	defer status.Clear()
	if !quietOdooContext() {
		printStripeOdooPartnerStageHeader(creds, acc, since, until)
	}
	if _, ok := loadLatestOdooJournalLinesCache(acc.OdooJournalID); ok {
		status.Update("Reading local Odoo journal cache...")
	} else {
		status.Update("Fetching Odoo journal lines...")
	}
	lines, err := fetchStripeOdooStageLinesQuiet(creds, uid, acc, since, until)
	if err != nil {
		return 0, 0, err
	}
	fallbackTargets := stripeOdooFallbackTargets(lines, true)
	var localMeta map[string]map[string]interface{}
	loadLocalMeta := func() map[string]map[string]interface{} {
		if localMeta == nil {
			if len(fallbackTargets) == 0 {
				localMeta = map[string]map[string]interface{}{}
				return localMeta
			}
			localMeta = localStripeOdooMetadataByImportID(acc, fallbackTargets)
		}
		return localMeta
	}
	stats := &syncStats{}
	partnerCache := map[string]int{}
	localPartnerIndex := loadLatestOdooPartnerIndex(DataDir())
	if localPartnerIndex == nil {
		return 0, 0, fmt.Errorf("missing local Odoo partner cache; run `chb odoo partners sync` first")
	}
	lineIDsByPartner := map[int][]int{}
	appliedPartners := map[int]int{}
	missingByKey := map[string]stripeOdooPartnerPlanTarget{}
	var missingOrder []string
	collectivesByPartner := map[int]map[string]bool{}
	rememberCollective := func(partnerID int, collective string) {
		collective = normalizeTransactionTagSlug(collective)
		if partnerID <= 0 || collective == "" || localPartnerHasCollectiveTag(localPartnerIndex, partnerID, collective) {
			return
		}
		if collectivesByPartner[partnerID] == nil {
			collectivesByPartner[partnerID] = map[string]bool{}
		}
		collectivesByPartner[partnerID][collective] = true
	}
	for _, line := range lines {
		if line.ID == 0 {
			continue
		}
		if !strings.EqualFold(metaString(line.Metadata, "provider"), "stripe") {
			if meta := loadLocalMeta()[line.UniqueImportID]; meta != nil {
				line.Metadata = meta
			}
		}
		if stripeOdooStageLineIsAggregateFee(line) {
			continue
		}
		reviewed++
		name := normalizeStripePartnerName(metaString(line.Metadata, "customerName"), metaString(line.Metadata, "customerEmail"))
		email := metaString(line.Metadata, "customerEmail")
		customerID := stripeCustomerIDFromLineMetadata(line.Metadata)
		collective := metaString(line.Metadata, "collective")
		if name == "" && email == "" {
			continue
		}
		if line.PartnerID > 0 {
			continue
		}
		partnerID := 0
		if dryRun {
			partnerID = resolveOdooPartnerFromLocalIndex(localPartnerIndex, name, email, partnerCache, stats)
		} else if pid, ambiguous := lookupOdooPartnerFromLocalIndex(localPartnerIndex, name, email); pid > 0 {
			partnerID = pid
			stats.PartnersMatched++
			if ambiguous {
				stats.recordPartnerMergeSuggestion(name, email, pid, localPartnerIndexCandidateIDs(localPartnerIndex, name, email))
			}
			rememberCollective(pid, collective)
		} else {
			key := stripeOdooPartnerPlanKey(name, email)
			if _, ok := missingByKey[key]; !ok {
				missingByKey[key] = stripeOdooPartnerPlanTarget{
					Name:       name,
					Email:      email,
					CustomerID: customerID,
					Collective: collective,
				}
				missingOrder = append(missingOrder, key)
			}
		}
		if partnerID == 0 {
			if !dryRun {
				missing := missingByKey[stripeOdooPartnerPlanKey(name, email)]
				missing.LineIDs = append(missing.LineIDs, line.ID)
				missingByKey[stripeOdooPartnerPlanKey(name, email)] = missing
			}
			continue
		}
		if dryRun {
			if partnerID > 0 {
				updated++
			}
			continue
		}
		if partnerID > 0 {
			lineIDsByPartner[partnerID] = append(lineIDsByPartner[partnerID], line.ID)
		}
	}
	existingPartnerCount := len(lineIDsByPartner)
	newPartnerCount := 0
	for _, key := range missingOrder {
		if len(missingByKey[key].LineIDs) > 0 {
			newPartnerCount++
		}
	}
	if !quietOdooContext() {
		status.Clear()
		fmt.Printf("  %s %d\n", padRight("Lines to process:", 17), reviewed)
		fmt.Printf("  %s %d (%d new, %d updated)\n\n", padRight("Partners:", 17), existingPartnerCount+newPartnerCount, newPartnerCount, existingPartnerCount)
	}
	if !dryRun {
		for i, key := range missingOrder {
			target := missingByKey[key]
			if len(target.LineIDs) == 0 {
				continue
			}
			status.Update("Creating/resolving partners %d/%d...", i+1, len(missingOrder))
			partnerID := resolveOdooPartner(creds, uid, target.Name, target.Email, target.CustomerID, target.Collective, true, partnerCache, stats)
			if partnerID == 0 {
				continue
			}
			lineIDsByPartner[partnerID] = append(lineIDsByPartner[partnerID], target.LineIDs...)
			rememberCollective(partnerID, target.Collective)
		}
		tagPartnerIDs := make([]int, 0, len(collectivesByPartner))
		for partnerID := range collectivesByPartner {
			tagPartnerIDs = append(tagPartnerIDs, partnerID)
		}
		sort.Ints(tagPartnerIDs)
		tagDone := 0
		tagTotal := 0
		for _, partnerID := range tagPartnerIDs {
			tagTotal += len(collectivesByPartner[partnerID])
		}
		for _, partnerID := range tagPartnerIDs {
			collectives := collectivesByPartner[partnerID]
			collectiveNames := make([]string, 0, len(collectives))
			for collective := range collectives {
				collectiveNames = append(collectiveNames, collective)
			}
			sort.Strings(collectiveNames)
			for _, collective := range collectiveNames {
				status.Update("Updating partner tags %d/%d...", tagDone, tagTotal)
				if err := ensureOdooPartnerCollectiveTag(creds, uid, partnerID, collective); err != nil {
					Warnf("  %s⚠ Could not tag Odoo partner #%d with collective %s: %v%s", Fmt.Yellow, partnerID, collective, err, Fmt.Reset)
				}
				tagDone++
				status.Update("Updating partner tags %d/%d...", tagDone, tagTotal)
			}
		}
		partnerIDs := make([]int, 0, len(lineIDsByPartner))
		totalPartnerLinks := 0
		for partnerID, lineIDs := range lineIDsByPartner {
			lineIDs = uniquePositiveInts(lineIDs)
			if len(lineIDs) == 0 {
				continue
			}
			lineIDsByPartner[partnerID] = lineIDs
			partnerIDs = append(partnerIDs, partnerID)
			totalPartnerLinks += len(lineIDs)
		}
		sort.Ints(partnerIDs)
		partnerLinksDone := 0
		for i, partnerID := range partnerIDs {
			lineIDs := lineIDsByPartner[partnerID]
			if len(lineIDs) == 0 {
				continue
			}
			if err := updateStatementLinesPartnerWithProgress(creds, uid, lineIDs, partnerID, status, "Writing partner links", partnerLinksDone, totalPartnerLinks, i+1, len(partnerIDs)); err != nil {
				return reviewed, updated, fmt.Errorf("set partner #%d on %d lines: %v", partnerID, len(lineIDs), err)
			}
			for _, lineID := range lineIDs {
				appliedPartners[lineID] = partnerID
			}
			partnerLinksDone += len(lineIDs)
			updated += len(lineIDs)
		}
		status.Clear()
		if len(appliedPartners) > 0 {
			if err := updateOdooJournalLinesCachePartners(acc.OdooJournalID, appliedPartners); err != nil {
				Warnf("  %s⚠ Odoo journal cache: %v%s", Fmt.Yellow, err, Fmt.Reset)
			}
		}
		if len(missingOrder) > 0 || len(collectivesByPartner) > 0 {
			status.Update("Refreshing Odoo partners...")
			if _, err := runQuietOdooStep(func() (int, error) { return refreshOdooPartnersCache(nil) }); err != nil {
				Warnf("  %s⚠ Odoo partners cache: %v%s", Fmt.Yellow, err, Fmt.Reset)
			}
		}
		if len(appliedPartners) > 0 {
			status.Update("Refreshing Odoo journal lines...")
			if _, err := writeOdooJournalLinesCache(creds, uid, acc.OdooJournalID); err != nil {
				Warnf("  %s⚠ Odoo journal cache: %v%s", Fmt.Yellow, err, Fmt.Reset)
			}
		}
	}
	if !quietOdooContext() {
		if len(stats.Ambiguous) > 0 {
			for _, suggestion := range stats.Ambiguous {
				Warnf("    %s⚠ %s%s", Fmt.Yellow, suggestion, Fmt.Reset)
			}
		}
	}
	return reviewed, updated, nil
}

func syncStripeOdooAccountsStage(creds *OdooCredentials, uid int, acc *AccountConfig, since, until time.Time, dryRun bool) (reviewed, updated int, err error) {
	status := newStatusLine()
	defer status.Clear()
	if !quietOdooContext() {
		printStripeOdooAccountStageHeader(creds, acc, since, until)
	}
	mappings, err := LoadOdooMappings()
	if err != nil {
		return 0, 0, err
	}
	if len(mappings) == 0 {
		if !quietOdooContext() {
			status.Clear()
			fmt.Printf("  %s %d\n", padRight("Lines to process:", 17), 0)
			fmt.Printf("  %s %d (%d updated)\n\n", padRight("Accounts:", 17), 0, 0)
		}
		return 0, 0, nil
	}
	if _, ok := loadLatestOdooJournalLinesCache(acc.OdooJournalID); ok {
		status.Update("Reading local files...")
	} else {
		status.Update("Fetching Odoo journal lines...")
	}
	lines, err := fetchStripeOdooStageLinesQuiet(creds, uid, acc, since, until)
	if err != nil {
		return 0, 0, err
	}
	var missingAccountMoveIDs []int
	for _, line := range lines {
		if line.MoveID > 0 && (line.AccountID == 0 || line.CounterpartID == 0) {
			missingAccountMoveIDs = append(missingAccountMoveIDs, line.MoveID)
		}
	}
	if len(missingAccountMoveIDs) > 0 {
		status.Update("Fetching counterpart move lines for %d Odoo moves...", len(uniquePositiveInts(missingAccountMoveIDs)))
		counterpartByMoveID, err := fetchCounterpartMoveLinesByMoveID(creds, uid, missingAccountMoveIDs)
		if err != nil {
			return 0, 0, err
		}
		for i := range lines {
			info := counterpartByMoveID[lines[i].MoveID]
			if lines[i].AccountID == 0 {
				lines[i].AccountID = info.AccountID
			}
			if lines[i].CounterpartID == 0 {
				lines[i].CounterpartID = info.LineID
			}
		}
		if err := updateOdooJournalLinesCacheCounterparts(acc.OdooJournalID, counterpartByMoveID); err != nil {
			Warnf("  %s⚠ Odoo journal cache: %v%s", Fmt.Yellow, err, Fmt.Reset)
		}
	}
	fallbackTargets := stripeOdooFallbackTargets(lines, false)
	var localMeta map[string]map[string]interface{}
	loadLocalMeta := func() map[string]map[string]interface{} {
		if localMeta == nil {
			if len(fallbackTargets) == 0 {
				localMeta = map[string]map[string]interface{}{}
				return localMeta
			}
			localMeta = localStripeOdooMetadataByImportID(acc, fallbackTargets)
		}
		return localMeta
	}
	accountIDs := map[string]int{}
	updatesByAccount := map[string][]int{}
	counterpartsByAccount := map[string][]int{}
	movesByAccount := map[string][]int{}
	plannedMoveAccountUpdates := map[int]int{}
	for i, line := range lines {
		if i == 0 || (i+1)%250 == 0 || i+1 == len(lines) {
			status.Update("Planning account rules %d/%d...", i+1, len(lines))
		}
		if !strings.EqualFold(metaString(line.Metadata, "provider"), "stripe") {
			if meta := loadLocalMeta()[line.UniqueImportID]; meta != nil {
				line.Metadata = meta
			}
		}
		if line.ID == 0 {
			continue
		}
		tx := stripeOdooLineRuleTransaction(acc, line)
		matched := LookupOdooMapping(mappings, tx)
		if matched == nil || matched.Set.AccountCode == "" {
			continue
		}
		reviewed++
		accountID, ok := accountIDs[matched.Set.AccountCode]
		if !ok {
			accountID, err = findOdooAccountIDByCode(creds, uid, matched.Set.AccountCode)
			if err != nil {
				return reviewed, updated, err
			}
			accountIDs[matched.Set.AccountCode] = accountID
		}
		currentAccountID := line.AccountID
		if currentAccountID == 0 && line.MoveID > 0 {
			status.Update("Fetching counterpart account for line #%d...", line.ID)
			counterpartID, accountID, _ := lookupCounterpartMoveLine(creds, uid, line.MoveID)
			currentAccountID = accountID
			line.CounterpartID = counterpartID
		}
		if currentAccountID == accountID {
			continue
		}
		updatesByAccount[matched.Set.AccountCode] = append(updatesByAccount[matched.Set.AccountCode], line.ID)
		if line.CounterpartID > 0 {
			counterpartsByAccount[matched.Set.AccountCode] = append(counterpartsByAccount[matched.Set.AccountCode], line.CounterpartID)
		}
		if line.MoveID > 0 {
			movesByAccount[matched.Set.AccountCode] = append(movesByAccount[matched.Set.AccountCode], line.MoveID)
		}
		if line.MoveID > 0 {
			plannedMoveAccountUpdates[line.MoveID] = accountID
		}
		updated++
	}
	if !dryRun && len(updatesByAccount) > 0 {
		codes := sortedStringMapKeys(updatesByAccount)
		applied := 0
		for _, accountCode := range codes {
			lineIDs := uniquePositiveInts(updatesByAccount[accountCode])
			status.Update("Applying account %s: %d/%d accounts, %d lines...", accountCode, applied+1, len(codes), len(lineIDs))
			counterpartIDs := uniquePositiveInts(counterpartsByAccount[accountCode])
			moveIDs := uniquePositiveInts(movesByAccount[accountCode])
			accountID := accountIDs[accountCode]
			if len(counterpartIDs) == len(lineIDs) && len(moveIDs) > 0 && accountID > 0 {
				if err := applyOdooMappingAccountBatch(creds, uid, moveIDs, counterpartIDs, accountID, accountCode, status); err != nil {
					return reviewed, applied, err
				}
			} else {
				if err := applyOdooMappingAccount(creds, uid, lineIDs, accountCode, status); err != nil {
					return reviewed, applied, err
				}
			}
			applied += len(lineIDs)
		}
		if err := updateOdooJournalLinesCacheAccounts(acc.OdooJournalID, plannedMoveAccountUpdates); err != nil {
			Warnf("  %s⚠ Odoo journal cache: %v%s", Fmt.Yellow, err, Fmt.Reset)
		}
	}
	if !quietOdooContext() {
		status.Clear()
		fmt.Printf("  %s %d\n", padRight("Lines to process:", 17), reviewed)
		fmt.Printf("  %s %d (%d updated)\n\n", padRight("Accounts:", 17), len(updatesByAccount), updated)
	}
	return reviewed, updated, nil
}

func syncStripeOdooMetadataStage(creds *OdooCredentials, uid int, acc *AccountConfig, since, until time.Time, dryRun, assumeYes bool, previewLimit int) (reviewed, updated int, err error) {
	status := newStatusLine()
	defer status.Clear()
	if !quietOdooContext() {
		printStripeOdooMetadataStageHeader(creds, acc, since, until)
	}
	if _, ok := loadLatestOdooJournalLinesCache(acc.OdooJournalID); ok {
		status.Update("Reading local Odoo journal cache...")
	} else {
		status.Update("Fetching Odoo journal lines...")
	}
	lines, err := fetchStripeOdooStageLinesQuiet(creds, uid, acc, since, until)
	if err != nil {
		return 0, 0, err
	}
	targetIDs := map[string]bool{}
	for _, line := range lines {
		if line.ID == 0 || line.UniqueImportID == "" || stripeOdooStageLineIsAggregateFee(line) {
			continue
		}
		targetIDs[line.UniqueImportID] = true
	}
	status.Update("Reading local Stripe metadata...")
	desired := localStripeOdooDesiredLinesByImportID(acc, targetIDs)
	type metadataUpdate struct {
		LineID     int
		MoveID     int
		UniqueID   string
		Date       string
		PaymentRef string // new value
		Narration  string // new value
		OldRef     string // current value on Odoo (before)
		OldNarr    string // current narration on Odoo (before)
		Changed    []string
	}
	var updates []metadataUpdate
	for i, line := range lines {
		if i == 0 || (i+1)%250 == 0 || i+1 == len(lines) {
			status.Update("Preparing metadata updates %d/%d...", i+1, len(lines))
		}
		if line.ID == 0 || line.UniqueImportID == "" || stripeOdooStageLineIsAggregateFee(line) {
			continue
		}
		want, ok := desired[line.UniqueImportID]
		if !ok || !want.IsFee {
			continue
		}
		reviewed++
		var changed []string
		if want.PaymentRef != "" && line.PaymentRef != want.PaymentRef {
			changed = append(changed, "payment_ref")
		}
		if want.Narration != "" && stripeFeeNarrationNeedsUpdate(line.Metadata, want.Metadata) {
			changed = append(changed, "narration")
		}
		if len(changed) == 0 {
			continue
		}
		oldNarr := ""
		if line.Metadata != nil {
			oldNarr = metaString(line.Metadata, "stripeDescription")
			if oldNarr == "" {
				oldNarr = metaString(line.Metadata, "description")
			}
		}
		updates = append(updates, metadataUpdate{
			LineID:     line.ID,
			MoveID:     line.MoveID,
			UniqueID:   line.UniqueImportID,
			Date:       line.Date,
			PaymentRef: want.PaymentRef,
			Narration:  want.Narration,
			OldRef:     line.PaymentRef,
			OldNarr:    oldNarr,
			Changed:    changed,
		})
	}
	status.Clear()
	if !quietOdooContext() {
		fmt.Printf("  %s %d\n", padRight("Lines reviewed:", 17), reviewed)
		fmt.Printf("  %s %d stale\n\n", padRight("Metadata:", 17), len(updates))
	}

	if len(updates) == 0 {
		if !quietOdooContext() {
			fmt.Printf("  %s↻ metadata stage: %d line%s reviewed, none stale.%s\n\n",
				Fmt.Dim, reviewed, plural(reviewed), Fmt.Reset)
		}
		return reviewed, 0, nil
	}

	// Always print the FROM → TO preview so the operator can verify
	// before any writes happen. Dry-run shows all of them; live mode
	// caps to previewLimit so a 500-line repair doesn't scroll off
	// (the operator confirms once and lets it run).
	if !quietOdooContext() {
		refCount, narrCount := 0, 0
		for _, u := range updates {
			for _, f := range u.Changed {
				switch f {
				case "payment_ref":
					refCount++
				case "narration":
					narrCount++
				}
			}
		}
		fmt.Printf("  %sWill update on each stale line (draft → write → repost):%s\n", Fmt.Dim, Fmt.Reset)
		if refCount > 0 {
			fmt.Printf("    %s• payment_ref%s on %d line%s — short human-readable label\n",
				Fmt.Yellow, Fmt.Reset, refCount, plural(refCount))
		}
		if narrCount > 0 {
			fmt.Printf("    %s• narration%s on %d line%s — structured JSON of the Stripe balance_transaction\n",
				Fmt.Yellow, Fmt.Reset, narrCount, plural(narrCount))
			fmt.Printf("      %s(bt id, type, fee/net, charge/payout id, customer, etc. — used by reports, NOT a duplicate of payment_ref)%s\n",
				Fmt.Dim, Fmt.Reset)
		}
		fmt.Println()

		printLimit := previewLimit
		if dryRun || printLimit <= 0 {
			printLimit = len(updates)
		}
		for i, u := range updates {
			if i >= printLimit {
				break
			}
			fmt.Printf("    %sline #%d%s  %s%s%s  [%s]\n",
				Fmt.Dim, u.LineID, Fmt.Reset,
				Fmt.Dim, u.Date, Fmt.Reset,
				strings.Join(u.Changed, " + "))
			for _, f := range u.Changed {
				switch f {
				case "payment_ref":
					fmt.Printf("      %spayment_ref:%s %s%s%s → %s%s%s\n",
						Fmt.Dim, Fmt.Reset,
						Fmt.Yellow, truncate(u.OldRef, 60), Fmt.Reset,
						Fmt.Green, truncate(u.PaymentRef, 60), Fmt.Reset)
				case "narration":
					printNarrationDiff(u.OldNarr, u.Narration)
				}
			}
		}
		if len(updates) > printLimit {
			fmt.Printf("    %s… and %d more (--dry-run to see all)%s\n",
				Fmt.Dim, len(updates)-printLimit, Fmt.Reset)
		}
	}

	if dryRun {
		if !quietOdooContext() {
			fmt.Printf("\n  %s(dry-run — no writes)%s\n\n", Fmt.Dim, Fmt.Reset)
		}
		return reviewed, len(updates), nil
	}

	// Live mode: confirm before any Odoo writes. --yes / -y skips
	// the prompt; non-TTY without --yes refuses to avoid silent
	// writes from CI / pipelines.
	if !assumeYes && isInteractiveTTY() {
		fmt.Printf("\n  %sApply %d metadata update%s to journal #%d on %s? [Y/n] %s",
			Fmt.Bold, len(updates), plural(len(updates)), acc.OdooJournalID, odooCredsHost(creds), Fmt.Reset)
		reader := bufio.NewReader(os.Stdin)
		resp, _ := reader.ReadString('\n')
		resp = strings.TrimSpace(strings.ToLower(resp))
		if resp == "n" || resp == "no" {
			fmt.Println("  Aborted.")
			return reviewed, 0, nil
		}
	} else if !assumeYes {
		return reviewed, 0, fmt.Errorf("refusing to apply %d metadata update(s) on a non-TTY without --yes", len(updates))
	}

	applied := map[int]stripeOdooDesiredLine{}
	for i, update := range updates {
		status.Update("Writing metadata %d/%d...", i, len(updates))
		vals := map[string]interface{}{}
		for _, field := range update.Changed {
			switch field {
			case "payment_ref":
				vals["payment_ref"] = update.PaymentRef
			case "narration":
				vals["narration"] = update.Narration
			}
		}
		if err := updateStatementLineFieldsForMetadata(creds, uid, update.LineID, update.MoveID, vals); err != nil {
			return reviewed, updated, fmt.Errorf("update metadata for %s: %v", update.UniqueID, err)
		}
		applied[update.LineID] = stripeOdooDesiredLine{PaymentRef: update.PaymentRef, Narration: update.Narration}
		updated++
		status.Update("Writing metadata %d/%d...", i+1, len(updates))
	}
	status.Clear()
	if err := updateOdooJournalLinesCacheMetadata(acc.OdooJournalID, applied); err != nil {
		Warnf("  %s⚠ Odoo journal cache: %v%s", Fmt.Yellow, err, Fmt.Reset)
	}
	if !quietOdooContext() {
		fmt.Printf("\n  %s✓ %d update%s written%s\n\n",
			Fmt.Green, updated, plural(updated), Fmt.Reset)
	}
	return reviewed, updated, nil
}

// odooCredsHost returns just the host of the Odoo URL for prompts —
// "citizenspring-test3.odoo.com" rather than the full URL. Falls back
// to the raw URL when parsing fails.
func odooCredsHost(creds *OdooCredentials) string {
	if creds == nil || creds.URL == "" {
		return ""
	}
	if u, err := url.Parse(creds.URL); err == nil && u.Host != "" {
		return u.Host
	}
	return creds.URL
}

// printNarrationDiff renders the narration FROM → TO with enough
// structure that the operator can see what's actually being stored.
// Both sides are JSON blobs of Stripe BT data; showing them as one
// truncated string makes the change look like a duplicate of
// payment_ref, which it isn't. We render:
//
//	narration:
//	  from: <empty>  OR  json{N keys: …}
//	  to:   json{N keys: balanceTransaction=txn_…, type=…, fee=…, net=…}
//	        description: "Billing - Usage Fee (2026-03-31)"
//
// Salient fields chosen to make it obvious this is the BT object,
// not a description string.
func printNarrationDiff(oldNarr, newNarr string) {
	fmt.Printf("      %snarration:%s\n", Fmt.Dim, Fmt.Reset)
	fmt.Printf("        %sfrom:%s %s%s%s\n",
		Fmt.Dim, Fmt.Reset,
		Fmt.Yellow, narrationSummary(oldNarr, 80), Fmt.Reset)
	fmt.Printf("        %sto:  %s%s%s%s\n",
		Fmt.Dim, Fmt.Reset,
		Fmt.Green, narrationSummary(newNarr, 100), Fmt.Reset)
	// If the new value has a readable description, surface it on a
	// dedicated line so the operator sees the human-friendly part
	// without having to mentally JSON-parse the summary.
	if desc := narrationDescription(newNarr); desc != "" {
		fmt.Printf("        %sdescription:%s %s\n",
			Fmt.Dim, Fmt.Reset, truncate(desc, 80))
	}
}

// narrationSummary renders a JSON narration as
// "json{N keys: balanceTransaction, type, fee, net, …}" — short
// enough for a single line, informative enough to recognise as
// structured Stripe BT data. Non-JSON / empty values pass through
// as-is.
func narrationSummary(narr string, limit int) string {
	if narr == "" {
		return "<empty>"
	}
	var m map[string]interface{}
	if json.Unmarshal([]byte(narr), &m) != nil {
		one := strings.ReplaceAll(strings.ReplaceAll(narr, "\n", " "), "\t", " ")
		return truncate(one, limit)
	}
	if len(m) == 0 {
		return "{}"
	}
	salient := []string{}
	for _, k := range []string{"balanceTransaction", "type", "reportingCategory", "fee", "net", "chargeId", "payoutId", "customerName"} {
		if _, ok := m[k]; ok {
			salient = append(salient, k)
		}
	}
	hint := fmt.Sprintf("json{%d keys", len(m))
	if len(salient) > 0 {
		cap := 4
		if len(salient) < cap {
			cap = len(salient)
		}
		hint += ": " + strings.Join(salient[:cap], ", ")
		if len(salient) > cap {
			hint += ", …"
		}
	}
	hint += "}"
	return truncate(hint, limit)
}

// narrationDescription pulls the human-friendly description field
// out of a JSON narration. Returns "" when the value isn't JSON or
// doesn't carry a description.
func narrationDescription(narr string) string {
	if narr == "" {
		return ""
	}
	var m map[string]interface{}
	if json.Unmarshal([]byte(narr), &m) != nil {
		return ""
	}
	for _, key := range []string{"description", "stripeDescription"} {
		if v, ok := m[key].(string); ok && v != "" {
			return v
		}
	}
	return ""
}

func stripeFeeNarrationNeedsUpdate(current, desired map[string]interface{}) bool {
	if desired == nil {
		return false
	}
	if !strings.EqualFold(metaString(current, "category"), "stripe_fee") {
		return true
	}
	if v, ok := current["stripeFee"].(bool); !ok || !v {
		return true
	}
	if metaString(current, "stripeDescription") != metaString(desired, "stripeDescription") {
		return true
	}
	if metaString(current, "description") != metaString(desired, "description") {
		return true
	}
	return !stringSliceMetadataContains(current, "tags", "stripe_fee")
}

func stringSliceMetadataContains(meta map[string]interface{}, key, want string) bool {
	want = strings.TrimSpace(want)
	if want == "" || meta == nil {
		return false
	}
	switch values := meta[key].(type) {
	case []string:
		for _, value := range values {
			if value == want {
				return true
			}
		}
	case []interface{}:
		for _, value := range values {
			if s, ok := value.(string); ok && s == want {
				return true
			}
		}
	}
	return false
}

func printStripeOdooMetadataStageHeader(creds *OdooCredentials, acc *AccountConfig, since, until time.Time) {
	journalName := OdooJournalName(acc.OdooJournalID)
	if journalName == "" {
		journalName = fmt.Sprintf("journal #%d", acc.OdooJournalID)
	}
	sinceLine := "full history"
	if !since.IsZero() {
		sinceLine = since.Format("2006-01-02")
	}
	if !until.IsZero() {
		sinceLine += " until " + until.AddDate(0, 0, -1).Format("2006-01-02")
	}

	fmt.Println()
	fmt.Printf("  %s %s (db: %s)\n", padRight("Odoo DB:", 10), creds.URL, creds.DB)
	fmt.Printf("  %s %s (#%d)\n", padRight("Journal:", 10), journalName, acc.OdooJournalID)
	fmt.Printf("  %s %s\n\n", padRight("Since:", 10), sinceLine)
}

func stripeOdooLineRuleTransaction(acc *AccountConfig, line stripeOdooStageLine) TransactionEntry {
	txType := "CREDIT"
	if line.Amount < 0 {
		txType = "DEBIT"
	}
	category := metaString(line.Metadata, "category")
	if strings.EqualFold(metaString(line.Metadata, "type"), "aggregate_fee") {
		category = "stripe_fee"
	}
	return TransactionEntry{
		ID:               BuildStripeURI(firstNonEmpty(metaString(line.Metadata, "balanceTransaction"), line.UniqueImportID)),
		Provider:         "stripe",
		AccountSlug:      acc.Slug,
		Account:          acc.AccountID,
		Currency:         strings.ToUpper(metaString(line.Metadata, "currency")),
		Type:             txType,
		Amount:           math.Abs(line.Amount),
		NormalizedAmount: math.Abs(line.Amount),
		GrossAmount:      math.Abs(metaFloat(line.Metadata, "amount")),
		Fee:              math.Abs(metaFloat(line.Metadata, "fee")),
		Counterparty:     normalizeStripePartnerName(metaString(line.Metadata, "customerName"), metaString(line.Metadata, "customerEmail")),
		TxHash:           metaString(line.Metadata, "balanceTransaction"),
		StripeChargeID:   metaString(line.Metadata, "chargeId"),
		Application:      metaString(line.Metadata, "application"),
		Category:         category,
		Collective:       metaString(line.Metadata, "collective"),
		Event:            metaString(line.Metadata, "event"),
		Metadata:         line.Metadata,
	}
}

func stripeOdooFallbackTargets(lines []stripeOdooStageLine, onlyMissingPartner bool) map[string]bool {
	targets := map[string]bool{}
	for _, line := range lines {
		if line.ID == 0 || line.UniqueImportID == "" || strings.HasSuffix(line.UniqueImportID, ":fees") {
			continue
		}
		if onlyMissingPartner && line.PartnerID > 0 {
			continue
		}
		if strings.EqualFold(metaString(line.Metadata, "provider"), "stripe") {
			continue
		}
		targets[line.UniqueImportID] = true
	}
	return targets
}

func sortedStringMapKeys[T any](m map[string]T) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func localStripeOdooMetadataByImportID(acc *AccountConfig, targetIDs map[string]bool) map[string]map[string]interface{} {
	desired := localStripeOdooDesiredLinesByImportID(acc, targetIDs)
	out := map[string]map[string]interface{}{}
	for importID, line := range desired {
		out[importID] = line.Metadata
	}
	return out
}

type stripeOdooDesiredLine struct {
	PaymentRef string
	Narration  string
	Metadata   map[string]interface{}
	IsFee      bool
}

func localStripeOdooDesiredLinesByImportID(acc *AccountConfig, targetIDs map[string]bool) map[string]stripeOdooDesiredLine {
	out := map[string]stripeOdooDesiredLine{}
	if acc == nil || acc.AccountID == "" {
		return out
	}
	if len(targetIDs) == 0 {
		return out
	}
	bts, err := stripesource.LoadTransactionsSince(DataDir(), acc.AccountID, 0)
	if err != nil {
		return out
	}
	sort.SliceStable(bts, func(i, j int) bool {
		if bts[i].Created == bts[j].Created {
			return strings.ToLower(bts[i].ID) < strings.ToLower(bts[j].ID)
		}
		return bts[i].Created < bts[j].Created
	})
	var chargeIndex map[string]*stripesource.Charge
	var eventHints []stripeLocalEventHint
	categorizer := NewCategorizer(nil)
	for _, bt := range bts {
		importID := stripeBTImportID(acc, bt)
		if importID == "" || !targetIDs[importID] {
			continue
		}
		if chargeIndex == nil {
			chargeIndex = loadArchivedStripeCharges(DataDir())
			eventHints = loadLocalStripeEventHints(DataDir())
		}
		bt = enrichStripeBTFromCharge(bt, chargeIndex)
		bt = enrichStripeBTFromLocalEvent(bt, eventHints)
		bt.CustomerName = normalizeStripePartnerName(bt.CustomerName, bt.CustomerEmail)
		amount := stripeStatementLineAmount(bt)
		tx := stripeRuleTransaction(acc, bt, amount)
		categorizer.Apply(&tx)
		narr := buildStripeOdooNarration(acc, bt, tx, importID, amount)
		var meta map[string]interface{}
		if err := json.Unmarshal([]byte(narr), &meta); err == nil {
			out[importID] = stripeOdooDesiredLine{
				PaymentRef: stripeOdooPaymentRef(bt, tx),
				Narration:  narr,
				Metadata:   meta,
				IsFee:      stripeBTIsFee(bt) || strings.EqualFold(tx.Category, "stripe_fee"),
			}
		}
	}
	return out
}

func stripeCustomerIDFromLineMetadata(meta map[string]interface{}) string {
	if v := metaString(meta, "stripeCustomerId"); v != "" {
		return v
	}
	if nested, ok := meta["stripeMetadata"].(map[string]interface{}); ok {
		return metaString(nested, "stripeCustomerId")
	}
	return ""
}

func metaString(meta map[string]interface{}, key string) string {
	if meta == nil {
		return ""
	}
	if v, ok := meta[key]; ok {
		switch x := v.(type) {
		case string:
			return strings.TrimSpace(x)
		case fmt.Stringer:
			return strings.TrimSpace(x.String())
		}
	}
	return ""
}

func metaFloat(meta map[string]interface{}, key string) float64 {
	if meta == nil {
		return 0
	}
	switch x := meta[key].(type) {
	case float64:
		return x
	case float32:
		return float64(x)
	case int:
		return float64(x)
	case int64:
		return float64(x)
	case json.Number:
		v, _ := x.Float64()
		return v
	}
	return 0
}

func applyStripeBatchAccountCodes(creds *OdooCredentials, uid int, createdIDs []int, accountCodes []string, status *statusLine, showProgress bool) {
	if len(createdIDs) != len(accountCodes) {
		return
	}
	total := 0
	for _, accountCode := range accountCodes {
		if accountCode != "" {
			total++
		}
	}
	done := 0
	for i, accountCode := range accountCodes {
		if accountCode == "" {
			continue
		}
		done++
		if showProgress && (done == 1 || done%10 == 0 || done == total) {
			status.Update("Stripe: applying account rules %d/%d...", done, total)
		}
		if err := applyOdooMappingAccount(creds, uid, []int{createdIDs[i]}, accountCode); err != nil {
			Warnf("    %s⚠ rule account %s: %v%s", Fmt.Yellow, accountCode, err, Fmt.Reset)
		}
	}
}

func loadArchivedStripeCharges(dataDir string) map[string]*stripesource.Charge {
	charges := map[string]*stripesource.Charge{}
	years, err := os.ReadDir(dataDir)
	if err != nil {
		return charges
	}
	for _, year := range years {
		if !year.IsDir() {
			continue
		}
		months, err := os.ReadDir(dataDir + "/" + year.Name())
		if err != nil {
			continue
		}
		for _, month := range months {
			if !month.IsDir() {
				continue
			}
			monthCharges, _ := stripesource.LoadChargeData(dataDir, year.Name(), month.Name())
			for id, charge := range monthCharges {
				if charge != nil {
					charges[id] = charge
				}
			}
		}
	}
	return charges
}

func enrichStripeBTFromCharge(bt stripesource.Transaction, chargeIndex map[string]*stripesource.Charge) stripesource.Transaction {
	chargeID := bt.ChargeID
	if chargeID == "" {
		chargeID = stripesource.ExtractChargeID(bt.Source)
	}
	if chargeID == "" {
		return bt
	}
	charge := chargeIndex[chargeID]
	if charge == nil {
		return bt
	}

	bt.ChargeID = chargeID
	if bt.Metadata == nil {
		bt.Metadata = map[string]interface{}{}
	}
	for k, v := range charge.Metadata {
		if strings.TrimSpace(v) != "" {
			bt.Metadata[k] = v
		}
	}
	for k, v := range charge.CustomFields {
		if strings.TrimSpace(v) != "" {
			bt.Metadata["customField."+k] = v
		}
	}
	if charge.PaymentLink != "" {
		bt.Metadata["paymentLink"] = charge.PaymentLink
	}
	if charge.CustomerID != "" {
		bt.Metadata["stripeCustomerId"] = charge.CustomerID
	}
	if charge.ApplicationName != "" {
		bt.Metadata["application"] = charge.ApplicationName
	} else if charge.Application != "" {
		bt.Metadata["application"] = charge.Application
	}
	if bt.Description == "" && charge.Description != "" {
		bt.Description = charge.Description
	}
	if bt.CustomerName == "" {
		bt.CustomerName = firstNonEmpty(charge.CustomerName, charge.BillingName)
	}
	bt.CustomerName = normalizeStripePartnerName(bt.CustomerName, bt.CustomerEmail)
	if bt.CustomerEmail == "" {
		bt.CustomerEmail = firstNonEmpty(charge.CustomerEmail, charge.BillingEmail, charge.ReceiptEmail)
	}
	bt.CustomerName = normalizeStripePartnerName(bt.CustomerName, bt.CustomerEmail)
	return bt
}

type stripeLocalEventHint struct {
	ID         string
	Name       string
	URL        string
	Collective string
}

func loadLocalStripeEventHints(dataDir string) []stripeLocalEventHint {
	var hints []stripeLocalEventHint
	seen := map[string]bool{}
	addFile := func(path string) {
		data, err := os.ReadFile(path)
		if err != nil {
			return
		}
		var file FullEventsFile
		if err := json.Unmarshal(data, &file); err != nil {
			return
		}
		for _, ev := range file.Events {
			if strings.TrimSpace(ev.Name) == "" {
				continue
			}
			key := ev.ID + "\x00" + ev.URL + "\x00" + ev.Name
			if seen[key] {
				continue
			}
			seen[key] = true
			hints = append(hints, stripeLocalEventHint{
				ID:         ev.ID,
				Name:       ev.Name,
				URL:        ev.URL,
				Collective: lumaCollectiveSlugFromURL(ev.URL),
			})
		}
	}

	addFile(filepath.Join(dataDir, "latest", "generated", "events.json"))
	years, err := os.ReadDir(dataDir)
	if err != nil {
		return hints
	}
	for _, year := range years {
		if !year.IsDir() || len(year.Name()) != 4 {
			continue
		}
		months, err := os.ReadDir(filepath.Join(dataDir, year.Name()))
		if err != nil {
			continue
		}
		for _, month := range months {
			if month.IsDir() {
				addFile(filepath.Join(dataDir, year.Name(), month.Name(), "generated", "events.json"))
			}
		}
	}
	return hints
}

func enrichStripeBTFromLocalEvent(bt stripesource.Transaction, hints []stripeLocalEventHint) stripesource.Transaction {
	if len(hints) == 0 || strings.TrimSpace(bt.Description) == "" || stripeBTIsFee(bt) {
		return bt
	}
	if strings.EqualFold(stringMetadata(bt.Metadata, "category"), "ticket") {
		return bt
	}
	desc := normalizeLumaMatchText(bt.Description)
	if desc == "" {
		return bt
	}
	var match stripeLocalEventHint
	for _, hint := range hints {
		name := normalizeLumaMatchText(hint.Name)
		if name == "" || (desc != name && !strings.HasPrefix(desc, name) && !strings.HasPrefix(name, desc)) {
			continue
		}
		if match.Name != "" && match.ID != hint.ID {
			return bt
		}
		match = hint
	}
	if match.Name == "" {
		return bt
	}
	if bt.Metadata == nil {
		bt.Metadata = map[string]interface{}{}
	}
	bt.Metadata["application"] = "luma"
	bt.Metadata["category"] = "ticket"
	bt.Metadata["eventName"] = match.Name
	if match.ID != "" {
		bt.Metadata["eventId"] = match.ID
	}
	if match.URL != "" {
		bt.Metadata["eventUrl"] = match.URL
	}
	if match.Collective != "" {
		bt.Metadata["collective"] = match.Collective
	}
	return bt
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
		payoutDate := bt.PayoutArrivalDate
		if payoutDate == 0 {
			payoutDate = bt.Created
		}
		if bt.PayoutAutomatic {
			return fmt.Sprintf("Auto payout %s", time.Unix(payoutDate, 0).Format("2006-01-02"))
		}
		return fmt.Sprintf("Manual payout %s", time.Unix(payoutDate, 0).Format("2006-01-02"))
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

func stripeOdooPaymentRef(bt stripesource.Transaction, tx TransactionEntry) string {
	if stripeBTIsFee(bt) {
		return stripeFeePaymentRef(bt)
	}
	category := tx.Category
	if category == "" {
		category = stringMetadata(bt.Metadata, "category")
	}
	collective := tx.Collective
	if collective == "" {
		collective = stringMetadata(bt.Metadata, "collective")
	}
	if strings.EqualFold(category, "ticket") {
		eventName := firstNonEmptyStripeMetadata(bt.Metadata, "eventName", "event_name", "eventTitle", "event_title")
		if eventName == "" {
			eventName = firstNonEmptyStripeMetadata(tx.Metadata, "eventName", "event_name", "eventTitle", "event_title")
		}
		if eventName == "" {
			eventName = strings.TrimSpace(bt.Description)
		}
		if eventName != "" {
			return "ticket " + eventName
		}
		return "ticket"
	}
	switch {
	case category != "" && collective != "":
		return category + " " + collective
	case category != "":
		return category
	case collective != "":
		return collective
	case bt.CustomerName != "":
		return bt.CustomerName
	default:
		return btPaymentRef(bt)
	}
}

func stripeFeePaymentRef(bt stripesource.Transaction) string {
	if desc := strings.TrimSpace(bt.Description); desc != "" {
		return desc
	}
	switch strings.ToLower(strings.TrimSpace(bt.Type)) {
	case "stripe_fee":
		return "Stripe fee"
	case "adjustment":
		return "Stripe adjustment"
	default:
		return "Stripe fee"
	}
}

func stripeRuleTransaction(acc *AccountConfig, bt stripesource.Transaction, statementAmount float64) TransactionEntry {
	txType := "CREDIT"
	if statementAmount < 0 {
		txType = "DEBIT"
	}
	metadata := map[string]interface{}{}
	for k, v := range bt.Metadata {
		metadata[k] = v
	}
	if bt.Description != "" {
		metadata["description"] = bt.Description
	}
	if bt.CustomerEmail != "" {
		metadata["email"] = bt.CustomerEmail
	}
	category := stringMetadata(metadata, "category")
	if category == "" && stripeBTIsFee(bt) {
		category = "stripe_fee"
	}
	return TransactionEntry{
		ID:               BuildStripeURI(bt.ID),
		Provider:         "stripe",
		AccountSlug:      acc.Slug,
		Account:          acc.AccountID,
		Currency:         strings.ToUpper(bt.Currency),
		Type:             txType,
		Amount:           math.Abs(statementAmount),
		NormalizedAmount: math.Abs(statementAmount),
		GrossAmount:      math.Abs(centsToEuros(bt.Amount)),
		Fee:              math.Abs(centsToEuros(bt.Fee)),
		Counterparty:     bt.CustomerName,
		Timestamp:        bt.Created,
		TxHash:           bt.ID,
		StripeChargeID:   bt.ChargeID,
		Application:      stringMetadata(metadata, "application"),
		Category:         category,
		Collective:       stringMetadata(metadata, "collective"),
		Event:            firstNonEmptyStripeMetadata(metadata, "event", "eventId", "event_api_id"),
		Metadata:         metadata,
	}
}

func buildStripeOdooNarration(acc *AccountConfig, bt stripesource.Transaction, tx TransactionEntry, importID string, statementAmount float64) string {
	details := map[string]interface{}{
		"provider":           "stripe",
		"account":            strings.ToLower(acc.AccountID),
		"balanceTransaction": bt.ID,
		"uniqueImportId":     importID,
		"type":               bt.Type,
		"reportingCategory":  bt.ReportingCategory,
		"currency":           strings.ToUpper(bt.Currency),
		"amount":             centsToEuros(bt.Amount),
		"fee":                centsToEuros(bt.Fee),
		"net":                centsToEuros(bt.Net),
		"statementAmount":    statementAmount,
	}
	if bt.Created > 0 {
		details["created"] = time.Unix(bt.Created, 0).UTC().Format(time.RFC3339)
	}
	if bt.Description != "" {
		details["stripeDescription"] = bt.Description
		details["description"] = bt.Description
	}
	if stripeBTIsFee(bt) {
		details["stripeFee"] = true
		details["tags"] = []string{"stripe_fee"}
		details["category"] = "stripe_fee"
	}
	if source := compactStripeSource(bt.Source); len(source) > 0 {
		details["stripeSource"] = source
	}
	if bt.CustomerName != "" {
		details["customerName"] = bt.CustomerName
	}
	if bt.CustomerEmail != "" {
		details["customerEmail"] = bt.CustomerEmail
	}
	if bt.ChargeID != "" {
		details["chargeId"] = bt.ChargeID
	}
	if bt.PayoutID != "" {
		details["payoutId"] = bt.PayoutID
	}
	if bt.PayoutBankLast4 != "" {
		details["payoutBankLast4"] = bt.PayoutBankLast4
	}
	if bt.PayoutStatementDescriptor != "" {
		details["payoutStatementDescriptor"] = bt.PayoutStatementDescriptor
	}
	if bt.PayoutArrivalDate > 0 {
		details["payoutArrivalDate"] = time.Unix(bt.PayoutArrivalDate, 0).UTC().Format("2006-01-02")
	}
	if meta := compactStripeMetadata(bt.Metadata); len(meta) > 0 {
		details["stripeMetadata"] = meta
	}
	if tx.Category != "" {
		details["category"] = tx.Category
	}
	if tx.Collective != "" {
		details["collective"] = tx.Collective
	}
	if tx.Event != "" {
		details["event"] = tx.Event
	}
	if tx.Application != "" {
		details["application"] = tx.Application
	}
	data, _ := json.Marshal(details)
	return string(data)
}

func compactStripeSource(source json.RawMessage) map[string]interface{} {
	out := map[string]interface{}{}
	if len(source) == 0 || string(source) == "null" {
		return out
	}
	var value interface{}
	if err := json.Unmarshal(source, &value); err != nil {
		return out
	}
	switch x := value.(type) {
	case string:
		if strings.TrimSpace(x) != "" {
			out["id"] = strings.TrimSpace(x)
		}
	case map[string]interface{}:
		for _, key := range []string{"id", "object", "type", "description", "statement_descriptor"} {
			if v, ok := x[key]; ok {
				if s, ok := v.(string); !ok || strings.TrimSpace(s) != "" {
					out[key] = v
				}
			}
		}
	}
	return out
}

func buildStripeAggregateFeeNarration(acc *AccountConfig, importID string, feeCents int64, feeBTs int, startDate, endDate string) string {
	details := map[string]interface{}{
		"provider":          "stripe",
		"account":           strings.ToLower(acc.AccountID),
		"uniqueImportId":    importID,
		"type":              "aggregate_fee",
		"balanceTxCount":    feeBTs,
		"fee":               centsToEuros(feeCents),
		"statementAmount":   stripeAggregateFeeLineAmount(feeCents),
		"aggregationPeriod": map[string]string{"start": startDate, "end": endDate},
	}
	data, _ := json.Marshal(details)
	return string(data)
}

func stripeOpenStatementFeeLineUpdateVals(acc *AccountConfig, importID string, amount float64, feeCents int64, feeBTs int, startDate, endDate string) map[string]interface{} {
	// Keep the original date on existing statement lines. Odoo posts an
	// accounting move behind the line, and changing its date can violate
	// date-based journal sequences unless the move is manually resequenced.
	return map[string]interface{}{
		"amount":      amount,
		"payment_ref": "Stripe fees for open statement",
		"narration":   buildStripeAggregateFeeNarration(acc, importID, feeCents, feeBTs, startDate, endDate),
	}
}

func compactStripeMetadata(metadata map[string]interface{}) map[string]interface{} {
	out := map[string]interface{}{}
	for k, v := range metadata {
		switch x := v.(type) {
		case string:
			if strings.TrimSpace(x) != "" {
				out[k] = x
			}
		case bool, float64, int, int64, json.Number:
			out[k] = x
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func firstNonEmptyStripeMetadata(metadata map[string]interface{}, keys ...string) string {
	for _, key := range keys {
		if value := stringMetadata(metadata, key); value != "" {
			return value
		}
	}
	return ""
}

// payoutStatementLabels returns (name, reference) for the closed statement
// representing this automatic payout.
func payoutStatementLabels(bt stripesource.Transaction) (string, string) {
	payoutDate := bt.PayoutArrivalDate
	if payoutDate == 0 {
		payoutDate = bt.Created
	}
	arrival := time.Unix(payoutDate, 0).In(BrusselsTZ()).Format("2006-01-02")
	amount := float64(-bt.Net) / 100.0 // payout BT net is negative
	currency := strings.ToUpper(firstNonEmpty(bt.Currency, "EUR"))
	var name string
	if bt.PayoutBankLast4 != "" {
		name = fmt.Sprintf("%s Stripe → ****%s (%.2f %s)", arrival, bt.PayoutBankLast4, amount, currency)
	} else {
		name = fmt.Sprintf("%s Stripe payout (%.2f %s)", arrival, amount, currency)
	}
	return name, bt.PayoutID
}

func stripePayoutClosesStatement(bt stripesource.Transaction) bool {
	if bt.Type != "payout" {
		return false
	}
	if stripePayoutMetadataPresent(bt) {
		return bt.PayoutAutomatic
	}
	return true
}

func stripePayoutMetadataPresent(bt stripesource.Transaction) bool {
	return bt.PayoutID != "" ||
		bt.PayoutArrivalDate != 0 ||
		bt.PayoutStatementDescriptor != "" ||
		bt.PayoutBankLast4 != ""
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

// stripeImplicitChargeFeeCents returns the per-charge processing fee that
// Stripe deducts but does NOT emit as a standalone balance transaction. We
// roll these into a single aggregate fee line per statement. Standalone fee
// BTs (type=stripe_fee, type=adjustment, etc.) are pushed as their own
// Odoo lines and must not be folded into the aggregate.
func stripeImplicitChargeFeeCents(bt stripesource.Transaction) (int64, bool) {
	switch bt.Type {
	case "charge", "payment", "refund", "payment_refund":
		if bt.Fee == 0 {
			return 0, false
		}
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
	case "payout", "payout_cancel":
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
		Amount odooJSONFloat `json:"amount"`
	}
	if err := json.Unmarshal(result, &groups); err != nil {
		return 0, err
	}
	if len(groups) == 0 {
		return 0, nil
	}
	return groups[0].Amount.Float64(), nil
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

// openStatementFeeImportID returns the stable unique_import_id for the
// rolling "Stripe fees for open statement" line. Tied to the open statement's
// Odoo ID so the same line is updated across sync runs (instead of a new
// duplicate line being created on each run, as happened with the prior
// BT-range-based key).
func openStatementFeeImportID(accountID string, openStmtID int) string {
	return fmt.Sprintf("stripe:%s:open:%d:fees", strings.ToLower(accountID), openStmtID)
}

// fetchOdooLineByImportID looks up a single statement line by its unique
// import id. Returns (0, 0, nil) when no line matches.
func fetchOdooLineByImportID(creds *OdooCredentials, uid int, journalID int, importID string) (int, float64, int, error) {
	result, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement.line", "search_read",
		[]interface{}{[]interface{}{
			[]interface{}{"journal_id", "=", journalID},
			[]interface{}{"unique_import_id", "=", importID},
		}},
		map[string]interface{}{"fields": []string{"id", "amount", "move_id"}, "limit": 1})
	if err != nil {
		return 0, 0, 0, err
	}
	var rows []map[string]interface{}
	if err := json.Unmarshal(result, &rows); err != nil {
		return 0, 0, 0, err
	}
	if len(rows) == 0 {
		return 0, 0, 0, nil
	}
	return odooInt(rows[0]["id"]), odooFloat(rows[0]["amount"]), odooFieldID(rows[0]["move_id"]), nil
}

// updateStatementLineFields writes arbitrary fields onto an existing
// statement line.
func updateStatementLineFields(creds *OdooCredentials, uid int, lineID int, vals map[string]interface{}) error {
	_, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement.line", "write",
		[]interface{}{[]interface{}{lineID}, vals}, nil)
	return err
}

func updateStatementLineFieldsForMetadata(creds *OdooCredentials, uid int, lineID, moveID int, vals map[string]interface{}) error {
	if lineID <= 0 || len(vals) == 0 {
		return nil
	}
	if moveID <= 0 {
		return updateStatementLineFields(creds, uid, lineID, vals)
	}
	wasPosted, err := odooMoveIsPosted(creds, uid, moveID)
	if err != nil {
		return err
	}
	if wasPosted {
		if _, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
			"account.move", "button_draft",
			[]interface{}{[]interface{}{moveID}}, nil); err != nil {
			return fmt.Errorf("reset move #%d to draft: %v", moveID, err)
		}
	}
	if err := updateStatementLineFields(creds, uid, lineID, vals); err != nil {
		if wasPosted {
			_, _ = odooExec(creds.URL, creds.DB, uid, creds.Password,
				"account.move", "action_post",
				[]interface{}{[]interface{}{moveID}}, nil)
		}
		return err
	}
	if wasPosted {
		if _, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
			"account.move", "action_post",
			[]interface{}{[]interface{}{moveID}}, nil); err != nil {
			return fmt.Errorf("repost move #%d: %v", moveID, err)
		}
	}
	return nil
}

func odooMoveIsPosted(creds *OdooCredentials, uid int, moveID int) (bool, error) {
	rows, err := odooReadMapsByIDs(creds, uid, "account.move", []int{moveID}, []string{"state"})
	if err != nil {
		return false, err
	}
	if len(rows) == 0 {
		return false, fmt.Errorf("move #%d not found", moveID)
	}
	return odooString(rows[0]["state"]) == "posted", nil
}

type counterpartMoveLineInfo struct {
	LineID      int
	AccountID   int
	AccountType string // "asset_receivable", "liability_payable", "income", "expense", …
}

func fetchCounterpartAccountIDsByMoveID(creds *OdooCredentials, uid int, moveIDs []int) (map[int]int, error) {
	infos, err := fetchCounterpartMoveLinesByMoveID(creds, uid, moveIDs)
	result := map[int]int{}
	for moveID, info := range infos {
		result[moveID] = info.AccountID
	}
	return result, err
}

func fetchCounterpartMoveLinesByMoveID(creds *OdooCredentials, uid int, moveIDs []int) (map[int]counterpartMoveLineInfo, error) {
	result := map[int]counterpartMoveLineInfo{}
	moveIDs = uniquePositiveInts(moveIDs)
	if len(moveIDs) == 0 {
		return result, nil
	}
	rows, err := odooSearchReadAllMaps(creds, uid, "account.move.line",
		[]interface{}{[]interface{}{"move_id", "in", intsToInterfaces(moveIDs)}},
		[]string{"id", "move_id", "account_id", "account_type"},
		"id asc")
	if err != nil {
		return result, err
	}
	for _, row := range rows {
		moveID := odooFieldID(row["move_id"])
		if moveID <= 0 || result[moveID].AccountID > 0 {
			continue
		}
		t := odooString(row["account_type"])
		if t == "asset_cash" || t == "liability_credit_card" {
			continue
		}
		result[moveID] = counterpartMoveLineInfo{
			LineID:      odooInt(row["id"]),
			AccountID:   odooFieldID(row["account_id"]),
			AccountType: t,
		}
	}
	return result, nil
}

func applyOdooMappingAccountBatch(creds *OdooCredentials, uid int, moveIDs, counterpartIDs []int, accountID int, accountCode string, status *statusLine) error {
	moveIDs = uniquePositiveInts(moveIDs)
	counterpartIDs = uniquePositiveInts(counterpartIDs)
	if len(moveIDs) == 0 || len(counterpartIDs) == 0 || accountID <= 0 {
		return nil
	}
	// Only POSTED moves can (and must) be reset to draft before rewriting a
	// non-bank line, then reposted. A move that is already DRAFT is written
	// directly: Odoo rejects button_draft on a draft move with "Seules les
	// pièces comptabilisées/annulées peuvent être remises en brouillon" — the
	// error a fresh push hits, since its just-created statement-line moves are
	// still draft. CANCELLED moves are skipped; we never mutate a cancelled
	// entry (matches withOdooMoveTemporarilyDraft's policy).
	stateRows, err := odooReadMapsByIDs(creds, uid, "account.move", moveIDs, []string{"id", "state"})
	if err != nil {
		return fmt.Errorf("read move states for account %s: %v", accountCode, err)
	}
	moveState := make(map[int]string, len(stateRows))
	for _, r := range stateRows {
		moveState[odooInt(r["id"])] = odooString(r["state"])
	}
	var postedMoves []int
	cancelledMoves := map[int]bool{}
	for _, id := range moveIDs {
		switch moveState[id] {
		case "posted":
			postedMoves = append(postedMoves, id)
		case "cancel":
			cancelledMoves[id] = true
		}
	}
	// Exclude counterpart lines that belong to a cancelled move (rare — one
	// extra read only when a cancelled move is actually present).
	writeIDs := counterpartIDs
	if len(cancelledMoves) > 0 {
		lineRows, lerr := odooReadMapsByIDs(creds, uid, "account.move.line", counterpartIDs, []string{"id", "move_id"})
		if lerr != nil {
			return fmt.Errorf("read counterpart line moves for account %s: %v", accountCode, lerr)
		}
		lineMove := make(map[int]int, len(lineRows))
		for _, r := range lineRows {
			lineMove[odooInt(r["id"])] = odooFieldID(r["move_id"])
		}
		writeIDs = writeIDs[:0]
		for _, lineID := range counterpartIDs {
			if !cancelledMoves[lineMove[lineID]] {
				writeIDs = append(writeIDs, lineID)
			}
		}
		Warnf("  %s⚠ Skipped %s while setting account %s (cancelled)%s",
			Fmt.Yellow, Pluralize(len(cancelledMoves), "cancelled move", ""), accountCode, Fmt.Reset)
	}
	if len(writeIDs) == 0 {
		return nil
	}

	if len(postedMoves) > 0 {
		if status != nil {
			status.Update("Applying account %s: resetting %d posted moves...", accountCode, len(postedMoves))
		}
		if err := runOdooIDChunks(status, "Resetting moves to draft", postedMoves, 100, func(chunk []interface{}) error {
			_, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
				"account.move", "button_draft",
				[]interface{}{chunk}, nil)
			return err
		}); err != nil {
			return fmt.Errorf("reset moves to draft for account %s: %v", accountCode, err)
		}
	}
	if status != nil {
		status.Update("Applying account %s: writing %d counterpart lines...", accountCode, len(writeIDs))
	}
	if err := runOdooIDChunks(status, "Writing counterpart accounts", writeIDs, 200, func(chunk []interface{}) error {
		// Bypass context (same shape used by the metadata stage and
		// reconcileStatementLineWithMove): tells Odoo to skip its
		// statement-line ↔ move synchronization and the
		// _check_journal_consistency constraint while we mutate
		// account_id. The lifecycle reposts the move afterwards which
		// re-runs full validation. Without the bypass, batches
		// occasionally trip "exactly one entry on the bank account"
		// when an earlier write in the same lifecycle left a
		// transient inconsistent state.
		_, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
			"account.move.line", "write",
			[]interface{}{chunk, map[string]interface{}{"account_id": accountID}},
			odooStatementLineMetadataWriteContext())
		return err
	}); err != nil {
		return fmt.Errorf("write counterpart account %s: %v", accountCode, err)
	}
	// Repost only the moves we drafted; draft moves stay draft (we never
	// promoted them) so the operator's intent — and the open statement's
	// not-yet-posted state — is preserved.
	if len(postedMoves) > 0 {
		if status != nil {
			status.Update("Applying account %s: reposting %d moves...", accountCode, len(postedMoves))
		}
		if err := runOdooIDChunks(status, "Reposting moves", postedMoves, 100, func(chunk []interface{}) error {
			_, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
				"account.move", "action_post",
				[]interface{}{chunk}, nil)
			return err
		}); err != nil {
			return fmt.Errorf("repost moves for account %s: %v", accountCode, err)
		}
	}
	return nil
}

func updateStatementLinesPartner(creds *OdooCredentials, uid int, lineIDs []int, partnerID int) error {
	lineIDs = uniquePositiveInts(lineIDs)
	if len(lineIDs) == 0 || partnerID <= 0 {
		return nil
	}
	_, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement.line", "write",
		[]interface{}{intsToInterfaces(lineIDs), map[string]interface{}{"partner_id": partnerID}}, nil)
	return err
}

func updateStatementLinesPartnerWithProgress(creds *OdooCredentials, uid int, lineIDs []int, partnerID int, status *statusLine, label string, doneBefore, total, groupIndex, groupTotal int) error {
	lineIDs = uniquePositiveInts(lineIDs)
	if len(lineIDs) == 0 || partnerID <= 0 {
		return nil
	}
	const chunkSize = 200
	for start := 0; start < len(lineIDs); start += chunkSize {
		end := start + chunkSize
		if end > len(lineIDs) {
			end = len(lineIDs)
		}
		if status != nil {
			status.Update("%s %d/%d (partner %d/%d, #%d)...", label, doneBefore+start, total, groupIndex, groupTotal, partnerID)
		}
		_, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
			"account.bank.statement.line", "write",
			[]interface{}{intsToInterfaces(lineIDs[start:end]), map[string]interface{}{"partner_id": partnerID}}, nil)
		if err != nil {
			return err
		}
		if status != nil {
			status.Update("%s %d/%d (partner %d/%d, #%d)...", label, doneBefore+end, total, groupIndex, groupTotal, partnerID)
		}
	}
	return nil
}

func setStatementBalanceEndReal(creds *OdooCredentials, uid int, stmtID int, value float64) error {
	_, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement", "write",
		[]interface{}{[]interface{}{stmtID}, map[string]interface{}{
			"balance_end_real": value,
		}}, nil)
	return err
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
	fmt.Printf("\n  %sLoading archived Stripe provider transactions...%s\n", Fmt.Dim, Fmt.Reset)
	all, err := stripesource.LoadTransactions(DataDir(), acc.AccountID)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("no Stripe provider data found; run `chb transactions sync --source stripe --reset` first")
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
