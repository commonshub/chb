package cmd

import (
	"fmt"
	"sort"
	"strings"
	"time"

	stripesource "github.com/CommonsHub/chb/providers/stripe"
)

// stripePaymentRefFix is a Stripe statement line whose payment_ref no longer
// matches the classification its local data resolves to (e.g. a donation whose
// collective changed commonshub → openletter). Lines truly matched against an
// invoice (receivable/payable counterpart) are excluded — only categorised
// lines are refreshed.
type stripePaymentRefFix struct {
	LineID, MoveID int
	ImportID       string
	Old, New       string
}

// detectStripePaymentRefFixes re-derives every Stripe line's payment_ref from
// the local balance-transaction data and returns the ones whose Odoo value is
// stale, excluding invoice-matched lines. Read-only.
func detectStripePaymentRefFixes(creds *OdooCredentials, uid, journalID int, acc *AccountConfig) ([]stripePaymentRefFix, error) {
	if acc == nil || acc.Provider != "stripe" {
		return nil, nil
	}
	bts, err := stripesource.LoadTransactionsSince(DataDir(), acc.AccountID, 0)
	if err != nil {
		return nil, fmt.Errorf("load Stripe transactions: %v", err)
	}
	cutoffUnix := int64(0)
	hasCutoff := false
	if c, ok := acc.OdooSyncSinceTime(); ok {
		cutoffUnix, hasCutoff = c.Unix(), true
	}
	chargeIndex := loadArchivedStripeCharges(DataDir())
	refundMap := loadArchivedStripeRefundMap(DataDir())
	eventHints := loadLocalStripeEventHints(DataDir())
	var refundOriginIndex map[int64][]stripeChargeClass
	originIndexReady := false
	categorizer := NewCategorizer(nil)
	expectedRef := map[string]string{}
	for _, bt := range bts {
		if hasCutoff && bt.Created < cutoffUnix {
			continue
		}
		// Payout lines are labelled by btPaymentRef (the payout's description),
		// not by the category-driven stripeOdooPaymentRef. Re-derive that label
		// directly so stale auto-generated labels (e.g. "Manual payout
		// 1970-01-01") get repaired to the payout's memo, then skip the
		// category logic below. payout_cancel has no meaningful label to fix.
		switch strings.ToLower(bt.Type) {
		case "payout_cancel":
			continue
		case "payout":
			if importID := stripeBTImportID(acc, bt); importID != "" {
				expectedRef[importID] = btPaymentRef(bt)
			}
			continue
		}
		importID := stripeBTImportID(acc, bt)
		if importID == "" {
			continue
		}
		bt = enrichStripeBTForClassification(bt, chargeIndex, refundMap, eventHints)
		amount := stripeStatementLineAmount(bt)
		ruleTx := stripeClassificationTransaction(acc, bt, amount)
		categorizer.Apply(&ruleTx)
		if stripeBTIsReversal(bt) && ruleTx.Category == "" {
			if !originIndexReady {
				refundOriginIndex = loadStripeRefundOriginIndex(acc, chargeIndex, eventHints)
				originIndexReady = true
			}
			stripeApplyReversalFallback(bt, &ruleTx, refundOriginIndex)
		}
		expectedRef[importID] = stripeOdooPaymentRef(bt, ruleTx)
	}
	if len(expectedRef) == 0 {
		return nil, nil
	}

	rows, err := odooSearchReadAllMaps(creds, uid, "account.bank.statement.line",
		[]interface{}{[]interface{}{"journal_id", "=", journalID}},
		[]string{"id", "unique_import_id", "payment_ref", "move_id", "is_reconciled"}, "id asc")
	if err != nil {
		return nil, err
	}
	type cand struct {
		LineID, MoveID int
		ImportID, Old, New string
		Recon              bool
	}
	var cands []cand
	var reconMoveIDs []int
	for _, r := range rows {
		importID := odooString(r["unique_import_id"])
		want, ok := expectedRef[importID]
		if !ok || want == "" {
			continue
		}
		cur := odooString(r["payment_ref"])
		if want == cur {
			continue
		}
		c := cand{LineID: odooInt(r["id"]), MoveID: odooFieldID(r["move_id"]), ImportID: importID, Old: cur, New: want, Recon: odooBool(r["is_reconciled"])}
		cands = append(cands, c)
		if c.Recon && c.MoveID > 0 {
			reconMoveIDs = append(reconMoveIDs, c.MoveID)
		}
	}
	counterpart, err := fetchCounterpartMoveLinesByMoveID(creds, uid, reconMoveIDs)
	if err != nil {
		return nil, err
	}
	var out []stripePaymentRefFix
	for _, c := range cands {
		// Truly invoice-matched lines (reconciled A/R or A/P counterpart) are
		// preserved; a categorized or over-categorized line is safe to relabel.
		if c.Recon && stripeCounterpartIsInvoiceMatched(counterpart[c.MoveID]) {
			continue
		}
		out = append(out, stripePaymentRefFix{LineID: c.LineID, MoveID: c.MoveID, ImportID: c.ImportID, Old: c.Old, New: c.New})
	}
	return out, nil
}

// stripeFeeAccountFix is a Stripe fee line (a standalone fee/adjustment BT, or a
// per-charge ":fee" line) whose counterpart account isn't the Stripe-fees
// account. The push routes fees there, but the generic re-classification misses
// fee credits (direction:in) and never sees per-charge ":fee" lines — this
// guarantees every fee lands on 657020.
type stripeFeeAccountFix struct {
	LineID   int
	ImportID string
	OldCode  string
}

func detectStripeFeeAccountFixes(creds *OdooCredentials, uid, journalID int, acc *AccountConfig) ([]stripeFeeAccountFix, string, error) {
	if acc == nil || acc.Provider != "stripe" {
		return nil, "", nil
	}
	mappings, _ := LoadOdooMappings()
	feeCode := stripeFeeOdooAccountCode(mappings)
	if feeCode == "" {
		return nil, "", nil
	}
	bts, err := stripesource.LoadTransactionsSince(DataDir(), acc.AccountID, 0)
	if err != nil {
		return nil, feeCode, err
	}
	cutoffUnix := int64(0)
	hasCutoff := false
	if c, ok := acc.OdooSyncSinceTime(); ok {
		cutoffUnix, hasCutoff = c.Unix(), true
	}
	feeIDs := map[string]bool{}
	for _, bt := range bts {
		if hasCutoff && bt.Created < cutoffUnix {
			continue
		}
		if stripeBTIsFee(bt) {
			if id := stripeBTImportID(acc, bt); id != "" {
				feeIDs[id] = true
			}
		}
		if cents, ok := stripeImplicitChargeFeeCents(bt); ok && cents != 0 {
			feeIDs[stripeBTFeeImportID(acc, bt)] = true
		}
	}

	rows, err := odooSearchReadAllMaps(creds, uid, "account.bank.statement.line",
		[]interface{}{[]interface{}{"journal_id", "=", journalID}},
		[]string{"id", "unique_import_id", "move_id"}, "id asc")
	if err != nil {
		return nil, feeCode, err
	}
	type lineRow struct {
		ID, MoveID int
		ImportID   string
	}
	var lines []lineRow
	var moveIDs []int
	for _, r := range rows {
		importID := odooString(r["unique_import_id"])
		// A line is a fee line if it's a known fee BT / per-charge fee, or its
		// import-id carries the ":fee"/":fees" suffix.
		if !feeIDs[importID] && !isStripeFeeLineImportID(importID) {
			continue
		}
		l := lineRow{ID: odooInt(r["id"]), MoveID: odooFieldID(r["move_id"]), ImportID: importID}
		lines = append(lines, l)
		if l.MoveID > 0 {
			moveIDs = append(moveIDs, l.MoveID)
		}
	}
	if len(lines) == 0 {
		return nil, feeCode, nil
	}
	counterpart, err := fetchCounterpartMoveLinesByMoveID(creds, uid, moveIDs)
	if err != nil {
		return nil, feeCode, err
	}
	acctIDs := make([]int, 0, len(counterpart))
	for _, info := range counterpart {
		if info.AccountID > 0 {
			acctIDs = append(acctIDs, info.AccountID)
		}
	}
	codeByID, err := fetchAccountCodesByID(creds, uid, acctIDs)
	if err != nil {
		return nil, feeCode, err
	}
	var out []stripeFeeAccountFix
	for _, l := range lines {
		cp := counterpart[l.MoveID]
		// Never re-account a fee line that's reconciled against an invoice/bill —
		// that match takes precedence (and drafting it would break it).
		if stripeCounterpartIsInvoiceMatched(cp) {
			continue
		}
		cur := codeByID[cp.AccountID]
		if cur != feeCode {
			out = append(out, stripeFeeAccountFix{LineID: l.ID, ImportID: l.ImportID, OldCode: cur})
		}
	}
	return out, feeCode, nil
}

func printStripeFeeAccountFixes(fixes []stripeFeeAccountFix, feeCode string) {
	fmt.Printf("\n  %s%s not on the Stripe-fees account (%s):%s\n",
		Fmt.Yellow, Pluralize(len(fixes), "fee line", ""), feeCode, Fmt.Reset)
	byCode := map[string]int{}
	for _, f := range fixes {
		from := f.OldCode
		if from == "" {
			from = "(none)"
		}
		byCode[from]++
	}
	for from, n := range byCode {
		fmt.Printf("    %s%s → %s  %s%s\n", Fmt.Dim, from, feeCode, Pluralize(n, "line", ""), Fmt.Reset)
	}
}

func printStripePaymentRefFixes(fixes []stripePaymentRefFix) {
	fmt.Printf("\n  %s%s with a stale label (classification changed):%s\n",
		Fmt.Yellow, Pluralize(len(fixes), "line", ""), Fmt.Reset)
	// Group by the change so the operator can review aggregates (a single
	// "membership commonshub → donation commonshub: 620" is far easier to vet
	// than 620 identical lines) and spot an unintended mass change.
	counts := map[string]int{}
	order := []string{}
	for _, f := range fixes {
		k := fmt.Sprintf("%q → %q", f.Old, f.New)
		if counts[k] == 0 {
			order = append(order, k)
		}
		counts[k]++
	}
	sort.Slice(order, func(i, j int) bool { return counts[order[i]] > counts[order[j]] })
	for i, k := range order {
		if i >= 15 {
			fmt.Printf("    %s… and %d more change types%s\n", Fmt.Dim, len(order)-i, Fmt.Reset)
			break
		}
		fmt.Printf("    %s%-5d %s%s\n", Fmt.Dim, counts[k], k, Fmt.Reset)
	}
}

// applyStripePaymentRefFixes rewrites payment_ref on each line, drafting →
// updating → reposting posted (but not invoice-matched) moves.
func applyStripePaymentRefFixes(creds *OdooCredentials, uid int, fixes []stripePaymentRefFix) int {
	// Each fix is a draft → write → repost round-trip (3+ XML-RPC calls), so a
	// few dozen lines take a noticeable while. Report progress per line.
	status := newStatusLine()
	defer status.Clear()
	done := 0
	for i, f := range fixes {
		status.Update("Refreshing label %d/%d (%s)…", i+1, len(fixes), f.New)
		if err := updateStatementLineFieldsForMetadata(creds, uid, f.LineID, f.MoveID, map[string]interface{}{"payment_ref": f.New}); err != nil {
			Warnf("  %s⚠ Could not refresh line %s: %v%s", Fmt.Yellow, f.ImportID, err, Fmt.Reset)
			continue
		}
		done++
	}
	return done
}

// odooLineAccountFix is a chb-owned journal line whose counterpart account in
// Odoo no longer matches the account its category maps to (e.g. a "membership"
// line on 700000 after the mapping moved to 730000).
type odooLineAccountFix struct {
	LineID   int
	ImportID string
	OldCode  string
	NewCode  string
}

// fetchAccountCodesByID reads the chart-of-accounts code for each account id.
func fetchAccountCodesByID(creds *OdooCredentials, uid int, ids []int) (map[int]string, error) {
	out := map[int]string{}
	ids = uniquePositiveInts(ids)
	if len(ids) == 0 {
		return out, nil
	}
	rows, err := odooReadMapsByIDs(creds, uid, "account.account", ids, []string{"id", "code"})
	if err != nil {
		return out, err
	}
	for _, r := range rows {
		out[odooInt(r["id"])] = odooString(r["code"])
	}
	return out, nil
}

// detectOdooJournalAccountReclassification re-resolves every chb-owned line's
// counterpart account from the CURRENT rules → account mapping (keyed off the
// local transaction's category) and returns the lines whose Odoo account
// differs. Generic across providers and journals: a tx categorised "membership"
// maps to the membership account everywhere, so editing odoo_mapping.json and
// re-running `fix` moves every affected line. Read-only.
//
// Only lines whose category resolves to a mapped account are considered — lines
// with no mapping keep whatever account they have (no clobbering of manual or
// default-revenue postings).
func detectOdooJournalAccountReclassification(creds *OdooCredentials, uid, journalID int, acc *AccountConfig, remap bool, since time.Time) ([]odooLineAccountFix, error) {
	if acc == nil {
		return nil, nil
	}
	mappings, _ := LoadOdooMappings()
	if len(mappings) == 0 {
		return nil, nil
	}
	localTxs := loadAccountTransactionsForOdoo(acc)
	cutoffUnix := int64(0)
	hasCutoff := false
	if c, ok := acc.OdooSyncSinceTime(); ok {
		cutoffUnix, hasCutoff = c.Unix(), true
	}
	expected := map[string]string{} // importID -> expected account code
	for _, tx := range localTxs {
		if hasCutoff && tx.Timestamp < cutoffUnix {
			continue
		}
		importID := buildUniqueImportID(acc, tx)
		if importID == "" {
			continue
		}
		// Re-resolve against the current mapping so a freshly-edited
		// odoo_mapping.json takes effect without a regenerate; fall back to the
		// value generate already stored.
		code := tx.AccountCode
		if matched := LookupOdooMapping(mappings, tx); matched != nil && matched.Set.AccountCode != "" {
			code = matched.Set.AccountCode
		}
		if code != "" {
			expected[importID] = code
		}
	}

	// Fetch payment_ref + amount too: for an Odoo-source-of-truth journal (KBC),
	// the entries originate in Odoo, not the local mirror, so the local tx may
	// carry no category. Categorise the Odoo line's own narration via the rules
	// — that's how a freshly-added `cp-order-…` drink line resolves to 700003.
	rows, err := odooSearchReadAllMaps(creds, uid, "account.bank.statement.line",
		journalLineSinceDomain(journalID, since),
		[]string{"id", "unique_import_id", "move_id", "payment_ref", "amount"}, "id asc")
	if err != nil {
		return nil, err
	}
	if cz := newCategorizerOrNil(); cz != nil {
		for _, r := range rows {
			importID := odooString(r["unique_import_id"])
			if importID == "" || expected[importID] != "" {
				continue
			}
			if code := proposeAccountFromOdooNarration(cz, mappings, acc.Provider, odooString(r["payment_ref"]), odooFloat(r["amount"])); code != "" {
				expected[importID] = code
			}
		}
	}
	if len(expected) == 0 {
		return nil, nil
	}

	type lineRow struct {
		ID, MoveID int
		ImportID   string
	}
	var lines []lineRow
	var moveIDs []int
	for _, r := range rows {
		importID := odooString(r["unique_import_id"])
		if _, ok := expected[importID]; !ok {
			continue
		}
		l := lineRow{ID: odooInt(r["id"]), MoveID: odooFieldID(r["move_id"]), ImportID: importID}
		lines = append(lines, l)
		if l.MoveID > 0 {
			moveIDs = append(moveIDs, l.MoveID)
		}
	}
	counterpart, err := fetchCounterpartMoveLinesByMoveID(creds, uid, moveIDs)
	if err != nil {
		return nil, err
	}
	acctIDs := make([]int, 0, len(counterpart))
	for _, info := range counterpart {
		if info.AccountID > 0 {
			acctIDs = append(acctIDs, info.AccountID)
		}
	}
	codeByID, err := fetchAccountCodesByID(creds, uid, acctIDs)
	if err != nil {
		return nil, err
	}
	var out []odooLineAccountFix
	for _, l := range lines {
		cp := counterpart[l.MoveID]
		// A line reconciled against an invoice/bill (receivable/payable
		// counterpart) is left alone — that match always takes precedence over
		// category→account reclassification, and re-accounting it would draft
		// and break the reconciliation.
		if stripeCounterpartIsInvoiceMatched(cp) {
			continue
		}
		want := expected[l.ImportID]
		cur := codeByID[cp.AccountID]
		if want == "" || want == cur {
			continue
		}
		// Default: only assign an account to a still-uncategorised (suspense)
		// line — never silently revert a counterpart someone set by hand. `remap`
		// opts into re-accounting already-assigned (chb-owned) lines too, e.g. to
		// apply a changed mapping like 612011→612300 across history.
		if !remap && !isSuspenseAccountCode(cur) {
			continue
		}
		out = append(out, odooLineAccountFix{LineID: l.ID, ImportID: l.ImportID, OldCode: cur, NewCode: want})
	}
	return out, nil
}

// isSuspenseAccountCode reports whether a counterpart account code means the line
// is still uncategorised — the bank suspense / "comptes d'attente" holding
// account (499000), or no account at all. Such lines are fair game for automatic
// categorisation; any other (real) account is treated as a deliberate assignment
// and left alone unless --remap is passed.
func isSuspenseAccountCode(code string) bool {
	switch strings.TrimSpace(code) {
	case "", "499000":
		return true
	}
	return false
}

// stripeCounterpartIsInvoiceMatched reports whether a move's counterpart is
// actually reconciled against an invoice (receivable) or bill (payable) — in
// which case its account must never be rewritten by `fix`. Requires BOTH an
// A/R/A/P account type AND the counterpart line being reconciled: an A/R/A/P
// line left unreconciled by a catch-all mapping rule is over-categorization, not
// a real match, and is fair game for reset/reclassification.
func stripeCounterpartIsInvoiceMatched(cp counterpartMoveLineInfo) bool {
	switch cp.AccountType {
	case "asset_receivable", "liability_payable":
		return cp.Reconciled
	}
	return false
}

func printOdooJournalAccountReclassification(fixes []odooLineAccountFix) {
	fmt.Printf("\n  %s%s on the wrong account (category → account mapping changed):%s\n",
		Fmt.Yellow, Pluralize(len(fixes), "line", ""), Fmt.Reset)
	byMove := map[string]int{}
	for _, f := range fixes {
		byMove[fmt.Sprintf("%s → %s", f.OldCode, f.NewCode)]++
	}
	for k, n := range byMove {
		fmt.Printf("    %s%s%s  %s\n", Fmt.Dim, k, Fmt.Reset, Pluralize(n, "line", ""))
	}
}

// applyOdooJournalAccountReclassification rewrites the counterpart account on
// each line, one batched draft → write → post pass per target account code.
func applyOdooJournalAccountReclassification(creds *OdooCredentials, uid int, fixes []odooLineAccountFix, status *statusLine) int {
	byCode := map[string][]int{}
	for _, f := range fixes {
		byCode[f.NewCode] = append(byCode[f.NewCode], f.LineID)
	}
	codes := make([]string, 0, len(byCode))
	for c := range byCode {
		codes = append(codes, c)
	}
	sort.Strings(codes)
	done := 0
	for _, code := range codes {
		if err := applyOdooMappingAccount(creds, uid, byCode[code], code, status); err != nil {
			Warnf("  %s⚠ Could not set account %s on %s: %v%s", Fmt.Yellow, code, Pluralize(len(byCode[code]), "line", ""), err, Fmt.Reset)
			continue
		}
		done += len(byCode[code])
	}
	return done
}
