package cmd

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"

	kbcbrusselssource "github.com/CommonsHub/chb/providers/kbcbrussels"
)

// categorizeOdooJournal walks every line in a bank journal, computes
// the desired (category, collective) from the same rule chain that
// `merge` uses, and writes the analytic_distribution on each line so
// the category lands on a costs/income analytic account and the
// collective lands on the collective plan. Idempotent: re-running skips
// lines whose analytic_distribution already matches the target.
//
// Currently only supports kbcbrussels journals because that's the only
// provider we can robustly re-categorize from local data (each line
// carries a unique_import_id derived from a CSV row we have on disk).
// Stripe support is straightforward to add — same shape, different
// row-loader.
func categorizeOdooJournal(creds *OdooCredentials, uid int, journalID int, acc *AccountConfig, dryRun, assumeYes, verbose bool) error {
	return categorizeOdooJournalMode(creds, uid, journalID, acc, dryRun, assumeYes, verbose, false)
}

// categorizeOdooJournalAnnotations is the consolidation run: only lines
// driven by a trusted annotation not yet applied are written.
func categorizeOdooJournalAnnotations(creds *OdooCredentials, uid int, journalID int, acc *AccountConfig, dryRun, assumeYes, verbose bool) error {
	return categorizeOdooJournalMode(creds, uid, journalID, acc, dryRun, assumeYes, verbose, true)
}

func categorizeOdooJournalMode(creds *OdooCredentials, uid int, journalID int, acc *AccountConfig, dryRun, assumeYes, verbose, annotationsOnly bool) error {
	if acc.Provider != "kbcbrussels" {
		return fmt.Errorf("`categorize` is currently implemented for kbcbrussels journals only (provider: %s)", acc.Provider)
	}

	plans := loadOdooAnalyticPlansFile()
	if plans == nil {
		return fmt.Errorf("no analytic plans cache — run `chb odoo pull` first to create plans and accounts")
	}

	iban := kbcbrusselssource.NormalizeIBAN(acc.IBAN)
	rows, err := kbcbrusselssource.LoadTransactionsForIBAN(DataDir(), iban)
	if err != nil {
		return fmt.Errorf("load CSV: %v", err)
	}
	csvByImportID := map[string]kbcbrusselssource.Transaction{}
	for _, row := range rows {
		csvByImportID[buildKBCImportID(iban, row.Hash)] = row
	}

	odooLines, err := loadOdooCategorizeLines(creds, uid, journalID)
	if err != nil {
		return fmt.Errorf("fetch Odoo lines: %v", err)
	}

	ctx := newKBCMergeContext(acc)
	dataDir := DataDir()
	txAnnotations := loadTransactionAnnotations(dataDir)
	ledger := loadAppliedAnnotations(dataDir)
	plan := buildCategorizeJournalPlanWithAnnotations(odooLines, csvByImportID, acc, ctx, plans, txAnnotations, ledger)
	if annotationsOnly {
		var keep []categorizeChange
		for _, c := range plan.ToUpdate {
			if c.AnnotationURI != "" {
				keep = append(keep, c)
			}
		}
		plan.ToUpdate = keep
	}
	if !dryRun && len(plan.Agreeing) > 0 {
		for _, a := range plan.Agreeing {
			ledger[a[0]] = a[1]
		}
		_ = saveAppliedAnnotations(dataDir, ledger)
	}

	printCategorizeJournalSummary(plan, journalID, iban)
	if verbose {
		printCategorizeJournalTable(plan, acc)
	} else {
		printCategorizeJournalPreview(plan, acc)
	}

	if dryRun {
		fmt.Printf("\n  %s(dry-run — no writes)%s\n\n", Fmt.Dim, Fmt.Reset)
		return nil
	}
	if len(plan.ToUpdate) == 0 {
		fmt.Printf("\n  %s✓ Nothing to do%s\n\n", Fmt.Green, Fmt.Reset)
		return nil
	}
	if !assumeYes {
		fmt.Println()
		fmt.Printf("  Apply: update analytic_distribution on %s? [y/N] ",
			Pluralize(len(plan.ToUpdate), "line", ""))
		reader := bufio.NewReader(os.Stdin)
		resp, _ := reader.ReadString('\n')
		if strings.ToLower(strings.TrimSpace(resp)) != "y" {
			return fmt.Errorf("cancelled")
		}
	}
	if err := applyCategorizeJournal(creds, uid, plan); err != nil {
		return err
	}
	for _, c := range plan.ToUpdate {
		if c.AnnotationURI != "" {
			ledger[c.AnnotationURI] = c.AnnotationEventID
		}
	}
	_ = saveAppliedAnnotations(dataDir, ledger)
	fmt.Printf("\n  %s✓ Categorize complete%s\n\n", Fmt.Green, Fmt.Reset)
	return nil
}

// odooCategorizeLine is the minimal projection we need to compute and
// persist the desired analytic_distribution. We carry both the
// account.bank.statement.line id (for matching against CSV via
// unique_import_id) and the linked account.move.line id (where the
// analytic_distribution actually lives in Odoo).
type odooCategorizeLine struct {
	StatementLineID      int
	MoveLineID           int
	Date                 string
	Amount               float64
	PaymentRef           string
	Narration            string
	ImportID             string
	AnalyticDistribution map[int]float64
	// AccountID is the GL account currently on the counterpart line.
	// Used to detect drift from the mapping-driven account so categorize
	// can re-apply OdooMapping.Set.AccountCode (e.g. when a description-
	// specific override is added after import).
	AccountID int
}

func loadOdooCategorizeLines(creds *OdooCredentials, uid int, journalID int) ([]odooCategorizeLine, error) {
	// 1) Fetch the statement lines for this journal. We need their
	//    unique_import_id (to match CSV rows) and a way to reach the
	//    counterpart move.line that carries the analytic_distribution.
	stmtData, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement.line", "search_read",
		[]interface{}{[]interface{}{
			[]interface{}{"journal_id", "=", journalID},
		}},
		map[string]interface{}{
			"fields": []string{
				"id", "date", "amount", "payment_ref", "narration",
				"unique_import_id", "move_id",
			},
			"order": "date asc, id asc",
			"limit": 0,
		})
	if err != nil {
		return nil, err
	}
	var stmtRaw []map[string]interface{}
	if err := json.Unmarshal(stmtData, &stmtRaw); err != nil {
		return nil, err
	}

	// 2) Collect the move_ids and fetch their lines. The counterpart
	//    move.line (the one NOT on the journal's default debit/credit
	//    account) is where we write analytic_distribution. Picking the
	//    "counterpart" is fragile in general; the simplest robust rule
	//    is "the line whose account differs from the journal's
	//    suspense/default account" — but for our purpose we can use a
	//    heuristic: the move.line with the same signed amount as the
	//    statement.line lives on the counterpart. Skip moves with no
	//    counterpart (unreconciled draft).
	moveIDs := make([]int, 0, len(stmtRaw))
	for _, row := range stmtRaw {
		if id := odooFieldID(row["move_id"]); id > 0 {
			moveIDs = append(moveIDs, id)
		}
	}
	moveLines := map[int][]map[string]interface{}{}
	if len(moveIDs) > 0 {
		moveLineData, err := odooSearchReadAllMaps(creds, uid, "account.move.line",
			[]interface{}{
				[]interface{}{"move_id", "in", intSliceToInterface(moveIDs)},
			},
			[]string{"id", "move_id", "account_id", "balance", "debit", "credit", "analytic_distribution"},
			"id asc",
		)
		if err != nil {
			return nil, err
		}
		for _, row := range moveLineData {
			moveID := odooFieldID(row["move_id"])
			moveLines[moveID] = append(moveLines[moveID], row)
		}
	}

	out := make([]odooCategorizeLine, 0, len(stmtRaw))
	for _, row := range stmtRaw {
		date := odooString(row["date"])
		if len(date) >= 10 {
			date = date[:10]
		}
		moveID := odooFieldID(row["move_id"])
		stmtAmount := odooFloat(row["amount"])
		mlID, dist, accountID := pickCategorizeCounterpart(moveLines[moveID], stmtAmount)
		out = append(out, odooCategorizeLine{
			StatementLineID:      odooInt(row["id"]),
			MoveLineID:           mlID,
			Date:                 date,
			Amount:               stmtAmount,
			PaymentRef:           odooString(row["payment_ref"]),
			Narration:            odooString(row["narration"]),
			ImportID:             odooString(row["unique_import_id"]),
			AnalyticDistribution: dist,
			AccountID:            accountID,
		})
	}
	return out, nil
}

// pickCategorizeCounterpart finds the counterpart move.line for a
// statement line. The counterpart's signed balance equals -statementAmount
// (the bank-account side has +statementAmount, the counterpart -). Returns
// (0, nil, 0) if no counterpart could be identified — typically an
// unreconciled draft with a suspense line we don't want to touch.
func pickCategorizeCounterpart(lines []map[string]interface{}, statementAmount float64) (int, map[int]float64, int) {
	for _, line := range lines {
		balance := odooFloat(line["balance"])
		if balance == 0 {
			balance = odooFloat(line["debit"]) - odooFloat(line["credit"])
		}
		// Bank-account side has the same sign as the statement amount;
		// the counterpart is opposite.
		if (statementAmount > 0 && balance < 0) || (statementAmount < 0 && balance > 0) {
			return odooInt(line["id"]),
				analyticDistributionMap(line["analytic_distribution"]),
				odooFieldID(line["account_id"])
		}
	}
	return 0, nil, 0
}

func intSliceToInterface(ids []int) []interface{} {
	out := make([]interface{}, len(ids))
	for i, id := range ids {
		out[i] = id
	}
	return out
}

// categorizeChange is one line whose analytic_distribution needs to be
// rewritten to match what the rule chain produces.
type categorizeChange struct {
	Line odooCategorizeLine
	// AnnotationURI / AnnotationEventID: set when a trusted Nostr
	// annotation drives this change (recorded in the ledger once written).
	AnnotationURI, AnnotationEventID string
	Category                         string
	// Collective is the resolved collective slug (may be empty).
	Collective string
	// AccountCode is the cost/income GL code (e.g. "740041"). Carried
	// for the preview only; not written by this command.
	AccountCode string
	// DesiredDistribution is the {analytic_account_id: 100} map we want
	// to persist. Empty if neither category nor collective resolved.
	DesiredDistribution map[int]float64
}

type categorizeJournalPlan struct {
	ToUpdate []categorizeChange
	// Agreeing lists annotations (uri, event id) Odoo already reflects.
	Agreeing [][2]string
	// Unchanged tracks lines whose existing distribution already
	// matches; surfaced for the summary so the operator sees scale.
	Unchanged int
	// Untagged tracks lines for which the rule chain didn't resolve
	// either a category or a collective.
	Untagged int
	// Missing tracks lines whose import_id has no CSV counterpart —
	// usually manual journal entries; we leave them alone.
	Missing int
}

func buildCategorizeJournalPlan(lines []odooCategorizeLine, csvByImportID map[string]kbcbrusselssource.Transaction, acc *AccountConfig, ctx kbcMergeContext, plans *OdooAnalyticPlansFile) categorizeJournalPlan {
	return buildCategorizeJournalPlanWithAnnotations(lines, csvByImportID, acc, ctx, plans, nil, nil)
}

// buildCategorizeJournalPlanWithAnnotations: as buildCategorizeJournalPlan,
// and a trusted Nostr annotation on the line's transaction URI
// (iban:<iban>:tx:<statement line id>) that has not been applied to Odoo
// yet (ledger) overrides the rules' category/collective. Lines with no CSV
// row are still categorised when they carry such an annotation.
func buildCategorizeJournalPlanWithAnnotations(lines []odooCategorizeLine, csvByImportID map[string]kbcbrusselssource.Transaction, acc *AccountConfig, ctx kbcMergeContext, plans *OdooAnalyticPlansFile, annotations map[string]*TxAnnotation, ledger map[string]string) categorizeJournalPlan {
	var plan categorizeJournalPlan
	iban := kbcbrusselssource.NormalizeIBAN(acc.IBAN)
	for _, line := range lines {
		uri := BuildIBANTxURI(iban, strconv.Itoa(line.StatementLineID))
		ann := annotations[uri]
		if ann != nil && ledger[uri] == ann.NostrEventID {
			ann = nil // already consolidated
		}
		row, ok := csvByImportID[line.ImportID]
		if !ok && ann == nil {
			plan.Missing++
			continue
		}
		csvRow := kbcMergeCSVRow{Row: row, ImportID: line.ImportID}
		if ok {
			annotateKBCMergeRowWithMapping(&csvRow, acc, &ctx)
		}
		if ann != nil {
			var event string
			applyAnnotation(ann, &csvRow.Category, &csvRow.Collective, &event)
		}
		desired := desiredAnalyticDistribution(csvRow, plans)
		if len(desired) == 0 && csvRow.Category == "" && csvRow.Collective == "" && csvRow.AccountCode == "" {
			plan.Untagged++
			continue
		}
		distOK := analyticDistributionsEqual(line.AnalyticDistribution, desired)
		// Account drift: the rule produced an AccountCode but the
		// counterpart move.line is on a different GL account. Happens
		// after a description-specific override is added post-import.
		accountDrift := csvRow.AccountCode != ""
		if distOK && !accountDrift {
			plan.Unchanged++
			if ann != nil {
				plan.Agreeing = append(plan.Agreeing, [2]string{uri, ann.NostrEventID})
			}
			continue
		}
		change := categorizeChange{
			Line:                line,
			Category:            csvRow.Category,
			Collective:          csvRow.Collective,
			AccountCode:         csvRow.AccountCode,
			DesiredDistribution: desired,
		}
		if ann != nil {
			change.AnnotationURI, change.AnnotationEventID = uri, ann.NostrEventID
		}
		plan.ToUpdate = append(plan.ToUpdate, change)
	}
	return plan
}

func desiredAnalyticDistribution(row kbcMergeCSVRow, plans *OdooAnalyticPlansFile) map[int]float64 {
	out := map[int]float64{}
	if id := plans.CategoryAccountIDFor(row.Category); id > 0 {
		out[id] = 100
	}
	if id := plans.CollectiveAccountIDFor(row.Collective); id > 0 {
		out[id] = 100
	}
	return out
}

func analyticDistributionsEqual(a, b map[int]float64) bool {
	if len(a) != len(b) {
		return false
	}
	for k, va := range a {
		vb, ok := b[k]
		if !ok || math.Abs(va-vb) > 0.001 {
			return false
		}
	}
	return true
}

func printCategorizeJournalSummary(plan categorizeJournalPlan, journalID int, iban string) {
	fmt.Printf("\n  %sCategorize plan for journal #%d (%s)%s\n", Fmt.Bold, journalID, iban, Fmt.Reset)
	fmt.Printf("    %sTo update:%s  %d %s(category/collective changed)%s\n",
		Fmt.Yellow, Fmt.Reset, len(plan.ToUpdate), Fmt.Dim, Fmt.Reset)
	fmt.Printf("    %sUnchanged:%s  %d %s(already matches rules)%s\n",
		Fmt.Green, Fmt.Reset, plan.Unchanged, Fmt.Dim, Fmt.Reset)
	if plan.Untagged > 0 {
		fmt.Printf("    %sUntagged:%s   %d %s(no rule matched)%s\n",
			Fmt.Dim, Fmt.Reset, plan.Untagged, Fmt.Dim, Fmt.Reset)
	}
	if plan.Missing > 0 {
		fmt.Printf("    %sNo CSV row:%s %d %s(manual entries — left alone)%s\n",
			Fmt.Dim, Fmt.Reset, plan.Missing, Fmt.Dim, Fmt.Reset)
	}
}

func printCategorizeJournalPreview(plan categorizeJournalPlan, acc *AccountConfig) {
	if len(plan.ToUpdate) == 0 {
		return
	}
	limit := kbcMergePreviewLimit(len(plan.ToUpdate))
	fmt.Printf("\n  %sTo update (first %d of %d):%s\n", Fmt.Bold, limit, len(plan.ToUpdate), Fmt.Reset)
	currency := accCurrency(acc)
	preview := plan.ToUpdate
	if len(preview) > limit {
		preview = preview[:limit]
	}
	for _, c := range preview {
		tag := categorizeShortTag(c)
		fmt.Printf("    #%-7d %s  %s  %s%s%s%s\n",
			c.Line.StatementLineID,
			c.Line.Date,
			formatAccountDataBalance(c.Line.Amount, currency),
			Fmt.Dim, truncateRunes(c.Line.PaymentRef, 50), Fmt.Reset,
			tag)
	}
}

func printCategorizeJournalTable(plan categorizeJournalPlan, acc *AccountConfig) {
	if len(plan.ToUpdate) == 0 {
		return
	}
	fmt.Printf("\n  %sTo update (%d):%s\n", Fmt.Bold, len(plan.ToUpdate), Fmt.Reset)
	headers := []string{"Odoo #", "Date", "Amount", "Description", "Category", "Collective", "Account"}
	rows := make([][]string, 0, len(plan.ToUpdate))
	currency := accCurrency(acc)
	for _, c := range plan.ToUpdate {
		cat := c.Category
		if cat == "" {
			cat = "-"
		}
		coll := c.Collective
		if coll == "" {
			coll = "-"
		}
		acct := c.AccountCode
		if acct == "" {
			acct = "-"
		}
		rows = append(rows, []string{
			fmt.Sprintf("#%d", c.Line.StatementLineID),
			c.Line.Date,
			formatBalancePlain(c.Line.Amount, currency),
			Truncate(c.Line.PaymentRef, 50),
			Truncate(cat, 18),
			Truncate(coll, 14),
			Truncate(acct, 10),
		})
	}
	renderTicketsTable(headers, rows, nil, map[int]bool{2: true})
}

func categorizeShortTag(c categorizeChange) string {
	parts := make([]string, 0, 3)
	if c.Category != "" {
		parts = append(parts, c.Category)
	}
	if c.Collective != "" {
		parts = append(parts, "@"+c.Collective)
	}
	if c.AccountCode != "" {
		parts = append(parts, c.AccountCode)
	}
	if len(parts) == 0 {
		return ""
	}
	return fmt.Sprintf(" %s[%s]%s", Fmt.Cyan, strings.Join(parts, " · "), Fmt.Reset)
}

func applyCategorizeJournal(creds *OdooCredentials, uid int, plan categorizeJournalPlan) error {
	fmt.Printf("\n  %sUpdating %s in Odoo…%s\n",
		Fmt.Dim, Pluralize(len(plan.ToUpdate), "line", ""), Fmt.Reset)
	// Group lines by identical desired distribution so we can issue one
	// write per distinct distribution instead of one per line — Odoo
	// happily takes a list of ids and applies the same vals to all.
	type group struct {
		key  string
		dist map[int]float64
		ids  []interface{}
	}
	groups := map[string]*group{}
	// Group by account code too so we can batch the account_id writes.
	byAccountCode := map[string][]int{}
	skipped := 0
	for _, c := range plan.ToUpdate {
		if c.Line.MoveLineID == 0 {
			skipped++
			continue
		}
		key := distributionKey(c.DesiredDistribution)
		g, ok := groups[key]
		if !ok {
			g = &group{key: key, dist: c.DesiredDistribution}
			groups[key] = g
		}
		g.ids = append(g.ids, c.Line.MoveLineID)
		if c.AccountCode != "" {
			byAccountCode[c.AccountCode] = append(byAccountCode[c.AccountCode], c.Line.MoveLineID)
		}
	}
	keys := make([]string, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		g := groups[k]
		vals := map[string]interface{}{
			"analytic_distribution": distributionForWrite(g.dist),
		}
		if _, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
			"account.move.line", "write",
			[]interface{}{g.ids, vals}, nil); err != nil {
			return fmt.Errorf("write distribution %s on %d line(s): %v",
				k, len(g.ids), err)
		}
	}
	// Re-apply the GL account from the mapping. applyOdooMappingAccount
	// resolves the code → account_id and writes account_id on the lines.
	codes := make([]string, 0, len(byAccountCode))
	for code := range byAccountCode {
		codes = append(codes, code)
	}
	sort.Strings(codes)
	for _, code := range codes {
		if err := applyOdooMappingAccountUnlessDocument(creds, uid, byAccountCode[code], code); err != nil {
			Warnf("  %s⚠ apply account %s on %d line(s): %v%s",
				Fmt.Yellow, code, len(byAccountCode[code]), err, Fmt.Reset)
		}
	}
	if skipped > 0 {
		Warnf("  %s⚠ skipped %d line%s with no resolvable counterpart move.line (likely unreconciled drafts)%s",
			Fmt.Yellow, skipped, plural(skipped), Fmt.Reset)
	}
	fmt.Printf("  %s✓ Updated %d line%s%s\n",
		Fmt.Green, len(plan.ToUpdate), plural(len(plan.ToUpdate)), Fmt.Reset)
	return nil
}

func distributionKey(dist map[int]float64) string {
	ids := make([]int, 0, len(dist))
	for id := range dist {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	parts := make([]string, 0, len(ids))
	for _, id := range ids {
		parts = append(parts, fmt.Sprintf("%d=%.2f", id, dist[id]))
	}
	return strings.Join(parts, ",")
}

func distributionForWrite(dist map[int]float64) map[string]float64 {
	out := make(map[string]float64, len(dist))
	for id, pct := range dist {
		out[fmt.Sprintf("%d", id)] = pct
	}
	return out
}
