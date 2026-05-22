package cmd

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

type odooJournalMergePlan struct {
	SourceJournalID int
	TargetJournalID int
	SourceLines     []odooStatementLineForReconcile
	TargetLines     []odooStatementLineForReconcile
	AccountingMoves []odooJournalMergeAccountingMove
	PresentRows     []odooJournalMergePresenceRow
	MissingRows     []odooJournalMergePresenceRow
	ReconcilePlan   *odooReconcileMigrationPlan
}

type odooJournalMergePresenceRow struct {
	Source odooStatementLineForReconcile
	Target odooStatementLineForReconcile
	Reason string
}

type odooJournalMergeAccountingMove struct {
	ID               int
	Name             string
	Date             string
	State            string
	MoveType         string
	Ref              string
	PaymentReference string
	AmountTotal      float64
	LineCount        int
}

func odooJournalMerge(creds *OdooCredentials, uid int, sourceJournalID int, targetArg string, dryRun, verbose, yes bool) error {
	if !dryRun {
		if err := RequireOdooWriteCapability(); err != nil {
			return err
		}
	}
	targetJournalID, err := resolveJournalIDArg(targetArg)
	if err != nil {
		return err
	}
	if sourceJournalID == targetJournalID {
		return fmt.Errorf("--merge-with must point to a different target journal")
	}

	plan, err := buildOdooJournalMergePlan(creds, uid, sourceJournalID, targetJournalID)
	if err != nil {
		return err
	}
	printOdooJournalMergePlan(plan, dryRun, verbose)
	if dryRun {
		return nil
	}

	moveCount := countOdooReconcileMigrationActions(plan.ReconcilePlan, "move")
	if moveCount > 0 && !yes {
		if !confirmOdooJournalMergeMove(sourceJournalID, targetJournalID, moveCount) {
			fmt.Println("  Aborted.")
			return nil
		}
	}

	moved, failed := applyOdooReconcileMigrationPlan(creds, uid, plan.ReconcilePlan, yes)
	fmt.Printf("\n  %sMoved:%s %d  %sFailed:%s %d\n\n", Fmt.Dim, Fmt.Reset, moved, Fmt.Dim, Fmt.Reset, failed)
	if failed > 0 {
		return fmt.Errorf("reconciliation move failed for %d line(s); source journal was not deleted", failed)
	}

	createdMissing := 0
	if len(plan.MissingRows) > 0 && confirmOdooJournalMergeAddMissing(targetJournalID, len(plan.MissingRows)) {
		created, createFailures, err := createOdooJournalMergeMissingLines(creds, uid, plan)
		if err != nil {
			return err
		}
		createdMissing = created
		fmt.Printf("  %sCreated missing target lines:%s %d", Fmt.Dim, Fmt.Reset, created)
		if createFailures > 0 {
			fmt.Printf("  %sFailed:%s %d", Fmt.Dim, Fmt.Reset, createFailures)
		}
		fmt.Println()
	}

	if len(plan.AccountingMoves) > 0 {
		if !confirmOdooJournalMergeAccountingMoves(sourceJournalID, targetJournalID, plan.AccountingMoves) {
			fmt.Println("  Accounting entries were not moved.")
		} else if err := moveOdooJournalMergeAccountingMoves(creds, uid, plan); err != nil {
			return err
		}
	}

	if !confirmOdooJournalMergeDeleteSource(sourceJournalID, len(plan.SourceLines), len(plan.MissingRows)-createdMissing) {
		fmt.Println("  Source journal was not deleted.")
		return nil
	}
	if err := emptyOdooJournal(creds, uid, sourceJournalID, true); err != nil {
		return err
	}
	deleted, err := deleteOrArchiveOdooJournalRecord(creds, uid, sourceJournalID)
	if err != nil {
		return fmt.Errorf("source journal was emptied but could not be deleted or archived: %v", err)
	}
	if deleted {
		fmt.Printf("  %s✓ Deleted source journal #%d%s\n\n", Fmt.Green, sourceJournalID, Fmt.Reset)
	} else {
		fmt.Printf("  %s✓ Archived source journal #%d%s\n\n", Fmt.Green, sourceJournalID, Fmt.Reset)
	}
	return nil
}

func buildOdooJournalMergePlan(creds *OdooCredentials, uid int, sourceJournalID, targetJournalID int) (*odooJournalMergePlan, error) {
	sourceLines, err := fetchAllStatementLinesInJournal(creds, uid, sourceJournalID)
	if err != nil {
		return nil, err
	}
	targetLines, err := fetchAllStatementLinesInJournal(creds, uid, targetJournalID)
	if err != nil {
		return nil, err
	}
	accountingMoves, err := fetchStandaloneAccountingMovesInJournal(creds, uid, sourceJournalID)
	if err != nil {
		return nil, err
	}
	enrichOdooReconciledTargets(creds, uid, sourceLines)
	enrichOdooReconciledTargets(creds, uid, targetLines)

	targetByImport, targetByFallback, targetByDateAmount := indexOdooStatementLinesForMerge(targetLines)
	plan := &odooJournalMergePlan{
		SourceJournalID: sourceJournalID,
		TargetJournalID: targetJournalID,
		SourceLines:     sourceLines,
		TargetLines:     targetLines,
		AccountingMoves: accountingMoves,
	}
	for _, source := range sourceLines {
		target, reason, ok := findOdooReconcileMigrationTarget(source, targetByImport, targetByFallback, targetByDateAmount)
		row := odooJournalMergePresenceRow{Source: source, Target: target, Reason: reason}
		if ok {
			plan.PresentRows = append(plan.PresentRows, row)
		} else {
			plan.MissingRows = append(plan.MissingRows, row)
		}
	}

	reconcilePlan, err := buildOdooReconcileMigrationPlan(creds, uid, sourceJournalID, targetJournalID)
	if err != nil {
		return nil, err
	}
	plan.ReconcilePlan = reconcilePlan
	return plan, nil
}

func fetchAllStatementLinesInJournal(creds *OdooCredentials, uid int, journalID int) ([]odooStatementLineForReconcile, error) {
	rows, err := odooSearchReadAllMaps(creds, uid, "account.bank.statement.line",
		[]interface{}{[]interface{}{"journal_id", "=", journalID}},
		statementLineReconcileFields(),
		"date asc, id asc",
	)
	if err != nil {
		return nil, fmt.Errorf("fetch statement lines for journal #%d: %v", journalID, err)
	}
	return parseStatementLineRows(rows), nil
}

func fetchStandaloneAccountingMovesInJournal(creds *OdooCredentials, uid int, journalID int) ([]odooJournalMergeAccountingMove, error) {
	rows, err := odooSearchReadAllMaps(creds, uid, "account.move",
		[]interface{}{
			[]interface{}{"journal_id", "=", journalID},
			[]interface{}{"statement_line_id", "=", false},
		},
		[]string{"id", "name", "date", "state", "move_type", "ref", "payment_reference", "amount_total", "line_ids"},
		"date asc, id asc",
	)
	if err != nil {
		return nil, fmt.Errorf("fetch standalone accounting moves for journal #%d: %v", journalID, err)
	}
	moves := make([]odooJournalMergeAccountingMove, 0, len(rows))
	for _, row := range rows {
		moves = append(moves, odooJournalMergeAccountingMove{
			ID:               odooInt(row["id"]),
			Name:             odooString(row["name"]),
			Date:             odooString(row["date"]),
			State:            odooString(row["state"]),
			MoveType:         odooString(row["move_type"]),
			Ref:              odooString(row["ref"]),
			PaymentReference: odooString(row["payment_reference"]),
			AmountTotal:      odooFloat(row["amount_total"]),
			LineCount:        len(odooIDList(row["line_ids"])),
		})
	}
	return moves, nil
}

func indexOdooStatementLinesForMerge(lines []odooStatementLineForReconcile) (map[string]odooStatementLineForReconcile, map[string][]odooStatementLineForReconcile, map[string][]odooStatementLineForReconcile) {
	byImport := map[string]odooStatementLineForReconcile{}
	byFallback := map[string][]odooStatementLineForReconcile{}
	byDateAmount := map[string][]odooStatementLineForReconcile{}
	for _, line := range lines {
		if line.UniqueImportID != "" {
			byImport[line.UniqueImportID] = line
			if canonical := CanonicalizeImportID(line.UniqueImportID); canonical != "" {
				byImport[canonical] = line
			}
		}
		if key := odooReconcileFallbackKey(line); key != "" {
			byFallback[key] = append(byFallback[key], line)
		}
		if key := odooReconcileDateAmountKey(line); key != "" {
			byDateAmount[key] = append(byDateAmount[key], line)
		}
	}
	return byImport, byFallback, byDateAmount
}

func printOdooJournalMergePlan(plan *odooJournalMergePlan, dryRun, verbose bool) {
	mode := "apply"
	if dryRun {
		mode = "dry-run"
	}
	moveCount := countOdooReconcileMigrationActions(plan.ReconcilePlan, "move")
	skipCount := countOdooReconcileMigrationActions(plan.ReconcilePlan, "skip")
	fmt.Printf("\n  %sOdoo journal merge%s  %s%s%s\n", Fmt.Bold, Fmt.Reset, Fmt.Dim, mode, Fmt.Reset)
	fmt.Printf("  %sSource journal #%d -> target journal #%d%s\n\n", Fmt.Dim, plan.SourceJournalID, plan.TargetJournalID, Fmt.Reset)

	printOdooReconcileMigrationPlan(plan.ReconcilePlan, dryRun, verbose)

	fmt.Printf("\n  %sMerge summary%s\n", Fmt.Bold, Fmt.Reset)
	fmt.Printf("    Reconciliations to move:           %d\n", moveCount)
	fmt.Printf("    Accounting entries to move:        %d\n", len(plan.AccountingMoves))
	fmt.Printf("    Reconciled source lines skipped:   %d\n", skipCount)
	fmt.Printf("    Target journal lines:              %d\n", len(plan.TargetLines))
	fmt.Printf("    Source lines already in target:    %d\n", len(plan.PresentRows))
	fmt.Printf("    Source lines missing in target:    %d\n", len(plan.MissingRows))
	fmt.Printf("    Source lines deleted if confirmed: %d\n", len(plan.SourceLines))
	fmt.Printf("    Source journal deletion step:      #%d\n", plan.SourceJournalID)

	if len(plan.MissingRows) > 0 {
		fmt.Printf("    %s⚠ Missing source lines are not represented in the target journal unless you confirm the add step.%s\n", Fmt.Yellow, Fmt.Reset)
	}
	if verbose {
		printOdooJournalMergeAccountingMoveTable(plan.AccountingMoves)
		printOdooJournalMergePresenceTable("Source lines missing in target", plan.MissingRows, false)
		printOdooJournalMergePresenceTable("Source lines already present in target", plan.PresentRows, true)
	} else if len(plan.AccountingMoves) > 0 {
		fmt.Printf("    %sUse --verbose to list accounting entries that will be moved.%s\n", Fmt.Dim, Fmt.Reset)
	}
	fmt.Println()
}

func printOdooJournalMergeAccountingMoveTable(rows []odooJournalMergeAccountingMove) {
	if len(rows) == 0 {
		return
	}
	fmt.Printf("\n  %sAccounting entries to move%s\n", Fmt.Bold, Fmt.Reset)
	tableRows := make([][]string, 0, len(rows))
	for _, row := range rows {
		tableRows = append(tableRows, []string{
			fmt.Sprintf("#%d", row.ID),
			row.Date,
			Truncate(row.Name, 18),
			row.State,
			row.MoveType,
			formatBalancePlain(row.AmountTotal, "EUR"),
			Truncate(firstNonEmpty(row.Ref, row.PaymentReference), 64),
		})
	}
	renderTicketsTable([]string{"ID", "Date", "Move", "State", "Type", "Total", "Ref"}, tableRows, nil, map[int]bool{5: true})
}

func printOdooJournalMergePresenceTable(title string, rows []odooJournalMergePresenceRow, includeTarget bool) {
	if len(rows) == 0 {
		return
	}
	fmt.Printf("\n  %s%s%s\n", Fmt.Bold, title, Fmt.Reset)
	headers := []string{"Date", "Amount", "Description", "Ref", "Reason"}
	if includeTarget {
		headers = []string{"Date", "Amount", "Description", "Target line", "Ref", "Reason"}
	}
	tableRows := make([][]string, 0, len(rows))
	for _, row := range rows {
		base := []string{
			row.Source.Date,
			formatBalancePlain(row.Source.Amount, "EUR"),
			Truncate(row.Source.PaymentRef, 48),
		}
		if includeTarget {
			base = append(base, fmt.Sprintf("#%d", row.Target.ID))
		}
		base = append(base,
			Truncate(row.Source.UniqueImportID, 42),
			row.Reason,
		)
		tableRows = append(tableRows, base)
	}
	renderTicketsTable(headers, tableRows, nil, map[int]bool{1: true})
}

func countOdooReconcileMigrationActions(plan *odooReconcileMigrationPlan, action string) int {
	if plan == nil {
		return 0
	}
	count := 0
	for _, row := range plan.Rows {
		if row.Action == action {
			count++
		}
	}
	return count
}

func confirmOdooJournalMergeMove(sourceJournalID, targetJournalID, moveCount int) bool {
	fmt.Printf("\n  %sMove %d reconciliations from journal #%d to journal #%d?%s [y/N] ",
		Fmt.Bold, moveCount, sourceJournalID, targetJournalID, Fmt.Reset)
	reader := bufio.NewReader(os.Stdin)
	resp, _ := reader.ReadString('\n')
	resp = strings.TrimSpace(strings.ToLower(resp))
	return resp == "y" || resp == "yes"
}

func confirmOdooJournalMergeAddMissing(targetJournalID, missingCount int) bool {
	fmt.Printf("\n  %sDo you want to add %d missing lines to journal #%d?%s [y/N] ",
		Fmt.Bold, missingCount, targetJournalID, Fmt.Reset)
	reader := bufio.NewReader(os.Stdin)
	resp, _ := reader.ReadString('\n')
	resp = strings.TrimSpace(strings.ToLower(resp))
	return resp == "y" || resp == "yes"
}

func confirmOdooJournalMergeAccountingMoves(sourceJournalID, targetJournalID int, moves []odooJournalMergeAccountingMove) bool {
	// Show the entries before asking — these are account.move records with
	// no statement_line_id (manual journal entries, year-end adjustments,
	// historical imports). The operator needs to see what they are to
	// decide whether they're keepers or junk to discard with the journal.
	printOdooJournalMergeAccountingMoveTable(moves)
	fmt.Printf("\n  %sThese are standalone accounting entries (not linked to a bank statement line):%s\n", Fmt.Dim, Fmt.Reset)
	fmt.Printf("  %s  manual journal entries, opening balances, year-end adjustments, etc.%s\n", Fmt.Dim, Fmt.Reset)
	fmt.Printf("  %sSay yes to keep them by reassigning to journal #%d; say no to discard them with journal #%d.%s\n",
		Fmt.Dim, targetJournalID, sourceJournalID, Fmt.Reset)
	fmt.Printf("\n  %sMove %d standalone accounting entries from journal #%d to journal #%d?%s [Y/n] ",
		Fmt.Bold, len(moves), sourceJournalID, targetJournalID, Fmt.Reset)
	reader := bufio.NewReader(os.Stdin)
	resp, _ := reader.ReadString('\n')
	resp = strings.TrimSpace(strings.ToLower(resp))
	return resp == "" || resp == "y" || resp == "yes"
}

func confirmOdooJournalMergeDeleteSource(sourceJournalID, sourceLineCount, stillMissingCount int) bool {
	fmt.Printf("\n  %sDo you want to delete journal #%d? This cannot be reversed.%s\n",
		Fmt.Bold, sourceJournalID, Fmt.Reset)
	if stillMissingCount > 0 {
		fmt.Printf("  %s⚠ %d source line(s) are still missing from the target journal.%s\n  %sDelete anyway?%s [Y/n] ",
			Fmt.Yellow, stillMissingCount, Fmt.Reset, Fmt.Bold, Fmt.Reset)
	} else if sourceLineCount > 0 {
		fmt.Printf("  %sThis will first delete %d source statement line(s) and their statements.%s\n  %sDelete journal #%d?%s [Y/n] ",
			Fmt.Yellow, sourceLineCount, Fmt.Reset, Fmt.Bold, sourceJournalID, Fmt.Reset)
	} else {
		fmt.Printf("  %sDelete journal #%d?%s [Y/n] ", Fmt.Bold, sourceJournalID, Fmt.Reset)
	}
	reader := bufio.NewReader(os.Stdin)
	resp, _ := reader.ReadString('\n')
	resp = strings.TrimSpace(strings.ToLower(resp))
	return resp == "" || resp == "y" || resp == "yes"
}

func createOdooJournalMergeMissingLines(creds *OdooCredentials, uid int, plan *odooJournalMergePlan) (created, failed int, err error) {
	targetStatements, err := fetchTargetStatementsForMerge(creds, uid, plan.TargetJournalID)
	if err != nil {
		return 0, 0, err
	}
	lines := make([]map[string]interface{}, 0, len(plan.MissingRows))
	for _, row := range plan.MissingRows {
		line := map[string]interface{}{
			"journal_id":  plan.TargetJournalID,
			"date":        row.Source.Date,
			"payment_ref": row.Source.PaymentRef,
			"amount":      row.Source.Amount,
		}
		if row.Source.UniqueImportID != "" {
			line["unique_import_id"] = row.Source.UniqueImportID
		}
		if row.Source.Narration != "" {
			line["narration"] = row.Source.Narration
		}
		if row.Source.PartnerID > 0 {
			line["partner_id"] = row.Source.PartnerID
		}
		if row.Source.PartnerBankID > 0 {
			line["partner_bank_id"] = row.Source.PartnerBankID
		}
		if stmtID := findTargetStatementForLineDate(targetStatements, row.Source.Date); stmtID > 0 {
			line["statement_id"] = stmtID
		}
		lines = append(lines, line)
	}
	result, err := batchCreateStatementLinesWithProgressReport(creds, uid, lines, "for journal merge")
	if err != nil {
		return 0, 0, err
	}
	return len(result.IDs), len(result.Failures), nil
}

func moveOdooJournalMergeAccountingMoves(creds *OdooCredentials, uid int, plan *odooJournalMergePlan) error {
	ids := make([]int, 0, len(plan.AccountingMoves))
	for _, move := range plan.AccountingMoves {
		if move.ID > 0 {
			ids = append(ids, move.ID)
		}
	}
	if len(ids) == 0 {
		return nil
	}
	_, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.move", "write",
		[]interface{}{intsToInterfaces(ids), map[string]interface{}{
			"journal_id": plan.TargetJournalID,
			"name":       "/",
		}}, nil)
	if err != nil {
		return fmt.Errorf("move standalone accounting entries to journal #%d: %v", plan.TargetJournalID, err)
	}
	fmt.Printf("  %s✓ Moved %s to journal #%d%s\n", Fmt.Green, Pluralize(len(ids), "standalone accounting entry", ""), plan.TargetJournalID, Fmt.Reset)
	return nil
}

type odooJournalMergeStatement struct {
	ID   int
	Date string
}

func fetchTargetStatementsForMerge(creds *OdooCredentials, uid int, journalID int) ([]odooJournalMergeStatement, error) {
	rows, err := odooSearchReadAllMaps(creds, uid, "account.bank.statement",
		[]interface{}{[]interface{}{"journal_id", "=", journalID}},
		[]string{"id", "date"},
		"date asc, id asc",
	)
	if err != nil {
		return nil, fmt.Errorf("fetch target statements: %v", err)
	}
	stmts := make([]odooJournalMergeStatement, 0, len(rows))
	for _, row := range rows {
		stmts = append(stmts, odooJournalMergeStatement{ID: odooInt(row["id"]), Date: odooString(row["date"])})
	}
	return stmts, nil
}

func findTargetStatementForLineDate(stmts []odooJournalMergeStatement, lineDate string) int {
	if lineDate == "" {
		return 0
	}
	for _, stmt := range stmts {
		if stmt.ID == 0 || stmt.Date == "" {
			continue
		}
		if stmt.Date >= lineDate {
			return stmt.ID
		}
	}
	return 0
}

func deleteOdooJournalRecord(creds *OdooCredentials, uid int, journalID int) error {
	_, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.journal", "unlink",
		[]interface{}{[]interface{}{journalID}}, nil)
	return err
}

func deleteOrArchiveOdooJournalRecord(creds *OdooCredentials, uid int, journalID int) (bool, error) {
	if err := deleteOdooJournalRecord(creds, uid, journalID); err == nil {
		return true, nil
	}
	if err := archiveOdooJournalRecord(creds, uid, journalID); err != nil {
		return false, err
	}
	return false, nil
}

func archiveOdooJournalRecord(creds *OdooCredentials, uid int, journalID int) error {
	_, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.journal", "write",
		[]interface{}{[]interface{}{journalID}, map[string]interface{}{"active": false}}, nil)
	return err
}
