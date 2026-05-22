package cmd

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
)

type odooReconcileMigrationPlan struct {
	SourceJournalID int
	TargetJournalID int
	Rows            []odooReconcileMigrationRow
}

type odooReconcileMigrationRow struct {
	Action string
	Reason string
	Source odooStatementLineForReconcile
	Target odooStatementLineForReconcile
}

func OdooReconcileCommand(args []string) error {
	if HasFlag(args, "--help", "-h") || len(args) == 0 {
		printOdooReconcileMigrateHelp()
		return nil
	}
	if err := RequireOdooWriteCapability(); err != nil {
		return err
	}
	targetJournalID, err := resolveJournalIDArg(args[0])
	if err != nil {
		return err
	}
	fromArg := GetOption(args, "--from-journal")
	if fromArg == "" {
		return fmt.Errorf("--from-journal <id|slug> is required")
	}
	sourceJournalID, err := resolveJournalIDArg(fromArg)
	if err != nil {
		return err
	}
	if sourceJournalID == targetJournalID {
		return fmt.Errorf("--from-journal must be different from the target journal")
	}

	creds, err := ResolveOdooCredentials()
	if err != nil {
		return err
	}
	uid, err := odooAuth(creds.URL, creds.DB, creds.Login, creds.Password)
	if err != nil || uid == 0 {
		return fmt.Errorf("Odoo authentication failed: %v", err)
	}
	dryRun := HasFlag(args, "--dry-run")
	yes := HasFlag(args, "--yes", "-y")
	verbose := HasFlag(args, "--verbose", "-v")

	plan, err := buildOdooReconcileMigrationPlan(creds, uid, sourceJournalID, targetJournalID)
	if err != nil {
		return err
	}
	printOdooReconcileMigrationPlan(plan, dryRun, verbose)
	if dryRun {
		return nil
	}

	applyCount := 0
	for _, row := range plan.Rows {
		if row.Action == "move" {
			applyCount++
		}
	}
	if applyCount == 0 {
		return nil
	}
	if !yes {
		fmt.Printf("\n  %sMove %s from journal #%d to #%d?%s [y/N] ",
			Fmt.Bold, Pluralize(applyCount, "reconciliation", ""), sourceJournalID, targetJournalID, Fmt.Reset)
		reader := bufio.NewReader(os.Stdin)
		resp, _ := reader.ReadString('\n')
		resp = strings.TrimSpace(strings.ToLower(resp))
		if resp != "y" && resp != "yes" {
			fmt.Println("  Aborted.")
			return nil
		}
	}

	moved, failed := applyOdooReconcileMigrationPlan(creds, uid, plan, yes)
	fmt.Printf("\n  %sMoved:%s %d  %sFailed:%s %d\n\n", Fmt.Dim, Fmt.Reset, moved, Fmt.Dim, Fmt.Reset, failed)
	return nil
}

func buildOdooReconcileMigrationPlan(creds *OdooCredentials, uid int, sourceJournalID, targetJournalID int) (*odooReconcileMigrationPlan, error) {
	sourceLines, err := fetchJournalReconciledStatementLines(creds, uid, sourceJournalID)
	if err != nil {
		return nil, err
	}
	enrichOdooReconciledTargets(creds, uid, sourceLines)
	importIDs := make([]string, 0, len(sourceLines))
	minDate, maxDate := "", ""
	for _, line := range sourceLines {
		if line.UniqueImportID != "" {
			importIDs = append(importIDs, line.UniqueImportID)
			if canonical := CanonicalizeImportID(line.UniqueImportID); canonical != "" {
				importIDs = append(importIDs, canonical)
			}
		}
		if line.Date != "" {
			if minDate == "" || line.Date < minDate {
				minDate = line.Date
			}
			if maxDate == "" || line.Date > maxDate {
				maxDate = line.Date
			}
		}
	}
	targetLines, err := fetchStatementLinesByImportIDInJournal(creds, uid, targetJournalID, importIDs)
	if err != nil {
		return nil, err
	}
	enrichOdooReconciledTargets(creds, uid, targetLines)
	fallbackTargetLines, err := fetchStatementLinesByDateWindowInJournal(creds, uid, targetJournalID, minDate, maxDate)
	if err != nil {
		return nil, err
	}
	enrichOdooReconciledTargets(creds, uid, fallbackTargetLines)
	targetByImport := map[string]odooStatementLineForReconcile{}
	for _, line := range targetLines {
		targetByImport[line.UniqueImportID] = line
		if canonical := CanonicalizeImportID(line.UniqueImportID); canonical != "" {
			targetByImport[canonical] = line
		}
	}
	targetByFallback := map[string][]odooStatementLineForReconcile{}
	targetByDateAmount := map[string][]odooStatementLineForReconcile{}
	for _, line := range fallbackTargetLines {
		key := odooReconcileFallbackKey(line)
		if key != "" {
			targetByFallback[key] = append(targetByFallback[key], line)
		}
		dateAmountKey := odooReconcileDateAmountKey(line)
		if dateAmountKey != "" {
			targetByDateAmount[dateAmountKey] = append(targetByDateAmount[dateAmountKey], line)
		}
	}

	plan := &odooReconcileMigrationPlan{SourceJournalID: sourceJournalID, TargetJournalID: targetJournalID}
	for _, source := range sourceLines {
		row := odooReconcileMigrationRow{Source: source, Action: "skip"}
		target, reason, ok := findOdooReconcileMigrationTarget(source, targetByImport, targetByFallback, targetByDateAmount)
		if !ok {
			row.Reason = reason
		} else {
			row.Target = target
			row.Action, row.Reason = classifyOdooReconcileMigrationMatch(source, target, reason)
			if row.Action == "move" && (strings.TrimSpace(source.ReconciledTo) == "" || len(source.ReconciledLineIDs) == 0) {
				row.Action = "skip"
				row.Reason = "current reconciliation target unknown"
			}
			if row.Action == "move" && target.IsReconciled && strings.TrimSpace(target.ReconciledTo) == "" {
				row.Action = "skip"
				row.Reason = "target reconciliation target unknown"
			}
		}
		plan.Rows = append(plan.Rows, row)
	}
	return plan, nil
}

func findOdooReconcileMigrationTarget(source odooStatementLineForReconcile, targetByImport map[string]odooStatementLineForReconcile, targetByFallback map[string][]odooStatementLineForReconcile, targetByDateAmount map[string][]odooStatementLineForReconcile) (odooStatementLineForReconcile, string, bool) {
	if source.UniqueImportID != "" {
		if target, ok := targetByImport[source.UniqueImportID]; ok {
			return target, "same unique_import_id", true
		}
		if canonical := CanonicalizeImportID(source.UniqueImportID); canonical != "" {
			if target, ok := targetByImport[canonical]; ok {
				return target, "canonical unique_import_id", true
			}
		}
	}

	descriptionKey := odooReconcileFallbackKey(source)
	if descriptionKey != "" {
		targets := targetByFallback[descriptionKey]
		switch len(targets) {
		case 1:
			return targets[0], "date+amount+description", true
		case 0:
		default:
			return odooStatementLineForReconcile{}, "ambiguous date+amount+description match", false
		}
	}

	dateAmountKey := odooReconcileDateAmountKey(source)
	if dateAmountKey == "" {
		if source.UniqueImportID == "" {
			return odooStatementLineForReconcile{}, "source has no unique_import_id", false
		}
		return odooStatementLineForReconcile{}, "missing in target journal", false
	}
	targets := targetByDateAmount[dateAmountKey]
	switch len(targets) {
	case 0:
		if source.UniqueImportID == "" {
			return odooStatementLineForReconcile{}, "no date+amount match", false
		}
		return odooStatementLineForReconcile{}, "missing in target journal", false
	case 1:
		return targets[0], "date+amount", true
	default:
		return odooStatementLineForReconcile{}, "ambiguous date+amount match", false
	}
}

func classifyOdooReconcileMigrationMatch(source, target odooStatementLineForReconcile, reason string) (string, string) {
	switch {
	case math.Abs(source.Amount-target.Amount) > 0.01:
		return "skip", "amount mismatch"
	case target.IsReconciled:
		return "move", fmt.Sprintf("%s; override %s → %s", reason, target.ReconciledTo, source.ReconciledTo)
	default:
		return "move", reason
	}
}

func fetchJournalReconciledStatementLines(creds *OdooCredentials, uid int, journalID int) ([]odooStatementLineForReconcile, error) {
	rows, err := odooSearchReadAllMaps(creds, uid, "account.bank.statement.line",
		[]interface{}{
			[]interface{}{"journal_id", "=", journalID},
			[]interface{}{"is_reconciled", "=", true},
		},
		statementLineReconcileFields(),
		"date asc, id asc",
	)
	if err != nil {
		return nil, fmt.Errorf("fetch reconciled statement lines: %v", err)
	}
	return parseStatementLineRows(rows), nil
}

func enrichOdooReconciledTargets(creds *OdooCredentials, uid int, lines []odooStatementLineForReconcile) {
	if len(lines) == 0 {
		return
	}
	moveToLine := map[int]int{}
	var moveIDs []int
	for i, line := range lines {
		if line.MoveID <= 0 {
			continue
		}
		moveToLine[line.MoveID] = i
		moveIDs = append(moveIDs, line.MoveID)
	}
	if len(moveIDs) == 0 {
		return
	}
	moveIDSet := map[int]bool{}
	for _, id := range moveIDs {
		moveIDSet[id] = true
	}
	rows, err := odooSearchReadAllMaps(creds, uid, "account.move.line",
		[]interface{}{
			[]interface{}{"move_id", "in", intsToInterfaces(moveIDs)},
			[]interface{}{"reconciled", "=", true},
			[]interface{}{"account_id.reconcile", "=", true},
		},
		[]string{"id", "move_id", "full_reconcile_id", "matched_debit_ids", "matched_credit_ids"},
		"id asc",
	)
	if err != nil {
		return
	}
	lineFullRecs := map[int][]int{}
	fullRecIDs := map[int]bool{}
	sourceMoveLineToLine := map[int]int{}
	partialToLine := map[int][]int{}
	partialIDs := map[int]bool{}
	for _, row := range rows {
		moveID := odooFieldID(row["move_id"])
		lineIdx, ok := moveToLine[moveID]
		if !ok {
			continue
		}
		moveLineID := odooInt(row["id"])
		if moveLineID > 0 {
			sourceMoveLineToLine[moveLineID] = lineIdx
		}
		fullRecID := odooFieldID(row["full_reconcile_id"])
		if fullRecID > 0 {
			lineFullRecs[lineIdx] = append(lineFullRecs[lineIdx], fullRecID)
			fullRecIDs[fullRecID] = true
		}
		for _, partialID := range append(odooIDList(row["matched_debit_ids"]), odooIDList(row["matched_credit_ids"])...) {
			if partialID <= 0 {
				continue
			}
			partialToLine[partialID] = append(partialToLine[partialID], lineIdx)
			partialIDs[partialID] = true
		}
	}

	lineTargets := map[int][]string{}
	lineTargetIDs := map[int][]int{}
	if len(fullRecIDs) > 0 {
		fullRecValues := make([]interface{}, 0, len(fullRecIDs))
		for id := range fullRecIDs {
			fullRecValues = append(fullRecValues, id)
		}
		otherRows, err := odooSearchReadAllMaps(creds, uid, "account.move.line",
			[]interface{}{
				[]interface{}{"full_reconcile_id", "in", fullRecValues},
				[]interface{}{"move_id", "not in", intsToInterfaces(moveIDs)},
				[]interface{}{"account_id.reconcile", "=", true},
			},
			[]string{"id", "move_id", "full_reconcile_id"},
			"date asc, id asc",
		)
		if err == nil {
			fullRecTargets := map[int][]string{}
			fullRecTargetLineIDs := map[int][]int{}
			for _, row := range otherRows {
				fullRecID := odooFieldID(row["full_reconcile_id"])
				moveID := odooFieldID(row["move_id"])
				if fullRecID <= 0 || moveIDSet[moveID] {
					continue
				}
				if target := odooMoveFieldDisplay(row["move_id"]); target != "" {
					fullRecTargets[fullRecID] = append(fullRecTargets[fullRecID], target)
				}
				if id := odooInt(row["id"]); id > 0 {
					fullRecTargetLineIDs[fullRecID] = append(fullRecTargetLineIDs[fullRecID], id)
				}
			}
			for lineIdx, fullIDs := range lineFullRecs {
				for _, fullID := range fullIDs {
					lineTargets[lineIdx] = append(lineTargets[lineIdx], fullRecTargets[fullID]...)
					lineTargetIDs[lineIdx] = append(lineTargetIDs[lineIdx], fullRecTargetLineIDs[fullID]...)
				}
			}
		}
	}

	if len(partialIDs) > 0 {
		partialValues := make([]interface{}, 0, len(partialIDs))
		for id := range partialIDs {
			partialValues = append(partialValues, id)
		}
		partialRows, err := odooSearchReadAllMaps(creds, uid, "account.partial.reconcile",
			[]interface{}{[]interface{}{"id", "in", partialValues}},
			[]string{"id", "debit_move_id", "credit_move_id"},
			"id asc",
		)
		if err == nil {
			oppositeMoveLineToLines := map[int][]int{}
			oppositeIDs := map[int]bool{}
			for _, row := range partialRows {
				partialID := odooInt(row["id"])
				debitID := odooFieldID(row["debit_move_id"])
				creditID := odooFieldID(row["credit_move_id"])
				lineIdxs := partialToLine[partialID]
				for _, lineIdx := range lineIdxs {
					oppositeID := 0
					if _, ok := sourceMoveLineToLine[debitID]; ok {
						oppositeID = creditID
					} else if _, ok := sourceMoveLineToLine[creditID]; ok {
						oppositeID = debitID
					}
					if oppositeID > 0 {
						oppositeMoveLineToLines[oppositeID] = append(oppositeMoveLineToLines[oppositeID], lineIdx)
						oppositeIDs[oppositeID] = true
					}
				}
			}
			if len(oppositeIDs) > 0 {
				oppositeValues := make([]interface{}, 0, len(oppositeIDs))
				for id := range oppositeIDs {
					oppositeValues = append(oppositeValues, id)
				}
				oppositeRows, err := odooSearchReadAllMaps(creds, uid, "account.move.line",
					[]interface{}{[]interface{}{"id", "in", oppositeValues}},
					[]string{"id", "move_id"},
					"id asc",
				)
				if err == nil {
					for _, row := range oppositeRows {
						moveID := odooFieldID(row["move_id"])
						if moveIDSet[moveID] {
							continue
						}
						target := odooMoveFieldDisplay(row["move_id"])
						if target == "" {
							continue
						}
						for _, lineIdx := range oppositeMoveLineToLines[odooInt(row["id"])] {
							lineTargets[lineIdx] = append(lineTargets[lineIdx], target)
							lineTargetIDs[lineIdx] = append(lineTargetIDs[lineIdx], odooInt(row["id"]))
						}
					}
				}
			}
		}
	}

	for lineIdx, targets := range lineTargets {
		lines[lineIdx].ReconciledTo = compactReconciliationTargetList(targets)
		lines[lineIdx].ReconciledLineIDs = uniquePositiveInts(lineTargetIDs[lineIdx])
	}
}

func fetchStatementLinesByImportIDInJournal(creds *OdooCredentials, uid int, journalID int, importIDs []string) ([]odooStatementLineForReconcile, error) {
	values := make([]interface{}, 0, len(importIDs))
	seen := map[string]bool{}
	for _, id := range importIDs {
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		values = append(values, id)
	}
	if len(values) == 0 {
		return nil, nil
	}
	rows, err := odooSearchReadAllMaps(creds, uid, "account.bank.statement.line",
		[]interface{}{
			[]interface{}{"journal_id", "=", journalID},
			[]interface{}{"unique_import_id", "in", values},
		},
		statementLineReconcileFields(),
		"date asc, id asc",
	)
	if err != nil {
		return nil, err
	}
	return parseStatementLineRows(rows), nil
}

func fetchStatementLinesByDateWindowInJournal(creds *OdooCredentials, uid int, journalID int, minDate, maxDate string) ([]odooStatementLineForReconcile, error) {
	if minDate == "" || maxDate == "" {
		return nil, nil
	}
	rows, err := odooSearchReadAllMaps(creds, uid, "account.bank.statement.line",
		[]interface{}{
			[]interface{}{"journal_id", "=", journalID},
			[]interface{}{"date", ">=", minDate},
			[]interface{}{"date", "<=", maxDate},
		},
		statementLineReconcileFields(),
		"date asc, id asc",
	)
	if err != nil {
		return nil, err
	}
	return parseStatementLineRows(rows), nil
}

func odooReconcileFallbackKey(line odooStatementLineForReconcile) string {
	desc := strings.Join(strings.Fields(strings.ToLower(strings.TrimSpace(line.PaymentRef))), " ")
	if line.Date == "" || desc == "" {
		return ""
	}
	return odooReconcileDateAmountKey(line) + "|" + desc
}

func odooReconcileDateAmountKey(line odooStatementLineForReconcile) string {
	if line.Date == "" {
		return ""
	}
	cents := int64(math.Round(line.Amount * 100))
	return line.Date + "|" + strconv.FormatInt(cents, 10)
}

func compactReconciliationTargetList(values []string) string {
	seen := map[string]bool{}
	var uniq []string
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		uniq = append(uniq, value)
	}
	if len(uniq) == 0 {
		return ""
	}
	if len(uniq) > 2 {
		return strings.Join(uniq[:2], ", ") + fmt.Sprintf(" +%d", len(uniq)-2)
	}
	return strings.Join(uniq, ", ")
}

func uniquePositiveInts(values []int) []int {
	seen := map[int]bool{}
	var out []int
	for _, value := range values {
		if value <= 0 || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	return out
}

func odooMoveFieldDisplay(value interface{}) string {
	if name := odooFieldName(value); name != "" {
		return name
	}
	if id := odooFieldID(value); id > 0 {
		return fmt.Sprintf("move #%d", id)
	}
	return ""
}

func printOdooReconcileMigrationPlan(plan *odooReconcileMigrationPlan, dryRun, verbose bool) {
	mode := "apply"
	if dryRun {
		mode = "dry-run"
	}
	fmt.Printf("\n  %sOdoo reconciliation migration%s  %s%s%s\n", Fmt.Bold, Fmt.Reset, Fmt.Dim, mode, Fmt.Reset)
	fmt.Printf("  %sFrom journal #%d -> journal #%d%s\n\n", Fmt.Dim, plan.SourceJournalID, plan.TargetJournalID, Fmt.Reset)
	headers := []string{"Action", "Date", "Amount", "Description", "Target currently", "Will reconcile to", "Ref", "Reason"}
	rows := make([][]string, 0, len(plan.Rows))
	counts := map[string]int{}
	for _, row := range plan.Rows {
		counts[row.Action]++
		if !verbose && row.Action != "move" {
			continue
		}
		rows = append(rows, []string{
			row.Action,
			row.Source.Date,
			formatBalancePlain(row.Source.Amount, "EUR"),
			Truncate(row.Source.PaymentRef, 46),
			Truncate(row.Target.ReconciledTo, 28),
			Truncate(row.Source.ReconciledTo, 30),
			Truncate(row.Source.UniqueImportID, 34),
			row.Reason,
		})
	}
	total := fmt.Sprintf("%s, %s", Pluralize(counts["move"], "move", ""), Pluralize(counts["skip"], "skip", ""))
	if !verbose && counts["skip"] > 0 {
		total += " (--verbose to list skipped)"
	}
	renderTicketsTable(headers, rows, []string{"", "", "", total, "", "", "", ""}, map[int]bool{2: true})
}

func applyOdooReconcileMigrationPlan(creds *OdooCredentials, uid int, plan *odooReconcileMigrationPlan, assumeYes bool) (moved, failed int) {
	status := newStatusLine()
	total := 0
	for _, row := range plan.Rows {
		if row.Action == "move" {
			total++
		}
	}
	done := 0
	for _, row := range plan.Rows {
		if row.Action != "move" {
			continue
		}
		if row.Target.IsReconciled && !assumeYes {
			status.Clear()
			if !confirmOdooReconcileOverride(row) {
				continue
			}
		}
		done++
		if !quietOdooContext() {
			status.Update("Moving reconciliations %d/%d", done, total)
		}
		if err := moveOdooLineReconciliation(creds, uid, row.Source, row.Target); err != nil {
			failed++
			Warnf("  %s⚠ %s: %v%s", Fmt.Yellow, row.Source.UniqueImportID, err, Fmt.Reset)
			continue
		}
		moved++
	}
	status.Clear()
	return moved, failed
}

func confirmOdooReconcileOverride(row odooReconcileMigrationRow) bool {
	fmt.Printf("\n  %sOverride target reconciliation?%s\n", Fmt.Bold, Fmt.Reset)
	fmt.Printf("    Date:        %s\n", row.Source.Date)
	fmt.Printf("    Amount:      %s\n", formatBalancePlain(row.Source.Amount, "EUR"))
	fmt.Printf("    Description: %s\n", row.Source.PaymentRef)
	fmt.Printf("    Currently:   %s\n", firstNonEmpty(row.Target.ReconciledTo, "unknown"))
	fmt.Printf("    New target:  %s\n", row.Source.ReconciledTo)
	fmt.Printf("    Line ref:    %s\n", row.Source.UniqueImportID)
	fmt.Printf("  %sOverride this reconciliation?%s [y/N] ", Fmt.Bold, Fmt.Reset)
	reader := bufio.NewReader(os.Stdin)
	resp, _ := reader.ReadString('\n')
	resp = strings.TrimSpace(strings.ToLower(resp))
	return resp == "y" || resp == "yes"
}

func moveOdooLineReconciliation(creds *OdooCredentials, uid int, source, target odooStatementLineForReconcile) error {
	if len(source.ReconciledLineIDs) == 0 {
		return fmt.Errorf("source reconciliation target unknown")
	}
	if target.IsReconciled {
		if err := unreconcileStatementLineMove(creds, uid, target); err != nil {
			return fmt.Errorf("unreconcile target: %v", err)
		}
	}
	if err := unreconcileStatementLineMove(creds, uid, source); err != nil {
		return fmt.Errorf("unreconcile source: %v", err)
	}
	updatedTarget, err := readStatementLineForReconcile(creds, uid, target.ID)
	if err != nil {
		return err
	}
	return reconcileStatementLineWithMoveLines(creds, uid, updatedTarget, source.ReconciledLineIDs)
}

func reconcileStatementLineWithMoveLines(creds *OdooCredentials, uid int, line odooStatementLineForReconcile, counterpartLineIDs []int) error {
	if line.MoveID == 0 {
		return fmt.Errorf("target statement line has no move")
	}
	targetLineID, err := prepareTargetStatementMoveLineForExactReconcile(creds, uid, line, counterpartLineIDs)
	if err != nil {
		return err
	}
	ids := append([]int{targetLineID}, counterpartLineIDs...)
	_, err = odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.move.line", "reconcile",
		[]interface{}{intsToInterfaces(uniquePositiveInts(ids))}, nil)
	return err
}

func prepareTargetStatementMoveLineForExactReconcile(creds *OdooCredentials, uid int, line odooStatementLineForReconcile, counterpartLineIDs []int) (int, error) {
	if len(counterpartLineIDs) == 0 {
		return 0, fmt.Errorf("source reconciliation target unknown")
	}
	counterparts, err := odooReadMapsByIDs(creds, uid, "account.move.line", counterpartLineIDs, []string{"account_id", "partner_id"})
	if err != nil {
		return 0, fmt.Errorf("read reconciliation counterpart lines: %v", err)
	}
	accountID := 0
	partnerID := 0
	for _, row := range counterparts {
		id := odooFieldID(row["account_id"])
		if id == 0 {
			continue
		}
		if accountID == 0 {
			accountID = id
		} else if accountID != id {
			return 0, fmt.Errorf("source reconciliation spans multiple accounts")
		}
		if partnerID == 0 {
			partnerID = odooFieldID(row["partner_id"])
		}
	}
	if accountID == 0 {
		return 0, fmt.Errorf("source reconciliation target has no account")
	}
	targetLineID, err := findStatementCounterpartMoveLine(creds, uid, line)
	if err != nil {
		return 0, fmt.Errorf("find target counterpart move line: %v", err)
	}
	if targetLineID == 0 {
		return 0, fmt.Errorf("could not identify target counterpart move line")
	}
	values := map[string]interface{}{"account_id": accountID}
	if partnerID > 0 {
		values["partner_id"] = partnerID
	}
	_, err = odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.move.line", "write",
		[]interface{}{[]interface{}{targetLineID}, values}, nil)
	if err != nil {
		return 0, fmt.Errorf("prepare target counterpart move line #%d: %v", targetLineID, err)
	}
	return targetLineID, nil
}

func unreconcileStatementLineMove(creds *OdooCredentials, uid int, line odooStatementLineForReconcile) error {
	if line.MoveID == 0 {
		return fmt.Errorf("source statement line has no move")
	}
	rows, err := odooSearchReadAllMaps(creds, uid, "account.move.line",
		[]interface{}{
			[]interface{}{"move_id", "=", line.MoveID},
			[]interface{}{"reconciled", "=", true},
			[]interface{}{"account_id.reconcile", "=", true},
		},
		[]string{"id"},
		"id asc",
	)
	if err != nil {
		return err
	}
	ids := make([]interface{}, 0, len(rows))
	for _, row := range rows {
		if id := odooInt(row["id"]); id > 0 {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return fmt.Errorf("source has no reconciled move lines")
	}
	_, err = odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.move.line", "remove_move_reconcile",
		[]interface{}{ids}, nil)
	return err
}

func printOdooReconcileMigrateHelp() {
	f := Fmt
	fmt.Printf(`
%schb odoo reconcile <journalId|slug>%s — Move reconciliations from another
journal onto equivalent statement lines in this journal, matched by
unique_import_id.

%sUSAGE%s
  %schb odoo reconcile 53 --from-journal 30 --dry-run%s
  %schb odoo reconcile 53 --from-journal 30 --yes%s

%sOPTIONS%s
  %s--from-journal <id|slug>%s  Source journal whose reconciled lines are moved
  %s--dry-run%s                 Preview only; no writes to Odoo
  %s-v%s, %s--verbose%s            Also list skipped/no-match source lines
  %s-y%s, %s--yes%s                Skip confirmation
`,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset, f.Yellow, f.Reset,
		f.Yellow, f.Reset, f.Yellow, f.Reset,
	)
}
