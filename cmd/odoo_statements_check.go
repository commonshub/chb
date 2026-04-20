package cmd

import (
	"encoding/json"
	"fmt"
	"math"
)

// StatementBalanceIssue describes a single invariant violation on an Odoo
// bank statement.
//
// Two invariants are enforced:
//
//   - balance_mismatch: balance_start + Σ(line.amount) must equal balance_end_real.
//     If not, Odoo blocks reconciliation with "The running balance doesn't
//     match the specified ending balance".
//   - chain_gap:        statement N's balance_start must equal statement N-1's
//     balance_end_real. A gap means a transaction is missing (or an extra one
//     was declared) between the two statements.
type StatementBalanceIssue struct {
	JournalID       int
	StatementID     int
	StatementName   string
	Date            string
	BalanceStart    float64
	BalanceEndReal  float64
	LineSum         float64
	RunningBalance  float64 // BalanceStart + LineSum
	LineCount       int
	Kind            string  // "balance_mismatch" | "chain_gap"
	PreviousEndReal float64 // for chain_gap only
}

// Diff returns running minus declared; positive means declared is too low.
func (i StatementBalanceIssue) Diff() float64 {
	return i.RunningBalance - i.BalanceEndReal
}

// CheckOdooJournalStatements returns all invariant violations for the given
// journal, in chronological order.
func CheckOdooJournalStatements(creds *OdooCredentials, uid int, journalID int) ([]StatementBalanceIssue, error) {
	stmtResult, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement", "search_read",
		[]interface{}{[]interface{}{
			[]interface{}{"journal_id", "=", journalID},
		}},
		map[string]interface{}{
			"fields": []string{"id", "name", "date", "balance_start", "balance_end_real"},
			"order":  "date asc, id asc",
		})
	if err != nil {
		return nil, fmt.Errorf("fetch statements: %v", err)
	}
	var stmts []struct {
		ID             int     `json:"id"`
		Name           string  `json:"name"`
		Date           string  `json:"date"`
		BalanceStart   float64 `json:"balance_start"`
		BalanceEndReal float64 `json:"balance_end_real"`
	}
	if err := json.Unmarshal(stmtResult, &stmts); err != nil {
		return nil, fmt.Errorf("parse statements: %v", err)
	}
	if len(stmts) == 0 {
		return nil, nil
	}

	// Fetch line sums grouped by statement_id in a single call.
	lineSums, lineCounts, err := odooLineSumsByStatement(creds, uid, journalID)
	if err != nil {
		return nil, err
	}

	var issues []StatementBalanceIssue
	var prevEndReal float64
	var hasPrev bool
	for _, s := range stmts {
		lineSum := lineSums[s.ID]
		lineCount := lineCounts[s.ID]
		running := s.BalanceStart + lineSum

		if math.Abs(running-s.BalanceEndReal) > 0.005 {
			issues = append(issues, StatementBalanceIssue{
				JournalID:      journalID,
				StatementID:    s.ID,
				StatementName:  s.Name,
				Date:           s.Date,
				BalanceStart:   s.BalanceStart,
				BalanceEndReal: s.BalanceEndReal,
				LineSum:        lineSum,
				RunningBalance: running,
				LineCount:      lineCount,
				Kind:           "balance_mismatch",
			})
		}

		if hasPrev && math.Abs(s.BalanceStart-prevEndReal) > 0.005 {
			issues = append(issues, StatementBalanceIssue{
				JournalID:       journalID,
				StatementID:     s.ID,
				StatementName:   s.Name,
				Date:            s.Date,
				BalanceStart:    s.BalanceStart,
				BalanceEndReal:  s.BalanceEndReal,
				LineSum:         lineSum,
				RunningBalance:  running,
				LineCount:       lineCount,
				Kind:            "chain_gap",
				PreviousEndReal: prevEndReal,
			})
		}

		prevEndReal = s.BalanceEndReal
		hasPrev = true
	}

	return issues, nil
}

// odooLineSumsByStatement returns amount sums and counts keyed by statement_id
// for every statement line in the journal (grouped server-side).
func odooLineSumsByStatement(creds *OdooCredentials, uid int, journalID int) (map[int]float64, map[int]int, error) {
	result, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement.line", "read_group",
		[]interface{}{
			[]interface{}{
				[]interface{}{"journal_id", "=", journalID},
				[]interface{}{"statement_id", "!=", false},
			},
			[]string{"statement_id", "amount:sum"},
			[]string{"statement_id"},
		},
		map[string]interface{}{"lazy": false})
	if err != nil {
		return nil, nil, fmt.Errorf("read_group lines: %v", err)
	}
	var groups []struct {
		Statement      []interface{} `json:"statement_id"` // [id, name]
		AmountSum      float64       `json:"amount"`
		StatementCount int           `json:"__count"`
	}
	if err := json.Unmarshal(result, &groups); err != nil {
		return nil, nil, fmt.Errorf("parse read_group: %v", err)
	}
	sums := map[int]float64{}
	counts := map[int]int{}
	for _, g := range groups {
		if len(g.Statement) == 0 {
			continue
		}
		idF, ok := g.Statement[0].(float64)
		if !ok {
			continue
		}
		id := int(idF)
		sums[id] = g.AmountSum
		counts[id] = g.StatementCount
	}
	return sums, counts, nil
}

// FixOdooStatementBalance sets balance_end_real to the supplied running balance
// so the statement's declared ending balance matches the sum of its lines.
func FixOdooStatementBalance(creds *OdooCredentials, uid int, stmtID int, runningBalance float64) error {
	_, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement", "write",
		[]interface{}{[]interface{}{stmtID}, map[string]interface{}{
			"balance_end_real": runningBalance,
		}}, nil)
	return err
}

// PrintStatementIssues writes a human-readable summary of the supplied issues.
func PrintStatementIssues(issues []StatementBalanceIssue) {
	if len(issues) == 0 {
		return
	}
	fmt.Printf("\n  %s⚠ %d invalid statement(s):%s\n\n", Fmt.Yellow, len(issues), Fmt.Reset)
	for _, i := range issues {
		switch i.Kind {
		case "balance_mismatch":
			fmt.Printf("  %s#%d  %s  %s(%s)%s\n", Fmt.Bold, i.StatementID, i.StatementName, Fmt.Dim, i.Date, Fmt.Reset)
			fmt.Printf("    %sbalance_start     %12s%s\n", Fmt.Dim, fmtEUR(i.BalanceStart), Fmt.Reset)
			fmt.Printf("    %ssum of %d line(s) %12s%s\n", Fmt.Dim, i.LineCount, fmtEURSigned(i.LineSum), Fmt.Reset)
			fmt.Printf("    %srunning balance   %12s  %s← should match end%s\n", Fmt.Dim, fmtEUR(i.RunningBalance), Fmt.Green, Fmt.Reset)
			fmt.Printf("    %sbalance_end_real  %12s  %s← off by %s%s\n\n",
				Fmt.Dim, fmtEUR(i.BalanceEndReal), Fmt.Red, fmtEURSigned(i.Diff()), Fmt.Reset)
		case "chain_gap":
			fmt.Printf("  %s#%d  %s  %s(%s) — chain gap%s\n", Fmt.Bold, i.StatementID, i.StatementName, Fmt.Dim, i.Date, Fmt.Reset)
			fmt.Printf("    %sprev end_real     %12s%s\n", Fmt.Dim, fmtEUR(i.PreviousEndReal), Fmt.Reset)
			fmt.Printf("    %sbalance_start     %12s  %s← should equal prev end_real (off by %s)%s\n\n",
				Fmt.Dim, fmtEUR(i.BalanceStart), Fmt.Red,
				fmtEURSigned(i.BalanceStart-i.PreviousEndReal), Fmt.Reset)
		}
	}
}
