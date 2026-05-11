package cmd

import (
	"encoding/json"
	"fmt"
	"math"
)

// odooStr tolerates Odoo's habit of returning `false` for unset string fields.
type odooStr string

func (s *odooStr) UnmarshalJSON(b []byte) error {
	if len(b) == 0 || string(b) == "false" || string(b) == "null" {
		*s = ""
		return nil
	}
	var v string
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	*s = odooStr(v)
	return nil
}

// StatementBalanceIssue describes a single invariant violation on an Odoo
// bank statement.
//
// Three invariants are enforced:
//
//   - balance_mismatch: balance_start + Σ(line.amount) must equal balance_end_real.
//     If not, Odoo blocks reconciliation with "The running balance doesn't
//     match the specified ending balance".
//   - chain_gap:        statement N's balance_start must equal statement N-1's
//     balance_end_real. A gap means a transaction is missing (or an extra one
//     was declared) between the two statements.
//   - duplicate_open_fee_lines: a statement must have at most one rolling
//     "Stripe fees for open statement" line. Earlier sync revisions used a
//     non-stable unique_import_id that produced a fresh duplicate on every
//     run; `chb odoo journals <id> fix` collapses them.
type StatementBalanceIssue struct {
	JournalID        int
	StatementID      int
	StatementName    string
	Date             string
	BalanceStart     float64
	BalanceEndReal   float64
	LineSum          float64
	RunningBalance   float64 // BalanceStart + LineSum
	LineCount        int
	Kind             string  // "balance_mismatch" | "chain_gap" | "duplicate_open_fee_lines"
	StatementRef     string  // Odoo statement reference field (Stripe payout ID for Stripe journals)
	PreviousEndReal  float64 // for chain_gap only
	PreviousStmtID   int     // for chain_gap only
	PreviousStmtName string  // for chain_gap only
	PreviousStmtDate string  // for chain_gap only
	PreviousStmtRef  string  // for chain_gap only
	DuplicateLines   []DuplicateFeeLine // for duplicate_open_fee_lines only
}

// DuplicateFeeLine identifies one of the duplicated open-statement fee lines.
type DuplicateFeeLine struct {
	ID       int
	Amount   float64
	Date     string
	ImportID string
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
			"fields": []string{"id", "name", "reference", "date", "balance_start", "balance_end_real"},
			"order":  "date asc, id asc",
		})
	if err != nil {
		return nil, fmt.Errorf("fetch statements: %v", err)
	}
	var stmts []struct {
		ID             int     `json:"id"`
		Name           odooStr `json:"name"`
		Reference      odooStr `json:"reference"`
		Date           odooStr `json:"date"`
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
	var prevID int
	var prevName, prevDate, prevRef string
	var hasPrev bool
	for _, s := range stmts {
		lineSum := lineSums[s.ID]
		lineCount := lineCounts[s.ID]
		running := s.BalanceStart + lineSum

		if math.Abs(running-s.BalanceEndReal) > 0.005 {
			issues = append(issues, StatementBalanceIssue{
				JournalID:      journalID,
				StatementID:    s.ID,
				StatementName:  string(s.Name),
				StatementRef:   string(s.Reference),
				Date:           string(s.Date),
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
				JournalID:        journalID,
				StatementID:      s.ID,
				StatementName:    string(s.Name),
				StatementRef:     string(s.Reference),
				Date:             string(s.Date),
				BalanceStart:     s.BalanceStart,
				BalanceEndReal:   s.BalanceEndReal,
				LineSum:          lineSum,
				RunningBalance:   running,
				LineCount:        lineCount,
				Kind:             "chain_gap",
				PreviousEndReal:  prevEndReal,
				PreviousStmtID:   prevID,
				PreviousStmtName: prevName,
				PreviousStmtDate: prevDate,
				PreviousStmtRef:  prevRef,
			})
		}

		prevEndReal = s.BalanceEndReal
		prevID = s.ID
		prevName = string(s.Name)
		prevDate = string(s.Date)
		prevRef = string(s.Reference)
		hasPrev = true
	}

	dupes, err := findDuplicateOpenFeeLines(creds, uid, journalID)
	if err == nil {
		stmtMeta := map[int]struct {
			Name, Date, Reference string
			BalanceStart          float64
			BalanceEndReal        float64
		}{}
		for _, s := range stmts {
			stmtMeta[s.ID] = struct {
				Name, Date, Reference string
				BalanceStart          float64
				BalanceEndReal        float64
			}{string(s.Name), string(s.Date), string(s.Reference), s.BalanceStart, s.BalanceEndReal}
		}
		for stmtID, lines := range dupes {
			meta := stmtMeta[stmtID]
			issues = append(issues, StatementBalanceIssue{
				JournalID:      journalID,
				StatementID:    stmtID,
				StatementName:  meta.Name,
				StatementRef:   meta.Reference,
				Date:           meta.Date,
				BalanceStart:   meta.BalanceStart,
				BalanceEndReal: meta.BalanceEndReal,
				LineSum:        lineSums[stmtID],
				RunningBalance: meta.BalanceStart + lineSums[stmtID],
				LineCount:      lineCounts[stmtID],
				Kind:           "duplicate_open_fee_lines",
				DuplicateLines: lines,
			})
		}
	}

	return issues, nil
}

// findDuplicateOpenFeeLines returns, per statement, the set of "Stripe fees
// for open statement" lines when more than one exists. The pattern matches
// both the current stable importID format (stripe:<acc>:open:<stmtID>:fees)
// and the legacy buggy format (stripe:<acc>:open:<bt1>:<bt2>:fees) so the
// fix command can collapse old duplicates left behind by prior syncs.
func findDuplicateOpenFeeLines(creds *OdooCredentials, uid int, journalID int) (map[int][]DuplicateFeeLine, error) {
	result, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement.line", "search_read",
		[]interface{}{[]interface{}{
			[]interface{}{"journal_id", "=", journalID},
			[]interface{}{"unique_import_id", "=ilike", "stripe:%:open:%:fees"},
		}},
		map[string]interface{}{
			"fields": []string{"id", "amount", "date", "unique_import_id", "statement_id"},
			"order":  "date asc, id asc",
		})
	if err != nil {
		return nil, err
	}
	var rows []struct {
		ID             int           `json:"id"`
		Amount         float64       `json:"amount"`
		Date           odooStr       `json:"date"`
		UniqueImportID odooStr       `json:"unique_import_id"`
		Statement      []interface{} `json:"statement_id"` // [id, name] or false
	}
	if err := json.Unmarshal(result, &rows); err != nil {
		return nil, err
	}
	byStmt := map[int][]DuplicateFeeLine{}
	for _, r := range rows {
		if len(r.Statement) == 0 {
			continue
		}
		idF, ok := r.Statement[0].(float64)
		if !ok {
			continue
		}
		stmtID := int(idF)
		byStmt[stmtID] = append(byStmt[stmtID], DuplicateFeeLine{
			ID:       r.ID,
			Amount:   r.Amount,
			Date:     string(r.Date),
			ImportID: string(r.UniqueImportID),
		})
	}
	for stmtID, lines := range byStmt {
		if len(lines) < 2 {
			delete(byStmt, stmtID)
		}
	}
	return byStmt, nil
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

// PrintStatementIssues writes a human-readable summary of the supplied issues.
func PrintStatementIssues(issues []StatementBalanceIssue) {
	if len(issues) == 0 {
		return
	}
	Warnf("  %s⚠ %d invalid statement(s):%s", Fmt.Yellow, len(issues), Fmt.Reset)
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
			prevLine := fmt.Sprintf("  %sprev statement    #%d  %s  (%s)", Fmt.Dim, i.PreviousStmtID, i.PreviousStmtName, i.PreviousStmtDate)
			if i.PreviousStmtRef != "" {
				prevLine += "  ref=" + i.PreviousStmtRef
			}
			fmt.Printf("  %s%s\n", prevLine, Fmt.Reset)
			fmt.Printf("    %sprev end_real     %12s%s\n", Fmt.Dim, fmtEUR(i.PreviousEndReal), Fmt.Reset)
			fmt.Printf("    %sbalance_start     %12s  %s← should equal prev end_real (off by %s)%s\n\n",
				Fmt.Dim, fmtEUR(i.BalanceStart), Fmt.Red,
				fmtEURSigned(i.BalanceStart-i.PreviousEndReal), Fmt.Reset)
		case "duplicate_open_fee_lines":
			fmt.Printf("  %s#%d  %s  %s(%s) — %d duplicate \"Stripe fees for open statement\" line(s)%s\n",
				Fmt.Bold, i.StatementID, i.StatementName, Fmt.Dim, i.Date, len(i.DuplicateLines), Fmt.Reset)
			var sum float64
			for _, d := range i.DuplicateLines {
				fmt.Printf("    %sline #%-7d  %12s  %s  %s%s\n",
					Fmt.Dim, d.ID, fmtEURSigned(d.Amount), d.Date, d.ImportID, Fmt.Reset)
				sum += d.Amount
			}
			fmt.Printf("    %s→ would collapse to a single line of %s%s\n\n",
				Fmt.Dim, fmtEURSigned(sum), Fmt.Reset)
		}
	}
}
