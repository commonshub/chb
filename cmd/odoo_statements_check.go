package cmd

import (
	"encoding/json"
	"fmt"
	"math"
	"time"
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

// odooJSONFloat tolerates Odoo's habit of returning `false` for unset numeric
// fields. Statement repair must not abort just because an old/incomplete
// account.bank.statement has nullable balances represented as false.
type odooJSONFloat float64

func (f *odooJSONFloat) UnmarshalJSON(b []byte) error {
	if len(b) == 0 || string(b) == "false" || string(b) == "null" {
		*f = 0
		return nil
	}
	var v float64
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	*f = odooJSONFloat(v)
	return nil
}

func (f odooJSONFloat) Float64() float64 { return float64(f) }

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
	JournalID      int
	StatementID    int
	StatementName  string
	Date           string
	BalanceStart   float64
	BalanceEndReal float64
	LineSum        float64
	RunningBalance float64 // BalanceStart + LineSum
	LineCount      int
	Kind           string // "balance_mismatch" | "chain_gap" | "duplicate_open_fee_lines" | "opening_in_line" | "odoo_running_mismatch" | "unstatemented_lines"
	StatementRef   string // Odoo statement reference field (Stripe payout ID for Stripe journals)
	// OdooBalanceEnd is Odoo's own computed running balance for the statement.
	// Odoo drops the journal's chronologically-first line from it (treating it
	// as a zero-impact anchor), so when the opening balance is carried as that
	// first line, OdooBalanceEnd disagrees with BalanceEndReal even though
	// chb's own RunningBalance matches. Set for opening_in_line / odoo_running_mismatch.
	OdooBalanceEnd        float64
	SuggestedBalanceStart float64            // opening_in_line: the balance_start that makes Odoo valid
	OrphanFirstDate       string             // unstatemented_lines: earliest line date
	OrphanLastDate        string             // unstatemented_lines: latest line date
	PreviousEndReal       float64            // for chain_gap only
	PreviousStmtID        int                // for chain_gap only
	PreviousStmtName      string             // for chain_gap only
	PreviousStmtDate      string             // for chain_gap only
	PreviousStmtRef       string             // for chain_gap only
	DuplicateLines        []DuplicateFeeLine // for duplicate_open_fee_lines only
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

// printJournalBalances shows the three figures an operator compares to trust a
// journal: the provider's live balance, the local archive, and the Odoo journal
// itself (a live read_group). For a cutoff journal (odooSyncSince) it also
// compares the post-cutoff transaction count — the journal carries an opening
// balance that local has no tx for, so only lines on/after the cutoff should
// match one-for-one. acc may be nil (an unlinked journal): then only the Odoo
// figure is shown.
func printJournalBalances(creds *OdooCredentials, uid int, acc *AccountConfig, journalID int) {
	currency := "EUR"
	localCount, localBalance, haveLocal := 0, 0.0, false
	providerBalance, providerKnown := 0.0, false

	if acc != nil {
		currency = accCurrency(acc)
		if t := computeAccountTotals(acc); t != nil {
			localCount, localBalance, haveLocal = t.TxCount, t.CurrentBalance, true
			if t.Currency != "" {
				currency = t.Currency
			}
		}
		if cache := loadBalanceCache(); cache != nil {
			if v, _, ok := resolveAccountBalance(acc, cache.Balances, nil); ok {
				providerBalance, providerKnown = v, true
			}
		}
	}
	journalCount, journalBalance, jerr := odooJournalAggregate(creds, uid, journalID)

	mark := func(ok bool) string {
		if ok {
			return Fmt.Green + "  ✓" + Fmt.Reset
		}
		return Fmt.Red + "  ✗" + Fmt.Reset
	}
	rowFmt := func(label string, bal float64, count int, m, count_ string) {
		fmt.Printf("    %-20s %16s%s%s\n", label, formatBalance(bal, currency), m,
			func() string {
				if count_ == "" {
					return ""
				}
				return "  " + Fmt.Dim + count_ + Fmt.Reset
			}())
	}

	fmt.Printf("\n  %sBalances%s\n", Fmt.Bold, Fmt.Reset)
	if providerKnown {
		rowFmt(accountProviderDisplayName(acc)+" (live):", providerBalance, 0, "  ", Pluralize(accountProviderRawCount(acc), "tx", ""))
	}
	if haveLocal {
		m := "  "
		if providerKnown {
			m = mark(roundCents(localBalance) == roundCents(providerBalance))
		}
		rowFmt("Local files:", localBalance, localCount, m, Pluralize(localCount, "tx", ""))
	}
	if jerr != nil {
		fmt.Printf("    %-20s %s(unavailable: %v)%s\n", fmt.Sprintf("Odoo journal #%d:", journalID), Fmt.Dim, jerr, Fmt.Reset)
	} else {
		m := "  "
		if haveLocal {
			m = mark(roundCents(journalBalance) == roundCents(localBalance))
		}
		rowFmt(fmt.Sprintf("Odoo journal #%d:", journalID), journalBalance, journalCount, m, Pluralize(journalCount, "tx", ""))
		// Cutoff journals: the journal keeps an opening balance line so total
		// counts differ by design; lines on/after the cutoff must match local.
		if acc != nil {
			if cutoff, ok := acc.OdooSyncSinceTime(); ok {
				localPost := accountLocalOdooSyncSnapshotSince(acc, cutoff).TxCount
				odooPost, perr := odooJournalImportedCountSince(creds, uid, journalID, cutoff)
				if perr == nil {
					cm := mark(localPost == odooPost)
					fmt.Printf("    %ssince %s%s   %d local vs %d journal%s\n",
						Fmt.Dim, cutoff.Format("2006-01-02"), Fmt.Reset, localPost, odooPost, cm)
				}
			}
		}
	}
	fmt.Println()
}

// odooJournalImportedCountSince counts the chb-owned statement lines (those
// carrying a unique_import_id, i.e. excluding the manual opening balance) dated
// on or after the cutoff. This is the count that must match local for a cutoff
// journal.
func odooJournalImportedCountSince(creds *OdooCredentials, uid int, journalID int, cutoff time.Time) (int, error) {
	res, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement.line", "search_count",
		[]interface{}{[]interface{}{
			[]interface{}{"journal_id", "=", journalID},
			[]interface{}{"date", ">=", cutoff.Format("2006-01-02")},
			[]interface{}{"unique_import_id", "!=", false},
		}}, nil)
	if err != nil {
		return 0, err
	}
	var count int
	if err := json.Unmarshal(res, &count); err != nil {
		return 0, err
	}
	return count, nil
}

// classifyStatementBalance decides whether one statement's balances are
// invalid, and how. idx is the statement's chronological position (0 = first).
//
//   - balance_mismatch: chb's own running (balanceStart + every line) ≠ declared
//     end — a genuine line-level defect.
//   - opening_in_line / odoo_running_mismatch: chb's running matches, but Odoo's
//     own running balance (odooBalanceEnd, which drops the journal's first line)
//     does not. On the first statement that dropped line is the opening balance
//     carried as a line; the opening belongs in balance_start instead. The
//     suggested balance_start that makes Odoo's running tie out is
//     balanceEndReal − odooBalanceEnd + balanceStart.
//
// Returns kind == "" when the statement is valid.
//
// Odoo's own running balance (odooBalanceEnd) is the source of truth for
// validity — it is what `journal_has_invalid_statements` is computed from. chb
// must NOT recompute its own `balanceStart + Σlines` as the gate: once an
// opening balance is moved into balance_start (the opening_in_line repair) the
// opening line is counted twice by that formula but exactly once by Odoo, so a
// recompute would report a phantom mismatch on a journal Odoo considers clean.
// chb's own running is used only to classify WHY Odoo rejects a statement.
func classifyStatementBalance(idx int, balanceStart, balanceEndReal, odooBalanceEnd, lineSum float64) (kind string, suggestedStart float64) {
	if math.Abs(odooBalanceEnd-balanceEndReal) <= 0.005 {
		return "", 0 // Odoo accepts it — trust that.
	}
	// Odoo rejects it. If chb's full-line running DOES match the declared end,
	// Odoo dropped a line chb counted — on the first statement that's the
	// opening balance carried as a line, which belongs in balance_start.
	if math.Abs((balanceStart+lineSum)-balanceEndReal) <= 0.005 {
		if idx == 0 {
			return "opening_in_line", roundCents(balanceEndReal - odooBalanceEnd + balanceStart)
		}
		return "odoo_running_mismatch", 0
	}
	// Both chb and Odoo disagree with the declared end → genuine line defect.
	return "balance_mismatch", 0
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
			"fields": []string{"id", "name", "reference", "date", "balance_start", "balance_end_real", "balance_end"},
			"order":  "date asc, id asc",
		})
	if err != nil {
		return nil, fmt.Errorf("fetch statements: %v", err)
	}
	var stmts []struct {
		ID             int           `json:"id"`
		Name           odooStr       `json:"name"`
		Reference      odooStr       `json:"reference"`
		Date           odooStr       `json:"date"`
		BalanceStart   odooJSONFloat `json:"balance_start"`
		BalanceEndReal odooJSONFloat `json:"balance_end_real"`
		BalanceEnd     odooJSONFloat `json:"balance_end"`
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
	for idx, s := range stmts {
		balanceStart := s.BalanceStart.Float64()
		balanceEndReal := s.BalanceEndReal.Float64()
		balanceEnd := s.BalanceEnd.Float64()
		lineSum := lineSums[s.ID]
		lineCount := lineCounts[s.ID]
		running := balanceStart + lineSum

		if kind, suggested := classifyStatementBalance(idx, balanceStart, balanceEndReal, balanceEnd, lineSum); kind != "" {
			issues = append(issues, StatementBalanceIssue{
				JournalID:             journalID,
				StatementID:           s.ID,
				StatementName:         string(s.Name),
				StatementRef:          string(s.Reference),
				Date:                  string(s.Date),
				BalanceStart:          balanceStart,
				BalanceEndReal:        balanceEndReal,
				OdooBalanceEnd:        balanceEnd,
				LineSum:               lineSum,
				RunningBalance:        running,
				LineCount:             lineCount,
				Kind:                  kind,
				SuggestedBalanceStart: suggested,
			})
		}

		if hasPrev && math.Abs(balanceStart-prevEndReal) > 0.005 {
			issues = append(issues, StatementBalanceIssue{
				JournalID:        journalID,
				StatementID:      s.ID,
				StatementName:    string(s.Name),
				StatementRef:     string(s.Reference),
				Date:             string(s.Date),
				BalanceStart:     balanceStart,
				BalanceEndReal:   balanceEndReal,
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

		prevEndReal = balanceEndReal
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
			}{string(s.Name), string(s.Date), string(s.Reference), s.BalanceStart.Float64(), s.BalanceEndReal.Float64()}
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

	// Lines that belong to no statement at all. Odoo's reconciliation report
	// leaves them outside the statement chain, so the chain's last end no
	// longer reflects the journal's real balance (e.g. journal 47's trailing
	// drain-to-zero lines). One summary issue keeps the noise down.
	if oc, osum, first, last, oerr := findUnstatementedLines(creds, uid, journalID); oerr == nil && oc > 0 {
		issues = append(issues, StatementBalanceIssue{
			JournalID:       journalID,
			Kind:            "unstatemented_lines",
			LineCount:       oc,
			LineSum:         osum,
			OrphanFirstDate: first,
			OrphanLastDate:  last,
		})
	}

	return issues, nil
}

// findUnstatementedLines returns the count, signed sum, and date range of
// journal lines that are not attached to any statement. One aggregate
// read_group + two cheap limit-1 reads, so it scales regardless of how many
// lines are loose.
func findUnstatementedLines(creds *OdooCredentials, uid int, journalID int) (count int, sum float64, firstDate, lastDate string, err error) {
	domain := []interface{}{
		[]interface{}{"journal_id", "=", journalID},
		[]interface{}{"statement_id", "=", false},
	}
	grpRes, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement.line", "read_group",
		[]interface{}{domain, []string{"amount:sum"}, []interface{}{}},
		map[string]interface{}{"lazy": false})
	if err != nil {
		return 0, 0, "", "", err
	}
	var grp []struct {
		Amount odooJSONFloat `json:"amount"`
		Count  int           `json:"__count"`
	}
	if err := json.Unmarshal(grpRes, &grp); err != nil {
		return 0, 0, "", "", err
	}
	if len(grp) == 0 || grp[0].Count == 0 {
		return 0, 0, "", "", nil
	}
	count = grp[0].Count
	sum = roundCents(grp[0].Amount.Float64())
	firstDate = odooFirstLineDate(creds, uid, domain, "date asc, id asc")
	lastDate = odooFirstLineDate(creds, uid, domain, "date desc, id desc")
	return count, sum, firstDate, lastDate, nil
}

// odooFirstLineDate returns the date of the first statement line matching the
// domain under the given order (empty string on error / no row).
func odooFirstLineDate(creds *OdooCredentials, uid int, domain []interface{}, order string) string {
	res, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement.line", "search_read",
		[]interface{}{domain},
		map[string]interface{}{"fields": []string{"date"}, "order": order, "limit": 1})
	if err != nil {
		return ""
	}
	var rows []struct {
		Date odooStr `json:"date"`
	}
	if json.Unmarshal(res, &rows) != nil || len(rows) == 0 {
		return ""
	}
	return string(rows[0].Date)
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
		Amount         odooJSONFloat `json:"amount"`
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
			Amount:   r.Amount.Float64(),
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
		AmountSum      odooJSONFloat `json:"amount"`
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
		sums[id] = g.AmountSum.Float64()
		counts[id] = g.StatementCount
	}
	return sums, counts, nil
}

// PrintStatementIssues writes a human-readable summary of the supplied issues.
func PrintStatementIssues(issues []StatementBalanceIssue) {
	if len(issues) == 0 {
		return
	}
	Warnf("  %s⚠ %s:%s", Fmt.Yellow, Pluralize(len(issues), "invalid statement", ""), Fmt.Reset)
	for _, i := range issues {
		switch i.Kind {
		case "balance_mismatch":
			fmt.Printf("  %s#%d  %s  %s(%s)%s\n", Fmt.Bold, i.StatementID, i.StatementName, Fmt.Dim, i.Date, Fmt.Reset)
			fmt.Printf("    %sbalance_start     %12s%s\n", Fmt.Dim, fmtEUR(i.BalanceStart), Fmt.Reset)
			fmt.Printf("    %ssum of %-10s %12s%s\n", Fmt.Dim, Pluralize(i.LineCount, "line", ""), fmtEURSigned(i.LineSum), Fmt.Reset)
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
			fmt.Printf("  %s#%d  %s  %s(%s) — %s of \"Stripe fees for open statement\"%s\n",
				Fmt.Bold, i.StatementID, i.StatementName, Fmt.Dim, i.Date, Pluralize(len(i.DuplicateLines), "duplicate line", ""), Fmt.Reset)
			var sum float64
			for _, d := range i.DuplicateLines {
				fmt.Printf("    %sline #%-7d  %12s  %s  %s%s\n",
					Fmt.Dim, d.ID, fmtEURSigned(d.Amount), d.Date, d.ImportID, Fmt.Reset)
				sum += d.Amount
			}
			fmt.Printf("    %s→ would collapse to a single line of %s%s\n\n",
				Fmt.Dim, fmtEURSigned(sum), Fmt.Reset)
		case "opening_in_line":
			fmt.Printf("  %s#%d  %s  %s(%s) — opening balance carried as a line%s\n",
				Fmt.Bold, i.StatementID, i.StatementName, Fmt.Dim, i.Date, Fmt.Reset)
			fmt.Printf("    %sbalance_start     %12s  %s← Odoo drops the journal's first line; opening belongs here%s\n",
				Fmt.Dim, fmtEUR(i.BalanceStart), Fmt.Yellow, Fmt.Reset)
			fmt.Printf("    %sOdoo balance_end  %12s%s\n", Fmt.Dim, fmtEURSigned(i.OdooBalanceEnd), Fmt.Reset)
			fmt.Printf("    %sbalance_end_real  %12s  %s← off by %s%s\n",
				Fmt.Dim, fmtEUR(i.BalanceEndReal), Fmt.Red, fmtEURSigned(i.BalanceEndReal-i.OdooBalanceEnd), Fmt.Reset)
			fmt.Printf("    %s→ fix sets balance_start %s → %s%s\n\n",
				Fmt.Green, fmtEUR(i.BalanceStart), fmtEUR(i.SuggestedBalanceStart), Fmt.Reset)
		case "odoo_running_mismatch":
			fmt.Printf("  %s#%d  %s  %s(%s) — Odoo running balance disagrees%s\n",
				Fmt.Bold, i.StatementID, i.StatementName, Fmt.Dim, i.Date, Fmt.Reset)
			fmt.Printf("    %sOdoo balance_end  %12s%s\n", Fmt.Dim, fmtEURSigned(i.OdooBalanceEnd), Fmt.Reset)
			fmt.Printf("    %sbalance_end_real  %12s  %s← off by %s%s\n\n",
				Fmt.Dim, fmtEUR(i.BalanceEndReal), Fmt.Red, fmtEURSigned(i.BalanceEndReal-i.OdooBalanceEnd), Fmt.Reset)
		case "unstatemented_lines":
			rng := ""
			if i.OrphanFirstDate != "" {
				rng = fmt.Sprintf("  %s(%s → %s)%s", Fmt.Dim, i.OrphanFirstDate, i.OrphanLastDate, Fmt.Reset)
			}
			fmt.Printf("  %s%s in no statement%s%s\n",
				Fmt.Bold, Pluralize(i.LineCount, "line", ""), Fmt.Reset, rng)
			fmt.Printf("    %ssum %s  %s← chain end no longer reflects the journal balance%s\n\n",
				Fmt.Dim, fmtEURSigned(i.LineSum), Fmt.Yellow, Fmt.Reset)
		}
	}
}
