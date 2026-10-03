package cmd

// Statement matches: for each bank statement line reconciled against a
// receivable or payable, the invoices and bills it settles. Read-only, part
// of the Odoo pull. Raw archive:
//
//	latest/providers/odoo/<db>/statement-matches.json   {"matches": {"<statement line id>": [move ids]}}
//
// Reconciling does not always bump the statement line's write_date, so the
// journal-lines cache cannot carry this incrementally: it is rebuilt from
// three batched reads (counterpart lines → partial reconciles → the matched
// lines' moves). cmd/transactions_odoo_category.go reads it at generate time.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	odoosource "github.com/CommonsHub/chb/providers/odoo"
)

type StatementMatchesFile struct {
	FetchedAt string           `json:"fetchedAt,omitempty"`
	Matches   map[string][]int `json:"matches"` // statement line id → matched move ids (bills, invoices, entries)
	// Moves: matched move id → its booking lines' amounts per GL account
	// code (product lines only: no tax, receivable or payable lines), so a
	// payment is categorised even when the move is not a cached bill or
	// invoice.
	Moves map[string]map[string]float64 `json:"moves,omitempty"`
}

func statementMatchesPath(dataDir string) string {
	return filepath.Join(dataDir, "latest", odoosource.RelPath("statement-matches.json"))
}

// cachedJournalLines returns every cached statement line of every journal
// in the latest mirror, linked or not.
func cachedJournalLines(dataDir string) []OdooCacheLine {
	dir := filepath.Dir(odoosource.Path(dataDir, "latest", "", "journals", "0.json"))
	files, _ := filepath.Glob(filepath.Join(dir, "*.json"))
	sort.Strings(files)
	var out []OdooCacheLine
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			continue
		}
		var jf OdooJournalLinesFile
		if json.Unmarshal(data, &jf) == nil {
			out = append(out, jf.Lines...)
		}
	}
	return out
}

func isDocumentCounterpart(t string) bool {
	return t == "asset_receivable" || t == "liability_payable"
}

func odooReadChunked(creds *OdooCredentials, uid int, model string, ids []int, fields []string) ([]map[string]interface{}, error) {
	var out []map[string]interface{}
	for start := 0; start < len(ids); start += 500 {
		end := start + 500
		if end > len(ids) {
			end = len(ids)
		}
		// search_read, not read: ids deleted since the cache was taken are
		// skipped instead of failing the batch.
		res, err := odooExec(creds.URL, creds.DB, uid, creds.Password, model, "search_read",
			[]interface{}{[]interface{}{[]interface{}{"id", "in", ids[start:end]}}}, map[string]interface{}{"fields": fields})
		if err != nil {
			return nil, err
		}
		var rows []map[string]interface{}
		if err := json.Unmarshal(res, &rows); err != nil {
			return nil, err
		}
		out = append(out, rows...)
	}
	return out, nil
}

func syncStatementMatches(creds *OdooCredentials, uid int, dataDir string) (int, error) {
	counterpartOf := map[int]OdooCacheLine{} // counterpart move line id → statement line
	var cpIDs []int
	for _, l := range cachedJournalLines(dataDir) {
		if l.CounterpartID > 0 && isDocumentCounterpart(l.CounterpartType) {
			counterpartOf[l.CounterpartID] = l
			cpIDs = append(cpIDs, l.CounterpartID)
		}
	}
	file := StatementMatchesFile{Matches: map[string][]int{}, Moves: map[string]map[string]float64{}}
	if len(cpIDs) > 0 {
		cps, err := odooReadChunked(creds, uid, "account.move.line", cpIDs, []string{"matched_debit_ids", "matched_credit_ids"})
		if err != nil {
			return 0, err
		}
		partialsOf := map[int][]int{}
		var partialIDs []int
		for _, r := range cps {
			ps := append(odooIDList(r["matched_debit_ids"]), odooIDList(r["matched_credit_ids"])...)
			partialsOf[odooInt(r["id"])] = ps
			partialIDs = append(partialIDs, ps...)
		}
		partials, err := odooReadChunked(creds, uid, "account.partial.reconcile", partialIDs, []string{"debit_move_id", "credit_move_id"})
		if err != nil {
			return 0, err
		}
		sides := map[int][2]int{}
		var lineIDs []int
		for _, p := range partials {
			d, c := odooFieldID(p["debit_move_id"]), odooFieldID(p["credit_move_id"])
			sides[odooInt(p["id"])] = [2]int{d, c}
			lineIDs = append(lineIDs, d, c)
		}
		lines, err := odooReadChunked(creds, uid, "account.move.line", lineIDs, []string{"move_id"})
		if err != nil {
			return 0, err
		}
		moveOf := map[int]int{}
		for _, l := range lines {
			moveOf[odooInt(l["id"])] = odooFieldID(l["move_id"])
		}
		for cp, ps := range partialsOf {
			st := counterpartOf[cp]
			seen := map[int]bool{}
			var moves []int
			for _, p := range ps {
				for _, lid := range sides[p] {
					if lid == cp {
						continue
					}
					if m := moveOf[lid]; m > 0 && m != st.MoveID && !seen[m] {
						seen[m] = true
						moves = append(moves, m)
					}
				}
			}
			if len(moves) > 0 {
				sort.Ints(moves)
				file.Matches[strconv.Itoa(st.ID)] = moves
			}
		}
		if err := fetchMatchedMoveAccounts(creds, uid, &file); err != nil {
			return 0, err
		}
	}
	err := writeIfChanged(statementMatchesPath(dataDir), &file,
		func(v interface{}) { v.(*StatementMatchesFile).FetchedAt = "" }, &StatementMatchesFile{},
		func() { file.FetchedAt = time.Now().UTC().Format(time.RFC3339) })
	return len(file.Matches), err
}

func loadStatementMatches(dataDir string) map[int][]int {
	out := map[int][]int{}
	data, err := os.ReadFile(statementMatchesPath(dataDir))
	if err != nil {
		return out
	}
	var f StatementMatchesFile
	if json.Unmarshal(data, &f) != nil {
		return out
	}
	for k, v := range f.Matches {
		if id, err := strconv.Atoi(strings.TrimSpace(k)); err == nil {
			out[id] = v
		}
	}
	return out
}

// OdooStatementMatchesSync is the pull step (`chb odoo pull`, `chb pull`).
func OdooStatementMatchesSync(args []string) (string, error) {
	creds, err := ResolveOdooCredentials()
	if err != nil {
		return "", err
	}
	uid, err := odooAuth(creds.URL, creds.DB, creds.Login, creds.Password)
	if err != nil || uid == 0 {
		return "", fmt.Errorf("Odoo authentication failed: %v", err)
	}
	n, err := syncStatementMatches(creds, uid, DataDir())
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s matched to invoices or bills", Pluralize(n, "bank line", "")), nil
}

// fetchMatchedMoveAccounts fills file.Moves with the product lines of every
// matched move, summed per GL account code.
func fetchMatchedMoveAccounts(creds *OdooCredentials, uid int, file *StatementMatchesFile) error {
	seen := map[int]bool{}
	var ids []int
	for _, ms := range file.Matches {
		for _, m := range ms {
			if !seen[m] {
				seen[m] = true
				ids = append(ids, m)
			}
		}
	}
	sort.Ints(ids)
	for start := 0; start < len(ids); start += 500 {
		end := start + 500
		if end > len(ids) {
			end = len(ids)
		}
		domain := []interface{}{
			[]interface{}{"move_id", "in", ids[start:end]},
			[]interface{}{"display_type", "in", []interface{}{"product", false}},
			[]interface{}{"account_id.account_type", "not in", []interface{}{"asset_receivable", "liability_payable"}},
		}
		res, err := odooExec(creds.URL, creds.DB, uid, creds.Password, "account.move.line", "search_read",
			[]interface{}{domain}, map[string]interface{}{"fields": []string{"move_id", "account_id", "balance"}})
		if err != nil {
			return err
		}
		var rows []map[string]interface{}
		if err := json.Unmarshal(res, &rows); err != nil {
			return err
		}
		for _, r := range rows {
			code := odooAccountCodeFromField(r["account_id"])
			if code == "" {
				continue
			}
			key := strconv.Itoa(odooFieldID(r["move_id"]))
			if file.Moves[key] == nil {
				file.Moves[key] = map[string]float64{}
			}
			amt := odooFloat(r["balance"])
			if amt < 0 {
				amt = -amt
			}
			file.Moves[key][code] = roundCents(file.Moves[key][code] + amt)
		}
	}
	return nil
}

// odooAccountCodeFromField: the code from a many2one [id, "613105 Fees"].
func odooAccountCodeFromField(v interface{}) string {
	if xs, ok := v.([]interface{}); ok && len(xs) > 1 {
		if name, ok := xs[1].(string); ok {
			if f := strings.Fields(name); len(f) > 0 {
				return f[0]
			}
		}
	}
	return ""
}

func loadStatementMatchedMoves(dataDir string) map[int]map[string]float64 {
	out := map[int]map[string]float64{}
	data, err := os.ReadFile(statementMatchesPath(dataDir))
	if err != nil {
		return out
	}
	var f StatementMatchesFile
	if json.Unmarshal(data, &f) != nil {
		return out
	}
	for k, v := range f.Moves {
		if id, err := strconv.Atoi(k); err == nil {
			out[id] = v
		}
	}
	return out
}
