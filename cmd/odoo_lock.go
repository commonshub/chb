package cmd

// Lock-date guard: chb never creates, modifies, deletes, posts, resets or
// reconciles accounting entries dated on or before the lock date. Closed
// periods belong to the accountant.
//
// The lock date is the latest of
//   - Odoo's company lock dates (fiscalyear_lock_date, hard_lock_date) —
//     Odoo enforces these itself, but chb refuses earlier and says why;
//   - settings.json "odoo": {"lockDate": "YYYY-MM-DD"} — a chb-side lock,
//     for when the books are closed but Odoo is not locked yet.
//
// Every mutating call goes through odooExec, which calls checkOdooLock.
// Matching (reconcile, unreconcile) is refused only when every line
// involved is locked: paying a closed year's bill in an open year is
// normal and changes no closed balance.

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"

	odoosource "github.com/CommonsHub/chb/providers/odoo"
)

// ErrOdooPeriodLocked is wrapped by every refusal.
var ErrOdooPeriodLocked = errors.New("period locked")

type odooLockInfo struct {
	Date   string // YYYY-MM-DD, "" = no lock
	Source string // "Odoo" or "chb settings"
}

var (
	odooLockMu    sync.Mutex
	odooLockCache = map[string]odooLockInfo{}
	// odooLockFetch is swapped in tests.
	odooLockFetch = fetchOdooLockDate
	// odooLockRead reads one field set for ids (swapped in tests).
	odooLockRead = func(url, db string, uid int, pw, model string, ids []int, fields []string) ([]map[string]interface{}, error) {
		res, err := odoosource.Exec(url, db, uid, pw, model, "read", []interface{}{ids}, map[string]interface{}{"fields": fields})
		if err != nil {
			return nil, err
		}
		var rows []map[string]interface{}
		if err := json.Unmarshal(res, &rows); err != nil {
			return nil, err
		}
		return rows, nil
	}
	odooLockCount = func(url, db string, uid int, pw, model string, domain []interface{}) (int, error) {
		res, err := odoosource.Exec(url, db, uid, pw, model, "search_count", []interface{}{domain}, nil)
		if err != nil {
			return 0, err
		}
		var n int
		err = json.Unmarshal(res, &n)
		return n, err
	}
)

func settingsLockDate() string {
	data, err := os.ReadFile(settingsFilePath("settings.json"))
	if err != nil {
		return ""
	}
	var s struct {
		Odoo struct {
			LockDate string `json:"lockDate"`
		} `json:"odoo"`
	}
	if json.Unmarshal(data, &s) != nil || !isISODate(s.Odoo.LockDate) {
		return ""
	}
	return s.Odoo.LockDate
}

// fetchOdooLockDate: the latest lock date over the companies the user can
// read. Only fields this Odoo version has are requested.
func fetchOdooLockDate(url, db string, uid int, pw string) (string, error) {
	res, err := odoosource.Exec(url, db, uid, pw, "res.company", "fields_get", []interface{}{}, map[string]interface{}{"attributes": []string{"type"}})
	if err != nil {
		return "", err
	}
	var defs map[string]interface{}
	if err := json.Unmarshal(res, &defs); err != nil {
		return "", err
	}
	var fields []string
	for _, f := range []string{"fiscalyear_lock_date", "hard_lock_date"} {
		if _, ok := defs[f]; ok {
			fields = append(fields, f)
		}
	}
	if len(fields) == 0 {
		return "", nil
	}
	res, err = odoosource.Exec(url, db, uid, pw, "res.company", "search_read", []interface{}{[]interface{}{}}, map[string]interface{}{"fields": fields})
	if err != nil {
		return "", err
	}
	var rows []map[string]interface{}
	if err := json.Unmarshal(res, &rows); err != nil {
		return "", err
	}
	lock := ""
	for _, r := range rows {
		for _, f := range fields {
			if d, ok := r[f].(string); ok && isISODate(d) && d > lock {
				lock = d
			}
		}
	}
	return lock, nil
}

// odooLockDate returns the effective lock date for this Odoo database,
// fetched once per process.
func odooLockDate(url, db string, uid int, pw string) odooLockInfo {
	key := url + "|" + db
	odooLockMu.Lock()
	defer odooLockMu.Unlock()
	if l, ok := odooLockCache[key]; ok {
		return l
	}
	l := odooLockInfo{}
	if d, err := odooLockFetch(url, db, uid, pw); err != nil {
		fmt.Fprintf(os.Stderr, "  %s⚠ could not read Odoo's lock date (%v); Odoo still enforces it%s\n", Fmt.Yellow, err, Fmt.Reset)
	} else if d != "" {
		l = odooLockInfo{Date: d, Source: "Odoo"}
	}
	if d := settingsLockDate(); d > l.Date {
		l = odooLockInfo{Date: d, Source: "chb settings"}
	}
	odooLockCache[key] = l
	return l
}

func (l odooLockInfo) String() string {
	if l.Date == "" {
		return "no lock date"
	}
	return fmt.Sprintf("locked through %s (%s)", l.Date, l.Source)
}

func lockedErr(l odooLockInfo, what string) error {
	return fmt.Errorf("%w: %s — the books are %s; chb does not change closed periods", ErrOdooPeriodLocked, what, l)
}

// recordDateFields: the date that places a record in a period.
var recordDateFields = map[string][]string{
	"account.move":                {"date"},
	"account.move.line":           {"date"},
	"account.bank.statement.line": {"date"},
}

func lockIDs(v interface{}) []int {
	var ids []int
	switch x := v.(type) {
	case int:
		ids = append(ids, x)
	case int64:
		ids = append(ids, int(x))
	case float64:
		ids = append(ids, int(x))
	case []int:
		ids = append(ids, x...)
	case []interface{}:
		for _, e := range x {
			ids = append(ids, lockIDs(e)...)
		}
	}
	return ids
}

func lockVals(v interface{}) []map[string]interface{} {
	switch x := v.(type) {
	case map[string]interface{}:
		return []map[string]interface{}{x}
	case []map[string]interface{}:
		return x
	case []interface{}:
		var out []map[string]interface{}
		for _, e := range x {
			out = append(out, lockVals(e)...)
		}
		return out
	}
	return nil
}

// valsDates: dates set by create/write values.
func valsDates(vals []map[string]interface{}) []string {
	var out []string
	for _, v := range vals {
		for _, f := range []string{"date", "invoice_date"} {
			if d, ok := v[f].(string); ok && isISODate(d) {
				out = append(out, d)
			}
		}
	}
	return out
}

// lockRecordDates reads the period date of each record. For statements,
// the dates of their lines; for partial reconciles, of both matched lines.
func lockRecordDates(url, db string, uid int, pw, model string, ids []int) ([]string, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	switch model {
	case "account.bank.statement":
		rows, err := odooLockRead(url, db, uid, pw, model, ids, []string{"line_ids"})
		if err != nil {
			return nil, err
		}
		var lineIDs []int
		for _, r := range rows {
			lineIDs = append(lineIDs, lockIDs(r["line_ids"])...)
		}
		return lockRecordDates(url, db, uid, pw, "account.bank.statement.line", lineIDs)
	case "account.partial.reconcile":
		rows, err := odooLockRead(url, db, uid, pw, model, ids, []string{"debit_move_id", "credit_move_id"})
		if err != nil {
			return nil, err
		}
		var lineIDs []int
		for _, r := range rows {
			for _, f := range []string{"debit_move_id", "credit_move_id"} {
				if m, ok := r[f].([]interface{}); ok && len(m) > 0 {
					lineIDs = append(lineIDs, lockIDs(m[0])...)
				}
			}
		}
		return lockRecordDates(url, db, uid, pw, "account.move.line", lineIDs)
	}
	fields := recordDateFields[model]
	if fields == nil {
		return nil, nil
	}
	rows, err := odooLockRead(url, db, uid, pw, model, ids, fields)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, r := range rows {
		if d, ok := r[fields[0]].(string); ok && isISODate(d) {
			out = append(out, d)
		}
	}
	return out, nil
}

func countLocked(dates []string, lock string) (locked int, first string) {
	sort.Strings(dates)
	for _, d := range dates {
		if d <= lock {
			if first == "" {
				first = d
			}
			locked++
		}
	}
	return
}

// matchingMethods change only which lines are matched, never amounts.
var matchingMethods = map[string]bool{
	"reconcile":             true,
	"remove_move_reconcile": true,
}

// checkOdooLock refuses a mutating call that touches a locked period.
func checkOdooLock(url, db string, uid int, pw, model, method string, args []interface{}) error {
	switch model {
	case "account.move", "account.move.line", "account.bank.statement.line",
		"account.bank.statement", "account.partial.reconcile", "account.journal":
	default:
		return nil
	}
	l := odooLockDate(url, db, uid, pw)
	if l.Date == "" {
		return nil
	}
	what := fmt.Sprintf("%s.%s", model, method)

	if method == "create" {
		if len(args) == 0 {
			return nil
		}
		if n, first := countLocked(valsDates(lockVals(args[0])), l.Date); n > 0 {
			return lockedErr(l, fmt.Sprintf("%s dated %s", what, first))
		}
		return nil
	}

	if len(args) == 0 {
		return nil
	}
	ids := lockIDs(args[0])

	if model == "account.journal" {
		if method != "unlink" || len(ids) == 0 {
			return nil
		}
		n, err := odooLockCount(url, db, uid, pw, "account.move.line", []interface{}{
			[]interface{}{"journal_id", "in", ids}, []interface{}{"date", "<=", l.Date}})
		if err != nil {
			return lockedErr(l, what+" (could not check its entries: "+err.Error()+")")
		}
		if n > 0 {
			return lockedErr(l, fmt.Sprintf("%s: the journal has %d items in the locked period", what, n))
		}
		return nil
	}

	if method == "write" && len(args) > 1 {
		if n, first := countLocked(valsDates(lockVals(args[1])), l.Date); n > 0 {
			return lockedErr(l, fmt.Sprintf("%s moving a record to %s", what, first))
		}
	}
	dates, err := lockRecordDates(url, db, uid, pw, model, ids)
	if err != nil {
		// Fail closed: a write we cannot place is not made.
		return lockedErr(l, what+" (could not read the records' dates: "+err.Error()+")")
	}
	n, first := countLocked(dates, l.Date)
	if n == 0 {
		return nil
	}
	if (matchingMethods[method] || model == "account.partial.reconcile") && n < len(dates) {
		return nil // matching across the lock date: allowed
	}
	return lockedErr(l, fmt.Sprintf("%s on %d record(s) dated from %s", what, n, first))
}

// odooLockBanner: shown with the Odoo target before the first write.
func odooLockBanner(url, db string, uid int, pw string) string {
	l := odooLockDate(url, db, uid, pw)
	if l.Date == "" {
		return ""
	}
	return "· " + strings.TrimSpace(l.String())
}

var odooLockPrinted bool

func printOdooLockOnce(url, db string, uid int, pw string) {
	if odooLockPrinted {
		return
	}
	odooLockPrinted = true
	if b := odooLockBanner(url, db, uid, pw); b != "" {
		fmt.Fprintf(os.Stderr, "  %s%s: no change on or before %s%s\n", Fmt.Dim, b, odooLockDate(url, db, uid, pw).Date, Fmt.Reset)
	}
}
