package cmd

import (
	"errors"
	"testing"
)

func withOdooLock(t *testing.T, lock string, dates map[string]map[int]string) {
	t.Helper()
	oldFetch, oldRead, oldCount := odooLockFetch, odooLockRead, odooLockCount
	odooLockCache = map[string]odooLockInfo{}
	t.Setenv("APP_DATA_DIR", t.TempDir())
	odooLockFetch = func(url, db string, uid int, pw string) (string, error) { return lock, nil }
	odooLockRead = func(url, db string, uid int, pw, model string, ids []int, fields []string) ([]map[string]interface{}, error) {
		var rows []map[string]interface{}
		for _, id := range ids {
			switch model {
			case "account.bank.statement":
				var lines []interface{}
				for lid := range dates["account.bank.statement.line"] {
					lines = append(lines, float64(lid))
				}
				rows = append(rows, map[string]interface{}{"id": float64(id), "line_ids": lines})
			case "account.partial.reconcile":
				rows = append(rows, map[string]interface{}{"id": float64(id),
					"debit_move_id": []interface{}{float64(1), "a"}, "credit_move_id": []interface{}{float64(2), "b"}})
			default:
				rows = append(rows, map[string]interface{}{"id": float64(id), "date": dates[model][id]})
			}
		}
		return rows, nil
	}
	odooLockCount = func(url, db string, uid int, pw, model string, domain []interface{}) (int, error) { return 3, nil }
	t.Cleanup(func() {
		odooLockFetch, odooLockRead, odooLockCount = oldFetch, oldRead, oldCount
		odooLockCache = map[string]odooLockInfo{}
	})
}

func TestOdooLockGuard(t *testing.T) {
	withOdooLock(t, "2025-12-31", map[string]map[int]string{
		"account.move":                {10: "2025-06-01", 11: "2026-02-01"},
		"account.move.line":           {1: "2025-12-20", 2: "2026-01-05", 3: "2025-03-01"},
		"account.bank.statement.line": {20: "2025-12-31", 21: "2026-01-01"},
	})
	ids := func(v ...int) []interface{} {
		out := []interface{}{}
		for _, i := range v {
			out = append(out, i)
		}
		return out
	}
	cases := []struct {
		name, model, method string
		args                []interface{}
		locked              bool
	}{
		{"create in closed year", "account.bank.statement.line", "create", []interface{}{[]interface{}{map[string]interface{}{"date": "2025-12-31"}}}, true},
		{"create in open year", "account.bank.statement.line", "create", []interface{}{map[string]interface{}{"date": "2026-01-01"}}, false},
		{"bill dated in closed year", "account.move", "create", []interface{}{[]interface{}{map[string]interface{}{"invoice_date": "2025-11-02"}}}, true},
		{"write a locked line", "account.bank.statement.line", "write", []interface{}{ids(20), map[string]interface{}{"payment_ref": "x"}}, true},
		{"write an open line", "account.bank.statement.line", "write", []interface{}{ids(21), map[string]interface{}{"payment_ref": "x"}}, false},
		{"move an open line into the closed year", "account.bank.statement.line", "write", []interface{}{ids(21), map[string]interface{}{"date": "2025-12-01"}}, true},
		{"reset a locked move to draft", "account.move", "button_draft", []interface{}{ids(10)}, true},
		{"post an open move", "account.move", "action_post", []interface{}{ids(11)}, false},
		{"unlink mixed moves", "account.move", "unlink", []interface{}{ids(10, 11)}, true},
		{"match across the lock date", "account.move.line", "reconcile", []interface{}{ids(1, 2)}, false},
		{"match inside the closed year", "account.move.line", "reconcile", []interface{}{ids(1, 3)}, true},
		{"unmatch across the lock date", "account.move.line", "remove_move_reconcile", []interface{}{ids(1, 2)}, false},
		{"partial across the lock date", "account.partial.reconcile", "unlink", []interface{}{ids(5)}, false},
		{"undo a locked statement line", "account.bank.statement.line", "action_undo_reconciliation", []interface{}{ids(20)}, true},
		{"delete a statement with locked lines", "account.bank.statement", "unlink", []interface{}{ids(7)}, true},
		{"delete a journal with locked items", "account.journal", "unlink", []interface{}{ids(48)}, true},
		{"rename a journal", "account.journal", "write", []interface{}{ids(48), map[string]interface{}{"name": "x"}}, false},
		{"partners are not entries", "res.partner", "write", []interface{}{ids(1), map[string]interface{}{"name": "x"}}, false},
	}
	for _, c := range cases {
		err := checkOdooLock("https://x.odoo.com", "x", 1, "pw", c.model, c.method, c.args)
		if got := errors.Is(err, ErrOdooPeriodLocked); got != c.locked {
			t.Errorf("%s: locked=%v, want %v (err=%v)", c.name, got, c.locked, err)
		}
	}
}

func TestOdooLockNone(t *testing.T) {
	withOdooLock(t, "", nil)
	if err := checkOdooLock("https://x.odoo.com", "x", 1, "pw", "account.move", "button_draft", []interface{}{[]interface{}{1}}); err != nil {
		t.Fatalf("no lock date: %v", err)
	}
}
