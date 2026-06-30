package cmd

import (
	"strings"
	"testing"
)

func TestReconcileStaleCacheErrorMatchesAlreadyReconciled(t *testing.T) {
	cases := []struct {
		msg  string
		want bool
	}{
		{"odoo.exceptions.UserError: You are trying to reconcile some entries (for invoice INV/2025/02141).", true},
		{"odoo error: already reconciled in Odoo", true},
		{"no open A/R or A/P line", true},
		{"some unrelated odoo failure", false},
	}
	for _, c := range cases {
		if got := reconcileStaleCacheError(errString(c.msg)); got != c.want {
			t.Errorf("reconcileStaleCacheError(%q) = %v, want %v", c.msg, got, c.want)
		}
	}
}

type errString string

func (e errString) Error() string { return string(e) }

func TestAlreadyReconciledAttachErrorMessage(t *testing.T) {
	creds := &OdooCredentials{URL: "https://citizenspring-test.odoo.com"}
	line := odooStatementLineForReconcile{ID: 555, MoveID: 7001}
	move := odooMoveCandidate{ID: 4242, Name: "INV/2025/02141"}
	states := []reconciledLineState{
		{lineID: 999, reconciled: true, matches: []matchedMoveRef{{moveID: 3030, name: "INV/2025/00099"}}},
	}
	err := alreadyReconciledAttachError(creds, "invoice", line, move, states)
	msg := err.Error()

	for _, want := range []string{
		"bank line #555 is already matched to another invoice/bill",
		"INV/2025/02141",
		"already matched with INV/2025/00099",
		"web#id=3030&model=account.move", // conflicting move URL
		"web#id=4242&model=account.move", // target invoice URL
		"web#id=7001&model=account.move", // bank entry URL
		"chb invoices reconcile -i",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("error message missing %q\n--- got ---\n%s", want, msg)
		}
	}
}

func TestReconciledLineStateMatchNames(t *testing.T) {
	s := reconciledLineState{matches: []matchedMoveRef{{name: "INV/A"}, {name: "INV/B"}, {moveID: 5}}}
	if got := s.matchNames(); got != "INV/A, INV/B" {
		t.Errorf("matchNames = %q", got)
	}
	if (reconciledLineState{}).matchNames() != "" {
		t.Errorf("empty state should have no names")
	}
}

func TestKindPluralLabel(t *testing.T) {
	if kindPluralLabel("bill") != "bills" || kindPluralLabel("invoice") != "invoices" {
		t.Errorf("kindPluralLabel mapping wrong")
	}
}
