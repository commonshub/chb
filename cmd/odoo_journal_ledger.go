package cmd

// The books' view of a bank journal: the posted balance of its default
// (bank) account. That is what drift is measured against, not the sum of
// the journal's statement lines: a journal migrated with an opening line
// plus its history, or with entries booked straight on the bank account
// (closing/restatement entries), has statement lines that do not add up
// to the account — journal 50 (multisig) sums to 9,089.19 in statement
// lines while account 550015 is 0.00, like the chain.

import (
	"encoding/json"
	"fmt"
)

// odooJournalLedgerBalance returns the posted balance of the journal's
// default account.
func odooJournalLedgerBalance(creds *OdooCredentials, uid int, journalID int) (float64, error) {
	rows, err := odooReadMapsByIDs(creds, uid, "account.journal", []int{journalID}, []string{"default_account_id"})
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, fmt.Errorf("journal #%d not found", journalID)
	}
	accID := odooFieldID(rows[0]["default_account_id"])
	if accID == 0 {
		return 0, fmt.Errorf("journal #%d has no default account", journalID)
	}
	res, err := odooExec(creds.URL, creds.DB, uid, creds.Password, "account.move.line", "read_group",
		[]interface{}{
			[]interface{}{[]interface{}{"account_id", "=", accID}, []interface{}{"parent_state", "=", "posted"}},
			[]string{"balance:sum"},
			[]string{},
		},
		map[string]interface{}{"lazy": false})
	if err != nil {
		return 0, err
	}
	var groups []struct {
		Balance float64 `json:"balance"`
	}
	if err := json.Unmarshal(res, &groups); err != nil {
		return 0, err
	}
	if len(groups) == 0 {
		return 0, nil
	}
	return roundCents(groups[0].Balance), nil
}

// statementLinesHint: a dim note when the statement lines disagree with
// the ledger (shown next to a drift, never on its own).
func statementLinesHint(statementLines, ledger float64, currency string) string {
	if d := roundCents(statementLines - ledger); d > 0.005 || d < -0.005 {
		return fmt.Sprintf("    %s· statement lines sum to %s, %s from the ledger (entries outside statements: openings, restatements)%s\n",
			Fmt.Dim, formatBalance(statementLines, currency), formatBalance(d, currency), Fmt.Reset)
	}
	return ""
}
