package cmd

import (
	"encoding/json"
	"fmt"
	"math"
)

// odooGlAccount is a chart-of-accounts entry resolved from Odoo.
type odooGlAccount struct {
	ID          int
	Code        string
	Name        string
	AccountType string
}

// fetchOdooAccountByCode resolves a single chart-of-accounts entry by its exact
// code (e.g. "550010"). Returns ok=false when no account has that code.
func fetchOdooAccountByCode(creds *OdooCredentials, uid int, code string) (odooGlAccount, bool, error) {
	rows, err := odooSearchReadAllMaps(creds, uid, "account.account",
		[]interface{}{[]interface{}{"code", "=", code}},
		[]string{"id", "code", "name", "account_type"}, "")
	if err != nil {
		return odooGlAccount{}, false, err
	}
	if len(rows) == 0 {
		return odooGlAccount{}, false, nil
	}
	r := rows[0]
	return odooGlAccount{
		ID:          odooInt(r["id"]),
		Code:        odooString(r["code"]),
		Name:        odooString(r["name"]),
		AccountType: odooString(r["account_type"]),
	}, true, nil
}

// fetchJournalDefaultAccountCode resolves a journal's default GL account (the
// "GL default account" shown in Odoo's journal config) to its chart-of-accounts
// code/name. Returns ok=false when the journal has no default account set.
func fetchJournalDefaultAccountCode(creds *OdooCredentials, uid, journalID int) (odooGlAccount, bool, error) {
	if journalID <= 0 {
		return odooGlAccount{}, false, nil
	}
	jrnRows, err := odooSearchReadAllMaps(creds, uid, "account.journal",
		[]interface{}{[]interface{}{"id", "=", journalID}},
		[]string{"id", "default_account_id"}, "")
	if err != nil {
		return odooGlAccount{}, false, err
	}
	if len(jrnRows) == 0 {
		return odooGlAccount{}, false, fmt.Errorf("journal %d not found", journalID)
	}
	accID := odooFieldID(jrnRows[0]["default_account_id"])
	if accID == 0 {
		return odooGlAccount{}, false, nil
	}
	accRows, err := odooSearchReadAllMaps(creds, uid, "account.account",
		[]interface{}{[]interface{}{"id", "=", accID}},
		[]string{"id", "code", "name", "account_type"}, "")
	if err != nil {
		return odooGlAccount{}, false, err
	}
	if len(accRows) == 0 {
		return odooGlAccount{}, false, nil
	}
	r := accRows[0]
	return odooGlAccount{
		ID:          odooInt(r["id"]),
		Code:        odooString(r["code"]),
		Name:        odooString(r["name"]),
		AccountType: odooString(r["account_type"]),
	}, true, nil
}

// fetchOdooAccountBalanceAt returns the balance of a GL account (debit − credit
// over its posted journal items) at the end of the given cutoff date
// (YYYY-MM-DD, inclusive; "" = all time), the number of journal items counted,
// and the date of the latest one. One read_group RPC does the aggregation
// server-side.
func fetchOdooAccountBalanceAt(creds *OdooCredentials, uid, accID int, cutoffDate string) (balance float64, count int, latest string, err error) {
	domain := []interface{}{
		[]interface{}{"account_id", "=", accID},
		[]interface{}{"parent_state", "=", "posted"},
	}
	if cutoffDate != "" {
		domain = append(domain, []interface{}{"date", "<=", cutoffDate})
	}
	result, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.move.line", "read_group",
		[]interface{}{
			domain,
			[]string{"balance:sum", "date:max"},
			[]string{},
		},
		map[string]interface{}{"lazy": false})
	if err != nil {
		return 0, 0, "", err
	}
	// With an empty groupby and no matching rows, Odoo returns a single group
	// whose aggregate fields are `false` (not 0/null) — so decode into
	// interface{} and coerce, rather than float64 which would fail to unmarshal.
	var groups []struct {
		Balance interface{} `json:"balance"`
		Date    interface{} `json:"date"`
		Count   int         `json:"__count"`
	}
	if err := json.Unmarshal(result, &groups); err != nil {
		return 0, 0, "", fmt.Errorf("parse read_group: %v", err)
	}
	if len(groups) == 0 {
		return 0, 0, "", nil
	}
	return roundCents(odooFloat(groups[0].Balance)), groups[0].Count, odooString(groups[0].Date), nil
}

// fetchJournalsUsingAccount finds the bank/cash journals that use this GL
// account as their default account — the reverse of fetchJournalDefaultAccountCode.
func fetchJournalsUsingAccount(creds *OdooCredentials, uid, accID int) ([]map[string]interface{}, error) {
	return odooSearchReadAllMaps(creds, uid, "account.journal",
		[]interface{}{[]interface{}{"default_account_id", "=", accID}},
		[]string{"id", "code", "name", "type"}, "code asc")
}

// printOdooAccount renders a one-account summary: identity, current balance,
// journal-item count, and any journals that default to it.
func printOdooAccount(creds *OdooCredentials, uid int, acc odooGlAccount) error {
	balance, count, latest, err := fetchOdooAccountBalanceAt(creds, uid, acc.ID, "")
	if err != nil {
		return fmt.Errorf("account balance: %v", err)
	}
	journals, err := fetchJournalsUsingAccount(creds, uid, acc.ID)
	if err != nil {
		return fmt.Errorf("journals: %v", err)
	}

	fmt.Printf("\n%s🧾 %s — %s%s\n", Fmt.Bold, acc.Code, acc.Name, Fmt.Reset)
	fmt.Printf("  %sType:%s    %s  %s(id %d)%s\n", Fmt.Dim, Fmt.Reset, acc.AccountType, Fmt.Dim, acc.ID, Fmt.Reset)
	fmt.Printf("  %sBalance:%s %s%s EUR  %s(%s",
		Fmt.Dim, Fmt.Reset, signPrefix(balance), fmtNumber(math.Abs(balance)),
		Fmt.Dim, Pluralize(count, "journal item", ""))
	if latest != "" {
		fmt.Printf(", latest %s", latest)
	}
	fmt.Printf(")%s\n", Fmt.Reset)

	if len(journals) > 0 {
		fmt.Printf("  %sDefault for:%s ", Fmt.Dim, Fmt.Reset)
		for i, j := range journals {
			if i > 0 {
				fmt.Print(", ")
			}
			fmt.Printf("%s (#%d)", odooString(j["name"]), odooInt(j["id"]))
		}
		fmt.Println()
	}
	fmt.Printf("\n  %sHistorical balance: %schb odoo accounts %s balance %s2024%s\n\n",
		Fmt.Dim, Fmt.Reset+Fmt.Cyan, acc.Code, Fmt.Reset+Fmt.Dim, Fmt.Reset)
	return nil
}

// OdooAccountBalance prints the balance of a GL account at the end of a period,
// mirroring `chb accounts <slug> balance [YYYY[/MM[/DD]]]` but reading posted
// journal items from Odoo rather than the local cache.
func OdooAccountBalance(creds *OdooCredentials, uid int, code string, args []string) error {
	if HasFlag(args, "--help", "-h", "help") {
		fmt.Printf("\n%schb odoo accounts <code> balance [YYYY[/MM[/DD]]]%s — GL account balance at end of period (from Odoo posted entries)\n\n", Fmt.Bold, Fmt.Reset)
		return nil
	}
	acc, ok, err := fetchOdooAccountByCode(creds, uid, code)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("no Odoo account with code %q", code)
	}
	cutoff, scope, err := parseAccountBalanceCutoff(args)
	if err != nil {
		return err
	}
	balance, count, latest, err := fetchOdooAccountBalanceAt(creds, uid, acc.ID, cutoff.Format("2006-01-02"))
	if err != nil {
		return err
	}
	fmt.Printf("\n%s%s — %s%s\n", Fmt.Bold, acc.Code, acc.Name, Fmt.Reset)
	fmt.Printf("  %sBalance at end of %s:%s %s%s EUR\n",
		Fmt.Dim, scope, Fmt.Reset, signPrefix(balance), fmtNumber(math.Abs(balance)))
	fmt.Printf("  %sBased on %s", Fmt.Dim, Pluralize(count, "posted journal item", ""))
	if latest != "" {
		fmt.Printf(" (latest %s)", latest)
	}
	fmt.Printf("%s\n\n", Fmt.Reset)
	return nil
}
