package cmd

import (
	"fmt"
	"strings"
)

// OdooAccountsCommand lists Odoo chart-of-accounts entries matching an
// optional code-prefix or name-substring query. Useful when setting up
// rules (`chb odoo rules add --account <code>`) — it lets the operator
// browse what's available without leaving the terminal.
//
//	chb odoo accounts            list first 50 accounts (code asc)
//	chb odoo accounts 70         match codes starting with "70"
//	chb odoo accounts donation   match accounts whose name contains "donation"
//	chb odoo accounts -n 200     widen the cap
func OdooAccountsCommand(args []string) error {
	if HasFlag(args, "--help", "-h") {
		printOdooAccountsHelp()
		return nil
	}
	limit := GetNumber(args, []string{"-n", "--limit"}, 50)
	if limit <= 0 {
		limit = 50
	}

	query := ""
	for _, a := range args {
		if strings.HasPrefix(a, "-") {
			continue
		}
		query = a
		break
	}

	creds, err := ResolveOdooCredentials()
	if err != nil {
		return err
	}
	uid, err := odooAuth(creds.URL, creds.DB, creds.Login, creds.Password)
	if err != nil || uid == 0 {
		return fmt.Errorf("Odoo authentication failed: %v", err)
	}

	domain := []interface{}{}
	if query != "" {
		domain = []interface{}{
			"|",
			[]interface{}{"code", "=ilike", query + "%"},
			[]interface{}{"name", "ilike", query},
		}
	}
	rows, err := odooSearchReadAllMaps(creds, uid, "account.account",
		domain,
		[]string{"id", "code", "name", "account_type"},
		"code asc")
	if err != nil {
		return fmt.Errorf("search accounts: %v", err)
	}
	if len(rows) > limit {
		rows = rows[:limit]
	}

	if HasFlag(args, "--csv") {
		fmt.Println("code,name,account_type,id")
		for _, r := range rows {
			fmt.Printf("%s,%s,%s,%d\n",
				csvCell(odooString(r["code"])),
				csvCell(odooString(r["name"])),
				csvCell(odooString(r["account_type"])),
				odooInt(r["id"]))
		}
		return nil
	}

	if len(rows) == 0 {
		fmt.Printf("\n%sNo accounts match%s%s\n\n", Fmt.Dim, queryHint(query), Fmt.Reset)
		return nil
	}

	fmt.Printf("\n%s🧾 Odoo Chart of Accounts%s%s\n\n",
		Fmt.Bold, queryHint(query), Fmt.Reset)
	headers := []string{"Code", "Name", "Type", "Id"}
	out := make([][]string, 0, len(rows))
	for _, r := range rows {
		out = append(out, []string{
			odooString(r["code"]),
			Truncate(odooString(r["name"]), 50),
			odooString(r["account_type"]),
			fmt.Sprintf("%d", odooInt(r["id"])),
		})
	}
	totalRow := []string{"", Pluralize(len(rows), "account", ""), "", ""}
	renderTicketsTable(headers, out, totalRow, map[int]bool{3: true})
	return nil
}

func queryHint(query string) string {
	if query == "" {
		return ""
	}
	return " — query: \"" + query + "\""
}

func printOdooAccountsHelp() {
	f := Fmt
	fmt.Printf(`
%schb odoo accounts [query]%s — Browse the Odoo chart of accounts.

%sUSAGE%s
  %schb odoo accounts%s              First 50 accounts, code asc
  %schb odoo accounts 70%s           Codes starting with "70"
  %schb odoo accounts donation%s     Accounts whose name contains "donation"

%sOPTIONS%s
  %s-n N%s, %s--limit N%s   Cap the number of rows (default 50)
  %s--csv%s             CSV output instead of a formatted table
`,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Yellow, f.Reset, f.Yellow, f.Reset,
		f.Yellow, f.Reset,
	)
}
