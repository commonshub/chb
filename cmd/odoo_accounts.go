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
		// Sub-command-specific help when a known verb precedes --help, so
		// `… review --help` shows the review help rather than this overview.
		switch {
		case HasFlag(args, "review"):
			printOdooAccountReviewHelp()
		case HasFlag(args, "list"):
			printOdooAccountListHelp()
		default:
			printOdooAccountsHelp()
		}
		return nil
	}
	limit := GetNumber(args, []string{"-n", "--limit"}, 50)
	if limit <= 0 {
		limit = 50
	}

	// Positional args, skipping flags and the value that follows -n/--limit.
	positionals := []string{}
	skipNext := false
	for _, a := range args {
		if skipNext {
			skipNext = false
			continue
		}
		if a == "-n" || a == "--limit" {
			skipNext = true
			continue
		}
		if strings.HasPrefix(a, "-") {
			continue
		}
		positionals = append(positionals, a)
	}
	query := ""
	if len(positionals) > 0 {
		query = positionals[0]
	}

	creds, err := ResolveOdooCredentials()
	if err != nil {
		return err
	}
	uid, err := odooAuth(creds.URL, creds.DB, creds.Login, creds.Password)
	if err != nil || uid == 0 {
		return fmt.Errorf("Odoo authentication failed: %v", err)
	}

	// `chb odoo accounts <code> balance [period]` — historical GL balance.
	if len(positionals) >= 2 && positionals[1] == "balance" {
		return OdooAccountBalance(creds, uid, query, positionals[2:])
	}

	// `chb odoo accounts <code> fix [--dry-run]` — reconcile the GL account to
	// its local source: post draft entries, flag foreign-journal entries.
	if len(positionals) >= 2 && positionals[1] == "fix" {
		return OdooAccountFix(creds, uid, query, args)
	}

	// `chb odoo accounts <code> review [--journal N] [--apply]` — sweep a
	// suspense/holding account: propose where each item belongs and allocate.
	if len(positionals) >= 2 && positionals[1] == "review" {
		return OdooAccountReview(creds, uid, query, args)
	}

	// `chb odoo accounts <code> list [--since --until --journal --csv]` — the
	// posted journal items on the account, with a running total.
	if len(positionals) >= 2 && positionals[1] == "list" {
		return OdooAccountList(creds, uid, query, args)
	}

	// `chb odoo accounts <code>` where <code> is an exact account code → show a
	// single-account summary. A prefix/name query (e.g. "70", "donation") has no
	// exact match and falls through to the browse list below.
	if len(positionals) == 1 && !HasFlag(args, "--csv") {
		if acc, ok, err := fetchOdooAccountByCode(creds, uid, query); err == nil && ok {
			return printOdooAccount(creds, uid, acc)
		}
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
  %schb odoo accounts%s                    First 50 accounts, code asc
  %schb odoo accounts 70%s                 Codes starting with "70"
  %schb odoo accounts donation%s           Accounts whose name contains "donation"
  %schb odoo accounts 550010%s             Summary of the account with exact code 550010
  %schb odoo accounts 550010 balance%s     Balance now (posted entries)
  %schb odoo accounts 550010 balance 2024%s End-of-2024 balance (also YYYY/MM, YYYY/MM/DD)
  %schb odoo accounts 550010 fix --dry-run%s Reconcile GL ↔ local: post drafts, flag foreign entries
  %schb odoo accounts 499000 review%s       Sweep a suspense account: propose + allocate items
  %schb odoo accounts 499000 review --journal 48 --apply%s  Allocate journal-48 items out of suspense
  %schb odoo accounts 580000 list%s         List posted items on the account (running total)
  %schb odoo accounts 580000 list --since 2025 --csv%s  Items since 2025, as CSV

%sOPTIONS%s
  %s-n N%s, %s--limit N%s   Cap the number of rows (default 50)
  %s--csv%s             CSV output instead of a formatted table
`,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Yellow, f.Reset, f.Yellow, f.Reset,
		f.Yellow, f.Reset,
	)
}
