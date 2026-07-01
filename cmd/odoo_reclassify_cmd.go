package cmd

import (
	"fmt"
	"strconv"
	"strings"
)

// OdooReclassifyCommand moves one or more bank-statement lines' counterpart GL
// account onto a chosen account code. It reuses applyOdooMappingAccount — the
// same draft → write → post engine the mapping-driven reclassifier uses — so a
// reconciled/posted line is drafted, rewritten and reposted safely.
//
//	chb odoo reclassify <statementLineId...> --to <accountCode>           Preview
//	chb odoo reclassify <statementLineId...> --to <accountCode> --apply   Write
//
// The ids are account.bank.statement.line ids (as shown by the journal caches /
// `chb odoo journals` listings). This is the surgical counterpart to the
// bulk `chb odoo reconcile --remap`: for one-off mis-tagged lines where the
// rules can't be made to express the exception.
func OdooReclassifyCommand(args []string) error {
	if HasFlag(args, "--help", "-h", "help") {
		printOdooReclassifyHelp()
		return nil
	}
	apply := HasFlag(args, "--apply")
	code := GetOption(args, "--to")
	if code == "" {
		return fmt.Errorf("missing --to <accountCode> (e.g. --to 640160)")
	}

	var lineIDs []int
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--to" { // skip the flag value
			i++
			continue
		}
		if strings.HasPrefix(a, "-") {
			continue
		}
		n, err := strconv.Atoi(a)
		if err != nil {
			return fmt.Errorf("invalid statement-line id %q (expected an integer)", a)
		}
		lineIDs = append(lineIDs, n)
	}
	if len(lineIDs) == 0 {
		return fmt.Errorf("no statement-line ids given")
	}

	creds, err := ResolveOdooCredentials()
	if err != nil {
		return err
	}
	uid, err := odooAuth(creds.URL, creds.DB, creds.Login, creds.Password)
	if err != nil || uid == 0 {
		return fmt.Errorf("Odoo authentication failed: %v", err)
	}

	targetID, err := findOdooAccountIDByCode(creds, uid, code)
	if err != nil {
		return fmt.Errorf("resolve target account %s: %v", code, err)
	}

	// Preview: current counterpart account per line.
	lineRows, err := odooReadMapsByIDs(creds, uid, "account.bank.statement.line",
		uniquePositiveInts(lineIDs), []string{"id", "move_id", "date", "amount", "payment_ref"})
	if err != nil {
		return fmt.Errorf("read statement lines: %v", err)
	}
	moveIDs := make([]int, 0, len(lineRows))
	for _, r := range lineRows {
		if mid := odooFieldID(r["move_id"]); mid > 0 {
			moveIDs = append(moveIDs, mid)
		}
	}
	cpByMove, _ := fetchCounterpartMoveLinesByMoveID(creds, uid, moveIDs)
	accIDs := []int{}
	for _, info := range cpByMove {
		if info.AccountID > 0 {
			accIDs = append(accIDs, info.AccountID)
		}
	}
	codeByID, _ := fetchAccountCodesByID(creds, uid, accIDs)

	fmt.Printf("\n%s↪ Reclassify → %s%s  %s%s (db: %s)%s\n",
		Fmt.Bold, code, Fmt.Reset, Fmt.Dim, creds.URL, creds.DB, Fmt.Reset)
	for _, r := range lineRows {
		mid := odooFieldID(r["move_id"])
		cur := "?"
		if info, ok := cpByMove[mid]; ok {
			if c, ok := codeByID[info.AccountID]; ok {
				cur = c
			}
		}
		fmt.Printf("    %s  %12.2f  [%s → %s]  %s\n",
			odooString(r["date"]), odooFloat(r["amount"]), cur, code,
			Truncate(odooString(r["payment_ref"]), 44))
	}

	if !apply {
		fmt.Printf("\n  %sPreview only — re-run with %s--apply%s to write.%s\n\n",
			Fmt.Yellow, Fmt.Cyan, Fmt.Yellow, Fmt.Reset)
		return nil
	}
	if err := RequireOdooWriteCapability(); err != nil {
		return err
	}
	if err := applyOdooMappingAccount(creds, uid, lineIDs, code); err != nil {
		return err
	}
	_ = targetID
	fmt.Printf("\n  %s✓ Reclassified %s to %s.%s\n\n",
		Fmt.Green, Pluralize(len(lineIDs), "line", "lines"), code, Fmt.Reset)
	return nil
}

func printOdooReclassifyHelp() {
	f := Fmt
	fmt.Printf(`
%schb odoo reclassify%s — Move specific bank-statement lines onto a GL account.

Reuses the maintained draft → write → post engine, so a reconciled/posted line
is drafted, rewritten and reposted safely. Use it for one-off mis-tagged lines
where the mapping rules can't express the exception.

%sUSAGE%s
  %schb odoo reclassify <statementLineId...> --to <accountCode>%s          Preview
  %schb odoo reclassify <statementLineId...> --to <accountCode> --apply%s  Write

The ids are account.bank.statement.line ids (from the journal caches /
%schb odoo journals%s listings).
`,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
	)
}
