package cmd

import (
	"fmt"
	"math"
)

// OdooAccountList prints the posted journal items booked on a GL account, oldest
// first, with a running total — the line-level detail behind
// `chb odoo accounts <code>` / `… balance`. Handy for accounts that should net to
// zero (e.g. 580000 internal transfers): the total at the foot is the imbalance.
//
//	chb odoo accounts 580000 list
//	chb odoo accounts 580000 list --since 2025 --until 2025
//	chb odoo accounts 580000 list --journal 48 --csv
func OdooAccountList(creds *OdooCredentials, uid int, code string, args []string) error {
	if HasFlag(args, "--help", "-h", "help") {
		printOdooAccountListHelp()
		return nil
	}
	csv := HasFlag(args, "--csv")
	unmatched := HasFlag(args, "--unmatched")
	journalFilter := GetNumber(args, []string{"--journal", "-j"}, 0)

	acc, ok, err := fetchOdooAccountByCode(creds, uid, code)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("no Odoo account with code %q", code)
	}

	domain := []interface{}{
		[]interface{}{"account_id", "=", acc.ID},
		[]interface{}{"parent_state", "=", "posted"},
	}
	if journalFilter > 0 {
		domain = append(domain, []interface{}{"journal_id", "=", journalFilter})
	}
	if s := GetOption(args, "--since"); s != "" {
		t, ok := ParseSinceDate(s)
		if !ok {
			return fmt.Errorf("invalid --since %q (expected %s)", s, DateFormatHelp)
		}
		domain = append(domain, []interface{}{"date", ">=", t.Format("2006-01-02")})
	}
	if u := GetOption(args, "--until"); u != "" {
		t, ok := ParseDateEndExclusive(u)
		if !ok {
			return fmt.Errorf("invalid --until %q (expected %s)", u, DateFormatHelp)
		}
		domain = append(domain, []interface{}{"date", "<", t.Format("2006-01-02")})
	}

	rows, err := odooSearchReadAllMaps(creds, uid, "account.move.line",
		domain,
		[]string{"id", "move_id", "journal_id", "date", "name", "partner_id", "balance"},
		"date asc, id asc")
	if err != nil {
		return fmt.Errorf("read account lines: %v", err)
	}

	// --unmatched: keep only the legs that have no opposite-sign counterpart of
	// the same magnitude. On a clearing account (e.g. 580000) every transfer
	// books two legs that cancel; what survives the pairing IS the imbalance.
	// The surviving legs always sum to the full account net, so this is a
	// strictly-narrower view of the same total — just the rows that explain it.
	matchedPairs := 0
	if unmatched {
		rows, matchedPairs = unmatchedAccountLines(rows)
	}

	if csv {
		fmt.Println("date,journal_id,move,partner,description,balance")
		for _, r := range rows {
			fmt.Printf("%s,%d,%s,%s,%s,%.2f\n",
				csvCell(odooString(r["date"])),
				odooFieldID(r["journal_id"]),
				csvCell(odooFieldName(r["move_id"])),
				csvCell(odooFieldName(r["partner_id"])),
				csvCell(odooString(r["name"])),
				roundCents(odooFloat(r["balance"])))
		}
		return nil
	}

	if len(rows) == 0 {
		if unmatched {
			fmt.Printf("\n%s✓ %s — %s: every leg pairs off; account nets to zero.%s\n\n", Fmt.Green, acc.Code, acc.Name, Fmt.Reset)
			return nil
		}
		fmt.Printf("\n%s%s — %s%s: no posted items%s\n\n", Fmt.Bold, acc.Code, acc.Name, Fmt.Reset, "")
		return nil
	}

	title := acc.Name
	if unmatched {
		title += " · unmatched legs"
	}
	fmt.Printf("\n%s🧾 %s — %s%s\n\n", Fmt.Bold, acc.Code, title, Fmt.Reset)
	headers := []string{"Date", "Jrnl", "Move", "Description", "Amount"}
	out := make([][]string, 0, len(rows))
	var total float64
	for _, r := range rows {
		bal := roundCents(odooFloat(r["balance"]))
		total += bal
		desc := odooString(r["name"])
		if p := odooFieldName(r["partner_id"]); p != "" {
			desc = p + " · " + desc
		}
		out = append(out, []string{
			odooString(r["date"]),
			fmt.Sprintf("%d", odooFieldID(r["journal_id"])),
			Truncate(odooFieldName(r["move_id"]), 18),
			Truncate(desc, 44),
			signPrefix(bal) + fmtNumber(math.Abs(bal)),
		})
	}
	totalRow := []string{"", "", "", Pluralize(len(rows), "item", ""), signPrefix(total) + fmtNumber(math.Abs(total))}
	renderTicketsTable(headers, out, totalRow, map[int]bool{4: true})

	if math.Abs(total) < 0.005 {
		fmt.Printf("\n  %s✓ Nets to zero.%s\n\n", Fmt.Green, Fmt.Reset)
	} else {
		fmt.Printf("\n  %s⚠ Net balance: %s%s EUR%s — this account does not net to zero.%s\n",
			Fmt.Yellow, signPrefix(total), fmtNumber(math.Abs(total)), Fmt.Reset, "")
		if unmatched {
			fmt.Printf("  %s%s hidden (cancelled out); the %s above are every leg with no opposite-sign counterpart.%s\n",
				Fmt.Dim, Pluralize(matchedPairs, "matched pair", "matched pairs"), Pluralize(len(rows), "leg", "legs"), Fmt.Reset)
		}
		fmt.Println()
	}
	return nil
}

// unmatchedAccountLines pairs opposite-sign legs of equal magnitude and returns
// only the legs left unpaired, plus the number of cancelling pairs removed.
// Legs are bucketed by |balance| rounded to the cent; within a bucket the
// min(pos,neg) count cancels and the surplus side survives. The surviving legs
// always sum to the same net as the full set (matched pairs net to zero), so
// this never hides part of the imbalance — only the noise that explains none of
// it. Input order (date asc) is preserved in the output.
func unmatchedAccountLines(rows []map[string]interface{}) ([]map[string]interface{}, int) {
	type bucket struct{ pos, neg int }
	buckets := map[string]*bucket{}
	for _, r := range rows {
		bal := roundCents(odooFloat(r["balance"]))
		if bal == 0 {
			continue // a zero leg is its own counterpart; ignore
		}
		key := fmt.Sprintf("%.2f", math.Abs(bal))
		b := buckets[key]
		if b == nil {
			b = &bucket{}
			buckets[key] = b
		}
		if bal > 0 {
			b.pos++
		} else {
			b.neg++
		}
	}

	pairs := 0
	for _, b := range buckets {
		if b.pos < b.neg {
			pairs += b.pos
		} else {
			pairs += b.neg
		}
	}

	// Emit only the surplus side of each bucket, in original (date) order. The
	// first `matched` legs of the surplus side are the ones that cancelled; the
	// remainder survive.
	out := make([]map[string]interface{}, 0)
	posSeen := map[string]int{}
	negSeen := map[string]int{}
	for _, r := range rows {
		bal := roundCents(odooFloat(r["balance"]))
		if bal == 0 {
			continue
		}
		key := fmt.Sprintf("%.2f", math.Abs(bal))
		b := buckets[key]
		matched := b.pos
		if b.neg < matched {
			matched = b.neg
		}
		if bal > 0 {
			idx := posSeen[key]
			posSeen[key]++
			if b.pos > b.neg && idx >= matched {
				out = append(out, r)
			}
		} else {
			idx := negSeen[key]
			negSeen[key]++
			if b.neg > b.pos && idx >= matched {
				out = append(out, r)
			}
		}
	}
	return out, pairs
}

func printOdooAccountListHelp() {
	f := Fmt
	fmt.Printf(`
%schb odoo accounts <code> list%s — List the posted journal items on a GL account.

Oldest first, with a running total. The foot total is the account's net balance —
useful for accounts that should net to zero (e.g. 580000 internal transfers).

%sUSAGE%s
  %schb odoo accounts 580000 list%s                  All posted items on 580000
  %schb odoo accounts 580000 list --unmatched%s       Only legs with no counterpart
  %schb odoo accounts 580000 list --since 2025%s      From 2025-01-01 onward
  %schb odoo accounts 580000 list --since 2025 --until 2025%s  Only 2025
  %schb odoo accounts 580000 list --journal 48 --csv%s  Journal #48 items, as CSV

%sOPTIONS%s
  %s--unmatched%s            Hide legs that cancel an equal, opposite-sign leg;
                       show only the residual that explains the imbalance
  %s--since D%s, %s--until D%s   Date window (%s)
  %s--journal N%s, %s-j N%s     Only items posted via that journal
  %s--csv%s                CSV output instead of a table
`,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset, f.Yellow, f.Reset, DateFormatHelp,
		f.Yellow, f.Reset, f.Yellow, f.Reset,
		f.Yellow, f.Reset,
	)
}
