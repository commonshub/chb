package cmd

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// AccountBalance prints the balance of a single account at the end of
// the supplied period:
//
//	chb accounts <slug> balance              → today (end of day)
//	chb accounts <slug> balance 2025         → end of 2025
//	chb accounts <slug> balance 2025/12      → end of December 2025
//	chb accounts <slug> balance 2025/12/31   → end of that day
//
// All timestamps are interpreted in Europe/Brussels. The balance is
// computed from the locally-cached transactions (the same ones that
// feed `chb accounts <slug>` and the Odoo sync), so it reflects what
// `chb accounts <slug> sync` last produced — re-run sync first if a
// fresh on-chain reading is needed.
func AccountBalance(slug string, args []string) error {
	if HasFlag(args, "--help", "-h", "help") {
		printAccountBalanceHelp()
		return nil
	}

	var acc *AccountConfig
	for _, a := range LoadAccountConfigs() {
		if strings.EqualFold(a.Slug, slug) {
			acc = &a
			break
		}
	}
	if acc == nil {
		return fmt.Errorf("account %q not found", slug)
	}

	cutoff, scope, err := parseAccountBalanceCutoff(args)
	if err != nil {
		return err
	}

	txs := loadAccountTransactionsForOdoo(acc)

	var balance float64
	var counted, future int
	var latest time.Time
	for _, tx := range txs {
		t := time.Unix(tx.Timestamp, 0)
		if t.After(cutoff) {
			future++
			continue
		}
		balance += signedOdooAmountForTransaction(acc, tx)
		counted++
		if t.After(latest) {
			latest = t
		}
	}
	balance = roundCents(balance)

	currency := accCurrency(acc)
	fmt.Printf("\n%s%s — %s%s\n", Fmt.Bold, acc.Slug, acc.Name, Fmt.Reset)
	fmt.Printf("  %sBalance at end of %s:%s %s%s %s\n",
		Fmt.Dim, scope, Fmt.Reset, signPrefix(balance), fmtNumber(math.Abs(balance)), currency)
	fmt.Printf("  %sBased on %s",
		Fmt.Dim, Pluralize(counted, "local transaction", ""))
	if latest.Unix() > 0 {
		fmt.Printf(" (latest %s)", latest.In(BrusselsTZ()).Format("2006-01-02 15:04"))
	}
	if future > 0 {
		fmt.Printf(", %s ignored after the cutoff", Pluralize(future, "tx", ""))
	}
	fmt.Printf("%s\n\n", Fmt.Reset)
	return nil
}

// parseAccountBalanceCutoff turns the positional date argument into a
// concrete cutoff time (end-of-period in Brussels TZ). With no arg, the
// cutoff is end of today.
func parseAccountBalanceCutoff(args []string) (time.Time, string, error) {
	tz := BrusselsTZ()
	now := time.Now().In(tz)

	raw := ""
	for _, a := range args {
		if strings.HasPrefix(a, "-") {
			continue
		}
		raw = a
		break
	}
	if raw == "" {
		eod := endOfDay(now)
		return eod, eod.Format("2006-01-02"), nil
	}

	// Accept YYYY, YYYY/MM, YYYY/MM/DD with `/`, `-`, or no separator.
	canonical := strings.ReplaceAll(raw, "-", "/")
	if !strings.Contains(canonical, "/") && len(canonical) == 8 {
		canonical = canonical[:4] + "/" + canonical[4:6] + "/" + canonical[6:]
	} else if !strings.Contains(canonical, "/") && len(canonical) == 6 {
		canonical = canonical[:4] + "/" + canonical[4:]
	}
	parts := strings.Split(canonical, "/")

	yr, err := strconv.Atoi(parts[0])
	if err != nil || len(parts[0]) != 4 {
		return time.Time{}, "", fmt.Errorf("bad year %q — expected YYYY[/MM[/DD]]", raw)
	}

	switch len(parts) {
	case 1:
		eoy := time.Date(yr, 12, 31, 23, 59, 59, 0, tz)
		return eoy, eoy.Format("2006"), nil
	case 2:
		mo, err := strconv.Atoi(parts[1])
		if err != nil || mo < 1 || mo > 12 {
			return time.Time{}, "", fmt.Errorf("bad month %q in %q", parts[1], raw)
		}
		// Last day of month, 23:59:59 — Go normalises Year-Mo-32 → next month.
		firstOfNext := time.Date(yr, time.Month(mo)+1, 1, 0, 0, 0, 0, tz)
		eom := firstOfNext.Add(-time.Second)
		return eom, eom.Format("2006-01"), nil
	case 3:
		mo, err := strconv.Atoi(parts[1])
		if err != nil || mo < 1 || mo > 12 {
			return time.Time{}, "", fmt.Errorf("bad month %q in %q", parts[1], raw)
		}
		day, err := strconv.Atoi(parts[2])
		if err != nil || day < 1 || day > 31 {
			return time.Time{}, "", fmt.Errorf("bad day %q in %q", parts[2], raw)
		}
		t := time.Date(yr, time.Month(mo), day, 23, 59, 59, 0, tz)
		// Reject obviously-bad days like 2025/02/30 — Go will roll them
		// over silently otherwise.
		if t.Month() != time.Month(mo) || t.Day() != day {
			return time.Time{}, "", fmt.Errorf("invalid date %q", raw)
		}
		return t, t.Format("2006-01-02"), nil
	}
	return time.Time{}, "", fmt.Errorf("unrecognised date %q — expected YYYY[/MM[/DD]]", raw)
}

func endOfDay(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 23, 59, 59, 0, t.Location())
}

func printAccountBalanceHelp() {
	f := Fmt
	fmt.Printf(`
%schb accounts <slug> balance%s — Historical balance of one account

%sUSAGE%s
  %schb accounts <slug> balance%s              End of today
  %schb accounts <slug> balance%s 2025         End of 2025
  %schb accounts <slug> balance%s 2025/12      End of December 2025
  %schb accounts <slug> balance%s 2025/12/31   End of that day

%sNOTES%s
  • Computed from locally-cached transactions (run %schb accounts <slug> sync%s
    first for fresh data). Dates parsed in Europe/Brussels.
  • Accepts %sYYYY%s, %sYYYY/MM%s, %sYYYY/MM/DD%s (or %s-%s / %sYYYYMMDD%s).

%sOPTIONS%s
  %s--help, -h%s           Show this help
`,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Yellow, f.Reset, f.Yellow, f.Reset, f.Yellow, f.Reset, f.Yellow, f.Reset, f.Yellow, f.Reset,
		f.Bold, f.Reset,
		f.Yellow, f.Reset,
	)
}

// signPrefix returns "+" / "-" / "" so we can build "<sign><amount> <currency>"
// without the hard-coded "EUR" suffix that fmtEURSigned tacks on.
func signPrefix(v float64) string {
	if v > 0 {
		return "+"
	}
	if v < 0 {
		return "-"
	}
	return ""
}
