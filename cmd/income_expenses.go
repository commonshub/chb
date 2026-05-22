package cmd

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

// Income runs `chb income <DATERANGE> [--account slug]`.
func Income(args []string) error {
	return runIncomeExpenseReport("income", args)
}

// Expenses runs `chb expenses <DATERANGE> [--account slug]`.
func Expenses(args []string) error {
	return runIncomeExpenseReport("expenses", args)
}

// incomeExpenseTx is the per-transaction row the interactive drill
// view shows after the operator picks a category from the list. Only
// populated when `-i` is on — the non-interactive (default + JSON)
// paths leave the Txs slice empty so disk + memory stay bounded.
type incomeExpenseTx struct {
	Date         string  // YYYY-MM-DD
	Timestamp    int64   // unix seconds, for tie-breaker sorts
	Counterparty string  // human-friendly other party
	AccountSlug  string  // resolved account (e.g. "stripe", "kbcbrussels")
	Amount       float64 // absolute EUR gross
	Currency     string
	Description  string // metadata.description (if any)
	URI          string // tx canonical URI for copy-paste / nostr lookup

	// Mutable annotation pair. Reflects the in-memory edit applied by
	// the TUI [e] hotkey so the drill view shows the new value
	// immediately. The Nostr annotation cache is the durable store;
	// `chb generate` re-runs rebucket the rows on the next read.
	Category   string
	Collective string

	// SignedAmount keeps the directional amount (positive ⇔ CREDIT
	// from the account's perspective, negative ⇔ DEBIT). Used by the
	// [r] reconcile flow to decide whether to surface invoice or
	// bill candidates.
	SignedAmount float64

	// ImportID is the unique_import_id this tx will (or did) get on
	// the Odoo bank statement line. Populated at collection time via
	// buildUniqueImportID(acc, tx); empty when the account isn't
	// configured. Used by the [r] reconcile flow to find the Odoo
	// line in the local journal cache.
	ImportID string
}

// incomeCategoryBucket aggregates transactions for one category over
// the selected date range. Exported only at package level so the TUI
// can render the same struct the JSON/CSV paths emit.
type incomeCategoryBucket struct {
	Category string            `json:"category"`
	Count    int               `json:"count"`
	Amount   float64           `json:"amount"`
	Txs      []incomeExpenseTx `json:"-"` // populated only in interactive mode
}

func runIncomeExpenseReport(direction string, args []string) error {
	if HasFlag(args, "--help", "-h", "help") {
		printIncomeExpenseHelp(direction)
		return nil
	}

	dateArg, ok := firstPositionalDateArg(args)
	if !ok {
		return fmt.Errorf("usage: chb %s <date-range> [--account <slug>]  (date-range: %s)", direction, DateRangeFormatHelp)
	}
	spec, ok := ParseDateRangeSpec(dateArg)
	if !ok {
		return fmt.Errorf("invalid date range %q (expected %s)", dateArg, DateRangeFormatHelp)
	}

	accountSlug := strings.TrimSpace(GetOption(args, "--account"))
	jsonOut := GetOption(args, "--format") == "json"
	interactive := HasFlag(args, "-i", "--interactive")

	dataDir := DataDir()

	type catBucket = incomeCategoryBucket
	type accountBucket struct {
		Slug   string  `json:"slug"`
		Name   string  `json:"name,omitempty"`
		Count  int     `json:"count"`
		Amount float64 `json:"amount"`
	}
	buckets := map[string]*catBucket{}
	accountBuckets := map[string]*accountBucket{}
	totalCount := 0
	totalAmount := 0.0

	// Resolve each tx's AccountSlug back to the configured slug. Stripe stores
	// the raw acct_… ID in tx.AccountSlug, so without this lookup every
	// Stripe payment shows up under a noisy 20-char key instead of "stripe".
	configuredAccounts := LoadAccountConfigs()
	slugIndex := map[string]string{}
	// accIndex by slug → *AccountConfig — used to build the
	// unique_import_id for the interactive [r] reconcile flow.
	accIndex := map[string]*AccountConfig{}
	for i := range configuredAccounts {
		acc := &configuredAccounts[i]
		slug := strings.ToLower(strings.TrimSpace(acc.Slug))
		if slug == "" {
			continue
		}
		slugIndex[slug] = acc.Slug
		accIndex[slug] = acc
		if acc.AccountID != "" {
			slugIndex[strings.ToLower(acc.AccountID)] = acc.Slug
			accIndex[strings.ToLower(acc.AccountID)] = acc
		}
		if acc.Address != "" {
			slugIndex[strings.ToLower(acc.Address)] = acc.Slug
			accIndex[strings.ToLower(acc.Address)] = acc
		}
	}
	resolveSlug := func(tx TransactionEntry) string {
		raw := strings.TrimSpace(tx.AccountSlug)
		if s, ok := slugIndex[strings.ToLower(raw)]; ok && s != "" {
			return s
		}
		if s, ok := slugIndex[strings.ToLower(strings.TrimSpace(tx.Account))]; ok && s != "" {
			return s
		}
		return raw
	}
	resolveAcc := func(tx TransactionEntry) *AccountConfig {
		if a, ok := accIndex[strings.ToLower(strings.TrimSpace(tx.AccountSlug))]; ok {
			return a
		}
		if a, ok := accIndex[strings.ToLower(strings.TrimSpace(tx.Account))]; ok {
			return a
		}
		return nil
	}

	months := ExpandMonthRange(spec.StartMonth, spec.EndMonth)
	for _, ym := range months {
		parts := strings.SplitN(ym, "-", 2)
		if len(parts) != 2 {
			continue
		}
		year, month := parts[0], parts[1]
		txFile := LoadTransactionsWithPII(dataDir, year, month)
		if txFile == nil {
			continue
		}
		for _, tx := range txFile.Transactions {
			if tx.Type == "INTERNAL" {
				continue
			}
			// Defensive: a tx tagged `category: internal_transfer` is
			// an internal transfer even if the rule that set its
			// category forgot to coerce the type. Categorizer.Apply
			// already coerces this at generate time, but old
			// generated files written before that fix still leak
			// through — skip on category too so the totals stay
			// honest regardless of when the file was generated.
			if strings.EqualFold(tx.Category, "internal_transfer") {
				continue
			}
			if !isEURCurrency(tx.Currency) {
				continue
			}
			if direction == "income" && !tx.IsIncoming() {
				continue
			}
			if direction == "expenses" && !tx.IsOutgoing() {
				continue
			}
			ts := time.Unix(tx.Timestamp, 0)
			if ts.Before(spec.Start) || !ts.Before(spec.End) {
				continue
			}
			if accountSlug != "" && !accountSlugMatchesTx(accountSlug, tx) {
				continue
			}
			amount := math.Abs(firstNonZeroFloat(tx.GrossAmount, tx.Amount, tx.NormalizedAmount, tx.NetAmount))
			if amount == 0 {
				continue
			}
			cat := strings.TrimSpace(tx.Category)
			if cat == "" {
				cat = "(uncategorized)"
			}
			b, ok := buckets[cat]
			if !ok {
				b = &catBucket{Category: cat}
				buckets[cat] = b
			}
			b.Count++
			b.Amount = roundReportAmount(b.Amount + amount)
			if interactive {
				signed := amount
				if tx.IsOutgoing() {
					signed = -amount
				}
				importID := ""
				if acc := resolveAcc(tx); acc != nil {
					importID = buildUniqueImportID(acc, tx)
				}
				b.Txs = append(b.Txs, incomeExpenseTx{
					Date:         time.Unix(tx.Timestamp, 0).In(BrusselsTZ()).Format("2006-01-02"),
					Timestamp:    tx.Timestamp,
					Counterparty: incomeExpenseTxCounterparty(tx),
					AccountSlug:  resolveSlug(tx),
					Amount:       amount,
					SignedAmount: signed,
					Currency:     tx.Currency,
					Description:  stringMetadata(tx.Metadata, "description"),
					URI:          tx.ID,
					Category:     tx.Category,
					Collective:   tx.Collective,
					ImportID:     importID,
				})
			}

			if accountSlug == "" {
				acctKey := resolveSlug(tx)
				if acctKey == "" {
					acctKey = "(no-account)"
				}
				ab, ok := accountBuckets[acctKey]
				if !ok {
					ab = &accountBucket{Slug: acctKey, Name: strings.TrimSpace(tx.AccountName)}
					accountBuckets[acctKey] = ab
				}
				ab.Count++
				ab.Amount = roundReportAmount(ab.Amount + amount)
				if ab.Name == "" {
					ab.Name = strings.TrimSpace(tx.AccountName)
				}
			}

			totalCount++
			totalAmount = roundReportAmount(totalAmount + amount)
		}
	}

	rows := make([]*catBucket, 0, len(buckets))
	for _, b := range buckets {
		rows = append(rows, b)
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Amount != rows[j].Amount {
			return rows[i].Amount > rows[j].Amount
		}
		return rows[i].Category < rows[j].Category
	})

	accountRows := make([]*accountBucket, 0, len(accountBuckets))
	for _, b := range accountBuckets {
		accountRows = append(accountRows, b)
	}
	sort.Slice(accountRows, func(i, j int) bool {
		if accountRows[i].Amount != accountRows[j].Amount {
			return accountRows[i].Amount > accountRows[j].Amount
		}
		return accountRows[i].Slug < accountRows[j].Slug
	})

	if interactive {
		// Sort each category's drill-down list newest-first by absolute
		// gross amount — that's what the drill view shows.
		for _, b := range rows {
			sort.SliceStable(b.Txs, func(i, j int) bool {
				return b.Txs[i].Amount > b.Txs[j].Amount
			})
		}
		runIncomeExpenseTUI(direction, formatIncomeExpenseRange(spec), accountSlug, rows, totalCount, totalAmount)
		return nil
	}

	if jsonOut {
		out := struct {
			Direction  string           `json:"direction"`
			StartDate  string           `json:"startDate"`
			EndDate    string           `json:"endDate"`
			Account    string           `json:"account,omitempty"`
			Count      int              `json:"count"`
			Amount     float64          `json:"amount"`
			Categories []*catBucket     `json:"categories"`
			Accounts   []*accountBucket `json:"accounts,omitempty"`
		}{
			Direction:  direction,
			StartDate:  spec.Start.Format("2006-01-02"),
			EndDate:    spec.End.Add(-time.Nanosecond).Format("2006-01-02"),
			Account:    accountSlug,
			Count:      totalCount,
			Amount:     totalAmount,
			Categories: rows,
			Accounts:   accountRows,
		}
		data, _ := json.MarshalIndent(out, "", "  ")
		fmt.Println(string(data))
		return nil
	}

	f := Fmt
	icon := "💰"
	titleColor := f.Green
	if direction == "expenses" {
		icon = "💸"
		titleColor = f.Red
	}

	rangeLabel := formatIncomeExpenseRange(spec)
	scope := "all accounts"
	if accountSlug != "" {
		scope = "account " + accountSlug
	}

	fmt.Printf("\n%s%s %s by category%s — %s%s%s  (%s)\n",
		f.Bold, icon, strings.Title(direction), f.Reset,
		titleColor, rangeLabel, f.Reset,
		scope,
	)
	if totalCount == 0 {
		fmt.Printf("  %sNo %s in this range.%s\n\n", f.Dim, direction, f.Reset)
		return nil
	}

	fmt.Printf("\n  %s%-30s %6s %16s%s\n", f.Dim, "CATEGORY", "TXS", "AMOUNT", f.Reset)
	for _, r := range rows {
		fmt.Printf("  %-30s %6d %16s\n",
			truncateCategory(r.Category, 30),
			r.Count,
			fmtEUR(r.Amount))
	}
	fmt.Printf("  %s%-30s %6d %16s%s\n",
		f.Bold, "TOTAL", totalCount, fmtEUR(totalAmount), f.Reset)

	if accountSlug == "" && len(accountRows) > 0 {
		fmt.Printf("\n  %s%-30s %6s %16s%s\n", f.Dim, "ACCOUNT", "TXS", "AMOUNT", f.Reset)
		for _, r := range accountRows {
			fmt.Printf("  %-30s %6d %16s\n",
				truncateCategory(accountDisplay(r.Slug, r.Name), 30),
				r.Count,
				fmtEUR(r.Amount))
		}
		fmt.Printf("  %s%-30s %6d %16s%s\n",
			f.Bold, "TOTAL", totalCount, fmtEUR(totalAmount), f.Reset)
	}

	fmt.Println()
	return nil
}

// accountDisplay renders an account row as "slug · name" when both are
// known and distinct, otherwise just the slug. AccountName carries the
// human-friendly emoji label (e.g. "💳 Stripe Account") while slug is
// the stable identifier used by --account.
func accountDisplay(slug, name string) string {
	slug = strings.TrimSpace(slug)
	name = strings.TrimSpace(name)
	if name == "" || strings.EqualFold(name, slug) {
		return slug
	}
	return slug + " · " + name
}

func truncateCategory(s string, max int) string {
	if len(s) <= max {
		return s
	}
	if max <= 3 {
		return s[:max]
	}
	return s[:max-1] + "…"
}

// incomeExpenseTxCounterparty returns ONLY the counterparty — empty
// when none. Description (metadata.description / memo) is kept in
// its own field so the TUI can render the two as distinct columns
// instead of collapsing them via a fallback.
func incomeExpenseTxCounterparty(tx TransactionEntry) string {
	return strings.TrimSpace(tx.Counterparty)
}

func formatIncomeExpenseRange(spec DateSpec) string {
	switch spec.Precision {
	case "day":
		return spec.Start.Format("2006-01-02")
	case "month":
		return spec.Start.Format("2006-01")
	case "year":
		return spec.Start.Format("2006")
	case "quarter":
		quarter := ((int(spec.Start.Month()) - 1) / 3) + 1
		return fmt.Sprintf("%d/Q%d", spec.Start.Year(), quarter)
	case "semester":
		semester := ((int(spec.Start.Month()) - 1) / 6) + 1
		return fmt.Sprintf("%d/S%d", spec.Start.Year(), semester)
	}
	endInclusive := spec.End.Add(-time.Nanosecond)
	return fmt.Sprintf("%s..%s", spec.Start.Format("2006-01-02"), endInclusive.Format("2006-01-02"))
}

func printIncomeExpenseHelp(direction string) {
	f := Fmt
	noun := "Income"
	verb := "received"
	icon := "💰"
	if direction == "expenses" {
		noun = "Expenses"
		verb = "paid"
		icon = "💸"
	}
	fmt.Printf(`
%s%s chb %s%s — %s breakdown per category

%sUSAGE%s
  %schb %s%s <date-range> [--account <slug>] [--format json]

%sARGUMENTS%s
  %s<date-range>%s         %s (e.g. 2025/11, 2025/Q4, 20250101-20250630)

%sOPTIONS%s
  %s-i%s, %s--interactive%s   Open the TUI: navigate categories, press
                          [enter] on one to drill into its transactions
                          (sorted by amount descending)
  %s--account <slug>%s     Filter to one configured account (default: all accounts)
  %s--format json%s        Output as JSON
  %s--help, -h%s           Show this help

%sNOTES%s
  • Only EUR-family currencies (EUR, EURe, EURb) are counted.
  • Internal transfers between accounts you own are excluded:
    Type=INTERNAL OR category="internal_transfer" both filter out.
  • Each row shows transactions %s in the date range, grouped by
    metadata.category. Rebuild via %schb generate%s if categories look stale.

%sEXAMPLES%s
  %s$ chb %s 2025                          # all of 2025
  $ chb %s 2025/Q1                       # Q1 2025
  $ chb %s 2025/11                       # November 2025
  $ chb %s 20250101-20250630             # custom range
  $ chb %s 2025 --account kbcbrussels    # only one account
  $ chb %s 2025 --format json            # JSON output%s
`,
		f.Bold, icon, direction, f.Reset, noun,
		f.Bold, f.Reset,
		f.Cyan, direction, f.Reset,
		f.Bold, f.Reset,
		f.Yellow, f.Reset, DateRangeFormatHelp,
		f.Bold, f.Reset,
		f.Yellow, f.Reset, f.Yellow, f.Reset, // -i, --interactive
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Bold, f.Reset,
		verb,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Dim,
		direction,
		direction,
		direction,
		direction,
		direction,
		direction,
		f.Reset,
	)
}
