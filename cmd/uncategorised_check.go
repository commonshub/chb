package cmd

// After generate, warn about hub money left without a category: commonshub
// EUR/EURe transactions of the current and previous month (Brussels) that
// no rule, mapping or Odoo booking categorised. The website's money screens
// tag every line with its category. Internal and excluded rows are skipped.
// The warning names only date, amount, account and id (no counterparty).

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
)

type uncategorisedTx struct {
	ID, Account, Currency string
	Amount                float64
	Timestamp             int64
}

func uncategorisedHubTransactions(dataDir, year, month string) []uncategorisedTx {
	data, err := os.ReadFile(audiencePath(dataDir, year, month, AudienceStewards, "transactions.json"))
	if err != nil {
		return nil
	}
	var f TransactionsFile
	if json.Unmarshal(data, &f) != nil {
		return nil
	}
	var out []uncategorisedTx
	for _, tx := range f.Transactions {
		if !isEURCurrency(tx.Currency) || tx.Type == "INTERNAL" || tx.Amount == 0 {
			continue
		}
		if tx.Collective != "commonshub" {
			continue
		}
		if _, excluded := tx.Metadata["excluded"]; excluded {
			continue
		}
		if strings.TrimSpace(tx.Category) != "" {
			continue
		}
		out = append(out, uncategorisedTx{ID: tx.ID, Account: tx.AccountSlug, Currency: tx.Currency, Amount: tx.Amount, Timestamp: tx.Timestamp})
	}
	return out
}

// checkUncategorisedHubTransactions warns and returns the step summary.
func checkUncategorisedHubTransactions(dataDir string, now time.Time) string {
	now = now.In(BrusselsTZ())
	var all []uncategorisedTx
	for _, d := range []time.Time{now.AddDate(0, -1, 0), now} {
		all = append(all, uncategorisedHubTransactions(dataDir, d.Format("2006"), d.Format("01"))...)
	}
	if len(all) == 0 {
		return "every hub transaction has a category"
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Timestamp > all[j].Timestamp })
	var ex []string
	for i, t := range all {
		if i == 5 {
			ex = append(ex, "…")
			break
		}
		ex = append(ex, fmt.Sprintf("%s %+.2f %s %s (%s)", time.Unix(t.Timestamp, 0).In(BrusselsTZ()).Format("2006-01-02"), t.Amount, t.Currency, t.Account, t.ID))
	}
	Warnf("⚠ %s without a category (commonshub EUR/EURe, this and last month): %s — add a rule (rules.json) or book it in Odoo",
		Pluralize(len(all), "transaction", ""), strings.Join(ex, "; "))
	return Pluralize(len(all), "uncategorised transaction", "")
}
