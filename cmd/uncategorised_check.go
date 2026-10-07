package cmd

// After generate, warn about hub money left without a category: commonshub
// EUR/EURe transactions of the current and previous month (Brussels) that
// no rule, mapping or Odoo booking categorised. The website's money screens
// tag every line with its category. Internal and excluded rows are skipped.
// The warning names only date, amount, account and id (no counterparty).
//
// Membership is checked too (memberships carry no VAT, rentals 21%): a
// payment categorised membership of another amount than €10/€100/€200,
// and a customer invoice line that looks like a membership (membership
// product, membership name, account 704200) but carries VAT or another
// price, are flagged rather than passing silently.

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
	summary := []string{}
	if n := checkMembershipAnomalies(dataDir, now); n > 0 {
		summary = append(summary, Pluralize(n, "membership anomaly", "membership anomalies"))
	}
	if len(all) == 0 {
		if len(summary) > 0 {
			return strings.Join(summary, ", ")
		}
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
	return strings.Join(append([]string{Pluralize(len(all), "uncategorised transaction", "")}, summary...), ", ")
}

// membershipAnomalies lists, for the given months, membership payments of
// another amount than a membership price and membership-looking invoice
// lines with VAT or another price.
func membershipAnomalies(dataDir string, months []string) []string {
	inMonth := map[string]bool{}
	for _, ym := range months {
		inMonth[ym] = true
	}
	var out []string
	for _, ym := range months {
		data, err := os.ReadFile(audiencePath(dataDir, ym[:4], ym[5:], AudienceStewards, "transactions.json"))
		if err != nil {
			continue
		}
		var f TransactionsFile
		if json.Unmarshal(data, &f) != nil {
			continue
		}
		for _, tx := range f.Transactions {
			if tx.Category != "membership" || !isEURCurrency(tx.Currency) || tx.Amount == 0 {
				continue
			}
			if _, excluded := tx.Metadata["excluded"]; excluded {
				continue
			}
			gross := tx.GrossAmount
			if gross == 0 {
				gross = tx.Amount
			}
			if !isMembershipAmount(gross) {
				out = append(out, fmt.Sprintf("payment %s %+.2f %s %s (%s): not €10/€100/€200",
					time.Unix(tx.Timestamp, 0).In(BrusselsTZ()).Format("2006-01-02"), gross, tx.Currency, tx.AccountSlug, tx.ID))
			}
		}
	}
	var ids []int
	invoices := loadAllCachedInvoices(dataDir)
	for id := range invoices {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	for _, id := range ids {
		inv := invoices[id]
		date := firstNonEmpty(inv.InvoiceDate, inv.Date)
		if len(date) < 7 || !inMonth[date[:7]] || inv.State != "posted" || !strings.HasPrefix(inv.MoveType, "out_") {
			continue
		}
		for _, li := range inv.LineItems {
			if li.DisplayType != "" && li.DisplayType != "product" {
				continue
			}
			looksMembership := membershipProductIDs[li.ProductID] || li.AccountCode == "704200" ||
				strings.Contains(strings.ToLower(li.ProductName), "membership")
			if !looksMembership || li.SubtotalAmount < 0 {
				continue
			}
			name := firstNonEmpty(inv.Number, fmt.Sprintf("#%d", inv.ID))
			switch {
			case invoiceLineHasVAT(li):
				out = append(out, fmt.Sprintf("invoice %s %s line %q (account %s): membership-looking line with VAT", name, date, li.ProductName, li.AccountCode))
			case !isMembershipAmount(li.UnitPrice):
				out = append(out, fmt.Sprintf("invoice %s %s line %q (account %s): membership at %.2f, not €10/€100/€200", name, date, li.ProductName, li.AccountCode, li.UnitPrice))
			}
		}
	}
	return out
}

func checkMembershipAnomalies(dataDir string, now time.Time) int {
	now = now.In(BrusselsTZ())
	months := []string{now.AddDate(0, -1, 0).Format("2006-01"), now.Format("2006-01")}
	found := membershipAnomalies(dataDir, months)
	if len(found) == 0 {
		return 0
	}
	shown := found
	if len(shown) > 8 {
		shown = append(append([]string{}, shown[:8]...), "…")
	}
	Warnf("⚠ %s (this and last month; memberships are €10/€100/€200 without VAT): %s — fix the category or the invoice in Odoo",
		Pluralize(len(found), "membership anomaly", "membership anomalies"), strings.Join(shown, "; "))
	return len(found)
}
