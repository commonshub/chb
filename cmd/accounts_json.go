package cmd

import (
	"strings"
	"time"
)

// emitAccountsJSON is the JSON counterpart of Accounts.
func emitAccountsJSON(args []string, configs []AccountConfig) {
	summaries := computeAccountSummaries()
	refresh := HasFlag(args, "--refresh", "-r")

	cache := loadBalanceCache()
	var liveBalances map[string]float64
	var fetchedAt string
	if cache != nil && !refresh {
		liveBalances = cache.Balances
		fetchedAt = cache.FetchedAt
	} else {
		liveBalances = fetchLiveBalances(configs)
		fetchedAt = time.Now().UTC().Format(time.RFC3339)
	}

	odooStatuses := fetchOdooSyncStatuses(configs, summaries)
	faAccounts := ToFinanceAccounts(configs)

	out := AccountsJSON{
		Accounts:  make([]AccountJSON, 0, len(configs)),
		Totals:    map[string]float64{},
		FetchedAt: fetchedAt,
	}

	for i, acc := range configs {
		fa := faAccounts[i]
		s := summaries[accountKey(fa)]

		row := AccountJSON{
			Slug:      acc.Slug,
			Name:      acc.Name,
			Provider:  acc.Provider,
			Chain:     acc.Chain,
			Address:   acc.Address,
			AccountID: acc.AccountID,
			Currency:  accountCurrency(&acc),
		}

		if balance, source, ok := resolveAccountBalance(&acc, liveBalances, s); ok {
			b := balance
			row.Balance = &b
			row.BalanceSource = source
			out.Totals[row.Currency] += balance
		}

		if s != nil {
			row.TxCount = s.TxCount
			if !s.LastTxAt.IsZero() {
				row.LastTxAt = s.LastTxAt.UTC().Format(time.RFC3339)
			}
		}
		if last := LastSyncTime("account:" + strings.ToLower(acc.Slug)); !last.IsZero() {
			row.LastSyncAt = last.UTC().Format(time.RFC3339)
		}
		if acc.OdooJournalID > 0 {
			row.OdooJournalID = acc.OdooJournalID
			row.OdooJournalName = acc.OdooJournalName
			if status, ok := odooStatuses[acc.OdooJournalID]; ok {
				m := status.Missing
				row.OdooMissing = &m
				if !status.LastOdooTxDate.IsZero() {
					row.OdooLastTxDate = status.LastOdooTxDate.UTC().Format("2006-01-02")
				}
			}
		}

		out.Accounts = append(out.Accounts, row)
	}

	_ = EmitJSON(out)
}

// emitAccountDetailJSON is the JSON counterpart of AccountDetail.
func emitAccountDetailJSON(acc *AccountConfig, args []string) {
	summaries := computeAccountSummaries()
	fa := FinanceAccount{
		Provider:  acc.Provider,
		Chain:     acc.Chain,
		Address:   acc.Address,
		AccountID: acc.AccountID,
		Slug:      acc.Slug,
	}
	s := summaries[accountKey(fa)]

	if HasFlag(args, "--refresh", "-r") {
		if v, key, err := refreshAccountBalance(acc); err == nil && key != "" {
			cache := loadBalanceCache()
			if cache == nil {
				cache = &balanceCache{Balances: map[string]float64{}}
			}
			if cache.Balances == nil {
				cache.Balances = map[string]float64{}
			}
			cache.Balances[key] = v
			cache.FetchedAt = time.Now().UTC().Format(time.RFC3339)
			saveBalanceCache(cache)
		}
	}

	row := AccountJSON{
		Slug:      acc.Slug,
		Name:      acc.Name,
		Provider:  acc.Provider,
		Chain:     acc.Chain,
		Address:   acc.Address,
		AccountID: acc.AccountID,
		Currency:  accountCurrency(acc),
	}

	cache := loadBalanceCache()
	var liveBalances map[string]float64
	if cache != nil {
		liveBalances = cache.Balances
	}
	if balance, source, ok := resolveAccountBalance(acc, liveBalances, s); ok {
		b := balance
		row.Balance = &b
		row.BalanceSource = source
	}

	if s != nil {
		row.TxCount = s.TxCount
		if !s.LastTxAt.IsZero() {
			row.LastTxAt = s.LastTxAt.UTC().Format(time.RFC3339)
		}
	}
	if last := LastSyncTime("account:" + strings.ToLower(acc.Slug)); !last.IsZero() {
		row.LastSyncAt = last.UTC().Format(time.RFC3339)
	}
	if acc.OdooJournalID > 0 {
		row.OdooJournalID = acc.OdooJournalID
		row.OdooJournalName = acc.OdooJournalName
	}

	_ = EmitJSON(row)
}

func accountCurrency(acc *AccountConfig) string {
	if acc.Currency != "" {
		return acc.Currency
	}
	if acc.Token != nil {
		return acc.Token.Symbol
	}
	return "EUR"
}

// resolveAccountBalance mirrors the lookup priority used by the pretty-print
// list and detail views: live cached balance first, then tx-history-derived.
func resolveAccountBalance(acc *AccountConfig, liveBalances map[string]float64, s *accountSummary) (float64, string, bool) {
	for _, key := range []string{acc.Address, acc.AccountID, acc.Slug} {
		if key == "" {
			continue
		}
		if v, ok := liveBalances[strings.ToLower(key)]; ok {
			return v, "live", true
		}
	}
	if s != nil && s.TxCount > 0 {
		return s.Balance, "tx-history", true
	}
	return 0, "", false
}
