package cmd

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	bitcoinsource "github.com/CommonsHub/chb/providers/bitcoin"
)

// syncBitcoinAccounts fetches the full confirmed history of every configured
// bitcoin address and archives the raw transactions per Brussels month under
// providers/bitcoin/<slug>.json. Months whose transaction set is unchanged
// are not rewritten. Returns the number of transactions in rewritten months.
func syncBitcoinAccounts(settings *Settings, slugFilter string, accountSyncMode bool) int {
	total := 0
	for _, acc := range settings.Finance.Accounts {
		if !strings.EqualFold(acc.Provider, bitcoinsource.Source) || acc.Address == "" {
			continue
		}
		if slugFilter != "" && !strings.EqualFold(acc.Slug, slugFilter) {
			continue
		}
		if !accountSyncMode {
			fmt.Printf("\n%sSyncing %s%s (%s)\n", Fmt.Bold, acc.Name, Fmt.Reset, acc.Address)
		}
		Progress(fmt.Sprintf("fetching Bitcoin transactions (%s)", acc.Name))
		txs, err := bitcoinsource.FetchAddressTransactions(acc.Address)
		if err != nil {
			Errorf("    %s✗ Bitcoin %s: %v%s", Fmt.Red, acc.Slug, err, Fmt.Reset)
			continue
		}
		byMonth := map[string][]json.RawMessage{}
		for _, raw := range txs {
			var tx bitcoinsource.Tx
			if json.Unmarshal(raw, &tx) != nil || tx.Status.BlockTime == 0 {
				continue
			}
			ym := time.Unix(tx.Status.BlockTime, 0).In(BrusselsTZ()).Format("2006-01")
			byMonth[ym] = append(byMonth[ym], raw)
		}
		saved := writeRawMonths(byMonth, func(year, month string) string {
			return bitcoinsource.Path(DataDir(), year, month, bitcoinsource.FileName(acc.Slug))
		}, func(path string, list []json.RawMessage) (bool, error) {
			if cached, ok := bitcoinsource.LoadCache(path); ok && bitcoinsource.SameTransactions(cached.Transactions, list) {
				return false, nil
			}
			return true, bitcoinsource.WriteCache(path, bitcoinsource.CacheFile{
				Address:      acc.Address,
				CachedAt:     time.Now().UTC().Format(time.RFC3339),
				Transactions: list,
			})
		})
		for _, n := range saved {
			total += n
		}
		if !accountSyncMode {
			printRawMonthsSaved(len(txs), "transaction", len(saved))
		}
	}
	return total
}

// writeRawMonths writes each month's raw list via write (which reports
// whether it actually wrote) and returns month → item count for the months
// that were rewritten.
func writeRawMonths(byMonth map[string][]json.RawMessage, pathFor func(year, month string) string, write func(path string, list []json.RawMessage) (bool, error)) map[string]int {
	months := make([]string, 0, len(byMonth))
	for ym := range byMonth {
		months = append(months, ym)
	}
	sort.Strings(months)
	saved := map[string]int{}
	for _, ym := range months {
		year, month := ym[:4], ym[5:]
		path := pathFor(year, month)
		wrote, err := write(path, byMonth[ym])
		if err != nil {
			Errorf("    %s✗ Failed to write %s: %v%s", Fmt.Red, filepath.Base(path), err, Fmt.Reset)
			continue
		}
		if wrote {
			saved[ym] = len(byMonth[ym])
		}
	}
	return saved
}

func printRawMonthsSaved(fetched int, noun string, savedMonths int) {
	fmt.Printf("    %sFetched %s%s\n", Fmt.Dim, Pluralize(fetched, noun, ""), Fmt.Reset)
	if savedMonths > 0 {
		fmt.Printf("    %s✓ Saved %s%s\n", Fmt.Green, Pluralize(savedMonths, "month", ""), Fmt.Reset)
	} else {
		fmt.Printf("    %s✓ Up to date%s\n", Fmt.Green, Fmt.Reset)
	}
}
