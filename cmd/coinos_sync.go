package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	coinossource "github.com/CommonsHub/chb/providers/coinos"
)

// syncCoinosAccounts fetches the payment history of every configured coinos
// account (auth via COINOS_TOKEN, or COINOS_TOKEN_<SLUG> for several wallets)
// and archives the raw payments per Brussels month under
// providers/coinos/<slug>.json.
func syncCoinosAccounts(settings *Settings, slugFilter string, accountSyncMode bool) int {
	total := 0
	for _, acc := range settings.Finance.Accounts {
		if !strings.EqualFold(acc.Provider, coinossource.Source) {
			continue
		}
		if slugFilter != "" && !strings.EqualFold(acc.Slug, slugFilter) {
			continue
		}
		token := coinosToken(acc.Slug)
		if token == "" {
			Warnf("%s⚠ COINOS_TOKEN not set, skipping coinos sync (%s)%s", Fmt.Yellow, acc.Slug, Fmt.Reset)
			continue
		}
		if !accountSyncMode {
			fmt.Printf("\n%sSyncing %s%s (%s)\n", Fmt.Bold, acc.Name, Fmt.Reset, coinossource.ProfileURL(acc.AccountID))
		}
		Progress(fmt.Sprintf("fetching coinos payments (%s)", acc.Name))
		payments, err := coinossource.FetchPayments(token)
		if err != nil {
			Errorf("    %s✗ coinos %s: %v%s", Fmt.Red, acc.Slug, err, Fmt.Reset)
			continue
		}
		byMonth := map[string][]json.RawMessage{}
		for _, raw := range payments {
			var p coinossource.Payment
			if json.Unmarshal(raw, &p) != nil || p.Created == 0 {
				continue
			}
			ym := time.UnixMilli(p.Created).In(BrusselsTZ()).Format("2006-01")
			byMonth[ym] = append(byMonth[ym], raw)
		}
		saved := writeRawMonths(byMonth, func(year, month string) string {
			return coinossource.Path(DataDir(), year, month, coinossource.FileName(acc.Slug))
		}, func(path string, list []json.RawMessage) (bool, error) {
			if cached, ok := coinossource.LoadCache(path); ok && coinossource.SamePayments(cached.Payments, list) {
				return false, nil
			}
			return true, coinossource.WriteCache(path, coinossource.CacheFile{
				Username: acc.AccountID,
				CachedAt: time.Now().UTC().Format(time.RFC3339),
				Payments: list,
			})
		})
		for _, n := range saved {
			total += n
		}
		if !accountSyncMode {
			printRawMonthsSaved(len(payments), "payment", len(saved))
		}
	}
	return total
}

// coinosToken returns COINOS_TOKEN_<SLUG> when set, else COINOS_TOKEN.
func coinosToken(slug string) string {
	key := "COINOS_TOKEN_" + strings.ToUpper(strings.NewReplacer("-", "_", ".", "_").Replace(slug))
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return strings.TrimSpace(os.Getenv("COINOS_TOKEN"))
}
