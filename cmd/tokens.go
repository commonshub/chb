package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	etherscansource "github.com/CommonsHub/chb/sources/etherscan"
)

// Tokens implements `chb tokens` — lists every token defined in
// settings/tokens.json with its on-chain coordinates plus the latest
// supply/holder/sync stats we have locally. With `sync` as the first arg,
// fetches the latest transactions for each token from Etherscan.
func Tokens(args []string) {
	if HasFlag(args, "--help", "-h", "help") {
		printTokensHelp()
		return
	}
	if len(args) > 0 && args[0] == "sync" {
		if err := TokensSync(args[1:]); err != nil {
			Errorf("%sError:%s %v", Fmt.Red, Fmt.Reset, err)
		}
		return
	}

	tokens := LoadTokenConfigs()
	if len(tokens) == 0 {
		fmt.Printf("\n%sNo tokens configured.%s\n\n", Fmt.Dim, Fmt.Reset)
		return
	}
	sort.Slice(tokens, func(i, j int) bool { return tokens[i].Slug < tokens[j].Slug })

	reportTokens := loadLatestReportTokens()
	syncState := LoadSyncState()
	dataDir := DataDir()

	fmt.Printf("\n%s🪙 Tokens%s (%d)\n\n", Fmt.Bold, Fmt.Reset, len(tokens))

	var latestSync time.Time
	for _, t := range tokens {
		header := fmt.Sprintf("  %s%s%s  %s%s%s", Fmt.Bold, t.Slug, Fmt.Reset, Fmt.Dim, t.Symbol, Fmt.Reset)
		if t.Name != "" {
			header += fmt.Sprintf("  %s%s%s", Fmt.Dim, t.Name, Fmt.Reset)
		}
		fmt.Println(header)

		if t.Address != "" {
			addrLabel := t.Address
			if t.Chain != "" {
				addrLabel = fmt.Sprintf("%s %s", t.Chain, t.Address)
			}
			url := txinfoTokenURL(t.Chain, t.Address)
			value := addrLabel
			if url != "" {
				value = hyperlink(url, addrLabel)
			}
			printAccountField("    ", "Address", value)
			if url != "" {
				printAccountField("    ", "URL", url)
			}
		}

		if rt := matchReportToken(reportTokens, t); rt != nil {
			printAccountField("    ", "Supply", formatTokenAmount(rt.TotalSupply, t.Symbol))
			printAccountField("    ", "Holders", fmt.Sprintf("%d (%d active in the past 90 days)", rt.TokenHolders, rt.ActiveTokenHolders))
		}

		stats := tokenTxStats(dataDir, t)
		if stats.Count > 0 {
			txValue := fmt.Sprintf("%s (from %s till %s)",
				formatThousands(stats.Count),
				stats.FirstTxAt.In(BrusselsTZ()).Format("2006-01-02"),
				stats.LastTxAt.In(BrusselsTZ()).Format("2006-01-02"))
			printAccountField("    ", "Total tx", txValue)
			printAccountField("    ", "Last tx", formatTimeAgoWithAbsolute(stats.LastTxAt))
		}

		if lastSync := tokenLastSync(syncState, t); !lastSync.IsZero() {
			printAccountField("    ", "Last sync", formatTimeAgoWithAbsolute(lastSync))
			if lastSync.After(latestSync) {
				latestSync = lastSync
			}
		}

		fmt.Println()
	}

	hint := "Run `chb tokens sync` to fetch the latest transactions."
	if !latestSync.IsZero() {
		hint = fmt.Sprintf("Last sync %s. Run `chb tokens sync` to fetch the latest transactions.",
			formatTimeAgo(latestSync))
	}
	fmt.Printf("  %s%s%s\n\n", Fmt.Dim, hint, Fmt.Reset)
}

// TokensSync runs the etherscan transaction sync for each token configured
// in settings/tokens.json. Delegates to TransactionsSync(--source=etherscan
// --slug=<token.slug>) so the sync path stays single-sourced.
func TokensSync(args []string) error {
	tokens := LoadTokenConfigs()
	if len(tokens) == 0 {
		fmt.Printf("\n%sNo tokens configured.%s\n\n", Fmt.Dim, Fmt.Reset)
		return nil
	}
	sort.Slice(tokens, func(i, j int) bool { return tokens[i].Slug < tokens[j].Slug })

	for _, t := range tokens {
		fmt.Printf("\n%s🪙 Syncing %s (%s) on %s%s\n", Fmt.Bold, t.Symbol, t.Name, t.Chain, Fmt.Reset)
		syncArgs := append([]string{"--source", "etherscan", "--slug", t.Slug}, args...)
		if _, err := TransactionsSync(syncArgs); err != nil {
			Warnf("  %s⚠ %s sync failed: %v%s", Fmt.Yellow, t.Slug, err, Fmt.Reset)
		}
	}
	return nil
}

func printTokensHelp() {
	f := Fmt
	fmt.Printf("\n%schb tokens%s — List configured tokens with on-chain stats\n\n", f.Bold, f.Reset)
	fmt.Printf("%sCOMMANDS%s\n\n", f.Bold, f.Reset)
	fmt.Printf("  %s%schb tokens%s\n", f.Bold, f.Cyan, f.Reset)
	fmt.Printf("    %sShow address, supply, holders, last tx, last sync for each token in settings/tokens.json%s\n\n", f.Dim, f.Reset)
	fmt.Printf("  %s%schb tokens sync%s\n", f.Bold, f.Cyan, f.Reset)
	fmt.Printf("    %sFetch the latest on-chain transactions for every configured token%s\n", f.Dim, f.Reset)
	fmt.Printf("    %s(equivalent to `chb transactions sync --source etherscan --slug <slug>` per token)%s\n\n", f.Dim, f.Reset)
}

func txinfoTokenURL(chain, address string) string {
	if chain == "" || address == "" {
		return ""
	}
	return fmt.Sprintf("https://txinfo.xyz/%s/token/%s", chain, address)
}

// loadLatestReportTokens reads DATA_DIR/latest/generated/report.json and
// returns its tokens slice, or nil when the file is missing or unreadable.
func loadLatestReportTokens() []MonthlyReportTokenData {
	path := filepath.Join(DataDir(), "latest", "generated", "summary.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var report MonthlyReportFile
	if json.Unmarshal(data, &report) != nil {
		return nil
	}
	return report.Tokens
}

// matchReportToken finds the report row for a given token config. Match
// preference: slug, then symbol, then address (case-insensitive).
func matchReportToken(report []MonthlyReportTokenData, t TokenConfig) *MonthlyReportTokenData {
	for i, rt := range report {
		if strings.EqualFold(rt.Slug, t.Slug) {
			return &report[i]
		}
	}
	for i, rt := range report {
		if strings.EqualFold(rt.Symbol, t.Symbol) {
			return &report[i]
		}
	}
	return nil
}

// tokenLastSync picks the most recent sync timestamp recorded under any of
// the keys the transaction-sync code might have used for this token.
func tokenLastSync(state *SyncState, t TokenConfig) time.Time {
	if state == nil || state.Accounts == nil {
		return time.Time{}
	}
	candidates := []string{t.Slug, strings.ToLower(t.Symbol)}
	var latest time.Time
	for _, key := range candidates {
		if key == "" {
			continue
		}
		entry := state.Accounts[key]
		if entry == nil || entry.LastSync == "" {
			continue
		}
		if ts, err := time.Parse(time.RFC3339, entry.LastSync); err == nil {
			if ts.After(latest) {
				latest = ts
			}
		}
	}
	return latest
}

// tokenTxStatsResult summarises the on-chain history we have cached locally
// for one token across every month under DATA_DIR/<year>/<month>/sources/
// etherscan/<chain>/<slug>.<SYMBOL>.json.
type tokenTxStatsResult struct {
	Count     int
	FirstTxAt time.Time
	LastTxAt  time.Time
}

// tokenTxStats walks the per-month Etherscan source files for the token and
// returns total transaction count + first/last timestamp. The aggregated
// latest/generated/transactions.json is intentionally NOT used — it covers
// only the rolling-window report period, so it lags behind the source files.
func tokenTxStats(dataDir string, t TokenConfig) tokenTxStatsResult {
	out := tokenTxStatsResult{}
	if t.Slug == "" || t.Symbol == "" || t.Chain == "" {
		return out
	}
	filename := etherscansource.FileName(t.Slug, t.Symbol)
	yearDirs, _ := os.ReadDir(dataDir)
	for _, yd := range yearDirs {
		if !yd.IsDir() || len(yd.Name()) != 4 {
			continue
		}
		monthDirs, _ := os.ReadDir(filepath.Join(dataDir, yd.Name()))
		for _, md := range monthDirs {
			if !md.IsDir() || len(md.Name()) != 2 {
				continue
			}
			path := etherscansource.Path(dataDir, yd.Name(), md.Name(), t.Chain, filename)
			cache, ok := etherscansource.LoadCache(path)
			if !ok {
				continue
			}
			for _, tx := range cache.Transactions {
				out.Count++
				if tx.TimeStamp == "" {
					continue
				}
				ts, err := strconv.ParseInt(tx.TimeStamp, 10, 64)
				if err != nil || ts <= 0 {
					continue
				}
				at := time.Unix(ts, 0)
				if out.FirstTxAt.IsZero() || at.Before(out.FirstTxAt) {
					out.FirstTxAt = at
				}
				if at.After(out.LastTxAt) {
					out.LastTxAt = at
				}
			}
		}
	}
	return out
}

// formatTokenAmount renders an integer-or-decimal token amount with its
// symbol, using thousands separators. Tokens use 2 decimals like the
// report.
func formatTokenAmount(amount float64, symbol string) string {
	return fmt.Sprintf("%s %s", fmtNumber(amount), symbol)
}
