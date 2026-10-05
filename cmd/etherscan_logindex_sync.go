package cmd

// Pull step: give every multi-transfer transaction in the on-chain
// archives its real log index, from the transaction receipt (see
// providers/etherscan/receipts.go). Runs over every month file of every
// on-chain account, so older archives are backfilled once; a receipt is
// fetched only for transactions still missing an index.

import (
	"path/filepath"
	"strconv"
	"strings"

	etherscansource "github.com/CommonsHub/chb/providers/etherscan"
)

func archiveRPCURL(acc FinanceAccount) string {
	if acc.ChainID != 0 {
		if u := defaultRPCForChainID(acc.ChainID); u != "" {
			return u
		}
	}
	switch strings.ToLower(acc.Chain) {
	case "gnosis":
		return defaultRPCForChainID(100)
	case "polygon":
		return defaultRPCForChainID(137)
	case "celo":
		return defaultRPCForChainID(42220)
	}
	return ""
}

func enrichEtherscanAccountsLogIndexes(dataDir string, accounts []FinanceAccount) {
	total := 0
	for _, acc := range accounts {
		rpc := archiveRPCURL(acc)
		if rpc == "" || acc.Slug == "" || acc.Chain == "" {
			continue
		}
		pattern := filepath.Join(dataDir, "*", "*", etherscansource.RelPath(acc.Chain, acc.Slug+".*.json"))
		files, _ := filepath.Glob(pattern)
		for _, f := range files {
			cache, ok := etherscansource.LoadCache(f)
			if !ok || len(etherscansource.MultiTransferHashes(cache.Transactions)) == 0 {
				continue
			}
			n, err := etherscansource.EnrichLogIndexes(cache.Transactions, rpc)
			if err != nil {
				Warnf("  ⚠ %s: log indexes for %s: %v", acc.Slug, filepath.Base(f), err)
			}
			if n == 0 {
				continue
			}
			rel, _ := filepath.Rel(dataDir, f)
			parts := strings.Split(filepath.ToSlash(rel), "/")
			if len(parts) < 2 {
				continue
			}
			if err := etherscansource.WriteJSON(dataDir, parts[0], parts[1], acc.Chain, cache, filepath.Base(f)); err != nil {
				Warnf("  ⚠ %s: write %s: %v", acc.Slug, filepath.Base(f), err)
				continue
			}
			total += n
		}
	}
	if total > 0 {
		odooLog("    %s↳ log indexes from receipts: %s%s\n", Fmt.Dim, strconv.Itoa(total), Fmt.Reset)
	}
}
