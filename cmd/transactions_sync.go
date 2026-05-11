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

	"github.com/CommonsHub/chb/sources"
	etherscansource "github.com/CommonsHub/chb/sources/etherscan"
	moneriumsource "github.com/CommonsHub/chb/sources/monerium"
	nostrsource "github.com/CommonsHub/chb/sources/nostr"
	stripesource "github.com/CommonsHub/chb/sources/stripe"
)

// EtherscanResponse represents the Etherscan V2 API response
type EtherscanResponse struct {
	Status  string            `json:"status"`
	Message string            `json:"message"`
	Result  []json.RawMessage `json:"result"`
}

type TokenTransfer = etherscansource.TokenTransfer
type TransactionsCacheFile = etherscansource.CacheFile

func TransactionsSync(args []string) (int, error) {
	startedAt := time.Now()
	if HasFlag(args, "--help", "-h", "help") {
		printTransactionsSyncHelp()
		return 0, nil
	}

	settings, err := LoadSettings()
	if err != nil {
		return 0, fmt.Errorf("failed to load settings: %w", err)
	}

	force := HasFlag(args, "--force")
	noNostr := HasFlag(args, "--no-nostr")
	monthFilter := GetOption(args, "--month")
	sourceFilter := strings.ToLower(GetOption(args, "--source"))
	slugFilter := strings.ToLower(GetOption(args, "--slug"))

	// --limit N: only fetch the N most recent transactions (applies to Stripe)
	limitStr := GetOption(args, "--limit")
	var fetchLimit int
	if limitStr != "" {
		fmt.Sscanf(limitStr, "%d", &fetchLimit)
	}

	// Positional year/month arg (e.g. "2025" or "2025/03")
	posYear, posMonth, posFound := ParseYearMonthArg(args)

	// Determine which months to process
	now := time.Now().In(BrusselsTZ())
	var startMonth, endMonth string

	// Check --since / --history first
	sinceMonth, isSince := ResolveSinceMonth(args, etherscansource.RelPath(""))
	isFullSync := isSince
	lastSyncTime := LastSyncTime("transactions")

	if isSince {
		startMonth = sinceMonth
		endMonth = fmt.Sprintf("%d-%02d", now.Year(), now.Month())
	} else if posFound {
		if posMonth != "" {
			startMonth = fmt.Sprintf("%s-%s", posYear, posMonth)
			endMonth = startMonth
		} else {
			startMonth = fmt.Sprintf("%s-01", posYear)
			endMonth = fmt.Sprintf("%s-12", posYear)
		}
	} else if monthFilter != "" {
		startMonth = monthFilter
		endMonth = monthFilter
	} else {
		// Default: keep syncs bounded to the recent window.
		endMonth = fmt.Sprintf("%d-%02d", now.Year(), now.Month())
		startMonth = DefaultRecentStartMonth(now)
	}

	fmt.Printf("\n%sSyncing data...%s\n", Fmt.Bold, Fmt.Reset)
	if sourceFilter != "" {
		fmt.Printf("  Source: %s\n", sourceFilter)
	}
	if slugFilter != "" {
		fmt.Printf("  Account: %s\n", slugFilter)
	}
	fmt.Printf("  Date range: %s -> %s\n", startMonth, endMonth)
	fmt.Printf("  Data dir: %s\n", DataDir())
	fmt.Println()

	totalProcessed := 0

	// --- Etherscan / blockchain sync ---
	if sourceFilter == "" || sourceFilter == "gnosis" || sourceFilter == "celo" || sourceFilter == "etherscan" || sourceFilter == "blockchain" {
		etherscanAccounts := make([]FinanceAccount, 0)
		for _, acc := range settings.Finance.Accounts {
			if acc.Provider == "etherscan" && acc.Token != nil {
				if slugFilter != "" && !strings.EqualFold(acc.Slug, slugFilter) {
					continue
				}
				etherscanAccounts = append(etherscanAccounts, acc)
			}
		}

		// Auto-include the contribution token (CHT) if present in settings
		if settings.ContributionToken != nil {
			ct := settings.ContributionToken
			// Only add if not already tracked (by token address match)
			found := false
			for _, acc := range etherscanAccounts {
				if acc.Token != nil && strings.EqualFold(acc.Token.Address, ct.Address) {
					found = true
					break
				}
			}
			if !found && (slugFilter == "" || strings.EqualFold(slugFilter, ct.Symbol)) {
				etherscanAccounts = append(etherscanAccounts, FinanceAccount{
					Name:     "🪙 " + ct.Name,
					Slug:     strings.ToLower(ct.Symbol),
					Provider: "etherscan",
					Chain:    ct.Chain,
					ChainID:  ct.ChainID,
					Address:  "", // empty = fetch all transfers for this token
					Currency: ct.Symbol,
					Token: &struct {
						Address  string `json:"address"`
						Name     string `json:"name"`
						Symbol   string `json:"symbol"`
						Decimals int    `json:"decimals"`
					}{
						Address:  ct.Address,
						Name:     ct.Name,
						Symbol:   ct.Symbol,
						Decimals: ct.Decimals,
					},
				})
			}
		}

		if len(etherscanAccounts) > 0 {
			apiKey := os.Getenv("ETHERSCAN_API_KEY")
			if apiKey == "" {
				apiKey = os.Getenv("GNOSISSCAN_API_KEY")
			}
			if apiKey == "" {
				Warnf("%s⚠ ETHERSCAN_API_KEY not set, skipping blockchain sync%s", Fmt.Yellow, Fmt.Reset)
			} else {
				fmt.Printf("%s⛓️  Syncing blockchain transactions%s\n\n", Fmt.Bold, Fmt.Reset)
				for _, acc := range etherscanAccounts {
					fmt.Printf("  %s%s%s (%s/%s)\n", Fmt.Bold, acc.Name, Fmt.Reset, acc.Chain, acc.Token.Symbol)

					// Check if we can skip the full fetch by peeking at the latest tx
					if !force {
						filename := etherscansource.FileName(acc.Slug, acc.Token.Symbol)
						peekHash, peekErr := etherscansource.PeekLatest(etherscanAccount(acc), apiKey)
						if peekErr == nil {
							cachedLatest := etherscansource.LatestCachedTxHashGlobal(DataDir(), acc.Chain, filename)
							if cachedLatest == "" {
								cachedLatest = readLastPeekHash(DataDir(), acc.Chain, acc.Slug+"."+acc.Token.Symbol)
							}
							if peekHash == cachedLatest {
								// Peek matches, but only skip if we're not missing data for months in range.
								// Etherscan accounts may have data in months we haven't cached yet.
								relPathFn := func(year, month string) string {
									return etherscansource.RelPath(acc.Chain, filename)
								}
								if peekHash == "" || allMonthsCached(DataDir(), startMonth, endMonth, relPathFn) {
									fmt.Printf("    %s✓ Up to date%s\n", Fmt.Green, Fmt.Reset)
									time.Sleep(400 * time.Millisecond)
									continue
								}
							}
						}
					}

					transfers, err := etherscansource.FetchTokenTransfers(etherscanAccount(acc), apiKey)
					if err != nil {
						Errorf("    %s✗ Error: %v%s", Fmt.Red, err, Fmt.Reset)
						continue
					}

					fmt.Printf("    %sFetched %d total transfers%s\n", Fmt.Dim, len(transfers), Fmt.Reset)

					// Group by month
					byMonth := etherscansource.GroupByMonth(transfers, BrusselsTZ())

					saved := 0
					for ym, monthTxs := range byMonth {
						if ym < startMonth || ym > endMonth {
							continue
						}

						parts := strings.Split(ym, "-")
						if len(parts) != 2 {
							continue
						}
						year, month := parts[0], parts[1]

						// Save to data/YYYY/MM/sources/etherscan/{chain}/{slug}.{token}.json
						dataDir := DataDir()
						filename := etherscansource.FileName(acc.Slug, acc.Token.Symbol)
						relPath := etherscansource.RelPath(acc.Chain, filename)
						filePath := filepath.Join(dataDir, year, month, relPath)

						// Skip if exists and not force
						if !force && fileExists(filePath) {
							// But always update current month
							if ym != fmt.Sprintf("%d-%02d", now.Year(), now.Month()) {
								continue
							}
						}

						cache := TransactionsCacheFile{
							Transactions: monthTxs,
							CachedAt:     time.Now().UTC().Format(time.RFC3339),
							Account:      acc.Address,
							Chain:        acc.Chain,
							Token:        acc.Token.Symbol,
						}

						if err := etherscansource.WriteJSON(dataDir, year, month, acc.Chain, cache, filename); err != nil {
							Errorf("    %s✗ Failed to write: %v%s", Fmt.Red, err, Fmt.Reset)
							continue
						}

						saved++
						totalProcessed += len(monthTxs)
					}

					if saved > 0 {
						fmt.Printf("    %s✓ Saved %d months%s\n", Fmt.Green, saved, Fmt.Reset)
					}

					// Fetch Nostr metadata for all transfers.
					// Tx annotations are append-only → safe to filter by `since`.
					// Address profiles mutate → always pull the full set.
					if !noNostr && acc.ChainID != 0 && len(transfers) > 0 {
						fmt.Printf("    %sFetching Nostr metadata...%s", Fmt.Dim, Fmt.Reset)
						var nostrSince *time.Time
						if !force && !isSince && !posFound && monthFilter == "" && !lastSyncTime.IsZero() {
							nostrSince = &lastSyncTime
							fmt.Printf(" %s(since %s)%s", Fmt.Dim, lastSyncTime.In(BrusselsTZ()).Format(time.RFC3339), Fmt.Reset)
						}
						txHashes := make([]string, 0, len(transfers))
						addressSet := map[string]struct{}{}
						for _, tx := range transfers {
							txHashes = append(txHashes, tx.Hash)
							addressSet[strings.ToLower(tx.From)] = struct{}{}
							addressSet[strings.ToLower(tx.To)] = struct{}{}
						}
						addresses := make([]string, 0, len(addressSet))
						for a := range addressSet {
							addresses = append(addresses, a)
						}

						txMeta, txErr := FetchNostrTxMetadata(acc.ChainID, txHashes, nostrSince)
						addrMeta, addrErr := FetchNostrAddressMetadata(acc.ChainID, addresses)
						if txErr != nil || addrErr != nil {
							Errorf(" %s✗ tx=%v addr=%v%s", Fmt.Red, txErr, addrErr, Fmt.Reset)
						} else {
							fmt.Printf(" %s✓ %d tx, %d address annotations%s\n", Fmt.Green, len(txMeta), len(addrMeta), Fmt.Reset)
							saveNostrMetadataLayers(acc.ChainID, transfers, startMonth, endMonth, txMeta, addrMeta)
						}
					}

					// Save the latest tx hash so we can skip next time if nothing changed
					if len(transfers) > 0 {
						writeLastPeekHash(DataDir(), acc.Chain, acc.Slug+"."+acc.Token.Symbol, transfers[0].Hash)
					}

					// Rate limit between accounts
					time.Sleep(400 * time.Millisecond)
				}
			}
		}
	}

	// --- Stripe sync ---
	if sourceFilter == "" || sourceFilter == "stripe" {
		stripeAccounts := make([]FinanceAccount, 0)
		for _, acc := range settings.Finance.Accounts {
			if acc.Provider == "stripe" {
				if slugFilter != "" && !strings.EqualFold(acc.Slug, slugFilter) {
					continue
				}
				stripeAccounts = append(stripeAccounts, acc)
			}
		}

		if len(stripeAccounts) > 0 {
			stripeKey := os.Getenv("STRIPE_SECRET_KEY")
			if stripeKey == "" {
				Warnf("%s⚠ STRIPE_SECRET_KEY not set, skipping Stripe sync%s", Fmt.Yellow, Fmt.Reset)
			} else {
				for _, acc := range stripeAccounts {
					printSyncVariablesIfNeeded(sourceFilter, slugFilter, "stripe", acc)
					status := newStatusLine()

					// Check if we can skip the full fetch. For ranges that include
					// the current month, do not trust a latest-ID peek alone: the
					// cache can have Stripe's newest BT while still missing earlier
					// same-month BTs from a partial incremental run. Let the normal
					// backward fetch below compare/merge month contents instead.
					if !force {
						relPathFn := func(year, month string) string {
							return stripesource.RelPath(stripesource.BalanceTransactionsFile)
						}
						if allMonthsCached(DataDir(), startMonth, endMonth, relPathFn) {
							if !monthRangeIncludes(time.Now().In(BrusselsTZ()).Format("2006-01"), startMonth, endMonth) {
								fmt.Printf("  %sAll requested Stripe source files are already cached%s\n", Fmt.Dim, Fmt.Reset)
								continue
							}
						}
					}

					// Stripe balance transactions must be reconciled against the
					// local archive, not against a wall-clock "last sync" timestamp.
					// That timestamp is shared with other transaction sources and can
					// advance even when this Stripe account did not get a complete
					// archive update. Fetch from the requested recent range and stop
					// once we reach a cached month whose transaction count matches.
					stopAtMonthBoundary := !force && !isSince && !posFound && monthFilter == ""
					status.Update("Fetching transactions from Stripe...")
					stripeTxs, err := stripesource.FetchTransactions(stripesource.FetchOptions{
						APIKey:              stripeKey,
						AccountID:           acc.AccountID,
						StartMonth:          startMonth,
						EndMonth:            endMonth,
						Limit:               fetchLimit,
						StopAtMonthBoundary: stopAtMonthBoundary,
						DataDir:             DataDir(),
						Location:            BrusselsTZ(),
						Progress:            stripeTransactionProgress(status),
					})
					status.Clear()
					if err != nil {
						Errorf("  %s✗ Error: %v%s", Fmt.Red, err, Fmt.Reset)
						continue
					}
					status.Update("%d transactions fetched from Stripe", len(stripeTxs))

					// Group by month and determine which months actually changed.
					byMonth := stripesource.GroupTransactionsByMonth(stripeTxs, BrusselsTZ())
					monthsToUpdate := map[string]bool{}
					for ym, monthTxs := range byMonth {
						if ym < startMonth || ym > endMonth {
							continue
						}
						parts := strings.Split(ym, "-")
						if len(parts) != 2 {
							continue
						}
						year, month := parts[0], parts[1]
						dataDir := DataDir()
						filePath := stripesource.TransactionCachePath(dataDir, year, month)

						if force || ym == fmt.Sprintf("%d-%02d", now.Year(), now.Month()) || !fileExists(filePath) {
							monthsToUpdate[ym] = true
							continue
						}
						if stripesource.LocalTransactionCount(filePath) != len(monthTxs) {
							monthsToUpdate[ym] = true
						}
					}

					if len(monthsToUpdate) == 0 {
						status.Clear()
						fmt.Printf("  %sNo Stripe source files changed%s\n", Fmt.Dim, Fmt.Reset)
					}

					updatedMonths := sortedTrueMonths(monthsToUpdate)
					printedMonths := map[string]bool{}
					for _, ym := range updatedMonths {
						monthTxs := byMonth[ym]

						parts := strings.Split(ym, "-")
						if len(parts) != 2 {
							continue
						}
						year, month := parts[0], parts[1]

						dataDir := DataDir()
						if !force {
							if existing, ok := stripesource.LoadCache(stripesource.TransactionCachePath(dataDir, year, month)); ok {
								monthTxs = stripesource.MergeTransactions(existing.Transactions, monthTxs)
							}
						}

						cache := stripesource.CacheFile{
							Transactions: monthTxs,
							CachedAt:     time.Now().UTC().Format(time.RFC3339),
							AccountID:    acc.AccountID,
							Currency:     acc.Currency,
						}

						relPath := stripesource.RelPath(stripesource.BalanceTransactionsFile)
						status.Update("Writing %s...", displayMonthRelPath(year, month, relPath))
						if err := stripesource.WriteJSON(dataDir, year, month, cache, stripesource.BalanceTransactionsFile); err != nil {
							status.Clear()
							Errorf("  %s✗ Failed to write Stripe source data: %v%s", Fmt.Red, err, Fmt.Reset)
							continue
						}
						status.Clear()
						printMonthHeadingOnce(ym, printedMonths)
						fmt.Printf("  %s✓%s %s (%d transactions)\n", Fmt.Green, Fmt.Reset, filepath.ToSlash(relPath), len(monthTxs))

						totalProcessed += len(monthTxs)
					}

					// Fetch Stripe charge details (customer name, app, metadata) only for months being updated.
					if len(monthsToUpdate) == 0 {
						continue
					}
					status.Update("Resolving Stripe refunds and charge IDs...")
					var chargeIDs []string
					chargeByTxn := map[string]string{} // txn_id → ch_id for month grouping
					refundLookups := 0
					for _, tx := range stripeTxs {
						txMonth := time.Unix(tx.Created, 0).In(BrusselsTZ()).Format("2006-01")
						if !monthsToUpdate[txMonth] {
							continue
						}
						chID := stripesource.ExtractChargeID(tx.Source)
						if chID != "" {
							chargeIDs = append(chargeIDs, chID)
							chargeByTxn[tx.ID] = chID
						} else {
							// For refunds, resolve to original charge
							srcID := stripesource.ExtractSourceID(tx.Source)
							if strings.HasPrefix(srcID, "re_") {
								refundLookups++
								status.Update("Resolving Stripe refund sources... %d", refundLookups)
								origCharge := stripesource.FetchRefundChargeID(stripeKey, acc.AccountID, srcID)
								if origCharge != "" {
									chargeIDs = append(chargeIDs, origCharge)
									chargeByTxn[tx.ID] = origCharge
								}
								time.Sleep(100 * time.Millisecond)
							}
						}
					}
					// Build refund→charge mapping
					refundToCharge := map[string]string{}
					for txnID, chID := range chargeByTxn {
						// Find the source ID for this txn to check if it's a refund
						for _, tx := range stripeTxs {
							if tx.ID == txnID {
								srcID := stripesource.ExtractSourceID(tx.Source)
								if strings.HasPrefix(srcID, "re_") {
									refundToCharge[srcID] = chID
								}
								break
							}
						}
					}

					status.Update("Fetching %d Stripe charge records...", len(chargeIDs))
					charges, err := stripesource.FetchChargesWithProgress(stripeKey, acc.AccountID, chargeIDs, stripeChargeProgress(status))
					if err != nil {
						status.Clear()
						Errorf("  %s✗ %v%s", Fmt.Red, err, Fmt.Reset)
					} else {
						// Save per month
						for _, ym := range updatedMonths {
							monthTxs := byMonth[ym]
							monthCharges := map[string]*stripesource.Charge{}
							for _, tx := range monthTxs {
								chID := chargeByTxn[tx.ID]
								if ch, ok := charges[chID]; ok {
									monthCharges[chID] = ch
								}
							}
							if len(monthCharges) > 0 {
								parts := strings.Split(ym, "-")
								if len(parts) == 2 {
									relPath := stripesource.RelPath(stripesource.ChargesFile)
									status.Update("Writing %s...", displayMonthRelPath(parts[0], parts[1], relPath))
									_ = stripesource.SaveChargeData(DataDir(), parts[0], parts[1], monthCharges, refundToCharge)
									status.Clear()
									printMonthHeadingOnce(ym, printedMonths)
									fmt.Printf("  %s✓%s %s (%d charges)\n", Fmt.Green, Fmt.Reset, filepath.ToSlash(relPath), len(monthCharges))
								}
							}
						}

						// Save per-month private customer data from enriched charges.
						for _, ym := range updatedMonths {
							monthTxs := byMonth[ym]
							customers := &stripesource.CustomerData{
								FetchedAt: time.Now().UTC().Format(time.RFC3339),
								Customers: map[string]*stripesource.CustomerPII{},
							}
							for _, tx := range monthTxs {
								chID := chargeByTxn[tx.ID]
								if ch, ok := charges[chID]; ok {
									name := ch.BestName()
									email := ch.BestEmail()
									if name != "" || email != "" {
										customers.Customers[tx.ID] = &stripesource.CustomerPII{Name: name, Email: email}
									}
								}
							}
							if len(customers.Customers) > 0 {
								parts := strings.Split(ym, "-")
								if len(parts) == 2 {
									relPath := stripesource.RelPath(stripesource.CustomersFile)
									status.Update("Writing %s...", displayMonthRelPath(parts[0], parts[1], relPath))
									_ = stripesource.WriteJSON(DataDir(), parts[0], parts[1], customers, stripesource.CustomersFile)
									status.Clear()
									printMonthHeadingOnce(ym, printedMonths)
									fmt.Printf("  %s✓%s %s (%d customers)\n", Fmt.Green, Fmt.Reset, filepath.ToSlash(relPath), len(customers.Customers))
								}
							}
						}
					}
					status.Clear()
				}
			}
		}
	}

	// --- Monerium sync ---
	// Also auto-include EURe etherscan accounts (Monerium mints/redeems happen on-chain)
	if sourceFilter == "" || sourceFilter == "monerium" || sourceFilter == "gnosis" {
		moneriumAccounts := make([]FinanceAccount, 0)
		for _, acc := range settings.Finance.Accounts {
			if slugFilter != "" && !strings.EqualFold(acc.Slug, slugFilter) {
				continue
			}
			if acc.Provider == "monerium" {
				moneriumAccounts = append(moneriumAccounts, acc)
			}
			// Auto-include EURe blockchain accounts for Monerium enrichment
			if acc.Provider == "etherscan" && acc.Address != "" && acc.Token != nil &&
				strings.EqualFold(acc.Token.Symbol, "EURe") {
				moneriumAccounts = append(moneriumAccounts, FinanceAccount{
					Name:     acc.Name + " (Monerium)",
					Slug:     acc.Slug,
					Provider: "monerium",
					Address:  acc.Address,
					Currency: "EURe",
				})
			}
		}

		if len(moneriumAccounts) > 0 {
			clientID := os.Getenv("MONERIUM_CLIENT_ID")
			clientSecret := os.Getenv("MONERIUM_CLIENT_SECRET")
			moneriumEnv := os.Getenv("MONERIUM_ENV")
			if moneriumEnv == "" {
				moneriumEnv = "production"
			}

			if clientID == "" || clientSecret == "" {
				Warnf("%s⚠ MONERIUM_CLIENT_ID/MONERIUM_CLIENT_SECRET not set, skipping Monerium sync%s", Fmt.Yellow, Fmt.Reset)
			} else {
				fmt.Printf("\n%s🏦 Syncing Monerium orders%s\n\n", Fmt.Bold, Fmt.Reset)

				token, err := moneriumsource.Authenticate(clientID, clientSecret, moneriumEnv)
				if err != nil {
					Errorf("  %s✗ Auth failed: %v%s", Fmt.Red, err, Fmt.Reset)
				} else {
					for _, acc := range moneriumAccounts {
						fmt.Printf("  %s%s%s (%s)\n", Fmt.Bold, acc.Name, Fmt.Reset, acc.Address)

						orders, err := moneriumsource.FetchOrders(token, acc.Address, moneriumEnv)
						if err != nil {
							Errorf("    %s✗ Error: %v%s", Fmt.Red, err, Fmt.Reset)
							continue
						}

						fmt.Printf("    %sFetched %d orders%s\n", Fmt.Dim, len(orders), Fmt.Reset)

						// Check if latest order matches cache — skip if no new data
						slug := acc.Slug
						if slug == "" {
							slug = acc.Address[:8]
						}
						filename := moneriumsource.FileName(slug)
						if !force && len(orders) > 0 {
							relPathFn := func(year, month string) string {
								return moneriumsource.RelPath(filename)
							}
							if allMonthsCached(DataDir(), startMonth, endMonth, relPathFn) {
								cachedPath := currentMonthCacheFile(DataDir(), relPathFn)
								if orders[0].ID == moneriumsource.LatestCachedOrderID(cachedPath) {
									fmt.Printf("    %s✓ Up to date%s\n", Fmt.Green, Fmt.Reset)
									continue
								}
							}
						}

						// Group by month
						byMonth := moneriumsource.GroupByMonth(orders, BrusselsTZ())
						saved := 0

						for ym, monthOrders := range byMonth {
							if ym < startMonth || ym > endMonth {
								continue
							}

							parts := strings.Split(ym, "-")
							if len(parts) != 2 {
								continue
							}
							year, month := parts[0], parts[1]

							dataDir := DataDir()
							relPath := moneriumsource.RelPath(filename)
							filePath := filepath.Join(dataDir, year, month, relPath)

							if !force && fileExists(filePath) {
								if ym != fmt.Sprintf("%d-%02d", now.Year(), now.Month()) {
									continue
								}
							}

							cache := moneriumsource.CacheFile{
								Orders:   monthOrders,
								CachedAt: time.Now().UTC().Format(time.RFC3339),
								Address:  acc.Address,
							}

							if err := moneriumsource.WriteJSON(dataDir, year, month, cache, filename); err != nil {
								Errorf("    %s✗ Failed to write: %v%s", Fmt.Red, err, Fmt.Reset)
								continue
							}

							saved++
							totalProcessed += len(monthOrders)
						}

						if saved > 0 {
							fmt.Printf("    %s✓ Saved %d months%s\n", Fmt.Green, saved, Fmt.Reset)
						}
					}
				}
			}
		}
	}

	elapsed := time.Since(startedAt).Round(time.Millisecond)
	fmt.Printf("\n%s✓ Source sync complete%s: %d transaction(s), %s\n\n", Fmt.Green, Fmt.Reset, totalProcessed, elapsed)
	UpdateSyncSource("transactions", isFullSync)
	UpdateSyncActivity(isFullSync)
	return totalProcessed, nil
}

func fetchTokenTransfers(acc FinanceAccount, apiKey string) ([]TokenTransfer, error) {
	return etherscansource.FetchTokenTransfers(etherscanAccount(acc), apiKey)
}

// saveNostrMetadataLayers writes Nostr metadata to two layers:
//   - per-month files (filtered to txs/addresses involved in that month) — frozen
//     snapshots so re-reading any month gives what was known at sync time.
//   - data/latest/sources/nostr/<chainID>/metadata.json — timeless union across
//     every chain ever synced.
//
// Both writes merge into existing entries by createdAt so concurrent accounts
// on the same chain don't clobber each other.
func saveNostrMetadataLayers(chainID int, transfers []TokenTransfer, startMonth, endMonth string,
	txMeta map[string]*TxMetadata, addrMeta map[string]*AddressMetadata) {
	dataDir := DataDir()
	chainStr := strconv.Itoa(chainID)
	now := time.Now().UTC().Format(time.RFC3339)
	zeroAddr := "0x0000000000000000000000000000000000000000"

	for ym, monthTxs := range etherscansource.GroupByMonth(transfers, BrusselsTZ()) {
		if ym < startMonth || ym > endMonth {
			continue
		}
		parts := strings.Split(ym, "-")
		if len(parts) != 2 {
			continue
		}
		year, month := parts[0], parts[1]

		monthTxMeta := map[string]*TxMetadata{}
		monthAddrSet := map[string]struct{}{}
		for _, tx := range monthTxs {
			if m, ok := txMeta[strings.ToLower(tx.Hash)]; ok {
				monthTxMeta[strings.ToLower(tx.Hash)] = m
			}
			if from := strings.ToLower(tx.From); from != "" && from != zeroAddr {
				monthAddrSet[from] = struct{}{}
			}
			if to := strings.ToLower(tx.To); to != "" && to != zeroAddr {
				monthAddrSet[to] = struct{}{}
			}
		}
		monthAddrMeta := map[string]*AddressMetadata{}
		for a := range monthAddrSet {
			if m, ok := addrMeta[a]; ok {
				monthAddrMeta[a] = m
			}
		}

		incoming := NostrMetadataCache{
			FetchedAt:    now,
			ChainID:      chainID,
			Transactions: monthTxMeta,
			Addresses:    monthAddrMeta,
		}
		monthPath := nostrsource.ChainMetadataPath(dataDir, year, month, chainID)
		merged := MergeNostrMetadata(LoadNostrMetadataCache(monthPath), incoming)
		_ = WriteNostrMetadataCache(monthPath, merged)
	}

	// Latest registry: union of every annotation we just learned about.
	latestPath := filepath.Join(dataDir, "latest", nostrsource.RelPath(chainStr, nostrsource.MetadataFile))
	incoming := NostrMetadataCache{
		FetchedAt:    now,
		ChainID:      chainID,
		Transactions: txMeta,
		Addresses:    addrMeta,
	}
	merged := MergeNostrMetadata(LoadNostrMetadataCache(latestPath), incoming)
	_ = WriteNostrMetadataCache(latestPath, merged)
}

func groupTransfersByMonth(transfers []TokenTransfer) map[string][]TokenTransfer {
	return etherscansource.GroupByMonth(transfers, BrusselsTZ())
}

// findFirstIncompleteMonth walks backwards from current month to find the
// first month where all configured transaction sources have data.
// Returns the month AFTER the last complete one (i.e. the first incomplete month).
func findFirstIncompleteMonth(settings *Settings, sourceFilter string) string {
	dataDir := DataDir()
	now := time.Now().In(BrusselsTZ())

	// Determine which sources are expected
	var expectedSources []string
	for _, acc := range settings.Finance.Accounts {
		provider := acc.Provider
		if provider == "etherscan" {
			provider = acc.Chain
		}
		if sourceFilter != "" && !strings.EqualFold(provider, sourceFilter) {
			continue
		}
		// Deduplicate
		found := false
		for _, s := range expectedSources {
			if s == provider {
				found = true
				break
			}
		}
		if !found {
			expectedSources = append(expectedSources, provider)
		}
	}

	if len(expectedSources) == 0 {
		return fmt.Sprintf("%d-%02d", now.Year(), now.Month())
	}

	// Walk backwards from current month, max 24 months
	for i := 0; i < 24; i++ {
		t := now.AddDate(0, -i, 0)
		ym := fmt.Sprintf("%d-%02d", t.Year(), t.Month())
		year := fmt.Sprintf("%d", t.Year())
		month := fmt.Sprintf("%02d", t.Month())

		allPresent := true
		for _, source := range expectedSources {
			sourceDir := filepath.Join(dataDir, year, month, "sources", source)
			if source == "monerium" {
				sourceDir = moneriumsource.Path(dataDir, year, month)
			} else if source == "celo" || source == "gnosis" || source == "ethereum" || source == "etherscan" {
				sourceDir = etherscansource.Path(dataDir, year, month, source)
			}
			entries, err := os.ReadDir(sourceDir)
			if err != nil || len(entries) == 0 {
				allPresent = false
				break
			}
		}

		if allPresent && i > 0 {
			// This month is complete — start syncing from it (inclusive, to refresh)
			return ym
		}
	}

	// Nothing complete found, sync current month only
	return fmt.Sprintf("%d-%02d", now.Year(), now.Month())
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func parseTokenValue(rawValue string, decimals int) float64 {
	return etherscansource.ParseTokenValue(rawValue, decimals)
}

func etherscanAccount(acc FinanceAccount) etherscansource.Account {
	out := etherscansource.Account{
		Slug:    acc.Slug,
		Chain:   acc.Chain,
		ChainID: acc.ChainID,
		Address: acc.Address,
	}
	if acc.Token != nil {
		out.TokenAddress = acc.Token.Address
		out.TokenSymbol = acc.Token.Symbol
		out.TokenDecimals = acc.Token.Decimals
	}
	return out
}

// allMonthsCached checks if every month in the range [startMonth, endMonth] has a cached file.
// relPathFn returns the relative path within the month directory for the cache file.
// Returns true only if all months have the file. The current month is NOT exempt.
func allMonthsCached(dataDir, startMonth, endMonth string, relPathFn func(year, month string) string) bool {
	months := expandMonthRange(startMonth, endMonth)
	for _, ym := range months {
		parts := strings.Split(ym, "-")
		if len(parts) != 2 {
			return false
		}
		relPath := relPathFn(parts[0], parts[1])
		filePath := filepath.Join(dataDir, parts[0], parts[1], relPath)
		if !fileExists(filePath) {
			return false
		}
	}
	return true
}

// expandMonthRange returns all YYYY-MM strings from start to end inclusive.
func expandMonthRange(startMonth, endMonth string) []string {
	startParts := strings.Split(startMonth, "-")
	endParts := strings.Split(endMonth, "-")
	if len(startParts) != 2 || len(endParts) != 2 {
		return nil
	}
	sy, _ := strconv.Atoi(startParts[0])
	sm, _ := strconv.Atoi(startParts[1])
	ey, _ := strconv.Atoi(endParts[0])
	em, _ := strconv.Atoi(endParts[1])

	var months []string
	for y, m := sy, sm; y < ey || (y == ey && m <= em); {
		months = append(months, fmt.Sprintf("%d-%02d", y, m))
		m++
		if m > 12 {
			m = 1
			y++
		}
	}
	return months
}

func monthRangeIncludes(month, startMonth, endMonth string) bool {
	return month >= startMonth && month <= endMonth
}

func printSyncVariablesIfNeeded(sourceFilter, slugFilter, source string, acc FinanceAccount) {
	if sourceFilter != "" && slugFilter != "" {
		return
	}
	if sourceFilter == "" {
		fmt.Printf("  Source: %s\n", source)
	}
	if slugFilter == "" {
		account := acc.Slug
		if acc.Name != "" && acc.Name != acc.Slug {
			account = fmt.Sprintf("%s (%s)", acc.Slug, acc.Name)
		}
		if acc.AccountID != "" {
			account = fmt.Sprintf("%s, %s", account, acc.AccountID)
		}
		fmt.Printf("  Account: %s\n", account)
	}
	fmt.Println()
}

func sortedTrueMonths(months map[string]bool) []string {
	out := make([]string, 0, len(months))
	for month, ok := range months {
		if ok {
			out = append(out, month)
		}
	}
	sort.Strings(out)
	return out
}

func printMonthHeadingOnce(ym string, printed map[string]bool) {
	if printed[ym] {
		return
	}
	printed[ym] = true
	fmt.Printf("%s\n", strings.ReplaceAll(ym, "-", "/"))
}

func stripeTransactionProgress(status *statusLine) sources.ProgressFunc {
	return func(ev sources.ProgressEvent) {
		if ev.Source != "stripe" || ev.Step != "fetch_transactions" {
			return
		}
		switch ev.Detail {
		case "page":
			status.Update("Fetching transactions from Stripe... page %d, %d fetched", ev.Current, ev.Total)
		case "rate_limited":
			status.Update("Stripe rate limited on page %d; waiting", ev.Current)
		case "stop_at_cached_month":
			status.Update("Stopping at %s; local source file already has %d transactions", ev.Month, ev.Total)
		}
	}
}

func stripeChargeProgress(status *statusLine) sources.ProgressFunc {
	return func(ev sources.ProgressEvent) {
		if ev.Source == "stripe" && ev.Step == "fetch_charges" && ev.Detail == "charge_session" {
			status.Update("Fetching Stripe charge records... %d/%d", ev.Current, ev.Total)
		}
	}
}

// currentMonthCacheFile returns the path to the cache file for the current month.
func currentMonthCacheFile(dataDir string, relPathFn func(year, month string) string) string {
	now := time.Now().In(BrusselsTZ())
	year := fmt.Sprintf("%d", now.Year())
	month := fmt.Sprintf("%02d", now.Month())
	return filepath.Join(dataDir, year, month, relPathFn(year, month))
}

// readLastPeekHash reads the stored latest tx hash from a previous sync.
func readLastPeekHash(dataDir, chain, slug string) string {
	fp := etherscansource.Path(dataDir, "latest", "", chain, ".peek-"+slug)
	data, err := os.ReadFile(fp)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}

// writeLastPeekHash stores the latest tx hash so we can compare on next sync.
func writeLastPeekHash(dataDir, chain, slug, hash string) {
	fp := etherscansource.Path(dataDir, "latest", "", chain, ".peek-"+slug)
	_ = writeDataFile(fp, []byte(hash+"\n"))
}

func printTransactionsSyncHelp() {
	f := Fmt
	fmt.Printf(`
%schb transactions sync%s — Fetch blockchain, Stripe & Monerium transactions

%sUSAGE%s
  %schb transactions sync%s [year[/month]] [options]

%sOPTIONS%s
  %s<year>%s                  Sync all months of a year (e.g. 2025)
  %s<year/month>%s            Sync a specific month (e.g. 2025/03)
  %s--source%s <name>         Sync only: gnosis, celo, stripe, monerium
  %s--month%s <YYYY-MM>       Alias for year/month filter
  %s--force%s                 Re-fetch even if cached
  %s--no-nostr%s              Skip Nostr metadata fetch
  %s--help, -h%s              Show this help

%sSOURCES%s
  %sgnosis%s      ERC20 token transfers via Etherscan V2 API (Gnosis Chain)
  %scelo%s        ERC20 token transfers via Etherscan V2 API (Celo)
  %sstripe%s      Balance transactions from Stripe
  %smonerium%s    SEPA orders from Monerium (stored in sources/monerium/)

%sENVIRONMENT%s
  %sETHERSCAN_API_KEY%s         Etherscan/Gnosisscan API key
  %sSTRIPE_SECRET_KEY%s         Stripe secret key
  %sMONERIUM_CLIENT_ID%s        Monerium OAuth client ID
  %sMONERIUM_CLIENT_SECRET%s    Monerium OAuth client secret
  %sMONERIUM_ENV%s              "production" (default) or "sandbox"

%sNOSTR%s
  For blockchain sources (gnosis, celo), Nostr metadata is automatically fetched
  from txinfo relays (NIP-73). Address names and transaction descriptions from
  Nostr annotations are used when generating reports. Use %s--no-nostr%s to skip.

%sEXAMPLES%s
  %schb transactions sync%s                       Sync all sources, last 2 months
  %schb transactions sync --source monerium%s     Monerium only
  %schb transactions sync 2025 --source stripe%s  Stripe for all of 2025
  %schb transactions sync --no-nostr%s            Skip Nostr metadata fetching
`,
		f.Bold, f.Reset, // 1: title
		f.Bold, f.Reset, // 2: USAGE
		f.Cyan, f.Reset, // 3: chb transactions sync
		f.Bold, f.Reset, // 4: OPTIONS
		f.Yellow, f.Reset, // 5: <year>
		f.Yellow, f.Reset, // 6: <year/month>
		f.Yellow, f.Reset, // 7: --source
		f.Yellow, f.Reset, // 8: --month
		f.Yellow, f.Reset, // 9: --force
		f.Yellow, f.Reset, // 10: --no-nostr
		f.Yellow, f.Reset, // 11: --help
		f.Bold, f.Reset, // 12: SOURCES
		f.Cyan, f.Reset, // 13: gnosis
		f.Cyan, f.Reset, // 14: celo
		f.Cyan, f.Reset, // 15: stripe
		f.Cyan, f.Reset, // 16: monerium
		f.Bold, f.Reset, // 17: ENVIRONMENT
		f.Yellow, f.Reset, // 18: ETHERSCAN_API_KEY
		f.Yellow, f.Reset, // 19: STRIPE_SECRET_KEY
		f.Yellow, f.Reset, // 20: MONERIUM_CLIENT_ID
		f.Yellow, f.Reset, // 21: MONERIUM_CLIENT_SECRET
		f.Yellow, f.Reset, // 22: MONERIUM_ENV
		f.Bold, f.Reset, // 23: NOSTR
		f.Yellow, f.Reset, // 24: --no-nostr in nostr section
		f.Bold, f.Reset, // 25: EXAMPLES
		f.Cyan, f.Reset, // 26: sync all
		f.Cyan, f.Reset, // 27: --source monerium
		f.Cyan, f.Reset, // 28: 2025 --source stripe
		f.Cyan, f.Reset, // 29: --no-nostr example
	)
}
