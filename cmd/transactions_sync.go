package cmd

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

var stripeHTTPClient = &http.Client{Timeout: 20 * time.Second}

// StripeTransaction represents a Stripe balance transaction
type StripeTransaction struct {
	ID                string                 `json:"id"`
	Created           int64                  `json:"created"`
	Amount            int64                  `json:"amount"`
	Fee               int64                  `json:"fee"`
	Net               int64                  `json:"net"`
	Currency          string                 `json:"currency"`
	Type              string                 `json:"type"`
	Description       string                 `json:"description,omitempty"`
	Source            json.RawMessage        `json:"source,omitempty"`
	ReportingCategory string                 `json:"reporting_category"`
	Metadata          map[string]interface{} `json:"metadata,omitempty"`
	// Enriched fields extracted from expanded source during fetch
	CustomerName  string `json:"customerName,omitempty"`
	CustomerEmail string `json:"customerEmail,omitempty"`
	ChargeID      string `json:"chargeId,omitempty"`
}

// StripeListResponse is the response from /v1/balance_transactions
type StripeListResponse struct {
	Data    []StripeTransaction `json:"data"`
	HasMore bool                `json:"has_more"`
}

// StripeCacheFile is the structure saved to disk (public, no PII)
type StripeCacheFile struct {
	Transactions []StripeTransaction `json:"transactions"`
	CachedAt     string              `json:"cachedAt"`
	AccountID    string              `json:"accountId,omitempty"`
	Currency     string              `json:"currency"`
}

// StripeCustomerData holds PII extracted from Stripe charges.
// Saved separately in finance/stripe/private/customers.json.
type StripeCustomerData struct {
	FetchedAt string                        `json:"fetchedAt"`
	Customers map[string]*StripeCustomerPII `json:"customers"` // keyed by balance transaction ID (txn_...)
}

type StripeCustomerPII struct {
	Name  string `json:"name,omitempty"`
	Email string `json:"email,omitempty"`
}

// EtherscanResponse represents the Etherscan V2 API response
type EtherscanResponse struct {
	Status  string            `json:"status"`
	Message string            `json:"message"`
	Result  []json.RawMessage `json:"result"`
}

// TokenTransfer represents a single ERC20 token transfer
type TokenTransfer struct {
	BlockNumber  string `json:"blockNumber"`
	TimeStamp    string `json:"timeStamp"`
	Hash         string `json:"hash"`
	From         string `json:"from"`
	To           string `json:"to"`
	Value        string `json:"value"`
	TokenName    string `json:"tokenName"`
	TokenSymbol  string `json:"tokenSymbol"`
	TokenDecimal string `json:"tokenDecimal"`
}

// TransactionsCacheFile is the structure saved to disk
type TransactionsCacheFile struct {
	Transactions []TokenTransfer `json:"transactions"`
	CachedAt     string          `json:"cachedAt"`
	Account      string          `json:"account"`
	Chain        string          `json:"chain"`
	Token        string          `json:"token"`
}

func TransactionsSync(args []string) (int, error) {
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
	sinceMonth, isSince := ResolveSinceMonth(args, "finance")
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

	fmt.Printf("\n%s⛓️  Syncing transactions%s\n", Fmt.Bold, Fmt.Reset)
	fmt.Printf("%sDATA_DIR: %s%s\n", Fmt.Dim, DataDir(), Fmt.Reset)
	fmt.Printf("%sMonth range: %s → %s%s\n", Fmt.Dim, startMonth, endMonth, Fmt.Reset)
	if sourceFilter != "" {
		fmt.Printf("%sSource filter: %s%s\n", Fmt.Dim, sourceFilter, Fmt.Reset)
	}
	fmt.Println()

	totalProcessed := 0

	// --- Etherscan / blockchain sync ---
	if sourceFilter == "" || sourceFilter == "gnosis" || sourceFilter == "celo" || sourceFilter == "etherscan" || sourceFilter == "blockchain" {
		etherscanAccounts := make([]FinanceAccount, 0)
		for _, acc := range settings.Finance.Accounts {
			if acc.Provider == "etherscan" && acc.Token != nil {
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
			if !found {
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
				fmt.Printf("%s⚠ ETHERSCAN_API_KEY not set, skipping blockchain sync%s\n", Fmt.Yellow, Fmt.Reset)
			} else {
				fmt.Printf("%s⛓️  Syncing blockchain transactions%s\n\n", Fmt.Bold, Fmt.Reset)
				for _, acc := range etherscanAccounts {
					fmt.Printf("  %s%s%s (%s/%s)\n", Fmt.Bold, acc.Name, Fmt.Reset, acc.Chain, acc.Token.Symbol)

					// Check if we can skip the full fetch by peeking at the latest tx
					if !force {
						filename := fmt.Sprintf("%s.%s.json", acc.Slug, acc.Token.Symbol)
						peekHash, peekErr := peekEtherscanLatest(acc, apiKey)
						if peekErr == nil {
							cachedLatest := latestCachedEtherscanTxHashGlobal(DataDir(), acc.Chain, filename)
							if cachedLatest == "" {
								cachedLatest = readLastPeekHash(DataDir(), acc.Chain, acc.Slug+"."+acc.Token.Symbol)
							}
							if peekHash == cachedLatest {
								// Peek matches, but only skip if we're not missing data for months in range.
								// Etherscan accounts may have data in months we haven't cached yet.
								relPathFn := func(year, month string) string {
									return filepath.Join("finance", acc.Chain, filename)
								}
								if peekHash == "" || allMonthsCached(DataDir(), startMonth, endMonth, relPathFn) {
									fmt.Printf("    %s✓ Up to date%s\n", Fmt.Green, Fmt.Reset)
									time.Sleep(400 * time.Millisecond)
									continue
								}
							}
						}
					}

					transfers, err := fetchTokenTransfers(acc, apiKey)
					if err != nil {
						fmt.Printf("    %s✗ Error: %v%s\n", Fmt.Red, err, Fmt.Reset)
						continue
					}

					fmt.Printf("    %sFetched %d total transfers%s\n", Fmt.Dim, len(transfers), Fmt.Reset)

					// Group by month
					byMonth := groupTransfersByMonth(transfers)

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

						// Save to data/YYYY/MM/finance/{chain}/{slug}.{token}.json
						dataDir := DataDir()
						filename := fmt.Sprintf("%s.%s.json", acc.Slug, acc.Token.Symbol)
						relPath := filepath.Join("finance", acc.Chain, filename)
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

						data, _ := json.MarshalIndent(cache, "", "  ")
						if err := writeMonthFile(dataDir, year, month, relPath, data); err != nil {
							fmt.Printf("    %s✗ Failed to write: %v%s\n", Fmt.Red, err, Fmt.Reset)
							continue
						}

						saved++
						totalProcessed += len(monthTxs)
					}

					if saved > 0 {
						fmt.Printf("    %s✓ Saved %d months%s\n", Fmt.Green, saved, Fmt.Reset)
					}

					// Fetch Nostr metadata for all transfers
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
						txMeta, addrMeta, nostrErr := FetchNostrMetadata(acc.ChainID, txHashes, addresses, nostrSince)
						if nostrErr != nil {
							fmt.Printf(" %s✗ %v%s\n", Fmt.Red, nostrErr, Fmt.Reset)
						} else {
							fmt.Printf(" %s✓ %d tx, %d address annotations%s\n", Fmt.Green, len(txMeta), len(addrMeta), Fmt.Reset)
							// Collect affected months to write per-month nostr-metadata.json
							type monthKey struct{ year, month string }
							byMonth := map[monthKey][]TokenTransfer{}
							for ym, monthTxs := range groupTransfersByMonth(transfers) {
								if ym < startMonth || ym > endMonth {
									continue
								}
								parts := strings.Split(ym, "-")
								if len(parts) != 2 {
									continue
								}
								byMonth[monthKey{parts[0], parts[1]}] = monthTxs
							}
							dataDir := DataDir()
							for mk := range byMonth {
								nostrCache := NostrMetadataCache{
									FetchedAt:    time.Now().UTC().Format(time.RFC3339),
									ChainID:      acc.ChainID,
									Transactions: txMeta,
									Addresses:    addrMeta,
								}
								nostrData, _ := json.MarshalIndent(nostrCache, "", "  ")
								nostrRelPath := filepath.Join("finance", acc.Chain, "nostr-metadata.json")
								writeMonthFile(dataDir, mk.year, mk.month, nostrRelPath, nostrData)
							}
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
				stripeAccounts = append(stripeAccounts, acc)
			}
		}

		if len(stripeAccounts) > 0 {
			stripeKey := os.Getenv("STRIPE_SECRET_KEY")
			if stripeKey == "" {
				fmt.Printf("\n%s⚠ STRIPE_SECRET_KEY not set, skipping Stripe sync%s\n", Fmt.Yellow, Fmt.Reset)
			} else {
				fmt.Printf("\n%s💳 Syncing Stripe transactions%s\n\n", Fmt.Bold, Fmt.Reset)
				for _, acc := range stripeAccounts {
					fmt.Printf("  %s%s%s", Fmt.Bold, acc.Name, Fmt.Reset)
					if acc.AccountID != "" {
						fmt.Printf(" (%s)", acc.AccountID)
					}
					fmt.Println()

					// Check if we can skip the full fetch
					if !force {
						relPathFn := func(year, month string) string {
							return filepath.Join("finance", "stripe", "transactions.json")
						}
						if allMonthsCached(DataDir(), startMonth, endMonth, relPathFn) {
							cachedPath := currentMonthCacheFile(DataDir(), relPathFn)
							cachedLatest := latestCachedStripeTxID(cachedPath)
							if cachedLatest != "" {
								peekID, peekErr := peekStripeLatest(stripeKey, acc.AccountID, startMonth, endMonth)
								if peekErr == nil && peekID == cachedLatest {
									fmt.Printf("    %s✓ Up to date%s\n", Fmt.Green, Fmt.Reset)
									continue
								}
							}
						}
					}

					var stripeCreatedAfter *time.Time
					if !force && !isSince && !posFound && monthFilter == "" && !lastSyncTime.IsZero() {
						stripeCreatedAfter = &lastSyncTime
						fmt.Printf("    %sIncremental since %s%s\n", Fmt.Dim, lastSyncTime.In(BrusselsTZ()).Format(time.RFC3339), Fmt.Reset)
					}

					stopAtMonthBoundary := !force && !isSince && !posFound && monthFilter == "" && stripeCreatedAfter == nil
					stripeTxs, err := fetchStripeTransactions(stripeKey, acc.AccountID, startMonth, endMonth, fetchLimit, stripeCreatedAfter, stopAtMonthBoundary, DataDir())
					if err != nil {
						fmt.Printf("    %s✗ Error: %v%s\n", Fmt.Red, err, Fmt.Reset)
						continue
					}

					fmt.Printf("    %sFetched %d transactions%s\n", Fmt.Dim, len(stripeTxs), Fmt.Reset)

					// Group by month and determine which months actually changed.
					byMonth := groupStripeByMonth(stripeTxs)
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
						relPath := filepath.Join("finance", "stripe", "transactions.json")
						filePath := filepath.Join(dataDir, year, month, relPath)

						if force || ym == fmt.Sprintf("%d-%02d", now.Year(), now.Month()) || !fileExists(filePath) {
							monthsToUpdate[ym] = true
							continue
						}
						if localStripeTransactionCount(filePath) != len(monthTxs) {
							monthsToUpdate[ym] = true
						}
					}

					saved := 0
					if len(monthsToUpdate) == 0 {
						fmt.Printf("    %s✓ Stripe months unchanged%s\n", Fmt.Green, Fmt.Reset)
					} else {
						fmt.Printf("    %sUpdating %d month(s)%s\n", Fmt.Dim, len(monthsToUpdate), Fmt.Reset)
					}

					for ym, monthTxs := range byMonth {
						if ym < startMonth || ym > endMonth || !monthsToUpdate[ym] {
							continue
						}

						parts := strings.Split(ym, "-")
						if len(parts) != 2 {
							continue
						}
						year, month := parts[0], parts[1]

						dataDir := DataDir()
						relPath := filepath.Join("finance", "stripe", "transactions.json")

						cache := StripeCacheFile{
							Transactions: monthTxs,
							CachedAt:     time.Now().UTC().Format(time.RFC3339),
							AccountID:    acc.AccountID,
							Currency:     acc.Currency,
						}

						data, _ := json.MarshalIndent(cache, "", "  ")
						if err := writeMonthFile(dataDir, year, month, relPath, data); err != nil {
							fmt.Printf("    %s✗ Failed to write: %v%s\n", Fmt.Red, err, Fmt.Reset)
							continue
						}

						saved++
						totalProcessed += len(monthTxs)
					}

					if saved > 0 {
						fmt.Printf("    %s✓ Saved %d months%s\n", Fmt.Green, saved, Fmt.Reset)
					}

					// Fetch Nostr annotations for Stripe transactions
					if !noNostr && len(stripeTxs) > 0 && len(monthsToUpdate) > 0 {
						fmt.Printf("    %sFetching Nostr annotations...%s", Fmt.Dim, Fmt.Reset)
						if stripeCreatedAfter != nil {
							fmt.Printf(" %s(since %s)%s", Fmt.Dim, stripeCreatedAfter.In(BrusselsTZ()).Format(time.RFC3339), Fmt.Reset)
						}
						var uris []string
						for _, tx := range stripeTxs {
							txMonth := time.Unix(tx.Created, 0).In(BrusselsTZ()).Format("2006-01")
							if monthsToUpdate[txMonth] {
								uris = append(uris, BuildStripeURI(tx.ID))
							}
						}
						annotations, err := FetchNostrAnnotations(uris, stripeCreatedAfter)
						if err != nil {
							fmt.Printf(" %s✗ %v%s\n", Fmt.Red, err, Fmt.Reset)
						} else {
							fmt.Printf(" %s✓ %d annotations%s\n", Fmt.Green, len(annotations), Fmt.Reset)
							// Save per month
							for ym, monthTxs := range byMonth {
								if ym < startMonth || ym > endMonth || !monthsToUpdate[ym] {
									continue
								}
								monthAnnotations := map[string]*TxAnnotation{}
								for _, tx := range monthTxs {
									uri := BuildStripeURI(tx.ID)
									if ann, ok := annotations[uri]; ok {
										monthAnnotations[uri] = ann
									}
								}
								if len(monthAnnotations) > 0 {
									parts := strings.Split(ym, "-")
									if len(parts) == 2 {
										cache := NostrAnnotationCache{
											FetchedAt:   time.Now().UTC().Format(time.RFC3339),
											Annotations: monthAnnotations,
										}
										cacheData, _ := json.MarshalIndent(cache, "", "  ")
										writeMonthFile(DataDir(), parts[0], parts[1],
											filepath.Join("finance", "stripe", "nostr-annotations.json"), cacheData)
									}
								}
							}
						}
					}
					// Fetch Stripe charge details (customer name, app, metadata) only for months being updated.
					if len(monthsToUpdate) == 0 {
						continue
					}
					fmt.Printf("    %sFetching charge details...%s", Fmt.Dim, Fmt.Reset)
					var chargeIDs []string
					chargeByTxn := map[string]string{} // txn_id → ch_id for month grouping
					refundLookups := 0
					for _, tx := range stripeTxs {
						txMonth := time.Unix(tx.Created, 0).In(BrusselsTZ()).Format("2006-01")
						if !monthsToUpdate[txMonth] {
							continue
						}
						chID := extractChargeID(tx.Source)
						if chID != "" {
							chargeIDs = append(chargeIDs, chID)
							chargeByTxn[tx.ID] = chID
						} else {
							// For refunds, resolve to original charge
							srcID := extractSourceID(tx.Source)
							if strings.HasPrefix(srcID, "re_") {
								refundLookups++
								if refundLookups == 1 || refundLookups%10 == 0 {
									fmt.Printf(" %srefunds:%d%s", Fmt.Dim, refundLookups, Fmt.Reset)
								}
								origCharge := fetchRefundChargeID(stripeKey, acc.AccountID, srcID)
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
								srcID := extractSourceID(tx.Source)
								if strings.HasPrefix(srcID, "re_") {
									refundToCharge[srcID] = chID
								}
								break
							}
						}
					}

					fmt.Printf(" %s(%d charge ids)%s", Fmt.Dim, len(chargeIDs), Fmt.Reset)
					charges, err := fetchStripeCharges(stripeKey, acc.AccountID, chargeIDs)
					if err != nil {
						fmt.Printf(" %s✗ %v%s\n", Fmt.Red, err, Fmt.Reset)
					} else {
						fmt.Printf(" %s✓ %d charges enriched%s\n", Fmt.Green, len(charges), Fmt.Reset)

						// Discover application names from fetched charges
						appNames := map[string]int{} // track ca_ IDs for discovery
						for _, ch := range charges {
							if ch.Application != "" {
								appNames[ch.Application]++
							}
						}
						if len(appNames) > 0 {
							fmt.Printf("    %sApplications:%s", Fmt.Dim, Fmt.Reset)
							for app, count := range appNames {
								name := app
								if n, ok := knownStripeApps[app]; ok {
									name = n
								}
								fmt.Printf(" %s(%d)", name, count)
							}
							fmt.Println()
						}

						// Save per month
						for ym, monthTxs := range byMonth {
							if ym < startMonth || ym > endMonth || !monthsToUpdate[ym] {
								continue
							}
							monthCharges := map[string]*StripeCharge{}
							for _, tx := range monthTxs {
								chID := chargeByTxn[tx.ID]
								if ch, ok := charges[chID]; ok {
									monthCharges[chID] = ch
								}
							}
							if len(monthCharges) > 0 {
								parts := strings.Split(ym, "-")
								if len(parts) == 2 {
									SaveStripeChargeEnrichment(DataDir(), parts[0], parts[1], monthCharges, refundToCharge)
								}
							}
						}

						// Save per-month private customer data from enriched charges.
						for ym, monthTxs := range byMonth {
							if ym < startMonth || ym > endMonth || !monthsToUpdate[ym] {
								continue
							}
							customers := &StripeCustomerData{
								FetchedAt: time.Now().UTC().Format(time.RFC3339),
								Customers: map[string]*StripeCustomerPII{},
							}
							for _, tx := range monthTxs {
								chID := chargeByTxn[tx.ID]
								if ch, ok := charges[chID]; ok {
									name := ch.BestName()
									email := ch.BestEmail()
									if name != "" || email != "" {
										customers.Customers[tx.ID] = &StripeCustomerPII{Name: name, Email: email}
									}
								}
							}
							if len(customers.Customers) > 0 {
								parts := strings.Split(ym, "-")
								if len(parts) == 2 {
									piiData, _ := json.MarshalIndent(customers, "", "  ")
									piiRelPath := filepath.Join("finance", "stripe", "private", "customers.json")
									writeMonthFile(DataDir(), parts[0], parts[1], piiRelPath, piiData)
								}
							}
						}
					}
				}
			}
		}
	}

	// --- Monerium sync ---
	// Also auto-include EURe etherscan accounts (Monerium mints/redeems happen on-chain)
	if sourceFilter == "" || sourceFilter == "monerium" || sourceFilter == "gnosis" {
		moneriumAccounts := make([]FinanceAccount, 0)
		for _, acc := range settings.Finance.Accounts {
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
				fmt.Printf("\n%s⚠ MONERIUM_CLIENT_ID/MONERIUM_CLIENT_SECRET not set, skipping Monerium sync%s\n", Fmt.Yellow, Fmt.Reset)
			} else {
				fmt.Printf("\n%s🏦 Syncing Monerium orders%s\n\n", Fmt.Bold, Fmt.Reset)

				token, err := authenticateMonerium(clientID, clientSecret, moneriumEnv)
				if err != nil {
					fmt.Printf("  %s✗ Auth failed: %v%s\n", Fmt.Red, err, Fmt.Reset)
				} else {
					for _, acc := range moneriumAccounts {
						fmt.Printf("  %s%s%s (%s)\n", Fmt.Bold, acc.Name, Fmt.Reset, acc.Address)

						orders, err := fetchMoneriumOrders(token, acc.Address, moneriumEnv)
						if err != nil {
							fmt.Printf("    %s✗ Error: %v%s\n", Fmt.Red, err, Fmt.Reset)
							continue
						}

						fmt.Printf("    %sFetched %d orders%s\n", Fmt.Dim, len(orders), Fmt.Reset)

						// Check if latest order matches cache — skip if no new data
						slug := acc.Slug
						if slug == "" {
							slug = acc.Address[:8]
						}
						if !force && len(orders) > 0 {
							relPathFn := func(year, month string) string {
								return filepath.Join("finance", "monerium", "private", slug+".json")
							}
							if allMonthsCached(DataDir(), startMonth, endMonth, relPathFn) {
								cachedPath := currentMonthCacheFile(DataDir(), relPathFn)
								if orders[0].ID == latestCachedMoneriumOrderID(cachedPath) {
									fmt.Printf("    %s✓ Up to date%s\n", Fmt.Green, Fmt.Reset)
									continue
								}
							}
						}

						// Group by month
						byMonth := groupMoneriumByMonth(orders)
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
							relPath := filepath.Join("finance", "monerium", "private", slug+".json")
							filePath := filepath.Join(dataDir, year, month, relPath)

							if !force && fileExists(filePath) {
								if ym != fmt.Sprintf("%d-%02d", now.Year(), now.Month()) {
									continue
								}
							}

							cache := MoneriumCacheFile{
								Orders:   monthOrders,
								CachedAt: time.Now().UTC().Format(time.RFC3339),
								Address:  acc.Address,
							}

							data, _ := json.MarshalIndent(cache, "", "  ")
							if err := writeMonthFile(dataDir, year, month, relPath, data); err != nil {
								fmt.Printf("    %s✗ Failed to write: %v%s\n", Fmt.Red, err, Fmt.Reset)
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

	fmt.Printf("\n%s✓ Done!%s %d transactions processed\n\n", Fmt.Green, Fmt.Reset, totalProcessed)
	UpdateSyncSource("transactions", isFullSync)
	UpdateSyncActivity(isFullSync)
	return totalProcessed, nil
}

func fetchTokenTransfers(acc FinanceAccount, apiKey string) ([]TokenTransfer, error) {
	baseURL := fmt.Sprintf("https://api.etherscan.io/v2/api?chainid=%d", acc.ChainID)

	// If address is empty or equals the token contract, fetch ALL transfers for the token
	// (contribution token mode — no specific wallet to filter on)
	var url string
	if acc.Address == "" || strings.EqualFold(acc.Address, acc.Token.Address) {
		url = fmt.Sprintf("%s&module=account&action=tokentx&contractaddress=%s&startblock=0&endblock=99999999&sort=desc&apikey=%s",
			baseURL, acc.Token.Address, apiKey)
	} else {
		url = fmt.Sprintf("%s&module=account&action=tokentx&contractaddress=%s&address=%s&startblock=0&endblock=99999999&sort=desc&apikey=%s",
			baseURL, acc.Token.Address, acc.Address, apiKey)
	}

	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * time.Second)
		}

		resp, err := http.Get(url)
		if err != nil {
			lastErr = err
			continue
		}
		defer resp.Body.Close()

		var result struct {
			Status  string          `json:"status"`
			Message string          `json:"message"`
			Result  json.RawMessage `json:"result"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			lastErr = err
			continue
		}

		if result.Status == "0" && result.Message != "No transactions found" {
			if strings.Contains(strings.ToLower(result.Message), "rate limit") {
				lastErr = fmt.Errorf("rate limited: %s", result.Message)
				time.Sleep(2 * time.Second)
				continue
			}
			return nil, fmt.Errorf("API error: %s", result.Message)
		}

		var transfers []TokenTransfer
		if err := json.Unmarshal(result.Result, &transfers); err != nil {
			// Could be "No transactions found" which returns a string
			return []TokenTransfer{}, nil
		}

		return transfers, nil
	}

	return nil, fmt.Errorf("failed after 3 attempts: %v", lastErr)
}

func groupTransfersByMonth(transfers []TokenTransfer) map[string][]TokenTransfer {
	byMonth := make(map[string][]TokenTransfer)
	tz := BrusselsTZ()

	for _, tx := range transfers {
		ts, err := strconv.ParseInt(tx.TimeStamp, 10, 64)
		if err != nil {
			continue
		}
		t := time.Unix(ts, 0).In(tz)
		ym := fmt.Sprintf("%d-%02d", t.Year(), t.Month())
		byMonth[ym] = append(byMonth[ym], tx)
	}

	return byMonth
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
			// Check if any file exists in transactions/<source>/
			sourceDir := filepath.Join(dataDir, year, month, "finance", source)
			if source == "monerium" {
				sourceDir = filepath.Join(dataDir, year, month, "finance", "monerium", "private")
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

// parseTokenValue converts raw token value string to float using decimals
func parseTokenValue(rawValue string, decimals int) float64 {
	val := new(big.Float)
	val.SetString(rawValue)
	divisor := new(big.Float).SetFloat64(math.Pow10(decimals))
	result := new(big.Float).Quo(val, divisor)
	f, _ := result.Float64()
	return f
}

func fetchStripeTransactions(apiKey, accountID, startMonth, endMonth string, limit int, createdAfter *time.Time, stopAtMonthBoundary bool, dataDir string) ([]StripeTransaction, error) {
	tz := BrusselsTZ()
	var allTxs []StripeTransaction

	// Parse month range to timestamps
	startParts := strings.Split(startMonth, "-")
	if len(startParts) != 2 {
		return nil, fmt.Errorf("invalid start month: %s", startMonth)
	}
	startYear, _ := strconv.Atoi(startParts[0])
	startMon, _ := strconv.Atoi(startParts[1])
	rangeStart := time.Date(startYear, time.Month(startMon), 1, 0, 0, 0, 0, tz)

	endParts := strings.Split(endMonth, "-")
	if len(endParts) != 2 {
		return nil, fmt.Errorf("invalid end month: %s", endMonth)
	}
	endYear, _ := strconv.Atoi(endParts[0])
	endMon, _ := strconv.Atoi(endParts[1])
	rangeEnd := time.Date(endYear, time.Month(endMon)+1, 1, 0, 0, 0, 0, tz) // first day of month after end

	createdGte := rangeStart.Unix()
	if createdAfter != nil && !createdAfter.IsZero() {
		after := createdAfter.Unix()
		if after > createdGte {
			createdGte = after
		}
	}
	createdLt := rangeEnd.Unix()

	pageSize := 100
	if limit > 0 && limit < pageSize {
		pageSize = limit
	}

	var startingAfter string
	page := 0
	for {
		page++
		url := fmt.Sprintf("https://api.stripe.com/v1/balance_transactions?limit=%d&created[gte]=%d&created[lt]=%d",
			pageSize, createdGte, createdLt)
		if startingAfter != "" {
			url += "&starting_after=" + startingAfter
		}

		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		if accountID != "" {
			req.Header.Set("Stripe-Account", accountID)
		}

		resp, err := stripeHTTPClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("stripe API error: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode == 429 {
			// Rate limited — wait and retry
			fmt.Printf("    %sStripe rate limited on page %d, waiting 2s...%s\n", Fmt.Dim, page, Fmt.Reset)
			time.Sleep(2 * time.Second)
			continue
		}

		if resp.StatusCode != 200 {
			return nil, fmt.Errorf("stripe API returned %d", resp.StatusCode)
		}

		var listResp StripeListResponse
		if err := json.NewDecoder(resp.Body).Decode(&listResp); err != nil {
			return nil, fmt.Errorf("failed to decode stripe response: %w", err)
		}

		allTxs = append(allTxs, listResp.Data...)
		fmt.Printf("    %sStripe page %d: +%d tx (total %d)%s\n", Fmt.Dim, page, len(listResp.Data), len(allTxs), Fmt.Reset)

		if stopAtMonthBoundary && dataDir != "" && len(allTxs) > 0 {
			oldestMonth := time.Unix(allTxs[len(allTxs)-1].Created, 0).In(tz).Format("2006-01")
			countSeen := 0
			for _, tx := range allTxs {
				if time.Unix(tx.Created, 0).In(tz).Format("2006-01") == oldestMonth {
					countSeen++
				}
			}
			localCount := localStripeTransactionCount(filepath.Join(dataDir, strings.ReplaceAll(oldestMonth, "-", "/"), "finance", "stripe", "transactions.json"))
			if localCount > 0 && countSeen == localCount {
				fmt.Printf("    %sStripe stop heuristic: %s count matches local cache (%d)%s\n", Fmt.Dim, oldestMonth, localCount, Fmt.Reset)
				break
			}
		}

		if limit > 0 && len(allTxs) >= limit {
			allTxs = allTxs[:limit]
			break
		}

		if !listResp.HasMore || len(listResp.Data) == 0 {
			break
		}
		startingAfter = listResp.Data[len(listResp.Data)-1].ID

		// Small delay between pages to be polite
		time.Sleep(200 * time.Millisecond)
	}

	return allTxs, nil
}

func localStripeTransactionCount(filePath string) int {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return 0
	}
	var cache StripeCacheFile
	if json.Unmarshal(data, &cache) != nil {
		return 0
	}
	return len(cache.Transactions)
}

// stripSourceToID reduces an expanded source object back to just the ID string for public storage.
func stripSourceToID(source json.RawMessage) json.RawMessage {
	if source == nil {
		return nil
	}
	var obj struct {
		ID string `json:"id"`
	}
	if json.Unmarshal(source, &obj) == nil && obj.ID != "" {
		quoted, _ := json.Marshal(obj.ID)
		return quoted
	}
	return source // already a string or unparseable, keep as-is
}

// enrichStripeTransaction extracts customer name, email, and charge ID from the expanded source.
func enrichStripeTransaction(tx *StripeTransaction) {
	if tx.Source == nil {
		return
	}

	var source struct {
		Object         string `json:"object"`
		ID             string `json:"id"`
		Description    string `json:"description"`
		BillingDetails struct {
			Name  string `json:"name"`
			Email string `json:"email"`
		} `json:"billing_details"`
		Customer interface{}            `json:"customer"`
		Metadata map[string]interface{} `json:"metadata"`
	}
	if json.Unmarshal(tx.Source, &source) != nil {
		return
	}

	if source.Object == "charge" || source.Object == "payment_intent" {
		tx.ChargeID = source.ID

		// Customer name: prefer customer object, fall back to billing_details
		if custObj, ok := source.Customer.(map[string]interface{}); ok {
			if name, ok := custObj["name"].(string); ok && name != "" {
				tx.CustomerName = name
			}
			if email, ok := custObj["email"].(string); ok && email != "" {
				tx.CustomerEmail = email
			}
		}
		if tx.CustomerName == "" {
			tx.CustomerName = source.BillingDetails.Name
		}
		if tx.CustomerEmail == "" {
			tx.CustomerEmail = source.BillingDetails.Email
		}

		// Use charge description if balance tx description is empty
		if tx.Description == "" && source.Description != "" {
			tx.Description = source.Description
		}

		// Merge charge metadata into tx metadata
		if len(source.Metadata) > 0 && tx.Metadata == nil {
			tx.Metadata = map[string]interface{}{}
		}
		for k, v := range source.Metadata {
			tx.Metadata[k] = v
		}
	}
}

func groupStripeByMonth(txs []StripeTransaction) map[string][]StripeTransaction {
	byMonth := make(map[string][]StripeTransaction)
	tz := BrusselsTZ()

	for _, tx := range txs {
		t := time.Unix(tx.Created, 0).In(tz)
		ym := fmt.Sprintf("%d-%02d", t.Year(), t.Month())
		byMonth[ym] = append(byMonth[ym], tx)
	}

	return byMonth
}

// ── Monerium ────────────────────────────────────────────────────────────────

// MoneriumOrder represents a single Monerium order (redeem = outgoing SEPA, issue = incoming mint)
type MoneriumOrder struct {
	ID          string `json:"id"`
	Kind        string `json:"kind"` // "redeem" or "issue"
	Profile     string `json:"profile"`
	Address     string `json:"address"`
	Chain       string `json:"chain"`
	Currency    string `json:"currency"`
	Amount      string `json:"amount"`
	Counterpart struct {
		Identifier struct {
			Standard string `json:"standard"`
			IBAN     string `json:"iban,omitempty"`
		} `json:"identifier"`
		Details struct {
			Name        string `json:"name,omitempty"`
			CompanyName string `json:"companyName,omitempty"`
			FirstName   string `json:"firstName,omitempty"`
			LastName    string `json:"lastName,omitempty"`
			Country     string `json:"country,omitempty"`
		} `json:"details"`
	} `json:"counterpart"`
	Memo  string `json:"memo,omitempty"`
	State string `json:"state"`
	Meta  struct {
		PlacedAt    string   `json:"placedAt"`
		ProcessedAt string   `json:"processedAt,omitempty"`
		TxHashes    []string `json:"txHashes,omitempty"`
	} `json:"meta"`
}

type MoneriumCacheFile struct {
	Orders   []MoneriumOrder `json:"orders"`
	CachedAt string          `json:"cachedAt"`
	Address  string          `json:"address"`
}

func authenticateMonerium(clientID, clientSecret, environment string) (string, error) {
	baseURL := "https://api.monerium.app"
	if environment == "sandbox" {
		baseURL = "https://api.monerium.dev"
	}

	data := fmt.Sprintf("grant_type=client_credentials&client_id=%s&client_secret=%s", clientID, clientSecret)
	req, err := http.NewRequest("POST", baseURL+"/auth/token", strings.NewReader(data))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("auth request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		var errResp struct {
			Error string `json:"error"`
		}
		json.NewDecoder(resp.Body).Decode(&errResp)
		return "", fmt.Errorf("auth failed (%d): %s", resp.StatusCode, errResp.Error)
	}

	var tokenResp struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		return "", fmt.Errorf("failed to decode token: %w", err)
	}

	return tokenResp.AccessToken, nil
}

func fetchMoneriumOrders(accessToken, address, environment string) ([]MoneriumOrder, error) {
	baseURL := "https://api.monerium.app"
	if environment == "sandbox" {
		baseURL = "https://api.monerium.dev"
	}

	url := fmt.Sprintf("%s/orders?address=%s", baseURL, address)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("Accept", "application/vnd.monerium.api-v2+json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("API error: %d", resp.StatusCode)
	}

	// API may return array directly or { orders: [...] }
	var raw json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	var orders []MoneriumOrder
	if err := json.Unmarshal(raw, &orders); err != nil {
		// Try wrapped format
		var wrapped struct {
			Orders []MoneriumOrder `json:"orders"`
		}
		if err := json.Unmarshal(raw, &wrapped); err != nil {
			return nil, fmt.Errorf("failed to parse orders: %w", err)
		}
		orders = wrapped.Orders
	}

	return orders, nil
}

func groupMoneriumByMonth(orders []MoneriumOrder) map[string][]MoneriumOrder {
	byMonth := make(map[string][]MoneriumOrder)
	tz := BrusselsTZ()

	for _, order := range orders {
		dateStr := order.Meta.PlacedAt
		if dateStr == "" {
			continue
		}
		t, err := time.Parse(time.RFC3339, dateStr)
		if err != nil {
			t, err = time.Parse(time.RFC3339Nano, dateStr)
			if err != nil {
				continue
			}
		}
		t = t.In(tz)
		ym := fmt.Sprintf("%d-%02d", t.Year(), t.Month())
		byMonth[ym] = append(byMonth[ym], order)
	}

	return byMonth
}

// latestCachedStripeTxID reads a Stripe cache file and returns the ID of the first (most recent) transaction.
func latestCachedStripeTxID(filePath string) string {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return ""
	}
	var cache StripeCacheFile
	if json.Unmarshal(data, &cache) != nil || len(cache.Transactions) == 0 {
		return ""
	}
	return cache.Transactions[0].ID
}

// peekStripeLatest fetches the single most recent balance transaction from Stripe for the given month range.
func peekStripeLatest(apiKey, accountID, startMonth, endMonth string) (string, error) {
	tz := BrusselsTZ()
	startParts := strings.Split(startMonth, "-")
	endParts := strings.Split(endMonth, "-")
	if len(startParts) != 2 || len(endParts) != 2 {
		return "", fmt.Errorf("invalid month range")
	}
	startYear, _ := strconv.Atoi(startParts[0])
	startMon, _ := strconv.Atoi(startParts[1])
	endYear, _ := strconv.Atoi(endParts[0])
	endMon, _ := strconv.Atoi(endParts[1])
	rangeStart := time.Date(startYear, time.Month(startMon), 1, 0, 0, 0, 0, tz)
	rangeEnd := time.Date(endYear, time.Month(endMon)+1, 1, 0, 0, 0, 0, tz)

	url := fmt.Sprintf("https://api.stripe.com/v1/balance_transactions?limit=1&created[gte]=%d&created[lt]=%d",
		rangeStart.Unix(), rangeEnd.Unix())
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	if accountID != "" {
		req.Header.Set("Stripe-Account", accountID)
	}
	resp, err := stripeHTTPClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("stripe API returned %d", resp.StatusCode)
	}
	var listResp StripeListResponse
	if err := json.NewDecoder(resp.Body).Decode(&listResp); err != nil {
		return "", err
	}
	if len(listResp.Data) == 0 {
		return "", nil
	}
	return listResp.Data[0].ID, nil
}

// latestCachedEtherscanTxHashGlobal finds the most recent cached etherscan tx hash across all months.
// Since etherscan returns transfers sorted desc, the first tx in the most recent month's cache
// is the latest transaction overall.
func latestCachedEtherscanTxHashGlobal(dataDir, chain, filename string) string {
	yearDirs, err := os.ReadDir(dataDir)
	if err != nil {
		return ""
	}
	// Walk year/month dirs in reverse order to find most recent cache file
	var latestYM string
	var latestPath string
	for _, yd := range yearDirs {
		if !yd.IsDir() || len(yd.Name()) != 4 {
			continue
		}
		monthDirs, _ := os.ReadDir(filepath.Join(dataDir, yd.Name()))
		for _, md := range monthDirs {
			if !md.IsDir() || len(md.Name()) != 2 {
				continue
			}
			ym := yd.Name() + "-" + md.Name()
			fp := filepath.Join(dataDir, yd.Name(), md.Name(), "finance", chain, filename)
			if fileExists(fp) && ym > latestYM {
				latestYM = ym
				latestPath = fp
			}
		}
	}
	if latestPath == "" {
		return ""
	}
	return latestCachedEtherscanTxHash(latestPath)
}

// latestCachedEtherscanTxHash reads an etherscan cache file and returns the hash of the first (most recent) transaction.
func latestCachedEtherscanTxHash(filePath string) string {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return ""
	}
	var cache TransactionsCacheFile
	if json.Unmarshal(data, &cache) != nil || len(cache.Transactions) == 0 {
		return ""
	}
	return cache.Transactions[0].Hash
}

// peekEtherscanLatest fetches the single most recent token transfer from etherscan.
func peekEtherscanLatest(acc FinanceAccount, apiKey string) (string, error) {
	baseURL := fmt.Sprintf("https://api.etherscan.io/v2/api?chainid=%d", acc.ChainID)
	var url string
	if acc.Address == "" || strings.EqualFold(acc.Address, acc.Token.Address) {
		url = fmt.Sprintf("%s&module=account&action=tokentx&contractaddress=%s&startblock=0&endblock=99999999&page=1&offset=1&sort=desc&apikey=%s",
			baseURL, acc.Token.Address, apiKey)
	} else {
		url = fmt.Sprintf("%s&module=account&action=tokentx&contractaddress=%s&address=%s&startblock=0&endblock=99999999&page=1&offset=1&sort=desc&apikey=%s",
			baseURL, acc.Token.Address, acc.Address, apiKey)
	}
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var result struct {
		Status  string          `json:"status"`
		Message string          `json:"message"`
		Result  json.RawMessage `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}
	var transfers []TokenTransfer
	if err := json.Unmarshal(result.Result, &transfers); err != nil || len(transfers) == 0 {
		return "", nil
	}
	return transfers[0].Hash, nil
}

// latestCachedMoneriumOrderID reads a Monerium cache file and returns the ID of the first order.
func latestCachedMoneriumOrderID(filePath string) string {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return ""
	}
	var cache MoneriumCacheFile
	if json.Unmarshal(data, &cache) != nil || len(cache.Orders) == 0 {
		return ""
	}
	return cache.Orders[0].ID
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

// currentMonthCacheFile returns the path to the cache file for the current month.
func currentMonthCacheFile(dataDir string, relPathFn func(year, month string) string) string {
	now := time.Now().In(BrusselsTZ())
	year := fmt.Sprintf("%d", now.Year())
	month := fmt.Sprintf("%02d", now.Month())
	return filepath.Join(dataDir, year, month, relPathFn(year, month))
}

// readLastPeekHash reads the stored latest tx hash from a previous sync.
func readLastPeekHash(dataDir, chain, slug string) string {
	fp := filepath.Join(dataDir, "latest", "finance", chain, ".peek-"+slug)
	data, err := os.ReadFile(fp)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}

// writeLastPeekHash stores the latest tx hash so we can compare on next sync.
func writeLastPeekHash(dataDir, chain, slug, hash string) {
	fp := filepath.Join(dataDir, "latest", "finance", chain, ".peek-"+slug)
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
  %smonerium%s    SEPA orders from Monerium (stored in finance/monerium/private/)

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
