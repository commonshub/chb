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
	Source             json.RawMessage        `json:"source,omitempty"`
	ReportingCategory string                 `json:"reporting_category"`
	Metadata          map[string]interface{} `json:"metadata,omitempty"`
}

// StripeListResponse is the response from /v1/balance_transactions
type StripeListResponse struct {
	Data    []StripeTransaction `json:"data"`
	HasMore bool                `json:"has_more"`
}

// StripeCacheFile is the structure saved to disk
type StripeCacheFile struct {
	Transactions []StripeTransaction `json:"transactions"`
	CachedAt     string              `json:"cachedAt"`
	AccountID    string              `json:"accountId,omitempty"`
	Currency     string              `json:"currency"`
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

func TransactionsSync(args []string) error {
	if HasFlag(args, "--help", "-h", "help") {
		printTransactionsSyncHelp()
		return nil
	}

	settings, err := LoadSettings()
	if err != nil {
		return fmt.Errorf("failed to load settings: %w", err)
	}

	force := HasFlag(args, "--force")
	monthFilter := GetOption(args, "--month")
	sourceFilter := strings.ToLower(GetOption(args, "--source"))

	// Positional year/month arg (e.g. "2025" or "2025/03")
	posYear, posMonth, posFound := ParseYearMonthArg(args)

	// Determine which months to process
	now := time.Now().In(BrusselsTZ())
	var startMonth, endMonth string

	// Check --since / --history first
	sinceMonth, isSince := ResolveSinceMonth(args, "finance")

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
		// Default: current month back to the first month missing any source
		endMonth = fmt.Sprintf("%d-%02d", now.Year(), now.Month())
		startMonth = findFirstIncompleteMonth(settings, sourceFilter)
		if startMonth == "" || startMonth > endMonth {
			startMonth = endMonth
		}
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
	if sourceFilter == "" || sourceFilter == "gnosis" || sourceFilter == "etherscan" || sourceFilter == "blockchain" {
		etherscanAccounts := make([]FinanceAccount, 0)
		for _, acc := range settings.Finance.Accounts {
			if acc.Provider == "etherscan" && acc.Token != nil {
				etherscanAccounts = append(etherscanAccounts, acc)
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

				stripeTxs, err := fetchStripeTransactions(stripeKey, startMonth, endMonth)
				if err != nil {
					fmt.Printf("    %s✗ Error: %v%s\n", Fmt.Red, err, Fmt.Reset)
					continue
				}

				fmt.Printf("    %sFetched %d transactions%s\n", Fmt.Dim, len(stripeTxs), Fmt.Reset)

				// Group by month
				byMonth := groupStripeByMonth(stripeTxs)
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

					dataDir := DataDir()
					relPath := filepath.Join("finance", "stripe", "transactions.json")
					filePath := filepath.Join(dataDir, year, month, relPath)

					// Skip if exists and not force (but always update current month)
					if !force && fileExists(filePath) {
						if ym != fmt.Sprintf("%d-%02d", now.Year(), now.Month()) {
							continue
						}
					}

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
			}
		}
		}
	}

	// --- Monerium sync ---
	if sourceFilter == "" || sourceFilter == "monerium" {
		moneriumAccounts := make([]FinanceAccount, 0)
		for _, acc := range settings.Finance.Accounts {
			if acc.Provider == "monerium" {
				moneriumAccounts = append(moneriumAccounts, acc)
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
							slug := acc.Slug
							if slug == "" {
								slug = acc.Address[:8]
							}
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
	return nil
}

func fetchTokenTransfers(acc FinanceAccount, apiKey string) ([]TokenTransfer, error) {
	baseURL := fmt.Sprintf("https://api.etherscan.io/v2/api?chainid=%d", acc.ChainID)
	url := fmt.Sprintf("%s&module=account&action=tokentx&contractaddress=%s&address=%s&startblock=0&endblock=99999999&sort=desc&apikey=%s",
		baseURL, acc.Token.Address, acc.Address, apiKey)

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

func fetchStripeTransactions(apiKey, startMonth, endMonth string) ([]StripeTransaction, error) {
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
	createdLt := rangeEnd.Unix()

	var startingAfter string
	for {
		url := fmt.Sprintf("https://api.stripe.com/v1/balance_transactions?limit=100&created[gte]=%d&created[lt]=%d",
			createdGte, createdLt)
		if startingAfter != "" {
			url += "&starting_after=" + startingAfter
		}

		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("stripe API error: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode == 429 {
			// Rate limited — wait and retry
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

		if !listResp.HasMore || len(listResp.Data) == 0 {
			break
		}
		startingAfter = listResp.Data[len(listResp.Data)-1].ID

		// Small delay between pages to be polite
		time.Sleep(200 * time.Millisecond)
	}

	return allTxs, nil
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
	ID       string `json:"id"`
	Kind     string `json:"kind"` // "redeem" or "issue"
	Profile  string `json:"profile"`
	Address  string `json:"address"`
	Chain    string `json:"chain"`
	Currency string `json:"currency"`
	Amount   string `json:"amount"`
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

func printTransactionsSyncHelp() {
	f := Fmt
	fmt.Printf(`
%schb transactions sync%s — Fetch blockchain, Stripe & Monerium transactions

%sUSAGE%s
  %schb transactions sync%s [year[/month]] [options]

%sOPTIONS%s
  %s<year>%s                  Sync all months of a year (e.g. 2025)
  %s<year/month>%s            Sync a specific month (e.g. 2025/03)
  %s--source%s <name>         Sync only: gnosis, stripe, monerium
  %s--month%s <YYYY-MM>       Alias for year/month filter
  %s--force%s                 Re-fetch even if cached
  %s--help, -h%s              Show this help

%sSOURCES%s
  %sgnosis%s      ERC20 token transfers via Etherscan V2 API
  %sstripe%s      Balance transactions from Stripe
  %smonerium%s    SEPA orders from Monerium (stored in finance/monerium/private/)

%sENVIRONMENT%s
  %sETHERSCAN_API_KEY%s         Etherscan/Gnosisscan API key
  %sSTRIPE_SECRET_KEY%s         Stripe secret key
  %sMONERIUM_CLIENT_ID%s        Monerium OAuth client ID
  %sMONERIUM_CLIENT_SECRET%s    Monerium OAuth client secret
  %sMONERIUM_ENV%s              "production" (default) or "sandbox"

%sEXAMPLES%s
  %schb transactions sync%s                       Sync all sources, last 2 months
  %schb transactions sync --source monerium%s     Monerium only
  %schb transactions sync 2025 --source stripe%s  Stripe for all of 2025
`,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
	)
}
