package cmd

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// accountSummary holds computed balance and last tx info for an account.
type accountSummary struct {
	Balance  float64
	Currency string
	LastTxAt time.Time
	TxCount  int
}

// fetchTokenBalance fetches the live on-chain token balance for an address via Etherscan V2 API.
func fetchTokenBalance(chainID int, tokenAddress, walletAddress string, decimals int) (float64, error) {
	apiKey := os.Getenv("ETHERSCAN_API_KEY")
	if apiKey == "" {
		return 0, fmt.Errorf("ETHERSCAN_API_KEY not set")
	}

	url := fmt.Sprintf("https://api.etherscan.io/v2/api?chainid=%d&module=account&action=tokenbalance&contractaddress=%s&address=%s&tag=latest&apikey=%s",
		chainID, tokenAddress, walletAddress, apiKey)

	resp, err := http.Get(url)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	var result struct {
		Status  string `json:"status"`
		Message string `json:"message"`
		Result  string `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, err
	}
	if result.Status != "1" {
		return 0, fmt.Errorf("etherscan: %s", result.Message)
	}

	balance := new(big.Float)
	balance.SetString(result.Result)
	divisor := new(big.Float).SetFloat64(math.Pow10(decimals))
	fResult := new(big.Float).Quo(balance, divisor)
	f, _ := fResult.Float64()
	return f, nil
}

// ── Balance cache ──

type balanceCache struct {
	FetchedAt string             `json:"fetchedAt"`
	Balances  map[string]float64 `json:"balances"` // keyed by lowercase address
}

func balanceCachePath() string {
	return filepath.Join(DataDir(), "latest", "balances.json")
}

func loadBalanceCache() *balanceCache {
	data, err := os.ReadFile(balanceCachePath())
	if err != nil {
		return nil
	}
	var cache balanceCache
	if json.Unmarshal(data, &cache) != nil {
		return nil
	}
	return &cache
}

func saveBalanceCache(cache *balanceCache) {
	data, _ := json.MarshalIndent(cache, "", "  ")
	dir := filepath.Dir(balanceCachePath())
	os.MkdirAll(dir, 0755)
	os.WriteFile(balanceCachePath(), data, 0644)
}

// fetchStripeBalance fetches the live Stripe balance (available + pending).
func fetchStripeBalance() (float64, error) {
	apiKey := os.Getenv("STRIPE_SECRET_KEY")
	if apiKey == "" {
		return 0, fmt.Errorf("STRIPE_SECRET_KEY not set")
	}

	req, err := http.NewRequest("GET", "https://api.stripe.com/v1/balance", nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return 0, fmt.Errorf("stripe API returned %d", resp.StatusCode)
	}

	var result struct {
		Available []struct {
			Amount   int64  `json:"amount"`
			Currency string `json:"currency"`
		} `json:"available"`
		Pending []struct {
			Amount   int64  `json:"amount"`
			Currency string `json:"currency"`
		} `json:"pending"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, err
	}

	var total int64
	for _, a := range result.Available {
		total += a.Amount
	}
	for _, p := range result.Pending {
		total += p.Amount
	}

	return float64(total) / 100, nil
}

// fetchLiveBalances fetches on-chain + Stripe balances for all accounts and caches them.
func fetchLiveBalances(configs []AccountConfig) map[string]float64 {
	balances := map[string]float64{}

	for _, acc := range configs {
		if acc.Provider == "etherscan" && acc.Address != "" && acc.Token != nil {
			balance, err := fetchTokenBalance(acc.ChainID, acc.Token.Address, acc.Address, acc.Token.Decimals)
			if err == nil {
				balances[strings.ToLower(acc.Address)] = balance
			}
			time.Sleep(200 * time.Millisecond)
		} else if acc.Provider == "stripe" {
			balance, err := fetchStripeBalance()
			if err == nil {
				key := strings.ToLower(acc.AccountID)
				if key == "" {
					key = "stripe"
				}
				balances[key] = balance
			}
		}
	}

	cache := &balanceCache{
		FetchedAt: time.Now().UTC().Format(time.RFC3339),
		Balances:  balances,
	}
	saveBalanceCache(cache)

	return balances
}

// computeAccountSummaries scans all generated transactions to compute per-account balances.
// Only counts transactions matching the account's configured token/currency.
func computeAccountSummaries() map[string]*accountSummary {
	dataDir := DataDir()
	summaries := map[string]*accountSummary{}

	// Build lookup: account key → expected currency from config
	expectedCurrency := map[string]string{}
	for _, acc := range LoadAccountConfigs() {
		key := strings.ToLower(acc.Address)
		if key == "" {
			key = strings.ToLower(acc.AccountID)
		}
		if key == "" {
			key = strings.ToLower(acc.Slug)
		}
		if acc.Token != nil {
			expectedCurrency[key] = acc.Token.Symbol
		} else if acc.Currency != "" {
			expectedCurrency[key] = acc.Currency
		}
	}

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
			txPath := filepath.Join(dataDir, yd.Name(), md.Name(), "generated", "transactions.json")
			data, err := os.ReadFile(txPath)
			if err != nil {
				continue
			}
			var txFile TransactionsFile
			if json.Unmarshal(data, &txFile) != nil {
				continue
			}

			for _, tx := range txFile.Transactions {
				// Build all possible keys for this transaction so we can look up by any
				keys := []string{}
				if tx.Account != "" {
					keys = append(keys, strings.ToLower(tx.Account))
				}
				if tx.AccountSlug != "" {
					keys = append(keys, strings.ToLower(tx.AccountSlug))
				}
				if len(keys) == 0 {
					continue
				}

				// Use first key as primary, but register all
				primaryKey := keys[0]

				// Only count transactions matching the account's configured currency
				if expCur, ok := expectedCurrency[primaryKey]; ok && expCur != "" {
					if !strings.EqualFold(tx.Currency, expCur) {
						continue
					}
				}

				s, ok := summaries[primaryKey]
				if !ok {
					s = &accountSummary{Currency: tx.Currency}
					for _, k := range keys {
						summaries[k] = s
					}
				}

				amount := tx.NormalizedAmount
				if amount == 0 {
					amount = tx.Amount
				}

				switch tx.Type {
				case "CREDIT":
					s.Balance += math.Abs(amount)
				case "DEBIT":
					s.Balance -= math.Abs(amount)
				}

				s.TxCount++

				if tx.Timestamp > 0 {
					t := time.Unix(tx.Timestamp, 0)
					if t.After(s.LastTxAt) {
						s.LastTxAt = t
					}
				}
			}
		}
	}

	return summaries
}

// accountKey returns the lookup key for an account in the summaries map.
func accountKey(acc FinanceAccount) string {
	if acc.Address != "" {
		return strings.ToLower(acc.Address)
	}
	if acc.AccountID != "" {
		return strings.ToLower(acc.AccountID)
	}
	return strings.ToLower(acc.Slug)
}

// AccountsCommand routes `chb accounts` subcommands.
func AccountsCommand(args []string) {
	if HasFlag(args, "--help", "-h", "help") {
		printAccountsHelp()
		return
	}

	// Check for `chb accounts <slug> <action>`
	if len(args) >= 1 && !strings.HasPrefix(args[0], "-") {
		slug := args[0]
		// Verify it's a known account slug
		found := false
		for _, acc := range LoadAccountConfigs() {
			if strings.EqualFold(acc.Slug, slug) {
				found = true
				break
			}
		}
		if found {
			action := ""
			if len(args) >= 2 {
				action = args[1]
			}
			switch action {
			case "sync":
				if err := AccountOdooSync(slug, args[2:]); err != nil {
					fmt.Fprintf(os.Stderr, "%sError:%s %v\n", Fmt.Red, Fmt.Reset, err)
					os.Exit(1)
				}
			case "link":
				if err := AccountOdooLink(slug, args[2:]); err != nil {
					fmt.Fprintf(os.Stderr, "%sError:%s %v\n", Fmt.Red, Fmt.Reset, err)
					os.Exit(1)
				}
			case "payouts":
				if err := AccountStripePayouts(slug, args[2:]); err != nil {
					fmt.Fprintf(os.Stderr, "%sError:%s %v\n", Fmt.Red, Fmt.Reset, err)
					os.Exit(1)
				}
			case "import-csv":
				if len(args) < 3 {
					fmt.Fprintf(os.Stderr, "%sUsage: chb accounts %s import-csv <file.csv>%s\n", Fmt.Yellow, slug, Fmt.Reset)
					os.Exit(1)
				}
				if err := ImportStripeCSV(args[2]); err != nil {
					fmt.Fprintf(os.Stderr, "%sError:%s %v\n", Fmt.Red, Fmt.Reset, err)
					os.Exit(1)
				}
			default:
				printAccountSlugHelp(slug)
			}
			return
		}
	}

	Accounts(args)
}

// Accounts lists all configured finance accounts with balance and last tx.
func Accounts(args []string) {
	configs := LoadAccountConfigs()
	if len(configs) == 0 {
		fmt.Printf("\n%sNo accounts configured.%s\n\n", Fmt.Dim, Fmt.Reset)
		return
	}

	faAccounts := ToFinanceAccounts(configs)
	summaries := computeAccountSummaries()
	refresh := HasFlag(args, "--refresh", "-r")

	// Load cached balances or fetch live
	var liveBalances map[string]float64
	var cacheTime string

	cache := loadBalanceCache()
	if cache != nil && !refresh {
		liveBalances = cache.Balances
		cacheTime = cache.FetchedAt
	} else {
		fmt.Printf("  Fetching on-chain balances...\n")
		liveBalances = fetchLiveBalances(configs)
		cacheTime = time.Now().UTC().Format(time.RFC3339)
	}

	// Fetch Odoo sync status for accounts with linked journals
	odooStatuses := fetchOdooSyncStatuses(configs, summaries)

	fmt.Printf("\n%s💰 Configured Accounts%s (%d)", Fmt.Bold, Fmt.Reset, len(configs))
	if cacheTime != "" {
		if t, err := time.Parse(time.RFC3339, cacheTime); err == nil {
			fmt.Printf("  %s(balances: %s)%s", Fmt.Dim, formatTimeAgo(t), Fmt.Reset)
		}
	}
	fmt.Println()
	fmt.Println()

	// Track totals per currency
	totals := map[string]float64{}

	for i, acc := range configs {
		fa := faAccounts[i]
		currency := acc.Currency
		if currency == "" && acc.Token != nil {
			currency = acc.Token.Symbol
		}
		if currency == "" {
			currency = "EUR"
		}

		s := summaries[accountKey(fa)]

		// Use live balance: check by address, then accountId, then slug
		var balance float64
		hasBalance := false
		for _, key := range []string{acc.Address, acc.AccountID, acc.Slug} {
			if key == "" {
				continue
			}
			if liveBalance, ok := liveBalances[strings.ToLower(key)]; ok {
				balance = liveBalance
				hasBalance = true
				break
			}
		}
		if !hasBalance && s != nil && s.TxCount > 0 {
			balance = s.Balance
			hasBalance = true
		}

		if hasBalance {
			totals[currency] += balance
		}

		// Account header: slug as title, full name below
		if hasBalance {
			fmt.Printf("  %s%s%s  %s\n", Fmt.Bold, acc.Slug, Fmt.Reset, formatBalance(balance, currency))
		} else {
			fmt.Printf("  %s%s%s\n", Fmt.Bold, acc.Slug, Fmt.Reset)
		}
		fmt.Printf("    %s%s%s\n", Fmt.Dim, acc.Name, Fmt.Reset)

		// Details with clickable hyperlinks
		if acc.Provider == "stripe" {
			url := "https://dashboard.stripe.com"
			fmt.Printf("    %s%s%s\n", Fmt.Dim, hyperlink(url, "Stripe Dashboard"), Fmt.Reset)
		} else if acc.Provider == "etherscan" && acc.Address != "" {
			if acc.WalletType == "eoa" {
				// txinfo.xyz link for EOA
				var txinfoURL string
				if acc.Token != nil {
					txinfoURL = fmt.Sprintf("https://txinfo.xyz/%s/token/%s?a=%s", acc.Chain, acc.Token.Address, acc.Address)
				} else {
					txinfoURL = fmt.Sprintf("https://txinfo.xyz/%s/address/%s", acc.Chain, acc.Address)
				}
				label := fmt.Sprintf("%s %s (EOA)", acc.Chain, truncateAddr(acc.Address))
				fmt.Printf("    %s%s%s\n", Fmt.Dim, hyperlink(txinfoURL, label), Fmt.Reset)
			} else {
				// Safe wallet — show Safe URL + txinfo URL
				chainPrefix := "eth"
				switch acc.Chain {
				case "gnosis":
					chainPrefix = "gno"
				case "celo":
					chainPrefix = "celo"
				case "polygon":
					chainPrefix = "matic"
				}
				safeURL := fmt.Sprintf("https://app.safe.global/home?safe=%s:%s", chainPrefix, acc.Address)
				safeLabel := fmt.Sprintf("Safe %s:%s", chainPrefix, truncateAddr(acc.Address))
				fmt.Printf("    %s%s%s", Fmt.Dim, hyperlink(safeURL, safeLabel), Fmt.Reset)

				// Also show txinfo link for the token
				if acc.Token != nil {
					txinfoURL := fmt.Sprintf("https://txinfo.xyz/%s/token/%s?a=%s", acc.Chain, acc.Token.Address, acc.Address)
					fmt.Printf("  %s%s%s", Fmt.Dim, hyperlink(txinfoURL, "txinfo"), Fmt.Reset)
				}
				fmt.Println()
			}
		}

		if s != nil && !s.LastTxAt.IsZero() {
			fmt.Printf("    %sLast tx: %s%s\n", Fmt.Dim, formatTimeAgo(s.LastTxAt), Fmt.Reset)
		}

		// Odoo journal sync status
		if acc.OdooJournalID > 0 {
			fmt.Printf("    %sOdoo: %s (journal #%d)%s", Fmt.Dim, acc.OdooJournalName, acc.OdooJournalID, Fmt.Reset)
			if status, ok := odooStatuses[acc.OdooJournalID]; ok {
				if status.Missing == 0 {
					fmt.Printf("  %s✓ in sync%s", Fmt.Green, Fmt.Reset)
				} else {
					fmt.Printf("  %s%d missing%s", Fmt.Yellow, status.Missing, Fmt.Reset)
				}
				if !status.LastOdooTxDate.IsZero() {
					fmt.Printf("  %s(last: %s)%s", Fmt.Dim, status.LastOdooTxDate.Format("02 Jan 2006"), Fmt.Reset)
				}
			}
			fmt.Println()
		}

		fmt.Println()
	}

	// Contribution token
	settings, _ := LoadSettings()
	if settings != nil && settings.ContributionToken != nil {
		ct := settings.ContributionToken
		found := false
		for _, acc := range configs {
			if acc.Token != nil && strings.EqualFold(acc.Token.Address, ct.Address) {
				found = true
				break
			}
		}
		if !found {
			s := summaries[strings.ToLower(ct.Chain)]
			fmt.Printf("  %s🪙 %s%s\n", Fmt.Bold, ct.Name, Fmt.Reset)
			txinfoURL := fmt.Sprintf("https://txinfo.xyz/%s/token/%s", ct.Chain, ct.Address)
			fmt.Printf("    %s%s%s\n", Fmt.Dim, hyperlink(txinfoURL, ct.Chain+"/"+ct.Symbol), Fmt.Reset)
			if s != nil && s.TxCount > 0 {
				fmt.Printf("    %s%d transactions%s\n", Fmt.Dim, s.TxCount, Fmt.Reset)
			}
			if s != nil && !s.LastTxAt.IsZero() {
				fmt.Printf("    %sLast tx: %s%s\n", Fmt.Dim, formatTimeAgo(s.LastTxAt), Fmt.Reset)
			}
			fmt.Println()
		}
	}

	// Totals per currency
	if len(totals) > 0 {
		// Merge EUR-family into one total
		eurTotal := 0.0
		for cur, bal := range totals {
			if isEURCurrency(cur) {
				eurTotal += bal
			}
		}

		fmt.Printf("  %s────────────────────────────%s\n", Fmt.Dim, Fmt.Reset)
		if eurTotal != 0 {
			fmt.Printf("  %sTotal EUR%s  %s\n", Fmt.Bold, Fmt.Reset, formatBalance(eurTotal, "EUR"))
		}
		for cur, bal := range totals {
			if !isEURCurrency(cur) {
				fmt.Printf("  %sTotal %s%s  %s\n", Fmt.Bold, cur, Fmt.Reset, formatBalance(bal, cur))
			}
		}
		fmt.Println()
	}
}

// hyperlink renders a clickable terminal hyperlink using OSC 8.
// Shows `label` as clickable text that opens `url` when clicked.
// Supported by iTerm2, Ghostty, Alacritty, Windows Terminal, etc.
func hyperlink(url, label string) string {
	return fmt.Sprintf("\x1b]8;;%s\x1b\\%s\x1b]8;;\x1b\\", url, label)
}

func formatBalance(balance float64, currency string) string {
	if isEURCurrency(currency) {
		if balance >= 0 {
			return fmt.Sprintf("%s%s%s", Fmt.Green, fmtEUR(balance), Fmt.Reset)
		}
		return fmt.Sprintf("%s-%s%s", Fmt.Red, fmtEUR(-balance), Fmt.Reset)
	}
	if balance >= 0 {
		return fmt.Sprintf("%s%s %s%s", Fmt.Green, fmtNumber(balance), currency, Fmt.Reset)
	}
	return fmt.Sprintf("%s-%s %s%s", Fmt.Red, fmtNumber(-balance), currency, Fmt.Reset)
}

func truncateAddr(addr string) string {
	if len(addr) <= 14 {
		return addr
	}
	return addr[:6] + "..." + addr[len(addr)-4:]
}

func formatTimeAgo(t time.Time) string {
	now := time.Now()
	diff := now.Sub(t)

	switch {
	case diff < time.Minute:
		return "just now"
	case diff < time.Hour:
		m := int(diff.Minutes())
		return fmt.Sprintf("%dm ago", m)
	case diff < 24*time.Hour:
		h := int(diff.Hours())
		return fmt.Sprintf("%dh ago", h)
	case diff < 48*time.Hour:
		return "yesterday"
	case diff < 7*24*time.Hour:
		d := int(diff.Hours() / 24)
		return fmt.Sprintf("%dd ago", d)
	default:
		return t.In(BrusselsTZ()).Format("02 Jan 2006")
	}
}

// ── Odoo journal sync status ──

type odooSyncStatus struct {
	Missing        int
	TotalOdoo      int
	TotalLocal     int
	LastOdooTxDate time.Time
}

// fetchOdooSyncStatuses checks Odoo for each account with a linked journal.
// Returns a map of journalID → sync status.
func fetchOdooSyncStatuses(configs []AccountConfig, summaries map[string]*accountSummary) map[int]*odooSyncStatus {
	result := map[int]*odooSyncStatus{}

	// Check if any account has an Odoo journal
	hasOdoo := false
	for _, acc := range configs {
		if acc.OdooJournalID > 0 {
			hasOdoo = true
			break
		}
	}
	if !hasOdoo {
		return result
	}

	creds, err := ResolveOdooCredentials()
	if err != nil {
		return result
	}

	uid, err := odooAuth(creds.URL, creds.DB, creds.Login, creds.Password)
	if err != nil || uid == 0 {
		return result
	}

	for _, acc := range configs {
		if acc.OdooJournalID == 0 {
			continue
		}

		// Count statement lines in Odoo for this journal
		countResult, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
			"account.bank.statement.line", "search_count",
			[]interface{}{[]interface{}{
				[]interface{}{"journal_id", "=", acc.OdooJournalID},
			}}, nil)
		if err != nil {
			continue
		}
		var odooCount int
		json.Unmarshal(countResult, &odooCount)

		// Get last statement line date
		lastResult, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
			"account.bank.statement.line", "search_read",
			[]interface{}{[]interface{}{
				[]interface{}{"journal_id", "=", acc.OdooJournalID},
			}},
			map[string]interface{}{
				"fields": []string{"date"},
				"order":  "date desc",
				"limit":  1,
			})
		var lastDate time.Time
		if err == nil {
			var lines []struct {
				Date string `json:"date"`
			}
			json.Unmarshal(lastResult, &lines)
			if len(lines) > 0 {
				lastDate, _ = time.Parse("2006-01-02", lines[0].Date)
			}
		}

		// Count local transactions for this account
		faAccounts := ToFinanceAccounts(configs)
		localCount := 0
		for i, fa := range faAccounts {
			if configs[i].Slug == acc.Slug {
				if s := summaries[accountKey(fa)]; s != nil {
					localCount = s.TxCount
				}
				break
			}
		}

		missing := localCount - odooCount
		if missing < 0 {
			missing = 0
		}

		result[acc.OdooJournalID] = &odooSyncStatus{
			Missing:        missing,
			TotalOdoo:      odooCount,
			TotalLocal:     localCount,
			LastOdooTxDate: lastDate,
		}
	}

	return result
}

// ── Account Odoo Link ──

// AccountOdooLink links a chb account to an Odoo bank journal.
func AccountOdooLink(slug string, args []string) error {
	configs := LoadAccountConfigs()
	idx := -1
	for i, acc := range configs {
		if strings.EqualFold(acc.Slug, slug) {
			idx = i
			break
		}
	}
	if idx == -1 {
		return fmt.Errorf("account '%s' not found", slug)
	}

	creds, err := ResolveOdooCredentials()
	if err != nil {
		return err
	}

	uid, err := odooAuth(creds.URL, creds.DB, creds.Login, creds.Password)
	if err != nil || uid == 0 {
		return fmt.Errorf("Odoo authentication failed: %v", err)
	}

	// Fetch bank journals
	journalsResult, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.journal", "search_read",
		[]interface{}{[]interface{}{
			[]interface{}{"type", "=", "bank"},
		}},
		map[string]interface{}{"fields": []string{"id", "name"}})
	if err != nil {
		return fmt.Errorf("failed to fetch journals: %v", err)
	}

	type journal struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}
	var journals []journal
	json.Unmarshal(journalsResult, &journals)

	fmt.Printf("\n%sAvailable Odoo bank journals:%s\n\n", Fmt.Bold, Fmt.Reset)
	for i, j := range journals {
		fmt.Printf("  %d. %s (ID: %d)\n", i+1, j.Name, j.ID)
	}
	fmt.Printf("  %d. %s+ Create new journal%s\n", len(journals)+1, Fmt.Green, Fmt.Reset)
	fmt.Printf("\n%sSelect journal number for '%s': %s", Fmt.Bold, configs[idx].Name, Fmt.Reset)

	var choice int
	fmt.Scanf("%d", &choice)
	if choice < 1 || choice > len(journals)+1 {
		return fmt.Errorf("invalid selection")
	}

	var selected journal
	if choice == len(journals)+1 {
		// Create new journal
		defaultName := configs[idx].Name
		fmt.Printf("%sJournal name [%s]: %s", Fmt.Bold, defaultName, Fmt.Reset)
		reader := bufio.NewReader(os.Stdin)
		name, _ := reader.ReadString('\n')
		name = strings.TrimSpace(name)
		if name == "" {
			name = defaultName
		}

		createResult, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
			"account.journal", "create",
			[]interface{}{[]interface{}{map[string]interface{}{
				"name": name,
				"type": "bank",
			}}}, nil)
		if err != nil {
			return fmt.Errorf("failed to create journal: %v", err)
		}
		var ids []int
		json.Unmarshal(createResult, &ids)
		if len(ids) == 0 {
			return fmt.Errorf("journal creation returned no ID")
		}
		selected = journal{ID: ids[0], Name: name}
		fmt.Printf("  %s✓ Created journal '%s' (#%d)%s\n", Fmt.Green, name, ids[0], Fmt.Reset)
	} else {
		selected = journals[choice-1]
	}

	configs[idx].OdooJournalID = selected.ID
	configs[idx].OdooJournalName = selected.Name

	if err := SaveAccountConfigs(configs); err != nil {
		return fmt.Errorf("failed to save: %v", err)
	}

	fmt.Printf("\n  %s✓ Linked '%s' → Odoo journal '%s' (#%d)%s\n\n", Fmt.Green, configs[idx].Name, selected.Name, selected.ID, Fmt.Reset)
	return nil
}

// ── Account Odoo Sync ──

// AccountOdooSync pushes local transactions to Odoo as bank statement lines.
func AccountOdooSync(slug string, args []string) error {
	configs := LoadAccountConfigs()
	var acc *AccountConfig
	for i := range configs {
		if strings.EqualFold(configs[i].Slug, slug) {
			acc = &configs[i]
			break
		}
	}
	if acc == nil {
		return fmt.Errorf("account '%s' not found", slug)
	}

	if acc.OdooJournalID == 0 {
		return fmt.Errorf("account '%s' has no linked Odoo journal. Run: chb accounts %s link", slug, slug)
	}

	creds, err := ResolveOdooCredentials()
	if err != nil {
		return err
	}

	uid, err := odooAuth(creds.URL, creds.DB, creds.Login, creds.Password)
	if err != nil || uid == 0 {
		return fmt.Errorf("Odoo authentication failed: %v", err)
	}

	dryRun := HasFlag(args, "--dry-run")
	force := HasFlag(args, "--force")
	payoutFilter := GetOption(args, "--payout")
	untilStr := GetOption(args, "--until")

	// Parse --months N to limit sync window
	monthsLimit := 0
	for i, a := range args {
		if a == "--months" && i+1 < len(args) {
			fmt.Sscanf(args[i+1], "%d", &monthsLimit)
		}
	}

	// Parse --until YYYY[MM[DD]] to cutoff date
	var untilDate time.Time
	if untilStr != "" {
		untilStr = strings.ReplaceAll(untilStr, "/", "")
		untilStr = strings.ReplaceAll(untilStr, "-", "")
		switch len(untilStr) {
		case 4: // YYYY → end of year
			y, _ := strconv.Atoi(untilStr)
			untilDate = time.Date(y+1, 1, 1, 0, 0, 0, 0, BrusselsTZ())
		case 6: // YYYYMM → end of month
			y, _ := strconv.Atoi(untilStr[:4])
			m, _ := strconv.Atoi(untilStr[4:6])
			untilDate = time.Date(y, time.Month(m)+1, 1, 0, 0, 0, 0, BrusselsTZ())
		case 8: // YYYYMMDD → end of day
			y, _ := strconv.Atoi(untilStr[:4])
			m, _ := strconv.Atoi(untilStr[4:6])
			d, _ := strconv.Atoi(untilStr[6:8])
			untilDate = time.Date(y, time.Month(m), d+1, 0, 0, 0, 0, BrusselsTZ())
		default:
			return fmt.Errorf("invalid --until format: %s (use YYYY, YYYYMM, or YYYYMMDD)", untilStr)
		}
	}

	fmt.Printf("\n%s🔄 Syncing '%s' → Odoo journal '%s' (#%d)%s",
		Fmt.Bold, acc.Name, acc.OdooJournalName, acc.OdooJournalID, Fmt.Reset)
	if monthsLimit > 0 {
		fmt.Printf("  %s(last %d months)%s", Fmt.Dim, monthsLimit, Fmt.Reset)
	}
	if !untilDate.IsZero() {
		fmt.Printf("  %s(until %s)%s", Fmt.Dim, untilDate.AddDate(0, 0, -1).Format("2006-01-02"), Fmt.Reset)
	}
	fmt.Println()
	fmt.Printf("  %sOdoo: %s (db: %s)%s\n\n", Fmt.Dim, creds.URL, creds.DB, Fmt.Reset)

	// --force without --payout: empty the entire journal first
	if force && !dryRun && payoutFilter == "" {
		if err := emptyOdooJournal(creds, uid, acc.OdooJournalID, acc.OdooJournalName); err != nil {
			return err
		}
	}

	if acc.Provider == "stripe" {
		return syncStripeToOdoo(acc, creds, uid, monthsLimit, dryRun, force, payoutFilter, untilDate)
	}
	return syncBlockchainToOdoo(acc, creds, uid, monthsLimit, dryRun, untilDate)
}

// syncBlockchainToOdoo syncs blockchain/monerium transactions to Odoo (no statements, just lines).
func syncBlockchainToOdoo(acc *AccountConfig, creds *OdooCredentials, uid int, monthsLimit int, dryRun bool, untilDate time.Time) error {
	localTxs := loadAccountTransactions(acc)
	if monthsLimit > 0 {
		cutoff := time.Now().AddDate(0, -monthsLimit, 0)
		var filtered []TransactionEntry
		for _, tx := range localTxs {
			if time.Unix(tx.Timestamp, 0).After(cutoff) {
				filtered = append(filtered, tx)
			}
		}
		localTxs = filtered
	}
	if !untilDate.IsZero() {
		var filtered []TransactionEntry
		for _, tx := range localTxs {
			if time.Unix(tx.Timestamp, 0).Before(untilDate) {
				filtered = append(filtered, tx)
			}
		}
		localTxs = filtered
	}
	fmt.Printf("  %sLocal transactions: %d%s\n", Fmt.Dim, len(localTxs), Fmt.Reset)

	existingIDs, err := fetchOdooImportIDs(creds.URL, creds.DB, uid, creds.Password, acc.OdooJournalID)
	if err != nil {
		return fmt.Errorf("failed to fetch existing Odoo entries: %v", err)
	}

	var missing []TransactionEntry
	for _, tx := range localTxs {
		if !existingIDs[buildUniqueImportID(acc, tx)] {
			missing = append(missing, tx)
		}
	}
	fmt.Printf("  %sMissing in Odoo: %d%s\n\n", Fmt.Dim, len(missing), Fmt.Reset)

	if len(missing) == 0 {
		fmt.Printf("  %s✓ Already in sync%s\n\n", Fmt.Green, Fmt.Reset)
		return nil
	}

	if dryRun {
		for _, tx := range missing {
			t := time.Unix(tx.Timestamp, 0).In(BrusselsTZ())
			amt := tx.Amount
			if tx.NormalizedAmount != 0 {
				amt = tx.NormalizedAmount
			}
			if tx.Type == "DEBIT" {
				amt = -math.Abs(amt)
			}
			fmt.Printf("    %s  %.2f  %s\n", t.Format("2006-01-02"), amt, tx.Counterparty)
		}
		fmt.Println()
		return nil
	}

	partnerCache := map[string]int{}
	synced, errors := 0, 0
	for _, tx := range missing {
		t := time.Unix(tx.Timestamp, 0).In(BrusselsTZ())
		amt := tx.Amount
		if tx.NormalizedAmount != 0 {
			amt = tx.NormalizedAmount
		}
		if tx.Type == "DEBIT" {
			amt = -math.Abs(amt)
		} else {
			amt = math.Abs(amt)
		}

		paymentRef := tx.Counterparty
		if paymentRef == "" {
			paymentRef = txDisplayDescription(tx)
		}
		if paymentRef == "" {
			paymentRef = tx.Provider + " " + strings.ToLower(tx.Type)
		}

		partnerEmail, _ := tx.Metadata["email"].(string)
		partnerID := resolveOdooPartner(creds, uid, tx.Counterparty, partnerEmail, partnerCache)

		lineData := map[string]interface{}{
			"journal_id":       acc.OdooJournalID,
			"date":             t.Format("2006-01-02"),
			"payment_ref":      paymentRef,
			"amount":           amt,
			"unique_import_id": buildUniqueImportID(acc, tx),
		}
		if partnerID > 0 {
			lineData["partner_id"] = partnerID
		}
		if narr := buildOdooNarration(acc, tx); narr != "" {
			lineData["narration"] = narr
		}

		_, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
			"account.bank.statement.line", "create",
			[]interface{}{[]interface{}{lineData}}, nil)
		if err != nil {
			fmt.Printf("  %s✗ %s %s: %v%s\n", Fmt.Red, t.Format("2006-01-02"), paymentRef, err, Fmt.Reset)
			errors++
			continue
		}
		synced++
		fmt.Printf("  %s✓%s %s  %s  %.2f\n", Fmt.Green, Fmt.Reset, t.Format("2006-01-02"), paymentRef, amt)
	}
	fmt.Printf("\n  %s✓ Synced %d, errors %d%s\n\n", Fmt.Green, synced, errors, Fmt.Reset)
	return nil
}

// syncStats tracks metrics for the sync summary report.
type syncStats struct {
	LinesCreated    int
	LinesSkipped    int
	Statements      int
	PartnersMatched int
	PartnersCreated int
	PartnersSkipped int
	Ambiguous       []string // "name <email>" entries
	Charges         int      // number of charge/payment lines (all sources)
	ChargesGross    float64  // total gross charges
	Refunds         int      // number of refund lines
	RefundsTotal    float64  // total refund amount (negative)
	ChargeFees      float64  // total charge fees (from all sources)
	StripeFees      float64  // Stripe billing fees (separate debit lines)
	PayoutsTotal    float64  // total payout withdrawals (negative)
}

func (s *syncStats) print() {
	fmt.Printf("\n  %s── Summary ──%s\n", Fmt.Bold, Fmt.Reset)
	fmt.Printf("    Lines:          %d created, %d skipped\n", s.LinesCreated, s.LinesSkipped)
	fmt.Printf("    Statements:     %d\n", s.Statements)
	fmt.Printf("    Partners:       %d matched, %d created, %d ambiguous\n",
		s.PartnersMatched, s.PartnersCreated, s.PartnersSkipped)
	if len(s.Ambiguous) > 0 {
		for _, a := range s.Ambiguous {
			fmt.Printf("      %s⚠ %s%s\n", Fmt.Yellow, a, Fmt.Reset)
		}
	}
	fmt.Println()
	fmt.Printf("    Charges:        %d  %s+%s%s\n", s.Charges, Fmt.Green, fmtEUR(s.ChargesGross), Fmt.Reset)
	fmt.Printf("    Refunds:        %d  %s-%s%s\n", s.Refunds, Fmt.Red, fmtEUR(-s.RefundsTotal), Fmt.Reset)
	fmt.Printf("    Charge fees:    %s-%s%s\n", Fmt.Red, fmtEUR(s.ChargeFees), Fmt.Reset)
	if s.StripeFees > 0 {
		fmt.Printf("    Stripe fees:    %s-%s%s\n", Fmt.Red, fmtEUR(s.StripeFees), Fmt.Reset)
	}
	fmt.Printf("    Payouts:        %s-%s%s\n", Fmt.Red, fmtEUR(-s.PayoutsTotal), Fmt.Reset)
	net := s.ChargesGross + s.RefundsTotal - s.ChargeFees - s.StripeFees + s.PayoutsTotal
	fmt.Printf("    Balance:        %s\n", fmtEURSigned(net))
	fmt.Println()
}

// syncStripeToOdoo syncs Stripe payouts as Odoo bank statements.
// Each payout becomes one statement containing its charges (gross), fees, and the payout line.
func syncStripeToOdoo(acc *AccountConfig, creds *OdooCredentials, uid int, monthsLimit int, dryRun, force bool, payoutFilter string, untilDate time.Time) error {
	stripeKey := os.Getenv("STRIPE_SECRET_KEY")
	if stripeKey == "" {
		return fmt.Errorf("STRIPE_SECRET_KEY not set")
	}

	// Fetch existing statement references to know which payouts are already synced
	// We store payout ID in the "reference" field, and also check "name" for backward compat
	existingStmts := map[string]bool{}
	stmtResult, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement", "search_read",
		[]interface{}{[]interface{}{
			[]interface{}{"journal_id", "=", acc.OdooJournalID},
		}},
		map[string]interface{}{"fields": []string{"name", "reference"}, "limit": 0})
	if err == nil {
		var stmts []struct {
			Name      string      `json:"name"`
			Reference interface{} `json:"reference"`
		}
		json.Unmarshal(stmtResult, &stmts)
		for _, s := range stmts {
			existingStmts[s.Name] = true
			if ref, ok := s.Reference.(string); ok && ref != "" {
				existingStmts[ref] = true
			}
		}
	}

	// Fetch payouts from Stripe
	if payoutFilter != "" {
		fmt.Printf("  %sPayout filter: %s%s\n", Fmt.Dim, payoutFilter, Fmt.Reset)
	} else {
		fmt.Printf("  %sFetching Stripe payouts...%s\n", Fmt.Dim, Fmt.Reset)
	}

	var payouts []stripePayout
	if payoutFilter != "" {
		// For a single payout, check cache first then fetch if needed
		cache := loadStripePayoutsCache()
		var found *stripePayout
		if cache != nil {
			for _, po := range cache.Payouts {
				if po.ID == payoutFilter {
					found = &po
					break
				}
			}
		}
		if found != nil {
			payouts = []stripePayout{*found}
		} else {
			singlePo, err := fetchSinglePayout(stripeKey, payoutFilter)
			if err != nil {
				return fmt.Errorf("failed to fetch payout %s: %v", payoutFilter, err)
			}
			payouts = []stripePayout{*singlePo}
		}
	} else {
		var err error
		payouts, err = refreshStripePayoutsCache(stripeKey)
		if err != nil {
			return fmt.Errorf("failed to fetch payouts: %v", err)
		}
		payouts = filterPayoutsByMonths(payouts, monthsLimit)
	}

	// Filter by --until date
	if !untilDate.IsZero() {
		var filtered []stripePayout
		for _, po := range payouts {
			if time.Unix(po.ArrivalDate, 0).Before(untilDate) {
				filtered = append(filtered, po)
			}
		}
		payouts = filtered
	}

	fmt.Printf("  %s%d payouts found%s\n", Fmt.Dim, len(payouts), Fmt.Reset)

	// Filter to unsynced payouts (unless --force)
	var toSync []stripePayout
	for _, po := range payouts {
		if existingStmts[po.ID] && !force {
			continue
		}
		toSync = append(toSync, po)
	}

	if len(toSync) == 0 {
		fmt.Printf("\n  %s✓ All payouts synced%s\n\n", Fmt.Green, Fmt.Reset)
		return nil
	}

	// Sort oldest first
	sort.Slice(toSync, func(i, j int) bool {
		return toSync[i].ArrivalDate < toSync[j].ArrivalDate
	})

	fmt.Printf("  %s%d payouts to sync%s\n\n", Fmt.Dim, len(toSync), Fmt.Reset)

	// Load cache for updating tx counts
	payoutsCache := loadStripePayoutsCache()
	payoutsCacheByID := map[string]int{} // po ID → index in cache
	if payoutsCache != nil {
		for i, po := range payoutsCache.Payouts {
			payoutsCacheByID[po.ID] = i
		}
	}

	partnerCache := map[string]int{}
	totalSynced := 0
	stats := &syncStats{}
	autoPayoutTxIDs := map[string]bool{} // txn IDs consumed by auto payout statements

	for _, po := range toSync {
		arrivalDate := time.Unix(po.ArrivalDate, 0).In(BrusselsTZ())
		poAmount := centsToEuros(po.Amount)

		fmt.Printf("  %s📦 Payout %s%s  %s  %.2f %s\n",
			Fmt.Bold, po.ID, Fmt.Reset, arrivalDate.Format("2006-01-02"), poAmount, po.Currency)

		// Fetch balance transactions for this payout
		poTxs, err := fetchPayoutTransactions(stripeKey, po.ID)
		isManual := err != nil && strings.Contains(err.Error(), "manual payout")
		if err != nil && !isManual {
			fmt.Printf("    %s✗ Failed to fetch transactions: %v%s\n", Fmt.Red, err, Fmt.Reset)
			continue
		}
		if isManual {
			fmt.Printf("    %sManual payout — recording withdrawal only%s\n", Fmt.Dim, Fmt.Reset)
		}

		// Update tx count in cache and track consumed txn IDs
		if payoutsCache != nil {
			if idx, ok := payoutsCacheByID[po.ID]; ok {
				payoutsCache.Payouts[idx].TxCount = len(poTxs)
			}
		}
		if !isManual {
			for _, tx := range poTxs {
				autoPayoutTxIDs[tx.ID] = true
			}
		}

		if dryRun {
			if isManual {
				fmt.Printf("    %sManual payout: -%.2f (withdrawal only)%s\n", Fmt.Dim, poAmount, Fmt.Reset)
			} else {
				var totalCharges, totalFees float64
				charges := 0
				for _, tx := range poTxs {
					if tx.Type == "charge" || tx.Type == "payment" {
						charges++
						totalCharges += centsToEuros(tx.Amount)
						totalFees += centsToEuros(tx.Fee)
					}
				}
				fmt.Printf("    %s%d charges, gross: %.2f, fees: -%.2f, payout: -%.2f%s\n",
					Fmt.Dim, charges, totalCharges, totalFees, poAmount, Fmt.Reset)
			}
			continue
		}

		// If --force and statement already exists, delete it first
		if force && existingStmts[po.ID] && !dryRun {
			deleteOdooStatementByName(creds, uid, acc.OdooJournalID, po.ID)
		}

		// Create the bank statement for this payout
		// balance_start=0, balance_end_real=0 (charges - fees - payout = 0)
		stmtName := po.statementName()
		stmtRef := po.ID
		if po.StatementDescriptor != "" {
			stmtRef = po.StatementDescriptor + " (" + po.ID + ")"
		}
		stmtCreateResult, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
			"account.bank.statement", "create",
			[]interface{}{[]interface{}{map[string]interface{}{
				"journal_id":       acc.OdooJournalID,
				"name":             stmtName,
				"reference":        stmtRef,
				"balance_start":    0,
				"balance_end_real": 0,
			}}}, nil)
		if err != nil {
			fmt.Printf("    %s✗ Failed to create statement: %v%s\n", Fmt.Red, err, Fmt.Reset)
			continue
		}
		var stmtIDs []int
		json.Unmarshal(stmtCreateResult, &stmtIDs)
		if len(stmtIDs) == 0 {
			fmt.Printf("    %s✗ Statement creation returned no ID%s\n", Fmt.Red, Fmt.Reset)
			continue
		}
		stmtID := stmtIDs[0]

		// Manual payouts: create orphan withdrawal line (no statement).
		// Statements would break the balance chain since they don't net to zero.
		if isManual {
			// Delete the empty statement we just created — manual payouts don't need one
			odooExec(creds.URL, creds.DB, uid, creds.Password,
				"account.bank.statement", "unlink",
				[]interface{}{[]interface{}{stmtID}}, nil)

			importID := fmt.Sprintf("stripe:%s:%s:0", strings.ToLower(acc.AccountID), strings.ToLower(po.ID))
			desc := po.Description
			if desc == "" {
				desc = fmt.Sprintf("Manual payout %s", arrivalDate.Format("2006-01-02"))
			}
			lineData := map[string]interface{}{
				"journal_id":       acc.OdooJournalID,
				"date":             arrivalDate.Format("2006-01-02"),
				"payment_ref":      desc,
				"amount":           -poAmount,
				"unique_import_id": importID,
			}
			created, err := createOrAdoptStatementLine(creds, uid, lineData)
			if err != nil {
				fmt.Printf("    %s✗ %v%s\n", Fmt.Red, err, Fmt.Reset)
			} else if created {
				totalSynced++
				stats.LinesCreated++
				stats.PayoutsTotal -= poAmount
				fmt.Printf("    %s✓ 1 line (withdrawal: -%.2f)%s\n", Fmt.Green, poAmount, Fmt.Reset)
			} else {
				stats.LinesSkipped++
			}
			continue
		}

		// Build all lines for this payout in one batch
		var batchLines []map[string]interface{}
		var totalFeeCents int64
		var chargeFeeCents int64
		var poCharges int
		var poChargesGross float64
		var poRefunds int
		var poRefundsTotal float64
		for _, tx := range poTxs {
			txDate := time.Unix(tx.Created, 0).In(BrusselsTZ())
			importID := fmt.Sprintf("stripe:%s:%s:0", strings.ToLower(acc.AccountID), strings.ToLower(tx.ID))

			switch tx.Type {
			case "charge", "payment":
				gross := centsToEuros(tx.Amount)
				totalFeeCents += tx.Fee
				chargeFeeCents += tx.Fee
				poCharges++
				poChargesGross += gross

				paymentRef := tx.CustomerName
				if paymentRef == "" {
					paymentRef = tx.Description
				}
				if paymentRef == "" {
					paymentRef = "Stripe charge"
				}

				partnerID := resolveOdooPartner(creds, uid, tx.CustomerName, tx.CustomerEmail, partnerCache, stats)

				lineData := map[string]interface{}{
					"statement_id":     stmtID,
					"journal_id":       acc.OdooJournalID,
					"date":             txDate.Format("2006-01-02"),
					"payment_ref":      paymentRef,
					"amount":           gross,
					"unique_import_id": importID,
				}
				if partnerID > 0 {
					lineData["partner_id"] = partnerID
				}
				batchLines = append(batchLines, lineData)

			case "refund", "payment_refund":
				refundAmt := centsToEuros(tx.Amount)
				poRefunds++
				poRefundsTotal += refundAmt // negative
				paymentRef := tx.Description
				if paymentRef == "" {
					paymentRef = "Refund"
				}
				batchLines = append(batchLines, map[string]interface{}{
					"statement_id":     stmtID,
					"journal_id":       acc.OdooJournalID,
					"date":             txDate.Format("2006-01-02"),
					"payment_ref":      paymentRef,
					"amount":           refundAmt,
					"unique_import_id": importID,
				})

			case "payout":
				payoutAmt := centsToEuros(tx.Amount)
				batchLines = append(batchLines, map[string]interface{}{
					"statement_id":     stmtID,
					"journal_id":       acc.OdooJournalID,
					"date":             txDate.Format("2006-01-02"),
					"payment_ref":      fmt.Sprintf("Payout to bank %s", arrivalDate.Format("2006-01-02")),
					"amount":           payoutAmt,
					"unique_import_id": importID,
				})

			default:
				// stripe_fee, adjustment, etc. — include in payout fee aggregation
				totalFeeCents += -tx.Net
				stats.StripeFees += centsToEuros(-tx.Net)
			}
		}

		// Add aggregated fee line
		totalFees := centsToEuros(totalFeeCents)
		if totalFees > 0 {
			batchLines = append(batchLines, map[string]interface{}{
				"statement_id":     stmtID,
				"journal_id":       acc.OdooJournalID,
				"date":             arrivalDate.Format("2006-01-02"),
				"payment_ref":      fmt.Sprintf("Stripe fees (payout %s)", arrivalDate.Format("2006-01-02")),
				"amount":           roundCents(-totalFees),
				"unique_import_id": fmt.Sprintf("stripe-fees:%s:%s:0", strings.ToLower(acc.AccountID), strings.ToLower(po.ID)),
			})
		}

		// Batch create all lines for this payout
		linesCreated, _ := batchCreateStatementLines(creds, uid, batchLines)
		skipped := len(batchLines) - linesCreated
		totalSynced += linesCreated
		stats.LinesCreated += linesCreated
		stats.LinesSkipped += skipped
		stats.Statements++
		stats.Charges += poCharges
		stats.ChargesGross += poChargesGross
		stats.Refunds += poRefunds
		stats.RefundsTotal += poRefundsTotal
		stats.ChargeFees += centsToEuros(chargeFeeCents)
		stats.PayoutsTotal -= poAmount
		fmt.Printf("    %s✓ %d lines (fees: -%.2f)%s\n", Fmt.Green, linesCreated, totalFees, Fmt.Reset)
	}

	// Save updated cache with tx counts
	if payoutsCache != nil {
		saveStripePayoutsCache(payoutsCache)
	}

	// Sync orphan charge lines — charges not already in any automatic payout statement.
	// This covers: charges in manual payouts + recent pending charges.
	if payoutFilter == "" {
		orphanCount, err := syncStripeOrphanChargesToOdoo(acc, creds, uid, monthsLimit, untilDate, partnerCache, dryRun, stats, autoPayoutTxIDs)
		if err != nil {
			fmt.Printf("  %s⚠ Orphan charges sync error: %v%s\n", Fmt.Yellow, err, Fmt.Reset)
		} else if orphanCount > 0 {
			totalSynced += orphanCount
		}
	}

	if !dryRun {
		stats.print()
	} else {
		fmt.Printf("\n  %s✓ Dry run: %d payouts%s\n\n", Fmt.Green, len(toSync), Fmt.Reset)
	}
	return nil
}

// batchCreateStatementLines creates multiple statement lines in one Odoo API call.
// Returns the number of successfully created lines. Skips duplicates silently.
// Falls back to one-by-one on batch failure.
func batchCreateStatementLines(creds *OdooCredentials, uid int, lines []map[string]interface{}) (int, error) {
	if len(lines) == 0 {
		return 0, nil
	}

	// Convert to []interface{} for Odoo RPC
	records := make([]interface{}, len(lines))
	for i, l := range lines {
		records[i] = l
	}

	result, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement.line", "create",
		[]interface{}{records}, nil)
	if err == nil {
		var ids []int
		json.Unmarshal(result, &ids)
		return len(ids), nil
	}

	// Batch failed (likely duplicate import IDs) — fall back to one-by-one
	created := 0
	for _, l := range lines {
		_, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
			"account.bank.statement.line", "create",
			[]interface{}{[]interface{}{l}}, nil)
		if err == nil {
			created++
		}
	}
	return created, nil
}

// createOrAdoptStatementLine creates a statement line, or if it already exists as an orphan
// (from pending sync), adopts it into the given statement.
func createOrAdoptStatementLine(creds *OdooCredentials, uid int, lineData map[string]interface{}) (created bool, err error) {
	_, err = odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement.line", "create",
		[]interface{}{[]interface{}{lineData}}, nil)
	if err == nil {
		return true, nil
	}

	if !strings.Contains(err.Error(), "imported only once") {
		return false, err
	}

	// Line already exists — find it and move into this statement
	importID, _ := lineData["unique_import_id"].(string)
	stmtID := lineData["statement_id"]
	if importID == "" || stmtID == nil {
		return false, nil // can't adopt without import ID or statement
	}

	existResult, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement.line", "search_read",
		[]interface{}{[]interface{}{
			[]interface{}{"unique_import_id", "=", importID},
		}},
		map[string]interface{}{"fields": []string{"id", "statement_id", "move_id"}, "limit": 1})
	if err != nil {
		return false, nil
	}
	var lines []struct {
		ID          int         `json:"id"`
		StatementID interface{} `json:"statement_id"`
		MoveID      interface{} `json:"move_id"`
	}
	json.Unmarshal(existResult, &lines)
	if len(lines) == 0 {
		return false, nil
	}

	existingStmtID := odooFieldID(lines[0].StatementID)
	if existingStmtID != 0 {
		// Already in a statement — skip (not an orphan)
		return false, nil
	}

	// Orphan line — adopt into the payout statement
	// Need to reset the move to draft to modify the statement_id
	if mid := odooFieldID(lines[0].MoveID); mid > 0 {
		odooExec(creds.URL, creds.DB, uid, creds.Password,
			"account.move", "button_draft",
			[]interface{}{[]interface{}{mid}}, nil)
	}
	_, err = odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement.line", "write",
		[]interface{}{[]interface{}{lines[0].ID}, map[string]interface{}{
			"statement_id": stmtID,
		}}, nil)
	if err != nil {
		return false, fmt.Errorf("failed to adopt line: %v", err)
	}
	// Re-post the move
	if mid := odooFieldID(lines[0].MoveID); mid > 0 {
		odooExec(creds.URL, creds.DB, uid, creds.Password,
			"account.move", "action_post",
			[]interface{}{[]interface{}{mid}}, nil)
	}

	return true, nil
}

// syncStripeOrphanChargesToOdoo syncs all local charge transactions not already in Odoo
// as orphan lines (no statement). This covers charges in manual payouts and recent pending charges.
// Charges already synced via automatic payout statements are skipped (same unique_import_id).
func syncStripeOrphanChargesToOdoo(acc *AccountConfig, creds *OdooCredentials, uid int, monthsLimit int, untilDate time.Time, partnerCache map[string]int, dryRun bool, stats *syncStats, autoPayoutTxIDs map[string]bool) (int, error) {
	// Load all local transactions for this account
	localTxs := loadAccountTransactions(acc)

	// Apply date filters
	if monthsLimit > 0 {
		cutoff := time.Now().AddDate(0, -monthsLimit, 0)
		var filtered []TransactionEntry
		for _, tx := range localTxs {
			if time.Unix(tx.Timestamp, 0).After(cutoff) {
				filtered = append(filtered, tx)
			}
		}
		localTxs = filtered
	}
	if !untilDate.IsZero() {
		var filtered []TransactionEntry
		for _, tx := range localTxs {
			if time.Unix(tx.Timestamp, 0).Before(untilDate) {
				filtered = append(filtered, tx)
			}
		}
		localTxs = filtered
	}

	// Fetch existing IDs from Odoo
	existingIDs, _ := fetchOdooImportIDs(creds.URL, creds.DB, uid, creds.Password, acc.OdooJournalID)

	// Find charges not yet in Odoo
	var toSync []TransactionEntry
	for _, tx := range localTxs {
		importID := buildUniqueImportID(acc, tx)
		if existingIDs[importID] {
			continue // already in Odoo (e.g. charge in auto payout statement)
		}
		// Skip billing fee DEBITs that are already aggregated in an auto payout fee line
		if tx.Type == "DEBIT" && autoPayoutTxIDs[tx.TxHash] {
			continue
		}
		toSync = append(toSync, tx)
	}

	if len(toSync) == 0 {
		return 0, nil
	}

	fmt.Printf("\n  %s📋 %d orphan transactions (not in any payout statement)%s\n", Fmt.Dim, len(toSync), Fmt.Reset)

	// Pre-resolve all partners to avoid per-line API calls
	if !dryRun {
		uniquePartners := map[string]string{} // name → email
		for _, tx := range toSync {
			name := tx.Counterparty
			email, _ := tx.Metadata["email"].(string)
			if name != "" {
				if _, exists := uniquePartners[name]; !exists {
					uniquePartners[name] = email
				}
			}
		}
		fmt.Printf("    %sResolving %d partners...%s\n", Fmt.Dim, len(uniquePartners), Fmt.Reset)
		for name, email := range uniquePartners {
			resolveOdooPartner(creds, uid, name, email, partnerCache, stats)
		}
	}

	if dryRun {
		var totalGross float64
		for _, tx := range toSync {
			amt := tx.GrossAmount
			if tx.Type == "DEBIT" {
				amt = -amt
			}
			totalGross += amt
		}
		fmt.Printf("    %sWould create %d orphan lines, total gross: %.2f%s\n", Fmt.Dim, len(toSync), totalGross, Fmt.Reset)
		return len(toSync), nil
	}

	// Build all orphan lines + monthly fee lines, then batch create
	var batchLines []map[string]interface{}
	monthlyFeeCents := map[string]int64{}
	var orphanFeeCents int64

	for _, tx := range toSync {
		txDate := time.Unix(tx.Timestamp, 0).In(BrusselsTZ())
		importID := buildUniqueImportID(acc, tx)

		amt := roundCents(tx.GrossAmount)
		if amt == 0 {
			amt = roundCents(math.Abs(tx.Amount) + tx.Fee)
		}

		// Track stats by type
		if tx.Type == "DEBIT" {
			amt = -amt
			// Distinguish refunds from Stripe billing fees using metadata.category
			metaCat, _ := tx.Metadata["category"].(string)
			if metaCat == "fee" {
				stats.StripeFees += -amt // positive
			} else {
				stats.Refunds++
				stats.RefundsTotal += amt // negative
			}
		} else {
			stats.Charges++
			stats.ChargesGross += amt
		}

		// Accumulate fees for monthly fee lines (using cents for precision)
		if tx.Fee > 0 {
			feeCents := int64(math.Round(tx.Fee * 100))
			ym := txDate.Format("2006-01")
			monthlyFeeCents[ym] += feeCents
			orphanFeeCents += feeCents
		}

		paymentRef := tx.Counterparty
		if paymentRef == "" {
			if d, ok := tx.Metadata["description"].(string); ok && d != "" {
				paymentRef = d
			}
		}
		if paymentRef == "" {
			paymentRef = "Stripe charge"
		}

		partnerEmail, _ := tx.Metadata["email"].(string)
		partnerID := resolveOdooPartner(creds, uid, tx.Counterparty, partnerEmail, partnerCache)

		lineData := map[string]interface{}{
			"journal_id":       acc.OdooJournalID,
			"date":             txDate.Format("2006-01-02"),
			"payment_ref":      paymentRef,
			"amount":           amt,
			"unique_import_id": importID,
		}
		if partnerID > 0 {
			lineData["partner_id"] = partnerID
		}
		batchLines = append(batchLines, lineData)
	}

	// Add monthly fee lines (computed from cent totals for precision)
	for ym, feeCents := range monthlyFeeCents {
		if feeCents == 0 {
			continue
		}
		feeImportID := fmt.Sprintf("stripe-orphan-fees:%s:%s", strings.ToLower(acc.AccountID), ym)
		if existingIDs[feeImportID] {
			continue
		}
		parts := strings.Split(ym, "-")
		var yi, mi int
		fmt.Sscanf(parts[0], "%d", &yi)
		fmt.Sscanf(parts[1], "%d", &mi)
		lastDay := time.Date(yi, time.Month(mi)+1, 0, 0, 0, 0, 0, BrusselsTZ())

		batchLines = append(batchLines, map[string]interface{}{
			"journal_id":       acc.OdooJournalID,
			"date":             lastDay.Format("2006-01-02"),
			"payment_ref":      fmt.Sprintf("Stripe fees %s", ym),
			"amount":           centsToEuros(-feeCents),
			"unique_import_id": feeImportID,
		})
	}

	// Update stats with cent-precise fee total
	stats.ChargeFees += centsToEuros(orphanFeeCents)

	// Batch create in chunks of 100
	linesCreated := 0
	chunkSize := 100
	for i := 0; i < len(batchLines); i += chunkSize {
		end := i + chunkSize
		if end > len(batchLines) {
			end = len(batchLines)
		}
		chunk := batchLines[i:end]
		n, _ := batchCreateStatementLines(creds, uid, chunk)
		linesCreated += n
		if i+chunkSize < len(batchLines) {
			fmt.Printf("    %s... %d/%d%s\r", Fmt.Dim, linesCreated, len(batchLines), Fmt.Reset)
		}
	}

	// Track stats
	skipped := len(batchLines) - linesCreated
	stats.LinesCreated += linesCreated
	stats.LinesSkipped += skipped

	if linesCreated > 0 {
		fmt.Printf("    %s✓ %d orphan lines created (incl. monthly fee lines)%s\n", Fmt.Green, linesCreated, Fmt.Reset)
	}

	return linesCreated, nil
}

// stripePayout represents a Stripe payout object.
type stripePayout struct {
	ID                  string `json:"id"`
	Amount              int64  `json:"amount"`
	ArrivalDate         int64  `json:"arrival_date"`
	Created             int64  `json:"created"`
	Currency            string `json:"currency"`
	Status              string `json:"status"`
	Description         string `json:"description,omitempty"`          // e.g. "STRIPE PAYOUT"
	StatementDescriptor string `json:"statement_descriptor,omitempty"` // custom bank statement text
	Automatic           bool   `json:"automatic,omitempty"`            // true for scheduled payouts
	TxCount             int    `json:"txCount,omitempty"`              // cached transaction count
	BankLast4           string `json:"bankLast4,omitempty"`            // last 4 digits of bank account
	BankName            string `json:"bankName,omitempty"`             // bank name
}

// statementName returns a human-readable name for an Odoo bank statement.
func (p stripePayout) statementName() string {
	date := time.Unix(p.ArrivalDate, 0).In(BrusselsTZ()).Format("2006-01-02")
	amount := float64(p.Amount) / 100
	if p.BankLast4 != "" {
		return fmt.Sprintf("%s Stripe → ****%s (%.2f %s)", date, p.BankLast4, amount, strings.ToUpper(p.Currency))
	}
	return fmt.Sprintf("%s Stripe payout (%.2f %s)", date, amount, strings.ToUpper(p.Currency))
}

// stripePayoutsCache is the structure saved to disk.
type stripePayoutsCache struct {
	FetchedAt string         `json:"fetchedAt"`
	Payouts   []stripePayout `json:"payouts"`
}

func stripePayoutsCachePath() string {
	return filepath.Join(DataDir(), "latest", "stripe-payouts.json")
}

func loadStripePayoutsCache() *stripePayoutsCache {
	data, err := os.ReadFile(stripePayoutsCachePath())
	if err != nil {
		return nil
	}
	var cache stripePayoutsCache
	if json.Unmarshal(data, &cache) != nil {
		return nil
	}
	return &cache
}

func saveStripePayoutsCache(cache *stripePayoutsCache) {
	data, _ := json.MarshalIndent(cache, "", "  ")
	dir := filepath.Dir(stripePayoutsCachePath())
	os.MkdirAll(dir, 0755)
	os.WriteFile(stripePayoutsCachePath(), data, 0644)
}

// refreshStripePayoutsCache fetches new payouts from Stripe API and merges with cache.
// Only fetches payouts created after the latest cached one.
func refreshStripePayoutsCache(apiKey string) ([]stripePayout, error) {
	cache := loadStripePayoutsCache()

	// Find the most recent cached payout's created timestamp for incremental fetch
	var fetchAfter int64
	cachedByID := map[string]bool{}
	if cache != nil {
		for _, po := range cache.Payouts {
			cachedByID[po.ID] = true
			if po.Created > fetchAfter {
				fetchAfter = po.Created
			}
		}
	}

	// Fetch new payouts from Stripe (only those created after the latest cached one)
	newPayouts, err := fetchStripePayoutsFromAPI(apiKey, fetchAfter)
	if err != nil {
		// If API fails but we have cache, return cached data
		if cache != nil {
			return cache.Payouts, nil
		}
		return nil, err
	}

	// Merge: add new payouts not already in cache, preserve tx counts from cache
	txCounts := map[string]int{}
	var merged []stripePayout
	if cache != nil {
		for _, po := range cache.Payouts {
			if po.TxCount > 0 {
				txCounts[po.ID] = po.TxCount
			}
		}
		merged = append(merged, cache.Payouts...)
	}
	added := 0
	for _, po := range newPayouts {
		if !cachedByID[po.ID] {
			merged = append(merged, po)
			added++
		}
	}

	// Update status of cached payouts that may have changed
	freshByID := map[string]stripePayout{}
	for _, po := range newPayouts {
		freshByID[po.ID] = po
	}
	for i, po := range merged {
		if fresh, ok := freshByID[po.ID]; ok {
			// Preserve cached TxCount
			fresh.TxCount = txCounts[po.ID]
			merged[i] = fresh
		}
	}

	// Sort by arrival_date descending (most recent first)
	sort.Slice(merged, func(i, j int) bool {
		return merged[i].ArrivalDate > merged[j].ArrivalDate
	})

	// Save updated cache
	saveStripePayoutsCache(&stripePayoutsCache{
		FetchedAt: time.Now().UTC().Format(time.RFC3339),
		Payouts:   merged,
	})

	if added > 0 {
		fmt.Printf("  %s%d new payouts fetched, %d total cached%s\n", Fmt.Dim, added, len(merged), Fmt.Reset)
	}

	return merged, nil
}

func filterPayoutsByMonths(payouts []stripePayout, monthsLimit int) []stripePayout {
	if monthsLimit <= 0 {
		return payouts
	}
	cutoff := time.Now().AddDate(0, -monthsLimit, 0).Unix()
	var filtered []stripePayout
	for _, po := range payouts {
		if po.ArrivalDate >= cutoff {
			filtered = append(filtered, po)
		}
	}
	return filtered
}

// fetchStripePayoutsFromAPI fetches payouts from Stripe API, optionally only those created after a timestamp.
func fetchStripePayoutsFromAPI(apiKey string, createdAfter int64) ([]stripePayout, error) {
	baseURL := "https://api.stripe.com/v1/payouts?limit=100&status=paid&expand[]=data.destination"
	if createdAfter > 0 {
		baseURL += fmt.Sprintf("&created[gt]=%d", createdAfter)
	}

	var allPayouts []stripePayout
	startingAfter := ""

	for {
		reqURL := baseURL
		if startingAfter != "" {
			reqURL += "&starting_after=" + startingAfter
		}

		req, err := http.NewRequest("GET", reqURL, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			return nil, fmt.Errorf("Stripe API returned %d", resp.StatusCode)
		}

		// Parse with raw destination for bank details extraction
		var listResp struct {
			Data    []json.RawMessage `json:"data"`
			HasMore bool              `json:"has_more"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&listResp); err != nil {
			return nil, err
		}

		for _, raw := range listResp.Data {
			var po stripePayout
			json.Unmarshal(raw, &po)

			// Extract bank details from expanded destination
			var expanded struct {
				Destination *struct {
					Last4    string `json:"last4"`
					BankName string `json:"bank_name"`
				} `json:"destination"`
			}
			json.Unmarshal(raw, &expanded)
			if expanded.Destination != nil {
				po.BankLast4 = expanded.Destination.Last4
				po.BankName = expanded.Destination.BankName
			}

			allPayouts = append(allPayouts, po)
		}

		if !listResp.HasMore || len(listResp.Data) == 0 {
			break
		}
		startingAfter = allPayouts[len(allPayouts)-1].ID
		time.Sleep(200 * time.Millisecond)
	}

	return allPayouts, nil
}

// fetchPayoutTransactions fetches all balance transactions for a specific payout.
func fetchPayoutTransactions(apiKey, payoutID string) ([]StripeTransaction, error) {
	var allTxs []StripeTransaction
	startingAfter := ""

	for {
		url := fmt.Sprintf("https://api.stripe.com/v1/balance_transactions?payout=%s&limit=100&expand[]=data.source&expand[]=data.source.customer",
			payoutID)
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
			return nil, err
		}
		defer resp.Body.Close()

		if resp.StatusCode == 429 {
			time.Sleep(2 * time.Second)
			continue
		}
		if resp.StatusCode == 400 {
			return nil, fmt.Errorf("manual payout")
		}
		if resp.StatusCode != 200 {
			return nil, fmt.Errorf("Stripe API returned %d", resp.StatusCode)
		}

		var listResp StripeListResponse
		if err := json.NewDecoder(resp.Body).Decode(&listResp); err != nil {
			return nil, err
		}

		for i := range listResp.Data {
			enrichStripeTransaction(&listResp.Data[i])
		}
		allTxs = append(allTxs, listResp.Data...)

		if !listResp.HasMore || len(listResp.Data) == 0 {
			break
		}
		startingAfter = listResp.Data[len(listResp.Data)-1].ID
		time.Sleep(200 * time.Millisecond)
	}

	return allTxs, nil
}

// emptyOdooJournal deletes all statement lines and statements for a journal after confirmation.
// In Odoo, each bank statement line auto-creates a journal entry (account.move).
// To delete: unreconcile → reset move to draft → delete move (which deletes the statement line).
func emptyOdooJournal(creds *OdooCredentials, uid int, journalID int, journalName string) error {
	// Count existing lines
	countResult, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement.line", "search_count",
		[]interface{}{[]interface{}{
			[]interface{}{"journal_id", "=", journalID},
		}}, nil)
	if err != nil {
		return fmt.Errorf("failed to count lines: %v", err)
	}
	var count int
	json.Unmarshal(countResult, &count)

	if count == 0 {
		fmt.Printf("  %sJournal '%s' is already empty%s\n\n", Fmt.Dim, journalName, Fmt.Reset)
		return nil
	}

	fmt.Printf("  %s⚠ This will delete %d statement lines from journal '%s'%s\n", Fmt.Yellow, count, journalName, Fmt.Reset)
	fmt.Printf("  %sType 'yes' to confirm: %s", Fmt.Bold, Fmt.Reset)
	reader := bufio.NewReader(os.Stdin)
	confirm, _ := reader.ReadString('\n')
	confirm = strings.TrimSpace(confirm)
	if confirm != "yes" {
		return fmt.Errorf("cancelled")
	}

	// Get statement lines with their moves
	linesData, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement.line", "search_read",
		[]interface{}{[]interface{}{
			[]interface{}{"journal_id", "=", journalID},
		}},
		map[string]interface{}{
			"fields": []string{"id", "move_id", "is_reconciled"},
			"limit":  0,
		})
	if err != nil {
		return fmt.Errorf("failed to fetch lines: %v", err)
	}

	type stmtLine struct {
		ID             int         `json:"id"`
		MoveID         interface{} `json:"move_id"`
		IsReconciled   bool        `json:"is_reconciled"`
	}
	var lines []stmtLine
	json.Unmarshal(linesData, &lines)

	// Step 1: Unreconcile any reconciled move lines
	var moveIDs []int
	for _, l := range lines {
		if mid := odooFieldID(l.MoveID); mid > 0 {
			moveIDs = append(moveIDs, mid)
		}
	}

	if len(moveIDs) > 0 {
		// Find reconciled move lines for these moves and remove reconciliation
		moveIDsIface := make([]interface{}, len(moveIDs))
		for i, id := range moveIDs {
			moveIDsIface[i] = id
		}

		reconciledLines, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
			"account.move.line", "search",
			[]interface{}{[]interface{}{
				[]interface{}{"move_id", "in", moveIDsIface},
				[]interface{}{"reconciled", "=", true},
			}}, nil)
		if err == nil {
			var reconIDs []int
			json.Unmarshal(reconciledLines, &reconIDs)
			if len(reconIDs) > 0 {
				reconIDsIface := make([]interface{}, len(reconIDs))
				for i, id := range reconIDs {
					reconIDsIface[i] = id
				}
				_, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
					"account.move.line", "remove_move_reconcile",
					[]interface{}{reconIDsIface}, nil)
				if err != nil {
					fmt.Printf("  %s⚠ Failed to unreconcile some lines: %v%s\n", Fmt.Yellow, err, Fmt.Reset)
				}
			}
		}

		// Step 2: Reset moves to draft
		_, err = odooExec(creds.URL, creds.DB, uid, creds.Password,
			"account.move", "button_draft",
			[]interface{}{moveIDsIface}, nil)
		if err != nil {
			fmt.Printf("  %s⚠ Failed to reset moves to draft: %v%s\n", Fmt.Yellow, err, Fmt.Reset)
		}

		// Step 3: Delete the moves (this cascades to delete statement lines)
		_, err = odooExec(creds.URL, creds.DB, uid, creds.Password,
			"account.move", "unlink",
			[]interface{}{moveIDsIface}, nil)
		if err != nil {
			fmt.Printf("  %s⚠ Failed to delete moves: %v%s\n", Fmt.Yellow, err, Fmt.Reset)
			// Fall back to trying to delete statement lines directly
			lineIDs := make([]interface{}, len(lines))
			for i, l := range lines {
				lineIDs[i] = l.ID
			}
			_, err2 := odooExec(creds.URL, creds.DB, uid, creds.Password,
				"account.bank.statement.line", "unlink",
				[]interface{}{lineIDs}, nil)
			if err2 != nil {
				return fmt.Errorf("failed to delete statement lines: %v (move deletion also failed: %v)", err2, err)
			}
		}
	}

	// Delete empty statements
	stmtIDs, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement", "search",
		[]interface{}{[]interface{}{
			[]interface{}{"journal_id", "=", journalID},
		}}, nil)
	if err == nil {
		var sids []int
		json.Unmarshal(stmtIDs, &sids)
		if len(sids) > 0 {
			sidsIface := make([]interface{}, len(sids))
			for i, id := range sids {
				sidsIface[i] = id
			}
			odooExec(creds.URL, creds.DB, uid, creds.Password,
				"account.bank.statement", "unlink",
				[]interface{}{sidsIface}, nil)
		}
	}

	// Verify deletion worked
	verifyResult, _ := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement.line", "search_count",
		[]interface{}{[]interface{}{
			[]interface{}{"journal_id", "=", journalID},
		}}, nil)
	var remaining int
	json.Unmarshal(verifyResult, &remaining)

	if remaining > 0 {
		return fmt.Errorf("could not delete all lines: %d of %d remain (may be reconciled or locked)", remaining, count)
	}

	fmt.Printf("  %s✓ Emptied journal '%s' (%d lines deleted)%s\n\n", Fmt.Green, journalName, count, Fmt.Reset)
	return nil
}

// buildOdooNarration creates a JSON blob with transaction details for the Odoo narration field.
func buildOdooNarration(acc *AccountConfig, tx TransactionEntry) string {
	details := map[string]interface{}{}

	// Core identifiers
	details["provider"] = tx.Provider
	if tx.TxHash != "" {
		details["txHash"] = tx.TxHash
	}
	if tx.Chain != nil {
		details["chain"] = *tx.Chain
	}
	if tx.StripeChargeID != "" {
		details["stripeChargeId"] = tx.StripeChargeID
	}

	// Categorization
	if tx.Collective != "" {
		details["collective"] = tx.Collective
	}
	if tx.Category != "" {
		details["category"] = tx.Category
	}
	if tx.Event != "" {
		details["event"] = tx.Event
	}

	// Fee info
	if tx.Fee > 0 {
		details["fee"] = tx.Fee
		details["grossAmount"] = tx.GrossAmount
	}

	// Selected metadata (non-PII)
	for _, key := range []string{"application", "paymentMethod", "stripe_from", "stripe_to", "stripe_orderId", "stripe_event_api_id"} {
		if v, ok := tx.Metadata[key].(string); ok && v != "" {
			details[key] = v
		}
	}

	if len(details) == 0 {
		return ""
	}

	data, _ := json.Marshal(details)
	return string(data)
}

// resolveOdooPartner finds or creates a partner in Odoo.
// Priority: email match → exact name match → skip if ambiguous → create new.
func resolveOdooPartner(creds *OdooCredentials, uid int, name, email string, cache map[string]int, stats ...*syncStats) int {
	var st *syncStats
	if len(stats) > 0 {
		st = stats[0]
	}
	if name == "" && email == "" {
		return 0
	}

	// Check cache first (keyed by email if available, else name)
	cacheKey := email
	if cacheKey == "" {
		cacheKey = name
	}
	if id, ok := cache[cacheKey]; ok {
		return id
	}

	type partner struct {
		ID    int         `json:"id"`
		Name  string      `json:"name"`
		Email interface{} `json:"email"`
	}

	// 1. Search by email (most reliable, email is unique-ish)
	if email != "" {
		result, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
			"res.partner", "search_read",
			[]interface{}{[]interface{}{
				[]interface{}{"email", "=", email},
			}},
			map[string]interface{}{
				"fields": []string{"id", "name", "email"},
				"limit":  1,
			})
		if err == nil {
			var partners []partner
			json.Unmarshal(result, &partners)
			if len(partners) > 0 {
				cache[cacheKey] = partners[0].ID
				if st != nil {
					st.PartnersMatched++
				}
				return partners[0].ID
			}
		}
	}

	// 2. Search by exact name match
	if name != "" {
		result, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
			"res.partner", "search_read",
			[]interface{}{[]interface{}{
				[]interface{}{"name", "=", name},
			}},
			map[string]interface{}{
				"fields": []string{"id", "name", "email"},
				"limit":  5,
			})
		if err == nil {
			var partners []partner
			json.Unmarshal(result, &partners)
			if len(partners) == 1 {
				existingEmail, _ := partners[0].Email.(string)
				if email != "" && existingEmail == "" {
					odooExec(creds.URL, creds.DB, uid, creds.Password,
						"res.partner", "write",
						[]interface{}{[]interface{}{partners[0].ID}, map[string]interface{}{
							"email": email,
						}}, nil)
				}
				cache[cacheKey] = partners[0].ID
				if st != nil {
					st.PartnersMatched++
				}
				return partners[0].ID
			}
			if len(partners) > 1 {
				cache[cacheKey] = 0
				if st != nil {
					st.PartnersSkipped++
					entry := fmt.Sprintf("%s <%s>", name, email)
					st.Ambiguous = append(st.Ambiguous, entry)
				}
				return 0
			}
		}
	}

	// 3. No match — create new partner
	if name == "" {
		return 0
	}
	partnerData := map[string]interface{}{
		"name": name,
	}
	if email != "" {
		partnerData["email"] = email
	}
	createResult, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"res.partner", "create",
		[]interface{}{[]interface{}{partnerData}}, nil)
	if err == nil {
		var ids []int
		json.Unmarshal(createResult, &ids)
		if len(ids) > 0 {
			cache[cacheKey] = ids[0]
			if st != nil {
				st.PartnersCreated++
			}
			return ids[0]
		}
	}

	return 0
}

// loadAccountTransactions loads all non-INTERNAL transactions for a specific account.
func loadAccountTransactions(acc *AccountConfig) []TransactionEntry {
	dataDir := DataDir()
	var result []TransactionEntry

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
			txFile := LoadTransactionsWithPII(dataDir, yd.Name(), md.Name())
			if txFile == nil {
				continue
			}
			for _, tx := range txFile.Transactions {
				if tx.Type == "INTERNAL" {
					continue
				}
				// Match by provider-specific criteria
				if acc.Provider == "stripe" {
					// Stripe: match by provider and accountSlug (Stripe account ID)
					if tx.Provider == "stripe" && strings.EqualFold(tx.AccountSlug, acc.AccountID) {
						result = append(result, tx)
					}
				} else {
					// Blockchain: match by account slug or wallet address
					if strings.EqualFold(tx.AccountSlug, acc.Slug) ||
						(acc.Address != "" && strings.EqualFold(tx.Account, acc.Address)) {
						result = append(result, tx)
					}
				}
			}
		}
	}

	return result
}

// buildUniqueImportID creates the dedup key for Odoo.
// Blockchain format (matching odoo-web3): {chain}:{walletAddress}:{txHash}:{logIndex}
// Stripe format:                          stripe:{accountId}:{txn_id}:0
func buildUniqueImportID(acc *AccountConfig, tx TransactionEntry) string {
	if acc.Provider == "stripe" {
		accountID := acc.AccountID
		if accountID == "" {
			accountID = acc.Slug
		}
		txnID := tx.TxHash
		if txnID == "" {
			txnID = tx.ID
		}
		return fmt.Sprintf("stripe:%s:%s:0", strings.ToLower(accountID), strings.ToLower(txnID))
	}

	chain := acc.Chain
	if chain == "" {
		chain = tx.Provider
	}
	address := acc.Address
	if address == "" {
		address = acc.Slug
	}
	txHash := tx.TxHash
	if txHash == "" {
		txHash = tx.ID
	}
	return fmt.Sprintf("%s:%s:%s:0", chain, strings.ToLower(address), strings.ToLower(txHash))
}

// fetchOdooImportIDs returns the set of unique_import_id values already in Odoo for a journal.
func fetchOdooImportIDs(odooURL, db string, uid int, password string, journalID int) (map[string]bool, error) {
	result := map[string]bool{}

	// Fetch all statement lines for this journal with their unique_import_id
	data, err := odooExec(odooURL, db, uid, password,
		"account.bank.statement.line", "search_read",
		[]interface{}{[]interface{}{
			[]interface{}{"journal_id", "=", journalID},
		}},
		map[string]interface{}{
			"fields": []string{"unique_import_id"},
			"limit":  0, // no limit
		})
	if err != nil {
		return nil, err
	}

	var lines []struct {
		UniqueImportID interface{} `json:"unique_import_id"`
	}
	json.Unmarshal(data, &lines)

	for _, line := range lines {
		if s, ok := line.UniqueImportID.(string); ok && s != "" {
			result[s] = true
		}
	}

	return result, nil
}

// deleteOdooStatementByName finds and deletes a statement and its lines by name or reference.
func deleteOdooStatementByName(creds *OdooCredentials, uid int, journalID int, name string) {
	// Find the statement by name or reference (payout ID)
	result, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement", "search_read",
		[]interface{}{[]interface{}{
			[]interface{}{"journal_id", "=", journalID},
			"|",
			[]interface{}{"name", "=", name},
			[]interface{}{"reference", "=", name},
		}},
		map[string]interface{}{"fields": []string{"id", "line_ids"}, "limit": 1})
	if err != nil {
		return
	}
	var stmts []struct {
		ID      int   `json:"id"`
		LineIDs []int `json:"line_ids"`
	}
	json.Unmarshal(result, &stmts)
	if len(stmts) == 0 {
		return
	}

	stmt := stmts[0]

	// Get move IDs for the statement lines
	if len(stmt.LineIDs) > 0 {
		lineIDsIface := make([]interface{}, len(stmt.LineIDs))
		for i, id := range stmt.LineIDs {
			lineIDsIface[i] = id
		}

		linesData, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
			"account.bank.statement.line", "search_read",
			[]interface{}{[]interface{}{
				[]interface{}{"id", "in", lineIDsIface},
			}},
			map[string]interface{}{"fields": []string{"move_id"}})
		if err == nil {
			var lines []struct {
				MoveID interface{} `json:"move_id"`
			}
			json.Unmarshal(linesData, &lines)
			var moveIDs []interface{}
			for _, l := range lines {
				if mid := odooFieldID(l.MoveID); mid > 0 {
					moveIDs = append(moveIDs, mid)
				}
			}
			if len(moveIDs) > 0 {
				// Unreconcile
				reconLines, _ := odooExec(creds.URL, creds.DB, uid, creds.Password,
					"account.move.line", "search",
					[]interface{}{[]interface{}{
						[]interface{}{"move_id", "in", moveIDs},
						[]interface{}{"reconciled", "=", true},
					}}, nil)
				var reconIDs []int
				json.Unmarshal(reconLines, &reconIDs)
				if len(reconIDs) > 0 {
					rIface := make([]interface{}, len(reconIDs))
					for i, id := range reconIDs {
						rIface[i] = id
					}
					odooExec(creds.URL, creds.DB, uid, creds.Password,
						"account.move.line", "remove_move_reconcile",
						[]interface{}{rIface}, nil)
				}
				// Reset to draft and delete
				odooExec(creds.URL, creds.DB, uid, creds.Password,
					"account.move", "button_draft", []interface{}{moveIDs}, nil)
				odooExec(creds.URL, creds.DB, uid, creds.Password,
					"account.move", "unlink", []interface{}{moveIDs}, nil)
			}
		}
	}

	// Delete the statement itself
	odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement", "unlink",
		[]interface{}{[]interface{}{stmt.ID}}, nil)
}

// fetchSinglePayout fetches a single payout by ID from Stripe.
func fetchSinglePayout(apiKey, payoutID string) (*stripePayout, error) {
	url := fmt.Sprintf("https://api.stripe.com/v1/payouts/%s?expand[]=destination", payoutID)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("Stripe API returned %d", resp.StatusCode)
	}

	var raw json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, err
	}

	var po stripePayout
	json.Unmarshal(raw, &po)

	var expanded struct {
		Destination *struct {
			Last4    string `json:"last4"`
			BankName string `json:"bank_name"`
		} `json:"destination"`
	}
	json.Unmarshal(raw, &expanded)
	if expanded.Destination != nil {
		po.BankLast4 = expanded.Destination.Last4
		po.BankName = expanded.Destination.BankName
	}
	return &po, nil
}

// AccountStripePayouts lists Stripe payouts from cache (no API calls unless --refresh).
func AccountStripePayouts(slug string, args []string) error {
	monthsLimit := 0
	for i, a := range args {
		if a == "--months" && i+1 < len(args) {
			fmt.Sscanf(args[i+1], "%d", &monthsLimit)
		}
	}

	if HasFlag(args, "--refresh") {
		stripeKey := os.Getenv("STRIPE_SECRET_KEY")
		if stripeKey == "" {
			return fmt.Errorf("STRIPE_SECRET_KEY not set")
		}
		fmt.Printf("\n  %sRefreshing payouts from Stripe...%s\n", Fmt.Dim, Fmt.Reset)
		// Full refresh: clear cache to re-fetch all payouts with expanded data
		os.Remove(stripePayoutsCachePath())
		_, err := refreshStripePayoutsCache(stripeKey)
		if err != nil {
			return fmt.Errorf("failed to refresh: %v", err)
		}
	}

	cache := loadStripePayoutsCache()
	if cache == nil || len(cache.Payouts) == 0 {
		fmt.Printf("\n  %sNo cached payouts. Run 'chb accounts %s payouts --refresh'.%s\n\n", Fmt.Dim, slug, Fmt.Reset)
		return nil
	}

	payouts := filterPayoutsByMonths(cache.Payouts, monthsLimit)
	if len(payouts) == 0 {
		fmt.Printf("\n  %sNo payouts in the selected range%s\n\n", Fmt.Dim, Fmt.Reset)
		return nil
	}

	// Check which payouts are synced to Odoo
	syncedPayouts := map[string]bool{}
	configs := LoadAccountConfigs()
	for _, acc := range configs {
		if strings.EqualFold(acc.Slug, slug) && acc.OdooJournalID > 0 {
			creds, err := ResolveOdooCredentials()
			if err == nil {
				uid, err := odooAuth(creds.URL, creds.DB, creds.Login, creds.Password)
				if err == nil && uid > 0 {
					result, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
						"account.bank.statement", "search_read",
						[]interface{}{[]interface{}{
							[]interface{}{"journal_id", "=", acc.OdooJournalID},
						}},
						map[string]interface{}{"fields": []string{"name", "reference"}, "limit": 0})
					if err == nil {
						var stmts []struct {
							Name      string      `json:"name"`
							Reference interface{} `json:"reference"`
						}
						json.Unmarshal(result, &stmts)
						for _, s := range stmts {
							syncedPayouts[s.Name] = true
							if ref, ok := s.Reference.(string); ok && ref != "" {
								syncedPayouts[ref] = true
							}
						}
					}
				}
			}
			break
		}
	}

	fetchedAt := ""
	if t, err := time.Parse(time.RFC3339, cache.FetchedAt); err == nil {
		fetchedAt = fmt.Sprintf("  %s(cached %s)%s", Fmt.Dim, formatTimeAgo(t), Fmt.Reset)
	}

	fmt.Printf("\n%s💳 Stripe Payouts%s (%d)%s\n", Fmt.Bold, Fmt.Reset, len(payouts), fetchedAt)

	labelStyle := Fmt.Dim
	for _, po := range payouts {
		arrivalDate := time.Unix(po.ArrivalDate, 0).In(BrusselsTZ())
		amount := centsToEuros(po.Amount)

		// Header line: date, amount, auto/manual, sync status
		poType := "manual"
		if po.Automatic {
			poType = "auto"
		}
		fmt.Println()
		fmt.Printf("  %s%s%s  %s%.2f %s%s  %s(%s)%s",
			Fmt.Bold, arrivalDate.Format("2006-01-02"), Fmt.Reset,
			Fmt.Green, amount, strings.ToUpper(po.Currency), Fmt.Reset,
			Fmt.Dim, poType, Fmt.Reset)
		if syncedPayouts[po.ID] {
			fmt.Printf("  %s✓ synced%s", Fmt.Green, Fmt.Reset)
		}
		fmt.Println()

		// Description
		desc := po.Description
		if desc == "" {
			desc = "STRIPE PAYOUT"
		}
		fmt.Printf("    %sDescription:%s  %s\n", labelStyle, Fmt.Reset, desc)

		// Bank
		if po.BankLast4 != "" {
			bank := fmt.Sprintf("****%s", po.BankLast4)
			if po.BankName != "" {
				bank += fmt.Sprintf(" (%s)", po.BankName)
			}
			fmt.Printf("    %sBank:%s         %s\n", labelStyle, Fmt.Reset, bank)
		}

		// Statement descriptor or computed name
		if po.StatementDescriptor != "" {
			fmt.Printf("    %sDescriptor:%s   %s\n", labelStyle, Fmt.Reset, po.StatementDescriptor)
		} else {
			fmt.Printf("    %sStatement:%s    %s\n", labelStyle, Fmt.Reset, po.statementName())
		}

		// Tx count
		if po.TxCount > 0 {
			fmt.Printf("    %sTransactions:%s %d\n", labelStyle, Fmt.Reset, po.TxCount)
		}

		// Payout ID
		fmt.Printf("    %sPayout ID:%s    %s\n", labelStyle, Fmt.Reset, po.ID)
	}
	fmt.Println()

	return nil
}

// getAllDataMonths returns all YYYY-MM directories that have generated transactions.
func getAllDataMonths() []string {
	dataDir := DataDir()
	var months []string
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
			months = append(months, yd.Name()+"-"+md.Name())
		}
	}
	return months
}

func printAccountSlugHelp(slug string) {
	f := Fmt

	// Find the account config
	var acc *AccountConfig
	for _, a := range LoadAccountConfigs() {
		if strings.EqualFold(a.Slug, slug) {
			acc = &a
			break
		}
	}
	if acc == nil {
		fmt.Fprintf(os.Stderr, "%sAccount '%s' not found%s\n", f.Red, slug, f.Reset)
		return
	}

	// Show account summary
	currency := acc.Currency
	if currency == "" && acc.Token != nil {
		currency = acc.Token.Symbol
	}
	if currency == "" {
		currency = "EUR"
	}

	fmt.Println()
	fmt.Printf("  %s%s%s\n", f.Bold, acc.Slug, f.Reset)
	fmt.Printf("  %s%s%s\n", f.Dim, acc.Name, f.Reset)
	fmt.Printf("  %sProvider: %s%s\n", f.Dim, acc.Provider, f.Reset)
	if acc.Chain != "" {
		fmt.Printf("  %sChain: %s%s\n", f.Dim, acc.Chain, f.Reset)
	}
	if acc.Address != "" {
		fmt.Printf("  %sAddress: %s%s\n", f.Dim, acc.Address, f.Reset)
	}
	if acc.AccountID != "" {
		fmt.Printf("  %sAccount ID: %s%s\n", f.Dim, acc.AccountID, f.Reset)
	}
	if acc.OdooJournalID > 0 {
		fmt.Printf("  %sOdoo journal: %s (#%d)%s\n", f.Dim, acc.OdooJournalName, acc.OdooJournalID, f.Reset)
	}
	fmt.Println()

	// Show available commands
	fmt.Printf("%sCOMMANDS%s\n\n", f.Bold, f.Reset)

	if acc.Provider == "stripe" {
		fmt.Printf("  %s%schb accounts %s payouts%s\n", f.Bold, f.Cyan, slug, f.Reset)
		fmt.Printf("    %sList Stripe payouts with amounts and transaction counts%s\n\n", f.Dim, f.Reset)
	}

	fmt.Printf("  %s%schb accounts %s sync%s\n", f.Bold, f.Cyan, slug, f.Reset)
	if acc.Provider == "stripe" {
		fmt.Printf("    %sSync Stripe payouts to Odoo as bank statements%s\n", f.Dim, f.Reset)
	} else {
		fmt.Printf("    %sSync transactions to linked Odoo journal%s\n", f.Dim, f.Reset)
	}
	fmt.Println()

	fmt.Printf("  %s%schb accounts %s link%s\n", f.Bold, f.Cyan, slug, f.Reset)
	fmt.Printf("    %sLink this account to an Odoo bank journal%s\n", f.Dim, f.Reset)
	fmt.Println()

	// Show sync options
	fmt.Printf("%sOPTIONS%s (for sync)\n\n", f.Bold, f.Reset)
	fmt.Printf("  %s--dry-run%s          Preview what would be synced\n", f.Yellow, f.Reset)
	fmt.Printf("  %s--force%s            Re-sync (delete existing data first)\n", f.Yellow, f.Reset)
	fmt.Printf("  %s--months N%s         Limit to last N months\n", f.Yellow, f.Reset)
	if acc.Provider == "stripe" {
		fmt.Printf("  %s--payout po_xxx%s   Sync a specific payout only\n", f.Yellow, f.Reset)
	}
	fmt.Println()

	// Show examples
	fmt.Printf("%sEXAMPLES%s\n\n", f.Bold, f.Reset)
	if acc.Provider == "stripe" {
		fmt.Printf("  %s$ chb accounts %s payouts%s\n", f.Dim, slug, f.Reset)
		fmt.Printf("  %s$ chb accounts %s sync --months 2 --dry-run%s\n", f.Dim, slug, f.Reset)
		fmt.Printf("  %s$ chb accounts %s sync --payout po_xxx --force%s\n", f.Dim, slug, f.Reset)
	} else {
		fmt.Printf("  %s$ chb accounts %s sync --dry-run%s\n", f.Dim, slug, f.Reset)
		fmt.Printf("  %s$ chb accounts %s sync --force%s\n", f.Dim, slug, f.Reset)
	}
	fmt.Println()
}

func printAccountsHelp() {
	f := Fmt
	fmt.Printf(`
%schb accounts%s — Manage finance accounts

%sUSAGE%s
  %schb accounts%s                   List all accounts with balances
  %schb accounts -r%s                Refresh on-chain balances
  %schb accounts <slug> link%s              Link account to an Odoo bank journal
  %schb accounts <slug> sync%s              Sync transactions to linked Odoo journal
  %schb accounts <slug> sync --dry-run%s    Show what would be synced
  %schb accounts <slug> sync --months 2%s   Sync only last N months
  %schb accounts <slug> sync --payout po_%s Sync a specific Stripe payout
  %schb accounts <slug> sync --force%s      Re-sync (delete + recreate)
  %schb accounts <slug> payouts%s           List Stripe payouts

%sENVIRONMENT%s
  %sODOO_URL%s            Odoo instance URL
  %sODOO_LOGIN%s          Odoo login email
  %sODOO_PASSWORD%s       Odoo password or API key
`,
		f.Bold, f.Reset,
		f.Bold, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Cyan, f.Reset,
		f.Bold, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
		f.Yellow, f.Reset,
	)
}
