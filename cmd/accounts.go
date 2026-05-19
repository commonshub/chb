package cmd

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	etherscansource "github.com/CommonsHub/chb/providers/etherscan"
	stripesource "github.com/CommonsHub/chb/providers/stripe"
)

// accountSummary holds computed balance and last tx info for an account.
type accountSummary struct {
	Balance         float64
	Currency        string
	LastTxAt        time.Time
	TxCount         int
	InternalTxCount int
}

type accountTotals struct {
	Currency          string
	TxCount           int
	GrossIncome       float64
	TotalPaidOut      float64
	TxFees            float64
	OtherFees         float64
	InternalTransfers float64
	CurrentBalance    float64
	FirstTxAt         time.Time
	LastTxAt          time.Time
}

type accountOnchainStats struct {
	Currency  string
	TxCount   int
	FirstTxAt time.Time
	LastTxAt  time.Time
}

type accountSyncVerification struct {
	AccountSlug      string
	Currency         string
	OnchainBalance   float64
	OnchainBalanceOK bool
	LocalBalance     float64
	OnchainTxCount   int
	LocalTxCount     int
	OnchainFirstTxAt time.Time
	OnchainLastTxAt  time.Time
	LocalFirstTxAt   time.Time
	LocalLastTxAt    time.Time
	Missing          []missingOnchainTransfer
	MissingByMonth   map[string][]missingOnchainTransfer
	OldestMonth      string
	LiveBalanceError error
}

type missingOnchainTransfer struct {
	Month        string
	Timestamp    time.Time
	Hash         string
	Type         string
	Amount       float64
	From         string
	To           string
	Counterparty string
}

type accountSourceCheckpoint struct {
	Exists    bool
	Month     string
	Timestamp int64
	Hash      string
}

type odooImportCursor struct {
	Found          bool
	UniqueImportID string
	Date           string
}

type accountOdooSyncSnapshot struct {
	Label     string
	TxCount   int
	FirstTxAt time.Time
	LastTxAt  time.Time
	Balance   float64
	Currency  string
}

type blockchainOdooSyncResult struct {
	Summary string
	Synced  int
}

type odooSyncPlanRow struct {
	Action      string
	Date        string
	Description string
	Partner     string
	Account     string
	Amount      float64
	Currency    string
	Ref         string
	Reason      string
}

// fetchTokenBalance fetches the live on-chain token balance. It prefers
// Etherscan when configured, then falls back to a public JSON-RPC eth_call.
func fetchTokenBalance(chainID int, tokenAddress, walletAddress string, decimals int) (float64, error) {
	apiKey := os.Getenv("ETHERSCAN_API_KEY")
	if apiKey != "" {
		if balance, err := fetchTokenBalanceFromEtherscan(chainID, tokenAddress, walletAddress, decimals, apiKey); err == nil {
			return balance, nil
		}
	}

	rpcURL := defaultRPCForChainID(chainID)
	if rpcURL == "" {
		if apiKey == "" {
			return 0, fmt.Errorf("ETHERSCAN_API_KEY not set and no default RPC for chain ID %d", chainID)
		}
		return 0, fmt.Errorf("no default RPC for chain ID %d", chainID)
	}
	return fetchTokenBalanceFromRPC(rpcURL, tokenAddress, walletAddress, decimals)
}

func fetchTokenBalanceFromEtherscan(chainID int, tokenAddress, walletAddress string, decimals int, apiKey string) (float64, error) {
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

	return rawTokenBalanceToFloat(result.Result, decimals)
}

func fetchTokenBalanceFromRPC(rpcURL, tokenAddress, walletAddress string, decimals int) (float64, error) {
	calldata, err := erc20BalanceOfCalldata(walletAddress)
	if err != nil {
		return 0, err
	}
	payload := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "eth_call",
		"params": []interface{}{
			map[string]string{
				"to":   tokenAddress,
				"data": calldata,
			},
			"latest",
		},
	}
	body, _ := json.Marshal(payload)
	resp, err := http.Post(rpcURL, "application/json", bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("RPC request failed: %w", err)
	}
	defer resp.Body.Close()

	var result struct {
		Result string `json:"result"`
		Error  *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("RPC decode failed: %w", err)
	}
	if result.Error != nil {
		return 0, fmt.Errorf("RPC error: %s", result.Error.Message)
	}
	if result.Result == "" || result.Result == "0x" {
		return 0, fmt.Errorf("RPC returned empty balance")
	}
	return rawTokenBalanceToFloat(result.Result, decimals)
}

func erc20BalanceOfCalldata(walletAddress string) (string, error) {
	addressHex := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(walletAddress)), "0x")
	if len(addressHex) != 40 {
		return "", fmt.Errorf("invalid wallet address: %s", walletAddress)
	}
	if _, err := hex.DecodeString(addressHex); err != nil {
		return "", fmt.Errorf("invalid wallet address: %s", walletAddress)
	}
	return "0x70a08231" + strings.Repeat("0", 24) + addressHex, nil
}

func rawTokenBalanceToFloat(raw string, decimals int) (float64, error) {
	raw = strings.TrimSpace(raw)
	balance := new(big.Float)
	if strings.HasPrefix(raw, "0x") || strings.HasPrefix(raw, "0X") {
		i := new(big.Int)
		if _, ok := i.SetString(strings.TrimPrefix(strings.TrimPrefix(raw, "0x"), "0X"), 16); !ok {
			return 0, fmt.Errorf("invalid hex token balance: %s", raw)
		}
		balance.SetInt(i)
	} else if _, ok := balance.SetString(raw); !ok {
		return 0, fmt.Errorf("invalid token balance: %s", raw)
	}
	divisor := new(big.Float).SetFloat64(math.Pow10(decimals))
	fResult := new(big.Float).Quo(balance, divisor)
	f, _ := fResult.Float64()
	return f, nil
}

func defaultRPCForChainID(chainID int) string {
	switch chainID {
	case 1:
		return "https://ethereum.publicnode.com"
	case 10:
		return "https://mainnet.optimism.io"
	case 100:
		return defaultGnosisRPC
	case 137:
		return "https://polygon-rpc.com"
	case 8453:
		return "https://mainnet.base.org"
	case 42161:
		return "https://arb1.arbitrum.io/rpc"
	case 42220:
		return defaultCeloRPC
	default:
		return ""
	}
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

// refreshAccountBalance fetches the live balance for a single account and
// returns (balance, cacheKey, err). cacheKey is the lowercase key under
// which this balance should be stored in the shared balance cache.
// Returns ("", 0, nil) if the account has no supported live source.
func refreshAccountBalance(acc *AccountConfig) (float64, string, error) {
	if acc.Provider == "etherscan" && acc.Address != "" && acc.Token != nil {
		v, err := fetchTokenBalance(acc.ChainID, acc.Token.Address, acc.Address, acc.Token.Decimals)
		if err != nil {
			return 0, "", err
		}
		return v, strings.ToLower(acc.Address), nil
	}
	if acc.Provider == "stripe" {
		v, err := stripesource.FetchBalance(os.Getenv("STRIPE_SECRET_KEY"))
		if err != nil {
			return 0, "", err
		}
		key := strings.ToLower(acc.AccountID)
		if key == "" {
			key = "stripe"
		}
		return v, key, nil
	}
	return 0, "", nil
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
			balance, err := stripesource.FetchBalance(os.Getenv("STRIPE_SECRET_KEY"))
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
				if tx.Type == "INTERNAL" {
					s.InternalTxCount++
					continue
				}

				amount := tx.NormalizedAmount
				if amount == 0 {
					amount = tx.Amount
				}

				switch tx.Type {
				case "CREDIT", "MINT":
					s.Balance += math.Abs(amount)
				case "DEBIT", "BURN":
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

func computeAccountTotals(acc *AccountConfig) *accountTotals {
	if acc == nil {
		return nil
	}
	if acc.Provider == "stripe" {
		txs, err := stripesource.LoadTransactions(DataDir(), acc.AccountID)
		if err == nil && len(txs) > 0 {
			currency := acc.Currency
			if currency == "" {
				currency = "EUR"
			}
			return accountTotalsFromStripeTransactions(txs, currency)
		}
	}
	return accountTotalsFromGeneratedTransactions(acc, loadAccountTransactionsWithOptions(acc, true))
}

func accountTotalsFromStripeTransactions(txs []stripesource.Transaction, currency string) *accountTotals {
	if len(txs) == 0 {
		return nil
	}
	if currency == "" {
		currency = strings.ToUpper(txs[0].Currency)
	}
	if currency == "" {
		currency = "EUR"
	}
	totals := &accountTotals{Currency: strings.ToUpper(currency)}
	for _, tx := range txs {
		totals.TxCount++
		totals.CurrentBalance += centsToEuros(tx.Net)
		if tx.Created > 0 {
			t := time.Unix(tx.Created, 0)
			updateAccountTotalsTimeRange(totals, t)
		}
		switch tx.Type {
		case "charge", "payment":
			totals.GrossIncome += centsToEuros(tx.Amount)
			totals.TxFees += centsToEuros(tx.Fee)
		case "refund", "payment_refund":
			totals.TotalPaidOut += centsToEuros(-tx.Amount)
			totals.TxFees += centsToEuros(tx.Fee)
		case "stripe_fee", "adjustment":
			totals.OtherFees += centsToEuros(-tx.Amount)
		case "payout", "payout_cancel":
			totals.InternalTransfers += centsToEuros(-tx.Amount)
		}
	}
	roundAccountTotals(totals)
	return totals
}

func accountTotalsFromGeneratedTransactions(acc *AccountConfig, txs []TransactionEntry) *accountTotals {
	if len(txs) == 0 {
		return nil
	}
	currency := acc.Currency
	if currency == "" && acc.Token != nil {
		currency = acc.Token.Symbol
	}
	if currency == "" {
		currency = txs[0].Currency
	}
	if currency == "" {
		currency = "EUR"
	}
	totals := &accountTotals{Currency: currency}
	for _, tx := range txs {
		if tx.Currency != "" && currency != "" && !strings.EqualFold(tx.Currency, currency) {
			continue
		}
		totals.TxCount++
		if tx.Timestamp > 0 {
			t := time.Unix(tx.Timestamp, 0)
			updateAccountTotalsTimeRange(totals, t)
		}

		amount := tx.NormalizedAmount
		if amount == 0 {
			amount = tx.Amount
		}
		gross := tx.GrossAmount
		if gross == 0 {
			gross = math.Abs(amount)
		}
		totals.CurrentBalance += signedAccountTransactionAmount(acc, tx)

		switch tx.Type {
		case "INTERNAL":
			switch internalTransactionDirection(acc, tx) {
			case "DEBIT":
				totals.InternalTransfers -= math.Abs(amount)
			case "CREDIT":
				totals.InternalTransfers += math.Abs(amount)
			}
		case "CREDIT", "MINT":
			totals.GrossIncome += math.Abs(gross)
			totals.TxFees += tx.Fee
		case "DEBIT", "BURN":
			if accountTransactionLooksLikeFee(tx) {
				totals.OtherFees += math.Abs(amount)
			} else {
				totals.TotalPaidOut += math.Abs(gross)
				totals.TxFees += tx.Fee
			}
		}
	}
	if totals.TxCount == 0 {
		return nil
	}
	roundAccountTotals(totals)
	return totals
}

func updateAccountTotalsTimeRange(totals *accountTotals, t time.Time) {
	if totals == nil || t.IsZero() {
		return
	}
	if totals.FirstTxAt.IsZero() || t.Before(totals.FirstTxAt) {
		totals.FirstTxAt = t
	}
	if t.After(totals.LastTxAt) {
		totals.LastTxAt = t
	}
}

func signedAccountTransactionAmount(acc *AccountConfig, tx TransactionEntry) float64 {
	amount := tx.NormalizedAmount
	if amount == 0 {
		amount = tx.Amount
	}
	switch tx.Type {
	case "INTERNAL":
		if internalTransactionDirection(acc, tx) == "DEBIT" {
			return -math.Abs(amount)
		}
		return math.Abs(amount)
	case "DEBIT", "BURN":
		return -math.Abs(amount)
	case "CREDIT", "MINT":
		return math.Abs(amount)
	default:
		return amount
	}
}

func accountTransactionLooksLikeFee(tx TransactionEntry) bool {
	if strings.EqualFold(tx.Category, "fee") {
		return true
	}
	if tx.Metadata != nil {
		for _, key := range []string{"category", "stripe_reporting_category"} {
			if value, ok := tx.Metadata[key].(string); ok && strings.EqualFold(value, "fee") {
				return true
			}
		}
	}
	for _, tag := range tx.Tags {
		if len(tag) >= 2 && strings.EqualFold(tag[0], "category") && strings.EqualFold(tag[1], "fee") {
			return true
		}
	}
	return false
}

func roundAccountTotals(t *accountTotals) {
	t.GrossIncome = roundCents(t.GrossIncome)
	t.TotalPaidOut = roundCents(t.TotalPaidOut)
	t.TxFees = roundCents(t.TxFees)
	t.OtherFees = roundCents(t.OtherFees)
	t.InternalTransfers = roundCents(t.InternalTransfers)
	t.CurrentBalance = roundCents(t.CurrentBalance)
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

	// `chb accounts sync` → fetch source → local, for all accounts.
	if len(args) >= 1 && args[0] == "sync" {
		if _, err := AccountsFetchAll(args[1:]); err != nil {
			Fatalf("%sError:%s %v", Fmt.Red, Fmt.Reset, err)
		}
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
				// Pure source→local fetch for one account. Use `chb odoo
				// journals <id> sync` to push into Odoo afterward.
				if err := AccountFetch(slug, args[2:]); err != nil {
					Fatalf("%sError:%s %v", Fmt.Red, Fmt.Reset, err)
				}
			case "push":
				// Back-channel for now: push local→Odoo for this one account.
				if err := AccountOdooPush(slug, args[2:]); err != nil {
					Fatalf("%sError:%s %v", Fmt.Red, Fmt.Reset, err)
				}
			case "link":
				if err := AccountOdooLink(slug, args[2:]); err != nil {
					Fatalf("%sError:%s %v", Fmt.Red, Fmt.Reset, err)
				}
			case "balance":
				if err := AccountBalance(slug, args[2:]); err != nil {
					Fatalf("%sError:%s %v", Fmt.Red, Fmt.Reset, err)
				}
			case "payouts":
				if err := AccountStripePayouts(slug, args[2:]); err != nil {
					Fatalf("%sError:%s %v", Fmt.Red, Fmt.Reset, err)
				}
			case "pending":
				if err := AccountStripePending(slug); err != nil {
					Fatalf("%sError:%s %v", Fmt.Red, Fmt.Reset, err)
				}
			case "import-csv":
				if len(args) < 3 {
					Fatalf("%sUsage: chb accounts %s import-csv <file.csv>%s", Fmt.Yellow, slug, Fmt.Reset)
				}
				if err := ImportStripeCSV(args[2]); err != nil {
					Fatalf("%sError:%s %v", Fmt.Red, Fmt.Reset, err)
				}
			case "transactions", "txs", "tx":
				// Shorthand for `chb transactions --account <slug>` with a
				// 20-row default cap. --csv / --json imply full export; an
				// explicit -n / --limit also wins over the default.
				txArgs := append([]string{"--account", slug}, args[2:]...)
				wantsFullExport := HasFlag(args[2:], "--csv") || HasFlag(args[2:], "--json")
				explicitLimit := GetOption(args[2:], "-n", "--limit") != ""
				if !wantsFullExport && !explicitLimit {
					txArgs = append(txArgs, "-n", "20")
				}
				TransactionsBrowser(txArgs)
			default:
				AccountDetail(slug, args[1:])
			}
			return
		}
	}

	Accounts(args)
}

// AccountDetail prints a per-account summary: latest balance, last tx, last
// sync, Odoo journal status. Useful as a quick verification glance after
// running a sync. Pass `--refresh`/`-r` in args to refresh the cached
// on-chain balance for this account before printing.
func AccountDetail(slug string, args []string) {
	var acc *AccountConfig
	for _, a := range LoadAccountConfigs() {
		if strings.EqualFold(a.Slug, slug) {
			acc = &a
			break
		}
	}
	if acc == nil {
		if JSONMode(args) {
			EmitJSONError(fmt.Errorf("account '%s' not found", slug))
			PrintDiagnosticsSummary()
			CloseDiagnosticsLog()
			os.Exit(1)
		}
		Errorf("  %sAccount '%s' not found%s", Fmt.Red, slug, Fmt.Reset)
		return
	}

	if JSONMode(args) {
		emitAccountDetailJSON(acc, args)
		return
	}

	printAccountDetailSummary(acc, args)
	fmt.Println()
	printAccountSlugHelp(slug)
}

func printAccountDetailSummary(acc *AccountConfig, args []string) {
	refresh := HasFlag(args, "--refresh", "-r")

	summaries := computeAccountSummaries()
	fa := FinanceAccount{
		Provider:  acc.Provider,
		Chain:     acc.Chain,
		Address:   acc.Address,
		AccountID: acc.AccountID,
		Slug:      acc.Slug,
	}
	s := summaries[accountKey(fa)]
	totals := computeAccountTotals(acc)

	currency := acc.Currency
	if currency == "" && acc.Token != nil {
		currency = acc.Token.Symbol
	}
	if currency == "" {
		currency = "EUR"
	}

	// On --refresh, hit the live source for this one account and update
	// just this account's entry in the shared balance cache.
	if refresh {
		fmt.Printf("\n  %sRefreshing %s balance for %s…%s\n", Fmt.Dim, accountLiveBalanceLabel(acc), acc.Slug, Fmt.Reset)
		refreshAndPersistAccountBalance(acc)
	}

	// Prefer the cached live on-chain balance (token-scoped) over the
	// tx-history-derived summary balance. Mirrors the list view logic.
	var balance float64
	hasBalance := false
	var balanceSource string
	if cache := loadBalanceCache(); cache != nil {
		for _, key := range []string{acc.Address, acc.AccountID, acc.Slug} {
			if key == "" {
				continue
			}
			if v, ok := cache.Balances[strings.ToLower(key)]; ok {
				balance = v
				hasBalance = true
				balanceSource = accountBalanceSourceLabel(acc.Provider, cache.FetchedAt)
				break
			}
		}
	}
	if !hasBalance && s != nil && s.TxCount > 0 {
		balance = s.Balance
		hasBalance = true
		balanceSource = "from tx history"
	}

	fmt.Println()
	fmt.Printf("  %sAccount:%s      %s\n", Fmt.Dim, Fmt.Reset, accountDisplayName(acc))
	printAccountOnlineLink(acc, "  ")
	if hasBalance {
		fmt.Printf("  %sBalance:%s      %s\n", Fmt.Dim, Fmt.Reset, formatAccountDataBalance(balance, currency))
	} else {
		fmt.Printf("  %sBalance:%s      unknown\n", Fmt.Dim, Fmt.Reset)
	}

	txCount, firstTx, lastTx := accountDetailTransactionRange(totals, s)
	fmt.Printf("  %sTransactions:%s %s\n", Fmt.Dim, Fmt.Reset, formatAccountTransactionRange(txCount, firstTx, lastTx))

	source := "account:" + strings.ToLower(acc.Slug)
	lastSync := LastSyncTime(source)
	if !lastSync.IsZero() {
		fmt.Printf("  %sLast sync:%s    %s (%s)\n",
			Fmt.Dim, Fmt.Reset,
			lastSync.In(BrusselsTZ()).Format("2006-01-02 15:04"),
			formatAccountTransactionRange(txCount, firstTx, lastTx))
	} else {
		fmt.Printf("  %sLast sync:%s    never\n", Fmt.Dim, Fmt.Reset)
	}
	lastFull := LastFullSyncTime(source)
	if !lastFull.IsZero() {
		fmt.Printf("  %sLast full:%s    %s\n", Fmt.Dim, Fmt.Reset, lastFull.In(BrusselsTZ()).Format("2006-01-02 15:04"))
	}

	printAccountDetailOdooJournal(acc, currency)

	neverFullySynced := lastFull.IsZero()
	if neverFullySynced {
		fmt.Printf("\n  %sThis account has never been fully synced yet, please run `chb accounts %s sync --history`%s\n",
			Fmt.Yellow, acc.Slug, Fmt.Reset)
	}
	if totals != nil {
		printAccountBalanceMismatch(acc, totals.CurrentBalance, totals.Currency, hasBalance, balance, balanceSource)
	}
}

func accountBalanceSourceLabel(provider, fetchedAt string) string {
	source := "live"
	switch provider {
	case "stripe":
		source = "Stripe API"
	case "etherscan":
		source = "on-chain"
	}
	if fetchedAt == "" {
		return source
	}
	return source + " (cached " + fetchedAt + ")"
}

func accCurrency(acc *AccountConfig) string {
	if acc == nil {
		return "EUR"
	}
	if acc.Currency != "" {
		return acc.Currency
	}
	if acc.Token != nil && acc.Token.Symbol != "" {
		return acc.Token.Symbol
	}
	return "EUR"
}

func accountDisplayName(acc *AccountConfig) string {
	if acc == nil {
		return ""
	}
	if acc.Name != "" {
		return acc.Name
	}
	return acc.Slug
}

func accountDetailTransactionRange(totals *accountTotals, summary *accountSummary) (int, time.Time, time.Time) {
	if totals != nil {
		return totals.TxCount, totals.FirstTxAt, totals.LastTxAt
	}
	if summary != nil {
		return summary.TxCount, time.Time{}, summary.LastTxAt
	}
	return 0, time.Time{}, time.Time{}
}

func formatAccountTransactionRange(count int, first, last time.Time) string {
	label := "transactions"
	if count == 1 {
		label = "transaction"
	}
	if count == 0 {
		return "0 transactions"
	}
	if !first.IsZero() && !last.IsZero() {
		return fmt.Sprintf("%d %s from %s till %s", count, label, first.In(BrusselsTZ()).Format("2006-01-02"), last.In(BrusselsTZ()).Format("2006-01-02"))
	}
	if !last.IsZero() {
		return fmt.Sprintf("%d %s till %s", count, label, last.In(BrusselsTZ()).Format("2006-01-02"))
	}
	return fmt.Sprintf("%d %s", count, label)
}

func printAccountDetailOdooJournal(acc *AccountConfig, currency string) {
	if acc == nil || acc.OdooJournalID == 0 {
		return
	}
	lastSync := LastSyncTime(fmt.Sprintf("odoo:journal:%d", acc.OdooJournalID))
	line := fmt.Sprintf("  %sOdoo journal:%s #%d", Fmt.Dim, Fmt.Reset, acc.OdooJournalID)
	if !lastSync.IsZero() {
		line += fmt.Sprintf(", last sync: %s", lastSync.In(BrusselsTZ()).Format("2006-01-02 15:04"))
	}
	if creds, err := ResolveOdooCredentials(); err == nil {
		if uid, err := odooAuth(creds.URL, creds.DB, creds.Login, creds.Password); err == nil && uid != 0 {
			if snap, err := fetchOdooJournalSnapshot(creds, uid, acc.OdooJournalID, currency); err == nil {
				line += fmt.Sprintf(" (%s)", formatAccountTransactionRange(snap.TxCount, snap.FirstTxAt, snap.LastTxAt))
			}
			if OdooJournalName(acc.OdooJournalID) == "" {
				_, _ = FetchAndCacheOdooJournalName(creds, uid, acc.OdooJournalID)
			}
		}
	}
	if name := OdooJournalName(acc.OdooJournalID); name != "" {
		line += fmt.Sprintf(" %s(%s)%s", Fmt.Dim, name, Fmt.Reset)
	}
	fmt.Println(line)
}

func printAccountOnchainData(acc *AccountConfig, currency string, hasBalance bool, balance float64) {
	if acc == nil || acc.Provider != "etherscan" {
		return
	}
	fmt.Printf("  %sOn-chain data:%s\n", Fmt.Dim, Fmt.Reset)
	if hasBalance {
		fmt.Printf("    %sCurrent balance:    %s%s\n", Fmt.Dim, formatAccountDataBalance(balance, currency), Fmt.Reset)
	} else {
		fmt.Printf("    %sCurrent balance:    not cached (run with --refresh)%s\n", Fmt.Dim, Fmt.Reset)
	}
	stats := loadAccountOnchainStats(acc)
	if stats != nil {
		fmt.Printf("    %sTransfers:          %d%s\n", Fmt.Dim, stats.TxCount, Fmt.Reset)
		if !stats.FirstTxAt.IsZero() {
			fmt.Printf("    %sFirst tx:           %s%s\n", Fmt.Dim, formatAccountDataTimestamp(stats.FirstTxAt), Fmt.Reset)
		}
		if !stats.LastTxAt.IsZero() {
			fmt.Printf("    %sLast tx:            %s%s\n", Fmt.Dim, formatAccountDataTimestamp(stats.LastTxAt), Fmt.Reset)
		}
	} else {
		fmt.Printf("    %sTransfers:          no local on-chain cache%s\n", Fmt.Dim, Fmt.Reset)
	}
	if url := safeBalancesURL(acc); url != "" {
		fmt.Printf("    %s%s%s\n", Fmt.Dim, hyperlink(url, "Safe balances"), Fmt.Reset)
	}
}

func printAccountOnlineLink(acc *AccountConfig, indent string) {
	if acc == nil {
		return
	}
	switch acc.Provider {
	case "stripe":
		if acc.AccountID != "" {
			printAccountField(indent, "Address", "stripe "+acc.AccountID)
		}
		if _, url := accountOnlineLink(acc); url != "" {
			printAccountField(indent, "URL", url)
		}
	case "etherscan":
		if acc.Address == "" {
			return
		}
		printAccountField(indent, "Address", strings.TrimSpace(acc.Chain+" "+acc.Address))
		if _, url := accountOnlineLink(acc); url != "" {
			printAccountField(indent, "URL", url)
		}
	}
}

// accountOnlineLink returns (visible label, target URL) for an account.
// The label is what should be shown to the user (e.g. the address); the URL
// is where clicking takes them.
func accountOnlineLink(acc *AccountConfig) (string, string) {
	if acc == nil {
		return "", ""
	}
	switch acc.Provider {
	case "stripe":
		url := stripeDashboardURL(acc)
		if url == "" {
			return "", ""
		}
		label := acc.AccountID
		if label == "" {
			label = url
		}
		return label, url
	case "etherscan":
		if acc.Address == "" {
			return "", ""
		}
		url := txinfoAddressURL(acc)
		if acc.IsSafe() {
			if safeURL := safeBalancesURL(acc); safeURL != "" {
				url = safeURL
			}
		}
		return fmt.Sprintf("%s %s", acc.Chain, acc.Address), url
	}
	return "", ""
}

// printAccountField renders one "Label: value" row, padding the label to
// accountFieldLabelWidth so values line up across rows.
func printAccountField(indent, label, value string) {
	fmt.Printf("%s%s%-*s%s %s\n", indent, Fmt.Dim, accountFieldLabelWidth, label+":", Fmt.Reset, value)
}

// accountFieldLabelWidth aligns account detail rows; "Transactions:" is the
// longest label rendered in the summary.
const accountFieldLabelWidth = 13

func stripeDashboardURL(acc *AccountConfig) string {
	if acc == nil || acc.Provider != "stripe" {
		return ""
	}
	return "https://dashboard.stripe.com"
}

func txinfoAddressURL(acc *AccountConfig) string {
	if acc == nil || acc.Address == "" || acc.Chain == "" {
		return ""
	}
	return fmt.Sprintf("https://txinfo.xyz/%s/address/%s", acc.Chain, acc.Address)
}

func printAccountBalanceMismatch(acc *AccountConfig, computed float64, currency string, hasLive bool, live float64, source string) {
	if acc == nil || !hasLive {
		return
	}
	sourceLabel := "live"
	if strings.Contains(source, "on-chain") {
		sourceLabel = "on-chain"
	} else if strings.Contains(source, "Stripe") {
		sourceLabel = "live"
	}
	if math.Abs(computed-live) < 0.01 {
		return
	}
	fmt.Printf("  %sBalance mismatch:%s computed %s vs %s %s (off by %s)\n",
		Fmt.Yellow, Fmt.Reset,
		formatAccountDataBalance(computed, currency),
		sourceLabel,
		formatAccountDataBalance(live, currency),
		formatAccountDataBalance(computed-live, currency))
	fmt.Printf("    %sFix:%s chb accounts %s sync --history && chb accounts %s --refresh\n",
		Fmt.Dim, Fmt.Reset, acc.Slug, acc.Slug)
}

func printAccountLocalFiles(totals *accountTotals) {
	fmt.Printf("  %sLocal files:%s\n", Fmt.Dim, Fmt.Reset)
	fmt.Printf("    %sTransactions:       %d%s\n", Fmt.Dim, totals.TxCount, Fmt.Reset)
	if !totals.FirstTxAt.IsZero() {
		fmt.Printf("    %sFirst tx:           %s%s\n", Fmt.Dim, formatAccountDataTimestamp(totals.FirstTxAt), Fmt.Reset)
	}
	if !totals.LastTxAt.IsZero() {
		fmt.Printf("    %sLast tx:            %s%s\n", Fmt.Dim, formatAccountDataTimestamp(totals.LastTxAt), Fmt.Reset)
	}
	fmt.Printf("    %sGross income:       %s%s\n", Fmt.Dim, formatBalancePlain(totals.GrossIncome, totals.Currency), Fmt.Reset)
	fmt.Printf("    %sTotal paid out:     %s%s\n", Fmt.Dim, formatBalancePlain(totals.TotalPaidOut, totals.Currency), Fmt.Reset)
	fmt.Printf("    %sTx fees:            %s%s\n", Fmt.Dim, formatBalancePlain(totals.TxFees, totals.Currency), Fmt.Reset)
	fmt.Printf("    %sOther fees:         %s%s\n", Fmt.Dim, formatBalancePlain(totals.OtherFees, totals.Currency), Fmt.Reset)
	fmt.Printf("    %sInternal transfers: %s%s\n", Fmt.Dim, formatBalancePlain(totals.InternalTransfers, totals.Currency), Fmt.Reset)
	fmt.Printf("    %sComputed balance:   %s%s\n", Fmt.Dim, formatAccountDataBalance(totals.CurrentBalance, totals.Currency), Fmt.Reset)
}

func loadAccountOnchainStats(acc *AccountConfig) *accountOnchainStats {
	if acc == nil || acc.Provider != "etherscan" || acc.Token == nil {
		return nil
	}
	dataDir := DataDir()
	filename := etherscansource.FileName(acc.Slug, acc.Token.Symbol)
	stats := &accountOnchainStats{Currency: acc.Token.Symbol}
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
			path := etherscansource.Path(dataDir, yd.Name(), md.Name(), acc.Chain, filename)
			cache, ok := etherscansource.LoadCache(path)
			if !ok {
				continue
			}
			for _, tx := range cache.Transactions {
				stats.TxCount++
				if tx.TimeStamp == "" {
					continue
				}
				ts, err := strconv.ParseInt(tx.TimeStamp, 10, 64)
				if err != nil || ts <= 0 {
					continue
				}
				t := time.Unix(ts, 0)
				if stats.FirstTxAt.IsZero() || t.Before(stats.FirstTxAt) {
					stats.FirstTxAt = t
				}
				if t.After(stats.LastTxAt) {
					stats.LastTxAt = t
				}
			}
		}
	}
	if stats.TxCount == 0 {
		return nil
	}
	return stats
}

func formatAccountDataTimestamp(t time.Time) string {
	return t.In(BrusselsTZ()).Format("2006-01-02 15:04")
}

// formatTimeAgoWithAbsolute returns "2h ago (2026-05-11 10:24)".
func formatTimeAgoWithAbsolute(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return fmt.Sprintf("%s (%s)", formatTimeAgo(t), formatAccountDataTimestamp(t))
}

// formatAccountOdooStatus renders the right-hand side of the "Odoo:" row
// for the account list: "journal #48 (synced)" or
// "journal #48 (3 missing, last synced 2026-05-11 10:24)".
func formatAccountOdooStatus(journalID int, status *odooSyncStatus) string {
	label := OdooJournalName(journalID)
	if label == "" {
		label = fmt.Sprintf("journal #%d", journalID)
	} else {
		label = fmt.Sprintf("%s (journal #%d)", label, journalID)
	}
	if status == nil {
		return label
	}
	if status.Missing == 0 {
		return fmt.Sprintf("%s %s(synced)%s", label, Fmt.Green, Fmt.Reset)
	}
	lastSync := LastSyncTime(fmt.Sprintf("odoo:journal:%d", journalID))
	detail := fmt.Sprintf("%d missing", status.Missing)
	if !lastSync.IsZero() {
		detail += fmt.Sprintf(", last synced %s", formatAccountDataTimestamp(lastSync))
	}
	return fmt.Sprintf("%s %s(%s)%s", label, Fmt.Yellow, detail, Fmt.Reset)
}

func formatAccountDataBalance(balance float64, currency string) string {
	if strings.EqualFold(currency, "EUR") || currency == "" {
		return formatBalancePlain(balance, "EUR")
	}
	if balance < 0 {
		return fmt.Sprintf("-%s %s", fmtNumber(-balance), currency)
	}
	return fmt.Sprintf("%s %s", fmtNumber(balance), currency)
}

// AccountJSON is the machine-readable shape of an account row, used by
// `chb accounts --json` and `chb accounts <slug> --json`.
type AccountJSON struct {
	Slug            string   `json:"slug"`
	Name            string   `json:"name"`
	Provider        string   `json:"provider"`
	Chain           string   `json:"chain,omitempty"`
	Address         string   `json:"address,omitempty"`
	AccountID       string   `json:"accountId,omitempty"`
	Currency        string   `json:"currency"`
	Balance         *float64 `json:"balance,omitempty"`
	BalanceSource   string   `json:"balanceSource,omitempty"`
	TxCount         int      `json:"txCount,omitempty"`
	InternalTxCount int      `json:"internalTxCount,omitempty"`
	LastTxAt        string   `json:"lastTxAt,omitempty"`
	LastSyncAt      string   `json:"lastSyncAt,omitempty"`
	LastFullSyncAt  string   `json:"lastFullSyncAt,omitempty"`
	OdooJournalID   int      `json:"odooJournalId,omitempty"`
	OdooJournalName string   `json:"odooJournalName,omitempty"`
	OdooMissing     *int     `json:"odooMissing,omitempty"`
	OdooLastTxDate  string   `json:"odooLastTxDate,omitempty"`
}

// AccountsJSON is the top-level payload for `chb accounts --json`.
type AccountsJSON struct {
	Accounts  []AccountJSON      `json:"accounts"`
	Totals    map[string]float64 `json:"totals"`
	FetchedAt string             `json:"fetchedAt,omitempty"`
}

// Accounts lists all configured finance accounts with balance and last tx.
func Accounts(args []string) {
	configs := LoadAccountConfigs()
	if len(configs) == 0 {
		if JSONMode(args) {
			_ = EmitJSON(AccountsJSON{Accounts: []AccountJSON{}, Totals: map[string]float64{}})
			return
		}
		fmt.Printf("\n%sNo accounts configured.%s\n\n", Fmt.Dim, Fmt.Reset)
		return
	}

	if JSONMode(args) {
		emitAccountsJSON(args, configs)
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

		// Account header: slug as title, balance to the right.
		if hasBalance {
			fmt.Printf("  %s%s%s  %s\n", Fmt.Bold, acc.Slug, Fmt.Reset, formatBalance(balance, currency))
		} else {
			fmt.Printf("  %s%s%s\n", Fmt.Bold, acc.Slug, Fmt.Reset)
		}

		printAccountField("    ", "Name", acc.Name)
		printAccountOnlineLink(&acc, "    ")

		if s != nil && !s.LastTxAt.IsZero() {
			printAccountField("    ", "Last tx", formatTimeAgoWithAbsolute(s.LastTxAt))
		}

		lastSync := LastSyncTime("account:" + strings.ToLower(acc.Slug))
		lastFull := LastFullSyncTime("account:" + strings.ToLower(acc.Slug))
		fullSyncRequired := lastFull.IsZero()
		if !lastSync.IsZero() {
			val := formatTimeAgoWithAbsolute(lastSync)
			if fullSyncRequired {
				val += fmt.Sprintf("  %s⚠ full sync required (run with --history)%s", Fmt.Yellow, Fmt.Reset)
			}
			printAccountField("    ", "Last sync", val)
		} else if fullSyncRequired {
			printAccountField("    ", "Last sync", fmt.Sprintf("%s⚠ never — run with --history%s", Fmt.Yellow, Fmt.Reset))
		}

		if acc.OdooJournalID > 0 {
			printAccountField("    ", "Odoo", formatAccountOdooStatus(acc.OdooJournalID, odooStatuses[acc.OdooJournalID]))
		}

		fmt.Println()
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

func safeBalancesURL(acc *AccountConfig) string {
	if acc == nil || !acc.IsSafe() || acc.Address == "" {
		return ""
	}
	return fmt.Sprintf("https://app.safe.global/balances?safe=%s:%s", safeChainPrefix(acc.Chain), acc.Address)
}

func safeChainPrefix(chain string) string {
	switch chain {
	case "gnosis":
		return "gno"
	case "celo":
		return "celo"
	case "polygon":
		return "matic"
	default:
		return "eth"
	}
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

func formatBalancePlain(balance float64, currency string) string {
	if isEURCurrency(currency) {
		if balance >= 0 {
			return fmtEUR(balance)
		}
		return "-" + fmtEUR(-balance)
	}
	if balance >= 0 {
		return fmt.Sprintf("%s %s", fmtNumber(balance), currency)
	}
	return fmt.Sprintf("-%s %s", fmtNumber(-balance), currency)
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
	CacheOdooJournalName(selected.ID, selected.Name)

	if err := SaveAccountConfigs(configs); err != nil {
		return fmt.Errorf("failed to save: %v", err)
	}

	fmt.Printf("\n  %s✓ Linked '%s' → Odoo journal '%s' (#%d)%s\n\n", Fmt.Green, configs[idx].Name, selected.Name, selected.ID, Fmt.Reset)
	return nil
}

// ── Account fetch (source → local) ──

// AccountFetch fetches this account's transactions from its source (Etherscan
// / Stripe / Monerium) into the local cache, then runs Generate so the
// unified per-month `generated/transactions.json` files (which downstream
// consumers like loadAccountTransactions and AccountOdooPush read from) are
// rebuilt from the freshly-fetched raw data. Does not touch Odoo.
func AccountFetch(slug string, args []string) error {
	startedAt := time.Now()
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
	checkpoint := latestAccountSourceCheckpoint(acc)
	beforeSourceMonths := accountSourceMonthFingerprints(acc)
	fetchArgs := accountFetchArgsForCheckpoint(*acc, args, checkpoint)
	if acc.Provider == "etherscan" {
		for _, line := range accountSyncPlanLines(acc, accountTransactionSource(*acc), checkpoint, accountFetchArgsHasExplicitRange(args)) {
			fmt.Printf("  %s\n", line)
		}
		fmt.Println()
	}
	if _, err := TransactionsSync(fetchArgs); err != nil {
		return err
	}
	touchedMonths := accountChangedSourceMonths(acc, beforeSourceMonths)
	if len(touchedMonths) > 0 {
		if err := GenerateTransactionsForMonths(touchedMonths); err != nil {
			return fmt.Errorf("generate transactions after fetch: %v", err)
		}
	}
	UpdateSyncSource("account:"+strings.ToLower(slug), accountSyncIsFull(args))
	// Refresh the persisted live-balance cache so a subsequent
	// `chb accounts <slug>` shows the current on-chain balance without
	// requiring a separate --refresh call.
	refreshAndPersistAccountBalance(acc)
	if verification := verifyAccountLocalAgainstOnchainCache(acc, nil); verification != nil {
		printAccountSyncVerification(verification)
	}
	fmt.Printf("%sSync done%s in %s\n\n", Fmt.Green, Fmt.Reset, time.Since(startedAt).Round(time.Millisecond))
	return nil
}

// refreshAndPersistAccountBalance pulls the current live balance from
// the account's source (etherscan / etc.) and writes it into the
// shared balance cache, so any subsequent `chb accounts` view shows
// the fresh value without needing --refresh. Soft-fails on errors —
// the sync itself succeeded and the cache write is opportunistic.
func refreshAndPersistAccountBalance(acc *AccountConfig) {
	if acc == nil {
		return
	}
	v, key, err := refreshAccountBalance(acc)
	if err != nil || key == "" {
		return
	}
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

// AccountsFetchAll fetches all configured accounts source → local. It runs
// accounts serially so each account gets a single summary line in order,
// captures each fetch's verbose output, and only prints that output if the
// fetch failed. GenerateTransactions runs once at the end, not after every
// account. Per-account errors are reported but do not abort the run; the
// returned error is non-nil if any account failed.
func AccountsFetchAll(args []string) (int, error) {
	configs := LoadAccountConfigs()
	if len(configs) == 0 {
		fmt.Printf("\n  %sNo accounts configured%s\n\n", Fmt.Dim, Fmt.Reset)
		return 0, nil
	}

	fmt.Printf("\n%s🔄 Syncing accounts%s\n\n", Fmt.Bold, Fmt.Reset)

	type accountFetchResult struct {
		Account AccountConfig
		Count   int
		Output  string
		Err     error
	}
	results := make([]accountFetchResult, 0, len(configs))
	failed := 0
	for _, acc := range configs {
		slugArgs := accountFetchArgs(acc, args)
		status := newStatusLine()
		status.Update("accounts: syncing %s...", acc.Slug)
		output, count, err := captureTransactionsSync(slugArgs)
		status.Clear()
		results = append(results, accountFetchResult{
			Account: acc,
			Count:   count,
			Output:  output,
			Err:     err,
		})
		if err != nil {
			failed++
			continue
		}
		UpdateSyncSource("account:"+strings.ToLower(acc.Slug), accountSyncIsFull(args))
	}

	// Regenerate the unified per-month transactions.json files ONCE after all
	// accounts have been fetched, rather than after each account.
	status := newStatusLine()
	status.Update("generated: regenerating per-month transactions...")
	generateOutput, err := captureGenerateTransactions(args)
	status.Clear()
	if err != nil {
		Errorf("  %s✗ generate: %v%s", Fmt.Red, err, Fmt.Reset)
		if strings.TrimSpace(generateOutput) != "" {
			fmt.Print(generateOutput)
		}
	} else {
		odooSyncLine("generated", "per-month transactions refreshed")
	}

	for _, row := range results {
		data := formatAccountSyncData(&row.Account)
		if row.Err != nil {
			fmt.Printf("  %s%s%s: %s (issue: %v)\n", Fmt.Bold, row.Account.Slug, Fmt.Reset, data, row.Err)
			if strings.TrimSpace(row.Output) != "" {
				fmt.Print(row.Output)
			}
			continue
		}
		fmt.Printf("  %s%s%s: %s (%s)\n", Fmt.Bold, row.Account.Slug, Fmt.Reset, data, accountSyncFetchStatus(row.Count))
	}
	fmt.Println()

	if failed > 0 {
		return failed, fmt.Errorf("%s failed", Pluralize(failed, "account", ""))
	}
	return 0, nil
}

func formatAccountSyncData(acc *AccountConfig) string {
	if acc == nil {
		return "0 txs, balance: 0.00 EUR"
	}
	totals := computeAccountTotals(acc)
	currency := acc.Currency
	if currency == "" && acc.Token != nil {
		currency = acc.Token.Symbol
	}
	if currency == "" {
		currency = "EUR"
	}
	if totals == nil {
		return fmt.Sprintf("0 txs, balance: %s", formatAccountDataBalance(0, currency))
	}
	return fmt.Sprintf("%s, balance: %s",
		Pluralize(totals.TxCount, "tx", ""),
		formatAccountDataBalance(totals.CurrentBalance, totals.Currency))
}

func accountSyncFetchStatus(count int) string {
	if count <= 0 {
		return "already in sync"
	}
	return fmt.Sprintf("%d synced", count)
}

func accountFetchArgs(acc AccountConfig, args []string) []string {
	out := append([]string{"--account-sync", "--slug", acc.Slug}, args...)
	if GetOption(args, "--source") != "" {
		return out
	}
	source := accountTransactionSource(acc)
	if source == "" {
		return out
	}
	return append([]string{"--source", source}, out...)
}

func accountFetchArgsForCheckpoint(acc AccountConfig, args []string, checkpoint accountSourceCheckpoint) []string {
	out := accountFetchArgs(acc, args)
	if acc.Provider != "etherscan" || !checkpoint.Exists || accountFetchArgsHasExplicitRange(args) {
		return out
	}
	if GetOption(out, "--since") == "" {
		out = append(out, "--since", checkpoint.Month)
	}
	return out
}

func accountFetchArgsHasExplicitRange(args []string) bool {
	if GetOption(args, "--since") != "" || GetOption(args, "--month") != "" || HasFlag(args, "--history") {
		return true
	}
	_, _, found := ParseYearMonthArg(args)
	return found
}

func accountSyncIsFull(args []string) bool {
	return HasFlag(args, "--history") || GetOption(args, "--since") != ""
}

func accountSyncPlanLines(acc *AccountConfig, source string, checkpoint accountSourceCheckpoint, explicitRange bool) []string {
	if acc == nil {
		return nil
	}
	if source == "" {
		source = accountTransactionSource(*acc)
	}
	token := ""
	if acc.Token != nil {
		token = acc.Token.Symbol
		if acc.Token.Address != "" {
			token += " (" + acc.Token.Address + ")"
		}
	}
	since := "default recent window"
	if explicitRange {
		since = "requested range"
	} else if checkpoint.Exists {
		since = time.Unix(checkpoint.Timestamp, 0).In(BrusselsTZ()).Format("2006-01-02") + " (last tx)"
	} else if lastSync := LastSyncTime("account:" + strings.ToLower(acc.Slug)); !lastSync.IsZero() {
		since = lastSync.In(BrusselsTZ()).Format("2006-01-02") + " (last sync)"
	}
	lines := []string{
		fmt.Sprintf("%-8s %s", "Source:", source),
		fmt.Sprintf("%-8s %s", "Address:", acc.Address),
	}
	if token != "" {
		lines = append(lines, fmt.Sprintf("%-8s %s", "Token:", token))
	}
	lines = append(lines, fmt.Sprintf("%-8s %s", "Since:", since))
	return lines
}

func accountTransactionSource(acc AccountConfig) string {
	switch strings.ToLower(strings.TrimSpace(acc.Provider)) {
	case "stripe":
		return "stripe"
	case "monerium":
		return "monerium"
	case "etherscan":
		if acc.Chain != "" {
			return strings.ToLower(acc.Chain)
		}
		return "etherscan"
	default:
		return ""
	}
}

// captureTransactionsSync runs TransactionsSync with its stdout redirected to
// a buffer. Returns the captured output, the sync's tx count, and any error.
// Used by aggregate callers that want one summary line per account instead
// of the full verbose output.
func captureTransactionsSync(args []string) (string, int, error) {
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		// Fallback: pipe creation failed, fall back to direct call with output.
		n, e := TransactionsSync(args)
		return "", n, e
	}
	os.Stdout = w
	done := make(chan string, 1)
	go func() {
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, r)
		done <- buf.String()
	}()
	count, syncErr := TransactionsSync(args)
	w.Close()
	os.Stdout = old
	return <-done, count, syncErr
}

func captureGenerateTransactions(args []string) (string, error) {
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		return "", GenerateTransactions(args)
	}
	os.Stdout = w
	done := make(chan string, 1)
	go func() {
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, r)
		done <- buf.String()
	}()
	genErr := GenerateTransactions(args)
	w.Close()
	os.Stdout = old
	return <-done, genErr
}

// quietOdooContext is set by aggregate callers (OdooSyncAll,
// odooJournalsSyncAll) so per-account sync functions can skip printing
// the Odoo URL / db line — it's already been shown once by the caller.
var quietOdooContextFlag bool

func quietOdooContext() bool     { return quietOdooContextFlag }
func setQuietOdooContext(v bool) { quietOdooContextFlag = v }

var odooTargetAlreadyPrintedFlag bool

func odooTargetAlreadyPrinted() bool     { return odooTargetAlreadyPrintedFlag }
func setOdooTargetAlreadyPrinted(v bool) { odooTargetAlreadyPrintedFlag = v }

// ── Account Odoo push (local → Odoo) ──

// AccountOdooPush pushes local transactions to Odoo as bank statement lines.
// Formerly AccountOdooSync; renamed to make direction explicit.
type odooSyncStages struct {
	Transactions bool
	Partners     bool
	Accounts     bool
	Metadata     bool
	Reconcile    bool
	Explicit     bool
}

func parseOdooSyncStages(args []string) odooSyncStages {
	explicit := odooSyncStageFlagsExplicit(args)
	if !explicit {
		return odooSyncStages{
			Transactions: true,
			Partners:     true,
			Accounts:     true,
			Metadata:     true,
			Reconcile:    true,
		}
	}
	return odooSyncStages{
		Transactions: HasFlag(args, "--transactions"),
		Partners:     HasFlag(args, "--partners"),
		Accounts:     HasFlag(args, "--accounts"),
		Metadata:     HasFlag(args, "--metadata"),
		Reconcile:    HasFlag(args, "--reconcile"),
		Explicit:     true,
	}
}

func odooSyncStageFlagsExplicit(args []string) bool {
	return HasFlag(args, "--transactions") ||
		HasFlag(args, "--partners") ||
		HasFlag(args, "--accounts") ||
		HasFlag(args, "--metadata") ||
		HasFlag(args, "--reconcile")
}

func AccountOdooPush(slug string, args []string) error {
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
	skipReconciliation := HasFlag(args, "--skip-reconciliation")
	if HasFlag(args, "--account") {
		return fmt.Errorf("unknown flag --account for journal sync; use --accounts")
	}
	stages := parseOdooSyncStages(args)
	assumeYes := HasFlag(args, "--yes", "-y")
	payoutFilter := GetOption(args, "--payout")
	untilStr := GetOption(args, "--until")
	sinceStr := GetOption(args, "--since")
	previewLimit := GetNumber(args, []string{"-n", "--limit"}, 30)
	if previewLimit < 0 {
		previewLimit = 0
	}

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

	// Parse --since YYYY[MM[DD]]: include only txs at/after this date.
	// Setting --since (like --history) also enables the rule re-apply
	// pass over already-imported lines within the window.
	var sinceDate time.Time
	if sinceStr != "" {
		t, ok := ParseSinceDate(sinceStr)
		if !ok {
			return fmt.Errorf("invalid --since format: %s (use YYYY, YYYYMM, or YYYYMMDD)", sinceStr)
		}
		sinceDate = t
	}

	useHistory := HasFlag(args, "--history") || force
	rescanExisting := !force && (useHistory || !sinceDate.IsZero())
	effectiveSinceDate := sinceDate
	sinceLabelOverride := ""
	partnerOnly := acc.Provider == "stripe" && stages.Explicit && stages.Partners && !stages.Transactions && !stages.Accounts && !stages.Metadata && !stages.Reconcile
	accountOnly := acc.Provider == "stripe" && stages.Explicit && stages.Accounts && !stages.Transactions && !stages.Partners && !stages.Metadata && !stages.Reconcile
	if acc.Provider == "stripe" && stages.Explicit && stages.Partners && !stages.Transactions && sinceStr == "" && !useHistory {
		partnerSince, found, err := latestStripePartnerStageSinceFromLocalCache(acc.OdooJournalID)
		if err == nil && !found && !partnerOnly {
			partnerSince, found, err = latestStripePartnerStageSince(creds, uid, acc.OdooJournalID)
		}
		if err != nil {
			Warnf("  %s⚠ Could not read latest partnered Stripe line, using full partner scan: %v%s", Fmt.Yellow, err, Fmt.Reset)
			useHistory = true
			sinceLabelOverride = "full history (partner cursor unavailable)"
		} else if found {
			effectiveSinceDate = partnerSince
			sinceLabelOverride = partnerSince.Format("2006-01-02") + " (last line with partner)"
		} else {
			useHistory = true
			sinceLabelOverride = "full history (no partnered line yet)"
		}
	}
	if sinceLabelOverride == "" && acc.Provider == "stripe" && stages.Transactions && !useHistory && effectiveSinceDate.IsZero() && untilDate.IsZero() {
		if cursor, err := fetchLatestStripeOdooImportCursor(creds, uid, acc.OdooJournalID, acc.AccountID); err == nil {
			sinceLabelOverride = stripeOdooCursorSinceLabel(cursor)
		}
	}

	if !quietOdooContext() && !partnerOnly && !accountOnly {
		printOdooSyncHeader(creds, acc, effectiveSinceDate, untilDate, useHistory, sinceStr != "", monthsLimit, sinceLabelOverride)
	}

	localBefore := accountLocalOdooSyncSnapshot(acc)

	// --force: empty the entire journal first. Stripe handles this inside
	// the sync itself, so we only run the global wipe for non-Stripe paths.
	if force && !dryRun && acc.Provider != "stripe" {
		if err := emptyOdooJournal(creds, uid, acc.OdooJournalID, true); err != nil {
			return err
		}
	}

	var syncErr error
	var summary string
	syncedCount := 0
	if acc.Provider == "stripe" {
		if stages.Transactions {
			summary, syncErr = syncStripeToOdoo(acc, creds, uid, monthsLimit, dryRun, force, skipReconciliation, payoutFilter, effectiveSinceDate, untilDate, previewLimit, stages, useHistory)
		} else {
			summary = "transactions skipped"
		}
		if syncErr == nil && stages.Explicit && stages.Partners {
			reviewed, updated, err := syncStripeOdooPartnersStage(creds, uid, acc, effectiveSinceDate, untilDate, dryRun, previewLimit, useHistory || sinceStr == "")
			if err != nil {
				syncErr = err
			} else if reviewed > 0 {
				summary += fmt.Sprintf(", partners %d/%d", updated, reviewed)
			}
		}
		if syncErr == nil && stages.Explicit && stages.Accounts {
			reviewed, updated, err := syncStripeOdooAccountsStage(creds, uid, acc, effectiveSinceDate, untilDate, dryRun)
			if err != nil {
				syncErr = err
			} else if reviewed > 0 {
				summary += fmt.Sprintf(", accounts %d/%d", updated, reviewed)
			}
		}
		if syncErr == nil && stages.Explicit && stages.Metadata {
			reviewed, updated, err := syncStripeOdooMetadataStage(creds, uid, acc, effectiveSinceDate, untilDate, dryRun, previewLimit)
			if err != nil {
				syncErr = err
			} else if reviewed > 0 {
				summary += fmt.Sprintf(", metadata %d/%d", updated, reviewed)
			}
		}
		if syncErr == nil && stages.Explicit && stages.Reconcile {
			if err := odooJournalReconcile(creds, uid, acc.OdooJournalID, assumeYes, dryRun, false); err != nil {
				syncErr = err
			} else {
				summary += ", reconcile pass complete"
			}
		}
		if stages.Transactions {
			syncedCount = parseStripeUploadCount(summary)
		}
	} else {
		if stages.Explicit && (!stages.Transactions || stages.Partners || stages.Accounts || stages.Metadata || stages.Reconcile) {
			return fmt.Errorf("staged sync flags are currently supported for Stripe journals; use --transactions for this account")
		}
		var result blockchainOdooSyncResult
		result, syncErr = syncBlockchainToOdoo(acc, creds, uid, monthsLimit, dryRun, skipReconciliation, sinceDate, untilDate, useHistory, previewLimit)
		summary = result.Summary
		syncedCount = result.Synced
	}

	// Post-sync pass: walk existing Odoo lines for this journal and
	// reapply any rule whose partner / account differs from what's
	// already there. Only runs when --history or --since narrows the
	// scope — without those flags, we trust the latest-cursor sync and
	// avoid the cost of fetching every line. Honors --dry-run.
	reviewedCount := 0
	updatedCount := 0
	if syncErr == nil && stages.Accounts && rescanExisting && !(acc.Provider == "stripe" && stages.Explicit) {
		reviewed, applied, err := applyOdooRulesToExistingLines(creds, uid, acc, sinceDate, untilDate, dryRun)
		if err != nil {
			Warnf("  %s⚠ rule re-apply: %v%s", Fmt.Yellow, err, Fmt.Reset)
		} else {
			reviewedCount = reviewed
			updatedCount = applied
		}
	}

	label := fmt.Sprintf("#%d %s", acc.OdooJournalID, acc.Slug)
	if syncErr != nil {
		if quietOdooContext() {
			if !finalizeJournalRow(journalSyncRow{
				JournalID: acc.OdooJournalID,
				Slug:      acc.Slug,
				Status:    fmt.Sprintf("✗ %v", syncErr),
				HasError:  true,
			}) {
				odooSyncSubLine(label, fmt.Sprintf("%s✗ %v%s", Fmt.Red, syncErr, Fmt.Reset))
			}
		}
		return syncErr
	}

	var mismatch string
	if !dryRun {
		live, detail := verifyJournalBalanceAgainstLive(acc, creds, uid)
		mismatch = detail
		if !quietOdooContext() && live != 0 {
			currency := acc.Currency
			if currency == "" && acc.Token != nil {
				currency = acc.Token.Symbol
			}
			if currency == "" {
				currency = "EUR"
			}
			fmt.Printf("  %slive %s balance: %s%s\n", Fmt.Dim, accountLiveBalanceLabel(acc), formatBalance(live, currency), Fmt.Reset)
		}
	}

	if quietOdooContext() {
		after, snapErr := fetchOdooJournalSnapshot(creds, uid, acc.OdooJournalID, accCurrency(acc))
		balanceStr := "?"
		txCount := 0
		if snapErr == nil {
			balanceStr = formatAccountDataBalance(after.Balance, after.Currency)
			txCount = after.TxCount
		}
		rowStatus := summary
		if mismatch != "" {
			rowStatus = fmt.Sprintf("%s, %sout of sync%s", rowStatus, Fmt.Yellow, Fmt.Reset)
		}
		if !finalizeJournalRow(journalSyncRow{
			JournalID: acc.OdooJournalID,
			Slug:      acc.Slug,
			TxCount:   txCount,
			Balance:   balanceStr,
			Status:    rowStatus,
			Mismatch:  mismatch,
		}) {
			odooSyncSubLine(label, fmt.Sprintf("%d txs, balance: %s (%s)", txCount, balanceStr, rowStatus))
			if mismatch != "" {
				fmt.Print(mismatch)
			}
		}
	}
	if !dryRun {
		UpdateSyncSource(fmt.Sprintf("odoo:journal:%d", acc.OdooJournalID), false)
		if acc.Provider == "stripe" && stages.Transactions && syncErr == nil {
			if count, err := writeOdooJournalLinesCache(creds, uid, acc.OdooJournalID); err != nil {
				Warnf("  %s⚠ Odoo journal cache: %v%s", Fmt.Yellow, err, Fmt.Reset)
			} else if !quietOdooContext() {
				fmt.Printf("  %sCached %d Odoo journal lines in %s%s\n", Fmt.Dim, count, odooJournalLinesCachePath(acc.OdooJournalID), Fmt.Reset)
			}
		}
	}
	if !quietOdooContext() && !partnerOnly && !accountOnly {
		odooAfter, odooAfterErr := fetchOdooJournalSnapshot(creds, uid, acc.OdooJournalID, accCurrency(acc))
		// In dry-run nothing was written, so the local snapshot we
		// captured at the top is the right "what would change against"
		// reference. In normal mode we re-read local to reflect any
		// post-sync state.
		localAfter := localBefore
		if !dryRun {
			localAfter = accountLocalOdooSyncSnapshot(acc)
		}
		printOdooSyncSummary(syncedCount, reviewedCount, updatedCount, dryRun, localAfter, odooAfter, odooAfterErr)
		printOdooSyncNextHints(acc, stages)
	}
	return nil
}

func parseStripeUploadCount(summary string) int {
	var n int
	if strings.HasPrefix(summary, "dry-run: ") {
		_, _ = fmt.Sscanf(summary, "dry-run: %d tx would be uploaded", &n)
		return n
	}
	_, _ = fmt.Sscanf(summary, "%d new", &n)
	return n
}

func parseStripeDryRunUploadCount(summary string) int {
	var n int
	_, _ = fmt.Sscanf(summary, "dry-run: %d tx would be uploaded", &n)
	return n
}

func printOdooSyncNextHints(acc *AccountConfig, stages odooSyncStages) {
	if acc == nil || acc.Provider != "stripe" || !stages.Explicit {
		return
	}
	var commands []string
	base := fmt.Sprintf("chb odoo journals %d sync", acc.OdooJournalID)
	if stages.Transactions {
		if !stages.Partners {
			commands = append(commands, base+" --partners")
		}
		if !stages.Accounts {
			commands = append(commands, base+" --accounts")
		}
		if !stages.Metadata {
			commands = append(commands, base+" --metadata")
		}
		if !stages.Reconcile {
			commands = append(commands, fmt.Sprintf("chb odoo journals %d reconcile", acc.OdooJournalID))
		}
	} else if stages.Partners && !stages.Accounts {
		commands = append(commands, base+" --accounts")
		if !stages.Metadata {
			commands = append(commands, base+" --metadata")
		}
		if !stages.Reconcile {
			commands = append(commands, fmt.Sprintf("chb odoo journals %d reconcile", acc.OdooJournalID))
		}
	} else if stages.Accounts && !stages.Metadata {
		commands = append(commands, base+" --metadata")
		if !stages.Reconcile {
			commands = append(commands, fmt.Sprintf("chb odoo journals %d reconcile", acc.OdooJournalID))
		}
	} else if (stages.Accounts || stages.Metadata) && !stages.Reconcile {
		commands = append(commands, fmt.Sprintf("chb odoo journals %d reconcile", acc.OdooJournalID))
	}
	if len(commands) == 0 {
		return
	}
	fmt.Printf("  %sNext:%s %s\n\n", Fmt.Dim, Fmt.Reset, strings.Join(commands, "  →  "))
}

// verifyJournalBalanceAgainstLive refreshes the live balance cache for this
// account and, if it disagrees with the Odoo journal balance, returns a
// multi-line explanation detailing the mismatch and the remediation hint.
// In verbose mode (not under OdooSyncAll) the explanation is also printed
// directly. The returned string is the full detail block when there's a
// mismatch, empty otherwise.
func verifyJournalBalanceAgainstLive(acc *AccountConfig, creds *OdooCredentials, uid int) (float64, string) {
	if acc.OdooJournalID == 0 {
		return 0, ""
	}
	live, cacheKey, err := refreshAccountBalance(acc)
	if cacheKey == "" && err == nil {
		return 0, "" // no live source supported
	}
	if err != nil {
		warn := fmt.Sprintf("  %s⚠ %s: could not fetch live balance: %v%s\n", Fmt.Yellow, acc.Slug, err, Fmt.Reset)
		if !quietOdooContext() {
			Warnf("%s", strings.TrimRight(warn, "\n"))
		}
		return 0, warn
	}

	// Persist the refreshed live balance so the list/detail views see the
	// newest number without a separate --refresh call.
	cache := loadBalanceCache()
	if cache == nil {
		cache = &balanceCache{Balances: map[string]float64{}}
	}
	if cache.Balances == nil {
		cache.Balances = map[string]float64{}
	}
	cache.Balances[cacheKey] = live
	cache.FetchedAt = time.Now().UTC().Format(time.RFC3339)
	saveBalanceCache(cache)

	odooBalance, err := odooJournalCurrentBalance(creds, uid, acc.OdooJournalID)
	if err != nil {
		warn := fmt.Sprintf("  %s⚠ %s: could not fetch Odoo journal balance: %v%s\n", Fmt.Yellow, acc.Slug, err, Fmt.Reset)
		if !quietOdooContext() {
			Warnf("%s", strings.TrimRight(warn, "\n"))
		}
		return live, warn
	}
	if math.Abs(odooBalance-live) < 0.01 {
		return live, "" // balances agree — stay silent
	}
	currency := acc.Currency
	if currency == "" && acc.Token != nil {
		currency = acc.Token.Symbol
	}
	if currency == "" {
		currency = "EUR"
	}
	var liveLabel string
	switch {
	case acc.Provider == "etherscan" && acc.Token != nil:
		liveLabel = fmt.Sprintf("on-chain %s/%s", acc.Chain, acc.Token.Symbol)
	case acc.Provider == "stripe":
		liveLabel = "Stripe"
	default:
		liveLabel = acc.Provider
	}
	diff := odooBalance - live
	detail := fmt.Sprintf("    %s⚠ Odoo %s ≠ live %s (%s) — off by %s%s\n",
		Fmt.Yellow,
		formatBalance(odooBalance, currency),
		formatBalance(live, currency), liveLabel,
		formatBalance(diff, currency),
		Fmt.Reset)
	detail += fmt.Sprintf("    %sHint: chb accounts %s sync  |  chb odoo journals %d fix%s\n",
		Fmt.Dim, acc.Slug, acc.OdooJournalID, Fmt.Reset)
	if !quietOdooContext() {
		Warnf("%s", strings.TrimRight(detail, "\n"))
	}
	return live, detail
}

func accountLocalOdooSnapshot(acc *AccountConfig, txs []TransactionEntry) accountOdooSyncSnapshot {
	snap := accountOdooSyncSnapshot{
		Label:    "Local files",
		Currency: accCurrency(acc),
	}
	for _, tx := range txs {
		snap.TxCount++
		snap.Balance += signedOdooAmountForTransaction(acc, tx)
		if tx.Timestamp > 0 {
			t := time.Unix(tx.Timestamp, 0)
			if snap.FirstTxAt.IsZero() || t.Before(snap.FirstTxAt) {
				snap.FirstTxAt = t
			}
			if t.After(snap.LastTxAt) {
				snap.LastTxAt = t
			}
		}
	}
	snap.Balance = roundCents(snap.Balance)
	return snap
}

func accountLocalOdooSyncSnapshot(acc *AccountConfig) accountOdooSyncSnapshot {
	if acc != nil && acc.Provider == "stripe" {
		if snap, ok := stripeOdooLocalSnapshot(acc); ok {
			return snap
		}
	}
	return accountLocalOdooSnapshot(acc, loadAccountTransactionsForOdoo(acc))
}

func fetchOdooJournalSnapshot(creds *OdooCredentials, uid int, journalID int, currency string) (accountOdooSyncSnapshot, error) {
	snap := accountOdooSyncSnapshot{
		Label:    fmt.Sprintf("Odoo journal #%d", journalID),
		Currency: currency,
	}
	rows, err := odooSearchReadAllMapsLabeled(creds, uid, "account.bank.statement.line",
		[]interface{}{[]interface{}{"journal_id", "=", journalID}},
		[]string{"date", "amount"},
		"date asc, id asc",
		fmt.Sprintf("transactions from journal #%d", journalID),
	)
	if err != nil {
		return snap, err
	}
	for _, row := range rows {
		snap.TxCount++
		snap.Balance += odooFloat(row["amount"])
		if date := odooString(row["date"]); date != "" {
			if t, err := time.Parse("2006-01-02", date); err == nil {
				if snap.FirstTxAt.IsZero() || t.Before(snap.FirstTxAt) {
					snap.FirstTxAt = t
				}
				if t.After(snap.LastTxAt) {
					snap.LastTxAt = t
				}
			}
		}
	}
	snap.Balance = roundCents(snap.Balance)
	return snap, nil
}

// printOdooSyncHeader prints the standardized pre-sync header for
// `chb odoo journals <id> sync` (and the equivalent invocation via
// AccountOdooPush). Mirrors the `chb accounts <slug> sync` style:
// each label padded to the same column width.
func printOdooSyncHeader(creds *OdooCredentials, acc *AccountConfig, since, until time.Time, useHistory, sinceExplicit bool, monthsLimit int, sinceLabelOverride ...string) {
	fmt.Println()
	w := 9 // matches the longest label, "Account: " etc.
	pad := func(label string) string { return padRight(label+":", w) }

	fmt.Printf("  %s%s%s %s (db: %s)\n", Fmt.Dim, pad("Odoo DB"), Fmt.Reset, creds.URL, creds.DB)

	journalName := OdooJournalName(acc.OdooJournalID)
	if journalName == "" {
		journalName = fmt.Sprintf("journal #%d", acc.OdooJournalID)
	}
	fmt.Printf("  %s%s%s %s (id: %d)\n", Fmt.Dim, pad("Journal"), Fmt.Reset, journalName, acc.OdooJournalID)

	fmt.Printf("  %s%s%s %s (%s)\n", Fmt.Dim, pad("Account"), Fmt.Reset, acc.Slug, accountSourceURI(acc))

	sinceLine := odooSyncSinceLabel(acc, since, useHistory, sinceExplicit, monthsLimit)
	if len(sinceLabelOverride) > 0 && strings.TrimSpace(sinceLabelOverride[0]) != "" {
		sinceLine = strings.TrimSpace(sinceLabelOverride[0])
	}
	fmt.Printf("  %s%s%s %s\n", Fmt.Dim, pad("Since"), Fmt.Reset, sinceLine)
	if !until.IsZero() {
		fmt.Printf("  %s%s%s %s\n", Fmt.Dim, pad("Until"), Fmt.Reset, until.AddDate(0, 0, -1).Format("2006-01-02"))
	}
	fmt.Println()
}

// accountSourceURI builds a compact identifier for the account's source.
// Stripe → "stripe:<acct id>". Etherscan → "<chain>:<symbol>:<address>".
func accountSourceURI(acc *AccountConfig) string {
	switch strings.ToLower(acc.Provider) {
	case "stripe":
		return "stripe:" + acc.AccountID
	case "etherscan":
		sym := ""
		if acc.Token != nil {
			sym = acc.Token.Symbol
		}
		return strings.ToLower(acc.Chain) + ":" + sym + ":" + acc.Address
	default:
		return acc.Provider
	}
}

// odooSyncSinceLabel computes the "Since" line annotation. Default is
// the latest local tx date with the "(last tx)" hint, matching the
// cursor-based sync semantics. --history / --since / --months override.
func odooSyncSinceLabel(acc *AccountConfig, since time.Time, useHistory, sinceExplicit bool, monthsLimit int) string {
	if useHistory {
		return "full history (--history)"
	}
	if sinceExplicit && !since.IsZero() {
		return since.Format("2006-01-02") + " (--since)"
	}
	if monthsLimit > 0 {
		cutoff := time.Now().AddDate(0, -monthsLimit, 0)
		return cutoff.Format("2006-01-02") + fmt.Sprintf(" (--months %d)", monthsLimit)
	}
	if cp := latestAccountSourceCheckpoint(acc); cp.Exists && cp.Timestamp > 0 {
		return time.Unix(cp.Timestamp, 0).In(BrusselsTZ()).Format("2006-01-02") + " (last tx)"
	}
	return "default recent window"
}

func stripeOdooCursorSinceLabel(cursor odooImportCursor) string {
	if !cursor.Found || strings.TrimSpace(cursor.Date) == "" {
		return ""
	}
	return strings.TrimSpace(cursor.Date) + " (last Odoo line)"
}

// printOdooSyncSummary prints the standardized post-sync summary:
//
//	New tx:       x
//	Reviewed:     y          (only when --history / --since rescanned)
//	Updated txs:  z          (always when reviewed > 0; otherwise only when z > 0)
//
//	Journal data: x txs between A and B, balance: ZZ EUR
//	Local data:   x txs between A and B, balance: ZZ EUR
//
// In dry-run a single "(dry-run — no writes)" hint is appended below.
func printOdooSyncSummary(synced, reviewed, updated int, dryRun bool, local, journal accountOdooSyncSnapshot, journalErr error) {
	w := 13 // longest label, "Updated txs:"
	pad := func(s string) string { return padRight(s, w) }
	fmt.Printf("  %s %d\n", pad("New tx:"), synced)
	if reviewed > 0 {
		fmt.Printf("  %s %d\n", pad("Reviewed:"), reviewed)
		fmt.Printf("  %s %d\n", pad("Updated txs:"), updated)
	} else if updated > 0 {
		fmt.Printf("  %s %d\n", pad("Updated txs:"), updated)
	}
	fmt.Println()

	rowW := 14
	padR := func(s string) string { return padRight(s, rowW) }
	if journalErr != nil {
		fmt.Printf("  %s%s%s unavailable (%v)\n", Fmt.Dim, padR("Journal data:"), Fmt.Reset, journalErr)
	} else {
		fmt.Printf("  %s%s%s %s\n", Fmt.Dim, padR("Journal data:"), Fmt.Reset, formatOdooSyncSnapshotLine(journal))
	}
	fmt.Printf("  %s%s%s %s\n", Fmt.Dim, padR("Local data:"), Fmt.Reset, formatOdooSyncSnapshotLine(local))
	if dryRun {
		fmt.Printf("\n  %s(dry-run — no writes)%s\n\n", Fmt.Dim, Fmt.Reset)
	} else {
		fmt.Println()
	}
}

func formatOdooSyncSnapshotLine(s accountOdooSyncSnapshot) string {
	if s.TxCount == 0 {
		return fmt.Sprintf("0 txs, balance: %s", formatAccountDataBalance(s.Balance, s.Currency))
	}
	first := formatAccountDataDate(s.FirstTxAt)
	last := formatAccountDataDate(s.LastTxAt)
	return fmt.Sprintf("%s between %s and %s, balance: %s",
		Pluralize(s.TxCount, "tx", ""),
		first, last,
		formatAccountDataBalance(s.Balance, s.Currency))
}

func printOdooSyncState(title string, local accountOdooSyncSnapshot, journal accountOdooSyncSnapshot, journalErr error) {
	fmt.Printf("  %s%s%s\n", Fmt.Bold, title, Fmt.Reset)
	printOdooSyncSnapshot(local)
	if journalErr != nil {
		fmt.Printf("    %sOdoo journal:%s unavailable (%v)\n\n", Fmt.Dim, Fmt.Reset, journalErr)
		return
	}
	printOdooSyncSnapshot(journal)
	fmt.Println()
}

func printOdooSyncSnapshot(s accountOdooSyncSnapshot) {
	fmt.Printf("    %s:%s %d tx, %s",
		s.Label,
		Fmt.Reset,
		s.TxCount,
		formatAccountDataBalance(s.Balance, s.Currency))
	if !s.FirstTxAt.IsZero() && !s.LastTxAt.IsZero() {
		fmt.Printf("  %s(%s → %s)%s",
			Fmt.Dim,
			formatAccountDataTimestamp(s.FirstTxAt),
			formatAccountDataTimestamp(s.LastTxAt),
			Fmt.Reset)
	}
	fmt.Println()
}

func accountLiveBalanceLabel(acc *AccountConfig) string {
	if acc == nil {
		return "source"
	}
	switch {
	case acc.Provider == "stripe":
		return "Stripe API"
	case acc.Provider == "etherscan" && acc.Token != nil:
		return fmt.Sprintf("on-chain %s/%s", acc.Chain, acc.Token.Symbol)
	case acc.Provider != "":
		return acc.Provider
	default:
		return "source"
	}
}

func verifyAccountLocalAgainstOnchainCache(acc *AccountConfig, liveBalance *float64) *accountSyncVerification {
	if acc == nil || acc.Provider != "etherscan" || acc.Token == nil {
		return nil
	}

	rawTransfers := loadAccountOnchainTransfers(acc)
	if len(rawTransfers) == 0 {
		return nil
	}

	currency := acc.Currency
	if currency == "" {
		currency = acc.Token.Symbol
	}
	result := &accountSyncVerification{
		AccountSlug:    acc.Slug,
		Currency:       currency,
		OnchainTxCount: len(rawTransfers),
		MissingByMonth: map[string][]missingOnchainTransfer{},
	}
	for _, raw := range rawTransfers {
		ts, err := strconv.ParseInt(raw.TimeStamp, 10, 64)
		if err != nil || ts <= 0 {
			continue
		}
		t := time.Unix(ts, 0)
		if result.OnchainFirstTxAt.IsZero() || t.Before(result.OnchainFirstTxAt) {
			result.OnchainFirstTxAt = t
		}
		if t.After(result.OnchainLastTxAt) {
			result.OnchainLastTxAt = t
		}
	}

	if liveBalance != nil {
		result.OnchainBalance = *liveBalance
		result.OnchainBalanceOK = true
	} else if v, _, err := refreshAccountBalance(acc); err == nil {
		result.OnchainBalance = v
		result.OnchainBalanceOK = true
	} else {
		result.LiveBalanceError = err
		result.OnchainBalance = accountBalanceFromRawTransfers(acc, rawTransfers)
	}

	totals := accountTotalsFromGeneratedTransactions(acc, loadAccountTransactionsWithOptions(acc, true))
	if totals != nil {
		result.LocalTxCount = totals.TxCount
		result.LocalBalance = totals.CurrentBalance
		result.LocalFirstTxAt = totals.FirstTxAt
		result.LocalLastTxAt = totals.LastTxAt
	}

	localKeys := map[string]int{}
	for _, tx := range loadAccountTransactionsWithOptions(acc, true) {
		if tx.Currency != "" && currency != "" && !strings.EqualFold(tx.Currency, currency) {
			continue
		}
		localKeys[accountLocalTransactionCompareKey(acc, tx)]++
	}

	for _, raw := range rawTransfers {
		key := accountRawTransferCompareKey(acc, raw)
		if localKeys[key] > 0 {
			localKeys[key]--
			continue
		}
		missing := missingTransferFromRaw(acc, raw)
		result.Missing = append(result.Missing, missing)
		result.MissingByMonth[missing.Month] = append(result.MissingByMonth[missing.Month], missing)
		if result.OldestMonth == "" || missing.Month < result.OldestMonth {
			result.OldestMonth = missing.Month
		}
	}

	if accountSyncVerificationMatches(result) {
		return result
	}
	return result
}

func accountSyncVerificationMatches(v *accountSyncVerification) bool {
	if v == nil {
		return true
	}
	balanceMatches := !v.OnchainBalanceOK || math.Abs(v.LocalBalance-v.OnchainBalance) < 0.01
	return balanceMatches && v.LocalTxCount == v.OnchainTxCount && len(v.Missing) == 0
}

func printAccountSyncVerification(v *accountSyncVerification) {
	if v == nil {
		return
	}
	fmt.Println()
	for _, row := range accountSyncVerificationSummaryRows(v) {
		fmt.Printf("  %s%s%s %s\n", Fmt.Dim, padRight(row[0]+":", 13), Fmt.Reset, row[1])
	}
	if !v.OnchainBalanceOK && v.LiveBalanceError != nil {
		fmt.Printf("  %sOn-chain live balance unavailable: %v; using raw cache sum%s\n", Fmt.Dim, v.LiveBalanceError, Fmt.Reset)
	}

	if accountSyncVerificationMatches(v) {
		fmt.Println()
		return
	}

	fmt.Printf("  %s⚠ mismatch%s\n", Fmt.Yellow, Fmt.Reset)
	if math.Abs(v.LocalBalance-v.OnchainBalance) >= 0.01 {
		fmt.Printf("  %sBalance diff:%s %s\n", Fmt.Dim, Fmt.Reset, formatAccountDataBalance(v.LocalBalance-v.OnchainBalance, v.Currency))
	}
	if v.LocalTxCount != v.OnchainTxCount {
		fmt.Printf("  %sTx count diff:%s local %d vs on-chain %d\n", Fmt.Dim, Fmt.Reset, v.LocalTxCount, v.OnchainTxCount)
	}

	if len(v.Missing) > 0 {
		fmt.Printf("\n    %sMissing local transactions by month:%s\n", Fmt.Dim, Fmt.Reset)
		months := make([]string, 0, len(v.MissingByMonth))
		for month := range v.MissingByMonth {
			months = append(months, month)
		}
		sort.Strings(months)
		for _, month := range months {
			items := v.MissingByMonth[month]
			fmt.Printf("      %s: %d missing\n", month, len(items))
			for _, item := range items {
				fmt.Printf("        %s  %s  %s  %s  counterparty=%s\n",
					item.Timestamp.In(BrusselsTZ()).Format("2006-01-02"),
					item.Hash,
					item.Type,
					formatAccountDataBalance(item.Amount, v.Currency),
					truncateAddr(item.Counterparty))
			}
		}
	}

	fmt.Printf("\n    %sFix:%s\n", Fmt.Dim, Fmt.Reset)
	fmt.Printf("      chb accounts %s sync --history\n", v.AccountSlug)
	if v.OldestMonth != "" {
		fmt.Printf("      chb generate transactions --since %s\n", v.OldestMonth)
	}
	fmt.Printf("      chb accounts %s --refresh\n\n", v.AccountSlug)
}

func accountSyncVerificationSummaryRows(v *accountSyncVerification) [][]string {
	if v == nil {
		return nil
	}
	return [][]string{
		{
			"Onchain data",
			accountSyncVerificationSummary(v.OnchainTxCount, v.OnchainFirstTxAt, v.OnchainLastTxAt, v.OnchainBalance, v.Currency),
		},
		{
			"Local data",
			accountSyncVerificationSummary(v.LocalTxCount, v.LocalFirstTxAt, v.LocalLastTxAt, v.LocalBalance, v.Currency),
		},
	}
}

func accountSyncVerificationSummary(txCount int, first, last time.Time, balance float64, currency string) string {
	return fmt.Sprintf("%s between %s and %s, balance: %s",
		Pluralize(txCount, "tx", ""),
		formatAccountDataDate(first),
		formatAccountDataDate(last),
		formatAccountDataBalance(balance, currency))
}

func accountSyncVerificationRows(v *accountSyncVerification) [][]string {
	if v == nil {
		return nil
	}
	return [][]string{
		{
			"On-chain",
			formatAccountDataBalance(v.OnchainBalance, v.Currency),
			Pluralize(v.OnchainTxCount, "tx", ""),
			formatAccountDataTimestamp(v.OnchainFirstTxAt),
			formatAccountDataTimestamp(v.OnchainLastTxAt),
		},
		{
			"Local files",
			formatAccountDataBalance(v.LocalBalance, v.Currency),
			Pluralize(v.LocalTxCount, "tx", ""),
			formatAccountDataTimestamp(v.LocalFirstTxAt),
			formatAccountDataTimestamp(v.LocalLastTxAt),
		},
	}
}

func formatAccountDataDate(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.In(BrusselsTZ()).Format("2006-01-02")
}

func accountVerificationRangeLabel(first, last time.Time) string {
	if first.IsZero() || last.IsZero() {
		return ""
	}
	return fmt.Sprintf("  %s(%s → %s)%s",
		Fmt.Dim,
		formatAccountDataTimestamp(first),
		formatAccountDataTimestamp(last),
		Fmt.Reset)
}

func loadAccountOnchainTransfers(acc *AccountConfig) []etherscansource.TokenTransfer {
	if acc == nil || acc.Token == nil {
		return nil
	}
	dataDir := DataDir()
	filename := etherscansource.FileName(acc.Slug, acc.Token.Symbol)
	var transfers []etherscansource.TokenTransfer
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
			path := etherscansource.Path(dataDir, yd.Name(), md.Name(), acc.Chain, filename)
			cache, ok := etherscansource.LoadCache(path)
			if !ok {
				continue
			}
			transfers = append(transfers, cache.Transactions...)
		}
	}
	return transfers
}

func latestAccountSourceCheckpoint(acc *AccountConfig) accountSourceCheckpoint {
	var latest accountSourceCheckpoint
	if acc == nil || acc.Provider != "etherscan" || acc.Token == nil {
		return latest
	}
	for _, tx := range loadAccountOnchainTransfers(acc) {
		ts, err := strconv.ParseInt(tx.TimeStamp, 10, 64)
		if err != nil || ts <= 0 {
			continue
		}
		if !latest.Exists || ts > latest.Timestamp {
			t := time.Unix(ts, 0).In(BrusselsTZ())
			latest = accountSourceCheckpoint{
				Exists:    true,
				Month:     t.Format("2006-01"),
				Timestamp: ts,
				Hash:      tx.Hash,
			}
		}
	}
	return latest
}

func accountSourceMonthFingerprints(acc *AccountConfig) map[string]string {
	out := map[string]string{}
	if acc == nil || acc.Provider != "etherscan" || acc.Token == nil {
		return out
	}
	dataDir := DataDir()
	filename := etherscansource.FileName(acc.Slug, acc.Token.Symbol)
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
			ym := yd.Name() + "-" + md.Name()
			path := etherscansource.Path(dataDir, yd.Name(), md.Name(), acc.Chain, filename)
			cache, ok := etherscansource.LoadCache(path)
			if !ok {
				continue
			}
			out[ym] = accountSourceTransfersFingerprint(acc, cache.Transactions)
		}
	}
	return out
}

func accountChangedSourceMonths(acc *AccountConfig, before map[string]string) []string {
	after := accountSourceMonthFingerprints(acc)
	changedSet := map[string]bool{}
	for month, fingerprint := range after {
		if before[month] != fingerprint {
			changedSet[month] = true
		}
	}
	for month := range before {
		if _, ok := after[month]; !ok {
			changedSet[month] = true
		}
	}
	months := make([]string, 0, len(changedSet))
	for month := range changedSet {
		months = append(months, month)
	}
	sort.Strings(months)
	return months
}

func accountSourceTransfersFingerprint(acc *AccountConfig, transfers []etherscansource.TokenTransfer) string {
	keys := make([]string, 0, len(transfers))
	for _, tx := range transfers {
		keys = append(keys, accountRawTransferCompareKey(acc, tx)+"|"+tx.TimeStamp+"|"+strings.ToLower(tx.From)+"|"+strings.ToLower(tx.To))
	}
	sort.Strings(keys)
	return strings.Join(keys, "\n")
}

func accountBalanceFromRawTransfers(acc *AccountConfig, transfers []etherscansource.TokenTransfer) float64 {
	var balance float64
	for _, raw := range transfers {
		amount := accountRawTransferAmount(acc, raw)
		if strings.EqualFold(raw.From, acc.Address) {
			balance -= amount
		} else {
			balance += amount
		}
	}
	return roundCents(balance)
}

func accountRawTransferCompareKey(acc *AccountConfig, raw etherscansource.TokenTransfer) string {
	return strings.ToLower(raw.Hash) + "|" + accountRawTransferDirection(acc, raw) + "|" + accountCompareAmount(accountRawTransferAmount(acc, raw))
}

func accountLocalTransactionCompareKey(acc *AccountConfig, tx TransactionEntry) string {
	return strings.ToLower(tx.TxHash) + "|" + accountLocalTransactionDirection(acc, tx) + "|" + accountCompareAmount(math.Abs(accountTransactionAmount(tx)))
}

func accountRawTransferDirection(acc *AccountConfig, raw etherscansource.TokenTransfer) string {
	if acc != nil && strings.EqualFold(raw.From, acc.Address) {
		return "DEBIT"
	}
	return "CREDIT"
}

func accountLocalTransactionDirection(acc *AccountConfig, tx TransactionEntry) string {
	if tx.Type == "INTERNAL" {
		return internalTransactionDirection(acc, tx)
	}
	if tx.IsOutgoing() {
		return "DEBIT"
	}
	return "CREDIT"
}

func accountTransactionAmount(tx TransactionEntry) float64 {
	if tx.NormalizedAmount != 0 {
		return tx.NormalizedAmount
	}
	return tx.Amount
}

func accountRawTransferAmount(acc *AccountConfig, raw etherscansource.TokenTransfer) float64 {
	decimals := 18
	if acc != nil && acc.Token != nil {
		decimals = acc.Token.Decimals
	}
	if raw.TokenDecimal != "" {
		if parsed, err := strconv.Atoi(raw.TokenDecimal); err == nil {
			decimals = parsed
		}
	}
	return etherscansource.ParseTokenValue(raw.Value, decimals)
}

func accountCompareAmount(amount float64) string {
	return strconv.FormatFloat(math.Round(amount*1e8)/1e8, 'f', 8, 64)
}

func filterTransactionsAfterOdooCursor(acc *AccountConfig, txs []TransactionEntry, cursor odooImportCursor) ([]TransactionEntry, bool) {
	if !cursor.Found || cursor.UniqueImportID == "" {
		return txs, false
	}
	lastIdx := -1
	for i, tx := range txs {
		if buildUniqueImportID(acc, tx) == cursor.UniqueImportID {
			lastIdx = i
		}
	}
	if lastIdx == -1 {
		return txs, false
	}
	if lastIdx+1 >= len(txs) {
		return []TransactionEntry{}, true
	}
	return append([]TransactionEntry(nil), txs[lastIdx+1:]...), true
}

func missingTransferFromRaw(acc *AccountConfig, raw etherscansource.TokenTransfer) missingOnchainTransfer {
	ts, _ := strconv.ParseInt(raw.TimeStamp, 10, 64)
	t := time.Unix(ts, 0)
	direction := accountRawTransferDirection(acc, raw)
	counterparty := raw.From
	if direction == "DEBIT" {
		counterparty = raw.To
	}
	return missingOnchainTransfer{
		Month:        t.In(BrusselsTZ()).Format("2006-01"),
		Timestamp:    t,
		Hash:         raw.Hash,
		Type:         direction,
		Amount:       accountRawTransferAmount(acc, raw),
		From:         raw.From,
		To:           raw.To,
		Counterparty: counterparty,
	}
}

// odooJournalLineCount returns the number of statement lines on the journal.
// Returns 0 on error.
func odooJournalLineCount(creds *OdooCredentials, uid int, journalID int) int {
	result, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement.line", "search_count",
		[]interface{}{[]interface{}{
			[]interface{}{"journal_id", "=", journalID},
		}}, nil)
	if err != nil {
		return 0
	}
	var count int
	_ = json.Unmarshal(result, &count)
	return count
}

// odooJournalLineSum returns Σ(line.amount) across every statement line on
// the given journal.
// odooJournalCurrentBalance returns the same value the Odoo reconciliation
// widget shows for a journal: the computed `current_statement_balance`
// on account.journal (the running balance over time, built from
// statement.balance_end_real). Falls back to the move-line sum on the
// journal's default account if the field isn't available.
//
// Note: if the journal's statement.balance_end_real values are stale
// (e.g. after a duplicate-line cleanup that didn't trigger a balance
// walk), this number will diverge from the move-line truth. Run
// `chb odoo journals <id> fix` to recompute statement balances.
func odooJournalCurrentBalance(creds *OdooCredentials, uid int, journalID int) (float64, error) {
	result, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.journal", "read",
		[]interface{}{[]interface{}{journalID}, []string{"current_statement_balance"}}, nil)
	if err == nil {
		var rows []struct {
			Balance float64 `json:"current_statement_balance"`
		}
		if json.Unmarshal(result, &rows) == nil && len(rows) > 0 {
			return rows[0].Balance, nil
		}
	}
	return odooJournalLineSum(creds, uid, journalID)
}

// odooStatementLineCount returns the number of account.bank.statement.line
// records on a journal.
func odooStatementLineCount(creds *OdooCredentials, uid int, journalID int) (int, error) {
	result, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement.line", "search_count",
		[]interface{}{[]interface{}{
			[]interface{}{"journal_id", "=", journalID},
		}}, nil)
	if err != nil {
		return 0, err
	}
	var n int
	if err := json.Unmarshal(result, &n); err != nil {
		return 0, err
	}
	return n, nil
}

func odooJournalLineSum(creds *OdooCredentials, uid int, journalID int) (float64, error) {
	result, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement.line", "read_group",
		[]interface{}{
			[]interface{}{[]interface{}{"journal_id", "=", journalID}},
			[]string{"amount:sum"},
			[]string{},
		},
		map[string]interface{}{"lazy": false})
	if err != nil {
		return 0, err
	}
	var groups []struct {
		Amount float64 `json:"amount"`
	}
	if err := json.Unmarshal(result, &groups); err != nil {
		return 0, err
	}
	if len(groups) == 0 {
		return 0, nil
	}
	return groups[0].Amount, nil
}

// syncBlockchainToOdoo syncs blockchain/monerium transactions to Odoo (no statements, just lines).
func syncBlockchainToOdoo(acc *AccountConfig, creds *OdooCredentials, uid int, monthsLimit int, dryRun bool, skipReconciliation bool, sinceDate, untilDate time.Time, useHistory bool, previewLimit int) (blockchainOdooSyncResult, error) {
	localTxs := loadAccountTransactionsForOdoo(acc)
	sort.SliceStable(localTxs, func(i, j int) bool {
		if localTxs[i].Timestamp == localTxs[j].Timestamp {
			return buildUniqueImportID(acc, localTxs[i]) < buildUniqueImportID(acc, localTxs[j])
		}
		return localTxs[i].Timestamp < localTxs[j].Timestamp
	})
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
	if !sinceDate.IsZero() {
		var filtered []TransactionEntry
		for _, tx := range localTxs {
			if !time.Unix(tx.Timestamp, 0).Before(sinceDate) {
				filtered = append(filtered, tx)
			}
		}
		localTxs = filtered
		// --since narrows the local window; use the full-history path
		// to compare against Odoo so older lines in that window also
		// surface (rather than only those newer than the cursor).
		useHistory = true
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

	// Auto-escalate to history mode when local has more txs than Odoo.
	// The cursor-based pass only looks at local txs newer than the latest
	// Odoo line, so older missing txs (e.g. a tx that was deleted in
	// Odoo, or a gap from an older partial sync) would be invisible.
	if !useHistory && acc.OdooJournalID != 0 {
		if odooCount, err := odooStatementLineCount(creds, uid, acc.OdooJournalID); err == nil && len(localTxs) > odooCount {
			odooLog("  %sDrift detected: local has %s, Odoo has %s — escalating to full history check.%s\n",
				Fmt.Dim, Pluralize(len(localTxs), "tx", ""), Pluralize(odooCount, "line", ""), Fmt.Reset)
			useHistory = true
		}
	}

	scopeLabel := "latest Odoo line"
	if useHistory {
		scopeLabel = "--history"
	} else {
		cursor, err := fetchLatestOdooImportCursor(creds, uid, acc.OdooJournalID)
		if err != nil {
			Warnf("  %s⚠ Could not read latest Odoo line, falling back to full duplicate check: %v%s", Fmt.Yellow, err, Fmt.Reset)
			useHistory = true
		} else if cursor.Found {
			filtered, matched := filterTransactionsAfterOdooCursor(acc, localTxs, cursor)
			if matched {
				localTxs = filtered
			} else {
				Warnf("  %s⚠ Latest Odoo import not found locally (%s), falling back to full duplicate check%s",
					Fmt.Yellow, cursor.UniqueImportID, Fmt.Reset)
				useHistory = true
			}
		}
		_ = scopeLabel
	}

	var existingIDs map[string]bool
	var err error
	if useHistory {
		existingIDs, err = fetchOdooImportIDs(creds.URL, creds.DB, uid, creds.Password, acc.OdooJournalID)
	} else {
		existingIDs, err = fetchOdooImportIDsForTransactions(creds, uid, acc.OdooJournalID, acc, localTxs)
	}
	if err != nil {
		return blockchainOdooSyncResult{}, fmt.Errorf("failed to fetch existing Odoo entries: %v", err)
	}

	var missing []TransactionEntry
	partnerUpdates := 0
	localByImportID := map[string][]TransactionEntry{}
	for _, tx := range localTxs {
		importID := buildUniqueImportID(acc, tx)
		localByImportID[importID] = append(localByImportID[importID], tx)
		if existingIDs[importID] {
			if !dryRun {
				if updated, err := ensureOdooStatementLinePartnerBank(creds, uid, acc.OdooJournalID, importID, tx); err == nil && updated {
					partnerUpdates++
				} else if err != nil {
					Warnf("  %s⚠ Could not update existing Odoo partner link %s: %v%s", Fmt.Yellow, importID, err, Fmt.Reset)
				}
			}
			continue
		}
		missing = append(missing, tx)
	}

	if useHistory && len(localTxs) > len(existingIDs) && len(missing) == 0 {
		reportLocalImportIDCollisions(creds, uid, acc, localByImportID, existingIDs)
	}

	if len(missing) == 0 {
		if dryRun {
			if err := printOdooBlockchainDryRunPlan(creds, uid, acc, localTxs, existingIDs, previewLimit); err != nil {
				return blockchainOdooSyncResult{}, err
			}
		}
		if partnerUpdates > 0 {
			return blockchainOdooSyncResult{Summary: fmt.Sprintf("already in sync, %d partner links updated", partnerUpdates)}, nil
		}
		return blockchainOdooSyncResult{Summary: "already in sync"}, nil
	}

	if dryRun {
		if err := printOdooBlockchainDryRunPlan(creds, uid, acc, localTxs, existingIDs, previewLimit); err != nil {
			return blockchainOdooSyncResult{}, err
		}
		return blockchainOdooSyncResult{Summary: fmt.Sprintf("dry-run: %d tx would be uploaded", len(missing))}, nil
	}

	partnerCache := map[string]int{}
	stats := &syncStats{}
	synced, errors := 0, 0
	odooRules, _ := LoadOdooRules()
	for i, tx := range missing {
		t := time.Unix(tx.Timestamp, 0).In(BrusselsTZ())
		amt := signedOdooAmountForTransaction(acc, tx)

		paymentRef := buildOdooPaymentRef(tx)

		partnerEmail, _ := tx.Metadata["email"].(string)
		partnerBankID, partnerID := resolveOdooPartnerBankForTransaction(creds, uid, tx)
		if partnerID == 0 {
			partnerID = resolveOdooPartner(creds, uid, tx.Counterparty, partnerEmail, stringMetadata(tx.Metadata, "stripeCustomerId"), tx.Collective, false, partnerCache, stats)
		}

		matchedRule := MatchOdooRule(odooRules, tx)
		if matchedRule != nil && matchedRule.Set.PartnerID > 0 {
			partnerID = matchedRule.Set.PartnerID
		}

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
		if partnerBankID > 0 {
			lineData["partner_bank_id"] = partnerBankID
		}
		if narr := buildOdooNarration(acc, tx); narr != "" {
			lineData["narration"] = narr
		}

		odooLog("  %s[%d/%d]%s creating %s  %s  %s\n",
			Fmt.Dim, i+1, len(missing), Fmt.Reset,
			t.Format("2006-01-02"),
			formatAccountDataBalance(amt, accCurrency(acc)),
			paymentRef)
		result, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
			"account.bank.statement.line", "create",
			[]interface{}{[]interface{}{lineData}}, nil)
		if err != nil {
			failure := classifyStatementLineCreateFailure(lineData, err)
			failures := []statementLineCreateFailure{failure}
			annotateStatementLineCreateFailures(creds, uid, failures)
			stats.recordCreateFailures(failures)
			failure = failures[0]
			fmt.Printf("  %s✗ %s %s: %s%s\n", Fmt.Red, t.Format("2006-01-02"), paymentRef, failure.Reason, Fmt.Reset)
			errors++
			continue
		}
		createdIDs := parseOdooCreatedIDs(result)
		if skipReconciliation {
			// Caller requested a fast import only; reconciliation can be run later
			// with `chb odoo journals <id> reconcile`.
		} else if tx.Type == "INTERNAL" {
			odooLog("    %smarking internal transfer in Odoo...%s\n", Fmt.Dim, Fmt.Reset)
			markCreatedStatementLinesInternal(creds, uid, createdIDs, false, stats)
		} else {
			odooLog("    %sreconciling statement line...%s\n", Fmt.Dim, Fmt.Reset)
			reconcileCreatedStatementLines(creds, uid, createdIDs, false, stats)
		}
		if matchedRule != nil && matchedRule.Set.AccountCode != "" {
			if err := applyOdooRuleAccount(creds, uid, createdIDs, matchedRule.Set.AccountCode); err != nil {
				Warnf("  %s⚠ rule account %s: %v%s", Fmt.Yellow, matchedRule.Set.AccountCode, err, Fmt.Reset)
			} else {
				odooLog("    %s↳ rule: account %s%s\n", Fmt.Dim, matchedRule.Set.AccountCode, Fmt.Reset)
			}
		}
		odooLog("    %s✓ done%s\n", Fmt.Green, Fmt.Reset)
		synced++
	}
	odooLog("  %s+%d tx%s\n", Fmt.Green, synced, Fmt.Reset)
	stats.LinesCreated = synced
	if !quietOdooContext() {
		stats.print()
	}
	warnInvalidStatements(creds, uid, acc.OdooJournalID)
	var summary string
	if synced == 0 {
		summary = "already in sync"
	} else {
		summary = fmt.Sprintf("%d new", synced)
	}
	if stats.LinesReconciled > 0 {
		summary = fmt.Sprintf("%s, %d reconciled", summary, stats.LinesReconciled)
	}
	if partnerUpdates > 0 {
		summary = fmt.Sprintf("%s, %d partner links updated", summary, partnerUpdates)
	}
	if errors > 0 {
		summary = fmt.Sprintf("%d new, %d failed", synced, errors)
	}
	return blockchainOdooSyncResult{Summary: summary, Synced: synced}, nil
}

func printOdooBlockchainDryRunPlan(creds *OdooCredentials, uid int, acc *AccountConfig, txs []TransactionEntry, existingIDs map[string]bool, limit int) error {
	if quietOdooContext() {
		return nil
	}
	plan, err := buildOdooBlockchainDryRunPlan(creds, uid, acc, txs, existingIDs, limit)
	if err != nil {
		return err
	}
	if len(plan) == 0 {
		odooLog("  %sNo local transactions in selected window.%s\n\n", Fmt.Dim, Fmt.Reset)
		return nil
	}
	printOdooDryRunPlanRows(plan, accCurrency(acc))
	return nil
}

func printOdooDryRunPlanRows(plan []odooSyncPlanRow, currency string) {
	headers := []string{"Action", "Date", "Description", "Partner", "Account", "Amount", "Ref"}
	rows := make([][]string, 0, len(plan))
	totals := map[string]int{}
	var amountTotal float64
	for _, row := range plan {
		totals[row.Action]++
		amountTotal += row.Amount
		desc := row.Description
		if row.Reason != "" {
			desc = fmt.Sprintf("%s (%s)", desc, row.Reason)
		}
		rows = append(rows, []string{
			row.Action,
			row.Date,
			Truncate(desc, 36),
			Truncate(row.Partner, 20),
			Truncate(row.Account, 18),
			formatBalancePlain(row.Amount, row.Currency),
			row.Ref,
		})
	}
	totalLabel := Pluralize(len(plan), "planned line", "")
	var parts []string
	for _, action := range []string{"create", "update", "skip"} {
		if totals[action] > 0 {
			parts = append(parts, fmt.Sprintf("%d %s", totals[action], action))
		}
	}
	if len(parts) > 0 {
		totalLabel += " (" + strings.Join(parts, ", ") + ")"
	}
	renderTicketsTable(headers, rows, []string{"", "", totalLabel, "", "", formatBalancePlain(amountTotal, currency), ""}, map[int]bool{5: true})
	odooLog("\n")
}

func buildOdooBlockchainDryRunPlan(creds *OdooCredentials, uid int, acc *AccountConfig, txs []TransactionEntry, existingIDs map[string]bool, limit int) ([]odooSyncPlanRow, error) {
	if limit > 0 && len(txs) > limit {
		txs = txs[:limit]
	}

	ids := make([]string, 0, len(txs))
	for _, tx := range txs {
		if id := buildUniqueImportID(acc, tx); id != "" && existingIDs[id] {
			ids = append(ids, id)
		}
	}
	existingRows, err := fetchOdooStatementLinesByImportID(creds, uid, ids)
	if err != nil {
		return nil, fmt.Errorf("fetch existing Odoo lines for preview: %v", err)
	}

	odooRules, _ := LoadOdooRules()
	plan := make([]odooSyncPlanRow, 0, len(txs))
	for _, tx := range txs {
		t := time.Unix(tx.Timestamp, 0).In(BrusselsTZ())
		importID := buildUniqueImportID(acc, tx)
		amount := signedOdooAmountForTransaction(acc, tx)
		paymentRef := buildOdooPaymentRef(tx)
		action := "create"
		reason := ""
		account := ""

		if matchedRule := MatchOdooRule(odooRules, tx); matchedRule != nil {
			if matchedRule.Set.AccountCode != "" {
				account = matchedRule.Set.AccountCode
			}
			if matchedRule.Set.PartnerID > 0 {
				reason = fmt.Sprintf("rule partner #%d", matchedRule.Set.PartnerID)
			}
		}

		if existingIDs[importID] {
			action = "skip"
			if row := existingRows[importID]; row != nil {
				update := map[string]interface{}{}
				if paymentRef != "" && odooString(row["payment_ref"]) != paymentRef {
					update["payment_ref"] = paymentRef
				}
				if narr := buildOdooNarration(acc, tx); narr != "" && odooString(row["narration"]) != narr {
					update["narration"] = narr
				}
				if len(update) > 0 {
					action = "update"
					reason = strings.Join(sortedMapKeys(update), ", ")
				}
			}
		}

		plan = append(plan, odooSyncPlanRow{
			Action:      action,
			Date:        t.Format("2006-01-02"),
			Description: paymentRef,
			Partner:     tx.Counterparty,
			Account:     account,
			Amount:      amount,
			Currency:    accCurrency(acc),
			Ref:         importID,
			Reason:      reason,
		})
	}
	return plan, nil
}

func fetchOdooStatementLinesByImportID(creds *OdooCredentials, uid int, importIDs []string) (map[string]map[string]interface{}, error) {
	rowsByID := map[string]map[string]interface{}{}
	if len(importIDs) == 0 {
		return rowsByID, nil
	}
	seen := map[string]bool{}
	var uniq []string
	for _, id := range importIDs {
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		uniq = append(uniq, id)
	}
	for start := 0; start < len(uniq); start += 80 {
		end := start + 80
		if end > len(uniq) {
			end = len(uniq)
		}
		values := make([]interface{}, 0, end-start)
		for _, id := range uniq[start:end] {
			values = append(values, id)
		}
		rows, err := odooSearchReadAllMaps(creds, uid, "account.bank.statement.line",
			[]interface{}{[]interface{}{"unique_import_id", "in", values}},
			[]string{"id", "date", "payment_ref", "narration", "partner_id", "partner_bank_id", "amount", "unique_import_id", "journal_id", "move_id", "statement_id", "create_date", "write_date"},
			"date desc, id desc",
		)
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			if id := odooString(row["unique_import_id"]); id != "" {
				rowsByID[id] = row
			}
		}
	}
	return rowsByID, nil
}

func sortedMapKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func sortedIntMapKeys(m map[string]int) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// syncStats tracks metrics for the sync summary report.
type syncStats struct {
	LinesCreated       int
	LinesSkipped       int
	LinesFailed        int
	CreateFailures     map[string]int
	CreateDetails      []string
	Statements         int
	PartnersMatched    int
	PartnersCreated    int
	PartnersSkipped    int
	Ambiguous          []string // partner merge suggestions
	Charges            int      // number of charge/payment lines (all providers)
	ChargesGross       float64  // total gross charges
	Refunds            int      // number of refund lines
	RefundsTotal       float64  // total refund amount (negative)
	ChargeFees         float64  // total charge fees (from all providers)
	StripeFees         float64  // Stripe billing fees (separate debit lines)
	PayoutsTotal       float64  // total payout withdrawals (negative)
	LinesReconciled    int
	InternalTransfers  int
	ReconcileAmbiguous int
	ReconcileNoPartner int
	ReconcileNoMatch   int
	ReconcileErrors    int
	ReconcileDetails   []string
}

func (s *syncStats) recordCreateFailures(failures []statementLineCreateFailure) {
	if s == nil || len(failures) == 0 {
		return
	}
	if s.CreateFailures == nil {
		s.CreateFailures = map[string]int{}
	}
	for _, failure := range failures {
		reason := failure.Reason
		if reason == "" {
			reason = "unknown create error"
		}
		s.LinesFailed++
		s.CreateFailures[reason]++
		detail := reason
		if failure.ImportID != "" {
			detail = failure.ImportID + ": " + reason
		}
		if failure.Detail != "" && failure.Detail != reason {
			detail += " (" + failure.Detail + ")"
		}
		s.CreateDetails = append(s.CreateDetails, detail)
	}
}

func (s *syncStats) recordPartnerMergeSuggestion(name, email string, selectedID int, candidateIDs []int) {
	if s == nil || len(candidateIDs) <= 1 {
		return
	}
	label := strings.TrimSpace(name)
	email = strings.TrimSpace(email)
	if email != "" {
		label = fmt.Sprintf("%s <%s>", label, email)
	}
	if label == "" {
		label = "Odoo partner"
	}
	suggestion := fmt.Sprintf("%s: linked to oldest partner #%d; consider merging duplicate partners %v", label, selectedID, candidateIDs)
	for _, existing := range s.Ambiguous {
		if existing == suggestion {
			return
		}
	}
	s.PartnersSkipped++
	s.Ambiguous = append(s.Ambiguous, suggestion)
}

func (s *syncStats) print() {
	fmt.Printf("\n  %s── Summary ──%s\n", Fmt.Bold, Fmt.Reset)
	lineSummary := fmt.Sprintf("%d created, %d skipped", s.LinesCreated, s.LinesSkipped)
	if s.LinesFailed > 0 {
		lineSummary += fmt.Sprintf(", %d failed", s.LinesFailed)
	}
	fmt.Printf("    Lines:          %s\n", lineSummary)
	if len(s.CreateFailures) > 0 {
		for _, reason := range sortedIntMapKeys(s.CreateFailures) {
			fmt.Printf("      %s✗ %s: %d%s\n", Fmt.Red, reason, s.CreateFailures[reason], Fmt.Reset)
		}
	}
	if len(s.CreateDetails) > 0 {
		limit := len(s.CreateDetails)
		if limit > 10 {
			limit = 10
		}
		for _, detail := range s.CreateDetails[:limit] {
			Warnf("      %s⚠ %s%s", Fmt.Yellow, detail, Fmt.Reset)
		}
		if len(s.CreateDetails) > limit {
			Warnf("      %s⚠ ... %d more create failure(s)%s", Fmt.Yellow, len(s.CreateDetails)-limit, Fmt.Reset)
		}
	}
	fmt.Printf("    Statements:     %d\n", s.Statements)
	fmt.Printf("    Partners:       %d matched, %d created, %d merge suggested\n",
		s.PartnersMatched, s.PartnersCreated, s.PartnersSkipped)
	if len(s.Ambiguous) > 0 {
		for _, a := range s.Ambiguous {
			Warnf("      %s⚠ %s%s", Fmt.Yellow, a, Fmt.Reset)
		}
	}
	fmt.Printf("    Reconciled:     %d, %s, %d ambiguous\n",
		s.LinesReconciled, Pluralize(s.InternalTransfers, "internal transfer", ""), s.ReconcileAmbiguous)
	if s.ReconcileNoPartner > 0 || s.ReconcileNoMatch > 0 || s.ReconcileErrors > 0 {
		fmt.Printf("    Unreconciled:   %d no partner, %d no match, %d errors\n",
			s.ReconcileNoPartner, s.ReconcileNoMatch, s.ReconcileErrors)
	}
	if len(s.ReconcileDetails) > 0 {
		for _, detail := range s.ReconcileDetails {
			Warnf("      %s⚠ %s%s", Fmt.Yellow, detail, Fmt.Reset)
		}
	}
	hasStripeBreakdown := s.Charges > 0 || s.Refunds > 0 || s.ChargeFees > 0 || s.StripeFees > 0 || math.Abs(s.PayoutsTotal) > 0.005
	if !hasStripeBreakdown {
		fmt.Println()
		return
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

func (s *syncStats) printStripeCompact() {
	lineSummary := fmt.Sprintf("%d created, %d skipped", s.LinesCreated, s.LinesSkipped)
	if s.LinesFailed > 0 {
		lineSummary += fmt.Sprintf(", %d failed", s.LinesFailed)
	}
	parts := []string{lineSummary, Pluralize(s.Statements, "statement", "")}
	if s.PartnersMatched > 0 || s.PartnersCreated > 0 || s.PartnersSkipped > 0 {
		parts = append(parts, fmt.Sprintf("partners %d matched/%d created/%d merge suggested", s.PartnersMatched, s.PartnersCreated, s.PartnersSkipped))
	}
	if s.LinesReconciled > 0 || s.InternalTransfers > 0 || s.ReconcileAmbiguous > 0 {
		parts = append(parts, fmt.Sprintf("reconciled %d, %d transfer, %d ambiguous", s.LinesReconciled, s.InternalTransfers, s.ReconcileAmbiguous))
	}
	fmt.Printf("  %sSummary:%s %s\n", Fmt.Bold, Fmt.Reset, strings.Join(parts, "; "))

	hasStripeBreakdown := s.Charges > 0 || s.Refunds > 0 || s.ChargeFees > 0 || s.StripeFees > 0 || math.Abs(s.PayoutsTotal) > 0.005
	if hasStripeBreakdown {
		net := s.ChargesGross + s.RefundsTotal - s.ChargeFees - s.StripeFees + s.PayoutsTotal
		breakdown := []string{
			fmt.Sprintf("%d charges %s", s.Charges, fmtEURSigned(s.ChargesGross)),
		}
		if s.Refunds > 0 {
			breakdown = append(breakdown, fmt.Sprintf("%d refunds %s", s.Refunds, fmtEURSigned(s.RefundsTotal)))
		}
		if s.ChargeFees > 0 {
			breakdown = append(breakdown, fmt.Sprintf("charge fees -%s", fmtEUR(s.ChargeFees)))
		}
		if s.StripeFees > 0 {
			breakdown = append(breakdown, fmt.Sprintf("Stripe fees -%s", fmtEUR(s.StripeFees)))
		}
		if math.Abs(s.PayoutsTotal) > 0.005 {
			breakdown = append(breakdown, fmt.Sprintf("payouts %s", fmtEURSigned(s.PayoutsTotal)))
		}
		breakdown = append(breakdown, "balance "+fmtEURSigned(net))
		fmt.Printf("  %sBreakdown:%s %s\n", Fmt.Dim, Fmt.Reset, strings.Join(breakdown, "; "))
	}
	if len(s.CreateFailures) > 0 {
		for _, reason := range sortedIntMapKeys(s.CreateFailures) {
			fmt.Printf("    %s✗ %s: %d%s\n", Fmt.Red, reason, s.CreateFailures[reason], Fmt.Reset)
		}
	}
	if len(s.CreateDetails) > 0 {
		limit := len(s.CreateDetails)
		if limit > 10 {
			limit = 10
		}
		for _, detail := range s.CreateDetails[:limit] {
			Warnf("    %s⚠ %s%s", Fmt.Yellow, detail, Fmt.Reset)
		}
		if len(s.CreateDetails) > limit {
			Warnf("    %s⚠ ... %d more create failure(s)%s", Fmt.Yellow, len(s.CreateDetails)-limit, Fmt.Reset)
		}
	}
	if len(s.Ambiguous) > 0 {
		for _, a := range s.Ambiguous {
			Warnf("    %s⚠ %s%s", Fmt.Yellow, a, Fmt.Reset)
		}
	}
	if s.ReconcileNoPartner > 0 || s.ReconcileNoMatch > 0 || s.ReconcileErrors > 0 {
		fmt.Printf("  %sUnreconciled:%s %d no partner, %d no match, %d errors\n",
			Fmt.Dim, Fmt.Reset, s.ReconcileNoPartner, s.ReconcileNoMatch, s.ReconcileErrors)
	}
	if len(s.ReconcileDetails) > 0 {
		for _, detail := range s.ReconcileDetails {
			Warnf("    %s⚠ %s%s", Fmt.Yellow, detail, Fmt.Reset)
		}
	}
}

// syncStripeToOdoo syncs Stripe balance transactions into Odoo, grouping
// them into bank statements bounded by automatic payouts. See
// stripe_odoo_sync.go for the detailed model.
//
// monthsLimit is accepted but ignored — the resume cursor is derived from
// the journal state. untilDate (if set) stops processing at that moment.
// payoutFilter is rejected with an error (targeted-payout resync is not
// supported in this model).
func syncStripeToOdoo(acc *AccountConfig, creds *OdooCredentials, uid int, monthsLimit int, dryRun, force bool, skipReconciliation bool, payoutFilter string, opts ...interface{}) (string, error) {
	_ = monthsLimit
	var sinceDate, untilDate time.Time
	previewLimit := 30
	stages := odooSyncStages{
		Transactions: true,
		Partners:     true,
		Accounts:     true,
		Reconcile:    true,
	}
	useHistory := false
	if len(opts) == 1 {
		untilDate, _ = opts[0].(time.Time)
	} else {
		if len(opts) > 0 {
			sinceDate, _ = opts[0].(time.Time)
		}
		if len(opts) > 1 {
			untilDate, _ = opts[1].(time.Time)
		}
		if len(opts) > 2 {
			if n, ok := opts[2].(int); ok {
				previewLimit = n
			}
		}
		if len(opts) > 3 {
			if s, ok := opts[3].(odooSyncStages); ok {
				stages = s
			}
		}
		if len(opts) > 4 {
			if h, ok := opts[4].(bool); ok {
				useHistory = h
			}
		}
	}
	return syncStripeChronological(acc, creds, uid, dryRun, force, skipReconciliation, payoutFilter, sinceDate, untilDate, previewLimit, stages, useHistory)
}

// warnInvalidStatements runs the statement invariant check and prints a warning
// block listing any violations. Non-fatal — if Odoo is unreachable, stay silent.
func warnInvalidStatements(creds *OdooCredentials, uid int, journalID int) {
	if journalID == 0 {
		return
	}
	issues, err := CheckOdooJournalStatements(creds, uid, journalID)
	if err != nil || len(issues) == 0 {
		return
	}
	PrintStatementIssues(issues)
	fmt.Printf("  %sTo fix: chb odoo journals %d fix%s\n\n", Fmt.Dim, journalID, Fmt.Reset)
}

type statementLineCreateFailure struct {
	ImportID string
	Reason   string
	Detail   string
	// ConflictJournalID is the Odoo journal that already owns this
	// unique_import_id, when the failure is a cross-journal collision.
	// Zero when the failure is something else.
	ConflictJournalID int
}

type statementLineCreateResult struct {
	IDs      []int
	Failures []statementLineCreateFailure
}

// batchCreateStatementLines creates multiple statement lines through Odoo.
// Returns the number of successfully created lines. Falls back to one-by-one
// on chunk failure.
func batchCreateStatementLines(creds *OdooCredentials, uid int, lines []map[string]interface{}) (int, error) {
	ids, err := batchCreateStatementLinesWithIDs(creds, uid, lines)
	return len(ids), err
}

func batchCreateStatementLinesWithIDs(creds *OdooCredentials, uid int, lines []map[string]interface{}) ([]int, error) {
	return batchCreateStatementLinesWithProgress(creds, uid, lines, "")
}

func batchCreateStatementLinesWithProgress(creds *OdooCredentials, uid int, lines []map[string]interface{}, reason string) ([]int, error) {
	result, err := batchCreateStatementLinesWithProgressReport(creds, uid, lines, reason)
	return result.IDs, err
}

func batchCreateStatementLinesWithProgressReport(creds *OdooCredentials, uid int, lines []map[string]interface{}, reason string) (statementLineCreateResult, error) {
	if len(lines) == 0 {
		return statementLineCreateResult{}, nil
	}

	const chunkSize = 100
	status := newStatusLine()
	if !quietOdooContext() && len(lines) > chunkSize {
		status.Update("Creating statement lines in Odoo 0/%d%s", len(lines), formatProgressReason(reason))
		defer status.Clear()
	}

	var result statementLineCreateResult
	for start := 0; start < len(lines); start += chunkSize {
		end := start + chunkSize
		if end > len(lines) {
			end = len(lines)
		}

		chunkResult := createStatementLineChunk(creds, uid, lines[start:end])
		result.IDs = append(result.IDs, chunkResult.IDs...)
		result.Failures = append(result.Failures, chunkResult.Failures...)

		if !quietOdooContext() && len(lines) > chunkSize {
			status.Update("Creating statement lines in Odoo %d/%d%s", end, len(lines), formatProgressReason(reason))
		}
	}
	annotateStatementLineCreateFailures(creds, uid, result.Failures)
	return result, nil
}

func createStatementLineChunk(creds *OdooCredentials, uid int, lines []map[string]interface{}) statementLineCreateResult {
	records := make([]interface{}, len(lines))
	for i, l := range lines {
		records[i] = l
	}

	result, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement.line", "create",
		[]interface{}{records}, nil)
	if err == nil {
		return statementLineCreateResult{IDs: parseOdooCreatedIDs(result)}
	}

	// Chunk failed (often duplicate import IDs) — fall back to one-by-one so
	// successful rows still import and failed rows can be reported precisely.
	var out statementLineCreateResult
	for _, l := range lines {
		result, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
			"account.bank.statement.line", "create",
			[]interface{}{[]interface{}{l}}, nil)
		if err == nil {
			out.IDs = append(out.IDs, parseOdooCreatedIDs(result)...)
			continue
		}
		out.Failures = append(out.Failures, classifyStatementLineCreateFailure(l, err))
	}
	return out
}

func classifyStatementLineCreateFailure(line map[string]interface{}, err error) statementLineCreateFailure {
	importID := ""
	if v, ok := line["unique_import_id"].(string); ok {
		importID = v
	}
	detail := ""
	if err != nil {
		detail = strings.TrimSpace(err.Error())
	}
	reason := "Odoo create error"
	lower := strings.ToLower(detail)
	switch {
	case strings.Contains(lower, "imported only once") ||
		strings.Contains(lower, "unique_import_id") ||
		strings.Contains(lower, "unique import") ||
		strings.Contains(lower, "already exists"):
		reason = "reference already exists in Odoo"
	case strings.Contains(lower, "access") || strings.Contains(lower, "permission"):
		reason = "Odoo permission error"
	case strings.Contains(lower, "mandatory") || strings.Contains(lower, "required"):
		reason = "missing required Odoo field"
	}
	return statementLineCreateFailure{ImportID: importID, Reason: reason, Detail: detail}
}

func annotateStatementLineCreateFailures(creds *OdooCredentials, uid int, failures []statementLineCreateFailure) {
	var ids []string
	for _, failure := range failures {
		if failure.ImportID != "" && failure.Reason == "reference already exists in Odoo" {
			ids = append(ids, failure.ImportID)
		}
	}
	if len(ids) == 0 {
		return
	}
	journals, err := fetchImportIDJournals(creds, uid, ids)
	if err != nil {
		return
	}
	for i := range failures {
		if journalID := journals[failures[i].ImportID]; journalID > 0 {
			failures[i].ConflictJournalID = journalID
			failures[i].Reason = fmt.Sprintf("reference already exists in journal #%d", journalID)
		}
	}
}

func formatProgressReason(reason string) string {
	if reason == "" {
		return ""
	}
	return " (" + reason + ")"
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

// emptyOdooJournal deletes all statement lines and statements for a journal after confirmation.
// In Odoo, each bank statement line auto-creates a journal entry (account.move).
// To delete: unreconcile → reset move to draft → delete move (which deletes the statement line).
func emptyOdooJournal(creds *OdooCredentials, uid int, journalID int, yes bool) error {
	journalName := OdooJournalName(journalID)
	if journalName == "" {
		if name, err := FetchAndCacheOdooJournalName(creds, uid, journalID); err == nil && name != "" {
			journalName = name
		} else {
			journalName = fmt.Sprintf("#%d", journalID)
		}
	}
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
		fmt.Printf("  %sJournal '%s' has no statement lines to reset%s\n", Fmt.Dim, journalName, Fmt.Reset)
	} else if !yes {
		Warnf("  %s⚠ This will delete %d statement lines from journal '%s'%s", Fmt.Yellow, count, journalName, Fmt.Reset)
		fmt.Printf("  %sType 'yes' to confirm: %s", Fmt.Bold, Fmt.Reset)
		reader := bufio.NewReader(os.Stdin)
		confirm, _ := reader.ReadString('\n')
		confirm = strings.TrimSpace(confirm)
		if confirm != "yes" {
			return fmt.Errorf("cancelled")
		}
	} else {
		Warnf("  %s⚠ Deleting %d statement lines from journal '%s'%s", Fmt.Yellow, count, journalName, Fmt.Reset)
	}

	status := newStatusLine()
	defer status.Clear()
	status.Update("Fetching %d statement lines from journal '%s'...", count, journalName)

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
		ID           int         `json:"id"`
		MoveID       interface{} `json:"move_id"`
		IsReconciled bool        `json:"is_reconciled"`
	}
	var lines []stmtLine
	json.Unmarshal(linesData, &lines)
	status.Update("Preparing %d statement lines for deletion...", len(lines))

	// Step 1: Unreconcile any reconciled move lines
	var moveIDs []int
	for _, l := range lines {
		if mid := odooFieldID(l.MoveID); mid > 0 {
			moveIDs = append(moveIDs, mid)
		}
	}

	if len(moveIDs) > 0 {
		moveIDs = uniquePositiveInts(moveIDs)
		moveIDsIface := intsToInterfaces(moveIDs)

		// Find reconciled move lines for these moves and remove reconciliation.
		status.Update("Finding reconciled move lines 0/%d...", len(moveIDs))
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
				err := runOdooIDChunks(status, "Unreconciling move lines", reconIDs, 200, func(chunk []interface{}) error {
					_, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
						"account.move.line", "remove_move_reconcile",
						[]interface{}{chunk}, nil)
					return err
				})
				if err != nil {
					status.Clear()
					Warnf("  %s⚠ Failed to unreconcile some lines: %v%s", Fmt.Yellow, err, Fmt.Reset)
				}
			}
		}

		// Step 2: Reset posted/cancelled moves to draft. Odoo rejects
		// button_draft when draft moves are present in the same call, so
		// split by current state first.
		movesToDraft, draftMoves, stateErr := partitionOdooMovesForDeletion(creds, uid, moveIDs)
		if stateErr != nil {
			status.Clear()
			Warnf("  %s⚠ Failed to read move states before deletion: %v%s", Fmt.Yellow, stateErr, Fmt.Reset)
			movesToDraft = moveIDs
			draftMoves = nil
		}
		if len(movesToDraft) > 0 {
			err = runOdooIDChunks(status, "Resetting posted moves to draft", movesToDraft, 200, func(chunk []interface{}) error {
				_, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
					"account.move", "button_draft",
					[]interface{}{chunk}, nil)
				return err
			})
			if err != nil {
				status.Clear()
				Warnf("  %s⚠ Failed to reset moves to draft: %v%s", Fmt.Yellow, err, Fmt.Reset)
			} else {
				draftMoves = uniquePositiveInts(append(draftMoves, movesToDraft...))
			}
		}
		if len(draftMoves) == 0 {
			draftMoves = moveIDs
		}

		// Step 3: Delete the moves (this cascades to delete statement lines)
		err = runOdooIDChunks(status, "Deleting statement line moves", draftMoves, 200, func(chunk []interface{}) error {
			_, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
				"account.move", "unlink",
				[]interface{}{chunk}, nil)
			return err
		})
		if err != nil {
			status.Clear()
			Warnf("  %s⚠ Failed to delete moves: %v%s", Fmt.Yellow, err, Fmt.Reset)
			// Fall back to trying to delete statement lines directly
			lineIDs := make([]int, 0, len(lines))
			for _, l := range lines {
				lineIDs = append(lineIDs, l.ID)
			}
			err2 := runOdooIDChunks(status, "Deleting statement lines", lineIDs, 200, func(chunk []interface{}) error {
				_, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
					"account.bank.statement.line", "unlink",
					[]interface{}{chunk}, nil)
				return err
			})
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
			_ = runOdooIDChunks(status, "Deleting empty statements", sids, 200, func(chunk []interface{}) error {
				_, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
					"account.bank.statement", "unlink",
					[]interface{}{chunk}, nil)
				return err
			})
		}
	}

	// Verify deletion worked
	status.Update("Verifying journal '%s' is empty...", journalName)
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

	status.Clear()
	if count == 0 {
		fmt.Printf("  %s✓ Reset journal '%s' (already empty)%s\n\n", Fmt.Green, journalName, Fmt.Reset)
	} else {
		fmt.Printf("  %s✓ Emptied journal '%s' (%d lines deleted)%s\n\n", Fmt.Green, journalName, count, Fmt.Reset)
	}
	return nil
}

func partitionOdooMovesForDeletion(creds *OdooCredentials, uid int, moveIDs []int) (toDraft []int, alreadyDraft []int, err error) {
	if len(moveIDs) == 0 {
		return nil, nil, nil
	}
	rows, err := odooReadMapsByIDs(creds, uid, "account.move", uniquePositiveInts(moveIDs), []string{"state"})
	if err != nil {
		return nil, nil, err
	}
	for _, row := range rows {
		id := odooInt(row["id"])
		switch odooString(row["state"]) {
		case "draft":
			alreadyDraft = append(alreadyDraft, id)
		default:
			toDraft = append(toDraft, id)
		}
	}
	return toDraft, alreadyDraft, nil
}

func runOdooIDChunks(status *statusLine, label string, ids []int, chunkSize int, fn func([]interface{}) error) error {
	if chunkSize <= 0 {
		chunkSize = len(ids)
	}
	total := len(ids)
	for start := 0; start < total; start += chunkSize {
		end := start + chunkSize
		if end > total {
			end = total
		}
		status.Update("%s %d/%d...", label, start, total)
		if err := fn(intsToInterfaces(ids[start:end])); err != nil {
			return err
		}
		status.Update("%s %d/%d...", label, end, total)
	}
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
	for _, key := range []string{"application", "from", "to", "orderId", "event_api_id"} {
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

func buildOdooPaymentRef(tx TransactionEntry) string {
	paymentRef := tx.Counterparty
	if tx.Type == "INTERNAL" {
		paymentRef = "Internal transfer"
		if tx.Counterparty != "" {
			paymentRef += ": " + tx.Counterparty
		}
	}
	if paymentRef == "" {
		paymentRef = txDisplayDescription(tx)
	}
	if paymentRef == "" {
		paymentRef = strings.ToLower(tx.Type)
		if tx.Currency != "" {
			paymentRef += " " + tx.Currency
		}
	}
	return paymentRef
}

func buildOdooLineSyncUpdate(acc *AccountConfig, tx TransactionEntry, row map[string]interface{}, partnerBankID, partnerID int) map[string]interface{} {
	update := map[string]interface{}{}
	if paymentRef := buildOdooPaymentRef(tx); paymentRef != "" && odooString(row["payment_ref"]) != paymentRef {
		update["payment_ref"] = paymentRef
	}
	if narr := buildOdooNarration(acc, tx); narr != "" && odooString(row["narration"]) != narr {
		update["narration"] = narr
	}
	if partnerID > 0 && odooFieldID(row["partner_id"]) != partnerID {
		update["partner_id"] = partnerID
	}
	if partnerBankID > 0 && odooFieldID(row["partner_bank_id"]) != partnerBankID {
		update["partner_bank_id"] = partnerBankID
	}
	return update
}

func buildMoneriumLineSyncUpdate(acc *AccountConfig, tx TransactionEntry, row map[string]interface{}) map[string]interface{} {
	if !transactionHasTag(tx, []string{"source", "monerium"}) && stringMetadata(tx.Metadata, "moneriumKind") == "" {
		return nil
	}
	if !isZeroAddressPaymentRef(odooString(row["payment_ref"])) {
		return nil
	}
	paymentRef := buildOdooPaymentRef(tx)
	if paymentRef == "" || strings.HasPrefix(strings.ToLower(paymentRef), "0x") {
		return nil
	}
	update := map[string]interface{}{"payment_ref": paymentRef}
	if narr := buildOdooNarration(acc, tx); narr != "" && odooString(row["narration"]) != narr {
		update["narration"] = narr
	}
	return update
}

func isZeroAddressPaymentRef(ref string) bool {
	ref = strings.ToLower(strings.TrimSpace(ref))
	return strings.Contains(ref, "0x0000...0000") || strings.Contains(ref, "0x0000000000000000000000000000000000000000")
}

// resolveOdooPartner finds or creates a partner in Odoo.
// Priority: email match → exact name match → skip if ambiguous → create new.
func resolveOdooPartner(creds *OdooCredentials, uid int, name, email, stripeCustomerID, collective string, normalizeName bool, cache map[string]int, stats ...*syncStats) int {
	var st *syncStats
	if len(stats) > 0 {
		st = stats[0]
	}
	if normalizeName {
		name = normalizeStripePartnerName(name, email)
	} else {
		name = strings.TrimSpace(name)
	}
	email = strings.TrimSpace(email)
	stripeCustomerID = strings.TrimSpace(stripeCustomerID)
	collective = normalizeTransactionTagSlug(collective)
	if name != "" {
		name = titleCaseName(name)
	}
	if name == "" && email == "" {
		return 0
	}

	// Check cache first (keyed by email if available, else name)
	cacheKey := strings.ToLower(email)
	if cacheKey == "" {
		cacheKey = name
	}
	if id, ok := cache[cacheKey]; ok {
		if id > 0 {
			_ = ensureOdooPartnerCollectiveTag(creds, uid, id, collective)
		}
		return id
	}

	lookupFields := odooPartnerLookupFields(creds, uid)

	// 1. Search by email (most reliable, email is unique-ish)
	if email != "" {
		result, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
			"res.partner", "search_read",
			[]interface{}{[]interface{}{
				[]interface{}{"email", "=", email},
			}},
			map[string]interface{}{
				"fields": lookupFields,
				"limit":  5,
				"order":  "id asc",
			})
		if err == nil {
			var partners []map[string]interface{}
			json.Unmarshal(result, &partners)
			if len(partners) == 1 {
				id := odooInt(partners[0]["id"])
				updateOdooPartnerFromStripe(creds, uid, id, partners[0], name, email, stripeCustomerID, collective)
				cache[cacheKey] = id
				if st != nil {
					st.PartnersMatched++
				}
				return id
			}
			if len(partners) > 1 {
				id := odooInt(partners[0]["id"])
				updateOdooPartnerFromStripe(creds, uid, id, partners[0], name, email, stripeCustomerID, collective)
				cache[cacheKey] = id
				if st != nil {
					st.PartnersMatched++
					st.recordPartnerMergeSuggestion(name, email, id, odooPartnerRowIDs(partners))
				}
				return id
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
				"fields": lookupFields,
				"limit":  5,
				"order":  "id asc",
			})
		if err == nil {
			var partners []map[string]interface{}
			json.Unmarshal(result, &partners)
			if len(partners) == 1 {
				id := odooInt(partners[0]["id"])
				updateOdooPartnerFromStripe(creds, uid, id, partners[0], name, email, stripeCustomerID, collective)
				cache[cacheKey] = id
				if st != nil {
					st.PartnersMatched++
				}
				return id
			}
			if len(partners) > 1 {
				id := odooInt(partners[0]["id"])
				updateOdooPartnerFromStripe(creds, uid, id, partners[0], name, email, stripeCustomerID, collective)
				cache[cacheKey] = id
				if st != nil {
					st.PartnersMatched++
					st.recordPartnerMergeSuggestion(name, email, id, odooPartnerRowIDs(partners))
				}
				return id
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
	for k, v := range odooPartnerDefaultLanguageValues(creds, uid) {
		partnerData[k] = v
	}
	for k, v := range odooPartnerStripeCustomerValues(creds, uid, nil, stripeCustomerID) {
		partnerData[k] = v
	}
	for k, v := range odooPartnerCollectiveValues(creds, uid, nil, collective) {
		partnerData[k] = v
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

func odooPartnerLookupFields(creds *OdooCredentials, uid int) []string {
	fields := []string{"id", "name", "email"}
	for _, field := range []string{"ref", "comment", "x_stripe_customer_id", "stripe_customer_id", "x_studio_stripe_customer_id"} {
		if odooModelHasField(creds, uid, "res.partner", field) {
			fields = append(fields, field)
		}
	}
	if odooModelHasField(creds, uid, "res.partner", "category_id") {
		fields = append(fields, "category_id")
	}
	return fields
}

func updateOdooPartnerFromStripe(creds *OdooCredentials, uid, partnerID int, existing map[string]interface{}, name, email, stripeCustomerID, collective string) {
	if partnerID <= 0 {
		return
	}
	values := map[string]interface{}{}
	existingName := odooString(existing["name"])
	if name != "" && existingName != name && (existingName == "" || strings.Contains(existingName, "@") || titleCaseName(existingName) == name) {
		values["name"] = name
	}
	if email != "" && odooString(existing["email"]) == "" {
		values["email"] = email
	}
	for k, v := range odooPartnerStripeCustomerValues(creds, uid, existing, stripeCustomerID) {
		values[k] = v
	}
	for k, v := range odooPartnerCollectiveValues(creds, uid, existing, collective) {
		values[k] = v
	}
	if len(values) == 0 {
		return
	}
	_, _ = odooExec(creds.URL, creds.DB, uid, creds.Password,
		"res.partner", "write",
		[]interface{}{[]interface{}{partnerID}, values}, nil)
}

func odooPartnerRowIDs(rows []map[string]interface{}) []int {
	ids := make([]int, 0, len(rows))
	seen := map[int]bool{}
	for _, row := range rows {
		id := odooInt(row["id"])
		if id <= 0 || seen[id] {
			continue
		}
		seen[id] = true
		ids = append(ids, id)
	}
	sort.Ints(ids)
	return ids
}

func odooPartnerStripeCustomerValues(creds *OdooCredentials, uid int, existing map[string]interface{}, stripeCustomerID string) map[string]interface{} {
	values := map[string]interface{}{}
	if strings.TrimSpace(stripeCustomerID) == "" || !strings.HasPrefix(stripeCustomerID, "cus_") {
		return values
	}
	for _, field := range []string{"x_stripe_customer_id", "stripe_customer_id", "x_studio_stripe_customer_id"} {
		if odooModelHasField(creds, uid, "res.partner", field) {
			if existing == nil || odooString(existing[field]) != stripeCustomerID {
				values[field] = stripeCustomerID
			}
			return values
		}
	}
	if odooModelHasField(creds, uid, "res.partner", "ref") {
		if existing == nil || strings.TrimSpace(odooString(existing["ref"])) == "" {
			values["ref"] = stripeCustomerID
		}
	}
	return values
}

func odooPartnerDefaultLanguageValues(creds *OdooCredentials, uid int) map[string]interface{} {
	values := map[string]interface{}{}
	if !odooModelHasField(creds, uid, "res.partner", "lang") {
		return values
	}
	if !odooLanguageCodeAvailable(creds, uid, "en_GB") {
		return values
	}
	values["lang"] = "en_GB"
	return values
}

func odooLanguageCodeAvailable(creds *OdooCredentials, uid int, code string) bool {
	code = strings.TrimSpace(code)
	if code == "" {
		return false
	}
	rows, err := odooSearchReadAllMaps(creds, uid, "res.lang",
		[]interface{}{
			[]interface{}{"code", "=", code},
			[]interface{}{"active", "=", true},
		},
		[]string{"id"},
		"id asc",
	)
	return err == nil && len(rows) > 0
}

func odooPartnerCollectiveValues(creds *OdooCredentials, uid int, existing map[string]interface{}, collective string) map[string]interface{} {
	values := map[string]interface{}{}
	collective = normalizeTransactionTagSlug(collective)
	if collective == "" || !odooModelHasField(creds, uid, "res.partner", "category_id") {
		return values
	}
	tagID := findOrCreateOdooPartnerCollectiveTag(creds, uid, collective)
	if tagID == 0 {
		return values
	}
	if existing != nil {
		for _, id := range odooIDList(existing["category_id"]) {
			if id == tagID {
				return values
			}
		}
	}
	values["category_id"] = []interface{}{[]interface{}{4, tagID}}
	return values
}

func findOrCreateOdooPartnerCollectiveTag(creds *OdooCredentials, uid int, collective string) int {
	collective = normalizeTransactionTagSlug(collective)
	if collective == "" {
		return 0
	}
	name := odooPartnerCollectiveTagName(collective)
	rows, err := odooSearchReadAllMaps(creds, uid, "res.partner.category",
		[]interface{}{[]interface{}{"name", "=", name}},
		[]string{"id", "name"},
		"id asc",
	)
	if err == nil && len(rows) > 0 {
		return odooInt(rows[0]["id"])
	}
	legacyName := "Collective: " + collective
	rows, err = odooSearchReadAllMaps(creds, uid, "res.partner.category",
		[]interface{}{[]interface{}{"name", "=", legacyName}},
		[]string{"id", "name"},
		"id asc",
	)
	if err == nil && len(rows) > 0 {
		id := odooInt(rows[0]["id"])
		if id > 0 {
			_, _ = odooExec(creds.URL, creds.DB, uid, creds.Password,
				"res.partner.category", "write",
				[]interface{}{[]interface{}{id}, map[string]interface{}{"name": name}}, nil)
			return id
		}
	}
	result, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"res.partner.category", "create",
		[]interface{}{[]interface{}{map[string]interface{}{"name": name}}}, nil)
	if err != nil {
		Warnf("  %s⚠ Could not create Odoo partner tag %q: %v%s", Fmt.Yellow, name, err, Fmt.Reset)
		return 0
	}
	ids := parseOdooCreatedIDs(result)
	if len(ids) == 0 {
		return 0
	}
	return ids[0]
}

func odooPartnerCollectiveTagName(collective string) string {
	collective = normalizeTransactionTagSlug(collective)
	if collective == "" {
		return ""
	}
	return "collective:" + collective
}

func ensureOdooPartnerCollectiveTag(creds *OdooCredentials, uid, partnerID int, collective string) error {
	if partnerID <= 0 {
		return nil
	}
	values := odooPartnerCollectiveValues(creds, uid, nil, collective)
	if len(values) == 0 {
		return nil
	}
	_, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"res.partner", "write",
		[]interface{}{[]interface{}{partnerID}, values}, nil)
	return err
}

func odooModelHasField(creds *OdooCredentials, uid int, model, field string) bool {
	fields, err := odooAvailableFields(creds, uid, model)
	return err == nil && fields[field]
}

func normalizeStripePartnerName(name, email string) string {
	name = strings.TrimSpace(name)
	if strings.Contains(name, "@") {
		name = strings.TrimSpace(strings.SplitN(name, "@", 2)[0])
	}
	if name == "" && strings.Contains(email, "@") {
		name = strings.TrimSpace(strings.SplitN(email, "@", 2)[0])
	}
	name = strings.Join(strings.Fields(name), " ")
	if name == "" {
		return ""
	}
	return titleCaseName(name)
}

func titleCaseName(s string) string {
	words := strings.Fields(strings.ToLower(s))
	for i, word := range words {
		runes := []rune(word)
		if len(runes) == 0 {
			continue
		}
		runes[0] = unicode.ToTitle(runes[0])
		for j := 1; j < len(runes); j++ {
			prev := runes[j-1]
			if prev == '-' || prev == '\'' {
				runes[j] = unicode.ToTitle(runes[j])
			}
		}
		words[i] = string(runes)
	}
	return strings.Join(words, " ")
}

// loadAccountTransactions loads all non-INTERNAL transactions for a specific account.
func loadAccountTransactions(acc *AccountConfig) []TransactionEntry {
	return loadAccountTransactionsWithOptions(acc, false)
}

func loadAccountTransactionsForOdoo(acc *AccountConfig) []TransactionEntry {
	return loadAccountTransactionsWithOptions(acc, true)
}

func loadAccountTransactionsWithOptions(acc *AccountConfig, includeInternal bool) []TransactionEntry {
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
				if tx.Type == "INTERNAL" && !includeInternal {
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

func signedOdooAmountForTransaction(acc *AccountConfig, tx TransactionEntry) float64 {
	amt := tx.Amount
	if tx.NormalizedAmount != 0 {
		amt = tx.NormalizedAmount
	}
	if tx.Type == "INTERNAL" {
		if internalTransactionDirection(acc, tx) == "DEBIT" {
			return -math.Abs(amt)
		}
		return math.Abs(amt)
	}
	if tx.IsOutgoing() {
		return -math.Abs(amt)
	}
	return math.Abs(amt)
}

func internalTransactionDirection(acc *AccountConfig, tx TransactionEntry) string {
	if tx.Metadata != nil {
		for _, key := range []string{"direction", "originalType", "original_type"} {
			if value, ok := tx.Metadata[key].(string); ok {
				switch strings.ToUpper(value) {
				case "DEBIT", "OUT", "OUTGOING":
					return "DEBIT"
				case "CREDIT", "IN", "INCOMING":
					return "CREDIT"
				}
			}
		}
	}
	if direction := internalTransactionDirectionFromRaw(acc, tx); direction != "" {
		return direction
	}
	return "CREDIT"
}

func internalTransactionDirectionFromRaw(acc *AccountConfig, tx TransactionEntry) string {
	if tx.Timestamp == 0 || tx.TxHash == "" {
		return ""
	}
	account := tx.Account
	if account == "" && acc != nil {
		account = acc.Address
	}
	if account == "" {
		return ""
	}
	chain := tx.Provider
	if tx.Chain != nil && *tx.Chain != "" {
		chain = *tx.Chain
	}
	if chain == "" && acc != nil {
		chain = acc.Chain
	}
	t := time.Unix(tx.Timestamp, 0).In(BrusselsTZ())
	slug := tx.AccountSlug
	if slug == "" && acc != nil {
		slug = acc.Slug
	}
	currency := tx.Currency
	if currency == "" && acc != nil && acc.Token != nil {
		currency = acc.Token.Symbol
	}
	if chain == "" || slug == "" || currency == "" {
		return ""
	}
	path := etherscansource.Path(DataDir(), t.Format("2006"), t.Format("01"), chain, etherscansource.FileName(slug, currency))
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	var txFile struct {
		Transactions []struct {
			Hash string `json:"hash"`
			From string `json:"from"`
			To   string `json:"to"`
		} `json:"transactions"`
	}
	if json.Unmarshal(data, &txFile) != nil {
		return ""
	}
	for _, raw := range txFile.Transactions {
		if !strings.EqualFold(raw.Hash, tx.TxHash) {
			continue
		}
		if strings.EqualFold(raw.From, account) {
			return "DEBIT"
		}
		if strings.EqualFold(raw.To, account) {
			return "CREDIT"
		}
		return ""
	}
	return ""
}

// buildUniqueImportID creates the dedup key for Odoo.
// Blockchain format (matching odoo-web3): {chain}:{walletAddress}:{txHash}:{logIndex}
// Stripe format:                          stripe:{accountId}:{txn_id}
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
		return fmt.Sprintf("stripe:%s:%s", strings.ToLower(accountID), strings.ToLower(txnID))
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
	return fmt.Sprintf("%s:%s:%s:%d", chain, strings.ToLower(address), strings.ToLower(txHash), tx.LogIndex)
}

// reportLocalImportIDCollisions explains why the dedup pass reported "0
// missing" even though local has more txs than Odoo. Two scenarios:
//
//   - Local collision: two local txs build the same unique_import_id and
//     therefore share a single Odoo line.
//   - Cross-journal collision: the local import_id exists in Odoo, but on
//     a different journal — the sync's per-journal existingIDs lookup
//     misses it, and Odoo's global uniqueness constraint blocks insertion.
//
// Both are listed so the operator can see exactly which rows are off.
func reportLocalImportIDCollisions(creds *OdooCredentials, uid int, acc *AccountConfig, localByImportID map[string][]TransactionEntry, existingIDs map[string]bool) {
	type collision struct {
		ImportID string
		Txs      []TransactionEntry
	}
	var collisions []collision
	uniqueLocal := 0
	for id, txs := range localByImportID {
		uniqueLocal++
		if len(txs) > 1 {
			collisions = append(collisions, collision{ImportID: id, Txs: txs})
		}
	}

	// Cross-journal check: for every local import_id, find the journal(s)
	// that already hold a line under it. Anything not on acc.OdooJournalID
	// is a cross-journal collision and explains drift the per-journal
	// existingIDs lookup can't see.
	localIDs := make([]string, 0, len(localByImportID))
	for id := range localByImportID {
		localIDs = append(localIDs, id)
	}
	odooLineLocations, locErr := fetchImportIDJournals(creds, uid, localIDs)
	type crossJournal struct {
		ImportID  string
		JournalID int
		Tx        TransactionEntry
	}
	var crossJournals []crossJournal
	if locErr == nil {
		for id, journalID := range odooLineLocations {
			if journalID == acc.OdooJournalID {
				continue
			}
			for _, tx := range localByImportID[id] {
				crossJournals = append(crossJournals, crossJournal{ImportID: id, JournalID: journalID, Tx: tx})
			}
		}
	}

	if len(collisions) == 0 && len(crossJournals) == 0 {
		Warnf("  %s⚠ Drift unexplained: %d unique local import_ids all match Odoo journal #%d, but the journal has %d lines.%s",
			Fmt.Yellow, uniqueLocal, acc.OdooJournalID, len(existingIDs), Fmt.Reset)
		Warnf("  %s  Some Odoo lines may have no local counterpart. Run: chb odoo journals %d (detail view lists missing-on-Odoo / missing-locally).%s",
			Fmt.Yellow, acc.OdooJournalID, Fmt.Reset)
		return
	}

	if len(crossJournals) > 0 {
		// Cache journal names we don't already know about.
		journalsSeen := map[int]bool{}
		for _, c := range crossJournals {
			if !journalsSeen[c.JournalID] {
				journalsSeen[c.JournalID] = true
				if OdooJournalName(c.JournalID) == "" {
					_, _ = FetchAndCacheOdooJournalName(creds, uid, c.JournalID)
				}
			}
		}
		sort.Slice(crossJournals, func(i, j int) bool {
			if crossJournals[i].JournalID != crossJournals[j].JournalID {
				return crossJournals[i].JournalID < crossJournals[j].JournalID
			}
			return crossJournals[i].ImportID < crossJournals[j].ImportID
		})
		Warnf("  %s⚠ %s of journal #%d live on a different Odoo journal — unique_import_id is globally unique, so these can't be inserted on #%d until the wrong-journal line is moved or deleted.%s",
			Fmt.Yellow, Pluralize(len(crossJournals), "local tx", ""), acc.OdooJournalID, acc.OdooJournalID, Fmt.Reset)
		for _, c := range crossJournals {
			t := time.Unix(c.Tx.Timestamp, 0).In(BrusselsTZ())
			amt := signedOdooAmountForTransaction(acc, c.Tx)
			name := OdooJournalName(c.JournalID)
			if name != "" {
				name = " (" + name + ")"
			}
			fmt.Printf("    %s%s  %12s  logIndex=%d  %s%s\n      %sin journal #%d%s  import_id=%s%s\n",
				Fmt.Dim, t.Format("2006-01-02 15:04"), fmtEURSigned(amt), c.Tx.LogIndex, c.Tx.TxHash, Fmt.Reset,
				Fmt.Dim, c.JournalID, name, c.ImportID, Fmt.Reset)
		}
		fmt.Println()
	} else if locErr != nil {
		Warnf("  %s⚠ Could not check cross-journal collisions: %v%s", Fmt.Yellow, locErr, Fmt.Reset)
	}

	if len(collisions) > 0 {
		sort.Slice(collisions, func(i, j int) bool { return collisions[i].ImportID < collisions[j].ImportID })
		Warnf("  %s⚠ %s share an import_id with another local tx — Odoo can only hold one line per import_id, so the extras are silently treated as duplicates.%s",
			Fmt.Yellow, Pluralize(len(collisions), "local tx group", ""), Fmt.Reset)
		Warnf("  %s  Likely cause: LogIndex collision on the local side (two transfers in the same tx with logIndex=0, or the same tx loaded twice from different provider files).%s",
			Fmt.Yellow, Fmt.Reset)
		for _, c := range collisions {
			fmt.Printf("    %simport_id=%s  (%d local txs):%s\n", Fmt.Dim, c.ImportID, len(c.Txs), Fmt.Reset)
			for _, tx := range c.Txs {
				t := time.Unix(tx.Timestamp, 0).In(BrusselsTZ())
				amt := signedOdooAmountForTransaction(acc, tx)
				fmt.Printf("      %s  %12s  logIndex=%d  %s  cp=%s\n",
					t.Format("2006-01-02 15:04"), fmtEURSigned(amt), tx.LogIndex, tx.TxHash, tx.Counterparty)
			}
		}
		fmt.Println()
	}
}

// fetchImportIDJournals returns importID → journalID for any Odoo
// account.bank.statement.line whose unique_import_id matches one of the
// supplied IDs, across all journals. Used to surface cross-journal
// collisions during sync.
func fetchImportIDJournals(creds *OdooCredentials, uid int, importIDs []string) (map[string]int, error) {
	out := map[string]int{}
	if len(importIDs) == 0 {
		return out, nil
	}
	const batch = 200
	for i := 0; i < len(importIDs); i += batch {
		end := i + batch
		if end > len(importIDs) {
			end = len(importIDs)
		}
		ids := make([]interface{}, len(importIDs[i:end]))
		for j, s := range importIDs[i:end] {
			ids[j] = s
		}
		data, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
			"account.bank.statement.line", "search_read",
			[]interface{}{[]interface{}{
				[]interface{}{"unique_import_id", "in", ids},
			}},
			map[string]interface{}{
				"fields": []string{"unique_import_id", "journal_id"},
				"limit":  0,
			})
		if err != nil {
			return out, err
		}
		var lines []struct {
			UniqueImportID interface{} `json:"unique_import_id"`
			JournalID      interface{} `json:"journal_id"`
		}
		if err := json.Unmarshal(data, &lines); err != nil {
			return out, err
		}
		for _, line := range lines {
			id, _ := line.UniqueImportID.(string)
			if id == "" {
				continue
			}
			// Odoo returns many2one as [id, "Name"] tuples.
			tuple, ok := line.JournalID.([]interface{})
			if !ok || len(tuple) == 0 {
				continue
			}
			jid, ok := tuple[0].(float64)
			if !ok {
				continue
			}
			out[id] = int(jid)
		}
	}
	return out, nil
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

func fetchLatestOdooImportCursor(creds *OdooCredentials, uid int, journalID int) (odooImportCursor, error) {
	return fetchLatestOdooImportCursorFiltered(creds, uid, journalID, nil)
}

func fetchLatestStripeOdooImportCursor(creds *OdooCredentials, uid int, journalID int, accountID string) (odooImportCursor, error) {
	return fetchLatestOdooImportCursorFiltered(creds, uid, journalID, func(importID string) bool {
		return stripeOpenStatementFeeImportID(accountID, importID)
	})
}

func fetchLatestOdooImportCursorFiltered(creds *OdooCredentials, uid int, journalID int, skip func(string) bool) (odooImportCursor, error) {
	data, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement.line", "search_read",
		[]interface{}{[]interface{}{
			[]interface{}{"journal_id", "=", journalID},
			[]interface{}{"unique_import_id", "!=", false},
		}},
		map[string]interface{}{
			"fields": []string{"date", "unique_import_id"},
			"order":  "date desc, id desc",
			"limit":  100,
		})
	if err != nil {
		return odooImportCursor{}, err
	}
	var lines []struct {
		Date           string      `json:"date"`
		UniqueImportID interface{} `json:"unique_import_id"`
	}
	if err := json.Unmarshal(data, &lines); err != nil {
		return odooImportCursor{}, err
	}
	if len(lines) == 0 {
		return odooImportCursor{}, nil
	}
	for _, line := range lines {
		importID, _ := line.UniqueImportID.(string)
		if importID == "" || (skip != nil && skip(importID)) {
			continue
		}
		return odooImportCursor{Found: true, UniqueImportID: importID, Date: line.Date}, nil
	}
	return odooImportCursor{}, nil
}

func stripeOpenStatementFeeImportID(accountID, importID string) bool {
	accountID = strings.ToLower(strings.TrimSpace(accountID))
	importID = strings.ToLower(strings.TrimSpace(importID))
	if accountID == "" || importID == "" {
		return false
	}
	prefix := "stripe:" + accountID + ":open:"
	if !strings.HasPrefix(importID, prefix) || !strings.HasSuffix(importID, ":fees") {
		return false
	}
	statementID := strings.TrimSuffix(strings.TrimPrefix(importID, prefix), ":fees")
	if statementID == "" {
		return false
	}
	for _, r := range statementID {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func fetchOdooImportIDsForTransactions(creds *OdooCredentials, uid int, journalID int, acc *AccountConfig, txs []TransactionEntry) (map[string]bool, error) {
	result := map[string]bool{}
	values := make([]string, 0, len(txs))
	seen := map[string]bool{}
	for _, tx := range txs {
		id := buildUniqueImportID(acc, tx)
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		values = append(values, id)
	}
	if len(values) == 0 {
		return result, nil
	}
	rows, err := odooSearchReadAllMaps(creds, uid, "account.bank.statement.line",
		[]interface{}{
			[]interface{}{"journal_id", "=", journalID},
			[]interface{}{"unique_import_id", "in", values},
		},
		[]string{"unique_import_id"},
		"id asc",
	)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		if id := odooString(row["unique_import_id"]); id != "" {
			result[id] = true
		}
	}
	return result, nil
}

// AccountStripePayouts lists Stripe payouts derived from archived Stripe provider data.
func AccountStripePayouts(slug string, args []string) error {
	monthsLimit := 0
	for i, a := range args {
		if a == "--months" && i+1 < len(args) {
			fmt.Sscanf(args[i+1], "%d", &monthsLimit)
		}
	}

	if HasFlag(args, "--refresh") {
		fmt.Printf("\n  %sRebuilding payouts from archived Stripe provider data...%s\n", Fmt.Dim, Fmt.Reset)
		payouts, err := stripesource.RebuildPayoutsCacheFromTransactions(DataDir())
		if err != nil {
			return fmt.Errorf("failed to rebuild payouts cache: %v", err)
		}
		fmt.Printf("  %s%s cached from provider archives%s\n", Fmt.Dim, Pluralize(len(payouts), "payout", ""), Fmt.Reset)
	}

	cache := stripesource.LoadPayoutsCache(DataDir())
	if cache == nil || len(cache.Payouts) == 0 {
		fmt.Printf("\n  %sNo cached payouts. Run 'chb transactions sync --source stripe --reset', then 'chb accounts %s payouts --refresh'.%s\n\n", Fmt.Dim, slug, Fmt.Reset)
		return nil
	}

	payouts := stripesource.FilterPayoutsByMonths(cache.Payouts, monthsLimit, time.Now())
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
			fmt.Printf("    %sStatement:%s    %s\n", labelStyle, Fmt.Reset, po.StatementName(BrusselsTZ()))
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
		Errorf("%sAccount '%s' not found%s", f.Red, slug, f.Reset)
		return
	}

	// Show available commands
	fmt.Printf("%sCOMMANDS%s\n\n", f.Bold, f.Reset)

	if acc.Provider == "stripe" {
		fmt.Printf("  %s%schb accounts %s payouts%s\n", f.Bold, f.Cyan, slug, f.Reset)
		fmt.Printf("    %sList Stripe payouts with amounts and transaction counts%s\n\n", f.Dim, f.Reset)
	}

	fmt.Printf("  %s%schb accounts %s sync%s\n", f.Bold, f.Cyan, slug, f.Reset)
	if acc.Provider == "stripe" {
		fmt.Printf("    %sSync Stripe balance transactions into the linked Odoo journal.%s\n", f.Dim, f.Reset)
		fmt.Printf("    %sStatements are opened/closed automatically around auto-payouts.%s\n", f.Dim, f.Reset)
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
	fmt.Printf("  %s--skip-reconciliation%s  Import lines without matching them to invoices/moves\n", f.Yellow, f.Reset)
	fmt.Printf("  %s--transactions%s     Stripe-only: import statement lines/statements/fees\n", f.Yellow, f.Reset)
	fmt.Printf("  %s--partners%s         Stripe-only: link/create partners and collective tags\n", f.Yellow, f.Reset)
	fmt.Printf("  %s--accounts%s         Stripe-only: apply account rules to journal lines\n", f.Yellow, f.Reset)
	fmt.Printf("  %s--metadata%s         Stripe-only: refresh descriptions and narration metadata\n", f.Yellow, f.Reset)
	fmt.Printf("  %s--reconcile%s        Stripe-only: reconcile journal lines\n", f.Yellow, f.Reset)
	fmt.Printf("  %s--until YYYYMMDD%s   Stop processing at this date\n", f.Yellow, f.Reset)
	fmt.Println()

	// Show examples
	fmt.Printf("%sEXAMPLES%s\n\n", f.Bold, f.Reset)
	if acc.Provider == "stripe" {
		fmt.Printf("  %s$ chb accounts %s payouts%s\n", f.Dim, slug, f.Reset)
		fmt.Printf("  %s$ chb accounts %s sync --dry-run%s\n", f.Dim, slug, f.Reset)
		fmt.Printf("  %s$ chb accounts %s sync --force%s\n", f.Dim, slug, f.Reset)
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
  %schb accounts <slug> sync --until YYYYMMDD%s   Stop processing at this date
  %schb accounts <slug> sync --force%s      Re-sync (delete + recreate)
  %schb accounts <slug> balance [YYYY[/MM[/DD]]]%s   Historical balance at end of period
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
