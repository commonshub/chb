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
	"sync"
	"time"
	"unicode"

	etherscansource "github.com/CommonsHub/chb/providers/etherscan"
	kbcbrusselssource "github.com/CommonsHub/chb/providers/kbcbrussels"
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
	// CursorMatched is true when the push exited via the local cursor
	// short-circuit (no Odoo RPCs at all). Callers can use this to
	// skip a follow-up auto-reconcile pass that has nothing to do.
	CursorMatched bool
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
	// Some RPC nodes return "" or "0x" for a zero balanceOf result. That's a
	// valid zero balance (e.g. a drained or never-funded wallet), not an error —
	// treating it as an error would silently drop the account from balances.json.
	if result.Result == "" || result.Result == "0x" {
		return 0, nil
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
		return "https://polygon-bor-rpc.publicnode.com"
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
// accountBalanceKey is the canonical balances-cache key for an account. For
// etherscan wallets it is chain-qualified ("<chain>:<address>") because the
// same EOA address can exist on several chains (e.g. eoa on gnosis and
// eoa-polygon on polygon both use 0xf5d0…); an address-only key makes them
// overwrite each other's live balance.
func accountBalanceKey(acc *AccountConfig) string {
	if acc == nil {
		return ""
	}
	switch acc.Provider {
	case "etherscan":
		if acc.Address != "" {
			return strings.ToLower(acc.Chain + ":" + acc.Address)
		}
	case "stripe":
		if acc.AccountID != "" {
			return strings.ToLower(acc.AccountID)
		}
		return "stripe"
	case "kbcbrussels":
		if acc.IBAN != "" {
			return strings.ToLower(acc.IBAN)
		}
	}
	return ""
}

// accountBalanceLookupKeys lists the cache keys to try when reading a live
// balance, canonical (chain-qualified) first. For etherscan the bare address is
// deliberately NOT a fallback — it would re-introduce the cross-chain collision
// (a re-pull writes the canonical key).
func accountBalanceLookupKeys(acc *AccountConfig) []string {
	if acc == nil {
		return nil
	}
	if acc.Provider == "etherscan" {
		var keys []string
		if k := accountBalanceKey(acc); k != "" {
			keys = append(keys, k)
		}
		// Fall back to the legacy bare-address key only when no other account
		// uses the same address on a different chain — otherwise the fallback
		// would re-introduce the cross-chain collision.
		if acc.Address != "" && !addressSharedAcrossChains(acc.Address) {
			keys = append(keys, strings.ToLower(acc.Address))
		}
		return keys
	}
	var keys []string
	for _, k := range []string{acc.AccountID, acc.IBAN, acc.Slug, acc.Address} {
		if k != "" {
			keys = append(keys, strings.ToLower(k))
		}
	}
	return keys
}

// addressSharedAcrossChains reports whether more than one configured etherscan
// account uses the given wallet address on different chains (e.g. eoa on gnosis
// and eoa-polygon on polygon). Such addresses must never fall back to the
// chain-agnostic balance-cache key.
func addressSharedAcrossChains(addr string) bool {
	a := strings.ToLower(strings.TrimSpace(addr))
	if a == "" {
		return false
	}
	chains := map[string]bool{}
	for _, c := range LoadAccountConfigs() {
		if c.Provider == "etherscan" && strings.ToLower(c.Address) == a {
			chains[strings.ToLower(c.Chain)] = true
		}
	}
	return len(chains) > 1
}

func refreshAccountBalance(acc *AccountConfig) (float64, string, error) {
	if acc.Provider == "etherscan" && acc.Address != "" && acc.Token != nil {
		// Note: PriorTokens are NOT summed here. Monerium's V2 upgrade keeps
		// the old and new EURe contract addresses reporting the SAME balanceOf
		// for a wallet, so adding them would double-count. The primary Token
		// already reflects the true current balance; PriorTokens exist only to
		// pull historical transfer events the new contract doesn't carry.
		v, err := fetchTokenBalance(acc.ChainID, acc.Token.Address, acc.Address, acc.Token.Decimals)
		if err != nil {
			return 0, "", err
		}
		return v, accountBalanceKey(acc), nil
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
	if acc.Provider == "kbcbrussels" && acc.IBAN != "" {
		v, _, ok := kbcLatestBalance(acc.IBAN)
		if !ok {
			return 0, "", fmt.Errorf("no CSV export found for %s under %s", acc.IBAN, kbcbrusselssource.LatestDir(DataDir()))
		}
		return v, strings.ToLower(acc.IBAN), nil
	}
	return 0, "", nil
}

// kbcLatestBalance returns the running balance + timestamp of the most
// recent row in the local CSV for the given IBAN. The CSV's running
// balance is the bank's authoritative figure — it always matches the
// account's actual balance at that row's date.
func kbcLatestBalance(iban string) (balance float64, ts int64, ok bool) {
	rows, err := kbcbrusselssource.LoadTransactionsForIBAN(DataDir(), iban)
	if err != nil || len(rows) == 0 {
		return 0, 0, false
	}
	latest := rows[0]
	for _, r := range rows[1:] {
		if r.Timestamp > latest.Timestamp ||
			(r.Timestamp == latest.Timestamp && r.Hash > latest.Hash) {
			latest = r
		}
	}
	return latest.Balance, latest.Timestamp, true
}

// fetchLiveBalances fetches on-chain + Stripe balances for all accounts and caches them.
func fetchLiveBalances(configs []AccountConfig) map[string]float64 {
	balances := map[string]float64{}

	for _, acc := range configs {
		if acc.Provider == "etherscan" && acc.Address != "" && acc.Token != nil {
			// PriorTokens are intentionally not summed — see refreshAccountBalance.
			balance, err := fetchTokenBalance(acc.ChainID, acc.Token.Address, acc.Address, acc.Token.Decimals)
			if err == nil {
				balances[accountBalanceKey(&acc)] = balance
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
		} else if acc.Provider == "kbcbrussels" && acc.IBAN != "" {
			if v, _, ok := kbcLatestBalance(acc.IBAN); ok {
				balances[strings.ToLower(acc.IBAN)] = v
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
	// `chb accounts balance [YYYY[/MM[/DD]]]` → aggregate balance of every
	// account at the end of the period, with a per-currency total. Checked
	// before the top-level --help so `chb accounts balance --help` reaches
	// the balance-specific help rather than the general accounts help.
	if len(args) >= 1 && args[0] == "balance" {
		if err := AccountsBalance(args[1:]); err != nil {
			Fatalf("%sError:%s %v", Fmt.Red, Fmt.Reset, err)
		}
		return
	}

	// `chb accounts internal` → audit internal-transfer legs across all local
	// accounts; they must net to zero. Checked before the slug loop so it isn't
	// mistaken for an account named "internal".
	if len(args) >= 1 && args[0] == "internal" {
		if err := AccountsInternal(args[1:]); err != nil {
			Fatalf("%sError:%s %v", Fmt.Red, Fmt.Reset, err)
		}
		return
	}

	if HasFlag(args, "--help", "-h", "help") {
		printAccountsHelp()
		return
	}

	// `chb accounts pull` (alias: deprecated `sync`) → fetch source → local,
	// for all accounts. Same verb contract as `chb pull` and `chb odoo pull`.
	if len(args) >= 1 && (args[0] == "pull" || args[0] == "sync") {
		if args[0] == "sync" {
			Warnf("%s'chb accounts sync' is deprecated — use 'chb accounts pull' instead%s", Fmt.Dim, Fmt.Reset)
		}
		if _, err := AccountsFetchAll(args[1:]); err != nil {
			Fatalf("%sError:%s %v", Fmt.Red, Fmt.Reset, err)
		}
		return
	}

	// `chb accounts push` (no slug) → push every pushable account to Odoo.
	if len(args) >= 1 && args[0] == "push" {
		if err := AccountsPushAll(args[1:]); err != nil {
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
			case "pull", "sync":
				// Pure source→local fetch for one account. Use `chb odoo
				// journals <id> push` to push into Odoo afterward.
				if action == "sync" {
					Warnf("%s'chb accounts %s sync' is deprecated — use 'chb accounts %s pull' instead%s", Fmt.Dim, slug, slug, Fmt.Reset)
				}
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
				wantsFullExport := HasFlag(args[2:], "--csv") || HasFlag(args[2:], "--json", "--jsonl")
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

	// The full command/options/examples reference is opt-in via --help; the
	// bare `chb accounts <slug>` stays a clean, instant status glance.
	if HasFlag(args, "--help", "-h", "help") {
		printAccountSlugHelp(slug)
		return
	}

	if JSONMode(args) {
		emitAccountDetailJSON(acc, args)
		return
	}

	printAccountDetailSummary(acc, args)
	fmt.Printf("\n  %sRun `chb accounts %s --help` for commands · `-r` to refresh live balances.%s\n\n",
		Fmt.Dim, slug, Fmt.Reset)
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
	currency := accCurrency(acc)

	// On --refresh, hit the live source for this one account and update just
	// this account's entry in the shared balance cache. Without --refresh the
	// whole summary is built from local files only (instant — no network).
	if refresh {
		fmt.Printf("\n  %sRefreshing %s balance for %s…%s\n", Fmt.Dim, accountLiveBalanceLabel(acc), acc.Slug, Fmt.Reset)
		refreshAndPersistAccountBalance(acc)
	}

	// Live balance: the provider's own figure (Stripe API / on-chain), as
	// last cached locally. The tx count is how many raw source rows back it.
	var liveBalance float64
	hasLive := false
	var liveFetchedAt string
	if cache := loadBalanceCache(); cache != nil {
		for _, key := range accountBalanceLookupKeys(acc) {
			if v, ok := cache.Balances[strings.ToLower(key)]; ok {
				liveBalance = v
				hasLive = true
				liveFetchedAt = cache.FetchedAt
				break
			}
		}
	}

	fmt.Println()
	printAccountField("  ", "Account", accountDisplayName(acc))
	printAccountOnlineLink(acc, "  ")
	fmt.Println()

	// Three balances, each with its own transaction count + provenance, so the
	// operator can see at a glance where they disagree.
	printAccountField("  ", "Live balance", liveBalanceDetail(acc, liveBalance, hasLive, currency, liveFetchedAt))

	localBalance, localCount, localFirst, localLast := accountLocalBalance(totals, s)
	printAccountField("  ", "Local balance", localBalanceDetail(localBalance, localCount, localFirst, localLast, currency))

	if acc.OdooJournalID > 0 {
		printAccountField("  ", "Journal balance", journalBalanceDetail(acc, currency, refresh))
	}
	if acc.OdooGlAccountCode != "" {
		printAccountField("  ", "GL balance", glBalanceDetail(acc))
	}

	// Last sync + last full sync on one line.
	fmt.Println()
	source := "account:" + strings.ToLower(acc.Slug)
	lastSync := LastSyncTime(source)
	lastFull := LastFullSyncTime(source)
	syncVal := "never"
	if !lastSync.IsZero() {
		syncVal = lastSync.In(BrusselsTZ()).Format("2006-01-02 15:04")
	}
	fullVal := Fmt.Yellow + "never" + Fmt.Reset
	if !lastFull.IsZero() {
		fullVal = lastFull.In(BrusselsTZ()).Format("2006-01-02 15:04")
	}
	fmt.Printf("  %sLast sync:%s %s %s·%s last full: %s\n", Fmt.Dim, Fmt.Reset, syncVal, Fmt.Dim, Fmt.Reset, fullVal)

	if lastFull.IsZero() {
		fmt.Printf("\n  %sThis account has never been fully synced yet, please run `chb accounts %s sync --history`%s\n",
			Fmt.Yellow, acc.Slug, Fmt.Reset)
	}
	if totals != nil {
		printAccountBalanceMismatch(acc, totals.CurrentBalance, totals.Currency, hasLive, liveBalance, accountBalanceSourceLabel(acc.Provider, liveFetchedAt))
	}
}

// accountLocalBalance returns the balance, tx count and date range derived from
// the locally-generated transactions (preferring the richer totals view, then
// the lighter summary). Used for the "Local balance" detail row.
func accountLocalBalance(totals *accountTotals, s *accountSummary) (balance float64, count int, first, last time.Time) {
	if totals != nil {
		return totals.CurrentBalance, totals.TxCount, totals.FirstTxAt, totals.LastTxAt
	}
	if s != nil {
		return s.Balance, s.TxCount, time.Time{}, s.LastTxAt
	}
	return 0, 0, time.Time{}, time.Time{}
}

// liveBalanceDetail renders the value side of the "Live balance" row, e.g.
// "125.50 EURe  (2 txs · on-chain, cached 2026-05-07 10:00)".
func liveBalanceDetail(acc *AccountConfig, balance float64, has bool, currency, fetchedAt string) string {
	if !has {
		if acc.Provider == "kbcbrussels" {
			return Fmt.Dim + "n/a (KBC has no live API)" + Fmt.Reset
		}
		return Fmt.Dim + "not cached — run with -r" + Fmt.Reset
	}
	parts := []string{}
	if n, ok := liveSourceTxCount(acc); ok {
		parts = append(parts, Pluralize(n, "tx", ""))
	}
	prov := liveBalanceSourceShort(acc)
	if fetchedAt != "" {
		if t, err := time.Parse(time.RFC3339, fetchedAt); err == nil {
			prov += ", cached " + t.In(BrusselsTZ()).Format("2006-01-02 15:04")
		}
	}
	parts = append(parts, prov)
	return fmt.Sprintf("%s  %s(%s)%s", formatAccountDataBalance(balance, currency), Fmt.Dim, strings.Join(parts, " · "), Fmt.Reset)
}

// localBalanceDetail renders the value side of the "Local balance" row.
func localBalanceDetail(balance float64, count int, first, last time.Time, currency string) string {
	if count == 0 {
		return Fmt.Dim + "no local transactions — run `chb accounts <slug> pull`" + Fmt.Reset
	}
	detail := Pluralize(count, "tx", "")
	if !first.IsZero() && !last.IsZero() {
		detail += fmt.Sprintf(" · %s → %s", first.In(BrusselsTZ()).Format("2006-01-02"), last.In(BrusselsTZ()).Format("2006-01-02"))
	} else if !last.IsZero() {
		detail += " · latest " + last.In(BrusselsTZ()).Format("2006-01-02")
	}
	return fmt.Sprintf("%s  %s(%s)%s", formatAccountDataBalance(balance, currency), Fmt.Dim, detail, Fmt.Reset)
}

// journalBalanceDetail renders the value side of the "Journal balance" row from
// the linked Odoo journal. It reads the local journal-lines cache by default
// (instant); --refresh pulls the live figures from Odoo.
func journalBalanceDetail(acc *AccountConfig, currency string, refresh bool) string {
	name := OdooJournalName(acc.OdooJournalID)
	snap, ok := accountJournalSnapshot(acc, currency, refresh)
	label := fmt.Sprintf("journal #%d", acc.OdooJournalID)
	if name != "" {
		label = fmt.Sprintf("journal #%d %s", acc.OdooJournalID, name)
	}
	if !ok {
		return Fmt.Dim + label + " — no local cache, run `chb accounts " + acc.Slug + " push` or -r" + Fmt.Reset
	}
	return fmt.Sprintf("%s  %s(%s · %s)%s",
		formatAccountDataBalance(snap.Balance, snap.Currency), Fmt.Dim, Pluralize(snap.TxCount, "tx", ""), label, Fmt.Reset)
}

// glBalanceDetail renders the linked GL account's live balance, in the form
// "<balance> EUR (<n> txs - account <code>)". The figure comes from posted
// account.move.line entries in Odoo (company currency = EUR), so it needs one
// live query; it degrades to "account <code>" when Odoo isn't reachable.
func glBalanceDetail(acc *AccountConfig) string {
	code := acc.OdooGlAccountCode
	fallback := Fmt.Dim + "account " + code + Fmt.Reset
	creds, err := ResolveOdooCredentials()
	if err != nil {
		return fallback + Fmt.Dim + " — Odoo not configured" + Fmt.Reset
	}
	uid, err := odooAuth(creds.URL, creds.DB, creds.Login, creds.Password)
	if err != nil || uid == 0 {
		return fallback + Fmt.Dim + " — Odoo auth failed" + Fmt.Reset
	}
	gl, ok, err := fetchOdooAccountByCode(creds, uid, code)
	if err != nil || !ok {
		return fallback + Fmt.Dim + " — not found in Odoo" + Fmt.Reset
	}
	balance, count, _, err := fetchOdooAccountBalanceAt(creds, uid, gl.ID, "")
	if err != nil {
		return fallback + Fmt.Dim + " — " + err.Error() + Fmt.Reset
	}
	return fmt.Sprintf("%s  %s(%s - account %s)%s",
		formatAccountDataBalance(balance, "EUR"), Fmt.Dim, Pluralize(count, "tx", ""), code, Fmt.Reset)
}

// accountJournalSnapshot returns the linked journal's balance/tx snapshot.
// Without refresh it uses the local journal-lines cache (no network); with
// refresh it queries Odoo and refreshes the cached journal name.
func accountJournalSnapshot(acc *AccountConfig, currency string, refresh bool) (accountOdooSyncSnapshot, bool) {
	if acc == nil || acc.OdooJournalID == 0 {
		return accountOdooSyncSnapshot{}, false
	}
	if refresh {
		if creds, err := ResolveOdooCredentials(); err == nil {
			if uid, err := odooAuth(creds.URL, creds.DB, creds.Login, creds.Password); err == nil && uid != 0 {
				if OdooJournalName(acc.OdooJournalID) == "" {
					_, _ = FetchAndCacheOdooJournalName(creds, uid, acc.OdooJournalID)
				}
				if snap, err := fetchOdooJournalSnapshot(creds, uid, acc.OdooJournalID, currency); err == nil {
					return snap, true
				}
			}
		}
	}
	return fetchOdooJournalSnapshotLocal(acc.OdooJournalID, currency)
}

// liveSourceTxCount returns how many raw source transactions are cached locally
// for this account — the data backing the live balance figure.
func liveSourceTxCount(acc *AccountConfig) (int, bool) {
	switch acc.Provider {
	case "etherscan":
		if st := loadAccountOnchainStats(acc); st != nil {
			return st.TxCount, true
		}
	case "stripe":
		if txs, err := stripesource.LoadTransactions(DataDir(), acc.AccountID); err == nil && len(txs) > 0 {
			return len(txs), true
		}
	}
	return 0, false
}

// liveBalanceSourceShort is the compact provenance word for the live balance.
func liveBalanceSourceShort(acc *AccountConfig) string {
	switch acc.Provider {
	case "stripe":
		return "Stripe"
	case "etherscan":
		return "on-chain"
	case "kbcbrussels":
		return "KBC CSV"
	default:
		return "live"
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
	case "kbcbrussels":
		if acc.IBAN == "" {
			return
		}
		printAccountField(indent, "Address", "iban "+acc.IBAN)
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
	case "kbcbrussels":
		if acc.IBAN == "" {
			return "", ""
		}
		// KBC Brussels doesn't have a per-account public URL; the IBAN is
		// the only canonical handle we can expose here.
		return "iban " + acc.IBAN, ""
	}
	return "", ""
}

// printAccountField renders one "Label: value" row, padding the label to
// accountFieldLabelWidth so values line up across rows.
func printAccountField(indent, label, value string) {
	fmt.Printf("%s%s%-*s%s %s\n", indent, Fmt.Dim, accountFieldLabelWidth, label+":", Fmt.Reset, value)
}

// accountFieldLabelWidth aligns account detail rows; "Journal balance:" is the
// longest label rendered in the summary.
const accountFieldLabelWidth = 16

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
			path, found := etherscansource.FindFileForAddr(dataDir, yd.Name(), md.Name(), acc.Chain, acc.Slug, acc.Address, acc.Token.Symbol)
			if !found {
				continue
			}
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
	OdooJournalID     int    `json:"odooJournalId,omitempty"`
	OdooJournalName   string `json:"odooJournalName,omitempty"`
	OdooGlAccountCode string `json:"odooGlAccountCode,omitempty"`
	OdooMissing       *int   `json:"odooMissing,omitempty"`
	OdooLastTxDate    string `json:"odooLastTxDate,omitempty"`
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
	details := HasFlag(args, "--details")
	// --live (alias --refresh/-r) fetches balances from the network and caches
	// them. By default `chb accounts` stays offline: it reads the cached
	// balances if present, otherwise falls back to the tx-history figure.
	live := HasFlag(args, "--live", "--refresh", "-r")

	var liveBalances map[string]float64
	var cacheTime string
	if live {
		fmt.Printf("  Fetching live balances...\n")
		liveBalances = fetchLiveBalances(configs)
		cacheTime = time.Now().UTC().Format(time.RFC3339)
	} else if cache := loadBalanceCache(); cache != nil {
		liveBalances = cache.Balances
		cacheTime = cache.FetchedAt
	}

	// Track totals per currency
	totals := map[string]float64{}

	// Per-account alignment: does each account's latest balance agree with its
	// linked Odoo journal AND GL account? Cached (local vs journal) by default;
	// --live adds the GL-account leg (one Odoo query per account).
	var alignCreds *OdooCredentials
	var alignUID int
	if live {
		if c, err := ResolveOdooCredentials(); err == nil {
			if u, err := odooAuth(c.URL, c.DB, c.Login, c.Password); err == nil && u != 0 {
				alignCreds, alignUID = c, u
			}
		}
	}
	aligns := map[string]accountAlignment{}
	for i := range configs {
		acc := &configs[i]
		if acc.OdooJournalID == 0 && acc.OdooGlAccountCode == "" {
			continue
		}
		s := summaries[accountKey(faAccounts[i])]
		bal, _, has := resolveAccountBalance(acc, liveBalances, s)
		aligns[acc.Slug] = computeAccountAlignment(acc, accountCurrency(acc), bal, has, live, "", alignCreds, alignUID)
	}

	if details {
		fmt.Printf("\n%s💰 Configured Accounts%s (%d)", Fmt.Bold, Fmt.Reset, len(configs))
		if cacheTime != "" {
			if t, err := time.Parse(time.RFC3339, cacheTime); err == nil {
				fmt.Printf("  %s(balances: %s)%s", Fmt.Dim, formatTimeAgo(t), Fmt.Reset)
			}
		}
		fmt.Println()
		fmt.Println()

		// Odoo sync status for accounts with linked journals. Local-cache only
		// by default so the view stays instant; --live queries Odoo live.
		var odooStatuses map[int]*odooSyncStatus
		if live {
			odooStatuses = fetchOdooSyncStatuses(configs, summaries)
		} else {
			odooStatuses = localOdooSyncStatuses(configs, summaries)
		}

		for i, acc := range configs {
			fa := faAccounts[i]
			currency := accountCurrency(&acc)
			s := summaries[accountKey(fa)]
			balance, _, hasBalance := resolveAccountBalance(&acc, liveBalances, s)
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
			if acc.OdooGlAccountCode != "" {
				printAccountField("    ", "GL account", acc.OdooGlAccountCode)
			}
			if al, ok := aligns[acc.Slug]; ok {
				printAccountAlignmentDetail("    ", al)
			}

			fmt.Println()
		}
		printAlignmentSummary(aligns, live)
	} else {
		// Default: one compact line per account — slug on the left, balance
		// right-aligned, ordered by balance DESC. Run with --details for the
		// full per-account view.
		fmt.Println()
		type acctRow struct {
			slug     string
			currency string
			balance  float64
			has      bool
		}
		rows := make([]acctRow, 0, len(configs))
		for i, acc := range configs {
			fa := faAccounts[i]
			currency := accountCurrency(&acc)
			s := summaries[accountKey(fa)]
			balance, _, has := resolveAccountBalance(&acc, liveBalances, s)
			rows = append(rows, acctRow{acc.Slug, currency, balance, has})
			if has {
				totals[currency] += balance
			}
		}
		// Highest balance first; accounts with no known balance sink to the bottom.
		sort.SliceStable(rows, func(i, j int) bool {
			if rows[i].has != rows[j].has {
				return rows[i].has
			}
			return rows[i].balance > rows[j].balance
		})

		// Pre-compute the EUR-family total so its width factors into the column.
		eurTotal := 0.0
		for cur, bal := range totals {
			if isEURCurrency(cur) {
				eurTotal += bal
			}
		}

		// Column widths: label = widest slug/total label, amount = widest plain
		// (un-coloured) amount, so the colour codes don't throw off alignment.
		labelWidth, amountWidth := 0, 1 // amount starts at 1 for the "—" placeholder
		measure := func(label, plain string) {
			if len(label) > labelWidth {
				labelWidth = len(label)
			}
			if w := len([]rune(plain)); w > amountWidth {
				amountWidth = w
			}
		}
		for _, r := range rows {
			plain := "—"
			if r.has {
				plain = formatBalancePlain(r.balance, r.currency)
			}
			measure(r.slug, plain)
		}
		if eurTotal != 0 {
			measure("Total EUR", formatBalancePlain(eurTotal, "EUR"))
		}
		for cur, bal := range totals {
			if !isEURCurrency(cur) {
				measure("Total "+cur, formatBalancePlain(bal, cur))
			}
		}

		for _, r := range rows {
			plain, colored := "—", Fmt.Dim+"—"+Fmt.Reset
			if r.has {
				plain = formatBalancePlain(r.balance, r.currency)
				colored = formatBalance(r.balance, r.currency)
			}
			fmt.Printf("  %s%-*s%s  %s%s\n", Fmt.Bold, labelWidth, r.slug, Fmt.Reset, rightAlignAmount(plain, colored, amountWidth), alignmentMarker(aligns[r.slug]))
		}

		if len(totals) > 0 {
			fmt.Printf("  %s%s%s\n", Fmt.Dim, strings.Repeat("─", labelWidth+2+amountWidth), Fmt.Reset)
			if eurTotal != 0 {
				fmt.Printf("  %s%-*s%s  %s\n", Fmt.Bold, labelWidth, "Total EUR", Fmt.Reset,
					rightAlignAmount(formatBalancePlain(eurTotal, "EUR"), formatBalance(eurTotal, "EUR"), amountWidth))
			}
			for cur, bal := range totals {
				if !isEURCurrency(cur) {
					fmt.Printf("  %s%-*s%s  %s\n", Fmt.Bold, labelWidth, "Total "+cur, Fmt.Reset,
						rightAlignAmount(formatBalancePlain(bal, cur), formatBalance(bal, cur), amountWidth))
				}
			}
			// "as of" the moment the balances were captured: now under --live,
			// otherwise the last balance sync (falling back to the most recent
			// per-account sync when no balance cache exists).
			if asOf := accountsAsOf(live, cacheTime, configs); !asOf.IsZero() {
				fmt.Printf("  %sas of %s%s\n", Fmt.Dim, asOf.In(BrusselsTZ()).Format("2006-01-02"), Fmt.Reset)
			}
		}
		fmt.Println()
		printAlignmentSummary(aligns, live)
		fmt.Println()
		return
	}

	// Totals per currency (details view)
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

// rightAlignAmount right-pads an ANSI-coloured amount so its visible glyphs end
// at column `width`. `plain` is the un-coloured form used purely to measure the
// real display width (the colour codes in `colored` would corrupt %-padding).
func rightAlignAmount(plain, colored string, width int) string {
	pad := width - len([]rune(plain))
	if pad < 0 {
		pad = 0
	}
	return strings.Repeat(" ", pad) + colored
}

// accountsAsOf returns the timestamp the displayed balances reflect: now under
// --live, otherwise the balance-cache fetch time, falling back to the most
// recent per-account sync when no balance cache exists.
func accountsAsOf(live bool, cacheTime string, configs []AccountConfig) time.Time {
	if live {
		return time.Now()
	}
	if cacheTime != "" {
		if t, err := time.Parse(time.RFC3339, cacheTime); err == nil {
			return t
		}
	}
	var latest time.Time
	for _, acc := range configs {
		if t := LastSyncTime("account:" + strings.ToLower(acc.Slug)); t.After(latest) {
			latest = t
		}
	}
	return latest
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
// localOdooSyncStatuses builds the same per-journal status as
// fetchOdooSyncStatuses but purely from the locally-cached journal lines (no
// network). It powers the default, instant `chb accounts` view; --refresh
// swaps in the live fetcher.
func localOdooSyncStatuses(configs []AccountConfig, summaries map[string]*accountSummary) map[int]*odooSyncStatus {
	result := map[int]*odooSyncStatus{}
	faAccounts := ToFinanceAccounts(configs)
	for i, acc := range configs {
		if acc.OdooJournalID == 0 {
			continue
		}
		cache, ok := loadLatestOdooJournalLinesCache(acc.OdooJournalID)
		if !ok {
			continue
		}
		var lastDate time.Time
		for _, ln := range cache {
			if t, err := time.Parse("2006-01-02", ln.Date); err == nil && t.After(lastDate) {
				lastDate = t
			}
		}
		localCount := 0
		if s := summaries[accountKey(faAccounts[i])]; s != nil {
			localCount = s.TxCount
		}
		missing := localCount - len(cache)
		if missing < 0 {
			missing = 0
		}
		result[acc.OdooJournalID] = &odooSyncStatus{
			Missing:        missing,
			TotalOdoo:      len(cache),
			TotalLocal:     localCount,
			LastOdooTxDate: lastDate,
		}
	}
	return result
}

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
		return wrapOdooAuthError(err)
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

	CacheOdooJournalName(selected.ID, selected.Name)

	// The link is keyed by the account's stable identity, so it lives in
	// odoo-journals.json (not the force-overwritten accounts.json) and applies
	// regardless of the currently configured Odoo instance.
	if err := setOdooJournalLink(creds.DB, accountIdentityKey(configs[idx]), selected.ID); err != nil {
		return fmt.Errorf("failed to save link: %v", err)
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
	verbose := HasFlag(args, "--verbose", "-v") || HasFlag(args, "--debug")

	// Snapshot before-counts so we can report "N new transactions" after.
	countBefore := accountProviderRawCount(acc)
	printAccountFetchHeader(acc, args)

	// Odoo-source-of-truth account (e.g. KBC): the Odoo journal is authoritative
	// and new entries are created there, not in the bank CSV. Pull them from
	// Odoo by refreshing the journal-lines cache, which is what the views,
	// reconcile and fix read.
	if acc.IsOdooSourceOfTruth() && acc.OdooJournalID > 0 {
		newLines, err := pullOdooSourceOfTruthAccount(acc, HasFlag(args, "--history"), verbose)
		if err != nil {
			return err
		}
		UpdateSyncSource("account:"+strings.ToLower(slug), HasFlag(args, "--history"))
		refreshAndPersistAccountBalance(acc)
		printAccountFetchSummary(acc, newLines, time.Since(startedAt), verbose)
		return nil
	}

	// Manual / CSV provider: no upstream API to call. Re-generate every
	// month touched by the CSV so monthly transactions.json files catch
	// up with anything the operator dropped under latest/providers/.
	if acc.Provider == "kbcbrussels" {
		// Nothing to read for this account → print actionable guidance
		// (where to drop the CSV / which IBAN to fix) instead of a
		// silent "0 new transactions" comparison block. syncKBCAccount
		// always surfaces this guidance, even in compact mode.
		if countBefore == 0 {
			if err := syncKBCAccount(acc, verbose); err != nil {
				return err
			}
			UpdateSyncSource("account:"+strings.ToLower(slug), true)
			return nil
		}
		if err := syncKBCAccount(acc, verbose); err != nil {
			return err
		}
		UpdateSyncSource("account:"+strings.ToLower(slug), true)
		refreshAndPersistAccountBalance(acc)
		newTxs := accountProviderRawCount(acc) - countBefore
		if newTxs < 0 {
			newTxs = 0
		}
		printAccountFetchSummary(acc, newTxs, time.Since(startedAt), verbose)
		return nil
	}
	checkpoint := latestAccountSourceCheckpoint(acc)
	beforeSourceMonths := accountSourceMonthFingerprints(acc)
	fetchArgs := accountFetchArgsForCheckpoint(*acc, args, checkpoint)
	if verbose && acc.Provider == "etherscan" {
		for _, line := range accountSyncPlanLines(acc, accountTransactionSource(*acc), checkpoint, accountFetchArgsHasExplicitRange(args)) {
			fmt.Printf("  %s\n", line)
		}
		fmt.Println()
	}
	// Default to quiet: capture the inner sync chatter so the operator
	// gets a focused summary at the end. --verbose surfaces the per-page
	// progress for diagnostics.
	var syncErr error
	if verbose {
		_, syncErr = TransactionsSync(fetchArgs)
	} else {
		restore := silenceStdout()
		_, syncErr = TransactionsSync(fetchArgs)
		restore()
	}
	if syncErr != nil {
		return syncErr
	}
	touchedMonths := accountChangedSourceMonths(acc, beforeSourceMonths)
	if len(touchedMonths) > 0 {
		if verbose {
			if err := GenerateTransactionsForMonths(touchedMonths); err != nil {
				return fmt.Errorf("generate transactions after fetch: %v", err)
			}
		} else {
			restore := silenceStdout()
			err := GenerateTransactionsForMonths(touchedMonths)
			restore()
			if err != nil {
				return fmt.Errorf("generate transactions after fetch: %v", err)
			}
		}
	}
	UpdateSyncSource("account:"+strings.ToLower(slug), accountSyncIsFull(args))
	// Refresh the persisted live-balance cache so the summary below can
	// print the live balance alongside the local total.
	refreshAndPersistAccountBalance(acc)
	if verification := verifyAccountLocalAgainstOnchainCache(acc, nil); verification != nil && verbose {
		printAccountSyncVerification(verification)
	}
	newTxs := accountProviderRawCount(acc) - countBefore
	if newTxs < 0 {
		newTxs = 0
	}
	printAccountFetchSummary(acc, newTxs, time.Since(startedAt), verbose)
	return nil
}

// pullOdooSourceOfTruthAccount refreshes the linked Odoo journal-lines cache for
// an odooSourceOfTruth account (the real pull source) and returns how many NEW
// lines arrived. fullHistory forces a full re-fetch; otherwise it's an
// incremental pull keyed on the write-date watermark.
func pullOdooSourceOfTruthAccount(acc *AccountConfig, fullHistory, verbose bool) (int, error) {
	creds, err := ResolveOdooCredentials()
	if err != nil {
		return 0, err
	}
	uid, err := odooAuth(creds.URL, creds.DB, creds.Login, creds.Password)
	if err != nil || uid == 0 {
		return 0, fmt.Errorf("Odoo authentication failed: %v", err)
	}

	before := 0
	if cached, ok := loadLatestOdooJournalLinesCache(acc.OdooJournalID); ok {
		before = len(cached)
	}

	if fullHistory {
		cur := LoadSyncCursor(SyncCursorKeyForOdooJournal(acc.OdooJournalID))
		if _, err := writeOdooJournalLinesCacheFullRefetch(creds, uid, acc.OdooJournalID, &cur); err != nil {
			return 0, err
		}
	} else if _, err := writeOdooJournalLinesCache(creds, uid, acc.OdooJournalID); err != nil {
		return 0, err
	}

	after := 0
	if cached, ok := loadLatestOdooJournalLinesCache(acc.OdooJournalID); ok {
		after = len(cached)
	}
	if verbose {
		fmt.Printf("  %sJournal #%d cache: %d → %d lines%s\n", Fmt.Dim, acc.OdooJournalID, before, after, Fmt.Reset)
	}
	newLines := after - before
	if newLines < 0 {
		newLines = 0
	}
	return newLines, nil
}

// reconcileAfterPushSummary appends a concise "x new · y reconciled" note to the
// push summary and, when some new items went unreconciled, prints them so the
// operator can resolve them (the rest are auto-matched to invoices/bills). x is
// the number of lines this push created; y is what the silent reconcile pass
// matched (lastReconcileApplied).
func reconcileAfterPushSummary(creds *OdooCredentials, uid, journalID, newItems int) string {
	reconciled := lastReconcileApplied
	if newItems <= 0 {
		return "reconcile pass complete"
	}
	unmatched := newItems - reconciled
	if unmatched < 0 {
		unmatched = 0
	}
	if unmatched > 0 {
		printUnreconciledNewItems(creds, uid, journalID, newItems)
	}
	return fmt.Sprintf("%d new item%s · %d reconciled", newItems, plural(newItems), reconciled)
}

// printUnreconciledNewItems lists the most-recently-created statement lines on
// the journal (≈ the ones this push added) that are still unreconciled, so the
// operator sees exactly what needs attention. Best-effort: any query error is
// silently skipped (the summary line still prints).
func printUnreconciledNewItems(creds *OdooCredentials, uid, journalID, newItems int) {
	res, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement.line", "search_read",
		[]interface{}{
			[]interface{}{[]interface{}{"journal_id", "=", journalID}, []interface{}{"is_reconciled", "=", false}},
			[]string{"date", "payment_ref", "amount"},
		},
		map[string]interface{}{"limit": newItems, "order": "id desc"})
	if err != nil {
		return
	}
	var lines []struct {
		Date       string  `json:"date"`
		PaymentRef string  `json:"payment_ref"`
		Amount     float64 `json:"amount"`
	}
	if json.Unmarshal(res, &lines) != nil || len(lines) == 0 {
		return
	}
	fmt.Printf("  %s%d new item%s left unreconciled — review with `chb odoo journals %d reconcile -i`:%s\n",
		Fmt.Yellow, len(lines), plural(len(lines)), journalID, Fmt.Reset)
	for _, l := range lines {
		fmt.Printf("    %s%s  %s%9s  %s%s\n",
			Fmt.Dim, l.Date, Fmt.Reset, fmtNumber(l.Amount), Truncate(l.PaymentRef, 50), Fmt.Reset)
	}
}

// reconcileAutoThreshold caps the auto-reconcile-after-push behaviour. At
// or below this many newly-created lines, the post-push reconcile runs
// automatically; above it the operator runs `chb odoo journals N
// reconcile` explicitly. Calibrated for the typical hourly cron rhythm
// (0–5 new lines) where auto-reconcile is essentially free, while
// keeping large back-fills (initial migration, journal reset+re-push)
// under explicit operator control to avoid accidental partner/account
// mass-edits.
const reconcileAutoThreshold = 20

// shouldReconcileAfterPush encodes the policy: --skip-reconcile opts
// out; otherwise the new-line count decides. When skipped because of
// the threshold, prints a hint pointing at the dedicated reconcile
// verb and queues a closing attention item for the compact `chb push`
// driver to surface at the end.
func shouldReconcileAfterPush(args []string, newLines int, label string) bool {
	if HasFlag(args, "--skip-reconcile", "--skip-reconciliation") {
		return false
	}
	if newLines <= reconcileAutoThreshold {
		return true
	}
	fmt.Printf("  %s↳ %d new lines exceeds the auto-reconcile threshold of %d; run `chb odoo journals <id> reconcile` to reconcile them%s\n",
		Fmt.Dim, newLines, reconcileAutoThreshold, Fmt.Reset)
	queuePushAttentionHint(label, fmt.Sprintf("%d new lines — auto-reconcile skipped (threshold %d)", newLines, reconcileAutoThreshold))
	return false
}

// queuePushAttentionHint adds a "needs attention" entry for the
// compact push driver to surface at the end. label is the account
// slug; the journal id is looked up so the suggested command is
// copy-pasteable.
func queuePushAttentionHint(label, message string) {
	if label == "" {
		return
	}
	journalID := 0
	for _, acc := range LoadAccountConfigs() {
		if strings.EqualFold(acc.Slug, label) && acc.OdooJournalID > 0 {
			journalID = acc.OdooJournalID
			break
		}
	}
	suggested := ""
	if journalID > 0 {
		suggested = fmt.Sprintf("chb odoo journals %d reconcile", journalID)
	}
	pushAttentionHints = append(pushAttentionHints, pushAttentionHint{
		JournalID: journalID,
		Slug:      label,
		Message:   message,
		Suggested: suggested,
	})
}

// accountProviderDisplayName returns the human-friendly source label used
// in the pull header ("Stripe", "Etherscan", "Monerium", "CSV", …).
func accountProviderDisplayName(acc *AccountConfig) string {
	if acc == nil {
		return ""
	}
	// When the linked Odoo journal is authoritative, the pull source IS Odoo
	// (e.g. KBC: the bank CSV only bootstrapped the journal; new entries are
	// created on the Odoo side and pulled from there).
	if acc.IsOdooSourceOfTruth() {
		return "Odoo"
	}
	switch acc.Provider {
	case "stripe":
		return "Stripe"
	case "etherscan":
		return "Etherscan"
	case "monerium":
		return "Monerium"
	case "kbcbrussels":
		return "CSV"
	}
	return strings.Title(acc.Provider)
}

// accountProviderRawCount counts the transactions sitting in the raw
// provider cache for this account. After a fresh pull this is what the
// upstream provider currently has, which is the right number to compare
// local + Odoo against.
func accountProviderRawCount(acc *AccountConfig) int {
	if acc == nil {
		return 0
	}
	dataDir := DataDir()
	switch acc.Provider {
	case "stripe":
		n := 0
		years, _ := os.ReadDir(dataDir)
		for _, y := range years {
			if !y.IsDir() || len(y.Name()) != 4 {
				continue
			}
			months, _ := os.ReadDir(filepath.Join(dataDir, y.Name()))
			for _, m := range months {
				if !m.IsDir() || len(m.Name()) != 2 {
					continue
				}
				cache, ok := stripesource.LoadCache(stripesource.TransactionCachePath(dataDir, y.Name(), m.Name()))
				if !ok || !strings.EqualFold(cache.AccountID, acc.AccountID) {
					continue
				}
				n += len(cache.Transactions)
			}
		}
		return n
	case "etherscan", "monerium":
		return len(accountCountableTransfers(acc))
	case "kbcbrussels":
		if acc.IBAN == "" {
			return 0
		}
		rows, err := kbcbrusselssource.LoadTransactionsForIBAN(dataDir, acc.IBAN)
		if err != nil {
			return 0
		}
		return len(rows)
	}
	return 0
}

// printAccountFetchHeader opens a per-account pull with a header tuned
// to what the user asked for:
//
//	chb accounts <slug> pull --history       → "Pulling all transactions from <Provider>"
//	chb accounts <slug> pull --since <date>   → "Pulling new transactions since <date> from <Provider>"
//	chb accounts <slug> pull                  → "Pulling new transactions since <last sync> from <Provider>"
//
// The "since" timeline matches the actual cursor the fetch will use, so
// the operator can predict the cost of the call from the header alone.
func printAccountFetchHeader(acc *AccountConfig, args []string) {
	provider := accountProviderDisplayName(acc)
	if HasFlag(args, "--history") {
		fmt.Printf("\n%sPulling all transactions from %s%s\n", Fmt.Bold, provider, Fmt.Reset)
		return
	}
	if v := GetOption(args, "--since"); v != "" {
		fmt.Printf("\n%sPulling new transactions since %s from %s%s\n", Fmt.Bold, v, provider, Fmt.Reset)
		return
	}
	last := LastSyncTime("account:" + strings.ToLower(acc.Slug))
	if last.IsZero() {
		fmt.Printf("\n%sPulling all transactions from %s%s %s(first sync)%s\n",
			Fmt.Bold, provider, Fmt.Reset, Fmt.Dim, Fmt.Reset)
		return
	}
	fmt.Printf("\n%sPulling new transactions since %s from %s%s\n",
		Fmt.Bold, last.In(BrusselsTZ()).Format("2006-01-02 15:04"), provider, Fmt.Reset)
}

// isOdooSyntheticLine returns true for cache lines that don't correspond
// to a single upstream transaction — fee-allocation entries chb itself
// creates to balance the Stripe gross-vs-net booking pattern:
//
//   - import_id ending in ":fees" — per-payout fee deduction
//   - import_id starting with "open:" — aggregate open-statement fee line
//
// These are bookkeeping artifacts, not real transactions, so they're
// excluded from the count when comparing against the upstream provider.
// The balance still includes them, since the journal's actual balance
// does include their amounts.
func isOdooSyntheticLine(ln OdooCacheLine) bool {
	iid := strings.ToLower(ln.UniqueImportID)
	if strings.HasSuffix(iid, ":fees") {
		return true
	}
	if strings.HasPrefix(iid, "open:") {
		return true
	}
	return false
}

// printAccountFetchSummary prints the aligned 3-row comparison block after
// a per-account pull:
//
//	0 new transactions
//	Stripe:           3,593 txs  balance:    982.62 EUR
//	Local:            3,593 txs  balance:    982.62 EUR  ✓
//	Odoo journal #48: 3,593 txs  balance:    983.60 EUR  ✗
//
// ✓ on Local means it matches the Provider snapshot we just fetched.
// ✓ on Odoo means the linked Odoo journal cache matches Local. Mismatches
// surface as ✗ and tell the operator what to investigate next.
//
// The Odoo line count excludes synthetic fee-allocation lines (see
// isOdooSyntheticLine) so it can be compared against the real Stripe BT
// count directly. --verbose adds the journal name + a synthetic-line
// annotation when any are present.
func printAccountFetchSummary(acc *AccountConfig, newTxs int, elapsed time.Duration, verbose bool) {
	if acc == nil {
		return
	}
	currency := accountCurrency(acc)
	totals := computeAccountTotals(acc)
	localCount := 0
	localBalance := 0.0
	if totals != nil {
		localCount = totals.TxCount
		localBalance = totals.CurrentBalance
		if totals.Currency != "" {
			currency = totals.Currency
		}
	}

	providerCount := accountProviderRawCount(acc)
	providerBalance := localBalance
	providerKnown := false
	if cache := loadBalanceCache(); cache != nil {
		if v, _, ok := resolveAccountBalance(acc, cache.Balances, nil); ok {
			providerBalance = v
			providerKnown = true
		}
	}

	type row struct {
		label   string
		count   int
		balance float64
		match   *bool
	}
	var rows []row

	// For an odooSourceOfTruth account the provider IS the Odoo journal (shown as
	// its own row below), so a separate "Odoo:" provider row — derived from the
	// stale bootstrap CSV — would just be a confusing duplicate. Skip it.
	odooSoT := acc.IsOdooSourceOfTruth()
	if !odooSoT {
		rows = append(rows, row{
			label:   accountProviderDisplayName(acc) + ":",
			count:   providerCount,
			balance: providerBalance,
		})
	}

	// The "Local" row is the CSV/generated mirror. For an odooSourceOfTruth
	// account that mirror isn't authoritative (the Odoo journal is), so drop it
	// entirely rather than show a misleading mismatch against it.
	if !odooSoT {
		matchLocal := localCount == providerCount && roundCents(localBalance) == roundCents(providerBalance)
		localRow := row{label: "Local:", count: localCount, balance: localBalance}
		if providerKnown {
			localRow.match = &matchLocal
		}
		rows = append(rows, localRow)
	}

	var syntheticCount int
	var syntheticBalance float64
	if acc.OdooJournalID > 0 {
		// Default: "Odoo journal #48:". --verbose adds "(Stripe (synced))"
		// so the operator can confirm they're looking at the right journal
		// when debugging; the bare ID is enough day-to-day.
		label := fmt.Sprintf("Odoo journal #%d:", acc.OdooJournalID)
		if verbose {
			if name := OdooJournalName(acc.OdooJournalID); name != "" {
				label = fmt.Sprintf("Odoo journal #%d (%s):", acc.OdooJournalID, name)
			}
		}
		cached, ok := loadLatestOdooJournalLinesCache(acc.OdooJournalID)
		if !ok {
			rows = append(rows, row{label: label})
		} else {
			var sum, realSum float64
			realCount := 0
			for _, ln := range cached {
				sum += ln.Amount
				if isOdooSyntheticLine(ln) {
					syntheticCount++
					syntheticBalance += ln.Amount
				} else {
					realCount++
					realSum += ln.Amount
				}
			}
			// Compare real-line count + total balance against local. The
			// balance match still uses the full sum because that's what the
			// journal actually balances to. For an odooSourceOfTruth account
			// there's no Local row to match against, so show the figures plainly.
			r := row{label: label, count: realCount, balance: roundCents(sum)}
			if !odooSoT {
				matchOdoo := realCount == localCount && roundCents(sum) == roundCents(localBalance)
				r.match = &matchOdoo
			}
			rows = append(rows, r)
		}
	}

	maxLabel, maxCount := 0, 0
	for _, r := range rows {
		if w := displayWidth(r.label); w > maxLabel {
			maxLabel = w
		}
		if c := len(Pluralize(r.count, "tx", "")); c > maxCount {
			maxCount = c
		}
	}

	fmt.Printf("\n  %s%s%s  %s%s%s\n\n",
		Fmt.Bold, Pluralize(newTxs, "new transaction", ""), Fmt.Reset,
		Fmt.Dim, elapsed.Round(100*time.Millisecond), Fmt.Reset)

	for _, r := range rows {
		mark := ""
		if r.match != nil {
			if *r.match {
				mark = " " + Fmt.Green + "✓" + Fmt.Reset
			} else {
				mark = " " + Fmt.Red + "✗" + Fmt.Reset
			}
		}
		fmt.Printf("  %s  %s  balance: %s%s\n",
			padRight(r.label, maxLabel),
			padLeft(Pluralize(r.count, "tx", ""), maxCount),
			padLeft(formatAccountDataBalance(r.balance, currency), 14),
			mark,
		)
	}

	// In verbose mode, surface the synthetic-fee detail so the operator
	// understands why Odoo's full line count is higher than the comparison
	// row above shows.
	if verbose && syntheticCount > 0 {
		fmt.Printf("  %s↳ Odoo journal includes %s totaling %s%s\n",
			Fmt.Dim,
			Pluralize(syntheticCount, "synthetic fee allocation", "synthetic fee allocations"),
			formatAccountDataBalance(roundCents(syntheticBalance), currency),
			Fmt.Reset)
	}
	fmt.Println()
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

// accountFetchResult captures one account's pull outcome for the summary table.
type accountFetchResult struct {
	Account AccountConfig
	Count   int
	Err     error
}

// accountsFetchParallelism bounds how many accounts are pulled concurrently.
// Each account writes its own provider files (distinct paths by slug/chain/
// token) so the fetches don't contend on disk; the cap keeps us comfortably
// under Etherscan's rate limit (which the source layer also retries on).
const accountsFetchParallelism = 4

// AccountsFetchAll fetches all configured accounts source → local, concurrently.
// Archived accounts are skipped (no new activity to pull). GenerateTransactions
// runs once at the end, not after every account. Per-account errors are reported
// but do not abort the run; the returned error is non-nil if any account failed.
func AccountsFetchAll(args []string) (int, error) {
	configs := LoadAccountConfigs()
	if len(configs) == 0 {
		fmt.Printf("\n  %sNo accounts configured%s\n\n", Fmt.Dim, Fmt.Reset)
		return 0, nil
	}

	// Skip archived accounts — they're closed and have nothing new to fetch.
	active := make([]AccountConfig, 0, len(configs))
	skipped := 0
	for _, acc := range configs {
		if acc.IsArchived() {
			skipped++
			continue
		}
		active = append(active, acc)
	}

	fmt.Printf("\n%s🔄 Syncing accounts%s", Fmt.Bold, Fmt.Reset)
	if skipped > 0 {
		fmt.Printf("  %s(%s skipped)%s", Fmt.Dim, Pluralize(skipped, "archived account", ""), Fmt.Reset)
	}
	fmt.Println()
	fmt.Println()

	results := make([]accountFetchResult, len(active))

	status := newStatusLine()
	status.Update("accounts: syncing %d accounts (×%d)...", len(active), accountsFetchParallelism)

	// Silence stdout for the whole concurrent phase: the per-account sync prints
	// progress lines that would interleave and corrupt the terminal. Warnings /
	// errors still reach stderr + the diagnostics log. Restored before the table.
	restore := silenceStdout()
	sem := make(chan struct{}, accountsFetchParallelism)
	var wg sync.WaitGroup
	for i := range active {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			count, err := TransactionsSync(accountFetchArgs(active[i], args))
			results[i] = accountFetchResult{Account: active[i], Count: count, Err: err}
		}(i)
	}
	wg.Wait()
	restore()
	status.Clear()

	failed, totalNew := 0, 0
	for _, r := range results {
		if r.Err != nil {
			failed++
			continue
		}
		totalNew += r.Count
		// Shared sync-state file — write serially, off the worker goroutines.
		UpdateSyncSource("account:"+strings.ToLower(r.Account.Slug), accountSyncIsFull(args))
	}

	// Regenerate the unified per-month transactions.json files ONCE after all
	// accounts have been fetched, rather than after each account — but only when
	// at least one account actually pulled a new transaction. Nothing new means
	// the inputs are unchanged, so the generated files would be identical.
	if totalNew > 0 {
		genStatus := newStatusLine()
		genStatus.Update("generated: regenerating per-month transactions...")
		generateOutput, err := captureGenerateTransactions(args)
		genStatus.Clear()
		if err != nil {
			Errorf("  %s✗ generate: %v%s", Fmt.Red, err, Fmt.Reset)
			if strings.TrimSpace(generateOutput) != "" {
				fmt.Print(generateOutput)
			}
		} else {
			odooSyncLine("generated", "per-month transactions refreshed")
		}
	} else {
		odooSyncLine("generated", "skipped — no new transactions")
	}

	printAccountsFetchTable(results)

	if failed > 0 {
		return failed, fmt.Errorf("%s failed", Pluralize(failed, "account", ""))
	}
	return 0, nil
}

// printAccountsFetchTable renders the post-pull summary as an aligned table —
// one row per account with its tx count, balance and fetch status — instead of
// repeating "N txs, balance: …" on every line.
func printAccountsFetchTable(results []accountFetchResult) {
	type cell struct{ slug, txs, txsC, bal, balC, status, statusC string }
	cells := make([]cell, 0, len(results))
	for _, r := range results {
		txCount, balance, currency := accountSyncTableData(&r.Account)
		c := cell{
			slug: r.Account.Slug,
			txs:  fmt.Sprintf("%d", txCount),
			txsC: fmt.Sprintf("%d", txCount),
			bal:  formatBalancePlain(balance, currency),
			balC: formatBalance(balance, currency),
		}
		switch {
		case r.Err != nil:
			c.status = "✗ " + r.Err.Error()
			c.statusC = Fmt.Red + "✗ " + r.Err.Error() + Fmt.Reset
		case r.Count > 0:
			c.status = fmt.Sprintf("%d synced", r.Count)
			c.statusC = Fmt.Green + fmt.Sprintf("%d synced", r.Count) + Fmt.Reset
		default:
			c.status = "already in sync"
			c.statusC = Fmt.Dim + "already in sync" + Fmt.Reset
		}
		cells = append(cells, c)
	}

	// Column widths from the plain (un-coloured) forms so ANSI codes don't skew
	// the alignment; header row included.
	wSlug, wTxs, wBal := len("Account"), len("Txs"), len("Balance")
	for _, c := range cells {
		if len(c.slug) > wSlug {
			wSlug = len(c.slug)
		}
		if len([]rune(c.txs)) > wTxs {
			wTxs = len([]rune(c.txs))
		}
		if len([]rune(c.bal)) > wBal {
			wBal = len([]rune(c.bal))
		}
	}

	fmt.Println()
	fmt.Printf("  %s%-*s  %*s  %*s  %s%s\n",
		Fmt.Dim, wSlug, "Account", wTxs, "Txs", wBal, "Balance", "Status", Fmt.Reset)
	for _, c := range cells {
		fmt.Printf("  %s%-*s%s  %s  %s  %s\n",
			Fmt.Bold, wSlug, c.slug, Fmt.Reset,
			rightAlignAmount(c.txs, c.txsC, wTxs),
			rightAlignAmount(c.bal, c.balC, wBal),
			c.statusC)
	}
	fmt.Println()
}

// accountSyncTableData returns the tx count, computed balance and currency for
// an account's post-pull summary row.
func accountSyncTableData(acc *AccountConfig) (int, float64, string) {
	currency := accountCurrency(acc)
	totals := computeAccountTotals(acc)
	if totals == nil {
		return 0, 0, currency
	}
	if totals.Currency != "" {
		currency = totals.Currency
	}
	return totals.TxCount, totals.CurrentBalance, currency
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

// AccountsPushAll pushes every pushable account to Odoo: skips archived accounts,
// accounts with no linked Odoo journal, and odooSourceOfTruth accounts (where
// Odoo is authoritative and CHB must not push local txs in). Accounts with
// nothing new are no-ops — AccountOdooPush only writes the lines missing from
// the journal — so this is safe to run repeatedly.
func AccountsPushAll(args []string) error {
	configs := LoadAccountConfigs()
	pushed, skipped, failed := 0, 0, 0
	for i := range configs {
		acc := &configs[i]
		var reason string
		switch {
		case acc.IsArchived():
			reason = "archived"
		case acc.OdooJournalID == 0:
			reason = "no Odoo journal"
		case acc.IsOdooSourceOfTruth():
			reason = "Odoo source-of-truth"
		}
		if reason != "" {
			skipped++
			fmt.Printf("  %s⤼ %s — skipped (%s)%s\n", Fmt.Dim, acc.Slug, reason, Fmt.Reset)
			continue
		}
		fmt.Printf("\n%s━━ %s ━━%s\n", Fmt.Bold, acc.Slug, Fmt.Reset)
		if err := AccountOdooPush(acc.Slug, args); err != nil {
			failed++
			Warnf("  %s✗ %s: %v%s", Fmt.Red, acc.Slug, err, Fmt.Reset)
			continue
		}
		pushed++
	}
	fmt.Printf("\n%sPushed %d account(s)%s · %d skipped", Fmt.Bold, pushed, Fmt.Reset, skipped)
	if failed > 0 {
		fmt.Printf(" · %s%d failed%s", Fmt.Red, failed, Fmt.Reset)
	}
	fmt.Println()
	return nil
}

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
		Explicit:     true,
	}
}

func odooSyncStageFlagsExplicit(args []string) bool {
	return HasFlag(args, "--transactions") ||
		HasFlag(args, "--partners") ||
		HasFlag(args, "--accounts") ||
		HasFlag(args, "--metadata")
}

func AccountOdooPush(slug string, args []string) error {
	if HasFlag(args, "--help", "-h", "help") {
		printAccountSlugHelp(slug)
		return nil
	}
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
	if acc.IsOdooSourceOfTruth() {
		return fmt.Errorf("account '%s' is marked odooSourceOfTruth; Odoo is authoritative, so CHB will not push local transactions into journal #%d (use pull/cache commands instead)", slug, acc.OdooJournalID)
	}

	creds, err := ResolveOdooCredentials()
	if err != nil {
		return err
	}

	uid, err := odooAuth(creds.URL, creds.DB, creds.Login, creds.Password)
	if err != nil || uid == 0 {
		return wrapOdooAuthError(err)
	}

	// At end-of-push, refetch any journal caches that the mirror hook
	// flagged as needing a fresh server snapshot (create/unlink/reconcile
	// side effects we can't always mirror precisely).
	defer FlushOdooCacheRefetches(creds, uid)

	dryRun := HasFlag(args, "--dry-run")
	force := HasFlag(args, "--force")
	if !dryRun && !quietOdooContext() {
		// Skip the standalone "Odoo target: …" banner when running
		// under PushAllTargets / odooJournalsSyncAll — the outer
		// "Pushing changes — Odoo: <db>" banner already names the
		// target, and printing it here would interrupt the per-journal
		// status-line stream.
		printOdooWriteBannerOnce(creds.URL, creds.DB)
	}
	// Reconciliation policy:
	//
	//   --skip-reconcile           never reconcile after this push
	//   (default)                  reconcile when ≤ reconcileAutoThreshold new lines
	//
	// The threshold makes hourly cron pushes (typically 0–5 new lines)
	// safely auto-reconcile, while large back-fills (hundreds of lines)
	// skip the post-push reconcile — the operator runs the dedicated
	// `chb odoo journals N reconcile` verb to handle them. The inner
	// per-line "skip reconciliation" flag controls whether the provider's
	// inline reconcile step runs for each new line as it's created; we
	// always skip that and let the post-push journal-wide reconcile do it
	// in batch (decided by shouldReconcileAfterPush).
	// --skip-reconciliation is kept as a deprecated alias for
	// --skip-reconcile.
	skipReconciliation := true
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

	// Parse --until as an exclusive cutoff at the end of the requested period.
	var untilDate time.Time
	if untilStr != "" {
		t, ok := ParseDateEndExclusive(untilStr)
		if !ok {
			return fmt.Errorf("invalid --until format: %s (use %s)", untilStr, DateFormatHelp)
		}
		untilDate = t
	}

	// Parse --since: include only txs at/after this date start.
	// Setting --since (like --history) also enables the rule re-apply
	// pass over already-imported lines within the window.
	var sinceDate time.Time
	if sinceStr != "" {
		t, ok := ParseSinceDate(sinceStr)
		if !ok {
			return fmt.Errorf("invalid --since format: %s (use %s)", sinceStr, DateFormatHelp)
		}
		sinceDate = t
	}

	// --startingBalance <date>: converge the journal onto the cutoff model
	// — a manual opening entry at the date plus CHB lines from the date on.
	// The pre-push stage (below) deletes pre-cutoff CHB lines and
	// creates/corrects the opening entry from a cache-computed plan; the
	// push itself then runs windowed exactly as if odooSyncSince were
	// configured. An accompanying --since must name the same date.
	var startingBalanceDate time.Time
	if sbStr := GetOption(args, "--startingBalance", "--starting-balance"); sbStr != "" {
		t, ok := ParseSinceDate(sbStr)
		if !ok {
			return fmt.Errorf("invalid --startingBalance format: %s (use %s)", sbStr, DateFormatHelp)
		}
		if sinceStr != "" && !sinceDate.Equal(t) {
			return fmt.Errorf("--since (%s) and --startingBalance (%s) must name the same date", sinceStr, sbStr)
		}
		startingBalanceDate = t
		// Adopt the cutoff for this run (overrides any configured value) and
		// drop the explicit --since: the window now comes from the cutoff,
		// which keeps the cursor stampable and the dedup pass complete.
		//
		// --startingBalance picks the cutoff; --force picks the strategy:
		//   - without --force: converge the existing journal in place (the
		//     pre-stage below deletes pre-cutoff CHB lines and corrects the
		//     opening entry), then push windowed from the cutoff.
		//   - with --force: wipe the journal and rebuild it from local — the
		//     opening entry is recomputed at this date and every tx from the
		//     date on is re-imported. The in-place convergence pre-stage is
		//     skipped (there's nothing to converge once it's wiped).
		acc.OdooSyncSince = t.Format("2006-01-02")
		sinceStr = ""
		sinceDate = time.Time{}
	}

	useHistory := HasFlag(args, "--history") || force
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

	// Pre-push freshness check: cache must match Odoo's current count +
	// balance, otherwise we'd plan against stale target state (which could
	// silently create duplicates or write against deleted lines). --force
	// skips the check — the operator explicitly opted into rewriting the
	// journal from scratch, so cache freshness is irrelevant.
	if !force && !HasFlag(args, "--no-freshness-check") {
		if err := verifyOdooJournalCacheFresh(creds, uid, acc.OdooJournalID); err != nil {
			return err
		}
	}

	// --startingBalance pre-stage: plan the minimal convergence from the
	// (just fresh-verified) local journal cache — delete pre-cutoff CHB
	// lines, create or correct the manual opening entry — preview it, and
	// apply only after confirmation. The windowed push below then handles
	// everything from the cutoff on; no journal reset involved.
	//
	// Skipped under --force: that wipes the journal and rebuilds it (creating
	// the opening entry at acc.OdooSyncSince, which --startingBalance set
	// above), so there are no existing lines to converge.
	if !startingBalanceDate.IsZero() && !force {
		cache, ok := loadLatestOdooJournalLinesCache(acc.OdooJournalID)
		if !ok {
			if _, err := writeOdooJournalLinesCacheFullRefetch(creds, uid, acc.OdooJournalID, nil); err != nil {
				return fmt.Errorf("populate journal #%d cache: %v", acc.OdooJournalID, err)
			}
			cache, _ = loadLatestOdooJournalLinesCache(acc.OdooJournalID)
		}
		plan := planStartingBalanceConvergence(acc, startingBalanceDate, cache)
		if _, err := applyStartingBalanceConvergence(creds, uid, acc, plan, assumeYes, dryRun); err != nil {
			return err
		}
	}

	// --force: empty the entire journal first. Stripe handles this inside
	// the sync itself, so we only run the global wipe for non-Stripe paths.
	if force && !dryRun && acc.Provider != "stripe" {
		if err := emptyOdooJournal(creds, uid, acc.OdooJournalID, true); err != nil {
			return err
		}
		// Cutoff journals re-create the manual opening entry the wipe just
		// removed; the windowed push below only rebuilds post-cutoff lines.
		if cutoff, ok := acc.OdooSyncSinceTime(); ok {
			if err := createOpeningBalanceLine(creds, uid, acc, cutoff); err != nil {
				return err
			}
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
			reviewed, updated, err := syncStripeOdooMetadataStage(creds, uid, acc, effectiveSinceDate, untilDate, dryRun, assumeYes, previewLimit)
			if err != nil {
				syncErr = err
			} else if reviewed > 0 {
				summary += fmt.Sprintf(", metadata %d/%d", updated, reviewed)
			}
		}
		// Determine newLines for the reconcile auto-threshold; parse Stripe
		// upload count first so shouldReconcileAfterPush sees the right number.
		if stages.Transactions {
			syncedCount = parseStripeUploadCount(summary)
		}
		stripeCursorMatched := strings.Contains(summary, "(cursor)")
		if syncErr == nil && shouldReconcileAfterPush(args, syncedCount, "stripe") && !dryRun && !stripeCursorMatched {
			if err := odooJournalReconcile(creds, uid, acc.OdooJournalID, true, dryRun, false); err != nil {
				syncErr = err
			} else {
				summary += ", " + reconcileAfterPushSummary(creds, uid, acc.OdooJournalID, syncedCount)
			}
		}
	} else if acc.Provider == "kbcbrussels" {
		// KBC has its own dedicated push code (`merge`) that handles
		// per-row partner+IBAN resolution, the OdooMapping lookup for
		// account assignment, and the right payment_ref / narration from
		// the CSV's Description / FreeReference / StandardReference
		// fields. Routing `push` through it ensures `--reset` + push
		// produces the same clean state as `merge` against an empty
		// journal — no IBAN-as-partner-name junk, no IBAN-as-payment-ref.
		// --history is implied: merge always considers the full CSV.
		//
		// Post-merge reconcile auto-runs only for small batches; large
		// merges defer to `chb odoo journals N reconcile` explicitly,
		// for consistency. The other Stripe-only stages (--partners,
		// --accounts, --metadata) don't have a KBC equivalent and are
		// rejected.
		if stages.Explicit && (!stages.Transactions || stages.Partners || stages.Accounts || stages.Metadata) {
			return fmt.Errorf("for KBC, only --transactions is supported; run reconcile separately with `chb odoo journals %d reconcile`", acc.OdooJournalID)
		}
		var kbcCreated int
		kbcCreated, syncErr = mergeKBCJournalWithCSV(creds, uid, acc.OdooJournalID, acc, dryRun, assumeYes, false)
		syncedCount = kbcCreated
		if syncErr == nil {
			switch {
			case dryRun:
				// applyKBCMerge is short-circuited in dry-run, so
				// "merged" would be a lie. Surface what the planner
				// found instead.
				if kbcCreated > 0 {
					summary = fmt.Sprintf("dry-run: %d line%s would be merged", kbcCreated, plural(kbcCreated))
				} else {
					summary = "dry-run: nothing to merge"
				}
			case kbcCreated > 0:
				summary = fmt.Sprintf("merged %d line%s from CSV", kbcCreated, plural(kbcCreated))
			default:
				summary = "already in sync"
			}
		}
		if syncErr == nil && shouldReconcileAfterPush(args, syncedCount, "kbc") && !dryRun {
			if err := odooJournalReconcile(creds, uid, acc.OdooJournalID, true, dryRun, false); err != nil {
				syncErr = err
			} else {
				summary += ", " + reconcileAfterPushSummary(creds, uid, acc.OdooJournalID, syncedCount)
			}
		}
	} else {
		// Blockchain accounts (etherscan / monerium): --transactions for
		// the regular push, --metadata to refresh payment_ref + narration
		// on already-pushed lines from the latest local cache (useful when
		// Monerium enrichment landed AFTER the original push). --partners
		// and --accounts have no blockchain equivalent and are rejected.
		if stages.Explicit && (stages.Partners || stages.Accounts) {
			return fmt.Errorf("for this account, only --transactions and --metadata are supported; run reconcile separately with `chb odoo journals %d reconcile`", acc.OdooJournalID)
		}
		cursorMatched := false
		if !stages.Explicit || stages.Transactions {
			var result blockchainOdooSyncResult
			result, syncErr = syncBlockchainToOdoo(acc, creds, uid, monthsLimit, dryRun, skipReconciliation, sinceDate, untilDate, useHistory, previewLimit, HasFlag(args, "--reapply-partners"))
			summary = result.Summary
			syncedCount = result.Synced
			cursorMatched = result.CursorMatched
		}
		if syncErr == nil && stages.Explicit && stages.Metadata {
			reviewed, updated, err := syncBlockchainOdooMetadataStage(creds, uid, acc, effectiveSinceDate, untilDate, dryRun, assumeYes)
			if err != nil {
				syncErr = err
			} else if reviewed > 0 {
				if summary != "" {
					summary += ", "
				}
				summary += fmt.Sprintf("metadata %d/%d", updated, reviewed)
			}
		}
		if syncErr == nil && shouldReconcileAfterPush(args, syncedCount, acc.Slug) && !dryRun && (!stages.Explicit || stages.Transactions) && !cursorMatched {
			if err := odooJournalReconcile(creds, uid, acc.OdooJournalID, true, dryRun, false); err != nil {
				syncErr = err
			} else {
				summary += ", " + reconcileAfterPushSummary(creds, uid, acc.OdooJournalID, syncedCount)
			}
		}
	}

	// Mapping resolution happens at generate time (see applyOdooMapping
	// in odoo_mapping.go and the generate.go step that calls it). The
	// push paths above read tx.AccountCode and tx.PartnerID from the
	// local transactions.json files, so there's no need for a post-sync
	// walk that recomputes the mapping against existing Odoo lines.
	// After editing rules.json or odoo_mapping.json, run `chb generate`
	// to refresh the local files; the next push applies the new
	// resolutions to any line it creates. To re-apply mappings onto
	// lines that already exist in Odoo, use
	// `chb odoo journals <id> categorize`.
	reviewedCount := 0
	updatedCount := 0

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
		Progress("verifying live balance")
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
		// Prefer the local journal-lines cache for the row render — we
		// just stamped it as part of this push (either via the cursor
		// short-circuit path or the watermark refresh). Zero RPCs, ~1ms
		// vs ~500ms-1s for the 3-RPC fetchOdooJournalSnapshot fallback.
		balanceStr := "?"
		txCount := 0
		if after, ok := fetchOdooJournalSnapshotLocal(acc.OdooJournalID, accCurrency(acc)); ok {
			balanceStr = formatAccountDataBalance(after.Balance, after.Currency)
			txCount = after.TxCount
		} else if after, snapErr := fetchOdooJournalSnapshot(creds, uid, acc.OdooJournalID, accCurrency(acc)); snapErr == nil {
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
		if !dryRun && odooAfterErr == nil {
			if hint := localJournalBalanceMismatchHint(acc, localAfter, odooAfter); hint != "" {
				fmt.Print(hint)
			}
		}
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
	detail += fmt.Sprintf("    %sHint: chb accounts %s pull && chb accounts %s push --force  |  chb odoo journals %d fix%s\n",
		Fmt.Dim, acc.Slug, acc.Slug, acc.OdooJournalID, Fmt.Reset)
	if !quietOdooContext() {
		Warnf("%s", strings.TrimRight(detail, "\n"))
	}
	return live, detail
}

// localJournalBalanceMismatchHint returns a warning block when, after a
// push, the Odoo journal balance still disagrees with the local balance.
// At that point the push has already created every missing line it owns,
// so the residue is either lines chb doesn't own (manual opening balances,
// accountant adjustments — `journals fix` lists and removes them) or
// history the journal predates the local archive on (a `push --force`
// rebuild resolves it). Empty when the balances agree.
func localJournalBalanceMismatchHint(acc *AccountConfig, local, journal accountOdooSyncSnapshot) string {
	if acc == nil || acc.OdooJournalID == 0 {
		return ""
	}
	diff := roundCents(journal.Balance - local.Balance)
	if math.Abs(diff) < 0.01 {
		return ""
	}
	currency := local.Currency
	if currency == "" {
		currency = journal.Currency
	}
	hint := fmt.Sprintf("  %s⚠ Odoo journal balance %s ≠ local %s — off by %s%s\n",
		Fmt.Yellow,
		formatBalance(journal.Balance, currency),
		formatBalance(local.Balance, currency),
		formatBalance(diff, currency),
		Fmt.Reset)
	hint += fmt.Sprintf("  %sRun `chb odoo journals %d fix` to review abnormal lines (manual entries, orphans) — or `chb accounts %s push --force` to rebuild the journal from the full local archive.%s\n\n",
		Fmt.Dim, acc.OdooJournalID, acc.Slug, Fmt.Reset)
	return hint
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

// accountLocalOdooSyncSnapshotSince is accountLocalOdooSyncSnapshot windowed
// to txs at/after the cutoff — the CHB-owned side of a journal that starts
// with a manual opening entry (odooSyncSince). A zero cutoff means no window.
func accountLocalOdooSyncSnapshotSince(acc *AccountConfig, cutoff time.Time) accountOdooSyncSnapshot {
	if cutoff.IsZero() {
		return accountLocalOdooSyncSnapshot(acc)
	}
	if acc != nil && acc.Provider == "stripe" {
		if snap, ok := stripeOdooLocalSnapshotSince(acc, cutoff); ok {
			return snap
		}
	}
	var windowed []TransactionEntry
	for _, tx := range loadAccountTransactionsForOdoo(acc) {
		if tx.Timestamp >= cutoff.Unix() {
			windowed = append(windowed, tx)
		}
	}
	return accountLocalOdooSnapshot(acc, windowed)
}

// createOpeningBalanceLine writes the manual-style opening entry for a
// cutoff journal (odooSyncSince): one statement line WITHOUT a
// unique_import_id, dated at the cutoff, whose amount is the locally
// computed balance of everything before the cutoff. Used by --force
// rebuilds of non-Stripe journals (Stripe folds the same line into its
// first statement batch). A zero opening creates nothing.
func createOpeningBalanceLine(creds *OdooCredentials, uid int, acc *AccountConfig, cutoff time.Time) error {
	opening := accountLocalBalanceBefore(acc, cutoff)
	if opening == 0 {
		return nil
	}
	date := cutoff.Format("2006-01-02")
	vals := map[string]interface{}{
		"journal_id":  acc.OdooJournalID,
		"date":        date,
		"payment_ref": fmt.Sprintf("Solde de départ %s", date),
		"amount":      opening,
		"narration": fmt.Sprintf(
			"Opening balance computed by CHB from the full local history of %s: signed sum of every transaction before %s.", acc.Slug, date),
	}
	created, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.bank.statement.line", "create", []interface{}{vals}, nil)
	if err != nil {
		return fmt.Errorf("create opening balance line: %v", err)
	}
	// A freshly created statement line's move is in draft; an unposted opening
	// is invisible to the GL balance (Odoo sums only posted lines), so post it.
	if ids := parseOdooCreatedIDs(created); len(ids) > 0 {
		if err := postStatementLineMoves(creds, uid, ids); err != nil {
			Warnf("  %s⚠ Created opening entry but could not post it (post it manually in Odoo): %v%s", Fmt.Yellow, err, Fmt.Reset)
		}
	}
	odooLog("  %s✓ Created opening entry %s: %s%s\n",
		Fmt.Green, date, formatBalance(opening, accCurrency(acc)), Fmt.Reset)
	return nil
}

// postStatementLineMoves posts the account.move behind each given bank
// statement line, so the opening (or any freshly created line) actually lands
// in the posted GL rather than sitting in draft.
func postStatementLineMoves(creds *OdooCredentials, uid int, stmtLineIDs []int) error {
	if len(stmtLineIDs) == 0 {
		return nil
	}
	args := make([]interface{}, len(stmtLineIDs))
	for i, id := range stmtLineIDs {
		args[i] = id
	}
	rows, err := odooSearchReadAllMaps(creds, uid, "account.bank.statement.line",
		[]interface{}{[]interface{}{"id", "in", args}},
		[]string{"id", "move_id"}, "")
	if err != nil {
		return err
	}
	moveSet := map[int]bool{}
	moveIDs := []interface{}{}
	for _, r := range rows {
		if mid := odooFieldID(r["move_id"]); mid > 0 && !moveSet[mid] {
			moveSet[mid] = true
			moveIDs = append(moveIDs, mid)
		}
	}
	if len(moveIDs) == 0 {
		return nil
	}
	_, err = odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.move", "action_post", []interface{}{moveIDs}, nil)
	return err
}

// accountLocalBalanceBefore returns the account balance at the cutoff,
// computed from the FULL local history — the value a manual opening entry
// dated at the cutoff must carry. For Stripe every balance transaction
// contributes its net (gross + fee at the BT's own date), which is exactly
// Stripe's ledger balance at that instant; for blockchain accounts it's the
// signed sum of all transfers before the cutoff.
func accountLocalBalanceBefore(acc *AccountConfig, cutoff time.Time) float64 {
	if acc == nil || cutoff.IsZero() {
		return 0
	}
	if acc.Provider == "stripe" {
		bts, err := stripesource.LoadTransactionsSince(DataDir(), acc.AccountID, 0)
		if err != nil {
			return 0
		}
		var cents int64
		for _, bt := range bts {
			if bt.Created < cutoff.Unix() {
				cents += bt.Net
			}
		}
		return roundCents(centsToEuros(cents))
	}
	var sum float64
	for _, tx := range loadAccountTransactionsForOdoo(acc) {
		if tx.Timestamp < cutoff.Unix() {
			sum += signedOdooAmountForTransaction(acc, tx)
		}
	}
	return roundCents(sum)
}

// fetchOdooJournalSnapshotLocal builds the same snapshot from the
// local journal-lines cache — zero Odoo RPCs. Used after a push when
// the cache is known fresh (we just stamped it). Returns ok=false
// when no cache exists or it's empty, so the caller can fall back to
// the RPC version.
func fetchOdooJournalSnapshotLocal(journalID int, currency string) (accountOdooSyncSnapshot, bool) {
	snap := accountOdooSyncSnapshot{
		Label:    fmt.Sprintf("Odoo journal #%d", journalID),
		Currency: currency,
	}
	cache, ok := loadLatestOdooJournalLinesCache(journalID)
	if !ok || len(cache) == 0 {
		return snap, false
	}
	var balance float64
	var first, last time.Time
	for _, ln := range cache {
		balance += ln.Amount
		if t, err := time.Parse("2006-01-02", ln.Date); err == nil {
			if first.IsZero() || t.Before(first) {
				first = t
			}
			if t.After(last) {
				last = t
			}
		}
	}
	snap.TxCount = len(cache)
	snap.Balance = roundCents(balance)
	snap.FirstTxAt = first
	snap.LastTxAt = last
	return snap, true
}

func fetchOdooJournalSnapshot(creds *OdooCredentials, uid int, journalID int, currency string) (accountOdooSyncSnapshot, error) {
	snap := accountOdooSyncSnapshot{
		Label:    fmt.Sprintf("Odoo journal #%d", journalID),
		Currency: currency,
	}

	// Count + balance via read_group (one RPC, server-side aggregate).
	// Replaces what used to be a full paginated search_read of every line.
	count, balance, err := odooJournalAggregate(creds, uid, journalID)
	if err != nil {
		return snap, err
	}
	snap.TxCount = count
	snap.Balance = balance
	if count == 0 {
		return snap, nil
	}

	// First + last tx date — one small search_read each, limit 1.
	dateAt := func(order string) (time.Time, error) {
		result, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
			"account.bank.statement.line", "search_read",
			[]interface{}{[]interface{}{[]interface{}{"journal_id", "=", journalID}}},
			map[string]interface{}{"fields": []string{"date"}, "limit": 1, "order": order})
		if err != nil {
			return time.Time{}, err
		}
		var rows []map[string]interface{}
		if err := json.Unmarshal(result, &rows); err != nil || len(rows) == 0 {
			return time.Time{}, err
		}
		return time.Parse("2006-01-02", odooString(rows[0]["date"]))
	}
	if t, err := dateAt("date asc, id asc"); err == nil {
		snap.FirstTxAt = t
	}
	if t, err := dateAt("date desc, id desc"); err == nil {
		snap.LastTxAt = t
	}
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
	case acc.Provider == "kbcbrussels":
		return "KBC CSV"
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

	// Use the countable set (excludes EURe migration mints) so the on-chain
	// count and the per-tx comparison line up with the generated, double-count
	// -free local view; the live balanceOf already reflects the migrated state.
	rawTransfers := accountCountableTransfers(acc)
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
	// Load the primary token AND every prior contract version (priorTokens),
	// so an account that migrated contracts (e.g. EURe V1->V2) reflects the
	// full transfer history across both addresses, not just the current one.
	// Prior-token files are named with the "<symbol>-<shortContract>" file
	// token the sync writes (transactions_sync.go) — mirror that here.
	fileTokens := []string{acc.Token.Symbol}
	for _, pt := range acc.PriorTokens {
		fileTokens = append(fileTokens, pt.Symbol+"-"+etherscansource.ShortAddr(pt.Address))
	}
	dataDir := DataDir()
	var transfers []etherscansource.TokenTransfer
	// Dedup identical transfers seen under more than one contract version. The
	// EURe V1->V2 upgrade reports some on-chain transfers in BOTH the legacy
	// and the new contract's transfer list, so loading both versions would
	// otherwise count such a transfer twice. The key is scoped to this one
	// account, so an internal transfer between two of our own wallets (same
	// hash/value, opposite directions in two accounts' files) is never
	// collapsed — that's handled per-account, and this loader is per-account.
	seen := map[string]bool{}
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
			for _, ft := range fileTokens {
				path, found := etherscansource.FindFileForAddr(dataDir, yd.Name(), md.Name(), acc.Chain, acc.Slug, acc.Address, ft)
				if !found {
					continue
				}
				cache, ok := etherscansource.LoadCache(path)
				if !ok {
					continue
				}
				for _, tr := range cache.Transactions {
					k := strings.ToLower(tr.Hash) + "|" + strings.ToLower(tr.From) + "|" + strings.ToLower(tr.To) + "|" + tr.Value
					if seen[k] {
						continue
					}
					seen[k] = true
					transfers = append(transfers, tr)
				}
			}
		}
	}
	return transfers
}

// accountCountableTransfers returns the account's on-chain transfers with
// excluded transfers (e.g. EURe V1->V2 migration mints — see
// settings/excluded-transactions.json) removed, so raw counts and balances
// line up with the generated, double-count-free accounting view.
func accountCountableTransfers(acc *AccountConfig) []etherscansource.TokenTransfer {
	all := loadAccountOnchainTransfers(acc)
	if acc == nil || len(all) == 0 {
		return all
	}
	out := all[:0]
	for _, raw := range all {
		if isExcludedOnchainTx(acc.Chain, raw.Hash, raw.To) {
			continue
		}
		out = append(out, raw)
	}
	return out
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
			path, found := etherscansource.FindFileForAddr(dataDir, yd.Name(), md.Name(), acc.Chain, acc.Slug, acc.Address, acc.Token.Symbol)
			if !found {
				continue
			}
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
		// Skip excluded transfers (e.g. EURe V1->V2 migration mints) — they
		// re-issue a balance already represented by the legacy-contract
		// transfers, so counting them would double the migrated amount and
		// diverge from the live on-chain balanceOf.
		if acc != nil && isExcludedOnchainTx(acc.Chain, raw.Hash, raw.To) {
			continue
		}
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
func syncBlockchainToOdoo(acc *AccountConfig, creds *OdooCredentials, uid int, monthsLimit int, dryRun bool, skipReconciliation bool, sinceDate, untilDate time.Time, useHistory bool, previewLimit int, reapplyPartners bool) (blockchainOdooSyncResult, error) {
	localTxs := loadAccountTransactionsForOdoo(acc)
	// odooSyncSince journals hold a manual opening entry at the cutoff;
	// everything before it is represented by that entry, so the push
	// universe is windowed up front — before the cursor snapshot and the
	// dedup passes — and pre-cutoff lines are never (re-)created.
	if cutoff, ok := acc.OdooSyncSinceTime(); ok {
		var windowed []TransactionEntry
		for _, tx := range localTxs {
			if tx.Timestamp >= cutoff.Unix() {
				windowed = append(windowed, tx)
			}
		}
		localTxs = windowed
	}
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

	// Local-first cursor check: if the saved cursor says the latest
	// local tx + count matches what we last pushed AND Odoo's
	// destination aggregate still matches what we left there, there's
	// nothing to do — skip every Odoo RPC for the common "nothing new
	// since last sync" case. --history / --force bypass for explicit
	// re-checks.
	//
	// The destination check is what stops the cursor from masking
	// external edits to Odoo (lines deleted, balances rewritten by
	// someone else, partial-push gaps). Without it the same class of
	// bug as the Stripe cursor short-circuit surfaces here: cursor
	// keeps reporting "in sync" forever while Odoo silently misses
	// rows the dedup pass would have re-created.
	if !useHistory && acc.OdooJournalID != 0 && len(localTxs) > 0 && !dryRun {
		cur := LoadSyncCursor(SyncCursorKeyForOdooJournal(acc.OdooJournalID))
		if cur.LastImportID != "" && cur.Count == len(localTxs) {
			latestLocalID := buildUniqueImportID(acc, localTxs[len(localTxs)-1])
			if cur.LastImportID == latestLocalID {
				if destStillMatchesCursor(creds, uid, acc.OdooJournalID, cur) {
					return blockchainOdooSyncResult{Summary: "already in sync (cursor)", CursorMatched: true}, nil
				}
				odooLog("  %sCursor matches local but Odoo journal #%d drifted — running full push%s\n",
					Fmt.Dim, acc.OdooJournalID, Fmt.Reset)
			}
		}
	}

	// Auto-escalate to history mode when local has more txs than Odoo.
	// The cursor-based pass only looks at local txs newer than the latest
	// Odoo line, so older missing txs (e.g. a tx that was deleted in
	// Odoo, or a gap from an older partial sync) would be invisible.
	if !useHistory && acc.OdooJournalID != 0 {
		Progress("checking Odoo line count")
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
		Progress("reading latest Odoo cursor")
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
		Progress("fetching all Odoo import ids")
		existingIDs, err = fetchOdooImportIDs(creds.URL, creds.DB, uid, creds.Password, acc.OdooJournalID)
	} else {
		Progress(fmt.Sprintf("checking %d import ids against Odoo", len(localTxs)))
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
			// Reapply-partners loop: previously this ran on every sync
			// for every existing line — 2–3 RPCs per tx, sequentially,
			// turning a 0-new-line push of a 700-line journal into
			// minutes of round-trips. Now gated behind --reapply-partners
			// so the normal "nothing new" sync stays instant. The flag
			// is the right tool for a one-time backfill after a new
			// rule lands or after Monerium enrichment fills in IBANs.
			if !dryRun && reapplyPartners {
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
		// Persist cursor even on the "nothing new" path so the next
		// sync hits the cheap local-first short-circuit instead of
		// going through this full re-check.
		saveOdooJournalPushCursor(creds, uid, acc, localTxs, false)
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
	// Note: tx.AccountCode / tx.PartnerID are resolved by `chb generate`
	// via applyOdooMapping; this loop trusts them as-is and does not
	// re-run the OdooMapping lookup chain.
	for i, tx := range missing {
		Progress(fmt.Sprintf("uploading line %d/%d", i+1, len(missing)))
		t := time.Unix(tx.Timestamp, 0).In(BrusselsTZ())
		amt := signedOdooAmountForTransaction(acc, tx)

		paymentRef := buildOdooPaymentRef(tx)

		partnerEmail, _ := tx.Metadata["email"].(string)
		partnerBankID, partnerID := resolveOdooPartnerBankForTransaction(creds, uid, tx)
		if partnerID == 0 {
			partnerID = resolveOdooPartner(creds, uid, tx.Counterparty, partnerEmail, stringMetadata(tx.Metadata, "stripeCustomerId"), tx.Collective, false, partnerCache, stats)
		}

		if tx.PartnerID > 0 {
			partnerID = tx.PartnerID
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
		if tx.AccountCode != "" {
			if err := applyOdooMappingAccount(creds, uid, createdIDs, tx.AccountCode); err != nil {
				Warnf("  %s⚠ mapping account %s: %v%s", Fmt.Yellow, tx.AccountCode, err, Fmt.Reset)
			} else {
				odooLog("    %s↳ mapping: account %s%s\n", Fmt.Dim, tx.AccountCode, Fmt.Reset)
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
	// Persist the cursor so the next sync's local-first check can
	// short-circuit. Only when at least one line was actually written.
	saveOdooJournalPushCursor(creds, uid, acc, localTxs, errors > 0)
	return blockchainOdooSyncResult{Summary: summary, Synced: synced}, nil
}

// saveOdooJournalPushCursor stamps the latest local tx + count onto
// the cursor file for this journal. Skipped when the push had errors
// (partial state would mark us as "in sync" while still missing lines).
func saveOdooJournalPushCursor(creds *OdooCredentials, uid int, acc *AccountConfig, localTxs []TransactionEntry, hadErrors bool) {
	if hadErrors || len(localTxs) == 0 || acc.OdooJournalID == 0 {
		return
	}
	latest := localTxs[len(localTxs)-1]
	cursor := SyncCursor{
		Key:           SyncCursorKeyForOdooJournal(acc.OdooJournalID),
		LastImportID:  buildUniqueImportID(acc, latest),
		LastTimestamp: latest.Timestamp,
		Count:         len(localTxs),
	}
	// Stamp Odoo's post-push aggregate so the next short-circuit can
	// detect destination drift (lines deleted/edited externally between
	// syncs). One read_group RPC; same call the freshness check uses.
	if creds != nil && uid > 0 {
		if destCount, destBalance, err := odooJournalAggregate(creds, uid, acc.OdooJournalID); err == nil {
			cursor.DestCount = destCount
			cursor.DestBalanceCents = int64(math.Round(destBalance * 100))
		}
	}
	_ = SaveSyncCursor(cursor)
}

// syncBlockchainOdooMetadataStage walks every Odoo bank-statement-line
// for the account's journal (optionally narrowed to a date window) and
// re-writes payment_ref + narration from the current local cache.
//
// Use case: a line was pushed before generate had Monerium order data,
// so payment_ref ended up as the bare counterparty/address. After a
// subsequent pull+generate the local tx has the proper memo with
// invoice ref — but the Odoo line is stale. This stage closes that
// gap without re-pushing.
//
// Only payment_ref and narration are updated; partner_id and amount
// are left alone (the regular --transactions stage handles those when
// creating fresh lines).
// normalizeNarration strips Odoo's HTML wrapping (`<p>…</p>`) and
// surrounding whitespace so two narrations that differ only in markup
// compare equal. buildOdooNarration emits raw JSON; Odoo stores it
// wrapped — without this, every refresh would think every line is
// stale.
func normalizeNarration(s string) string {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "<p>")
	s = strings.TrimSuffix(s, "</p>")
	return strings.TrimSpace(s)
}

// metadataUpdatePlan is one row in the metadata-refresh plan: the
// statement-line + move ids, the old/new payment_ref values, and the
// optional narration diff. Promoted out of the function body so the
// preview helper (planChangedFields) can take it as a parameter.
type metadataUpdatePlan struct {
	LineID     int
	MoveID     int
	ImportID   string
	Old        string
	New        string
	NarrChange bool
	NarrOld    string
	NarrNew    string
}

// planChangedFields returns a compact label like "payment_ref" /
// "narration" / "payment_ref + narration" for the preview row tag.
func planChangedFields(p metadataUpdatePlan) string {
	refChange := p.New != p.Old
	switch {
	case refChange && p.NarrChange:
		return "payment_ref + narration"
	case refChange:
		return "payment_ref"
	case p.NarrChange:
		return "narration"
	}
	return "no-op"
}

func syncBlockchainOdooMetadataStage(creds *OdooCredentials, uid int, acc *AccountConfig, since, until time.Time, dryRun, assumeYes bool) (reviewed, updated int, err error) {
	// Local cache → index by unique_import_id.
	local := loadAccountTransactionsForOdoo(acc)
	localByImportID := make(map[string]TransactionEntry, len(local))
	for _, tx := range local {
		if id := buildUniqueImportID(acc, tx); id != "" {
			localByImportID[id] = tx
		}
	}
	if len(localByImportID) == 0 {
		return 0, 0, nil
	}

	// Live Odoo journal lines (with date window). is_reconciled lets us
	// skip lines that are already paid — their payment_ref no longer
	// affects the matcher, and modifying them requires unreconciling +
	// re-reconciling which is destructive (and not what --metadata is
	// for). move_id is the move we'd draft-write-post on.
	lines, err := odooSearchReadAllMaps(creds, uid, "account.bank.statement.line",
		[]interface{}{[]interface{}{"journal_id", "=", acc.OdooJournalID}},
		[]string{"id", "move_id", "date", "payment_ref", "unique_import_id", "narration", "is_reconciled"},
		"date asc, id asc")
	if err != nil {
		return 0, 0, fmt.Errorf("fetch journal lines: %v", err)
	}

	var plan []metadataUpdatePlan
	skippedReconciled := 0

	for _, row := range lines {
		dateStr := odooString(row["date"])
		if !since.IsZero() || !until.IsZero() {
			if d, derr := time.Parse("2006-01-02", dateStr); derr == nil {
				if !since.IsZero() && d.Before(since) {
					continue
				}
				if !until.IsZero() && !d.Before(until) {
					continue
				}
			}
		}
		importID := odooString(row["unique_import_id"])
		if importID == "" {
			continue
		}
		tx, ok := localByImportID[importID]
		if !ok {
			continue
		}
		reviewed++
		wantRef := buildOdooPaymentRef(tx)
		wantNarr := buildOdooNarration(acc, tx)
		curRef := odooString(row["payment_ref"])
		curNarr := odooString(row["narration"])
		refChange := wantRef != "" && wantRef != curRef
		// Odoo wraps stored narration in <p>...</p> but
		// buildOdooNarration emits raw JSON. Normalize both sides
		// before comparing so the wrapper alone doesn't classify a
		// line as stale — that produced hundreds of false positives.
		narrChange := wantNarr != "" && normalizeNarration(wantNarr) != normalizeNarration(curNarr)
		if !refChange && !narrChange {
			continue
		}
		// Already-reconciled lines: skip. Their payment_ref is no longer
		// load-bearing (the matcher filters reconciled lines out anyway),
		// and writing to them on a posted+reconciled move triggers
		// "vous ne pouvez pas supprimer une écriture comptable validée".
		// Operator should unreconcile first if they really want to retag.
		if odooBool(row["is_reconciled"]) {
			skippedReconciled++
			continue
		}
		plan = append(plan, metadataUpdatePlan{
			LineID:     odooInt(row["id"]),
			MoveID:     odooFieldID(row["move_id"]),
			ImportID:   importID,
			Old:        curRef,
			New:        wantRef,
			NarrChange: narrChange,
			NarrOld:    curNarr,
			NarrNew:    wantNarr,
		})
	}

	// Bucket the plan by which attribute(s) actually change so the
	// preview header tells the operator exactly what's about to move.
	refOnly, narrOnly, both := 0, 0, 0
	for _, p := range plan {
		refChange := p.New != p.Old
		switch {
		case refChange && p.NarrChange:
			both++
		case refChange:
			refOnly++
		case p.NarrChange:
			narrOnly++
		}
	}

	if len(plan) == 0 {
		hint := ""
		if skippedReconciled > 0 {
			hint = fmt.Sprintf(" (%d already-reconciled line%s skipped)", skippedReconciled, plural(skippedReconciled))
		}
		fmt.Printf("  %s↻ metadata stage: %d line%s reviewed, none stale.%s%s\n",
			Fmt.Dim, reviewed, plural(reviewed), hint, Fmt.Reset)
		return reviewed, 0, nil
	}

	skipHint := ""
	if skippedReconciled > 0 {
		skipHint = fmt.Sprintf(", %d already-reconciled skipped", skippedReconciled)
	}
	fmt.Printf("\n  %s↻ Metadata refresh — %d reviewed, %d stale%s%s\n",
		Fmt.Bold, reviewed, len(plan), skipHint, Fmt.Reset)
	fmt.Printf("  %sWill update on each stale line (draft → write → repost):%s\n", Fmt.Dim, Fmt.Reset)
	if refOnly+both > 0 {
		fmt.Printf("    %s• payment_ref%s on %d line%s\n",
			Fmt.Yellow, Fmt.Reset, refOnly+both, plural(refOnly+both))
	}
	if narrOnly+both > 0 {
		fmt.Printf("    %s• narration%s on %d line%s\n",
			Fmt.Yellow, Fmt.Reset, narrOnly+both, plural(narrOnly+both))
	}
	fmt.Println()

	previewLimit := 5
	if dryRun {
		previewLimit = len(plan)
	}
	for i, p := range plan {
		if i >= previewLimit {
			break
		}
		fields := planChangedFields(p)
		oldRef := truncate(p.Old, 50)
		newRef := truncate(p.New, 50)
		fmt.Printf("    %sline #%d%s  [%s]\n",
			Fmt.Dim, p.LineID, Fmt.Reset, fields)
		if p.New != p.Old {
			fmt.Printf("      %spayment_ref:%s %s%s%s → %s%s%s\n",
				Fmt.Dim, Fmt.Reset,
				Fmt.Yellow, oldRef, Fmt.Reset,
				Fmt.Green, newRef, Fmt.Reset)
		}
		if p.NarrChange {
			fmt.Printf("      %snarration:%s  %s%s%s → %s%s%s\n",
				Fmt.Dim, Fmt.Reset,
				Fmt.Yellow, truncate(p.NarrOld, 50), Fmt.Reset,
				Fmt.Green, truncate(p.NarrNew, 50), Fmt.Reset)
		}
	}
	if len(plan) > previewLimit {
		fmt.Printf("    %s… and %d more (--verbose / --dry-run to see all)%s\n",
			Fmt.Dim, len(plan)-previewLimit, Fmt.Reset)
	}

	if dryRun {
		fmt.Printf("\n  %s(dry-run — no writes)%s\n", Fmt.Dim, Fmt.Reset)
		return reviewed, 0, nil
	}

	if !assumeYes && isInteractiveTTY() {
		fmt.Printf("\n  %sApply %d update%s to %s? [Y/n] %s",
			Fmt.Bold, len(plan), plural(len(plan)), acc.Slug, Fmt.Reset)
		reader := bufio.NewReader(os.Stdin)
		resp, _ := reader.ReadString('\n')
		resp = strings.TrimSpace(strings.ToLower(resp))
		if resp == "n" || resp == "no" {
			fmt.Println("  Aborted.")
			return reviewed, 0, nil
		}
	}

	// Apply: each unreconciled bank line lives on a posted move; writing
	// payment_ref triggers an Odoo recompute that needs to unlink+recreate
	// the underlying move lines. On a posted move that fails with
	// "vous ne pouvez pas supprimer une écriture comptable validée".
	// Draft → write → repost is the standard workaround, same shape as
	// applyOdooMappingAccount uses for counterpart-account rewrites.
	//
	// Fetch the current state of every move up front so we don't try to
	// button_draft a move that's already in draft (Odoo rejects that
	// with "Seules les pièces comptabilisées/annulées peuvent être
	// remises en brouillon"). For draft moves we write directly and
	// only post when the move was posted to begin with.
	//
	// Per-line progress: each iteration costs 1–3 RPCs (write, optional
	// draft + post), so a 200-line batch easily takes 30s+. Without a live
	// counter the loop looks frozen — surface it on stderr.
	moveStates := map[int]string{}
	moveIDs := make([]interface{}, 0, len(plan))
	seenMoves := map[int]bool{}
	for _, p := range plan {
		if p.MoveID == 0 || seenMoves[p.MoveID] {
			continue
		}
		seenMoves[p.MoveID] = true
		moveIDs = append(moveIDs, p.MoveID)
	}
	if len(moveIDs) > 0 {
		rows, err := odooSearchReadAllMaps(creds, uid, "account.move",
			[]interface{}{[]interface{}{"id", "in", moveIDs}},
			[]string{"id", "state"}, "")
		if err != nil {
			return reviewed, 0, fmt.Errorf("fetch move states: %v", err)
		}
		for _, row := range rows {
			id := odooInt(row["id"])
			if id > 0 {
				moveStates[id] = odooString(row["state"])
			}
		}
	}

	status := newStatusLine()
	defer status.Clear()
	failed := 0
	for i, p := range plan {
		status.Update("Metadata refresh %d/%d (%d ok, %d failed)…", i+1, len(plan), updated, failed)
		if p.MoveID == 0 {
			Warnf("    %s⚠ line #%d: missing move_id; skipped%s", Fmt.Yellow, p.LineID, Fmt.Reset)
			failed++
			continue
		}
		state := moveStates[p.MoveID]
		switch state {
		case "draft":
			// Already in draft: write directly, don't try to draft again.
		case "posted":
			if _, werr := odooExec(creds.URL, creds.DB, uid, creds.Password,
				"account.move", "button_draft",
				[]interface{}{[]interface{}{p.MoveID}}, nil); werr != nil {
				Warnf("    %s⚠ line #%d draft: %v%s", Fmt.Yellow, p.LineID, werr, Fmt.Reset)
				failed++
				continue
			}
		case "cancel":
			// Cancelled moves can be drafted back, but writing to them is
			// usually not what the operator intended — surface and skip.
			Warnf("    %s⚠ line #%d: move is cancelled; skipping%s", Fmt.Yellow, p.LineID, Fmt.Reset)
			failed++
			continue
		default:
			// Unknown state — try the legacy draft → write → post sequence
			// and let Odoo's error surface naturally.
			if _, werr := odooExec(creds.URL, creds.DB, uid, creds.Password,
				"account.move", "button_draft",
				[]interface{}{[]interface{}{p.MoveID}}, nil); werr != nil {
				Warnf("    %s⚠ line #%d draft (state=%q): %v%s", Fmt.Yellow, p.LineID, state, werr, Fmt.Reset)
				failed++
				continue
			}
		}
		patch := map[string]interface{}{"payment_ref": p.New}
		if p.NarrChange {
			patch["narration"] = p.NarrNew
		}
		writeErr := func() error {
			_, e := odooExec(creds.URL, creds.DB, uid, creds.Password,
				"account.bank.statement.line", "write",
				[]interface{}{[]interface{}{p.LineID}, patch}, nil)
			return e
		}()
		// Re-post only when we left the move posted to begin with —
		// otherwise we'd promote a draft move the operator didn't want
		// posted. Always run the post even if the write failed, so we
		// don't leave a move stuck in draft due to a partial run.
		if state == "posted" {
			if _, postErr := odooExec(creds.URL, creds.DB, uid, creds.Password,
				"account.move", "action_post",
				[]interface{}{[]interface{}{p.MoveID}}, nil); postErr != nil {
				Warnf("    %s⚠ line #%d repost: %v%s", Fmt.Yellow, p.LineID, postErr, Fmt.Reset)
				failed++
				continue
			}
		}
		if writeErr != nil {
			Warnf("    %s⚠ line #%d write: %v%s", Fmt.Yellow, p.LineID, writeErr, Fmt.Reset)
			failed++
			continue
		}
		updated++
	}
	status.Clear()
	mark := Fmt.Green + "✓" + Fmt.Reset
	if failed > 0 {
		mark = Fmt.Yellow + "⚠" + Fmt.Reset
	}
	fmt.Printf("  %s %d/%d updated", mark, updated, len(plan))
	if failed > 0 {
		fmt.Printf(" (%s%d failed%s)", Fmt.Red, failed, Fmt.Reset)
	}
	fmt.Println()
	// Refresh local journal cache so subsequent dry-run / reconcile
	// passes see the new payment_refs.
	if updated > 0 {
		if _, rerr := writeOdooJournalLinesCache(creds, uid, acc.OdooJournalID); rerr != nil {
			Warnf("    %s⚠ journal cache refresh: %v%s", Fmt.Yellow, rerr, Fmt.Reset)
		}
	}
	return reviewed, updated, nil
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

	plan := make([]odooSyncPlanRow, 0, len(txs))
	for _, tx := range txs {
		t := time.Unix(tx.Timestamp, 0).In(BrusselsTZ())
		importID := buildUniqueImportID(acc, tx)
		amount := signedOdooAmountForTransaction(acc, tx)
		paymentRef := buildOdooPaymentRef(tx)
		action := "create"
		reason := ""
		account := tx.AccountCode

		if tx.PartnerID > 0 {
			reason = fmt.Sprintf("rule partner #%d", tx.PartnerID)
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
			[]string{"id", "date", "payment_ref", "narration", "partner_id", "partner_bank_id", "amount", "unique_import_id", "journal_id", "move_id", "statement_id", "is_reconciled", "create_date", "write_date"},
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
	if tx.Type == "INTERNAL" {
		ref := "Internal transfer"
		if tx.Counterparty != "" {
			ref += ": " + tx.Counterparty
		}
		return ref
	}
	// Prefer description — it's where the memo with the invoice ref
	// lives, and that's what the reconcile matcher's strategy 1 needs.
	// Counterparty is intentionally NOT concatenated: partner_id on
	// the bank line already carries the customer identity, so repeating
	// the name in payment_ref just clutters the reconcile-i view ("RCR?
	// — mem/2026/00059" duplicates the Counterparty column). Falls
	// back to counterparty only when there's no description at all.
	if desc := strings.TrimSpace(txDisplayDescription(tx)); desc != "" {
		return desc
	}
	if cp := strings.TrimSpace(tx.Counterparty); cp != "" {
		return cp
	}
	paymentRef := strings.ToLower(tx.Type)
	if tx.Currency != "" {
		paymentRef += " " + tx.Currency
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
	if !isMoneriumTransaction(tx) {
		return nil
	}
	currentRef := strings.TrimSpace(odooString(row["payment_ref"]))
	paymentRef := strings.TrimSpace(buildOdooPaymentRef(tx))
	if paymentRef == "" || strings.HasPrefix(strings.ToLower(paymentRef), "0x") {
		return nil
	}
	// Only refresh a payment_ref that looks auto-generated. We never
	// overwrite an arbitrary string a human (or another Odoo flow)
	// could have written deliberately. "Safe to replace" cases:
	//   - empty
	//   - zero-address placeholder ("EURe from 0x0000...0000") — legacy
	//     raw-blockchain ingest before the Monerium processor ran
	//   - the counterparty name verbatim — legacy upload where the
	//     Monerium order memo wasn't yet enriched at upload time, so
	//     buildOdooPaymentRef fell back to tx.Counterparty
	//   - the bare type fallback ("burn EURe", "mint EURe" = lowercase
	//     type + currency) — a redeem/issue line uploaded before its
	//     Monerium order (memo + IBAN) existed locally, so buildOdooPaymentRef
	//     hit its final fallback. Now that the memo is enriched, refresh it.
	typeFallbackRef := strings.TrimSpace(strings.ToLower(tx.Type))
	if tx.Currency != "" {
		typeFallbackRef = strings.TrimSpace(typeFallbackRef + " " + tx.Currency)
	}
	safeToReplace := currentRef == "" ||
		isZeroAddressPaymentRef(currentRef) ||
		strings.EqualFold(currentRef, strings.TrimSpace(tx.Counterparty)) ||
		(typeFallbackRef != "" && strings.EqualFold(currentRef, typeFallbackRef))
	if !safeToReplace {
		return nil
	}
	if strings.EqualFold(currentRef, paymentRef) {
		return nil
	}
	update := map[string]interface{}{"payment_ref": paymentRef}
	if narr := buildOdooNarration(acc, tx); narr != "" && odooString(row["narration"]) != narr {
		update["narration"] = narr
	}
	return update
}

// isMoneriumTransaction reports whether a tx is a Monerium-enriched
// blockchain tx (either via the "source:monerium" tag or via the
// processor stamping moneriumKind into metadata). Shared between the
// line-sync planner (which uses it to scope which Odoo lines to fetch)
// and the per-line builder (which uses it as the safety gate before
// refreshing payment_ref).
func isMoneriumTransaction(tx TransactionEntry) bool {
	return transactionHasTag(tx, []string{"source", "monerium"}) ||
		stringMetadata(tx.Metadata, "moneriumKind") != ""
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
	// Skip archived partners — the write would fail with a confusing
	// "(Record: res.partner(N,), User: M)" access-error even though the
	// real cause is that Odoo's ir.rules hide archived records from
	// modification by non-admin users. We use the local partner cache
	// to avoid a per-call existence probe; if the cache is missing,
	// we let the write proceed and surface the error normally.
	if idx := loadLatestOdooPartnerIndex(DataDir()); idx != nil {
		if _, ok := idx.byID[partnerID]; !ok {
			return nil
		}
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
	txs := loadAccountTransactionsWithOptions(acc, true)
	// Enrich with the Odoo-specific resolution (account_code / partner_id)
	// computed at generate time. The public transactions.json doesn't
	// carry it anymore — it lives in providers/odoo/pending/.
	PopulateOdooPending(DataDir(), txs)
	return txs
}

func loadAccountTransactionsWithOptions(acc *AccountConfig, includeInternal bool) []TransactionEntry {
	dataDir := DataDir()
	// kbcbrussels has a single rolling CSV in latest/providers/kbcbrussels/
	// — the per-month generated files only exist when the operator (or
	// another provider) has triggered generation for that month. Read the
	// CSV directly so the account view always sees every row, regardless
	// of which months have been generated.
	if acc != nil && acc.Provider == "kbcbrussels" && acc.IBAN != "" {
		return loadKBCAccountTransactions(acc)
	}
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
	// Last resort: trust the direction the generator already stamped on the
	// stored amount. A migrated/multi-contract wallet's legacy-contract
	// transfers live in a priorTokens file the raw lookup above may still
	// miss; falling through to a blanket "CREDIT" counted every unresolved
	// internal transfer as incoming, inflating the account balance (a drained
	// wallet showed 0 on-chain but +121k locally — ~90 outgoing internals
	// scored as credits). NormalizedAmount/Amount is already direction-signed,
	// so a negative value is an outflow.
	amount := tx.NormalizedAmount
	if amount == 0 {
		amount = tx.Amount
	}
	if amount < 0 {
		return "DEBIT"
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
	// Try the primary token's file first, then every prior contract version
	// (priorTokens) — a migrated wallet's older transfers live in files named
	// "<symbol>-<shortContract>" (see loadAccountOnchainTransfers), so looking
	// up only the primary symbol misses legacy-contract internal transfers.
	fileTokens := []string{currency}
	if acc != nil {
		for _, pt := range acc.PriorTokens {
			fileTokens = append(fileTokens, pt.Symbol+"-"+etherscansource.ShortAddr(pt.Address))
		}
	}
	for _, ft := range fileTokens {
		path, found := etherscansource.FindFileForAddr(DataDir(), t.Format("2006"), t.Format("01"), chain, slug, account, ft)
		if !found {
			continue
		}
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		var txFile struct {
			Transactions []struct {
				Hash string `json:"hash"`
				From string `json:"from"`
				To   string `json:"to"`
			} `json:"transactions"`
		}
		if json.Unmarshal(data, &txFile) != nil {
			continue
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
		}
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

	if acc.Provider == "kbcbrussels" {
		iban := strings.ToLower(strings.ReplaceAll(strings.TrimSpace(acc.IBAN), " ", ""))
		if iban == "" {
			iban = strings.ToLower(acc.Slug)
		}
		hash := tx.TxHash
		if hash == "" {
			hash = tx.ID
		}
		return fmt.Sprintf("kbcbrussels:%s:%s", iban, strings.ToLower(hash))
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

	fmt.Printf("  %s%schb accounts %s pull%s\n", f.Bold, f.Cyan, slug, f.Reset)
	if acc.Provider == "stripe" {
		fmt.Printf("    %sPull Stripe balance transactions into the local cache.%s\n", f.Dim, f.Reset)
	} else if acc.Provider == "kbcbrussels" {
		fmt.Printf("    %sRe-read the local KBC CSV under latest/providers/kbcbrussels/ and regenerate.%s\n", f.Dim, f.Reset)
	} else {
		fmt.Printf("    %sFetch transactions from the source provider into the local cache.%s\n", f.Dim, f.Reset)
	}
	fmt.Println()

	fmt.Printf("  %s%schb accounts %s push%s\n", f.Bold, f.Cyan, slug, f.Reset)
	fmt.Printf("    %sPush local transactions into the linked Odoo journal (see also: chb odoo journals <id> push)%s\n", f.Dim, f.Reset)
	fmt.Println()

	fmt.Printf("  %s%schb accounts %s link%s\n", f.Bold, f.Cyan, slug, f.Reset)
	fmt.Printf("    %sLink this account to an Odoo bank journal%s\n", f.Dim, f.Reset)
	fmt.Println()

	// Show pull options
	fmt.Printf("%sPULL OPTIONS%s\n\n", f.Bold, f.Reset)
	fmt.Printf("  %s--history%s          Pull from the earliest cached month (full re-fetch)\n", f.Yellow, f.Reset)
	fmt.Printf("  %s--since <date>%s     Pull from this date onwards\n", f.Yellow, f.Reset)
	fmt.Printf("  %s--force%s            Re-fetch even if cached data exists\n", f.Yellow, f.Reset)
	fmt.Printf("  %s--verbose, -v%s      Surface inner sync progress instead of compact view\n", f.Yellow, f.Reset)
	fmt.Println()

	// Show push options
	fmt.Printf("%sPUSH OPTIONS%s\n\n", f.Bold, f.Reset)
	fmt.Printf("  %s--dry-run%s          Preview what would be pushed\n", f.Yellow, f.Reset)
	fmt.Printf("  %s--force%s            Empty the journal first, then re-push everything\n", f.Yellow, f.Reset)
	fmt.Printf("  %s--skip-reconcile%s   Skip the post-push reconcile (auto-runs on small batches by default)\n", f.Yellow, f.Reset)
	fmt.Printf("  %s--until <date>%s     Stop processing at this date end\n", f.Yellow, f.Reset)
	if acc.Provider == "stripe" {
		fmt.Printf("  %sStripe-only stage flags:%s\n", f.Dim, f.Reset)
		fmt.Printf("  %s--transactions%s     Import statement lines/statements/fees\n", f.Yellow, f.Reset)
		fmt.Printf("  %s--partners%s         Link/create partners and collective tags\n", f.Yellow, f.Reset)
		fmt.Printf("  %s--accounts%s         Apply account rules to existing journal lines\n", f.Yellow, f.Reset)
		fmt.Printf("  %s--metadata%s         Refresh descriptions and narration metadata\n", f.Yellow, f.Reset)
	}
	fmt.Println()

	// Show examples
	fmt.Printf("%sEXAMPLES%s\n\n", f.Bold, f.Reset)
	if acc.Provider == "stripe" {
		fmt.Printf("  %s$ chb accounts %s payouts%s\n", f.Dim, slug, f.Reset)
	}
	fmt.Printf("  %s$ chb accounts %s pull%s\n", f.Dim, slug, f.Reset)
	fmt.Printf("  %s$ chb accounts %s pull --since 2026-01-01%s\n", f.Dim, slug, f.Reset)
	fmt.Printf("  %s$ chb accounts %s push --dry-run%s\n", f.Dim, slug, f.Reset)
	fmt.Printf("  %s$ chb odoo journals <id> reconcile%s\n", f.Dim, f.Reset)
	fmt.Println()
}

func printAccountsHelp() {
	f := Fmt
	fmt.Printf(`
%schb accounts%s — Manage finance accounts (bank/payment accounts only)

%sUSAGE%s
  %schb accounts%s                          One line per account (slug + balance), local files only
  %schb accounts --details%s                Full per-account view (sync status, Odoo, etc.)
  %schb accounts --live%s                   Fetch live balances from the network (alias: -r)
  %schb accounts pull%s                     Pull all accounts from their source providers
  %schb accounts <slug> pull%s              Pull one account
  %schb accounts <slug> pull --history%s    Pull from earliest cached month
  %schb accounts <slug> pull --since <date>%s   Pull from a specific date onwards
  %schb accounts <slug> push%s              Push local transactions → linked Odoo journal
  %schb accounts <slug> push --dry-run%s    Preview what would be pushed
  %schb accounts <slug> push --force%s      Empty the journal first, then re-push
  %schb accounts <slug> link%s              Link account to an Odoo bank journal
  %schb accounts balance [YYYY[/MM[/DD]]]%s          All accounts + total at end of period
  %schb accounts <slug> balance [YYYY[/MM[/DD]]]%s   Historical balance at end of period
  %schb accounts <slug> payouts%s           List Stripe payouts
  %schb accounts internal%s                 Audit internal-transfer legs (must net to zero)

  %sNote%s: for the full loop across all accounts + targets, use 'chb sync'.

%sENVIRONMENT%s
  %sODOO_URL%s            Odoo instance URL
  %sODOO_LOGIN%s          Odoo login email
  %sODOO_PASSWORD%s       Odoo password or API key
`,
		f.Bold, f.Reset, // title
		f.Bold, f.Reset, // USAGE
		// 14 USAGE rows
		f.Cyan, f.Reset, f.Cyan, f.Reset, f.Cyan, f.Reset, f.Cyan, f.Reset, f.Cyan, f.Reset,
		f.Cyan, f.Reset, f.Cyan, f.Reset, f.Cyan, f.Reset, f.Cyan, f.Reset, f.Cyan, f.Reset,
		f.Cyan, f.Reset, f.Cyan, f.Reset, f.Cyan, f.Reset, f.Cyan, f.Reset, f.Cyan, f.Reset,
		f.Bold, f.Reset, // Note word
		f.Bold, f.Reset, // ENVIRONMENT
		f.Yellow, f.Reset, f.Yellow, f.Reset, f.Yellow, f.Reset,
	)
}
