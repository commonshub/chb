package cmd

import (
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"
)

var etherscanAPIBaseURL = "https://api.etherscan.io/v2/api"

var evmAddressPattern = regexp.MustCompile(`^0x[0-9a-fA-F]{40}$`)

type balanceTarget struct {
	Chain   string
	Token   string
	Address string
}

type balanceResolvedToken struct {
	Chain    string
	ChainID  int
	Address  string
	Symbol   string
	Decimals int
}

type balanceToolRequest struct {
	Chain        string
	ChainID      int
	TokenAddress string
	TokenSymbol  string
	Decimals     int
	Wallet       string
	Date         string
	APIKey       string
}

type balanceToolResult struct {
	Chain        string
	ChainID      int
	TokenAddress string
	TokenSymbol  string
	Wallet       string
	Date         string
	Block        string
	Raw          string
	Balance      string
}

func BalanceTool(args []string) error {
	if len(args) == 0 || HasFlag(args, "--help", "-h", "help") {
		PrintBalanceToolHelp()
		return nil
	}
	if len(args) > 2 {
		return fmt.Errorf("usage: chb tool balance $chain:$token:$address [date]\nExample: chb tool balance gnosis:EURe:0xabc... 2026-05-23")
	}
	target, err := parseBalanceTarget(args[0])
	if err != nil {
		return err
	}
	date := ""
	if len(args) == 2 {
		date = strings.TrimSpace(args[1])
	}
	token, err := resolveBalanceToken(target.Chain, target.Token, LoadTokenConfigs())
	if err != nil {
		return err
	}
	apiKey := os.Getenv("ETHERSCAN_API_KEY")
	if apiKey == "" {
		apiKey = os.Getenv("GNOSISSCAN_API_KEY")
	}
	if apiKey == "" {
		return fmt.Errorf("ETHERSCAN_API_KEY not set")
	}
	result, err := fetchBalanceToolResult(balanceToolRequest{
		Chain:        target.Chain,
		ChainID:      token.ChainID,
		TokenAddress: token.Address,
		TokenSymbol:  token.Symbol,
		Decimals:     token.Decimals,
		Wallet:       target.Address,
		Date:         date,
		APIKey:       apiKey,
	})
	if err != nil {
		return err
	}
	printBalanceToolResult(result)
	return nil
}

func parseBalanceTarget(raw string) (balanceTarget, error) {
	parts := strings.Split(strings.TrimSpace(raw), ":")
	var target balanceTarget
	switch len(parts) {
	case 2:
		target = balanceTarget{Chain: "ethereum", Token: strings.TrimSpace(parts[0]), Address: strings.TrimSpace(parts[1])}
	case 3:
		target = balanceTarget{Chain: normalizeChainSlug(parts[0]), Token: strings.TrimSpace(parts[1]), Address: strings.TrimSpace(parts[2])}
	default:
		return target, fmt.Errorf("invalid balance target %q; expected token:address or chain:token:address", raw)
	}
	if target.Chain == "" {
		target.Chain = "ethereum"
	}
	if target.Token == "" {
		return target, fmt.Errorf("token symbol/address is required")
	}
	if !evmAddressPattern.MatchString(target.Address) {
		return target, fmt.Errorf("invalid wallet address: %s", target.Address)
	}
	return target, nil
}

func resolveBalanceToken(chain, tokenRef string, configs []TokenConfig) (balanceResolvedToken, error) {
	chain = normalizeChainSlug(chain)
	tokenRef = strings.TrimSpace(tokenRef)
	if chain == "" {
		chain = "ethereum"
	}
	if evmAddressPattern.MatchString(tokenRef) {
		decimals := 18
		symbol := tokenRef
		for _, cfg := range configs {
			if strings.EqualFold(cfg.Chain, chain) && strings.EqualFold(cfg.Address, tokenRef) {
				if cfg.Decimals != 0 {
					decimals = cfg.Decimals
				}
				symbol = cfg.Symbol
				break
			}
		}
		return balanceResolvedToken{Chain: chain, ChainID: chainIDForBalanceChain(chain, configs), Address: tokenRef, Symbol: symbol, Decimals: decimals}, nil
	}
	for _, cfg := range configs {
		if strings.EqualFold(cfg.Chain, chain) && strings.EqualFold(cfg.Symbol, tokenRef) && cfg.Address != "" {
			decimals := cfg.Decimals
			if decimals == 0 {
				decimals = 18
			}
			return balanceResolvedToken{Chain: chain, ChainID: firstNonZeroInt(cfg.ChainID, chainIDForBalanceChain(chain, configs)), Address: cfg.Address, Symbol: cfg.Symbol, Decimals: decimals}, nil
		}
	}
	if builtin, ok := builtinBalanceToken(chain, tokenRef); ok {
		return builtin, nil
	}
	return balanceResolvedToken{}, fmt.Errorf("unknown token %q on %s; use a token contract address or add it to settings/tokens.json", tokenRef, chain)
}

func fetchBalanceToolResult(req balanceToolRequest) (balanceToolResult, error) {
	if req.ChainID == 0 {
		return balanceToolResult{}, fmt.Errorf("unknown chain id for %s", req.Chain)
	}
	if !evmAddressPattern.MatchString(req.TokenAddress) {
		return balanceToolResult{}, fmt.Errorf("invalid token address: %s", req.TokenAddress)
	}
	if !evmAddressPattern.MatchString(req.Wallet) {
		return balanceToolResult{}, fmt.Errorf("invalid wallet address: %s", req.Wallet)
	}
	tag := "latest"
	dateLabel := "latest"
	if strings.TrimSpace(req.Date) != "" && !strings.EqualFold(strings.TrimSpace(req.Date), "today") {
		ts, err := balanceDateTimestamp(req.Date)
		if err != nil {
			return balanceToolResult{}, err
		}
		block, err := fetchEtherscanBlockByTime(req.ChainID, ts, req.APIKey)
		if err != nil {
			return balanceToolResult{}, err
		}
		tag = block
		dateLabel = req.Date
	}
	raw, err := fetchEtherscanTokenBalance(req.ChainID, req.TokenAddress, req.Wallet, tag, req.APIKey, tag != "latest")
	if err != nil {
		return balanceToolResult{}, err
	}
	balance, err := formatRawTokenBalance(raw, req.Decimals)
	if err != nil {
		return balanceToolResult{}, err
	}
	return balanceToolResult{Chain: req.Chain, ChainID: req.ChainID, TokenAddress: req.TokenAddress, TokenSymbol: req.TokenSymbol, Wallet: req.Wallet, Date: dateLabel, Block: tag, Raw: raw, Balance: balance}, nil
}

func fetchEtherscanBlockByTime(chainID int, timestamp int64, apiKey string) (string, error) {
	values := url.Values{}
	values.Set("chainid", fmt.Sprintf("%d", chainID))
	values.Set("module", "block")
	values.Set("action", "getblocknobytime")
	values.Set("timestamp", fmt.Sprintf("%d", timestamp))
	values.Set("closest", "before")
	values.Set("apikey", apiKey)
	var out struct {
		Status  string `json:"status"`
		Message string `json:"message"`
		Result  string `json:"result"`
	}
	if err := fetchEtherscanJSON(values, &out); err != nil {
		return "", err
	}
	if out.Status != "1" || out.Result == "" {
		return "", fmt.Errorf("etherscan block lookup failed: %s", firstNonEmptyStr(out.Message, out.Result))
	}
	return out.Result, nil
}

func fetchEtherscanTokenBalance(chainID int, tokenAddress, walletAddress, tag, apiKey string, historical bool) (string, error) {
	values := url.Values{}
	values.Set("chainid", fmt.Sprintf("%d", chainID))
	values.Set("module", "account")
	if historical {
		values.Set("action", "tokenbalancehistory")
		values.Set("blockno", tag)
	} else {
		values.Set("action", "tokenbalance")
		values.Set("tag", tag)
	}
	values.Set("contractaddress", tokenAddress)
	values.Set("address", walletAddress)
	values.Set("apikey", apiKey)
	var out struct {
		Status  string `json:"status"`
		Message string `json:"message"`
		Result  string `json:"result"`
	}
	if err := fetchEtherscanJSON(values, &out); err != nil {
		return "", err
	}
	if out.Status != "1" {
		return "", fmt.Errorf("etherscan token balance failed: %s", firstNonEmptyStr(out.Message, out.Result))
	}
	return out.Result, nil
}

func fetchEtherscanJSON(values url.Values, out interface{}) error {
	u := etherscanAPIBaseURL + "?" + values.Encode()
	resp, err := http.Get(u)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("etherscan HTTP %d", resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func balanceDateTimestamp(raw string) (int64, error) {
	raw = strings.TrimSpace(raw)
	if spec, ok := ParseDateValue(raw); ok {
		return spec.End.Add(-time.Second).Unix(), nil
	}
	if t, err := time.Parse(time.RFC3339, raw); err == nil {
		return t.Unix(), nil
	}
	return 0, fmt.Errorf("invalid date %q; use %s or RFC3339", raw, DateFormatHelp)
}

func formatRawTokenBalance(raw string, decimals int) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("empty token balance")
	}
	i := new(big.Int)
	var ok bool
	if strings.HasPrefix(raw, "0x") || strings.HasPrefix(raw, "0X") {
		i, ok = new(big.Int).SetString(raw[2:], 16)
	} else {
		i, ok = new(big.Int).SetString(raw, 10)
	}
	if !ok {
		return "", fmt.Errorf("invalid token balance: %s", raw)
	}
	if decimals < 0 {
		decimals = 0
	}
	divisor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil)
	whole := new(big.Int).Quo(i, divisor).String()
	frac := new(big.Int).Mod(i, divisor).String()
	if decimals == 0 {
		return whole, nil
	}
	if len(frac) < decimals {
		frac = strings.Repeat("0", decimals-len(frac)) + frac
	}
	frac = strings.TrimRight(frac, "0")
	if frac == "" {
		return whole, nil
	}
	return whole + "." + frac, nil
}

func printBalanceToolResult(result balanceToolResult) {
	fmt.Printf("%s %s balance for %s on %s", result.Balance, result.TokenSymbol, result.Wallet, result.Chain)
	if result.Date != "" && result.Date != "latest" {
		fmt.Printf(" at %s", result.Date)
	}
	fmt.Println()
	fmt.Printf("  Token: %s\n", result.TokenAddress)
	fmt.Printf("  Chain ID: %d\n", result.ChainID)
	fmt.Printf("  Block: %s\n", result.Block)
	fmt.Printf("  Raw: %s\n", result.Raw)
}

func normalizeChainSlug(chain string) string {
	chain = strings.ToLower(strings.TrimSpace(chain))
	chain = strings.ReplaceAll(chain, "_", "-")
	switch chain {
	case "", "eth", "mainnet":
		return "ethereum"
	case "gno", "xdai":
		return "gnosis"
	case "cell":
		return "celo"
	default:
		return chain
	}
}

func chainIDForBalanceChain(chain string, configs []TokenConfig) int {
	chain = normalizeChainSlug(chain)
	for _, cfg := range configs {
		if strings.EqualFold(cfg.Chain, chain) && cfg.ChainID != 0 {
			return cfg.ChainID
		}
	}
	switch chain {
	case "ethereum":
		return 1
	case "gnosis":
		return 100
	case "celo":
		return 42220
	case "polygon":
		return 137
	case "base":
		return 8453
	case "arbitrum":
		return 42161
	case "optimism":
		return 10
	default:
		return 0
	}
}

func builtinBalanceToken(chain, symbol string) (balanceResolvedToken, bool) {
	chain = normalizeChainSlug(chain)
	symbolKey := strings.ToUpper(strings.TrimSpace(symbol))
	builtins := []balanceResolvedToken{
		{Chain: "ethereum", ChainID: 1, Address: "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", Symbol: "USDC", Decimals: 6},
		{Chain: "gnosis", ChainID: 100, Address: "0xcB444e90D8198415266c6a2724b7900fb12FC56E", Symbol: "EURe", Decimals: 18},
		{Chain: "polygon", ChainID: 137, Address: "0x18ec0A6E18E5bc3784fDd3a3634b31245ab704F6", Symbol: "EURe", Decimals: 18},
		{Chain: "celo", ChainID: 42220, Address: "0x765DE816845861e75A25fCA122bb6898B8B1282a", Symbol: "cUSD", Decimals: 18},
		{Chain: "celo", ChainID: 42220, Address: "0xD8763CBA276a3738E6DE85b4b3bF5FDed6D6cA73", Symbol: "cEUR", Decimals: 18},
	}
	for _, token := range builtins {
		if token.Chain == chain && strings.EqualFold(token.Symbol, symbolKey) {
			return token, true
		}
	}
	return balanceResolvedToken{}, false
}

func firstNonZeroInt(values ...int) int {
	for _, v := range values {
		if v != 0 {
			return v
		}
	}
	return 0
}
