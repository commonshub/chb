package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
)

type TokenConfig struct {
	Name      string `json:"name"`
	Slug      string `json:"slug"`
	Provider  string `json:"provider,omitempty"`
	Chain     string `json:"chain,omitempty"`
	ChainID   int    `json:"chainId,omitempty"`
	Address   string `json:"address"`
	Symbol    string `json:"symbol"`
	Decimals  int    `json:"decimals"`
	Mintable  bool   `json:"mintable,omitempty"`
	Burnable  bool   `json:"burnable,omitempty"`
	RpcUrl    string `json:"rpcUrl,omitempty"`
	WalletURL string `json:"walletUrl,omitempty"`
}

func tokensConfigPath() string {
	return settingsFilePath("tokens.json")
}

func LoadTokenConfigs() []TokenConfig {
	tokens, ok := readTokenConfigs()
	if ok {
		return tokens
	}
	return nil
}

func SaveTokenConfigs(tokens []TokenConfig) error {
	tokens = dedupeTokenConfigs(tokens)
	data, err := json.MarshalIndent(tokens, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(tokensConfigPath()), 0755); err != nil {
		return err
	}
	return os.WriteFile(tokensConfigPath(), data, 0644)
}

func loadTokenConfigsForSettings(settings *Settings) []TokenConfig {
	if tokens, ok := readTokenConfigs(); ok {
		return tokens
	}
	tokens := migrateTokenConfigs(settings)
	if len(tokens) > 0 {
		_ = SaveTokenConfigs(tokens)
		stripTokenTrackersFromAccountsJSON()
	}
	return tokens
}

func readTokenConfigs() ([]TokenConfig, bool) {
	data, err := os.ReadFile(existingSettingsFilePath("tokens.json"))
	if err != nil {
		return nil, false
	}
	var tokens []TokenConfig
	if json.Unmarshal(data, &tokens) != nil {
		return nil, true
	}
	return dedupeTokenConfigs(tokens), true
}

func migrateTokenConfigs(settings *Settings) []TokenConfig {
	var tokens []TokenConfig
	for _, acc := range settings.Finance.Accounts {
		if !financeAccountIsTokenTracker(acc) {
			continue
		}
		tokens = append(tokens, tokenConfigFromFinanceAccount(acc))
	}
	if settings.ContributionToken != nil {
		tokens = append(tokens, tokenConfigFromContributionToken(*settings.ContributionToken))
	}
	return dedupeTokenConfigs(tokens)
}

func tokenConfigFromFinanceAccount(acc FinanceAccount) TokenConfig {
	token := TokenConfig{
		Name:     acc.Name,
		Slug:     acc.Slug,
		Provider: firstNonEmptyStr(acc.Provider, "etherscan"),
		Chain:    acc.Chain,
		ChainID:  acc.ChainID,
		Symbol:   acc.Currency,
		Mintable: true,
		Burnable: true,
	}
	if acc.Token != nil {
		token.Address = acc.Token.Address
		token.Name = firstNonEmptyStr(token.Name, acc.Token.Name)
		token.Symbol = firstNonEmptyStr(acc.Token.Symbol, token.Symbol)
		token.Decimals = acc.Token.Decimals
	}
	if token.Slug == "" {
		token.Slug = strings.ToLower(token.Symbol)
	}
	return normalizeTokenConfig(token)
}

func tokenConfigFromContributionToken(ct ContributionTokenSettings) TokenConfig {
	return normalizeTokenConfig(TokenConfig{
		Name:     ct.Name,
		Slug:     strings.ToLower(ct.Symbol),
		Provider: "etherscan",
		Chain:    ct.Chain,
		ChainID:  ct.ChainID,
		Address:  ct.Address,
		Symbol:   ct.Symbol,
		Decimals: ct.Decimals,
		Mintable: true,
		Burnable: true,
		RpcUrl:   ct.RpcUrl,
	})
}

func normalizeTokenConfig(token TokenConfig) TokenConfig {
	token.Slug = strings.TrimSpace(token.Slug)
	token.Provider = strings.TrimSpace(token.Provider)
	token.Chain = strings.TrimSpace(token.Chain)
	token.Address = strings.TrimSpace(token.Address)
	token.Symbol = strings.TrimSpace(token.Symbol)
	if token.Provider == "" {
		token.Provider = "etherscan"
	}
	if token.Slug == "" {
		token.Slug = strings.ToLower(token.Symbol)
	}
	if token.Name == "" {
		token.Name = token.Symbol
	}
	if token.Decimals == 0 {
		token.Decimals = 18
	}
	return token
}

func dedupeTokenConfigs(tokens []TokenConfig) []TokenConfig {
	seen := map[string]bool{}
	out := make([]TokenConfig, 0, len(tokens))
	for _, token := range tokens {
		token = normalizeTokenConfig(token)
		key := tokenConfigKey(token)
		if key == "" || seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, token)
	}
	return out
}

func tokenConfigKey(token TokenConfig) string {
	address := strings.ToLower(strings.TrimSpace(token.Address))
	chain := strings.ToLower(strings.TrimSpace(token.Chain))
	if address != "" {
		return chain + "\x00" + address
	}
	return chain + "\x00" + strings.ToLower(strings.TrimSpace(token.Symbol))
}

func ToFinanceTokenAccounts(tokens []TokenConfig) []FinanceAccount {
	accounts := make([]FinanceAccount, 0, len(tokens))
	for _, token := range dedupeTokenConfigs(tokens) {
		if token.Address == "" {
			continue
		}
		accounts = append(accounts, FinanceAccount{
			Name:     "🪙 " + token.Name,
			Slug:     token.Slug,
			Provider: firstNonEmptyStr(token.Provider, "etherscan"),
			Chain:    token.Chain,
			ChainID:  token.ChainID,
			Address:  "",
			Currency: token.Symbol,
			Token: &struct {
				Address  string `json:"address"`
				Name     string `json:"name"`
				Symbol   string `json:"symbol"`
				Decimals int    `json:"decimals"`
			}{
				Address:  token.Address,
				Name:     token.Name,
				Symbol:   token.Symbol,
				Decimals: token.Decimals,
			},
		})
	}
	return accounts
}

func financeAccountIsTokenTracker(acc FinanceAccount) bool {
	return acc.Provider == "etherscan" && acc.Token != nil && strings.TrimSpace(acc.Address) == ""
}

func accountConfigIsTokenTracker(acc AccountConfig) bool {
	return acc.Provider == "etherscan" && acc.Token != nil && strings.TrimSpace(acc.Address) == ""
}

func filterTokenTrackerFinanceAccounts(accounts []FinanceAccount) []FinanceAccount {
	out := make([]FinanceAccount, 0, len(accounts))
	for _, acc := range accounts {
		if financeAccountIsTokenTracker(acc) {
			continue
		}
		out = append(out, acc)
	}
	return out
}

func stripTokenTrackersFromAccountsJSON() {
	data, err := os.ReadFile(existingSettingsFilePath("accounts.json"))
	if err != nil {
		return
	}
	var accounts []AccountConfig
	if json.Unmarshal(data, &accounts) != nil {
		return
	}
	filtered := make([]AccountConfig, 0, len(accounts))
	changed := false
	for _, acc := range accounts {
		if accountConfigIsTokenTracker(acc) {
			changed = true
			continue
		}
		filtered = append(filtered, acc)
	}
	if changed {
		_ = SaveAccountConfigs(filtered)
	}
}
