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

	// ExplorerUrl is a public block-explorer base URL (e.g. https://celoscan.io).
	ExplorerUrl string `json:"explorerUrl,omitempty"`

	// Contribution flags the token used for community contributions / wallet
	// resolution. At most one token should have this set; helpers like
	// ContributionToken(settings) return it.
	Contribution bool `json:"contribution,omitempty"`

	// WalletManager selects how Discord user IDs map to wallet addresses for
	// this contribution token. Either "citizenwallet" (CardManager contract)
	// or "opencollective" (Safe owners). Inferred from the chain if blank.
	WalletManager string `json:"walletManager,omitempty"`

	// CardManagerAddress and CardManagerInstanceID configure the
	// CitizenWallet CardManager contract for wallet resolution. Only used
	// when WalletManager is "citizenwallet"; default values are baked into
	// wallet.go for the common Celo deployment.
	CardManagerAddress    string `json:"cardManagerAddress,omitempty"`
	CardManagerInstanceID string `json:"cardManagerInstanceId,omitempty"`
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

func loadTokenConfigsForSettings(_ *Settings) []TokenConfig {
	tokens, _ := readTokenConfigs()
	return tokens
}

func readTokenConfigs() ([]TokenConfig, bool) {
	data, err := os.ReadFile(tokensConfigPath())
	if err != nil {
		return nil, false
	}
	var tokens []TokenConfig
	if json.Unmarshal(data, &tokens) != nil {
		return nil, true
	}
	return dedupeTokenConfigs(tokens), true
}

// ContributionTokenConfig returns the token marked Contribution=true, or nil
// when no token is flagged. tokens.json is the source of truth for the
// contribution token; the Settings.ContributionToken field is a derived view.
func ContributionTokenConfig(tokens []TokenConfig) *TokenConfig {
	for i := range tokens {
		if tokens[i].Contribution {
			return &tokens[i]
		}
	}
	return nil
}

// contributionTokenSettingsFromTokens projects a TokenConfig back into the
// ContributionTokenSettings shape that the rest of the CLI reads. Returns
// nil when no contribution token is configured.
func contributionTokenSettingsFromTokens(tokens []TokenConfig) *ContributionTokenSettings {
	t := ContributionTokenConfig(tokens)
	if t == nil {
		return nil
	}
	return &ContributionTokenSettings{
		Chain:                 t.Chain,
		ChainID:               t.ChainID,
		RpcUrl:                t.RpcUrl,
		ExplorerUrl:           t.ExplorerUrl,
		Address:               t.Address,
		Name:                  t.Name,
		Symbol:                t.Symbol,
		Decimals:              t.Decimals,
		WalletManager:         t.WalletManager,
		CardManagerAddress:    t.CardManagerAddress,
		CardManagerInstanceID: t.CardManagerInstanceID,
	}
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

