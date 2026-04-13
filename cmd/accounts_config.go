package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// AccountConfig represents a finance account in accounts.json.
// Extends FinanceAccount with additional fields.
type AccountConfig struct {
	Name      string `json:"name"`
	Slug      string `json:"slug"`
	Provider  string `json:"provider"`             // stripe, etherscan, monerium
	Chain     string `json:"chain,omitempty"`       // gnosis, celo, ethereum
	ChainID   int    `json:"chainId,omitempty"`
	Address   string `json:"address,omitempty"`     // wallet address
	AccountID string `json:"accountId,omitempty"`   // stripe account ID
	Currency  string `json:"currency,omitempty"`    // EUR, EURe, etc.
	WalletType     string `json:"walletType,omitempty"`     // "eoa" or "safe" (default: "safe")
	OdooJournalID   int    `json:"odooJournalId,omitempty"`   // linked Odoo bank journal ID
	OdooJournalName string `json:"odooJournalName,omitempty"` // journal display name
	Token     *struct {
		Address  string `json:"address"`
		Name     string `json:"name"`
		Symbol   string `json:"symbol"`
		Decimals int    `json:"decimals"`
	} `json:"token,omitempty"`
}

// IsSafe returns true if this is a Safe (multisig) wallet. Defaults to true for crypto accounts.
func (a *AccountConfig) IsSafe() bool {
	if a.WalletType == "eoa" {
		return false
	}
	return a.Address != "" // crypto accounts default to safe
}

func accountsConfigPath() string {
	return filepath.Join(chbDir(), "accounts.json")
}

// LoadAccountConfigs reads accounts from ~/.chb/accounts.json.
// On first load, migrates from settings.json if accounts.json doesn't exist.
func LoadAccountConfigs() []AccountConfig {
	data, err := os.ReadFile(accountsConfigPath())
	if err != nil {
		if os.IsNotExist(err) {
			accounts := migrateAccountsFromSettings()
			if len(accounts) > 0 {
				SaveAccountConfigs(accounts)
			}
			return accounts
		}
		return nil
	}
	var accounts []AccountConfig
	if json.Unmarshal(data, &accounts) != nil {
		return nil
	}
	return accounts
}

// SaveAccountConfigs writes accounts to ~/.chb/accounts.json.
func SaveAccountConfigs(accounts []AccountConfig) error {
	data, err := json.MarshalIndent(accounts, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(accountsConfigPath(), data, 0644)
}

// ToFinanceAccounts converts AccountConfigs to FinanceAccounts for backward compatibility.
func ToFinanceAccounts(configs []AccountConfig) []FinanceAccount {
	var accounts []FinanceAccount
	for _, c := range configs {
		fa := FinanceAccount{
			Name:      c.Name,
			Slug:      c.Slug,
			Provider:  c.Provider,
			Chain:     c.Chain,
			ChainID:   c.ChainID,
			Address:   c.Address,
			AccountID: c.AccountID,
			Currency:  c.Currency,
		}
		if c.Token != nil {
			fa.Token = &struct {
				Address  string `json:"address"`
				Name     string `json:"name"`
				Symbol   string `json:"symbol"`
				Decimals int    `json:"decimals"`
			}{
				Address:  c.Token.Address,
				Name:     c.Token.Name,
				Symbol:   c.Token.Symbol,
				Decimals: c.Token.Decimals,
			}
		}
		accounts = append(accounts, fa)
	}
	return accounts
}

func migrateAccountsFromSettings() []AccountConfig {
	settings, err := LoadSettings()
	if err != nil {
		return nil
	}

	var configs []AccountConfig
	for _, fa := range settings.Finance.Accounts {
		ac := AccountConfig{
			Name:      fa.Name,
			Slug:      fa.Slug,
			Provider:  fa.Provider,
			Chain:     fa.Chain,
			ChainID:   fa.ChainID,
			Address:   fa.Address,
			AccountID: fa.AccountID,
			Currency:  fa.Currency,
			WalletType: "safe", // default for crypto
		}
		if fa.Token != nil {
			ac.Token = &struct {
				Address  string `json:"address"`
				Name     string `json:"name"`
				Symbol   string `json:"symbol"`
				Decimals int    `json:"decimals"`
			}{
				Address:  fa.Token.Address,
				Name:     fa.Token.Name,
				Symbol:   fa.Token.Symbol,
				Decimals: fa.Token.Decimals,
			}
		}
		// Stripe accounts don't have a wallet type
		if fa.Provider == "stripe" {
			ac.WalletType = ""
		}
		configs = append(configs, ac)
	}
	return configs
}
