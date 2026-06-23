package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// AccountConfig represents a finance account in accounts.json.
// Extends FinanceAccount with additional fields.
//
// The display name for the linked Odoo journal is intentionally NOT stored
// here — Odoo journal names typically embed the IBAN (e.g. "IBAN EE72 7777
// …"), which we don't want to leak via the public default accounts.json.
// Display callers should use OdooJournalName(id) which reads from a local
// cache populated by sync runs.
type AccountConfig struct {
	Name              string `json:"name"`
	Slug              string `json:"slug"`
	Provider          string `json:"provider"`        // stripe, etherscan, monerium, kbcbrussels
	Chain             string `json:"chain,omitempty"` // gnosis, celo, ethereum
	ChainID           int    `json:"chainId,omitempty"`
	Address           string `json:"address,omitempty"`           // wallet address
	AccountID         string `json:"accountId,omitempty"`         // stripe account ID
	IBAN              string `json:"iban,omitempty"`              // bank account IBAN (kbcbrussels, …)
	Currency          string `json:"currency,omitempty"`          // EUR, EURe, etc.
	WalletType        string `json:"walletType,omitempty"`        // "eoa" or "safe"
	OdooJournalID     int    `json:"odooJournalId,omitempty"`     // linked Odoo bank journal ID
	OdooSourceOfTruth bool   `json:"odooSourceOfTruth,omitempty"` // true when Odoo journal lines are authoritative and CHB must not push local txs into it
	OdooSyncSince     string `json:"odooSyncSince,omitempty"`     // journal starts at this date (YYYY-MM-DD) with a manual opening-balance entry; CHB owns only lines from this date on
	ArchivedAt        string `json:"archivedAt,omitempty"`        // date after which the account is no longer active (YYYY-MM-DD)
	Token             *struct {
		Address  string `json:"address"`
		Name     string `json:"name"`
		Symbol   string `json:"symbol"`
		Decimals int    `json:"decimals"`
	} `json:"token,omitempty"`
	// PriorTokens lists earlier contract versions of the SAME logical currency
	// (e.g. the pre-migration Monerium EURe contract). The sync pulls each one
	// in addition to Token; transfers from every contract are merged into this
	// account's history at generate time. Each prior contract is archived under
	// its own filename ({slug}.{addr}.{symbol}-{contractShort}.json) so it never
	// clobbers the primary token's file. See https://docs.monerium.com/contracts-v2.
	PriorTokens []AccountToken `json:"priorTokens,omitempty"`
}

// AccountToken is a single ERC20 contract an account tracks. Same shape as the
// inline Token field; named so it can back the PriorTokens slice.
type AccountToken struct {
	Address  string `json:"address"`
	Name     string `json:"name"`
	Symbol   string `json:"symbol"`
	Decimals int    `json:"decimals"`
}

// IsArchived reports whether the account's archivedAt date has passed. Archived
// accounts are excluded from bulk pulls (`chb accounts pull`) — they have no new
// activity to fetch — while their cached history is still kept and generated. A
// present-but-malformed archivedAt is treated as archived (the intent is clear).
func (a *AccountConfig) IsArchived() bool {
	if a == nil || a.ArchivedAt == "" {
		return false
	}
	archDate, err := time.ParseInLocation("2006-01-02", a.ArchivedAt, BrusselsTZ())
	if err != nil {
		return true
	}
	now := time.Now().In(BrusselsTZ())
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, BrusselsTZ())
	return today.After(archDate)
}

// IsSafe returns true only when this is explicitly configured as a Safe wallet.
func (a *AccountConfig) IsSafe() bool {
	if a == nil {
		return false
	}
	return strings.EqualFold(a.WalletType, "safe")
}

// IsOdooSourceOfTruth returns true when the linked Odoo journal is authoritative
// for this account. Such accounts are still pulled/cached from Odoo, but CHB
// must not push locally generated transactions into their journal.
func (a *AccountConfig) IsOdooSourceOfTruth() bool {
	return a != nil && a.OdooSourceOfTruth
}

// OdooSyncSinceTime parses the odooSyncSince cutoff. When set, the Odoo
// journal is expected to hold a manual opening-balance entry at the cutoff
// plus CHB-owned lines from the cutoff on; all push and repair tooling
// windows the local transaction universe to >= this date (Brussels time).
// Returns ok=false when unset or malformed.
func (a *AccountConfig) OdooSyncSinceTime() (time.Time, bool) {
	if a == nil || a.OdooSyncSince == "" {
		return time.Time{}, false
	}
	t, err := time.ParseInLocation("2006-01-02", a.OdooSyncSince, BrusselsTZ())
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

func accountsConfigPath() string {
	return settingsFilePath("accounts.json")
}

// LoadAccountConfigs reads accounts from APP_DATA_DIR/settings/accounts.json.
//
// If the file still has the legacy "odooJournalName" field (pre-cache
// schema), the value is drained into the journal-name cache and the file is
// rewritten without the field. This avoids re-leaking IBANs through public
// settings exports.
func LoadAccountConfigs() []AccountConfig {
	data, err := os.ReadFile(accountsConfigPath())
	if err != nil {
		return nil
	}
	var accounts []AccountConfig
	if json.Unmarshal(data, &accounts) != nil {
		return nil
	}
	migrateLegacyOdooJournalNames(data)
	// The account↔Odoo-journal mapping lives in odoo-journals.json (not the
	// force-overwritten accounts.json); overlay it so downstream readers keep
	// using acc.OdooJournalID.
	applyOdooJournalLinks(accounts)
	return accounts
}

// migrateLegacyOdooJournalNames pulls any "odooJournalName" entries out of
// raw accounts.json bytes and copies them into the cache, then rewrites the
// file without the field. Idempotent: silent no-op once the field is gone.
func migrateLegacyOdooJournalNames(data []byte) {
	var raw []map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return
	}
	dirty := false
	for _, entry := range raw {
		nameRaw, ok := entry["odooJournalName"]
		if !ok {
			continue
		}
		var name string
		_ = json.Unmarshal(nameRaw, &name)
		var id int
		if idRaw, ok := entry["odooJournalId"]; ok {
			_ = json.Unmarshal(idRaw, &id)
		}
		if id > 0 && name != "" {
			CacheOdooJournalName(id, name)
		}
		delete(entry, "odooJournalName")
		dirty = true
	}
	if !dirty {
		return
	}
	out, err := json.MarshalIndent(raw, "", "  ")
	if err != nil {
		return
	}
	_ = os.WriteFile(accountsConfigPath(), out, 0644)
}

// SaveAccountConfigs writes accounts to APP_DATA_DIR/settings/accounts.json.
func SaveAccountConfigs(accounts []AccountConfig) error {
	data, err := json.MarshalIndent(accounts, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(accountsConfigPath()), 0755); err != nil {
		return err
	}
	return os.WriteFile(accountsConfigPath(), data, 0644)
}

// ToFinanceAccounts converts AccountConfigs to FinanceAccounts for backward compatibility.
func ToFinanceAccounts(configs []AccountConfig) []FinanceAccount {
	var accounts []FinanceAccount
	for _, c := range configs {
		fa := FinanceAccount{
			Name:       c.Name,
			Slug:       c.Slug,
			Provider:   c.Provider,
			Chain:      c.Chain,
			ChainID:    c.ChainID,
			Address:    c.Address,
			AccountID:  c.AccountID,
			Currency:   c.Currency,
			ArchivedAt: c.ArchivedAt,
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
		if len(c.PriorTokens) > 0 {
			fa.PriorTokens = append([]AccountToken(nil), c.PriorTokens...)
		}
		accounts = append(accounts, fa)
	}
	return accounts
}
