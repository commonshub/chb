package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// OdooJournalLinks maps stable account identity keys to Odoo bank journal IDs.
//
// Why this lives outside accounts.json:
//
//   - It is mutated LOCALLY by `chb accounts <slug> link`. accounts.json is
//     force-overwritten from the org-wide embedded default on every bootstrap
//     (see forceOverwriteDefaults), which would silently revert any link.
//   - The account↔journal mapping belongs to the account identity itself, not
//     to the currently configured Odoo instance. CHB's Odoo journals are copied
//     across environments with stable IDs, so using the Odoo DB name as a scope
//     makes links disappear when switching between test/prod/mirrors.
//
// The key is a STABLE account identity (IBAN/address), not the slug: a slug like
// "savings" can be reassigned to a different real account over time, but its
// IBAN/address is durable, so the link follows the actual account.
type OdooJournalLinks map[string]int

const odooJournalLinksFileName = "odoo-journals.json"

func odooJournalLinksPath() string { return settingsFilePath(odooJournalLinksFileName) }

// loadOdooJournalLinks reads odoo-journals.json. Returns an empty (non-nil) map
// when the file is missing or unreadable so callers can index it safely.
func loadOdooJournalLinks() OdooJournalLinks {
	data, err := os.ReadFile(odooJournalLinksPath())
	if err != nil {
		return OdooJournalLinks{}
	}
	return parseOdooJournalLinks(data)
}

func parseOdooJournalLinks(data []byte) OdooJournalLinks {
	var flat OdooJournalLinks
	if json.Unmarshal(data, &flat) == nil && flat != nil {
		return flat
	}

	// Backward compatibility for the short-lived instance-scoped schema:
	// {"db-name": {"identity": 47}}. If the current Odoo instance is present,
	// prefer its entries, then fill any remaining keys from other instances.
	var scoped map[string]map[string]int
	if json.Unmarshal(data, &scoped) != nil || scoped == nil {
		return OdooJournalLinks{}
	}
	out := OdooJournalLinks{}
	if instance := currentOdooInstanceKey(); instance != "" {
		for key, id := range scoped[instance] {
			if id > 0 {
				out[key] = id
			}
		}
	}
	instances := make([]string, 0, len(scoped))
	for instance := range scoped {
		instances = append(instances, instance)
	}
	sort.Strings(instances)
	for _, instance := range instances {
		for key, id := range scoped[instance] {
			if id > 0 {
				if _, exists := out[key]; !exists {
					out[key] = id
				}
			}
		}
	}
	return out
}

func saveOdooJournalLinks(links OdooJournalLinks) error {
	data, err := json.MarshalIndent(links, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(odooJournalLinksPath()), 0755); err != nil {
		return err
	}
	return os.WriteFile(odooJournalLinksPath(), data, 0644)
}

// setOdooJournalLink records (or replaces) the journal ID for one account
// identity and persists the file. The instance parameter is retained for call
// site compatibility but deliberately ignored: account↔journal links are global.
func setOdooJournalLink(_ string, identityKey string, journalID int) error {
	if identityKey == "" {
		return nil
	}
	links := loadOdooJournalLinks()
	links[identityKey] = journalID
	return saveOdooJournalLinks(links)
}

// accountIdentityKey returns a key that survives slug reuse by preferring the
// account's durable identity: IBAN, then chain-scoped on-chain address, then
// Stripe account ID, falling back to the slug only when nothing more stable
// exists. Chain-scoping avoids linking the same EVM address on two different
// networks to the same Odoo journal.
func accountIdentityKey(acc AccountConfig) string {
	if iban := normalizeIBAN(acc.IBAN); iban != "" {
		return "iban:" + iban
	}
	if acc.Address != "" {
		addr := strings.ToLower(strings.TrimSpace(acc.Address))
		if acc.ChainID > 0 {
			return "ethereum:" + strconv.Itoa(acc.ChainID) + ":address:" + addr
		}
		if chain := strings.ToLower(strings.TrimSpace(acc.Chain)); chain != "" {
			return chain + ":address:" + addr
		}
		return legacyAddressIdentityKey(acc)
	}
	if acc.AccountID != "" {
		return "stripe:" + strings.TrimSpace(acc.AccountID)
	}
	return "slug:" + strings.ToLower(strings.TrimSpace(acc.Slug))
}

func legacyAddressIdentityKey(acc AccountConfig) string {
	if acc.Address == "" {
		return ""
	}
	return "address:" + strings.ToLower(strings.TrimSpace(acc.Address))
}

// currentOdooInstanceKey identifies the configured Odoo database. It is used
// only to migrate old instance-scoped odoo-journals.json files.
func currentOdooInstanceKey() string {
	db := os.Getenv("ODOO_DATABASE")
	url := os.Getenv("ODOO_URL")
	if db == "" || url == "" {
		env := loadConfigEnv(configEnvPath())
		if db == "" {
			db = env["ODOO_DATABASE"]
		}
		if url == "" {
			url = env["ODOO_URL"]
		}
	}
	if db != "" {
		return db
	}
	if url != "" {
		return odooDBFromURL(url)
	}
	return ""
}

// applyOdooJournalLinks overlays the global account↔journal links onto each
// account, so the rest of the code keeps reading acc.OdooJournalID unchanged.
func applyOdooJournalLinks(accounts []AccountConfig) {
	m := loadOdooJournalLinks()
	if len(m) == 0 {
		return
	}
	legacyCounts := map[string]int{}
	for _, acc := range accounts {
		if key := legacyAddressIdentityKey(acc); key != "" {
			legacyCounts[key]++
		}
	}
	for i := range accounts {
		if id, ok := m[accountIdentityKey(accounts[i])]; ok && id > 0 {
			accounts[i].OdooJournalID = id
			continue
		}
		// Backward compatibility for links saved before chain-scoped on-chain
		// identities existed. Only apply the legacy address key when it identifies
		// exactly one configured account; otherwise the link is ambiguous.
		if key := legacyAddressIdentityKey(accounts[i]); key != "" && legacyCounts[key] == 1 {
			if id, ok := m[key]; ok && id > 0 {
				accounts[i].OdooJournalID = id
			}
		}
	}
}

// migrateOdooJournalLinks is a one-shot move of per-account journal IDs out of
// accounts.json and into odoo-journals.json. It runs during bootstrap BEFORE the
// reconciler force-overwrites accounts.json (which would otherwise discard the
// legacy IDs). No-op once the file exists or when accounts.json carries no IDs.
func migrateOdooJournalLinks(dir string) {
	linksPath := filepath.Join(dir, odooJournalLinksFileName)
	if _, err := os.Stat(linksPath); err == nil {
		return // already migrated
	}
	data, err := os.ReadFile(filepath.Join(dir, "accounts.json"))
	if err != nil {
		return
	}
	var accounts []AccountConfig
	if json.Unmarshal(data, &accounts) != nil {
		return
	}
	m := OdooJournalLinks{}
	for _, acc := range accounts {
		if acc.OdooJournalID > 0 {
			m[accountIdentityKey(acc)] = acc.OdooJournalID
		}
	}
	if len(m) == 0 {
		return
	}
	_ = saveOdooJournalLinks(m)
}
