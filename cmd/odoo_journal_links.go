package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
)

// OdooJournalLinks maps an Odoo instance to the per-account journal mapping for
// that instance: instance → accountIdentityKey → Odoo bank journal ID.
//
// Why this lives outside accounts.json:
//
//   - The account↔journal mapping is INSTANCE-SPECIFIC. Journal IDs differ
//     between Odoo databases (test vs prod), so a single shared accounts.json
//     can't carry a value that is correct for everyone.
//   - It is mutated LOCALLY by `chb accounts <slug> link`. accounts.json is
//     force-overwritten from the org-wide embedded default on every bootstrap
//     (see forceOverwriteDefaults), which would silently revert any link.
//
// The inner key is a STABLE account identity (IBAN/address), not the slug: a
// slug like "savings" can be reassigned to a different real account over time,
// but its IBAN/address is durable, so the link follows the actual account.
type OdooJournalLinks map[string]map[string]int

const odooJournalLinksFileName = "odoo-journals.json"

func odooJournalLinksPath() string { return settingsFilePath(odooJournalLinksFileName) }

// loadOdooJournalLinks reads odoo-journals.json. Returns an empty (non-nil) map
// when the file is missing or unreadable so callers can index it safely.
func loadOdooJournalLinks() OdooJournalLinks {
	out := OdooJournalLinks{}
	data, err := os.ReadFile(odooJournalLinksPath())
	if err != nil {
		return out
	}
	if json.Unmarshal(data, &out) != nil || out == nil {
		return OdooJournalLinks{}
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
// identity under one Odoo instance and persists the file.
func setOdooJournalLink(instance, identityKey string, journalID int) error {
	if instance == "" || identityKey == "" {
		return nil
	}
	links := loadOdooJournalLinks()
	if links[instance] == nil {
		links[instance] = map[string]int{}
	}
	links[instance][identityKey] = journalID
	return saveOdooJournalLinks(links)
}

// accountIdentityKey returns a key that survives slug reuse by preferring the
// account's durable identity: IBAN, then on-chain address, then Stripe account
// ID, falling back to the slug only when nothing more stable exists.
func accountIdentityKey(acc AccountConfig) string {
	if iban := normalizeIBAN(acc.IBAN); iban != "" {
		return "iban:" + iban
	}
	if acc.Address != "" {
		return "address:" + strings.ToLower(strings.TrimSpace(acc.Address))
	}
	if acc.AccountID != "" {
		return "stripe:" + strings.TrimSpace(acc.AccountID)
	}
	return "slug:" + strings.ToLower(strings.TrimSpace(acc.Slug))
}

// currentOdooInstanceKey identifies the configured Odoo database. It works both
// at runtime (after LoadEnvFromConfig populates the environment) and during
// settings bootstrap (before env load) by falling back to reading config.env
// directly. Returns "" when no Odoo instance is configured.
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

// applyOdooJournalLinks overlays the journal ID for the currently-configured
// Odoo instance onto each account, so the rest of the code keeps reading
// acc.OdooJournalID unchanged. A no-op when Odoo isn't configured or the
// instance has no recorded links.
func applyOdooJournalLinks(accounts []AccountConfig) {
	instance := currentOdooInstanceKey()
	if instance == "" {
		return
	}
	m := loadOdooJournalLinks()[instance]
	if len(m) == 0 {
		return
	}
	for i := range accounts {
		if id, ok := m[accountIdentityKey(accounts[i])]; ok && id > 0 {
			accounts[i].OdooJournalID = id
		}
	}
}

// migrateOdooJournalLinks is a one-shot move of per-account journal IDs out of
// accounts.json and into odoo-journals.json under the current instance. It runs
// during bootstrap BEFORE the reconciler force-overwrites accounts.json (which
// would otherwise discard the legacy IDs). No-op once the file exists, when no
// Odoo instance is configured, or when accounts.json carries no IDs.
func migrateOdooJournalLinks(dir string) {
	linksPath := filepath.Join(dir, odooJournalLinksFileName)
	if _, err := os.Stat(linksPath); err == nil {
		return // already migrated
	}
	instance := currentOdooInstanceKey()
	if instance == "" {
		return // can't attribute IDs to an instance — leave them for a later run
	}
	data, err := os.ReadFile(filepath.Join(dir, "accounts.json"))
	if err != nil {
		return
	}
	var accounts []AccountConfig
	if json.Unmarshal(data, &accounts) != nil {
		return
	}
	m := map[string]int{}
	for _, acc := range accounts {
		if acc.OdooJournalID > 0 {
			m[accountIdentityKey(acc)] = acc.OdooJournalID
		}
	}
	if len(m) == 0 {
		return
	}
	_ = saveOdooJournalLinks(OdooJournalLinks{instance: m})
}
