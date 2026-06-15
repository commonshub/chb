package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
)

// OdooAnalyticLinks binds a stable analytic-account identity key
// ("<kind>:<slug>", e.g. "collective:openletter" or "category:accounting")
// to an Odoo account.analytic.account id.
//
// Why this exists: the analytic-plans sync would otherwise reuse-or-create
// accounts purely by display NAME (analyticAccountKey), derived from the slug
// via prettyCollectiveName/prettyCategoryName. That mints a twin whenever the
// Odoo account's human name differs from the slug's pretty form ("Open Letter"
// vs slug `openletter` → "Openletter"), and can never reuse an acronym account
// ("OSV" vs slug `osv` → "Osv"). Binding by id decouples reuse from the name:
// once a slug is bound here, sync reuses that exact account regardless of what
// it's called in Odoo, so duplicates can't be recreated.
//
// Maintained by `chb odoo fix`. Unlike odoo-journals.json this is a brand-new
// file with no legacy instance-scoped schema to migrate — keep it flat.
type OdooAnalyticLinks map[string]int

const odooAnalyticLinksFileName = "odoo-analytic-accounts.json"

func odooAnalyticLinksPath() string { return settingsFilePath(odooAnalyticLinksFileName) }

// analyticLinkKey builds the stable identity key. kind is "category" or
// "collective"; slug is the lowercased category/collective slug.
func analyticLinkKey(kind, slug string) string {
	return strings.ToLower(strings.TrimSpace(kind)) + ":" + strings.ToLower(strings.TrimSpace(slug))
}

// loadOdooAnalyticLinks reads odoo-analytic-accounts.json. Returns an empty
// (non-nil) map when the file is missing or unreadable so callers can index it
// safely.
func loadOdooAnalyticLinks() OdooAnalyticLinks {
	data, err := os.ReadFile(odooAnalyticLinksPath())
	if err != nil {
		return OdooAnalyticLinks{}
	}
	var links OdooAnalyticLinks
	if json.Unmarshal(data, &links) != nil || links == nil {
		return OdooAnalyticLinks{}
	}
	return links
}

func saveOdooAnalyticLinks(links OdooAnalyticLinks) error {
	data, err := json.MarshalIndent(links, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(odooAnalyticLinksPath()), 0755); err != nil {
		return err
	}
	return os.WriteFile(odooAnalyticLinksPath(), data, 0644)
}

// setOdooAnalyticLink records (or replaces) the account id for one
// kind+slug identity and persists the file. A non-positive id removes the
// binding instead (used to self-heal a stale link).
func setOdooAnalyticLink(kind, slug string, id int) error {
	key := analyticLinkKey(kind, slug)
	if strings.TrimSpace(slug) == "" {
		return nil
	}
	links := loadOdooAnalyticLinks()
	if id > 0 {
		links[key] = id
	} else {
		delete(links, key)
	}
	return saveOdooAnalyticLinks(links)
}
