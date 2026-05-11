package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
)

// odooJournalNameCache stores journal_id → display_name lookups so we don't
// have to ship those names in accounts.json (which would leak IBANs/refs to
// the public repo) and don't have to round-trip Odoo every time we render
// the account list.
//
// File: APP_DATA_DIR/cache/odoo-journal-names.json
type odooJournalNameCache map[string]string

func odooJournalNameCachePath() string {
	return filepath.Join(AppDataDir(), "cache", "odoo-journal-names.json")
}

// legacyOdooJournalNameCachePath returns the pre-simplification location.
// Read-once on startup so existing users don't lose their cached names.
func legacyOdooJournalNameCachePath() string {
	return filepath.Join(DataDir(), "generated", "cache", "odoo-journal-names.json")
}

func loadOdooJournalNameCache() odooJournalNameCache {
	out := odooJournalNameCache{}
	data, err := os.ReadFile(odooJournalNameCachePath())
	if err == nil {
		_ = json.Unmarshal(data, &out)
		return out
	}
	// Fall back to the legacy path and migrate if found.
	if data, err := os.ReadFile(legacyOdooJournalNameCachePath()); err == nil {
		if json.Unmarshal(data, &out) == nil && len(out) > 0 {
			_ = saveOdooJournalNameCache(out)
			_ = os.Remove(legacyOdooJournalNameCachePath())
		}
	}
	return out
}

func saveOdooJournalNameCache(cache odooJournalNameCache) error {
	if cache == nil {
		return nil
	}
	path := odooJournalNameCachePath()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(cache, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

// OdooJournalName returns the cached display name for a journal, or "" if
// the cache has no entry yet. Callers that don't have an Odoo connection
// fall back to "journal #<id>" in their own rendering.
func OdooJournalName(journalID int) string {
	if journalID == 0 {
		return ""
	}
	return loadOdooJournalNameCache()[strconv.Itoa(journalID)]
}

// CacheOdooJournalName records a journal id → name mapping so subsequent
// renders can show the name without an API round-trip. Silently no-ops on
// missing arguments or write failure (the cache is best-effort).
func CacheOdooJournalName(journalID int, name string) {
	if journalID == 0 || name == "" {
		return
	}
	cache := loadOdooJournalNameCache()
	key := strconv.Itoa(journalID)
	if cache[key] == name {
		return
	}
	cache[key] = name
	_ = saveOdooJournalNameCache(cache)
}

// FetchAndCacheOdooJournalName looks up a journal name via the Odoo API and
// caches it. Used by sync paths that already have credentials/uid handy.
func FetchAndCacheOdooJournalName(creds *OdooCredentials, uid, journalID int) (string, error) {
	if journalID == 0 {
		return "", nil
	}
	result, err := odooExec(creds.URL, creds.DB, uid, creds.Password,
		"account.journal", "read",
		[]interface{}{[]interface{}{journalID}, []string{"name"}}, nil)
	if err != nil {
		return "", err
	}
	var rows []struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(result, &rows); err != nil {
		return "", err
	}
	if len(rows) == 0 {
		return "", nil
	}
	CacheOdooJournalName(journalID, rows[0].Name)
	return rows[0].Name, nil
}
