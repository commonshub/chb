package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Provider owns one external source. A provider syncs source data into the
// monthly sources/<source>/ archive, then maps that archived data into standard
// generated objects. Cross-source enrichment belongs in DataPlugin.
type Provider interface {
	Source() string
	EnvVars() []ProviderEnvVar
	SyncSourceData(*ProviderSyncContext, ProviderSyncScope) error
	GenerateObjects(*ProviderGenerateContext, ProviderGenerateScope) (*ProviderGeneratedObjects, error)
}

type ProviderEnvVar struct {
	Name        string
	Description string
	Required    bool
}

type ProviderSyncContext struct {
	DataDir  string
	Settings *Settings
}

type ProviderGenerateContext struct {
	DataDir  string
	Settings *Settings
}

type ProviderSyncScope struct {
	Source     string
	Account    string
	StartMonth string
	EndMonth   string
	Force      bool
}

type ProviderGenerateScope struct {
	Year  string
	Month string
}

type ProviderGeneratedObjects struct {
	Transactions []TransactionEntry
	Events       []FullEvent
	Messages     []json.RawMessage
	Images       []ImageEntry
}

func providerSourceRelPath(source string, elems ...string) string {
	parts := append([]string{"sources", normalizeSourceName(source)}, elems...)
	return filepath.Join(parts...)
}

func providerSourcePath(dataDir, year, month, source string, elems ...string) string {
	parts := []string{dataDir, year, month, providerSourceRelPath(source, elems...)}
	return filepath.Join(parts...)
}

func writeProviderSourceJSON(dataDir, year, month, source string, v interface{}, elems ...string) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return writeMonthFile(dataDir, year, month, providerSourceRelPath(source, elems...), data)
}

func pluginDataRelPath(plugin string, elems ...string) string {
	parts := append([]string{"plugins", normalizeSourceName(plugin)}, elems...)
	return filepath.Join(parts...)
}

func pluginDataPath(dataDir, year, month, plugin string, elems ...string) string {
	parts := []string{dataDir, year, month, pluginDataRelPath(plugin, elems...)}
	return filepath.Join(parts...)
}

func writePluginDataJSON(dataDir, year, month, plugin string, v interface{}, elems ...string) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return writeMonthFile(dataDir, year, month, pluginDataRelPath(plugin, elems...), data)
}

func normalizeSourceName(source string) string {
	source = strings.TrimSpace(strings.ToLower(source))
	source = strings.ReplaceAll(source, "_", "-")
	source = strings.Join(strings.Fields(source), "-")
	return source
}

func registeredProviders() []Provider {
	return []Provider{
		stripeProvider{},
	}
}

type stripeProvider struct{}

func (stripeProvider) Source() string {
	return "stripe"
}

func (stripeProvider) EnvVars() []ProviderEnvVar {
	return []ProviderEnvVar{
		{
			Name:        "STRIPE_SECRET_KEY",
			Description: "Stripe secret key used to sync source balance transactions and charge details.",
			Required:    true,
		},
	}
}

func (stripeProvider) SyncSourceData(*ProviderSyncContext, ProviderSyncScope) error {
	return nil
}

func (stripeProvider) GenerateObjects(*ProviderGenerateContext, ProviderGenerateScope) (*ProviderGeneratedObjects, error) {
	return &ProviderGeneratedObjects{}, nil
}

func stripeTransactionCachePaths(dataDir, year, month string) []string {
	sourcePath := providerSourcePath(dataDir, year, month, "stripe", "balance-transactions.json")
	if fileExists(sourcePath) {
		return []string{sourcePath}
	}
	return nil
}

func loadStripeBTsSinceFromSources(dataDir, accountID string, sinceUnix int64) ([]StripeTransaction, error) {
	all, err := loadStripeBTsFromSources(dataDir, accountID)
	if err != nil {
		return nil, err
	}
	var out []StripeTransaction
	for _, bt := range all {
		if bt.Created > sinceUnix {
			out = append(out, bt)
		}
	}
	return out, nil
}

func loadStripeBTsFromSources(dataDir, accountID string) ([]StripeTransaction, error) {
	years, err := os.ReadDir(dataDir)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	foundCache := false
	var out []StripeTransaction
	for _, y := range years {
		if !y.IsDir() || len(y.Name()) != 4 {
			continue
		}
		months, err := os.ReadDir(filepath.Join(dataDir, y.Name()))
		if err != nil {
			continue
		}
		for _, m := range months {
			if !m.IsDir() || len(m.Name()) != 2 {
				continue
			}
			cache, ok := loadStripeBTCache(providerSourcePath(dataDir, y.Name(), m.Name(), "stripe", "balance-transactions.json"))
			if !ok {
				continue
			}
			if accountID != "" && cache.AccountID != "" && !strings.EqualFold(accountID, cache.AccountID) {
				continue
			}
			foundCache = true
			for _, bt := range cache.Transactions {
				if bt.ID == "" || seen[bt.ID] {
					continue
				}
				seen[bt.ID] = true
				out = append(out, bt)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Created == out[j].Created {
			return out[i].ID < out[j].ID
		}
		return out[i].Created < out[j].Created
	})
	if !foundCache {
		return nil, os.ErrNotExist
	}
	return out, nil
}

func loadStripeBTCache(path string) (StripeCacheFile, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return StripeCacheFile{}, false
	}
	var cache StripeCacheFile
	if json.Unmarshal(data, &cache) != nil {
		return StripeCacheFile{}, false
	}
	return cache, len(cache.Transactions) > 0
}
