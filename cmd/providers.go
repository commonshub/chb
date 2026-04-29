package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
)

// Provider owns one external source. A provider syncs source data into the
// monthly data/<source>/ archive, then maps that archived data into standard
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

func providerDataRelPath(source string, elems ...string) string {
	parts := append([]string{"data", normalizeSourceName(source)}, elems...)
	return filepath.Join(parts...)
}

func providerDataPath(dataDir, year, month, source string, elems ...string) string {
	parts := []string{dataDir, year, month, providerDataRelPath(source, elems...)}
	return filepath.Join(parts...)
}

func writeProviderDataJSON(dataDir, year, month, source string, v interface{}, elems ...string) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return writeMonthFile(dataDir, year, month, providerDataRelPath(source, elems...), data)
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
	sourcePath := providerDataPath(dataDir, year, month, "stripe", "balance-transactions.json")
	if fileExists(sourcePath) {
		return []string{sourcePath}
	}

	stripeDir := filepath.Join(dataDir, year, month, "finance", "stripe")
	entries, err := os.ReadDir(stripeDir)
	if err != nil {
		return nil
	}
	var paths []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		paths = append(paths, filepath.Join(stripeDir, e.Name()))
	}
	return paths
}
