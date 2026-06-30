package odoo

import (
	"encoding/json"
	"os"
	"path/filepath"

	"github.com/CommonsHub/chb/providers"
)

const (
	Source                 = "odoo"
	InvoicesFile           = "invoices.json"
	BillsFile              = "bills.json"
	PartnersFile           = "partners.json"
	SubscriptionsFile      = "subscriptions.json"
	AnalyticEnrichmentFile = "analytic-enrichment.json"
	AnalyticPlansFile      = "analytic-plans.json"
)

type SourceProvider struct{}

func (SourceProvider) Name() string {
	return Source
}

func (SourceProvider) Files() []providers.File {
	return []providers.File{
		{Name: InvoicesFile, Description: "Monthly public Odoo customer invoices.", Private: false},
		{Name: BillsFile, Description: "Monthly public Odoo vendor bills.", Private: false},
		{Name: PartnersFile, Description: "Odoo partner snapshot for local matching.", Private: true},
		{Name: "journals/<journal-id>.json", Description: "Odoo bank journal line snapshots for staged local processing.", Private: true},
		{Name: SubscriptionsFile, Description: "Monthly Odoo membership subscription snapshot.", Private: true},
		{Name: AnalyticEnrichmentFile, Description: "Monthly Odoo analytic/category enrichment for transactions.", Private: false},
		{Name: "private/invoices.json", Description: "Monthly Odoo customer invoices with PII.", Private: true},
		{Name: "private/bills.json", Description: "Monthly Odoo vendor bills with PII.", Private: true},
		{Name: "private/attachments/<kind>/<document-id>/<attachment-id>.<ext>", Description: "Downloaded Odoo invoice and bill attachments.", Private: true},
	}
}

// pathNamespace, when set, scopes every Odoo file under
// providers/odoo/<namespace>/… so data from different Odoo databases never
// shares a directory. It is the sanitised database name, set once at startup by
// the cmd layer via SetPathNamespace (empty → legacy un-namespaced layout, so
// installs with no Odoo configured are unaffected).
var pathNamespace string

// SetPathNamespace scopes all subsequent Odoo paths to the given database
// namespace. Pass "" to use the legacy un-namespaced layout.
func SetPathNamespace(ns string) {
	pathNamespace = ns
}

// PathNamespace returns the currently active database namespace ("" if none).
func PathNamespace() string {
	return pathNamespace
}

func RelPath(elems ...string) string {
	parts := []string{"providers", Source}
	if pathNamespace != "" {
		parts = append(parts, pathNamespace)
	}
	parts = append(parts, elems...)
	return filepath.Join(parts...)
}

func PrivateRelPath(elems ...string) string {
	parts := append([]string{"private"}, elems...)
	return RelPath(parts...)
}

// LegacyRelPath / LegacyPrivateRelPath return the pre-namespacing path (no
// per-database segment). Readers use these to fall back to data synced before
// per-database namespacing was introduced; writers always use the namespaced
// RelPath so a re-sync moves the data forward.
func LegacyRelPath(elems ...string) string {
	parts := append([]string{"providers", Source}, elems...)
	return filepath.Join(parts...)
}

func LegacyPrivateRelPath(elems ...string) string {
	return LegacyRelPath(append([]string{"private"}, elems...)...)
}

func Path(dataDir, year, month string, elems ...string) string {
	parts := []string{dataDir, year}
	if month != "" {
		parts = append(parts, month)
	}
	parts = append(parts, RelPath(elems...))
	return filepath.Join(parts...)
}

func PrivatePath(dataDir, year, month string, elems ...string) string {
	parts := append([]string{"private"}, elems...)
	return Path(dataDir, year, month, parts...)
}

func WriteJSON(dataDir, year, month string, v interface{}, elems ...string) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	path := Path(dataDir, year, month, elems...)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		return err
	}
	_ = os.Chmod(filepath.Dir(path), 0700)
	_ = os.Chmod(path, 0600)
	return nil
}
