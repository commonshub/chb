package odoo

import (
	"encoding/json"
	"os"
	"path/filepath"

	"github.com/CommonsHub/chb/sources"
)

const (
	Source                 = "odoo"
	InvoicesFile           = "invoices.json"
	BillsFile              = "bills.json"
	SubscriptionsFile      = "subscriptions.json"
	AnalyticEnrichmentFile = "analytic-enrichment.json"
)

type SourceProvider struct{}

func (SourceProvider) Name() string {
	return Source
}

func (SourceProvider) Files() []sources.File {
	return []sources.File{
		{Name: InvoicesFile, Description: "Monthly public Odoo customer invoices.", Private: false},
		{Name: BillsFile, Description: "Monthly public Odoo vendor bills.", Private: false},
		{Name: SubscriptionsFile, Description: "Monthly Odoo membership subscription snapshot.", Private: true},
		{Name: AnalyticEnrichmentFile, Description: "Monthly Odoo analytic/category enrichment for transactions.", Private: false},
		{Name: "private/invoices.json", Description: "Monthly Odoo customer invoices with PII.", Private: true},
		{Name: "private/bills.json", Description: "Monthly Odoo vendor bills with PII.", Private: true},
		{Name: "private/attachments/<kind>/<document-id>/<attachment-id>.<ext>", Description: "Downloaded Odoo invoice and bill attachments.", Private: true},
	}
}

func RelPath(elems ...string) string {
	parts := append([]string{"sources", Source}, elems...)
	return filepath.Join(parts...)
}

func PrivateRelPath(elems ...string) string {
	parts := append([]string{"private"}, elems...)
	return RelPath(parts...)
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
