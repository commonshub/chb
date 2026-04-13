package cmd

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// StripeCSVEnrichment holds categorization data imported from a CSV export.
type StripeCSVEnrichment struct {
	ImportedAt string                          `json:"importedAt"`
	Source     string                          `json:"source"` // original CSV filename
	Entries    map[string]*StripeCSVEntry      `json:"entries"` // keyed by txn_id
}

type StripeCSVEntry struct {
	Category         string `json:"category,omitempty"`         // MEMBERSHIP, TICKET, DONATION, COWORKING, etc.
	Collective       string `json:"collective,omitempty"`       // commonshub, openletter, etc.
	Product          string `json:"product,omitempty"`          // monthly membership, daypass, etc.
	PayoutDescriptor string `json:"payoutDescriptor,omitempty"` // manual payout descriptor
	PayoutDate       string `json:"payoutDate,omitempty"`
}

func stripeCSVEnrichmentPath() string {
	return filepath.Join(chbDir(), "stripe-csv-enrichment.json")
}

func LoadStripeCSVEnrichment() *StripeCSVEnrichment {
	data, err := os.ReadFile(stripeCSVEnrichmentPath())
	if err != nil {
		return nil
	}
	var e StripeCSVEnrichment
	if json.Unmarshal(data, &e) != nil {
		return nil
	}
	return &e
}

// ImportStripeCSV imports a categorization CSV into the enrichment file.
func ImportStripeCSV(csvPath string) error {
	f, err := os.Open(csvPath)
	if err != nil {
		return fmt.Errorf("failed to open %s: %v", csvPath, err)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to parse CSV: %v", err)
	}

	if len(records) < 2 {
		return fmt.Errorf("CSV has no data rows")
	}

	header := records[0]
	colIdx := map[string]int{}
	for i, h := range header {
		colIdx[strings.TrimSpace(h)] = i
	}

	// Verify required columns
	idCol, ok := colIdx["id"]
	if !ok {
		return fmt.Errorf("CSV missing 'id' column")
	}

	getCol := func(row []string, name string) string {
		if idx, ok := colIdx[name]; ok && idx < len(row) {
			return strings.TrimSpace(row[idx])
		}
		return ""
	}

	// Load existing enrichment to merge
	existing := LoadStripeCSVEnrichment()
	if existing == nil {
		existing = &StripeCSVEnrichment{Entries: map[string]*StripeCSVEntry{}}
	}

	imported := 0
	skipped := 0
	for _, row := range records[1:] {
		if idCol >= len(row) {
			continue
		}
		txnID := strings.TrimSpace(row[idCol])
		if txnID == "" {
			continue
		}

		cat := strings.ToUpper(getCol(row, "category"))
		// Skip internal categories
		if cat == "PAYOUT" || cat == "FEE" || cat == "STRIPE FEE" || cat == "INTERNAL" || cat == "EURB" {
			skipped++
			continue
		}

		entry := &StripeCSVEntry{
			Category:         normalizeCSVCategory(cat),
			Collective:       getCol(row, "collective"),
			Product:          getCol(row, "product"),
			PayoutDescriptor: getCol(row, "Payout descriptor"),
			PayoutDate:       getCol(row, "Payout date"),
		}

		// Only store if there's useful data
		if entry.Category != "" || entry.Collective != "" || entry.Product != "" || entry.PayoutDescriptor != "" {
			existing.Entries[txnID] = entry
			imported++
		}
	}

	existing.ImportedAt = fmt.Sprintf("%s", strings.Replace(csvPath, os.Getenv("HOME"), "~", 1))
	existing.Source = filepath.Base(csvPath)

	data, _ := json.MarshalIndent(existing, "", "  ")
	if err := os.WriteFile(stripeCSVEnrichmentPath(), data, 0644); err != nil {
		return fmt.Errorf("failed to save: %v", err)
	}

	fmt.Printf("  %s✓ Imported %d entries, skipped %d (internal/fee/payout), total %d%s\n",
		Fmt.Green, imported, skipped, len(existing.Entries), Fmt.Reset)
	return nil
}

// normalizeCSVCategory maps CSV categories to local category slugs.
func normalizeCSVCategory(cat string) string {
	switch cat {
	case "MEMBERSHIP":
		return "membership"
	case "TICKET", "TICKET-NOVAT":
		return "tickets"
	case "DONATION":
		return "donations"
	case "COWORKING":
		return "coworking"
	case "SERVICE":
		return "services"
	case "DEBT":
		return "other-expense"
	default:
		return strings.ToLower(cat)
	}
}
