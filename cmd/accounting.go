package cmd

import (
	"strings"
)

// AccountingSettings holds the accounting configuration from settings.json.
// Rules have moved to APP_DATA_DIR/settings/rules.json — the Rules field is kept for migration only.
type AccountingSettings struct {
	Categories        []CategoryDef         `json:"categories"`
	DefaultCollective string                `json:"defaultCollective,omitempty"` // e.g. "commonshub"
	Rules             []CategoryRule        `json:"rules,omitempty"`             // DEPRECATED: migrated to rules.json
	Odoo              *OdooAccountingConfig `json:"odoo,omitempty"`
}

// OdooAccountingConfig holds the Odoo→local category mapping.
type OdooAccountingConfig struct {
	// CategoryMapping maps Odoo analytic account ID (as string) to local category slug.
	CategoryMapping map[string]string `json:"categoryMapping"`
}

// CategoryDef defines a category with its slug, label, and direction.
type CategoryDef struct {
	Slug      string `json:"slug"`
	Label     string `json:"label"`
	Direction string `json:"direction"` // "income" or "expense"
}

// CategoryRule maps transactions to categories based on matching criteria.
// Fields are ANDed: all non-empty fields must match.
type CategoryRule struct {
	// Match criteria (all non-empty fields must match)
	Account  string `json:"account,omitempty"`  // account slug (e.g. "fridge", "coffee")
	Match    string `json:"match,omitempty"`    // glob pattern on counterparty/description
	Provider string `json:"provider,omitempty"` // "stripe", "etherscan", "monerium"
	Currency string `json:"currency,omitempty"` // "EUR", "EURe", "CHT"
	TxType   string `json:"txType,omitempty"`   // "CREDIT", "DEBIT"

	// Assignment
	Category   string `json:"category"`             // category slug
	Collective string `json:"collective,omitempty"` // collective slug
}

// DefaultAccountingSettings returns a sensible default config for a commons/coworking space.
func DefaultAccountingSettings() *AccountingSettings {
	return &AccountingSettings{
		Categories: []CategoryDef{
			// Income
			{Slug: "membership", Label: "Membership", Direction: "income"},
			{Slug: "donation", Label: "Donation", Direction: "income"},
			{Slug: "rentals", Label: "Rentals", Direction: "income"},
			{Slug: "fridge", Label: "Fridge", Direction: "income"},
			{Slug: "ticket", Label: "Ticket", Direction: "income"},
			{Slug: "grants", Label: "Grants", Direction: "income"},
			{Slug: "other-income", Label: "Other Income", Direction: "income"},
			// Expenses
			{Slug: "rent", Label: "Rent", Direction: "expense"},
			{Slug: "salaries", Label: "Salaries", Direction: "expense"},
			{Slug: "catering", Label: "Catering", Direction: "expense"},
			{Slug: "utilities", Label: "Utilities", Direction: "expense"},
			{Slug: "insurance", Label: "Insurance", Direction: "expense"},
			{Slug: "supplies", Label: "Supplies", Direction: "expense"},
			{Slug: "equipment", Label: "Equipment", Direction: "expense"},
			{Slug: "services", Label: "Services", Direction: "expense"},
			{Slug: "taxes", Label: "Taxes", Direction: "expense"},
			{Slug: "events", Label: "Events", Direction: "expense"},
			{Slug: "other-expense", Label: "Other Expense", Direction: "expense"},
		},
		Rules: []CategoryRule{},
	}
}

// Categorizer applies rules to classify transactions.
type Categorizer struct {
	rules      []Rule
	categories map[string]CategoryDef
}

// NewCategorizer creates a categorizer from categories.json + rules.json.
func NewCategorizer(settings *Settings) *Categorizer {
	c := &Categorizer{
		categories: make(map[string]CategoryDef),
	}

	for _, cat := range LoadCategories() {
		c.categories[cat.Slug] = cat
	}

	c.rules, _ = LoadRules()

	return c
}

// Categorize returns the category slug for a transaction, or "" if uncategorized.
func (c *Categorizer) Categorize(tx TransactionEntry) string {
	for _, rule := range c.rules {
		if rule.MatchesTransaction(tx) {
			return rule.Assign.Category
		}
	}
	return ""
}

// CollectiveFor returns the collective slug for a transaction, or "" if none.
func (c *Categorizer) CollectiveFor(tx TransactionEntry) string {
	for _, rule := range c.rules {
		if rule.MatchesTransaction(tx) && rule.Assign.Collective != "" {
			return rule.Assign.Collective
		}
	}
	return ""
}

// Apply walks the rules and assigns category/collective/event (fallback —
// only when empty), plus type/description (override — always applied) from
// every matching rule. Finally stamps metadata.vatAmount on incoming
// EUR/EURe transactions whose category isn't VAT-exempt.
func (c *Categorizer) Apply(tx *TransactionEntry) {
	for _, rule := range c.rules {
		if !rule.MatchesTransaction(*tx) {
			continue
		}
		if rule.Assign.Category != "" && tx.Category == "" {
			tx.Category = rule.Assign.Category
		}
		if rule.Assign.Collective != "" && tx.Collective == "" {
			tx.Collective = rule.Assign.Collective
		}
		if rule.Assign.Event != "" && tx.Event == "" {
			tx.Event = rule.Assign.Event
		}
		if rule.Assign.Type != "" {
			tx.Type = rule.Assign.Type
		}
		if rule.Assign.Description != "" {
			if tx.Metadata == nil {
				tx.Metadata = map[string]interface{}{}
			}
			tx.Metadata["description"] = rule.Assign.Description
		}
	}
	stampVAT(tx)
}

// vatRate is the Belgian standard VAT rate. The gross amount is treated as
// VAT-inclusive, so vatAmount = gross × rate / (1 + rate).
const vatRate = 0.21

// vatExemptCategories never get a vatAmount stamped on them: rent doesn't
// invoke VAT (or VAT is paid separately on the landlord side), donations
// have no taxable consideration, memberships are exempt.
var vatExemptCategories = map[string]struct{}{
	"rent":       {},
	"donation":   {},
	"membership": {},
}

// stampVAT computes metadata.vatAmount for incoming EUR/EURe transactions
// whose category isn't VAT-exempt. Re-stamps each call so categorization
// changes are reflected; if a non-VAT category gets assigned later, the
// stamp is cleared. Only writes when the tx has a positive grossAmount.
func stampVAT(tx *TransactionEntry) {
	if tx == nil || !tx.IsIncoming() {
		clearMetadataKey(tx, "vatAmount")
		return
	}
	if !isEURCurrency(tx.Currency) {
		clearMetadataKey(tx, "vatAmount")
		return
	}
	if _, exempt := vatExemptCategories[tx.Category]; exempt {
		clearMetadataKey(tx, "vatAmount")
		return
	}
	gross := tx.GrossAmount
	if gross <= 0 {
		clearMetadataKey(tx, "vatAmount")
		return
	}
	vat := roundCents(gross * vatRate / (1 + vatRate))
	if vat <= 0 {
		clearMetadataKey(tx, "vatAmount")
		return
	}
	if tx.Metadata == nil {
		tx.Metadata = map[string]interface{}{}
	}
	tx.Metadata["vatAmount"] = vat
}

func clearMetadataKey(tx *TransactionEntry, key string) {
	if tx == nil || tx.Metadata == nil {
		return
	}
	delete(tx.Metadata, key)
}

// CategoryLabel returns the human label for a category slug.
func (c *Categorizer) CategoryLabel(slug string) string {
	if cat, ok := c.categories[slug]; ok {
		return cat.Label
	}
	return slug
}

// CategoryDirection returns "income" or "expense" for a category slug.
func (c *Categorizer) CategoryDirection(slug string) string {
	if cat, ok := c.categories[slug]; ok {
		return cat.Direction
	}
	return ""
}

// globMatch does simple glob matching with * wildcards.
func globMatch(pattern, s string) bool {
	if pattern == "*" {
		return true
	}

	// Handle prefix*, *suffix, *contains*
	if strings.HasPrefix(pattern, "*") && strings.HasSuffix(pattern, "*") {
		return strings.Contains(s, pattern[1:len(pattern)-1])
	}
	if strings.HasSuffix(pattern, "*") {
		return strings.HasPrefix(s, pattern[:len(pattern)-1])
	}
	if strings.HasPrefix(pattern, "*") {
		return strings.HasSuffix(s, pattern[1:])
	}

	return s == pattern
}
