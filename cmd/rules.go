package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// RuleMatch defines what a rule matches against.
type RuleMatch struct {
	Sender      string `json:"sender,omitempty"`      // glob on counterparty for incoming (IBAN, 0xaddr, name)
	Recipient   string `json:"recipient,omitempty"`   // glob on counterparty for outgoing (IBAN, 0xaddr, name)
	Description string `json:"description,omitempty"` // glob on tx description/memo
	Account     string `json:"account,omitempty"`     // account slug (fridge, coffee, stripe, savings)
	Provider    string `json:"provider,omitempty"`    // stripe, etherscan, monerium
	Currency    string `json:"currency,omitempty"`    // EUR, EURe, EURb, CHT
	Direction   string `json:"direction,omitempty"`   // "in" or "out"
	Application string `json:"application,omitempty"` // stripe connect app: Luma, Open Collective, etc.
}

// RuleAssign defines what a matching rule assigns.
type RuleAssign struct {
	Category   string `json:"category"`             // category slug (required)
	Collective string `json:"collective,omitempty"` // collective slug
	Event      string `json:"event,omitempty"`      // event UID
}

// Rule is a categorization rule.
type Rule struct {
	Match  RuleMatch  `json:"match"`
	Assign RuleAssign `json:"assign"`
}

func rulesPath() string {
	return settingsFilePath("rules.json")
}

// LoadRules reads rules from APP_DATA_DIR/settings/rules.json.
// On first load, migrates from settings.json if rules.json doesn't exist.
func LoadRules() ([]Rule, error) {
	data, err := os.ReadFile(existingSettingsFilePath("rules.json"))
	if err != nil {
		if os.IsNotExist(err) {
			// Try migration from settings.json
			rules := migrateRulesFromSettings()
			if len(rules) > 0 {
				SaveRules(rules)
				return rules, nil
			}
			return []Rule{}, nil
		}
		return nil, err
	}
	var rules []Rule
	if err := json.Unmarshal(data, &rules); err != nil {
		return nil, err
	}
	return rules, nil
}

// SaveRules writes rules to APP_DATA_DIR/settings/rules.json.
func SaveRules(rules []Rule) error {
	data, err := json.MarshalIndent(rules, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(rulesPath())
	os.MkdirAll(dir, 0755)
	return os.WriteFile(rulesPath(), data, 0644)
}

// migrateRulesFromSettings converts old CategoryRule entries from settings.json.
func migrateRulesFromSettings() []Rule {
	settings, err := LoadSettings()
	if err != nil || settings.Accounting == nil {
		return nil
	}

	var rules []Rule
	for _, old := range settings.Accounting.Rules {
		r := Rule{
			Assign: RuleAssign{
				Category:   old.Category,
				Collective: old.Collective,
			},
		}

		// Convert old Match field to sender/description based on context
		if old.Match != "" {
			// Old "match" was against counterparty/description
			r.Match.Description = old.Match
		}
		if old.Account != "" {
			r.Match.Account = old.Account
		}
		if old.Provider != "" {
			r.Match.Provider = old.Provider
		}
		if old.Currency != "" {
			r.Match.Currency = old.Currency
		}
		if old.TxType != "" {
			if strings.EqualFold(old.TxType, "CREDIT") {
				r.Match.Direction = "in"
			} else if strings.EqualFold(old.TxType, "DEBIT") {
				r.Match.Direction = "out"
			}
		}

		rules = append(rules, r)
	}

	return rules
}

// MatchesTransaction checks if a rule matches a transaction.
func (r *Rule) MatchesTransaction(tx TransactionEntry) bool {
	m := r.Match

	if m.Account != "" {
		if !strings.EqualFold(m.Account, tx.AccountSlug) {
			return false
		}
	}

	if m.Provider != "" {
		if !strings.EqualFold(m.Provider, tx.Provider) {
			return false
		}
	}

	if m.Currency != "" {
		if !strings.EqualFold(m.Currency, tx.Currency) {
			return false
		}
	}

	if m.Direction != "" {
		if m.Direction == "in" && tx.Type != "CREDIT" {
			return false
		}
		if m.Direction == "out" && tx.Type != "DEBIT" {
			return false
		}
	}

	if m.Application != "" {
		txApp := tx.Application
		if app, ok := tx.Metadata["application"]; ok {
			if s, ok := app.(string); ok {
				txApp = s
			}
		}
		if !strings.EqualFold(m.Application, txApp) {
			return false
		}
	}

	if m.Sender != "" {
		if tx.Type != "CREDIT" {
			return false
		}
		target := strings.ToLower(tx.Counterparty)
		if !globMatch(strings.ToLower(m.Sender), target) {
			return false
		}
	}

	if m.Recipient != "" {
		if tx.Type != "DEBIT" {
			return false
		}
		target := strings.ToLower(tx.Counterparty)
		if !globMatch(strings.ToLower(m.Recipient), target) {
			return false
		}
	}

	if m.Description != "" {
		target := strings.ToLower(tx.Counterparty)
		if desc, ok := tx.Metadata["description"]; ok {
			if s, ok := desc.(string); ok {
				target = strings.ToLower(s)
			}
		}
		if !globMatch(strings.ToLower(m.Description), target) {
			return false
		}
	}

	return true
}

// RuleSummary returns a human-readable summary of the match conditions.
func (r *Rule) RuleSummary() string {
	var parts []string
	if r.Match.Sender != "" {
		parts = append(parts, fmt.Sprintf("sender: %s", r.Match.Sender))
	}
	if r.Match.Recipient != "" {
		parts = append(parts, fmt.Sprintf("recipient: %s", r.Match.Recipient))
	}
	if r.Match.Description != "" {
		parts = append(parts, fmt.Sprintf("description: %s", r.Match.Description))
	}
	if r.Match.Account != "" {
		parts = append(parts, fmt.Sprintf("account: %s", r.Match.Account))
	}
	if r.Match.Provider != "" {
		parts = append(parts, fmt.Sprintf("provider: %s", r.Match.Provider))
	}
	if r.Match.Currency != "" {
		parts = append(parts, fmt.Sprintf("currency: %s", r.Match.Currency))
	}
	if r.Match.Direction != "" {
		parts = append(parts, fmt.Sprintf("direction: %s", r.Match.Direction))
	}
	if r.Match.Application != "" {
		parts = append(parts, fmt.Sprintf("app: %s", r.Match.Application))
	}
	if len(parts) == 0 {
		return "(no conditions)"
	}
	return strings.Join(parts, ", ")
}
