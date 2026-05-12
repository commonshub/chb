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
	IBAN        string `json:"iban,omitempty"`        // exact match on counterparty IBAN (spaces stripped, case-insensitive)
	Account     string `json:"account,omitempty"`     // account slug (fridge, coffee, stripe, savings)
	Provider    string `json:"provider,omitempty"`    // stripe, etherscan, monerium
	Currency    string `json:"currency,omitempty"`    // EUR, EURe, EURb, CHT
	Direction   string `json:"direction,omitempty"`   // "in" or "out"
	Application string `json:"application,omitempty"` // stripe connect app: luma, opencollective, etc.
}

// RuleAssign defines what a matching rule assigns.
// Category/Collective/Event are fallback assignments (only set when empty).
// Type and Description are overrides (always applied when present).
type RuleAssign struct {
	Category    string `json:"category,omitempty"`    // category slug
	Collective  string `json:"collective,omitempty"`  // collective slug
	Event       string `json:"event,omitempty"`       // event UID
	Type        string `json:"type,omitempty"`        // override tx.Type (CREDIT/DEBIT/MINT/BURN/INTERNAL/TRANSFER)
	Description string `json:"description,omitempty"` // override metadata.description
}

// Rule is a categorization rule.
type Rule struct {
	Match  RuleMatch  `json:"match"`
	Assign RuleAssign `json:"assign"`
}

func rulesPath() string {
	return settingsFilePath("rules.json")
}

// LoadRules reads rules from APP_DATA_DIR/settings/rules.json. The file is
// seeded from the embedded defaults on first run and kept in sync by
// EnsureSettingsBootstrapped when the user hasn't edited it locally.
func LoadRules() ([]Rule, error) {
	data, err := os.ReadFile(rulesPath())
	if err != nil {
		if os.IsNotExist(err) {
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
		if m.Direction == "in" && !tx.IsIncoming() {
			return false
		}
		if m.Direction == "out" && !tx.IsOutgoing() {
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

	if m.IBAN != "" {
		txIBAN := normalizeIBAN(stringMetadata(tx.Metadata, "iban"))
		if txIBAN == "" || txIBAN != normalizeIBAN(m.IBAN) {
			return false
		}
	}

	if m.Sender != "" {
		if !tx.IsIncoming() {
			return false
		}
		target := strings.ToLower(tx.Counterparty)
		if !globMatch(strings.ToLower(m.Sender), target) {
			return false
		}
	}

	if m.Recipient != "" {
		if !tx.IsOutgoing() {
			return false
		}
		target := strings.ToLower(tx.Counterparty)
		if !globMatch(strings.ToLower(m.Recipient), target) {
			return false
		}
	}

	if m.Description != "" {
		// Match against the first non-empty of: metadata.description, memo
		// (Monerium SEPA reference, etc.), then the raw counterparty. All
		// comparisons are lower-cased so the rule pattern is case-insensitive.
		description := stringMetadata(tx.Metadata, "description")
		if description == "" {
			description = stringMetadata(tx.Metadata, "memo")
		}
		if description == "" {
			description = tx.Counterparty
		}
		if !globMatch(strings.ToLower(m.Description), strings.ToLower(description)) {
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
	if r.Match.IBAN != "" {
		parts = append(parts, fmt.Sprintf("iban: %s", r.Match.IBAN))
	}
	if len(parts) == 0 {
		return "(no conditions)"
	}
	return strings.Join(parts, ", ")
}
